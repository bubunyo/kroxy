package proxy_test

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/proxy"
	"github.com/bubunyo/kroxy/resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fakeBroker is a minimal in-process Kafka-protocol server used to validate
// the proxy's upstream pipeline. It performs the SASL/PLAIN handshake and
// then replies to any subsequent request with a fixed sentinel payload that
// echoes the request's correlation id.
type fakeBroker struct {
	t        *testing.T
	addr     string
	ln       net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	gotCreds string
	gotKeys  []int16

	// Topics that exist on the (synthetic) upstream cluster. Used by the
	// Metadata handler to build realistic responses.
	upstreamTopics []string
	// Last Metadata request the broker saw, post-decode. Useful for
	// asserting topic-name rewriting on the request leg.
	lastMetadataTopics []string
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &fakeBroker{t: t, ln: ln, addr: ln.Addr().String()}
	b.wg.Add(1)
	go b.serve()
	return b
}

func (b *fakeBroker) close() { _ = b.ln.Close(); b.wg.Wait() }

func (b *fakeBroker) serve() {
	defer b.wg.Done()
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.wg.Add(1)
		go func() { defer b.wg.Done(); b.handle(c) }()
	}
}

func (b *fakeBroker) handle(c net.Conn) {
	defer c.Close()
	authed := false
	for {
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		hdr, err := protocol.ParseRequestHeader(frame)
		if err != nil {
			return
		}
		body := frame[hdr.HeaderSize:]

		switch hdr.APIKey {
		case protocol.ApiVersionsKey:
			resp := kmsg.NewPtrApiVersionsResponse()
			resp.SetVersion(hdr.APIVersion)
			resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
				{ApiKey: protocol.SaslHandshakeKey, MaxVersion: 1},
				{ApiKey: protocol.SaslAuthenticateKey, MaxVersion: 1},
			}
			b.write(c, resp, hdr)
		case protocol.SaslHandshakeKey:
			resp := kmsg.NewPtrSASLHandshakeResponse()
			resp.SetVersion(hdr.APIVersion)
			resp.SupportedMechanisms = []string{"PLAIN"}
			b.write(c, resp, hdr)
		case protocol.SaslAuthenticateKey:
			req := kmsg.NewPtrSASLAuthenticateRequest()
			req.SetVersion(hdr.APIVersion)
			_ = req.ReadFrom(body)
			b.mu.Lock()
			b.gotCreds = string(req.SASLAuthBytes)
			b.mu.Unlock()
			resp := kmsg.NewPtrSASLAuthenticateResponse()
			resp.SetVersion(hdr.APIVersion)
			b.write(c, resp, hdr)
			authed = true
		default:
			if !authed {
				return
			}
			b.mu.Lock()
			b.gotKeys = append(b.gotKeys, hdr.APIKey)
			b.mu.Unlock()

			if hdr.APIKey == protocol.MetadataKey {
				b.handleMetadata(c, hdr, body)
				continue
			}

			// Synthesise a fixed payload: response header + 4 bytes "PING".
			out := protocol.AppendResponseHeader(nil,
				hdr.CorrelationID,
				protocol.ResponseHeaderVersion(hdr.APIKey, hdr.APIVersion),
			)
			out = append(out, 'P', 'I', 'N', 'G')
			_ = protocol.WriteFrame(c, out)
		}
	}
}

func (b *fakeBroker) handleMetadata(c net.Conn, hdr protocol.RequestHeader, body []byte) {
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(hdr.APIVersion)
	_ = req.ReadFrom(body)

	requested := make([]string, 0, len(req.Topics))
	for _, t := range req.Topics {
		if t.Topic != nil {
			requested = append(requested, *t.Topic)
		}
	}
	b.mu.Lock()
	b.lastMetadataTopics = requested
	b.mu.Unlock()

	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.Brokers = []kmsg.MetadataResponseBroker{
		{NodeID: 11, Host: "real-broker-1", Port: 9092},
		{NodeID: 12, Host: "real-broker-2", Port: 9092},
	}
	resp.ControllerID = 11

	for _, name := range b.upstreamTopics {
		topic := name
		t := kmsg.NewMetadataResponseTopic()
		t.Topic = &topic
		p := kmsg.NewMetadataResponseTopicPartition()
		p.Partition = 0
		p.Leader = 11
		p.Replicas = []int32{11, 12}
		p.ISR = []int32{11, 12}
		t.Partitions = []kmsg.MetadataResponseTopicPartition{p}
		resp.Topics = append(resp.Topics, t)
	}

	out := protocol.EncodeResponse(resp, hdr.CorrelationID, hdr.APIKey, hdr.APIVersion)
	_ = protocol.WriteFrame(c, out)
}

func (b *fakeBroker) write(c net.Conn, resp kmsg.Response, hdr protocol.RequestHeader) {
	out := protocol.EncodeResponse(resp, hdr.CorrelationID, hdr.APIKey, hdr.APIVersion)
	_ = protocol.WriteFrame(c, out)
}

func startTestServerWithUpstream(t *testing.T, upstreamAddr string) (string, func()) {
	t.Helper()
	r, err := resolver.NewMemory([]resolver.MemoryUser{
		{
			Username: "alice", Password: "alicepw",
			TenantID: "tenantA", TopicPrefix: "tenantA.",
			Upstream:     upstreamAddr,
			UpstreamSASL: resolver.SASLCreds{Username: "kroxy", Password: "kroxypw"},
		},
	})
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	srv := proxy.NewServer(proxy.ServerConfig{Listen: addr, Advertised: addr}, r, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	waitDial(t, addr)
	return addr, func() { cancel(); <-done }
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestM2_PassthroughForwardsAndRewritesCorrelationID(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	defer broker.close()
	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	// Authenticate.
	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 1, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	require.Equal(t, int16(0), hsResp.ErrorCode)

	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00alice\x00alicepw")
	sendRequest(t, c, authReq, 2, "test")
	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	require.Equal(t, int16(0), authResp.ErrorCode)

	// Now send a DescribeConfigs request — still byte-passthrough in M3.
	dcReq := kmsg.NewPtrDescribeConfigsRequest()
	dcReq.SetVersion(0)
	clientCID := int32(4242)
	sendRequest(t, c, dcReq, clientCID, "test")

	// We expect a frame with the original correlation id and the fake's
	// PING payload after the response header.
	frame, err := protocol.ReadFrame(c)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(frame), 4)
	gotCID := int32(binary.BigEndian.Uint32(frame[0:4]))
	assert.Equal(t, clientCID, gotCID, "client must see original correlation id")

	off := 4
	if protocol.ResponseHeaderVersion(protocol.DescribeConfigsKey, 0) >= 1 {
		off++
	}
	assert.Equal(t, []byte("PING"), frame[off:])

	// Verify the upstream actually saw a DescribeConfigs request and that
	// we sent PLAIN credentials to it (not the client's).
	broker.mu.Lock()
	defer broker.mu.Unlock()
	assert.Contains(t, broker.gotKeys, protocol.DescribeConfigsKey)
	assert.Equal(t, "\x00kroxy\x00kroxypw", broker.gotCreds)
}

func TestM2_RequestBeforeAuthIsRejected(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	defer broker.close()
	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	dcReq := kmsg.NewPtrDescribeConfigsRequest()
	dcReq.SetVersion(0)
	sendRequest(t, c, dcReq, 1, "test")

	// Proxy should close the connection on unauthenticated traffic.
	_, err = protocol.ReadFrame(c)
	assert.Error(t, err)
	assert.True(t, err == io.EOF || err == io.ErrUnexpectedEOF || isClosedConnErr(err))
}

func isClosedConnErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, sub := range []string{"use of closed", "connection reset", "EOF", "broken pipe"} {
		if contains(s, sub) {
			return true
		}
	}
	return false
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// authenticate runs the SASL/PLAIN handshake against the proxy and returns
// the dialled connection.
func authenticate(t *testing.T, addr, username, password string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 1, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	require.Equal(t, int16(0), hsResp.ErrorCode)

	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00" + username + "\x00" + password)
	sendRequest(t, c, authReq, 2, "test")
	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	require.Equal(t, int16(0), authResp.ErrorCode)
	return c
}

func TestM3_MetadataRewrite_PrefixesAndStrips(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	broker.upstreamTopics = []string{"tenantA.orders", "tenantA.payments", "tenantB.secrets"}
	defer broker.close()
	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c := authenticate(t, addr, "alice", "alicepw")
	defer c.Close()

	mdReq := kmsg.NewPtrMetadataRequest()
	mdReq.SetVersion(1)
	mdReq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: strPtrM3("orders")},
		{Topic: strPtrM3("payments")},
	}
	sendRequest(t, c, mdReq, 99, "test")

	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(1)
	gotCID := recvResponse(t, c, resp, protocol.MetadataKey, 1)
	assert.Equal(t, int32(99), gotCID)

	// Upstream must have seen prefixed topic names.
	broker.mu.Lock()
	mdTopics := append([]string(nil), broker.lastMetadataTopics...)
	broker.mu.Unlock()
	assert.ElementsMatch(t, []string{"tenantA.orders", "tenantA.payments"}, mdTopics)

	// Client must see prefix-stripped topics, no tenantB leakage,
	// single virtual broker pointing at the proxy.
	require.Len(t, resp.Brokers, 1)
	assert.Equal(t, int32(0), resp.Brokers[0].NodeID)
	require.Len(t, resp.Topics, 2, "tenantB.secrets must not be visible")
	names := []string{*resp.Topics[0].Topic, *resp.Topics[1].Topic}
	assert.ElementsMatch(t, []string{"orders", "payments"}, names)
	for _, t1 := range resp.Topics {
		for _, p := range t1.Partitions {
			assert.Equal(t, int32(0), p.Leader)
			for _, r := range p.Replicas {
				assert.Equal(t, int32(0), r)
			}
		}
	}
}

func strPtrM3(s string) *string { return &s }
