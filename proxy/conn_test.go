package proxy_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/proxy"
	"github.com/bubunyo/kroxy/resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	r, err := resolver.NewMemoryResolver([]resolver.Tenant{
		{ID: "alice", TopicPrefix: "tenantA.", Upstream: "kafka:9092"},
	})
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bind ourselves first to get a port; then create the server with that port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	srv := proxy.NewServer(proxy.ServerConfig{Listen: addr, Advertised: addr}, r, nil, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return addr, func() { cancel(); <-done }
}

func sendRequest(t *testing.T, c net.Conn, req kmsg.Request, correlationID int32, clientID string) {
	t.Helper()
	f := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientID))
	// AppendRequest prepends the 4-byte length prefix; write it raw.
	full := f.AppendRequest(nil, req, correlationID)
	_, err := c.Write(full)
	require.NoError(t, err)
}

func recvResponse(t *testing.T, c net.Conn, resp kmsg.Response, apiKey, apiVersion int16) (correlationID int32) {
	t.Helper()
	frame, err := protocol.ReadFrame(c)
	require.NoError(t, err)
	hv := protocol.ResponseHeaderVersion(apiKey, apiVersion)
	off := 4
	correlationID = int32(uint32(frame[0])<<24 | uint32(frame[1])<<16 | uint32(frame[2])<<8 | uint32(frame[3]))
	if hv >= 1 {
		off++ // empty tagged fields
	}
	require.NoError(t, resp.ReadFrom(frame[off:]))
	return correlationID
}

func TestM1_ApiVersionsThenSaslPlain(t *testing.T) {
	t.Parallel()

	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	// 1. ApiVersions
	avReq := kmsg.NewPtrApiVersionsRequest()
	avReq.SetVersion(3)
	avReq.ClientSoftwareName = "test"
	avReq.ClientSoftwareVersion = "0"
	sendRequest(t, c, avReq, 1, "test")

	avResp := kmsg.NewPtrApiVersionsResponse()
	avResp.SetVersion(3)
	cid := recvResponse(t, c, avResp, protocol.ApiVersionsKey, 3)
	assert.Equal(t, int32(1), cid)
	assert.Equal(t, int16(0), avResp.ErrorCode)
	require.NotEmpty(t, avResp.ApiKeys)

	// 2. SaslHandshake (PLAIN)
	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 2, "test")

	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	cid = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	assert.Equal(t, int32(2), cid)
	assert.Equal(t, int16(0), hsResp.ErrorCode)
	assert.Contains(t, hsResp.SupportedMechanisms, "PLAIN")

	// 3. SaslAuthenticate with valid creds
	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00alice\x00alicepw")
	sendRequest(t, c, authReq, 3, "test")

	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	cid = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	assert.Equal(t, int32(3), cid)
	assert.Equal(t, int16(0), authResp.ErrorCode, "expected auth ok, got msg=%v", strOrNil(authResp.ErrorMessage))
}

// TestM1_AnyPasswordAccepted asserts kroxy v1 ignores the SASL/PLAIN
// password byte field; auth succeeds for a known username regardless of the
// password value supplied by the client.
func TestM1_AnyPasswordAccepted(t *testing.T) {
	t.Parallel()

	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 1, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)

	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00alice\x00literally-anything")
	sendRequest(t, c, authReq, 2, "test")

	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	assert.Equal(t, int16(0), authResp.ErrorCode, "expected auth ok, got msg=%v", strOrNil(authResp.ErrorMessage))
}

// TestM1_UnknownUserFails asserts auth fails when the username is not in
// the resolver, even if the password field is non-empty.
func TestM1_UnknownUserFails(t *testing.T) {
	t.Parallel()

	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 1, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)

	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00ghost\x00anything")
	sendRequest(t, c, authReq, 2, "test")

	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	assert.NotEqual(t, int16(0), authResp.ErrorCode)
}

func strOrNil(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
