package proxy_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
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

// genSelfSignedCert returns a self-signed server certificate valid for the
// loopback addresses, plus a CertPool that trusts it.
func genSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kroxy-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}, pool
}

// startTestServerTLS starts a TLS-terminating proxy and returns its address,
// a CertPool trusting its certificate, and a stop func.
func startTestServerTLS(t *testing.T) (string, *x509.CertPool, func()) {
	t.Helper()
	r, err := resolver.New(resolver.Config{
		Memory: resolver.MemoryConfig{Tenants: []resolver.Tenant{
			{ID: "alice", TopicPrefix: "tenantA.", Upstream: "kafka:9092"},
		}},
	})
	require.NoError(t, err)

	cert, pool := genSelfSignedCert(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	srv := proxy.NewServer(proxy.ServerConfig{
		Listen:     addr,
		Advertised: addr,
		TLS:        &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}, r, nil, log)

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

	return addr, pool, func() { cancel(); <-done }
}

// TestTLS_HandshakeThenSaslPlain verifies a TLS client completes the SASL/PLAIN
// flow through a TLS-terminating listener. SASL runs inside the TLS session.
func TestTLS_HandshakeThenSaslPlain(t *testing.T) {
	t.Parallel()

	addr, pool, stop := startTestServerTLS(t)
	defer stop()

	c, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

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

	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "PLAIN"
	sendRequest(t, c, hsReq, 2, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	assert.Equal(t, int16(0), hsResp.ErrorCode)

	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00alice\x00alicepw")
	sendRequest(t, c, authReq, 3, "test")
	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	assert.Equal(t, int16(0), authResp.ErrorCode, "expected auth ok, got msg=%v", strOrNil(authResp.ErrorMessage))
}

// TestTLS_PlaintextClientRejected verifies a plaintext client cannot speak to a
// TLS listener: the Kafka request bytes are not a valid TLS ClientHello, so the
// server aborts the handshake and the client gets no valid frame back.
func TestTLS_PlaintextClientRejected(t *testing.T) {
	t.Parallel()

	addr, _, stop := startTestServerTLS(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	avReq := kmsg.NewPtrApiVersionsRequest()
	avReq.SetVersion(3)
	avReq.ClientSoftwareName = "test"
	avReq.ClientSoftwareVersion = "0"
	f := kmsg.NewRequestFormatter(kmsg.FormatterClientID("test"))
	_, _ = c.Write(f.AppendRequest(nil, avReq, 1))

	_, err = protocol.ReadFrame(c)
	require.Error(t, err, "plaintext client must not receive a valid frame from a TLS listener")
}

// TestTLS_BadClientDoesNotKillServer asserts a failed/garbage connection does
// not tear down the listener: a subsequent valid TLS client still completes the
// SASL flow.
func TestTLS_BadClientDoesNotKillServer(t *testing.T) {
	t.Parallel()

	addr, pool, stop := startTestServerTLS(t)
	defer stop()

	// A bad client: plaintext garbage against the TLS listener. Its handshake
	// fails in the per-connection handler, not the accept loop.
	bad, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_, _ = bad.Write([]byte("not a tls client hello"))
	_ = bad.Close()

	// The server must still serve a well-behaved TLS client.
	good, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	defer good.Close()
	require.NoError(t, good.SetDeadline(time.Now().Add(3*time.Second)))

	avReq := kmsg.NewPtrApiVersionsRequest()
	avReq.SetVersion(3)
	avReq.ClientSoftwareName = "test"
	avReq.ClientSoftwareVersion = "0"
	sendRequest(t, good, avReq, 1, "test")
	avResp := kmsg.NewPtrApiVersionsResponse()
	avResp.SetVersion(3)
	cid := recvResponse(t, good, avResp, protocol.ApiVersionsKey, 3)
	assert.Equal(t, int32(1), cid)
	assert.Equal(t, int16(0), avResp.ErrorCode)
}
