package proxy_test

import (
	"net"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func scramHandshake(t *testing.T, c net.Conn, mechanism string, cid *int32) {
	t.Helper()
	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = mechanism
	*cid++
	sendRequest(t, c, hsReq, *cid, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	require.Equal(t, int16(0), hsResp.ErrorCode, "handshake rejected: mechs=%v", hsResp.SupportedMechanisms)
}

func sendSaslAuth(t *testing.T, c net.Conn, payload []byte, cid *int32) *kmsg.SASLAuthenticateResponse {
	t.Helper()
	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = payload
	*cid++
	sendRequest(t, c, authReq, *cid, "test")
	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	_ = recvResponse(t, c, authResp, protocol.SaslAuthenticateKey, 1)
	return authResp
}

func TestSCRAM_HandshakeAdvertisesAllMechanisms(t *testing.T) {
	t.Parallel()

	addr, stop := startTestServer(t)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = "SCRAM-SHA-256"
	sendRequest(t, c, hsReq, 1, "test")
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	_ = recvResponse(t, c, hsResp, protocol.SaslHandshakeKey, 1)
	assert.Equal(t, int16(0), hsResp.ErrorCode)
	assert.Contains(t, hsResp.SupportedMechanisms, "PLAIN")
	assert.Contains(t, hsResp.SupportedMechanisms, "SCRAM-SHA-256")
	assert.Contains(t, hsResp.SupportedMechanisms, "SCRAM-SHA-512")
}

func TestSCRAM_RelayHappyPath(t *testing.T) {
	t.Parallel()

	upstreamReplies := [][]byte{
		[]byte("server-first-message-stub"),
		[]byte("v=server-final-signature-stub"),
	}
	broker := newFakeBroker(t)
	broker.scramResponses = upstreamReplies
	defer broker.close()

	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	var cid int32
	scramHandshake(t, c, "SCRAM-SHA-256", &cid)

	first := []byte("n,,n=alice,r=clientnonce")
	r1 := sendSaslAuth(t, c, first, &cid)
	assert.Equal(t, int16(0), r1.ErrorCode)
	assert.Equal(t, upstreamReplies[0], r1.SASLAuthBytes)

	final := []byte("c=biws,r=clientnoncesrvnonce,p=proof")
	r2 := sendSaslAuth(t, c, final, &cid)
	assert.Equal(t, int16(0), r2.ErrorCode)
	assert.Equal(t, upstreamReplies[1], r2.SASLAuthBytes)

	broker.mu.Lock()
	got := append([][]byte(nil), broker.receivedSaslBytes...)
	broker.mu.Unlock()
	require.Len(t, got, 2)
	assert.Equal(t, first, got[0])
	assert.Equal(t, final, got[1])
}

func TestSCRAM_UnknownTenantRejected(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	broker.scramResponses = [][]byte{[]byte("server-first")}
	defer broker.close()

	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	var cid int32
	scramHandshake(t, c, "SCRAM-SHA-512", &cid)

	r := sendSaslAuth(t, c, []byte("n,,n=ghost,r=nonce"), &cid)
	assert.NotEqual(t, int16(0), r.ErrorCode)

	// Upstream must not have received any SaslAuthenticate.
	broker.mu.Lock()
	defer broker.mu.Unlock()
	assert.Empty(t, broker.receivedSaslBytes)
}

func TestSCRAM_ChannelBindingRejected(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	broker.scramResponses = [][]byte{[]byte("ignored")}
	defer broker.close()

	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	var cid int32
	scramHandshake(t, c, "SCRAM-SHA-256", &cid)

	r := sendSaslAuth(t, c, []byte("y,,n=alice,r=nonce"), &cid)
	assert.NotEqual(t, int16(0), r.ErrorCode)

	broker.mu.Lock()
	defer broker.mu.Unlock()
	assert.Empty(t, broker.receivedSaslBytes)
}

func TestSCRAM_UpstreamRejectsFinal(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker(t)
	broker.scramResponses = [][]byte{[]byte("server-first"), []byte("ignored")}
	broker.scramFailOnRound = 2
	broker.scramFailCode = 58
	defer broker.close()

	addr, stop := startTestServerWithUpstream(t, broker.addr)
	defer stop()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(3*time.Second)))

	var cid int32
	scramHandshake(t, c, "SCRAM-SHA-256", &cid)

	r1 := sendSaslAuth(t, c, []byte("n,,n=alice,r=nonce"), &cid)
	require.Equal(t, int16(0), r1.ErrorCode)

	r2 := sendSaslAuth(t, c, []byte("c=biws,r=nonce,p=proof"), &cid)
	assert.Equal(t, int16(58), r2.ErrorCode)
}
