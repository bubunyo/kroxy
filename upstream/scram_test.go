package upstream

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/bubunyo/kroxy/auth"
	"github.com/bubunyo/kroxy/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestDialSCRAM drives the real franz-go SCRAM client (the same code kroxy uses
// to bridge a PLAIN client to a SCRAM broker) against a minimal in-process
// SCRAM-SHA-256 server. The server implements RFC 5802 verification, so a
// successful dial proves kroxy computed a correct client proof and verified the
// server's signature end to end.
func TestDialSCRAM(t *testing.T) {
	t.Parallel()

	const (
		user = "tenantA"
		pass = "tenantA-secret"
	)

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		addr := startFakeSCRAMBroker(t, user, pass)
		c, err := DialSCRAM(context.Background(), addr, auth.MechanismSCRAMSHA256, user, pass)
		require.NoError(t, err)
		require.NotNil(t, c)
		_ = c.Close()
	})

	t.Run("wrong password is rejected by the broker", func(t *testing.T) {
		t.Parallel()
		addr := startFakeSCRAMBroker(t, user, pass)
		c, err := DialSCRAM(context.Background(), addr, auth.MechanismSCRAMSHA256, user, "not-the-password")
		require.Error(t, err)
		assert.Nil(t, c)
	})

	t.Run("non-scram mechanism is rejected before dialing", func(t *testing.T) {
		t.Parallel()
		c, err := DialSCRAM(context.Background(), "127.0.0.1:0", auth.MechanismPlain, user, pass)
		require.Error(t, err)
		assert.Nil(t, c)
	})
}

// startFakeSCRAMBroker listens on a loopback port and serves the upstream
// handshake (ApiVersions, SaslHandshake, SaslAuthenticate) for a single
// SCRAM-SHA-256 identity, giving each accepted connection fresh SCRAM state.
// It returns the listener address.
func startFakeSCRAMBroker(t *testing.T, user, pass string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go serveFakeBroker(t, nc, &scramServerSession{user: user, pass: pass, salt: []byte("kroxy-test-salt-0001"), iter: 4096})
		}
	}()
	return ln.Addr().String()
}

func serveFakeBroker(t *testing.T, nc net.Conn, s *scramServerSession) {
	defer func() { _ = nc.Close() }()
	for {
		frame, err := protocol.ReadFrame(nc)
		if err != nil {
			return
		}
		hdr, err := protocol.ParseRequestHeader(frame)
		if err != nil {
			t.Errorf("fake broker: parse header: %v", err)
			return
		}
		body := frame[hdr.HeaderSize:]
		switch hdr.APIKey {
		case protocol.ApiVersionsKey:
			resp := kmsg.NewPtrApiVersionsResponse()
			resp.SetVersion(0)
			writeBrokerResponse(t, nc, hdr.CorrelationID, resp.AppendTo(nil))
		case protocol.SaslHandshakeKey:
			resp := kmsg.NewPtrSASLHandshakeResponse()
			resp.SetVersion(1)
			resp.SupportedMechanisms = []string{auth.MechanismSCRAMSHA256}
			writeBrokerResponse(t, nc, hdr.CorrelationID, resp.AppendTo(nil))
		case protocol.SaslAuthenticateKey:
			req := kmsg.NewPtrSASLAuthenticateRequest()
			req.SetVersion(1)
			if err := req.ReadFrom(body); err != nil {
				t.Errorf("fake broker: read authenticate: %v", err)
				return
			}
			out, code, msg := s.step(req.SASLAuthBytes)
			resp := kmsg.NewPtrSASLAuthenticateResponse()
			resp.SetVersion(1)
			resp.SASLAuthBytes = out
			resp.ErrorCode = code
			if msg != "" {
				resp.ErrorMessage = &msg
			}
			writeBrokerResponse(t, nc, hdr.CorrelationID, resp.AppendTo(nil))
		default:
			return
		}
	}
}

// writeBrokerResponse frames a v0/v1 (non-flexible) response: a 4-byte
// correlation id followed by the encoded message body, matching what the
// upstream Conn reads back during the handshake.
func writeBrokerResponse(t *testing.T, nc net.Conn, corrID int32, body []byte) {
	out := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(corrID))
	out = append(out, body...)
	require.NoError(t, protocol.WriteFrame(nc, out))
}

// scramServerSession holds the per-connection SCRAM-SHA-256 server state and
// implements the two-message verification from RFC 5802 §5.
type scramServerSession struct {
	user, pass  string
	salt        []byte
	iter        int
	clientFirst string
	serverFirst string
}

// step consumes one client SCRAM message and returns the server's reply,
// a Kafka error code (0 = ok, 58 = SASL auth failed), and an optional message.
func (s *scramServerSession) step(in []byte) ([]byte, int16, string) {
	msg := string(in)
	if s.serverFirst == "" {
		bare := stripGS2Header(msg)
		s.clientFirst = bare
		cnonce := scramAttr(bare, "r")
		if cnonce == "" {
			return nil, 58, "missing client nonce"
		}
		s.serverFirst = fmt.Sprintf("r=%sservernonce,s=%s,i=%d",
			cnonce, base64.StdEncoding.EncodeToString(s.salt), s.iter)
		return []byte(s.serverFirst), 0, ""
	}

	i := strings.Index(msg, ",p=")
	if i < 0 {
		return nil, 58, "missing proof"
	}
	withoutProof := msg[:i]
	proof, err := base64.StdEncoding.DecodeString(msg[i+3:])
	if err != nil {
		return nil, 58, "malformed proof"
	}
	authMessage := s.clientFirst + "," + s.serverFirst + "," + withoutProof
	salted, err := pbkdf2.Key(sha256.New, s.pass, s.salt, s.iter, sha256.Size)
	if err != nil {
		return nil, 58, "key derivation failed"
	}
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSig := hmacSHA256(storedKey[:], []byte(authMessage))
	recoveredKey := xorBytes(proof, clientSig)
	recoveredStored := sha256.Sum256(recoveredKey)
	if !bytes.Equal(recoveredStored[:], storedKey[:]) {
		return nil, 58, "authentication failed"
	}
	serverKey := hmacSHA256(salted, []byte("Server Key"))
	serverSig := hmacSHA256(serverKey, []byte(authMessage))
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSig)), 0, ""
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func xorBytes(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// stripGS2Header returns the client-first-message-bare by dropping the
// gs2-cbind-flag and optional authzid (everything up to and including the
// second comma).
func stripGS2Header(s string) string {
	i1 := strings.IndexByte(s, ',')
	if i1 < 0 {
		return s
	}
	rest := s[i1+1:]
	i2 := strings.IndexByte(rest, ',')
	if i2 < 0 {
		return rest
	}
	return rest[i2+1:]
}

// scramAttr returns the value of a "key=value" attribute in a comma-separated
// SCRAM message, or "" if absent.
func scramAttr(s, key string) string {
	for _, part := range strings.Split(s, ",") {
		if strings.HasPrefix(part, key+"=") {
			return part[len(key)+1:]
		}
	}
	return ""
}
