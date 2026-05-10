// Package upstream manages the authenticated TCP connection from the proxy
// to the shared Kafka cluster on behalf of a single tenant.
package upstream

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"time"

	"github.com/bubunyo/kroxy/auth"
	"github.com/bubunyo/kroxy/protocol"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// dialTimeout caps the time spent establishing the TCP connection and
// completing the SASL handshake to the upstream broker.
const dialTimeout = 10 * time.Second

// DefaultRequestTimeout bounds the wall-clock time of a single upstream
// request/response cycle. Callers can override per-Conn via SetRequestTimeout.
const DefaultRequestTimeout = 30 * time.Second

// Conn is an authenticated upstream connection used by exactly one client
// connection. It is not safe for concurrent use; the caller is expected to
// drive a single in-flight request at a time, which matches the per-client
// serialised request model the proxy uses.
type Conn struct {
	nc        net.Conn
	corrIDGen atomic.Int32
	reqTO     time.Duration
}

// SetRequestTimeout overrides the per-request deadline. A zero value disables
// the deadline (not recommended outside tests).
func (c *Conn) SetRequestTimeout(d time.Duration) { c.reqTO = d }

func (c *Conn) applyRequestDeadline() error {
	if c.reqTO <= 0 {
		return c.nc.SetDeadline(time.Time{})
	}
	return c.nc.SetDeadline(time.Now().Add(c.reqTO))
}

// Dial opens a TCP connection to addr and performs the SASL/PLAIN
// handshake using the supplied credentials. The credentials are forwarded
// verbatim — kroxy does not validate them; the upstream broker is the auth
// authority.
func Dial(ctx context.Context, addr, username, password string) (*Conn, error) {
	c, err := dialAndNegotiate(ctx, addr, auth.MechanismPlain)
	if err != nil {
		return nil, errors.Wrap(err, "Dial")
	}
	if err := c.plainAuthenticate(username, password); err != nil {
		_ = c.nc.Close()
		return nil, errors.Wrap(err, "Dial")
	}
	if err := c.nc.SetDeadline(time.Time{}); err != nil {
		_ = c.nc.Close()
		return nil, errors.Wrap(err, "Dial")
	}
	return c, nil
}

// DialForSCRAM opens a TCP connection to addr and performs ApiVersions +
// SaslHandshake selecting mechanism, but does NOT complete the
// SaslAuthenticate exchange. Callers drive the SCRAM message rounds via
// RelaySASLAuthenticate, which forwards the client-supplied payloads
// verbatim. mechanism must be one of auth.MechanismSCRAMSHA256 /
// auth.MechanismSCRAMSHA512.
func DialForSCRAM(ctx context.Context, addr, mechanism string) (*Conn, error) {
	if !auth.IsSCRAMMechanism(mechanism) {
		return nil, errors.Errorf("DialForSCRAM: unsupported mechanism %q", mechanism)
	}
	c, err := dialAndNegotiate(ctx, addr, mechanism)
	if err != nil {
		return nil, errors.Wrap(err, "DialForSCRAM")
	}
	// Leave the deadline cleared; per-request deadlines will be applied
	// during RelaySASLAuthenticate via applyRequestDeadline.
	if err := c.nc.SetDeadline(time.Time{}); err != nil {
		_ = c.nc.Close()
		return nil, errors.Wrap(err, "DialForSCRAM")
	}
	return c, nil
}

// dialAndNegotiate opens the TCP connection, runs ApiVersions, and runs
// SaslHandshake selecting the given mechanism. The returned Conn still has
// the dial-deadline applied; the caller must clear it once initial
// authentication is complete.
func dialAndNegotiate(ctx context.Context, addr, mechanism string) (*Conn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dialAndNegotiate")
	}
	c := &Conn{nc: nc, reqTO: DefaultRequestTimeout}
	deadline := time.Now().Add(dialTimeout)
	if err := nc.SetDeadline(deadline); err != nil {
		_ = nc.Close()
		return nil, errors.Wrap(err, "dialAndNegotiate")
	}
	if err := c.apiVersions(); err != nil {
		_ = nc.Close()
		return nil, errors.Wrap(err, "dialAndNegotiate")
	}
	if err := c.saslHandshake(mechanism); err != nil {
		_ = nc.Close()
		return nil, errors.Wrap(err, "dialAndNegotiate")
	}
	return c, nil
}

// Close terminates the upstream connection.
func (c *Conn) Close() error { return c.nc.Close() }

// nextCorrelationID returns a fresh correlation ID for an upstream request.
// Correlation IDs are private to the upstream connection; the proxy
// substitutes them in and out so the client never sees them.
func (c *Conn) nextCorrelationID() int32 { return c.corrIDGen.Add(1) }

// RoundTrip sends a pre-encoded request body (header + body, NO 4-byte
// length prefix) on the upstream connection and returns the matching
// response frame body (response header + body, NO 4-byte length prefix).
//
// The caller passes the original client correlation ID; RoundTrip rewrites
// it to an upstream-private correlation ID for transmission and rewrites the
// response back to clientCorrelationID before returning.
func (c *Conn) RoundTrip(reqFrame []byte, clientCorrelationID int32, headerVersion int8, respHeaderVersion int8) ([]byte, error) {
	if len(reqFrame) < 8 {
		return nil, errors.New("RoundTrip: request frame too short")
	}
	upstreamCID := c.nextCorrelationID()

	// Rewrite correlation_id in place at offset 4..8 of the header. We must
	// not mutate the caller's slice; copy first.
	out := make([]byte, len(reqFrame))
	copy(out, reqFrame)
	binary.BigEndian.PutUint32(out[4:8], uint32(upstreamCID))
	_ = headerVersion // kept for symmetry/future use

	if err := c.applyRequestDeadline(); err != nil {
		return nil, errors.Wrap(err, "RoundTrip")
	}
	if err := protocol.WriteFrame(c.nc, out); err != nil {
		return nil, errors.Wrap(err, "RoundTrip")
	}

	respFrame, err := protocol.ReadFrame(c.nc)
	if err != nil {
		return nil, errors.Wrap(err, "RoundTrip")
	}
	if len(respFrame) < 4 {
		return nil, errors.New("RoundTrip: response frame too short")
	}
	gotCID := int32(binary.BigEndian.Uint32(respFrame[0:4]))
	if gotCID != upstreamCID {
		return nil, errors.Errorf("RoundTrip: correlation id mismatch: want %d got %d", upstreamCID, gotCID)
	}
	binary.BigEndian.PutUint32(respFrame[0:4], uint32(clientCorrelationID))
	_ = respHeaderVersion
	return respFrame, nil
}

// RoundTripRequest sends a typed request to the upstream broker and returns
// the response payload positioned AFTER the response header (i.e. ready to
// be passed to a kmsg.Response.ReadFrom). The caller's clientID is
// propagated so the upstream sees a faithful client identifier.
//
// Correlation IDs are private to the upstream connection; the caller does
// not supply one and never sees it.
func (c *Conn) RoundTripRequest(req kmsg.Request, clientID string) ([]byte, error) {
	apiKey := req.Key()
	apiVersion := req.GetVersion()
	cid := c.nextCorrelationID()
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientID))
	frameWithLen := formatter.AppendRequest(nil, req, cid)
	if err := c.applyRequestDeadline(); err != nil {
		return nil, errors.Wrap(err, "RoundTripRequest")
	}
	if _, err := c.nc.Write(frameWithLen); err != nil {
		return nil, errors.Wrap(err, "RoundTripRequest")
	}
	respFrame, err := protocol.ReadFrame(c.nc)
	if err != nil {
		return nil, errors.Wrap(err, "RoundTripRequest")
	}
	if len(respFrame) < 4 {
		return nil, errors.New("RoundTripRequest: short response")
	}
	gotCID := int32(binary.BigEndian.Uint32(respFrame[0:4]))
	if gotCID != cid {
		return nil, errors.Errorf("RoundTripRequest: correlation id mismatch: want %d got %d", cid, gotCID)
	}
	off := 4
	if protocol.ResponseHeaderVersion(apiKey, apiVersion) >= 1 {
		off++
	}
	return respFrame[off:], nil
}

// apiVersions runs an ApiVersions v0 exchange against the upstream broker.
// v0 is the smallest, broadest-compat variant.
func (c *Conn) apiVersions() error {
	avReq := kmsg.NewPtrApiVersionsRequest()
	avReq.SetVersion(0)
	if _, err := c.directRoundTrip(avReq, protocol.ApiVersionsKey, 0); err != nil {
		return errors.Wrap(err, "apiVersions")
	}
	return nil
}

// saslHandshake runs SaslHandshake v1 selecting the given mechanism.
func (c *Conn) saslHandshake(mechanism string) error {
	hsReq := kmsg.NewPtrSASLHandshakeRequest()
	hsReq.SetVersion(1)
	hsReq.Mechanism = mechanism
	hsRespBody, err := c.directRoundTrip(hsReq, protocol.SaslHandshakeKey, 1)
	if err != nil {
		return errors.Wrap(err, "saslHandshake")
	}
	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(1)
	if err := hsResp.ReadFrom(hsRespBody); err != nil {
		return errors.Wrap(err, "saslHandshake")
	}
	if hsResp.ErrorCode != 0 {
		return errors.Errorf("saslHandshake: upstream error code %d", hsResp.ErrorCode)
	}
	return nil
}

// plainAuthenticate completes a single-shot SASL/PLAIN authenticate against
// the upstream broker using the supplied credentials.
func (c *Conn) plainAuthenticate(username, password string) error {
	authReq := kmsg.NewPtrSASLAuthenticateRequest()
	authReq.SetVersion(1)
	authReq.SASLAuthBytes = []byte("\x00" + username + "\x00" + password)
	authRespBody, err := c.directRoundTrip(authReq, protocol.SaslAuthenticateKey, 1)
	if err != nil {
		return errors.Wrap(err, "plainAuthenticate")
	}
	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(1)
	if err := authResp.ReadFrom(authRespBody); err != nil {
		return errors.Wrap(err, "plainAuthenticate")
	}
	if authResp.ErrorCode != 0 {
		msg := ""
		if authResp.ErrorMessage != nil {
			msg = *authResp.ErrorMessage
		}
		return errors.Errorf("plainAuthenticate: upstream error %d: %s", authResp.ErrorCode, msg)
	}
	return nil
}

// RelaySASLAuthenticate forwards a single SCRAM SaslAuthenticate payload
// (the opaque SASLAuthBytes blob produced by the client) to the upstream
// broker and returns the upstream's response payload, error code, and
// optional error message. err is non-nil only on transport / decoding
// failures; protocol-level errors are surfaced via errCode/errMsg so the
// caller can relay them to the client unchanged.
func (c *Conn) RelaySASLAuthenticate(payload []byte) (respPayload []byte, errCode int16, errMsg string, err error) {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(1)
	req.SASLAuthBytes = payload
	body, err := c.directRoundTrip(req, protocol.SaslAuthenticateKey, 1)
	if err != nil {
		return nil, 0, "", errors.Wrap(err, "RelaySASLAuthenticate")
	}
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(1)
	if err := resp.ReadFrom(body); err != nil {
		return nil, 0, "", errors.Wrap(err, "RelaySASLAuthenticate")
	}
	if resp.ErrorMessage != nil {
		errMsg = *resp.ErrorMessage
	}
	return resp.SASLAuthBytes, resp.ErrorCode, errMsg, nil
}

// directRoundTrip is used only during the handshake when we control both
// sides of the framing and don't need correlation-id rewriting against an
// outer client. It returns the response body slice positioned after the
// response header.
func (c *Conn) directRoundTrip(req kmsg.Request, apiKey, apiVersion int16) ([]byte, error) {
	cid := c.nextCorrelationID()
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("kroxy"))
	frameWithLen := formatter.AppendRequest(nil, req, cid)
	if _, err := c.nc.Write(frameWithLen); err != nil {
		return nil, errors.Wrap(err, "directRoundTrip")
	}
	respFrame, err := protocol.ReadFrame(c.nc)
	if err != nil {
		return nil, errors.Wrap(err, "directRoundTrip")
	}
	if len(respFrame) < 4 {
		return nil, errors.New("directRoundTrip: short response")
	}
	gotCID := int32(binary.BigEndian.Uint32(respFrame[0:4]))
	if gotCID != cid {
		return nil, errors.Errorf("directRoundTrip: correlation id mismatch: want %d got %d", cid, gotCID)
	}
	off := 4
	if protocol.ResponseHeaderVersion(apiKey, apiVersion) >= 1 {
		off++
	}
	return respFrame[off:], nil
}
