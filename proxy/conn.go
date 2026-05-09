package proxy

import (
	"context"
	"io"
	"log/slog"
	"net"

	"github.com/bubunyo/kroxy/auth"
	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// errClientClosed is returned when the client closed the connection cleanly.
var errClientClosed = errors.New("client closed connection")

// Kafka error codes used by the proxy itself before any upstream is involved.
const (
	errSaslAuthFailed         int16 = 58
	errUnsupportedSaslMech    int16 = 33
	errIllegalSaslState       int16 = 34
	errUnsupportedVersion     int16 = 35
	errUnsupportedKafkaServer int16 = 35
)

// connState tracks the per-connection authentication phase.
type connState int

const (
	stateAwaitHandshake connState = iota
	stateAwaitAuth
	stateAuthenticated
)

type conn struct {
	ctx      context.Context
	nc       net.Conn
	resolver resolver.Resolver
	cfg      ServerConfig
	log      *slog.Logger

	state  connState
	tenant resolver.Tenant
}

func newConn(ctx context.Context, nc net.Conn, r resolver.Resolver, cfg ServerConfig, log *slog.Logger) *conn {
	return &conn{ctx: ctx, nc: nc, resolver: r, cfg: cfg, log: log, state: stateAwaitHandshake}
}

func (c *conn) close() { _ = c.nc.Close() }

// serve runs the per-connection request/response loop.
func (c *conn) serve() error {
	for {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		frame, err := protocol.ReadFrame(c.nc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errClientClosed
			}
			return errors.Wrap(err, "serve")
		}
		hdr, err := protocol.ParseRequestHeader(frame)
		if err != nil {
			return errors.Wrap(err, "serve")
		}
		body := frame[hdr.HeaderSize:]
		if err := c.dispatch(hdr, body); err != nil {
			return errors.Wrap(err, "serve")
		}
	}
}

// dispatch handles a single request. In M1 only ApiVersions, SaslHandshake
// and SaslAuthenticate are answered; any other API key is rejected with an
// error response while the upstream pipe is being built.
func (c *conn) dispatch(hdr protocol.RequestHeader, body []byte) error {
	switch hdr.APIKey {
	case protocol.ApiVersionsKey:
		return c.handleApiVersions(hdr, body)
	case protocol.SaslHandshakeKey:
		return c.handleSaslHandshake(hdr, body)
	case protocol.SaslAuthenticateKey:
		return c.handleSaslAuthenticate(hdr, body)
	default:
		// In M1 we have no upstream yet. Reject with UNSUPPORTED_VERSION
		// so the client surfaces a clear error rather than hanging.
		return c.rejectNotImplemented(hdr)
	}
}

func (c *conn) handleApiVersions(hdr protocol.RequestHeader, _ []byte) error {
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: protocol.ApiVersionsKey, MinVersion: 0, MaxVersion: 3},
		{ApiKey: protocol.SaslHandshakeKey, MinVersion: 0, MaxVersion: 1},
		{ApiKey: protocol.SaslAuthenticateKey, MinVersion: 0, MaxVersion: 2},
	}
	return c.writeResponse(hdr, resp)
}

func (c *conn) handleSaslHandshake(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLHandshakeRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleSaslHandshake")
	}
	resp := kmsg.NewPtrSASLHandshakeResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.SupportedMechanisms = []string{auth.MechanismPlain}
	if c.state != stateAwaitHandshake {
		resp.ErrorCode = errIllegalSaslState
	} else if req.Mechanism != auth.MechanismPlain {
		resp.ErrorCode = errUnsupportedSaslMech
	} else {
		c.state = stateAwaitAuth
	}
	return c.writeResponse(hdr, resp)
}

func (c *conn) handleSaslAuthenticate(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleSaslAuthenticate")
	}
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(hdr.APIVersion)

	if c.state != stateAwaitAuth {
		resp.ErrorCode = errIllegalSaslState
		msg := "SASL handshake required"
		resp.ErrorMessage = &msg
		return c.writeResponse(hdr, resp)
	}

	creds, err := auth.ParsePlain(req.SASLAuthBytes)
	if err != nil {
		resp.ErrorCode = errSaslAuthFailed
		msg := "malformed PLAIN payload"
		resp.ErrorMessage = &msg
		return c.writeResponse(hdr, resp)
	}

	tenant, err := c.resolver.Get(c.ctx, creds.Username, creds.Password)
	if err != nil {
		resp.ErrorCode = errSaslAuthFailed
		msg := "authentication failed"
		resp.ErrorMessage = &msg
		c.log.InfoContext(c.ctx, "sasl auth failed", "username", creds.Username, "err", err)
		return c.writeResponse(hdr, resp)
	}

	c.tenant = tenant
	c.state = stateAuthenticated
	c.log.InfoContext(c.ctx, "sasl auth ok", "username", creds.Username, "tenant_id", tenant.ID)
	return c.writeResponse(hdr, resp)
}

func (c *conn) rejectNotImplemented(hdr protocol.RequestHeader) error {
	// We cannot generically build a typed error response without knowing
	// every API key. The cheapest correct behaviour is to close the
	// connection; clients will see a network error which is honest given
	// M1 has no upstream wired in yet.
	c.log.WarnContext(c.ctx, "api key not implemented in M1, closing",
		"api_key", hdr.APIKey, "api_version", hdr.APIVersion, "authenticated", c.state == stateAuthenticated)
	return errors.Errorf("api key %d not implemented", hdr.APIKey)
}

func (c *conn) writeResponse(hdr protocol.RequestHeader, resp kmsg.Response) error {
	body := protocol.EncodeResponse(resp, hdr.CorrelationID, hdr.APIKey, hdr.APIVersion)
	if err := protocol.WriteFrame(c.nc, body); err != nil {
		return errors.Wrap(err, "writeResponse")
	}
	return nil
}
