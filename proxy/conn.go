package proxy

import (
	"context"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/bubunyo/kroxy/auth"
	"github.com/bubunyo/kroxy/observability"
	"github.com/bubunyo/kroxy/protocol"
	"github.com/bubunyo/kroxy/resolver"
	"github.com/bubunyo/kroxy/upstream"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// errClientClosed is returned when the client closed the connection cleanly.
var errClientClosed = errors.New("client closed connection")

// Kafka error codes used by the proxy itself before any upstream is involved.
const (
	errSaslAuthFailed      int16 = 58
	errUnsupportedSaslMech int16 = 33
	errIllegalSaslState    int16 = 34
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
	metrics  *observability.Metrics
	log      *slog.Logger

	state    connState
	tenant   resolver.Tenant
	username string
	password string
	upstream *upstream.Conn
}

func newConn(ctx context.Context, nc net.Conn, r resolver.Resolver, cfg ServerConfig, m *observability.Metrics, log *slog.Logger) *conn {
	return &conn{ctx: ctx, nc: nc, resolver: r, cfg: cfg, metrics: m, log: log, state: stateAwaitHandshake}
}

func (c *conn) close() {
	_ = c.nc.Close()
	if c.upstream != nil {
		_ = c.upstream.Close()
	}
}

// clientIdleTimeout bounds how long the proxy will wait for the next
// request from an authenticated client before closing the connection.
// SASL handshake gets a tighter window via clientHandshakeTimeout.
const (
	clientIdleTimeout      = 10 * time.Minute
	clientHandshakeTimeout = 30 * time.Second
)

// serve runs the per-connection request/response loop.
func (c *conn) serve() error {
	for {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		to := clientIdleTimeout
		if c.state != stateAuthenticated {
			to = clientHandshakeTimeout
		}
		if err := c.nc.SetReadDeadline(time.Now().Add(to)); err != nil {
			return errors.Wrap(err, "serve")
		}
		frame, err := protocol.ReadFrame(c.nc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errClientClosed
			}
			return errors.Wrap(err, "serve")
		}
		if err := c.nc.SetReadDeadline(time.Time{}); err != nil {
			return errors.Wrap(err, "serve")
		}
		hdr, err := protocol.ParseRequestHeader(frame)
		if err != nil {
			return errors.Wrap(err, "serve")
		}
		body := frame[hdr.HeaderSize:]
		start := time.Now()
		err = c.dispatch(hdr, frame, body)
		if c.metrics != nil {
			tenantLabel := c.tenant.ID
			if tenantLabel == "" {
				tenantLabel = "_unauth"
			}
			c.metrics.ObserveRequest(hdr.APIKey, tenantLabel, time.Since(start))
		}
		if err != nil {
			return errors.Wrap(err, "serve")
		}
	}
}

// dispatch handles a single request. SASL-related keys are terminated at
// the proxy. All other authenticated traffic is forwarded as raw bytes to
// the upstream connection (no body rewriting yet — that lands in M3+).
func (c *conn) dispatch(hdr protocol.RequestHeader, frame, body []byte) error {
	switch hdr.APIKey {
	case protocol.ApiVersionsKey:
		return c.handleApiVersions(hdr, body)
	case protocol.SaslHandshakeKey:
		return c.handleSaslHandshake(hdr, body)
	case protocol.SaslAuthenticateKey:
		return c.handleSaslAuthenticate(hdr, body)
	default:
		if c.state != stateAuthenticated {
			c.log.WarnContext(c.ctx, "request before auth", "api_key", hdr.APIKey)
			return errors.Errorf("client sent api key %d before authentication", hdr.APIKey)
		}
		if c.rewriteHandler(hdr.APIKey) != nil {
			return c.forwardRewritten(hdr, body)
		}
		return c.forwardPassthrough(hdr, frame)
	}
}

// rewriteHandler returns a function that performs the request/response
// rewrite cycle for apiKey, or nil if the proxy currently treats apiKey as
// pure byte-passthrough.
func (c *conn) rewriteHandler(apiKey int16) func(hdr protocol.RequestHeader, body []byte) error {
	switch apiKey {
	case protocol.MetadataKey:
		return c.handleMetadata
	case protocol.ProduceKey:
		return c.handleProduce
	case protocol.FetchKey:
		return c.handleFetch
	case protocol.ListOffsetsKey:
		return c.handleListOffsets
	case protocol.FindCoordinatorKey:
		return c.handleFindCoordinator
	case protocol.OffsetCommitKey:
		return c.handleOffsetCommit
	case protocol.OffsetFetchKey:
		return c.handleOffsetFetch
	case protocol.OffsetDeleteKey:
		return c.handleOffsetDelete
	case protocol.JoinGroupKey:
		return c.handleJoinGroup
	case protocol.SyncGroupKey:
		return c.handleSyncGroup
	case protocol.HeartbeatKey:
		return c.handleHeartbeat
	case protocol.LeaveGroupKey:
		return c.handleLeaveGroup
	case protocol.DescribeGroupsKey:
		return c.handleDescribeGroups
	case protocol.ListGroupsKey:
		return c.handleListGroups
	case protocol.DeleteGroupsKey:
		return c.handleDeleteGroups
	case protocol.InitProducerIDKey:
		return c.handleInitProducerID
	case protocol.AddPartitionsToTxnKey:
		return c.handleAddPartitionsToTxn
	case protocol.AddOffsetsToTxnKey:
		return c.handleAddOffsetsToTxn
	case protocol.EndTxnKey:
		return c.handleEndTxn
	case protocol.TxnOffsetCommitKey:
		return c.handleTxnOffsetCommit
	case protocol.CreateTopicsKey:
		return c.handleCreateTopics
	case protocol.DeleteTopicsKey:
		return c.handleDeleteTopics
	case protocol.DescribeConfigsKey:
		return c.handleDescribeConfigs
	}
	return nil
}

// forwardRewritten dispatches to the per-API rewrite handler.
func (c *conn) forwardRewritten(hdr protocol.RequestHeader, body []byte) error {
	if err := c.ensureUpstream(); err != nil {
		return errors.Wrap(err, "forwardRewritten")
	}
	h := c.rewriteHandler(hdr.APIKey)
	return h(hdr, body)
}

func (c *conn) ensureUpstream() error {
	if c.upstream != nil {
		return nil
	}
	up, err := upstream.Dial(c.ctx, c.tenant.Upstream, c.username, c.password)
	if err != nil {
		if c.metrics != nil {
			c.metrics.UpstreamErrorTotal.WithLabelValues("dial").Inc()
		}
		return errors.Wrap(err, "ensureUpstream")
	}
	c.upstream = up
	c.log.InfoContext(c.ctx, "upstream connected", "addr", c.tenant.Upstream, "tenant_id", c.tenant.ID)
	return nil
}

func (c *conn) handleApiVersions(hdr protocol.RequestHeader, _ []byte) error {
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: protocol.ApiVersionsKey, MinVersion: 0, MaxVersion: 3},
		{ApiKey: protocol.SaslHandshakeKey, MinVersion: 0, MaxVersion: 1},
		{ApiKey: protocol.SaslAuthenticateKey, MinVersion: 0, MaxVersion: 2},
		// Once authenticated, every other key is byte-passthrough; advertise
		// generous ranges so franz-go and other clients pick versions the
		// upstream broker can handle. The upstream broker will reject any
		// unsupported version itself.
		{ApiKey: protocol.ProduceKey, MinVersion: 0, MaxVersion: 9},
		{ApiKey: protocol.FetchKey, MinVersion: 0, MaxVersion: 11},
		{ApiKey: protocol.ListOffsetsKey, MinVersion: 0, MaxVersion: 7},
		{ApiKey: protocol.MetadataKey, MinVersion: 0, MaxVersion: 9},
		{ApiKey: protocol.OffsetCommitKey, MinVersion: 0, MaxVersion: 9},
		{ApiKey: protocol.OffsetFetchKey, MinVersion: 0, MaxVersion: 9},
		{ApiKey: protocol.FindCoordinatorKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.JoinGroupKey, MinVersion: 0, MaxVersion: 9},
		{ApiKey: protocol.HeartbeatKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.LeaveGroupKey, MinVersion: 0, MaxVersion: 5},
		{ApiKey: protocol.SyncGroupKey, MinVersion: 0, MaxVersion: 5},
		{ApiKey: protocol.DescribeGroupsKey, MinVersion: 0, MaxVersion: 5},
		{ApiKey: protocol.ListGroupsKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.CreateTopicsKey, MinVersion: 0, MaxVersion: 6},
		{ApiKey: protocol.DeleteTopicsKey, MinVersion: 0, MaxVersion: 5},
		{ApiKey: protocol.InitProducerIDKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.AddPartitionsToTxnKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.AddOffsetsToTxnKey, MinVersion: 0, MaxVersion: 3},
		{ApiKey: protocol.EndTxnKey, MinVersion: 0, MaxVersion: 3},
		{ApiKey: protocol.TxnOffsetCommitKey, MinVersion: 0, MaxVersion: 3},
		{ApiKey: protocol.DescribeConfigsKey, MinVersion: 0, MaxVersion: 4},
		{ApiKey: protocol.DeleteGroupsKey, MinVersion: 0, MaxVersion: 2},
		{ApiKey: protocol.OffsetDeleteKey, MinVersion: 0, MaxVersion: 0},
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
	switch {
	case c.state != stateAwaitHandshake:
		resp.ErrorCode = errIllegalSaslState
	case req.Mechanism != auth.MechanismPlain:
		resp.ErrorCode = errUnsupportedSaslMech
	default:
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

	// kroxy is a SASL/PLAIN pass-through. The username selects the
	// tenant; the password is forwarded verbatim to the tenant's upstream
	// Kafka cluster on the first dial. The upstream is the auth authority.
	tenant, err := c.resolver.Get(c.ctx, creds.Username)
	if err != nil {
		if c.metrics != nil {
			c.metrics.ResolverCallsTotal.WithLabelValues("unauthorized").Inc()
		}
		resp.ErrorCode = errSaslAuthFailed
		msg := "authentication failed"
		resp.ErrorMessage = &msg
		c.log.InfoContext(c.ctx, "sasl auth failed", "username", creds.Username, "err", err)
		return c.writeResponse(hdr, resp)
	}

	if c.metrics != nil {
		c.metrics.ResolverCallsTotal.WithLabelValues("ok").Inc()
	}
	c.tenant = tenant
	c.username = creds.Username
	c.password = creds.Password
	c.state = stateAuthenticated
	c.log.InfoContext(c.ctx, "sasl auth ok", "username", creds.Username, "tenant_id", tenant.ID)
	return c.writeResponse(hdr, resp)
}

// forwardPassthrough sends frame to the upstream connection unchanged
// (modulo correlation-id substitution) and writes the response back to the
// client. The upstream connection is dialled lazily.
func (c *conn) forwardPassthrough(hdr protocol.RequestHeader, frame []byte) error {
	if err := c.ensureUpstream(); err != nil {
		return errors.Wrap(err, "forwardPassthrough")
	}
	respFrame, err := c.upstream.RoundTrip(
		frame,
		hdr.CorrelationID,
		hdr.HeaderVersion,
		protocol.ResponseHeaderVersion(hdr.APIKey, hdr.APIVersion),
	)
	if err != nil {
		return errors.Wrap(err, "forwardPassthrough")
	}
	return protocol.WriteFrame(c.nc, respFrame)
}

func (c *conn) writeResponse(hdr protocol.RequestHeader, resp kmsg.Response) error {
	body := protocol.EncodeResponse(resp, hdr.CorrelationID, hdr.APIKey, hdr.APIVersion)
	if err := protocol.WriteFrame(c.nc, body); err != nil {
		return errors.Wrap(err, "writeResponse")
	}
	return nil
}
