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
	stateRelaySaslInFlight
	stateAuthenticated
)

type conn struct {
	ctx      context.Context
	nc       net.Conn
	resolver resolver.Resolver
	cfg      ServerConfig
	metrics  *observability.Metrics
	log      *slog.Logger

	state                connState
	clientMechanism      string // SASL mechanism the client used to authenticate to kroxy
	tenant               resolver.Tenant
	password             string // populated only on the PLAIN path
	upstream             *upstream.Conn
	scramRoundsCompleted int
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

// shouldTranslatePlainToSCRAM reports whether this connection's upstream dial
// must bridge a PLAIN client to a SCRAM broker. It is true only when the
// client authenticated to kroxy with PLAIN (so kroxy holds the plaintext
// password needed to compute a SCRAM proof) and kroxy is configured with an
// upstream SCRAM mechanism. Clients that already speak SCRAM authenticate
// during handshake via the relay path and never reach ensureUpstream.
func (c *conn) shouldTranslatePlainToSCRAM() bool {
	return c.clientMechanism == auth.MechanismPlain && auth.IsSCRAMMechanism(c.cfg.UpstreamSASLMechanism)
}

func (c *conn) ensureUpstream() error {
	if c.upstream != nil {
		return nil
	}
	var (
		up  *upstream.Conn
		err error
	)
	if c.shouldTranslatePlainToSCRAM() {
		up, err = upstream.DialSCRAM(c.ctx, c.tenant.Upstream, c.cfg.UpstreamSASLMechanism, c.tenant.ID, c.password)
	} else {
		up, err = upstream.Dial(c.ctx, c.tenant.Upstream, c.tenant.ID, c.password)
	}
	if err != nil {
		if c.metrics != nil {
			c.metrics.UpstreamErrorTotal.WithLabelValues("dial").Inc()
		}
		return errors.Wrap(err, "ensureUpstream")
	}
	c.upstream = up
	upstreamMech := c.clientMechanism
	if c.shouldTranslatePlainToSCRAM() {
		upstreamMech = c.cfg.UpstreamSASLMechanism
	}
	c.log.InfoContext(c.ctx, "upstream connected", "addr", c.tenant.Upstream, "tenant_id", c.tenant.ID, "upstream_mechanism", upstreamMech)
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
	resp.SupportedMechanisms = []string{
		auth.MechanismPlain,
		auth.MechanismSCRAMSHA256,
		auth.MechanismSCRAMSHA512,
	}
	switch {
	case c.state != stateAwaitHandshake:
		resp.ErrorCode = errIllegalSaslState
		c.observeHandshake(req.Mechanism, "illegal_state")
	case req.Mechanism != auth.MechanismPlain && !auth.IsSCRAMMechanism(req.Mechanism):
		resp.ErrorCode = errUnsupportedSaslMech
		c.observeHandshake(req.Mechanism, "unsupported")
	default:
		c.clientMechanism = req.Mechanism
		c.state = stateAwaitAuth
	}
	return c.writeResponse(hdr, resp)
}

// observeHandshake records the result of a SASL handshake or authenticate
// step against the SaslHandshakeTotal counter, if metrics are enabled.
//
// The mechanism label is bounded to a fixed allow-list (PLAIN,
// SCRAM-SHA-256, SCRAM-SHA-512); any other value — including the empty
// string and arbitrary client-supplied strings from a SaslHandshake
// request — is normalized to "unknown" so a hostile or buggy client
// cannot create unbounded Prometheus label cardinality.
func (c *conn) observeHandshake(mech, result string) {
	if c.metrics == nil {
		return
	}
	switch mech {
	case auth.MechanismPlain, auth.MechanismSCRAMSHA256, auth.MechanismSCRAMSHA512:
		// allowed
	default:
		mech = "unknown"
	}
	c.metrics.SaslHandshakeTotal.WithLabelValues(mech, result).Inc()
}

func (c *conn) handleSaslAuthenticate(hdr protocol.RequestHeader, body []byte) error {
	if auth.IsSCRAMMechanism(c.clientMechanism) {
		return c.handleSaslAuthenticateSCRAM(hdr, body)
	}
	return c.handleSaslAuthenticatePlain(hdr, body)
}

func (c *conn) handleSaslAuthenticatePlain(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleSaslAuthenticatePlain")
	}
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(hdr.APIVersion)

	if c.state != stateAwaitAuth {
		resp.ErrorCode = errIllegalSaslState
		msg := "SASL handshake required"
		resp.ErrorMessage = &msg
		c.observeHandshake(auth.MechanismPlain, "illegal_state")
		return c.writeResponse(hdr, resp)
	}

	creds, err := auth.ParsePlain(req.SASLAuthBytes)
	if err != nil {
		resp.ErrorCode = errSaslAuthFailed
		msg := "malformed PLAIN payload"
		resp.ErrorMessage = &msg
		c.observeHandshake(auth.MechanismPlain, "malformed")
		return c.writeResponse(hdr, resp)
	}

	// kroxy is a SASL/PLAIN pass-through. The username on the wire is
	// treated as the tenant ID; the password is forwarded verbatim to the
	// tenant's upstream Kafka cluster on the first dial. The upstream is
	// the auth authority.
	tenant, err := c.resolver.Get(c.ctx, creds.Username)
	if err != nil {
		if c.metrics != nil {
			c.metrics.ResolverCallsTotal.WithLabelValues("unauthorized").Inc()
		}
		resp.ErrorCode = errSaslAuthFailed
		msg := "authentication failed"
		resp.ErrorMessage = &msg
		c.observeHandshake(auth.MechanismPlain, "unauthorized")
		c.log.InfoContext(c.ctx, "sasl auth failed", "tenant_id", creds.Username, "err", err)
		return c.writeResponse(hdr, resp)
	}

	if c.metrics != nil {
		c.metrics.ResolverCallsTotal.WithLabelValues("ok").Inc()
	}
	c.tenant = tenant
	c.password = creds.Password
	c.state = stateAuthenticated
	c.observeHandshake(auth.MechanismPlain, "ok")
	c.log.InfoContext(c.ctx, "sasl auth ok", "tenant_id", tenant.ID, "mechanism", auth.MechanismPlain)
	return c.writeResponse(hdr, resp)
}

// handleSaslAuthenticateSCRAM relays SCRAM SaslAuthenticate frames between
// the client and the upstream broker. On the first message kroxy parses the
// SASLname (== tenant ID) from the SCRAM client-first-message in order to
// resolve the tenant and dial the correct upstream; subsequent messages are
// forwarded verbatim. kroxy never inspects nonces, salts, or proofs — the
// upstream broker is the sole authentication authority.
func (c *conn) handleSaslAuthenticateSCRAM(hdr protocol.RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return errors.Wrap(err, "handleSaslAuthenticateSCRAM")
	}
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(hdr.APIVersion)

	if c.state != stateAwaitAuth && c.state != stateRelaySaslInFlight {
		resp.ErrorCode = errIllegalSaslState
		msg := "SASL handshake required"
		resp.ErrorMessage = &msg
		c.observeHandshake(c.clientMechanism, "illegal_state")
		return c.writeResponse(hdr, resp)
	}

	// First SCRAM message: extract username, resolve tenant, dial upstream.
	if c.state == stateAwaitAuth {
		username, err := auth.ParseSCRAMClientFirstUsername(req.SASLAuthBytes)
		if err != nil {
			resp.ErrorCode = errSaslAuthFailed
			msg := "malformed SCRAM client-first-message"
			resp.ErrorMessage = &msg
			c.observeHandshake(c.clientMechanism, "malformed")
			c.log.InfoContext(c.ctx, "sasl scram parse failed", "err", err)
			return c.writeResponse(hdr, resp)
		}
		tenant, err := c.resolver.Get(c.ctx, username)
		if err != nil {
			if c.metrics != nil {
				c.metrics.ResolverCallsTotal.WithLabelValues("unauthorized").Inc()
			}
			resp.ErrorCode = errSaslAuthFailed
			msg := "authentication failed"
			resp.ErrorMessage = &msg
			c.observeHandshake(c.clientMechanism, "unauthorized")
			c.log.InfoContext(c.ctx, "sasl auth failed", "tenant_id", username, "err", err)
			return c.writeResponse(hdr, resp)
		}
		if c.metrics != nil {
			c.metrics.ResolverCallsTotal.WithLabelValues("ok").Inc()
		}
		c.tenant = tenant

		up, dErr := upstream.DialForSCRAM(c.ctx, tenant.Upstream, c.clientMechanism)
		if dErr != nil {
			if c.metrics != nil {
				c.metrics.UpstreamErrorTotal.WithLabelValues("scram_dial").Inc()
			}
			resp.ErrorCode = errSaslAuthFailed
			msg := "upstream unavailable"
			resp.ErrorMessage = &msg
			c.observeHandshake(c.clientMechanism, "upstream_error")
			c.log.WarnContext(c.ctx, "scram upstream dial failed", "tenant_id", tenant.ID, "err", dErr)
			return c.writeResponse(hdr, resp)
		}
		c.upstream = up
		c.state = stateRelaySaslInFlight
	}

	respBytes, errCode, errMsg, rErr := c.upstream.RelaySASLAuthenticate(req.SASLAuthBytes)
	if rErr != nil {
		if c.metrics != nil {
			c.metrics.UpstreamErrorTotal.WithLabelValues("scram_relay").Inc()
		}
		c.observeHandshake(c.clientMechanism, "upstream_error")
		return errors.Wrap(rErr, "handleSaslAuthenticateSCRAM")
	}
	resp.SASLAuthBytes = respBytes
	resp.ErrorCode = errCode
	if errMsg != "" {
		m := errMsg
		resp.ErrorMessage = &m
	}
	if errCode != 0 {
		c.observeHandshake(c.clientMechanism, "unauthorized")
		c.log.InfoContext(c.ctx, "scram upstream rejected", "tenant_id", c.tenant.ID, "code", errCode, "msg", errMsg)
		return c.writeResponse(hdr, resp)
	}

	c.scramRoundsCompleted++
	if c.scramRoundsCompleted >= 2 {
		c.state = stateAuthenticated
		c.observeHandshake(c.clientMechanism, "ok")
		c.log.InfoContext(c.ctx, "sasl auth ok", "tenant_id", c.tenant.ID, "mechanism", c.clientMechanism)
	}
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
