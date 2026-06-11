// Package proxy implements the TCP listener and the per-connection state
// machine that terminates SASL/PLAIN, resolves the tenant and (in M2+)
// pipes traffic to the upstream cluster.
package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/bubunyo/kroxy/observability"
	"github.com/bubunyo/kroxy/resolver"
	pkgerrors "github.com/pkg/errors"
)

// Server accepts client connections and dispatches them to per-conn handlers.
type Server struct {
	cfg      ServerConfig
	resolver resolver.Resolver
	metrics  *observability.Metrics
	log      *slog.Logger

	listener net.Listener
	wg       sync.WaitGroup
}

// ServerConfig is the subset of static configuration the listener needs.
type ServerConfig struct {
	Listen     string
	Advertised string
	// TLS, when non-nil, terminates TLS on the client-facing listener. A nil
	// value leaves the listener plaintext.
	TLS *tls.Config
}

// NewServer constructs a Server. It does not start listening; call Run.
// metrics may be nil to disable observation.
func NewServer(cfg ServerConfig, r resolver.Resolver, m *observability.Metrics, log *slog.Logger) *Server {
	return &Server{cfg: cfg, resolver: r, metrics: m, log: log}
}

// maxAcceptBackoff caps the retry delay applied after a transient Accept error.
const maxAcceptBackoff = time.Second

// Run begins accepting connections until ctx is cancelled or the listener is
// closed. It blocks the caller.
//
// Transient Accept errors (fd exhaustion, a connection reset between accept and
// return, etc.) are logged and retried with a capped exponential backoff rather
// than treated as fatal — a single misbehaving client must never tear down the
// proxy. When TLS is enabled the handshake is deferred to the first read, so a
// failed handshake surfaces per-connection in handle (logged, non-fatal), not
// here.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Listen)
	if err != nil {
		return pkgerrors.Wrap(err, "Run")
	}
	if s.cfg.TLS != nil {
		ln = tls.NewListener(ln, s.cfg.TLS)
	}
	s.listener = ln
	s.log.InfoContext(ctx, "kroxy listening", "addr", ln.Addr().String(), "advertised", s.cfg.Advertised, "tls", s.cfg.TLS != nil)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var backoff time.Duration
	for {
		c, err := ln.Accept()
		if err != nil {
			// Clean shutdown: ctx cancelled or the listener was closed.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return nil
			}
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
			}
			if backoff > maxAcceptBackoff {
				backoff = maxAcceptBackoff
			}
			s.log.WarnContext(ctx, "accept error; retrying", "err", err, "delay", backoff.String())
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				s.wg.Wait()
				return nil
			case <-t.C:
			}
			continue
		}
		backoff = 0
		s.wg.Go(func() { s.handle(ctx, c) })
	}
}

func (s *Server) handle(ctx context.Context, nc net.Conn) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	log := s.log.With("client", nc.RemoteAddr().String())
	c := newConn(connCtx, nc, s.resolver, s.cfg, s.metrics, log)
	defer c.close()

	if s.metrics != nil {
		s.metrics.ConnectionsTotal.Inc()
		s.metrics.ConnectionsActive.Inc()
		defer s.metrics.ConnectionsActive.Dec()
	}

	if err := c.serve(); err != nil && !errors.Is(err, errClientClosed) {
		log.WarnContext(connCtx, "connection terminated", "err", err)
	}
}

// drainTimeout is how long the listener waits for in-flight conns to finish
// after ctx cancellation before returning.
const drainTimeout = 30 * time.Second

// Wait blocks until all in-flight connections have returned or the drain
// timeout elapses. Useful in main() after Run exits.
func (s *Server) Wait() {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(drainTimeout):
	}
}
