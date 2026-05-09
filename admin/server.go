package admin

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	rpc "github.com/bubunyo/go-rpc"
	"github.com/pkg/errors"

	"github.com/bubunyo/kroxy/resolver"
)

// JSON-RPC error codes used by this package. The -32000 to -32099 range is
// reserved by the JSON-RPC spec for application-defined server errors. The
// go-rpc library claims -32001 through -32004 for transport-level conditions,
// so we start our domain codes at -32011 to avoid any collision.
const (
	// CodeDuplicate is returned when a write would create a user that already
	// exists.
	CodeDuplicate = -32011
	// CodeNotFound is returned when an update or delete targets a user that
	// does not exist.
	CodeNotFound = -32012
	// CodeInvalid is returned when a payload fails validation.
	CodeInvalid = -32013
)

// Service exposes tenant CRUD methods over JSON-RPC.
type Service struct {
	store resolver.Resolver
	log   *slog.Logger
}

// NewService creates a Service backed by the supplied resolver.
func NewService(store resolver.Resolver, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, log: log}
}

// Registry implements rpc.ServiceRegistrar; it exposes the methods under the
// "Tenants" service name.
func (s *Service) Registry() *rpc.ServiceRegistry {
	return rpc.NewRegistry("Tenants").
		Handle("Set", s.Set).
		Handle("Delete", s.Delete).
		Handle("List", s.List)
}

// Set creates a new tenant.
func (s *Service) Set(ctx context.Context, p *rpc.RequestParams) (any, error) {
	var in SetParams
	if err := p.Bind(&in); err != nil {
		return nil, rpc.NewError(CodeInvalid, err.Error())
	}
	user := resolver.MemoryUser{
		Username:    in.Username,
		TenantID:    in.TenantID,
		TopicPrefix: in.TopicPrefix,
		Upstream:    in.Upstream,
	}
	if err := s.store.Set(ctx, user); err != nil {
		return nil, mapErr(err)
	}
	s.log.InfoContext(ctx, "tenant created", "username", in.Username, "tenant_id", in.TenantID)
	return OKResult{OK: true}, nil
}

// Delete removes a tenant.
func (s *Service) Delete(ctx context.Context, p *rpc.RequestParams) (any, error) {
	var in DeleteParams
	if err := p.Bind(&in); err != nil {
		return nil, rpc.NewError(CodeInvalid, err.Error())
	}
	if err := s.store.Delete(ctx, in.Username); err != nil {
		return nil, mapErr(err)
	}
	s.log.InfoContext(ctx, "tenant deleted", "username", in.Username)
	return OKResult{OK: true}, nil
}

// List returns a snapshot of all tenants.
func (s *Service) List(ctx context.Context, _ *rpc.RequestParams) (any, error) {
	summaries, err := s.store.List(ctx)
	if err != nil {
		return nil, mapErr(err)
	}
	out := ListResult{Tenants: make([]TenantView, 0, len(summaries))}
	for _, t := range summaries {
		out.Tenants = append(out.Tenants, TenantView{
			Username:    t.Username,
			TenantID:    t.TenantID,
			TopicPrefix: t.TopicPrefix,
			Upstream:    t.Upstream,
		})
	}
	return out, nil
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, resolver.ErrDuplicate):
		return rpc.NewError(CodeDuplicate, err.Error())
	case errors.Is(err, resolver.ErrNotFound):
		return rpc.NewError(CodeNotFound, err.Error())
	case errors.Is(err, resolver.ErrInvalidUser):
		return rpc.NewError(CodeInvalid, err.Error())
	default:
		return err
	}
}

// Serve runs an HTTP server that exposes the JSON-RPC service at /rpc and a
// /healthz liveness endpoint. It blocks until ctx is cancelled or the server
// fails, then performs a graceful shutdown.
func Serve(ctx context.Context, addr string, svc *Service, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	server := rpc.NewServer(rpc.Opts{
		MaxBytesRead:     rpc.MaxBytesRead,
		ExecutionTimeout: 15 * time.Second,
	})
	server.Register(svc)

	mux := http.NewServeMux()
	mux.Handle("/rpc", server)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "Serve")
	}

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("admin RPC server listening", "addr", listener.Addr().String())
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- errors.Wrap(err, "Serve")
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return errors.Wrap(err, "Serve")
		}
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}
