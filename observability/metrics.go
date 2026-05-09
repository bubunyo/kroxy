package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the set of Prometheus instruments the proxy exposes.
type Metrics struct {
	registry *prometheus.Registry

	ConnectionsActive  prometheus.Gauge
	ConnectionsTotal   prometheus.Counter
	RequestsTotal      *prometheus.CounterVec
	RequestDuration    *prometheus.HistogramVec
	UpstreamErrorTotal *prometheus.CounterVec
	ResolverCallsTotal *prometheus.CounterVec
}

// NewMetrics builds and registers the proxy's Prometheus metrics on a
// dedicated registry so they don't conflict with other libraries.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		ConnectionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "kroxy", Name: "connections_active",
			Help: "Number of currently open client connections.",
		}),
		ConnectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "kroxy", Name: "connections_total",
			Help: "Total number of client connections accepted since process start.",
		}),
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kroxy", Name: "requests_total",
			Help: "Total Kafka requests handled, labelled by API key and tenant.",
		}, []string{"api_key", "tenant"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kroxy", Name: "request_duration_seconds",
			Help:    "Wall-clock time spent serving a single Kafka request.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}, []string{"api_key", "tenant"}),
		UpstreamErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kroxy", Name: "upstream_errors_total",
			Help: "Total number of upstream connection / round-trip errors.",
		}, []string{"kind"}),
		ResolverCallsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kroxy", Name: "resolver_calls_total",
			Help: "Total number of resolver lookups, labelled by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(
		m.ConnectionsActive,
		m.ConnectionsTotal,
		m.RequestsTotal,
		m.RequestDuration,
		m.UpstreamErrorTotal,
		m.ResolverCallsTotal,
	)
	return m
}

// Handler returns an http.Handler that exposes the metrics in Prometheus
// text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// ObserveRequest records request count + duration for one API call.
func (m *Metrics) ObserveRequest(apiKey int16, tenant string, dur time.Duration) {
	k := strconv.Itoa(int(apiKey))
	m.RequestsTotal.WithLabelValues(k, tenant).Inc()
	m.RequestDuration.WithLabelValues(k, tenant).Observe(dur.Seconds())
}

// ServeMetrics starts an HTTP server exposing /metrics on addr until ctx is
// cancelled. It blocks the caller.
func ServeMetrics(ctx context.Context, addr string, m *Metrics, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.InfoContext(ctx, "metrics server listening", "addr", addr)
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
