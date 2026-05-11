package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_Handler_ExposesProxyMetrics(t *testing.T) {
	t.Parallel()

	m := observability.NewMetrics()
	m.ConnectionsTotal.Inc()
	m.ConnectionsActive.Inc()
	m.ObserveRequest(3, "tenantA", 5*time.Millisecond)
	m.UpstreamErrorTotal.WithLabelValues("dial").Inc()
	m.ResolverCallsTotal.WithLabelValues("ok").Inc()
	m.SaslHandshakeTotal.WithLabelValues("PLAIN", "ok").Inc()
	m.SaslHandshakeTotal.WithLabelValues("SCRAM-SHA-256", "unauthorized").Inc()

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(body)

	for _, want := range []string{
		"kroxy_connections_total 1",
		"kroxy_connections_active 1",
		`kroxy_requests_total{api_key="3",tenant="tenantA"} 1`,
		`kroxy_upstream_errors_total{kind="dial"} 1`,
		`kroxy_resolver_calls_total{result="ok"} 1`,
		`kroxy_sasl_handshakes_total{mechanism="PLAIN",result="ok"} 1`,
		`kroxy_sasl_handshakes_total{mechanism="SCRAM-SHA-256",result="unauthorized"} 1`,
	} {
		assert.True(t, strings.Contains(out, want), "metrics output missing %q\n---\n%s", want, out)
	}
}
