package proxy

import (
	"testing"

	"github.com/bubunyo/kroxy/observability"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// TestObserveHandshake_BoundedMechanismLabel verifies that the mechanism
// label on SaslHandshakeTotal is restricted to the supported allow-list.
// Arbitrary client-supplied strings (e.g., from a malicious or buggy
// client probing SaslHandshake mechanisms) must collapse to "unknown" so
// they cannot create unbounded Prometheus label cardinality. Regression
// test for a Copilot review finding on the SCRAM relay PR.
func TestObserveHandshake_BoundedMechanismLabel(t *testing.T) {
	t.Parallel()

	m := observability.NewMetrics()
	c := &conn{metrics: m}

	c.observeHandshake("PLAIN", "ok")
	c.observeHandshake("SCRAM-SHA-256", "ok")
	c.observeHandshake("SCRAM-SHA-512", "ok")
	c.observeHandshake("FOOBAR-1234", "unsupported")
	c.observeHandshake("FOOBAR-5678", "unsupported")
	c.observeHandshake("", "illegal_state")

	cv := m.SaslHandshakeTotal

	assert.Equal(t, float64(1), testutil.ToFloat64(cv.WithLabelValues("PLAIN", "ok")))
	assert.Equal(t, float64(1), testutil.ToFloat64(cv.WithLabelValues("SCRAM-SHA-256", "ok")))
	assert.Equal(t, float64(1), testutil.ToFloat64(cv.WithLabelValues("SCRAM-SHA-512", "ok")))

	// Two separate hostile mechanism strings + one empty mechanism all
	// collapse onto the single "unknown" series (split by result label).
	assert.Equal(t, float64(2), testutil.ToFloat64(cv.WithLabelValues("unknown", "unsupported")))
	assert.Equal(t, float64(1), testutil.ToFloat64(cv.WithLabelValues("unknown", "illegal_state")))

	// And the raw client-supplied strings must NOT appear as label values.
	assert.Equal(t, float64(0), testutil.ToFloat64(cv.WithLabelValues("FOOBAR-1234", "unsupported")))
	assert.Equal(t, float64(0), testutil.ToFloat64(cv.WithLabelValues("FOOBAR-5678", "unsupported")))
}
