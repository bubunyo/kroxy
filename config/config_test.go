package config_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(contents), 0o600))
	return p
}

// writeKeyPair writes a self-signed cert/key PEM pair into a temp dir and
// returns their paths.
func writeKeyPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kroxy-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "server.crt")
	keyPath = filepath.Join(dir, "server.key")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}

func TestTLSConfig_Build(t *testing.T) {
	t.Parallel()

	t.Run("disabled returns nil", func(t *testing.T) {
		t.Parallel()
		got, err := config.TLSConfig{Enabled: false}.Build()
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("enabled loads keypair", func(t *testing.T) {
		t.Parallel()
		cert, key := writeKeyPair(t)
		got, err := config.TLSConfig{Enabled: true, CertFile: cert, KeyFile: key}.Build()
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Len(t, got.Certificates, 1)
		assert.Equal(t, uint16(tls.VersionTLS12), got.MinVersion)
	})

	t.Run("enabled with missing file errors", func(t *testing.T) {
		t.Parallel()
		_, err := config.TLSConfig{Enabled: true, CertFile: "/nope.crt", KeyFile: "/nope.key"}.Build()
		require.Error(t, err)
	})
}

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		yaml    string
		wantErr bool
		check   func(t *testing.T, c config.Config)
	}{
		{
			name: "minimal valid",
			yaml: `
advertised: "kroxy:9092"
upstream:
  bootstrap: "kafka:9092"
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
`,
			check: func(t *testing.T, c config.Config) {
				assert.Equal(t, ":9092", c.Listen)
				assert.Equal(t, "info", c.Log.Level)
				assert.Equal(t, "json", c.Log.Format)
				assert.Equal(t, "127.0.0.1:9095", c.Admin.Listen)
				assert.False(t, c.Admin.Enabled)
				assert.Equal(t, "memory", c.Resolver.Type)
				tenants := c.Resolver.Memory.Tenants
				require.Len(t, tenants, 1)
				assert.Equal(t, "tenantA", tenants[0].ID)
				assert.Equal(t, "kafka:9092", tenants[0].Upstream, "should fall back to upstream.bootstrap")
			},
		},
		{
			name: "explicit resolver type",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  type: memory
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
        upstream: "other:9092"
`,
			check: func(t *testing.T, c config.Config) {
				assert.Equal(t, "memory", c.Resolver.Type)
				assert.Equal(t, "other:9092", c.Resolver.Memory.Tenants[0].Upstream)
			},
		},
		{
			name: "unknown resolver type",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  type: postgres
`,
			wantErr: true,
		},
		{
			name: "admin enabled with no tenants",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
admin:
  enabled: true
`,
			check: func(t *testing.T, c config.Config) {
				assert.True(t, c.Admin.Enabled)
				assert.Equal(t, "127.0.0.1:9095", c.Admin.Listen)
				assert.Empty(t, c.Resolver.Memory.Tenants)
				assert.Equal(t, "memory", c.Resolver.Type)
			},
		},
		{
			name: "admin enabled with custom listen",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
admin:
  enabled: true
  listen: "0.0.0.0:9099"
`,
			check: func(t *testing.T, c config.Config) {
				assert.Equal(t, "0.0.0.0:9099", c.Admin.Listen)
			},
		},
		{
			name: "admin enabled with invalid listen",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
admin:
  enabled: true
  listen: "not-a-host-port"
`,
			wantErr: true,
		},
		{
			name:    "missing advertised",
			yaml:    `upstream: { bootstrap: "k:9092" }`,
			wantErr: true,
		},
		{
			name: "missing tenants",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
`,
			wantErr: true,
		},
		{
			name: "tenant missing id",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  memory:
    tenants:
      - topic_prefix: "tenantA."
`,
			wantErr: true,
		},
		{
			name: "tls enabled without cert/key",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
tls:
  enabled: true
`,
			wantErr: true,
		},
		{
			name: "tls enabled with cert and key",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
tls:
  enabled: true
  cert_file: /etc/kroxy/certs/server.crt
  key_file: /etc/kroxy/certs/server.key
`,
			check: func(t *testing.T, c config.Config) {
				assert.True(t, c.TLS.Enabled)
				assert.Equal(t, "/etc/kroxy/certs/server.crt", c.TLS.CertFile)
				assert.Equal(t, "/etc/kroxy/certs/server.key", c.TLS.KeyFile)
			},
		},
		{
			name: "upstream sasl scram-256 valid",
			yaml: `
advertised: "kroxy:9092"
upstream:
  bootstrap: "k:9092"
  sasl:
    mechanism: SCRAM-SHA-256
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
`,
			check: func(t *testing.T, c config.Config) {
				assert.Equal(t, "SCRAM-SHA-256", c.Upstream.SASL.Mechanism)
			},
		},
		{
			name: "upstream sasl empty is plain passthrough",
			yaml: `
advertised: "kroxy:9092"
upstream: { bootstrap: "k:9092" }
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
`,
			check: func(t *testing.T, c config.Config) {
				assert.Empty(t, c.Upstream.SASL.Mechanism)
			},
		},
		{
			name: "upstream sasl plain rejected",
			yaml: `
advertised: "kroxy:9092"
upstream:
  bootstrap: "k:9092"
  sasl:
    mechanism: PLAIN
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
`,
			wantErr: true,
		},
		{
			name: "upstream sasl unknown mechanism rejected",
			yaml: `
advertised: "kroxy:9092"
upstream:
  bootstrap: "k:9092"
  sasl:
    mechanism: GSSAPI
resolver:
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := writeFile(t, tt.yaml)
			c, err := config.Load(p)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}
