package config_test

import (
	"os"
	"path/filepath"
	"testing"

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
