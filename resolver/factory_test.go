package resolver_test

import (
	"context"
	"testing"

	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tenants := []resolver.Tenant{sampleTenant("alice")}

	tests := []struct {
		name    string
		cfg     resolver.Config
		wantErr bool
		check   func(t *testing.T, r resolver.Resolver)
	}{
		{
			name: "default type is memory",
			cfg:  resolver.Config{Memory: resolver.MemoryConfig{Tenants: tenants}},
			check: func(t *testing.T, r resolver.Resolver) {
				got, err := r.Get(context.Background(), "alice")
				require.NoError(t, err)
				assert.Equal(t, "alice", got.ID)
			},
		},
		{
			name: "explicit memory",
			cfg: resolver.Config{
				Type:   "memory",
				Memory: resolver.MemoryConfig{Tenants: tenants},
			},
			check: func(t *testing.T, r resolver.Resolver) {
				got, err := r.List(context.Background())
				require.NoError(t, err)
				assert.Len(t, got, 1)
			},
		},
		{
			name: "empty memory list is allowed",
			cfg:  resolver.Config{Type: "memory"},
			check: func(t *testing.T, r resolver.Resolver) {
				got, err := r.List(context.Background())
				require.NoError(t, err)
				assert.Empty(t, got)
			},
		},
		{
			name:    "unknown type rejected",
			cfg:     resolver.Config{Type: "postgres"},
			wantErr: true,
		},
		{
			name: "invalid tenant propagates",
			cfg: resolver.Config{
				Memory: resolver.MemoryConfig{Tenants: []resolver.Tenant{{ID: "a"}}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := resolver.New(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, r)
			if tt.check != nil {
				tt.check(t, r)
			}
		})
	}
}

func TestNew_InvalidTenantWraps(t *testing.T) {
	t.Parallel()
	_, err := resolver.New(resolver.Config{
		Memory: resolver.MemoryConfig{Tenants: []resolver.Tenant{{ID: "a"}}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidTenant))
}
