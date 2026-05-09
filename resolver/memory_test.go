package resolver_test

import (
	"context"
	"testing"

	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemory_Get(t *testing.T) {
	t.Parallel()

	users := []resolver.MemoryUser{
		{
			Username:     "alice",
			Password:     "alicepw",
			TenantID:     "tenantA",
			TopicPrefix:  "tenantA.",
			Upstream:     "kafka:9092",
			UpstreamSASL: resolver.SASLCreds{Username: "kroxy", Password: "kroxypw"},
		},
	}

	m, err := resolver.NewMemory(users)
	require.NoError(t, err)

	tests := []struct {
		name     string
		username string
		password string
		wantErr  error
		want     resolver.Tenant
	}{
		{
			name:     "valid credentials",
			username: "alice",
			password: "alicepw",
			want: resolver.Tenant{
				ID:           "tenantA",
				TopicPrefix:  "tenantA.",
				Upstream:     "kafka:9092",
				UpstreamSASL: resolver.SASLCreds{Username: "kroxy", Password: "kroxypw"},
			},
		},
		{
			name:     "wrong password",
			username: "alice",
			password: "nope",
			wantErr:  resolver.ErrUnauthorized,
		},
		{
			name:     "unknown user",
			username: "ghost",
			password: "x",
			wantErr:  resolver.ErrUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := m.Get(context.Background(), tt.username, tt.password)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewMemory_DuplicateUser(t *testing.T) {
	t.Parallel()
	_, err := resolver.NewMemory([]resolver.MemoryUser{
		{Username: "a"},
		{Username: "a"},
	})
	require.Error(t, err)
}
