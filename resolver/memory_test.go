package resolver_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleUser(name string) resolver.MemoryUser {
	return resolver.MemoryUser{
		Username:    name,
		TenantID:    name,
		TopicPrefix: name + ".",
		Upstream:    "kafka:9092",
	}
}

func TestMemory_Get(t *testing.T) {
	t.Parallel()

	m, err := resolver.NewMemory([]resolver.MemoryUser{sampleUser("alice")})
	require.NoError(t, err)

	tests := []struct {
		name     string
		username string
		wantErr  error
		want     resolver.Tenant
	}{
		{
			name:     "known user",
			username: "alice",
			want: resolver.Tenant{
				ID:          "alice",
				TopicPrefix: "alice.",
				Upstream:    "kafka:9092",
			},
		},
		{
			name:     "unknown user",
			username: "ghost",
			wantErr:  resolver.ErrUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := m.Get(context.Background(), tt.username)
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
		sampleUser("a"),
		sampleUser("a"),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrDuplicate))
}

func TestNewMemory_InvalidUser(t *testing.T) {
	t.Parallel()
	_, err := resolver.NewMemory([]resolver.MemoryUser{{Username: "a"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidUser))
}

func TestMemory_Set(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemory(nil)
	require.NoError(t, err)

	require.NoError(t, m.Set(context.Background(), sampleUser("alice")))

	err = m.Set(context.Background(), sampleUser("alice"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrDuplicate))

	err = m.Set(context.Background(), resolver.MemoryUser{Username: "x"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidUser))

	tenant, err := m.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", tenant.ID)
}

func TestMemory_Delete(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemory([]resolver.MemoryUser{sampleUser("alice")})
	require.NoError(t, err)

	require.NoError(t, m.Delete(context.Background(), "alice"))

	_, err = m.Get(context.Background(), "alice")
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrUnauthorized))

	err = m.Delete(context.Background(), "alice")
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrNotFound))

	err = m.Delete(context.Background(), "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidUser))
}

func TestMemory_List_Detached(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemory([]resolver.MemoryUser{
		sampleUser("alice"),
		sampleUser("bob"),
	})
	require.NoError(t, err)

	got, err := m.List(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)

	for _, s := range got {
		assert.NotEmpty(t, s.Username)
		assert.NotEmpty(t, s.TenantID)
	}

	got[0].TenantID = "mutated"
	got2, err := m.List(context.Background())
	require.NoError(t, err)
	for _, s := range got2 {
		assert.NotEqual(t, "mutated", s.TenantID, "List result must be detached from internal storage")
	}
}

func TestMemory_ConcurrentReadDuringWrite(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemory([]resolver.MemoryUser{sampleUser("alice")})
	require.NoError(t, err)

	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = m.Get(ctx, "alice")
					_, _ = m.List(ctx)
				}
			}
		}()
	}

	for i := 0; i < 50; i++ {
		u := sampleUser("user")
		_ = m.Set(ctx, u)
		_ = m.Delete(ctx, "user")
	}
	close(stop)
	wg.Wait()
}
