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

func sampleTenant(id string) resolver.Tenant {
	return resolver.Tenant{
		ID:          id,
		TopicPrefix: id + ".",
		Upstream:    "kafka:9092",
	}
}

func TestMemoryResolver_Get(t *testing.T) {
	t.Parallel()

	m, err := resolver.NewMemoryResolver([]resolver.Tenant{sampleTenant("alice")})
	require.NoError(t, err)

	tests := []struct {
		name    string
		id      string
		wantErr error
		want    resolver.Tenant
	}{
		{
			name: "known tenant",
			id:   "alice",
			want: resolver.Tenant{
				ID:          "alice",
				TopicPrefix: "alice.",
				Upstream:    "kafka:9092",
			},
		},
		{
			name:    "unknown tenant",
			id:      "ghost",
			wantErr: resolver.ErrUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := m.Get(context.Background(), tt.id)
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

func TestNewMemoryResolver_Duplicate(t *testing.T) {
	t.Parallel()
	_, err := resolver.NewMemoryResolver([]resolver.Tenant{
		sampleTenant("a"),
		sampleTenant("a"),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrDuplicate))
}

func TestNewMemoryResolver_Invalid(t *testing.T) {
	t.Parallel()
	_, err := resolver.NewMemoryResolver([]resolver.Tenant{{ID: "a"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidTenant))
}

func TestMemoryResolver_Set(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemoryResolver(nil)
	require.NoError(t, err)

	require.NoError(t, m.Set(context.Background(), sampleTenant("alice")))

	err = m.Set(context.Background(), sampleTenant("alice"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrDuplicate))

	err = m.Set(context.Background(), resolver.Tenant{ID: "x"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidTenant))

	tenant, err := m.Get(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", tenant.ID)
}

func TestMemoryResolver_Delete(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemoryResolver([]resolver.Tenant{sampleTenant("alice")})
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
	assert.True(t, errors.Is(err, resolver.ErrInvalidTenant))
}

func TestMemoryResolver_List_Detached(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemoryResolver([]resolver.Tenant{
		sampleTenant("alice"),
		sampleTenant("bob"),
	})
	require.NoError(t, err)

	got, err := m.List(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)

	for _, tn := range got {
		assert.NotEmpty(t, tn.ID)
		assert.NotEmpty(t, tn.TopicPrefix)
	}

	got[0].TopicPrefix = "mutated."
	got2, err := m.List(context.Background())
	require.NoError(t, err)
	for _, tn := range got2 {
		assert.NotEqual(t, "mutated.", tn.TopicPrefix, "List result must be detached from internal storage")
	}
}

func TestMemoryResolver_ConcurrentReadDuringWrite(t *testing.T) {
	t.Parallel()
	m, err := resolver.NewMemoryResolver([]resolver.Tenant{sampleTenant("alice")})
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
		_ = m.Set(ctx, sampleTenant("transient"))
		_ = m.Delete(ctx, "transient")
	}
	close(stop)
	wg.Wait()
}
