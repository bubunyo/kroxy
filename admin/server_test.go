package admin_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rpc "github.com/bubunyo/go-rpc"

	"github.com/bubunyo/kroxy/admin"
	"github.com/bubunyo/kroxy/resolver"
)

func newTestServer(t *testing.T) (*admin.Client, *resolver.Memory, func()) {
	t.Helper()
	store, err := resolver.NewMemory(nil)
	require.NoError(t, err)

	svc := admin.NewService(store, nil)
	server := rpc.NewServer(rpc.Opts{
		MaxBytesRead:     rpc.MaxBytesRead,
		ExecutionTimeout: 5 * 1e9,
	})
	server.Register(svc)

	ts := httptest.NewServer(server)
	client := admin.NewClient(ts.URL)
	return client, store, ts.Close
}

func sampleSet(name string) admin.SetParams {
	return admin.SetParams{
		Username:    name,
		TenantID:    name,
		TopicPrefix: name + ".",
		Upstream:    "kafka:9092",
	}
}

func TestService_Set(t *testing.T) {
	t.Parallel()
	client, store, cleanup := newTestServer(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.Set(ctx, sampleSet("alice")))

	tenant, err := store.Get(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", tenant.ID)

	err = client.Set(ctx, sampleSet("alice"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrDuplicate))

	err = client.Set(ctx, admin.SetParams{Username: "bob"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidUser))
}

func TestService_Delete(t *testing.T) {
	t.Parallel()
	client, _, cleanup := newTestServer(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.Set(ctx, sampleSet("alice")))
	require.NoError(t, client.Delete(ctx, "alice"))

	err := client.Delete(ctx, "alice")
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrNotFound))

	err = client.Delete(ctx, "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, resolver.ErrInvalidUser))
}

func TestService_List(t *testing.T) {
	t.Parallel()
	client, _, cleanup := newTestServer(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.Set(ctx, sampleSet("alice")))
	require.NoError(t, client.Set(ctx, sampleSet("bob")))

	got, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, got.Tenants, 2)

	for _, tv := range got.Tenants {
		assert.NotEmpty(t, tv.Username)
		assert.NotEmpty(t, tv.TenantID)
	}
}
