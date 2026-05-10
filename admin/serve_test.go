package admin_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bubunyo/kroxy/admin"
	"github.com/bubunyo/kroxy/resolver"
)

func TestServe_StartShutdown(t *testing.T) {
	t.Parallel()

	store, err := resolver.New(resolver.Config{})
	require.NoError(t, err)
	svc := admin.NewService(store, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- admin.Serve(ctx, addr, svc, nil)
	}()

	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 25*time.Millisecond, "admin server should accept connections")

	resp, err := http.Get("http://" + addr + "/healthz") //nolint:noctx
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "ok", string(body))

	cancel()
	select {
	case err := <-doneCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}
