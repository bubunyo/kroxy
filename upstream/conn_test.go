package upstream

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRelaySASLAuthenticate_AppliesRequestDeadline verifies the SCRAM
// relay path applies a per-request deadline so a silent upstream cannot
// block the caller indefinitely. Regression test for a Copilot review
// finding on the SCRAM relay PR.
func TestRelaySASLAuthenticate_AppliesRequestDeadline(t *testing.T) {
	t.Parallel()

	// Listener that accepts connections but never reads or writes.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		accepted <- nc
	}()

	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = nc.Close() }()

	// Drain the server-side accepted conn so the goroutine doesn't leak.
	defer func() {
		select {
		case sc := <-accepted:
			_ = sc.Close()
		case <-time.After(time.Second):
		}
	}()

	c := &Conn{nc: nc, reqTO: 100 * time.Millisecond}

	start := time.Now()
	_, _, _, err = c.RelaySASLAuthenticate([]byte("client-first-message-bare"))
	elapsed := time.Since(start)

	require.Error(t, err, "expected deadline error from silent upstream")
	assert.Less(t, elapsed, 2*time.Second, "RelaySASLAuthenticate hung past deadline")
}
