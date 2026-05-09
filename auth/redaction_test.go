package auth_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/bubunyo/kroxy/auth"
	"github.com/stretchr/testify/assert"
)

func TestPlainCredentials_Redaction(t *testing.T) {
	t.Parallel()

	const secret = "topsecretpw"
	c := auth.PlainCredentials{Authzid: "", Username: "alice", Password: secret}

	for _, format := range []string{"%v", "%+v", "%s", "%#v"} {
		s := fmt.Sprintf(format, c)
		assert.NotContains(t, s, secret, "format %s leaked password: %s", format, s)
		assert.Contains(t, s, "[REDACTED]")
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	log.Info("creds", "c", c)
	assert.NotContains(t, buf.String(), secret)
	assert.Contains(t, buf.String(), "[REDACTED]")
}
