package auth_test

import (
	"testing"

	"github.com/bubunyo/kroxy/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePlain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      []byte
		want    auth.PlainCredentials
		wantErr bool
	}{
		{
			name: "empty authzid",
			in:   []byte("\x00alice\x00alicepw"),
			want: auth.PlainCredentials{Username: "alice", Password: "alicepw"},
		},
		{
			name: "with authzid",
			in:   []byte("authz\x00alice\x00alicepw"),
			want: auth.PlainCredentials{Authzid: "authz", Username: "alice", Password: "alicepw"},
		},
		{
			name:    "missing separator",
			in:      []byte("alice"),
			wantErr: true,
		},
		{
			name:    "single separator only",
			in:      []byte("alice\x00pw"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := auth.ParsePlain(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
