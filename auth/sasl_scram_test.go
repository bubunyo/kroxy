package auth_test

import (
	"testing"

	"github.com/bubunyo/kroxy/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsSCRAMMechanism(t *testing.T) {
	t.Parallel()

	assert.True(t, auth.IsSCRAMMechanism(auth.MechanismSCRAMSHA256))
	assert.True(t, auth.IsSCRAMMechanism(auth.MechanismSCRAMSHA512))
	assert.False(t, auth.IsSCRAMMechanism(auth.MechanismPlain))
	assert.False(t, auth.IsSCRAMMechanism(""))
	assert.False(t, auth.IsSCRAMMechanism("scram-sha-256"))
}

func TestParseSCRAMClientFirstUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{
			name: "no channel binding, no authzid",
			in:   "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL",
			want: "alice",
		},
		{
			name: "no channel binding, with authzid (ignored)",
			in:   "n,a=admin,n=alice,r=abc",
			want: "alice",
		},
		{
			name: "escaped comma in name",
			in:   "n,,n=al=2Cice,r=abc",
			want: "al,ice",
		},
		{
			name: "escaped equals in name",
			in:   "n,,n=al=3Dice,r=abc",
			want: "al=ice",
		},
		{
			name: "with extensions after nonce",
			in:   "n,,n=tenantA,r=abc,m=foo",
			want: "tenantA",
		},
		{
			name: "leading reserved-mext skipped",
			in:   "n,,m=ignored,n=tenantA,r=abc",
			want: "tenantA",
		},
		{
			name:    "channel binding y rejected",
			in:      "y,,n=alice,r=abc",
			wantErr: true,
		},
		{
			name:    "channel binding p= rejected",
			in:      "p=tls-unique,,n=alice,r=abc",
			wantErr: true,
		},
		{
			name:    "missing gs2 cbind separator",
			in:      "n",
			wantErr: true,
		},
		{
			name:    "missing authzid separator",
			in:      "n,",
			wantErr: true,
		},
		{
			name:    "missing n= attribute",
			in:      "n,,r=abc,n=alice",
			wantErr: true,
		},
		{
			name:    "missing nonce",
			in:      "n,,n=alice",
			wantErr: true,
		},
		{
			name:    "empty username",
			in:      "n,,n=,r=abc",
			wantErr: true,
		},
		{
			name:    "invalid escape",
			in:      "n,,n=al=FFice,r=abc",
			wantErr: true,
		},
		{
			name:    "truncated escape",
			in:      "n,,n=alice=2,r=abc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := auth.ParseSCRAMClientFirstUsername([]byte(tt.in))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
