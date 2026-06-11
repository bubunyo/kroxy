package proxy

import (
	"testing"

	"github.com/bubunyo/kroxy/auth"
	"github.com/stretchr/testify/assert"
)

func TestShouldTranslatePlainToSCRAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clientMech    string
		upstreamMech  string
		wantTranslate bool
	}{
		{name: "plain client, scram-256 upstream", clientMech: auth.MechanismPlain, upstreamMech: auth.MechanismSCRAMSHA256, wantTranslate: true},
		{name: "plain client, scram-512 upstream", clientMech: auth.MechanismPlain, upstreamMech: auth.MechanismSCRAMSHA512, wantTranslate: true},
		{name: "plain client, no upstream mech (passthrough)", clientMech: auth.MechanismPlain, upstreamMech: "", wantTranslate: false},
		{name: "scram client never translates", clientMech: auth.MechanismSCRAMSHA256, upstreamMech: auth.MechanismSCRAMSHA256, wantTranslate: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &conn{
				clientMechanism: tt.clientMech,
				cfg:             ServerConfig{UpstreamSASLMechanism: tt.upstreamMech},
			}
			assert.Equal(t, tt.wantTranslate, c.shouldTranslatePlainToSCRAM())
		})
	}
}
