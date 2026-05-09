// Package auth implements the server-side SASL handshake.
//
// v1 only supports the PLAIN mechanism. The handshake is a state machine
// driven by Kafka SaslHandshake / SaslAuthenticate requests received from a
// client that has already negotiated an ApiVersions exchange.
package auth

import (
	"github.com/pkg/errors"
)

// MechanismPlain is the only mechanism advertised and accepted by the proxy.
const MechanismPlain = "PLAIN"

// PlainCredentials are the (authzid, authcid, password) tuple decoded from a
// SASL/PLAIN authentication payload.
type PlainCredentials struct {
	Authzid  string
	Username string
	Password string
}

// ParsePlain decodes the SASL/PLAIN payload, which is:
//
//	authzid \0 authcid \0 passwd
//
// authzid may be empty.
func ParsePlain(payload []byte) (PlainCredentials, error) {
	first := -1
	second := -1
	for i, b := range payload {
		if b != 0 {
			continue
		}
		if first == -1 {
			first = i
			continue
		}
		second = i
		break
	}
	if first == -1 || second == -1 {
		return PlainCredentials{}, errors.New("ParsePlain: malformed PLAIN payload")
	}
	return PlainCredentials{
		Authzid:  string(payload[:first]),
		Username: string(payload[first+1 : second]),
		Password: string(payload[second+1:]),
	}, nil
}
