// Package auth implements the server-side SASL handshake.
//
// v1 only supports the PLAIN mechanism. The handshake is a state machine
// driven by Kafka SaslHandshake / SaslAuthenticate requests received from a
// client that has already negotiated an ApiVersions exchange.
package auth

import (
	"fmt"
	"log/slog"

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

// String implements fmt.Stringer with the password redacted.
func (p PlainCredentials) String() string {
	return fmt.Sprintf("PlainCredentials{Authzid:%q Username:%q}",
		p.Authzid, p.Username)
}

// GoString implements fmt.GoStringer so %#v is also safe.
func (p PlainCredentials) GoString() string { return p.String() }

// LogValue implements slog.LogValuer so structured logs never expose the
// password. kroxy forwards the password verbatim to the upstream broker but
// must never log it.
func (p PlainCredentials) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("authzid", p.Authzid),
		slog.String("username", p.Username),
	)
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
