package auth

import (
	"strings"

	"github.com/pkg/errors"
)

// SCRAM mechanisms advertised and accepted by the proxy alongside PLAIN.
// kroxy implements these in pass-through "relay" mode: the SaslAuthenticate
// payloads are forwarded verbatim between client and the upstream broker,
// which is the sole authentication authority. kroxy peeks only at the first
// client message in order to extract the SASLname (== tenant ID) for
// routing.
const (
	MechanismSCRAMSHA256 = "SCRAM-SHA-256"
	MechanismSCRAMSHA512 = "SCRAM-SHA-512"
)

// IsSCRAMMechanism reports whether mech is one of the SCRAM mechanisms
// supported by the proxy.
func IsSCRAMMechanism(mech string) bool {
	return mech == MechanismSCRAMSHA256 || mech == MechanismSCRAMSHA512
}

// ParseSCRAMClientFirstUsername extracts the SASLname (== tenant ID) from a
// SCRAM client-first-message as defined by RFC 5802 §7. The grammar we
// accept is:
//
//	gs2-cbind-flag "," [ authzid ] "," "n=" saslname "," "r=" c-nonce ...
//	gs2-cbind-flag = "n" | "y" | "p=..."
//
// kroxy does NOT support SASL channel binding, so only the "n" flag is
// accepted; any "y" or "p=..." flag is rejected. authzid (if present) is
// ignored. SASLname escapes "=2C" / "=3D" are decoded.
func ParseSCRAMClientFirstUsername(payload []byte) (string, error) {
	s := string(payload)

	// gs2-cbind-flag.
	cb, rest, ok := cutByte(s, ',')
	if !ok {
		return "", errors.New("ParseSCRAMClientFirstUsername: missing gs2 cbind-flag separator")
	}
	switch {
	case cb == "n":
		// no channel binding, ok.
	case cb == "y" || strings.HasPrefix(cb, "p="):
		return "", errors.New("ParseSCRAMClientFirstUsername: channel binding not supported")
	default:
		return "", errors.Errorf("ParseSCRAMClientFirstUsername: invalid gs2 cbind-flag %q", cb)
	}

	// optional authzid then "," then client-first-message-bare.
	_, bare, ok := cutByte(rest, ',')
	if !ok {
		return "", errors.New("ParseSCRAMClientFirstUsername: missing authzid separator")
	}

	// client-first-message-bare = [reserved-mext ","] username "," nonce ["," extensions]
	// Skip any leading m=... reserved-mext attribute.
	if strings.HasPrefix(bare, "m=") {
		_, after, ok := cutByte(bare, ',')
		if !ok {
			return "", errors.New("ParseSCRAMClientFirstUsername: malformed reserved-mext")
		}
		bare = after
	}

	if !strings.HasPrefix(bare, "n=") {
		return "", errors.New("ParseSCRAMClientFirstUsername: missing n= attribute")
	}
	rest = bare[2:]
	rawName, _, ok := cutByte(rest, ',')
	if !ok {
		return "", errors.New("ParseSCRAMClientFirstUsername: missing nonce separator")
	}
	if rawName == "" {
		return "", errors.New("ParseSCRAMClientFirstUsername: empty username")
	}
	name, err := decodeSASLname(rawName)
	if err != nil {
		return "", errors.Wrap(err, "ParseSCRAMClientFirstUsername")
	}
	return name, nil
}

// cutByte splits s at the first occurrence of sep. It is a tiny helper to
// avoid pulling in strings.Cut's allocation pattern repeatedly.
func cutByte(s string, sep byte) (before, after string, found bool) {
	if i := strings.IndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

// decodeSASLname reverses the "=2C" / "=3D" escapes used by SCRAM SASLnames
// (RFC 5802 §5.1). Any other "=XX" sequence, or a stray '=' or ',' in the
// raw name, is rejected.
func decodeSASLname(raw string) (string, error) {
	if !strings.ContainsRune(raw, '=') {
		// fast path: no escapes.
		if strings.ContainsRune(raw, ',') {
			return "", errors.New("decodeSASLname: unescaped comma")
		}
		return raw, nil
	}
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch c {
		case ',':
			return "", errors.New("decodeSASLname: unescaped comma")
		case '=':
			if i+2 >= len(raw) {
				return "", errors.New("decodeSASLname: truncated escape")
			}
			esc := raw[i+1 : i+3]
			switch esc {
			case "2C":
				b.WriteByte(',')
			case "3D":
				b.WriteByte('=')
			default:
				return "", errors.Errorf("decodeSASLname: invalid escape =%s", esc)
			}
			i += 2
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}
