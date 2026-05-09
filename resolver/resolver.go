// Package resolver maps client SASL credentials to a tenant and the upstream
// connection details the proxy should use on the tenant's behalf.
package resolver

import "context"

import "github.com/pkg/errors"

// ErrUnauthorized is returned by Resolver implementations when the supplied
// credentials are unknown or do not match.
var ErrUnauthorized = errors.New("unauthorized")

// SASLCreds is a SASL/PLAIN username + password pair the proxy uses to
// authenticate to the upstream Kafka cluster.
type SASLCreds struct {
	Username string
	Password string
}

// Tenant describes how a single authenticated client maps onto the shared
// upstream cluster.
type Tenant struct {
	ID           string
	TopicPrefix  string
	Upstream     string
	UpstreamSASL SASLCreds
}

// Resolver looks up the Tenant associated with a SASL/PLAIN credential pair.
//
// Implementations must return ErrUnauthorized when the credentials are not
// recognised; any other error is treated as a transient resolver failure.
type Resolver interface {
	Get(ctx context.Context, username, password string) (Tenant, error)
}
