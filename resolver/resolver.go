// Package resolver maps a tenant ID (carried as the SASL/PLAIN username on
// the wire) to the upstream connection details the proxy should use on the
// tenant's behalf.
package resolver

import (
	"context"

	"github.com/pkg/errors"
)

// ErrUnauthorized is returned by Resolver implementations when the supplied
// tenant ID is unknown.
var ErrUnauthorized = errors.New("unauthorized")

// ErrDuplicate is returned when a write operation would create a tenant that
// already exists.
var ErrDuplicate = errors.New("tenant already exists")

// ErrNotFound is returned when a delete targets a tenant that does not
// exist.
var ErrNotFound = errors.New("tenant not found")

// ErrInvalidTenant is returned when a tenant record fails validation (e.g.
// an empty required field).
var ErrInvalidTenant = errors.New("invalid tenant")

// Tenant describes a single tenant's mapping onto the shared upstream Kafka
// cluster. kroxy stores no client secrets: the SASL/PLAIN password supplied
// by the client is forwarded verbatim to the tenant's upstream.
type Tenant struct {
	ID          string
	TopicPrefix string
	Upstream    string
}

// Resolver looks up the Tenant associated with a tenant ID and exposes
// write operations for managing the tenant catalogue.
//
// Get must return ErrUnauthorized when the ID is not recognised; any other
// error is treated as a transient resolver failure.
type Resolver interface {
	Get(ctx context.Context, id string) (Tenant, error)
	Set(ctx context.Context, t Tenant) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]Tenant, error)
}
