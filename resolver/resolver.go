// Package resolver maps a SASL/PLAIN username to a tenant and the upstream
// connection details the proxy should use on the tenant's behalf.
package resolver

import (
	"context"

	"github.com/pkg/errors"
)

// ErrUnauthorized is returned by Resolver implementations when the supplied
// username is unknown.
var ErrUnauthorized = errors.New("unauthorized")

// ErrDuplicate is returned when a write operation would create a user that
// already exists.
var ErrDuplicate = errors.New("user already exists")

// ErrNotFound is returned when a delete targets a user that does not exist.
var ErrNotFound = errors.New("user not found")

// ErrInvalidUser is returned when a user record fails validation (e.g. an
// empty required field).
var ErrInvalidUser = errors.New("invalid user")

// Tenant describes how a single authenticated client maps onto an upstream
// Kafka cluster. kroxy stores no client secrets: the SASL/PLAIN password
// supplied by the client is forwarded verbatim to the tenant's upstream.
type Tenant struct {
	ID          string
	TopicPrefix string
	Upstream    string
}

// TenantSummary is a view of a configured tenant returned to admin clients.
type TenantSummary struct {
	Username    string
	TenantID    string
	TopicPrefix string
	Upstream    string
}

// Resolver looks up the Tenant associated with a SASL/PLAIN username and
// exposes write operations for managing the tenant catalogue.
//
// Get must return ErrUnauthorized when the username is not recognised; any
// other error is treated as a transient resolver failure.
type Resolver interface {
	Get(ctx context.Context, username string) (Tenant, error)
	Set(ctx context.Context, user MemoryUser) error
	Delete(ctx context.Context, username string) error
	List(ctx context.Context) ([]TenantSummary, error)
}
