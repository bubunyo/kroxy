// Package resolver maps client SASL credentials to a tenant and the upstream
// connection details the proxy should use on the tenant's behalf.
package resolver

import (
	"context"

	"github.com/pkg/errors"
)

// ErrUnauthorized is returned by Resolver implementations when the supplied
// credentials are unknown or do not match.
var ErrUnauthorized = errors.New("unauthorized")

// ErrDuplicate is returned when a write operation would create a user that
// already exists.
var ErrDuplicate = errors.New("user already exists")

// ErrNotFound is returned when an update or delete targets a user that does
// not exist.
var ErrNotFound = errors.New("user not found")

// ErrInvalidUser is returned when a user record fails validation (e.g. an
// empty required field).
var ErrInvalidUser = errors.New("invalid user")

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

// TenantSummary is a password-free view of a configured tenant suitable for
// returning to admin clients.
type TenantSummary struct {
	Username         string
	TenantID         string
	TopicPrefix      string
	Upstream         string
	UpstreamSASLUser string
}

// Resolver looks up the Tenant associated with a SASL/PLAIN credential pair
// and exposes write operations for managing the tenant catalogue.
//
// Get must return ErrUnauthorized when the credentials are not recognised;
// any other error is treated as a transient resolver failure.
type Resolver interface {
	Get(ctx context.Context, username, password string) (Tenant, error)
	Set(ctx context.Context, user MemoryUser) error
	Update(ctx context.Context, patch UserPatch) error
	Delete(ctx context.Context, username string) error
	List(ctx context.Context) ([]TenantSummary, error)
}

// UserPatch is a partial-update payload for an existing user. A nil pointer
// means "leave the existing field unchanged"; a non-nil pointer (including
// pointer to empty string) replaces the field.
type UserPatch struct {
	Username             string
	Password             *string
	TenantID             *string
	TopicPrefix          *string
	Upstream             *string
	UpstreamSASLUsername *string
	UpstreamSASLPassword *string
}
