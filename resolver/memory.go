package resolver

import (
	"context"
	"crypto/subtle"
)

// MemoryUser is a single configured user in the in-memory resolver.
type MemoryUser struct {
	Username     string
	Password     string
	TenantID     string
	TopicPrefix  string
	Upstream     string
	UpstreamSASL SASLCreds
}

// Memory is a Resolver backed by a static in-memory map of usernames.
//
// It is safe for concurrent use; the map is built once at construction and
// never mutated.
type Memory struct {
	users map[string]MemoryUser
}

// NewMemory builds a Memory resolver from the supplied user list. Duplicate
// usernames are rejected to make configuration mistakes loud.
func NewMemory(users []MemoryUser) (*Memory, error) {
	m := make(map[string]MemoryUser, len(users))
	for _, u := range users {
		if _, ok := m[u.Username]; ok {
			return nil, errDuplicateUser(u.Username)
		}
		m[u.Username] = u
	}
	return &Memory{users: m}, nil
}

// Get implements Resolver.
func (m *Memory) Get(_ context.Context, username, password string) (Tenant, error) {
	u, ok := m.users[username]
	if !ok {
		return Tenant{}, ErrUnauthorized
	}
	if subtle.ConstantTimeCompare([]byte(u.Password), []byte(password)) != 1 {
		return Tenant{}, ErrUnauthorized
	}
	return Tenant{
		ID:           u.TenantID,
		TopicPrefix:  u.TopicPrefix,
		Upstream:     u.Upstream,
		UpstreamSASL: u.UpstreamSASL,
	}, nil
}

type duplicateUserError string

func (e duplicateUserError) Error() string { return "resolver: duplicate user " + string(e) }

func errDuplicateUser(username string) error { return duplicateUserError(username) }
