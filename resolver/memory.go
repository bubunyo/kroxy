package resolver

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// MemoryResolver is a Resolver backed by an in-memory map of usernames.
//
// It is safe for concurrent use; Get takes a read lock and the mutators take
// a write lock.
type MemoryResolver struct {
	mu      sync.RWMutex
	tenants map[string]Tenant
}

// NewMemoryResolver builds a MemoryResolver resolver from the supplied user list. Duplicate
// usernames are rejected to make configuration mistakes loud.
func NewMemoryResolver(tenants []Tenant) (*MemoryResolver, error) {
	m := make(map[string]MemoryUser, len(users))
	for _, u := range users {
		if err := validateUser(u); err != nil {
			return nil, errors.Wrap(err, "NewMemory")
		}
		if _, ok := m[u.Username]; ok {
			return nil, errors.Wrapf(ErrDuplicate, "NewMemory: %s", u.Username)
		}
		m[u.Username] = u
	}
	return &MemoryResolver{users: m}, nil
}

// Get implements Resolver.
func (m *MemoryResolver) Get(_ context.Context, username string) (Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.users[username]
	if !ok {
		return Tenant{}, ErrUnauthorized
	}
	return Tenant{
		ID:          u.TenantID,
		TopicPrefix: u.TopicPrefix,
		Upstream:    u.Upstream,
	}, nil
}

// Set inserts a brand-new user. It returns ErrDuplicate if the username
// already exists.
func (m *MemoryResolver) Set(_ context.Context, u MemoryUser) error {
	if err := validateUser(u); err != nil {
		return errors.Wrap(err, "Memory.Set")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[u.Username]; ok {
		return errors.Wrapf(ErrDuplicate, "Memory.Set: %s", u.Username)
	}
	m.users[u.Username] = u
	return nil
}

// Delete removes a user. It returns ErrNotFound when the username is unknown.
func (m *MemoryResolver) Delete(_ context.Context, username string) error {
	if username == "" {
		return errors.Wrap(ErrInvalidUser, "Memory.Delete: empty username")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[username]; !ok {
		return errors.Wrapf(ErrNotFound, "Memory.Delete: %s", username)
	}
	delete(m.users, username)
	return nil
}

// List returns a snapshot of all configured tenants. The returned slice is
// detached from internal storage; callers may modify it freely.
func (m *MemoryResolver) List(_ context.Context) ([]Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Tenant, 0, len(m.tenants))
	for _, u := range m.tenants {
		out = append(out, u)
	}
	return out, nil
}

func validateUser(u Tenant) error {
	switch {
	case u.ID == "":
		return errors.Wrap(ErrInvalidUser, "empty tenant id")
	case u.TopicPrefix == "":
		return errors.Wrap(ErrInvalidUser, "empty topic prefix")
	case u.Upstream == "":
		return errors.Wrap(ErrInvalidUser, "empty upstream")
	}
	return nil
}
