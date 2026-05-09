package resolver

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// MemoryUser is a single configured tenant in the in-memory resolver.
type MemoryUser struct {
	Username    string
	TenantID    string
	TopicPrefix string
	Upstream    string
}

// Memory is a Resolver backed by an in-memory map of usernames.
//
// It is safe for concurrent use; Get takes a read lock and the mutators take
// a write lock.
type Memory struct {
	mu    sync.RWMutex
	users map[string]MemoryUser
}

// NewMemory builds a Memory resolver from the supplied user list. Duplicate
// usernames are rejected to make configuration mistakes loud.
func NewMemory(users []MemoryUser) (*Memory, error) {
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
	return &Memory{users: m}, nil
}

// Get implements Resolver.
func (m *Memory) Get(_ context.Context, username string) (Tenant, error) {
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
func (m *Memory) Set(_ context.Context, u MemoryUser) error {
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
func (m *Memory) Delete(_ context.Context, username string) error {
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
func (m *Memory) List(_ context.Context) ([]TenantSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TenantSummary, 0, len(m.users))
	for _, u := range m.users {
		out = append(out, TenantSummary(u))
	}
	return out, nil
}

func validateUser(u MemoryUser) error {
	switch {
	case u.Username == "":
		return errors.Wrap(ErrInvalidUser, "empty username")
	case u.TenantID == "":
		return errors.Wrap(ErrInvalidUser, "empty tenant id")
	case u.TopicPrefix == "":
		return errors.Wrap(ErrInvalidUser, "empty topic prefix")
	case u.Upstream == "":
		return errors.Wrap(ErrInvalidUser, "empty upstream")
	}
	return nil
}
