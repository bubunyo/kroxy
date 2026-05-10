package resolver

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// MemoryResolver is a Resolver backed by an in-memory map of tenants keyed
// by tenant ID.
//
// It is safe for concurrent use; Get takes a read lock and the mutators take
// a write lock.
type MemoryResolver struct {
	mu      sync.RWMutex
	tenants map[string]Tenant
}

// NewMemoryResolver builds a MemoryResolver from the supplied tenant list.
// Duplicate IDs are rejected to make configuration mistakes loud.
func NewMemoryResolver(tenants []Tenant) (*MemoryResolver, error) {
	m := make(map[string]Tenant, len(tenants))
	for _, t := range tenants {
		if err := validateTenant(t); err != nil {
			return nil, errors.Wrap(err, "NewMemoryResolver")
		}
		if _, ok := m[t.ID]; ok {
			return nil, errors.Wrapf(ErrDuplicate, "NewMemoryResolver: %s", t.ID)
		}
		m[t.ID] = t
	}
	return &MemoryResolver{tenants: m}, nil
}

// Get implements Resolver.
func (m *MemoryResolver) Get(_ context.Context, id string) (Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[id]
	if !ok {
		return Tenant{}, ErrUnauthorized
	}
	return t, nil
}

// Set inserts a brand-new tenant. It returns ErrDuplicate if the tenant ID
// already exists.
func (m *MemoryResolver) Set(_ context.Context, t Tenant) error {
	if err := validateTenant(t); err != nil {
		return errors.Wrap(err, "MemoryResolver.Set")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[t.ID]; ok {
		return errors.Wrapf(ErrDuplicate, "MemoryResolver.Set: %s", t.ID)
	}
	m.tenants[t.ID] = t
	return nil
}

// Delete removes a tenant. It returns ErrNotFound when the tenant ID is
// unknown.
func (m *MemoryResolver) Delete(_ context.Context, id string) error {
	if id == "" {
		return errors.Wrap(ErrInvalidTenant, "MemoryResolver.Delete: empty tenant id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[id]; !ok {
		return errors.Wrapf(ErrNotFound, "MemoryResolver.Delete: %s", id)
	}
	delete(m.tenants, id)
	return nil
}

// List returns a snapshot of all configured tenants. The returned slice is
// detached from internal storage; callers may modify it freely.
func (m *MemoryResolver) List(_ context.Context) ([]Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		out = append(out, t)
	}
	return out, nil
}

func validateTenant(t Tenant) error {
	switch {
	case t.ID == "":
		return errors.Wrap(ErrInvalidTenant, "empty tenant id")
	case t.TopicPrefix == "":
		return errors.Wrap(ErrInvalidTenant, "empty topic prefix")
	case t.Upstream == "":
		return errors.Wrap(ErrInvalidTenant, "empty upstream")
	}
	return nil
}
