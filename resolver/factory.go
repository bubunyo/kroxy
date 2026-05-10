package resolver

import (
	"github.com/pkg/errors"
)

// Config selects and configures a Resolver implementation. The zero value
// (or an explicit Type of "memory") yields an in-memory resolver seeded
// from Memory.Tenants.
type Config struct {
	Type   string       `yaml:"type"`
	Memory MemoryConfig `yaml:"memory"`
}

// MemoryConfig holds the in-memory resolver's tenant table.
type MemoryConfig struct {
	Tenants []Tenant `yaml:"tenants"`
}

// New constructs a Resolver from cfg. An empty Type is treated as "memory".
// Unknown types return an error so misconfiguration is loud.
func New(cfg Config) (Resolver, error) {
	switch cfg.Type {
	case "", "memory":
		return newMemoryResolver(cfg.Memory.Tenants)
	default:
		return nil, errors.Errorf("New: unknown resolver type %q", cfg.Type)
	}
}
