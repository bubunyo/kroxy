// Package config defines the on-disk configuration shape and a loader.
package config

import (
	"net"
	"os"

	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration loaded from YAML.
type Config struct {
	Listen     string          `yaml:"listen"`
	Advertised string          `yaml:"advertised"`
	Upstream   UpstreamConfig  `yaml:"upstream"`
	Resolver   resolver.Config `yaml:"resolver"`
	Log        LogConfig       `yaml:"log"`
	Metrics    MetricsConfig   `yaml:"metrics"`
	Admin      AdminConfig     `yaml:"admin"`
}

// MetricsConfig configures the Prometheus metrics endpoint.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

// AdminConfig configures the JSON-RPC admin endpoint.
//
// The endpoint is unauthenticated; bind it to a loopback address (the
// default) or otherwise gate access at the network layer.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

// UpstreamConfig describes the shared upstream Kafka cluster.
type UpstreamConfig struct {
	Bootstrap string `yaml:"bootstrap"`
}

// LogConfig configures the slog handler.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load reads, parses and validates a YAML config file at path.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, errors.Wrap(err, "Load")
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return Config{}, errors.Wrap(err, "Load")
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return Config{}, errors.Wrap(err, "Load")
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":9092"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Metrics.Listen == "" {
		c.Metrics.Listen = ":9090"
	}
	if c.Admin.Listen == "" {
		c.Admin.Listen = "127.0.0.1:9095"
	}
	if c.Resolver.Type == "" {
		c.Resolver.Type = "memory"
	}
	// Fill in tenant.Upstream from the shared bootstrap when omitted.
	for i := range c.Resolver.Memory.Tenants {
		if c.Resolver.Memory.Tenants[i].Upstream == "" {
			c.Resolver.Memory.Tenants[i].Upstream = c.Upstream.Bootstrap
		}
	}
}

func (c *Config) validate() error {
	if c.Advertised == "" {
		return errors.New("config: advertised is required")
	}
	if c.Upstream.Bootstrap == "" {
		return errors.New("config: upstream.bootstrap is required")
	}
	switch c.Resolver.Type {
	case "memory":
		if err := c.validateMemoryResolver(); err != nil {
			return err
		}
	default:
		return errors.Errorf("config: unknown resolver.type %q", c.Resolver.Type)
	}
	if c.Admin.Enabled {
		if _, _, err := net.SplitHostPort(c.Admin.Listen); err != nil {
			return errors.Wrapf(err, "config: admin.listen is invalid")
		}
	}
	return nil
}

func (c *Config) validateMemoryResolver() error {
	tenants := c.Resolver.Memory.Tenants
	if len(tenants) == 0 && !c.Admin.Enabled {
		return errors.New("config: resolver.memory.tenants must contain at least one entry (or enable the admin RPC)")
	}
	for i, t := range tenants {
		if t.ID == "" {
			return errors.Errorf("config: resolver.memory.tenants[%d].id is required", i)
		}
		if t.TopicPrefix == "" {
			return errors.Errorf("config: resolver.memory.tenants[%d].topic_prefix is required", i)
		}
	}
	return nil
}
