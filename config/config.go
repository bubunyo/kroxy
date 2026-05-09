// Package config defines the on-disk configuration shape and a loader.
package config

import (
	"os"

	"github.com/bubunyo/kroxy/resolver"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration loaded from YAML.
type Config struct {
	Listen     string         `yaml:"listen"`
	Advertised string         `yaml:"advertised"`
	Upstream   UpstreamConfig `yaml:"upstream"`
	Resolver   ResolverConfig `yaml:"resolver"`
	Log        LogConfig      `yaml:"log"`
	Metrics    MetricsConfig  `yaml:"metrics"`
}

// MetricsConfig configures the Prometheus metrics endpoint.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

// UpstreamConfig describes the shared upstream Kafka cluster.
type UpstreamConfig struct {
	Bootstrap string `yaml:"bootstrap"`
}

// ResolverConfig holds the in-memory resolver's user list.
type ResolverConfig struct {
	Users []UserConfig `yaml:"users"`
}

// UserConfig is a single tenant credential entry.
type UserConfig struct {
	Username     string     `yaml:"username"`
	Password     string     `yaml:"password"`
	TenantID     string     `yaml:"tenant_id"`
	TopicPrefix  string     `yaml:"topic_prefix"`
	Upstream     string     `yaml:"upstream"`
	UpstreamSASL SASLConfig `yaml:"upstream_sasl"`
}

// SASLConfig is a SASL/PLAIN credential pair.
type SASLConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
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
}

func (c *Config) validate() error {
	if c.Advertised == "" {
		return errors.New("config: advertised is required")
	}
	if c.Upstream.Bootstrap == "" {
		return errors.New("config: upstream.bootstrap is required")
	}
	if len(c.Resolver.Users) == 0 {
		return errors.New("config: resolver.users must contain at least one user")
	}
	for i, u := range c.Resolver.Users {
		if u.Username == "" {
			return errors.Errorf("config: resolver.users[%d].username is required", i)
		}
		if u.Password == "" {
			return errors.Errorf("config: resolver.users[%d].password is required", i)
		}
		if u.TenantID == "" {
			return errors.Errorf("config: resolver.users[%d].tenant_id is required", i)
		}
		if u.TopicPrefix == "" {
			return errors.Errorf("config: resolver.users[%d].topic_prefix is required", i)
		}
	}
	return nil
}

// MemoryUsers maps the user list onto the resolver package's input type.
func (c *Config) MemoryUsers() []resolver.MemoryUser {
	out := make([]resolver.MemoryUser, len(c.Resolver.Users))
	for i, u := range c.Resolver.Users {
		upstream := u.Upstream
		if upstream == "" {
			upstream = c.Upstream.Bootstrap
		}
		out[i] = resolver.MemoryUser{
			Username:    u.Username,
			Password:    u.Password,
			TenantID:    u.TenantID,
			TopicPrefix: u.TopicPrefix,
			Upstream:    upstream,
			UpstreamSASL: resolver.SASLCreds{
				Username: u.UpstreamSASL.Username,
				Password: u.UpstreamSASL.Password,
			},
		}
	}
	return out
}
