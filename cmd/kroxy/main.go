// Command kroxy runs the multi-tenant Kafka proxy.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bubunyo/kroxy/admin"
	"github.com/bubunyo/kroxy/config"
	"github.com/bubunyo/kroxy/observability"
	"github.com/bubunyo/kroxy/proxy"
	"github.com/bubunyo/kroxy/resolver"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kroxy: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfgPath     string
		showVersion bool
	)
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to YAML config")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		return nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	log := observability.NewLogger(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	log.Info("kroxy starting", "version", version)

	res, err := resolver.New(cfg.Resolver)
	if err != nil {
		return err
	}

	var metrics *observability.Metrics
	if cfg.Metrics.Enabled {
		metrics = observability.NewMetrics()
	}

	srv := proxy.NewServer(proxy.ServerConfig{
		Listen:     cfg.Listen,
		Advertised: cfg.Advertised,
	}, res, metrics, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	metricsErrCh := make(chan error, 1)
	if metrics != nil {
		go func() { metricsErrCh <- observability.ServeMetrics(ctx, cfg.Metrics.Listen, metrics, log) }()
	}

	adminErrCh := make(chan error, 1)
	if cfg.Admin.Enabled {
		svc := admin.NewService(res, log)
		go func() { adminErrCh <- admin.Serve(ctx, cfg.Admin.Listen, svc, log) }()
	}

	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-errCh:
		if err != nil {
			return err
		}
	case err := <-metricsErrCh:
		if err != nil {
			return err
		}
	case err := <-adminErrCh:
		if err != nil {
			return err
		}
	}
	srv.Wait()
	return nil
}
