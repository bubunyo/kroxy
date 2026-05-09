// Command kroxy runs the multi-tenant Kafka proxy.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bubunyo/kroxy/config"
	"github.com/bubunyo/kroxy/observability"
	"github.com/bubunyo/kroxy/proxy"
	"github.com/bubunyo/kroxy/resolver"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kroxy: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to YAML config")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	log := observability.NewLogger(os.Stdout, cfg.Log.Level, cfg.Log.Format)

	res, err := resolver.NewMemory(cfg.MemoryUsers())
	if err != nil {
		return err
	}

	srv := proxy.NewServer(proxy.ServerConfig{
		Listen:     cfg.Listen,
		Advertised: cfg.Advertised,
	}, res, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-errCh:
		if err != nil {
			return err
		}
	}
	srv.Wait()
	return nil
}
