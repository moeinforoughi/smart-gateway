// Command gateway is the single executable entry point of Smart Gateway
// (FR-001). It loads and validates configuration before any subsystem starts
// (FR-002, FR-003) and delegates to internal/app for the lifecycle.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"smart-gateway/internal/app"
	"smart-gateway/internal/config"
	"smart-gateway/internal/log"
)

func main() {
	var configPath string
	var showVersion bool
	flag.StringVar(&configPath, "config", "", "path to config.yaml (optional; defaults apply when empty)")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(app.Version())
		return
	}

	// FR-003: fail fast and terminate if configuration is invalid.
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "smart-gateway: %v\n", err)
		os.Exit(1)
	}

	logger := log.New(os.Stderr, log.LevelFromString(cfg.Log.Level))
	logger.Info("starting", "version", app.Version())

	a, err := app.New(cfg, logger)
	if err != nil {
		logger.Error("initialization failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := a.Run(ctx); err != nil {
		logger.Error("gateway exited with error", "error", err)
		os.Exit(1)
	}
}
