// Command gomm2 starts the gomm2 Kafka replication engine.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/mirror"
)

// version is injected at build time via -ldflags "-X main.version=..."
var version = "dev"

func main() {
	configPath := flag.String("config", "/etc/gomm2/config.yaml", "path to configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("gomm2 version", version)
		os.Exit(0)
	}

	// Check for subcommands
	args := flag.Args()
	if len(args) > 0 && args[0] == "validate" {
		runValidate(*configPath)
		return
	}

	// Default: load config → validate → run engine
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid config: %v\n", err)
		os.Exit(1)
	}

	engine, err := mirror.NewEngine(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: create engine: %v\n", err)
		os.Exit(1)
	}

	if err := engine.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: engine run: %v\n", err)
		os.Exit(1)
	}
}

func runValidate(configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "validation FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("configuration is valid")
}
