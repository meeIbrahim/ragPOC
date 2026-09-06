package main

import (
	"flag"
	"log"

	"center-service/internal/config"
)

func main() {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	log.Printf("center service starting on port %d", cfg.Go.Port)
}
