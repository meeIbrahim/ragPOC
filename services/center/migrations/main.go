// Command migrate applies (or rolls back) the center service's SQLite schema.
package main

import (
	"embed"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"center-service/internal/config"
)

//go:embed *.sql
var migrationFiles embed.FS

func main() {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	down := flag.Bool("down", false, "roll back the last applied migration instead of applying pending ones")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	if err := os.MkdirAll(cfg.Go.SQLiteDir, 0o755); err != nil {
		log.Fatalf("sqlite dir: %v", err)
	}
	dbPath := filepath.Join(cfg.Go.SQLiteDir, "center.db")

	source, err := iofs.New(migrationFiles, ".")
	if err != nil {
		log.Fatalf("migration source: %v", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", source, "sqlite://"+dbPath)
	if err != nil {
		log.Fatalf("migrate init: %v", err)
	}
	defer m.Close()

	if *down {
		err = m.Steps(-1)
	} else {
		err = m.Up()
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("migrate: %v", err)
	}

	log.Printf("migrations applied to %s", dbPath)
}
