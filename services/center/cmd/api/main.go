package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"center-service/internal/config"
	"center-service/internal/httpapi"
	"center-service/internal/storage/minio"
	"center-service/internal/store/sqlite"
	"center-service/internal/upload"
)

func main() {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	db, err := sqlite.Open(cfg)
	if err != nil {
		log.Fatalf("Error Opening Database: %v", err)
	}
	defer db.Close()

	minioClient, err := minio.New(cfg.Minio)
	if err != nil {
		log.Fatalf("Error Creating Minio Client: %v", err)
	}

	store := sqlite.NewStore(db)
	svc := upload.NewService(store, minioClient, logger)
	router := httpapi.NewRouter(svc, logger)

	addr := fmt.Sprintf(":%d", cfg.Go.Port)
	log.Printf("center service starting on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("server: %v", err)
	}
}
