package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"center-service/internal/config"
	"center-service/internal/hashing"
	"center-service/internal/httpapi"
	"center-service/internal/storage/minio"
	"center-service/internal/store/sqlite"
	"center-service/internal/upload"
)

const hashWorkers = 2

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

	// Cancelled on Ctrl-C / SIGTERM; stops the HTTP server, hash workers and sweeper.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := sqlite.NewStore(db)

	hashCfg := hashing.DefaultConfig()
	var wg sync.WaitGroup
	for range hashWorkers {
		worker := hashing.NewWorker(hashing.NewWorkerID(), hashCfg, store, minioClient, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Run(ctx)
		}()
	}
	sweeper := hashing.NewSweeper(hashCfg, store, logger)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sweeper.Run(ctx)
	}()

	svc := upload.NewService(store, minioClient, logger)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Go.Port),
		Handler: httpapi.NewRouter(svc, logger),
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown", "err", err)
		}
	}()

	log.Printf("center service starting on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server: %v", err)
	}
	wg.Wait()
	log.Printf("center service stopped")
}
