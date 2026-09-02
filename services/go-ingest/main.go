package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"ragingest/internal/config"
	"ragingest/internal/db"
	"ragingest/internal/handlers"
	"ragingest/internal/outbox"
	"ragingest/internal/storage"
	"ragingest/internal/stream"
)

func main() {
	cfg, err := config.Load("../../config.toml")
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	conn, err := db.Open(cfg.Go.SqliteDir + "/documents.db")
	if err != nil {
		log.Fatalf("db open failed: %v", err)
	}
	defer conn.Close()

	minioStore, err := storage.NewMinioStore(cfg.Minio.URL, cfg.Minio.AccessKey, cfg.Minio.SecretKey, cfg.Minio.Bucket)
	if err != nil {
		log.Fatalf("minio client failed: %v", err)
	}

	publisher := stream.NewRedisPublisher(fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port), cfg.Redis.Stream)

	h := &handlers.IngestHandler{DB: conn, Storage: minioStore, Publisher: publisher}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest/init", h.Init)
	mux.HandleFunc("POST /ingest/confirm", h.Confirm)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	outbox.Start(ctx, conn, publisher, 5*time.Second)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Go.Port), Handler: mux}
	go func() {
		log.Printf("go-ingest listening on :%d", cfg.Go.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
