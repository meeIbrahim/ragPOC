package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"center-service/internal/config"
	redisevents "center-service/internal/events/redis"
	"center-service/internal/hashing"
	"center-service/internal/httpapi"
	"center-service/internal/outbox"
	"center-service/internal/status"
	"center-service/internal/storage/minio"
	"center-service/internal/store/sqlite"
	"center-service/internal/upload"
)

const (
	hashWorkers   = 2
	outboxWorkers = 2
)

// The sqlite store and redis producer must satisfy the consumer-side interfaces the workers
// and the upload service declare.
var (
	_ hashing.Store   = (*sqlite.Store)(nil)
	_ outbox.Store    = (*sqlite.Store)(nil)
	_ upload.Store    = (*sqlite.Store)(nil)
	_ outbox.Producer = (*redisevents.Producer)(nil)
	_ status.Store    = (*sqlite.Store)(nil)
	_ status.Source   = (*redisevents.StatusSource)(nil)
)

func main() {
	if err := run(); err != nil {
		log.Printf("center service: %v", err)
		os.Exit(1)
	}
}

// run holds everything that needs deferred cleanup, so an error returns through the defers
// instead of exiting the process past them.
func run() error {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if len(cfg.UnknownKeys) > 0 {
		logger.Warn("config has keys this service does not read", "keys", cfg.UnknownKeys)
	}

	db, err := sqlite.Open(cfg)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	minioClient, err := minio.New(cfg.Minio)
	if err != nil {
		return fmt.Errorf("create minio client: %w", err)
	}

	// Cancelled on Ctrl-C / SIGTERM; stops the HTTP server, hash and outbox workers, and sweepers.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisevents.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("create redis client: %w", err)
	}
	defer redisClient.Close()

	// Bind the port before any worker starts, so an instance that cannot serve exits without
	// claiming rows or publishing events.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Go.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

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

	outboxCfg := outbox.DefaultConfig()
	producer := redisevents.NewProducer(redisClient)
	for range outboxWorkers {
		worker := outbox.NewWorker(outbox.NewWorkerId(), outboxCfg, store, producer, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Run(ctx)
		}()
	}
	outboxSweeper := outbox.NewSweeper(outboxCfg, store, logger)
	wg.Add(1)
	go func() {
		defer wg.Done()
		outboxSweeper.Run(ctx)
	}()

	statusHandler := status.NewHandler(store, cfg.Ingestion.MaxAttempts, cfg.Go.StatusConsumer.EntryTimeout.Std(), logger)
	statusWorker := status.NewWorker(
		status.NewWorkerID(),
		status.WorkerConfigFrom(cfg.Go.StatusConsumer),
		redisevents.NewStatusSource(redisClient, logger),
		statusHandler,
		logger,
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statusWorker.Run(ctx)
	}()

	svc := upload.NewService(store, minioClient, logger)
	server := &http.Server{
		Handler: httpapi.NewRouter(svc, logger, cfg.Ingestion.AllowedExtensions),
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown", "err", err)
		}
	}()

	log.Printf("center service starting on %s", listener.Addr())
	serveErr := server.Serve(listener)
	// A server that failed to start must still stop the workers, or wg.Wait blocks forever.
	stop()
	<-shutdownDone
	wg.Wait()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", serveErr)
	}
	log.Printf("center service stopped")
	return nil
}
