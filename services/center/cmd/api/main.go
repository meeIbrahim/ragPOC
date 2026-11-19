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

	"center-service/internal/ask"
	"center-service/internal/config"
	"center-service/internal/documents"
	redisevents "center-service/internal/events/redis"
	"center-service/internal/hashing"
	"center-service/internal/httpapi"
	"center-service/internal/outbox"
	"center-service/internal/ragclient"
	"center-service/internal/status"
	"center-service/internal/storage/minio"
	"center-service/internal/store/sqlite"
	"center-service/internal/upload"
	"center-service/internal/webui"
)

const (
	hashWorkers   = 2
	outboxWorkers = 2
	// shutdownTimeout bounds http.Server.Shutdown. Open ask streams end at once on shutdown
	// (see newServer), so it only has to cover in-flight short requests.
	shutdownTimeout = 10 * time.Second
)

// The sqlite store and redis producer must satisfy the consumer-side interfaces the workers
// and the upload service declare.
var (
	_ hashing.Store    = (*sqlite.Store)(nil)
	_ outbox.Store     = (*sqlite.Store)(nil)
	_ upload.Store     = (*sqlite.Store)(nil)
	_ outbox.Producer  = (*redisevents.Producer)(nil)
	_ status.Store     = (*sqlite.Store)(nil)
	_ status.Source    = (*redisevents.StatusSource)(nil)
	_ ask.Store        = (*sqlite.Store)(nil)
	_ ask.Asker        = (*ragclient.Client)(nil)
	_ documents.Lister = (*sqlite.Store)(nil)
)

func main() {
	if err := run(); err != nil {
		log.Printf("center service: %v", err)
		os.Exit(1)
	}
}

// loadConfig loads the config and checks static_dir. run calls it before anything else, so a bad
// UI folder stops the API before it opens the database, claims rows or starts a worker.
func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := webui.CheckDir(cfg.Go.UI.StaticDir); err != nil {
		return nil, fmt.Errorf("config: go.ui.static_dir %w", err)
	}
	return cfg, nil
}

// run holds everything that needs deferred cleanup, so an error returns through the defers
// instead of exiting the process past them.
func run() error {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if len(cfg.UnknownKeys) > 0 {
		logger.Warn("config has keys this service does not read", "keys", cfg.UnknownKeys)
	}
	faultValue := os.Getenv("CENTER_FAULT")
	f, err := parseFault(faultValue)
	if err != nil {
		return err
	}
	if faultValue != "" {
		logger.Warn("fault hook enabled: this center holds claims on purpose; e2e only", "CENTER_FAULT", faultValue)
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

	hashCfg := hashingConfig(cfg.Go.Hashing, f)
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

	outboxCfg := outboxConfig(cfg.Go.Outbox, f)
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

	// Lazy: no connection until the first ask, so ingest never waits on rag-server.
	ragClient, err := ragclient.New(*cfg)
	if err != nil {
		return fmt.Errorf("create rag client: %w", err)
	}
	// Deferred, so it closes when run returns: after Serve and Shutdown, once every stream ended.
	defer ragClient.Close()

	svc := upload.NewService(store, minioClient, logger)
	askSvc := ask.NewService(store, ask.Limits{MaxQueryChars: cfg.RAG.MaxQueryChars, MaxDocumentIDs: cfg.RAG.MaxDocumentIDs})
	documentHandlers := documents.NewHandlers(store, cfg.Go.Documents.MaxEntries, logger)
	var static http.Handler
	if cfg.Go.UI.StaticDir != "" {
		static = webui.Handler(cfg.Go.UI.StaticDir)
		log.Printf("serving the web UI from %s", cfg.Go.UI.StaticDir)
	}
	server := newServer(func(askShutdown context.Context) http.Handler {
		askHandlers := ask.NewHandlers(askSvc, ragClient, askShutdown, logger)
		return httpapi.NewRouter(httpapi.Deps{
			Upload:            svc,
			Ask:               askHandlers.Ask,
			Documents:         documentHandlers.List,
			Logger:            logger,
			AllowedExtensions: cfg.Ingestion.AllowedExtensions,
			Static:            static,
		})
	})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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

// newServer builds the HTTP server with the handler from build. The context passed to build
// is cancelled when Shutdown starts, which ends open POST /ask streams with an error event:
// Shutdown waits for active connections, and a streaming answer can run for minutes.
func newServer(build func(askShutdown context.Context) http.Handler) *http.Server {
	askShutdown, cancelAsks := context.WithCancel(context.Background())
	server := &http.Server{Handler: build(askShutdown)}
	server.RegisterOnShutdown(cancelAsks)
	return server
}

// fault is the e2e fault hook selected by CENTER_FAULT; the zero value runs no fault.
type fault struct {
	hashHang               bool
	outboxHangAfterPublish bool
}

func parseFault(v string) (fault, error) {
	switch v {
	case "":
		return fault{}, nil
	case "hash-hang":
		return fault{hashHang: true}, nil
	case "outbox-hang-after-publish":
		return fault{outboxHangAfterPublish: true}, nil
	}
	return fault{}, fmt.Errorf("CENTER_FAULT=%q: want hash-hang, outbox-hang-after-publish or unset", v)
}

func hashingConfig(w config.WorkerConfig, f fault) hashing.Config {
	return hashing.Config{
		Interval:         w.Interval.Std(),
		BatchSize:        w.BatchSize,
		JobTimeout:       w.JobTimeout.Std(),
		MaxRetries:       w.MaxRetries,
		RecoveryInterval: w.RecoveryInterval.Std(),
		StaleThreshold:   w.StaleThreshold.Std(),
		HangAfterClaim:   f.hashHang,
	}
}

func outboxConfig(o config.OutboxConfig, f fault) outbox.Config {
	return outbox.Config{
		Interval:         o.Interval.Std(),
		BatchSize:        o.BatchSize,
		JobTimeout:       o.JobTimeout.Std(),
		MaxRetries:       o.MaxRetries,
		RecoveryInterval: o.RecoveryInterval.Std(),
		StaleThreshold:   o.StaleThreshold.Std(),
		SchemaVersion:    o.SchemaVersion,
		HangAfterPublish: f.outboxHangAfterPublish,
	}
}
