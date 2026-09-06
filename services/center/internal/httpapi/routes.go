package httpapi

import (
	"center-service/internal/upload"
	"log/slog"
	"net/http"
)

// NewRouter builds the root mux, dispatching by leading path prefix to each
// service's own route table (each service's Routes() sees paths with its
// prefix already stripped).
func NewRouter(svc *upload.Service, logger *slog.Logger) http.Handler {
	uploadHandlers := upload.NewHandlers(svc, logger)

	mux := http.NewServeMux()
	// bare "/upload" handled directly: StripPrefix would reduce it to an empty
	// path, which the inner mux 301-redirects to "/" instead of matching "POST /".
	mux.HandleFunc("POST /upload", uploadHandlers.CreateUpload)
	mux.Handle("/upload/", http.StripPrefix("/upload", uploadHandlers.Routes()))
	return mux
}
