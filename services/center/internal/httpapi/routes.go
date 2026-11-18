package httpapi

import (
	"log/slog"
	"net/http"

	"center-service/internal/upload"
)

// Deps is everything the root router mounts. A struct, so adding a route does not change every
// caller.
type Deps struct {
	Upload            *upload.Service
	Ask               http.HandlerFunc
	Documents         http.HandlerFunc
	Logger            *slog.Logger
	AllowedExtensions []string
	// Static serves the web UI at "/" when set (from [go.ui].static_dir); nil serves no UI.
	Static http.Handler
}

// NewRouter builds the root mux, dispatching by leading path prefix to each
// service's own route table (each service's Routes() sees paths with its
// prefix already stripped).
func NewRouter(d Deps) http.Handler {
	uploadHandlers := upload.NewHandlers(d.Upload, d.Logger, d.AllowedExtensions)

	mux := http.NewServeMux()
	// bare "/upload" handled directly: StripPrefix would reduce it to an empty
	// path, which the inner mux 301-redirects to "/" instead of matching "POST /".
	mux.HandleFunc("POST /upload", uploadHandlers.CreateUpload)
	mux.Handle("/upload/", http.StripPrefix("/upload", uploadHandlers.Routes()))
	mux.HandleFunc("POST /ask", d.Ask)
	mux.HandleFunc("GET /documents", d.Documents)
	// "/" with no method: "GET /" would overlap "/upload/" (each is more specific in one way)
	// and make ServeMux panic. Every pattern above is more specific, so it still wins.
	if d.Static != nil {
		mux.Handle("/", d.Static)
	}
	return mux
}
