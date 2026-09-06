package upload

import "net/http"

// Routes returns this service's handlers mounted at its own root — the caller
// is expected to have already stripped its prefix (e.g. "/upload").
func (h *Handlers) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", h.CreateUpload)
	mux.HandleFunc("POST /{id}/confirm", h.ConfirmUpload)
	return mux
}
