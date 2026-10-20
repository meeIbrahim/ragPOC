package upload

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

type Handlers struct {
	svc               *Service
	logger            *slog.Logger
	allowedExtensions map[string]struct{}
}

// NewHandlers builds the upload handlers. allowedExtensions (e.g. ".pdf") are matched
// case-insensitively. This is an early UX check only: the RAG service stays the authority on
// which file types it can ingest.
func NewHandlers(svc *Service, logger *slog.Logger, allowedExtensions []string) *Handlers {
	allowed := make(map[string]struct{}, len(allowedExtensions))
	for _, ext := range allowedExtensions {
		allowed[strings.ToLower(ext)] = struct{}{}
	}
	return &Handlers{svc: svc, logger: logger, allowedExtensions: allowed}
}

type UploadDocumentRequest struct {
	FileName string `json:"filename"`
}
type UploadDocumentResponse struct {
	ID        string    `json:"id"`
	FileName  string    `json:"filename"`
	UploadURL string    `json:"upload_url"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ConfirmUploadResponse struct {
	ID         string     `json:"id"`
	FileName   string     `json:"filename"`
	Status     string     `json:"status"`
	UploadedAt *time.Time `json:"uploaded_at,omitempty"`
}

type UploadStatusResponse struct {
	ID         string     `json:"id"`
	FileName   string     `json:"filename"`
	Status     string     `json:"status"`
	UploadedAt *time.Time `json:"uploaded_at,omitempty"`
}

func (h *Handlers) CreateUpload(w http.ResponseWriter, r *http.Request) {
	var req UploadDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.FileName == "" {
		http.Error(w, "filename is required", http.StatusBadRequest)
		return
	}

	if ext := strings.ToLower(filepath.Ext(req.FileName)); !h.extensionAllowed(ext) {
		if ext == "" {
			ext = "(none)"
		}
		http.Error(w, fmt.Sprintf("unsupported file extension: %s", ext), http.StatusBadRequest)
		return
	}

	intent, uploadURL, err := h.svc.CreateIntent(r.Context(), req.FileName)
	if err != nil {
		h.logger.Error("create upload_intent failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, UploadDocumentResponse{
		ID:        intent.ID,
		FileName:  intent.FileName,
		UploadURL: uploadURL.String(),
		Status:    intent.Status.String(),
		CreatedAt: intent.CreatedAt,
		ExpiresAt: intent.ExpiresAt,
	})
}

func (h *Handlers) ConfirmUpload(w http.ResponseWriter, r *http.Request) {

	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	intent, err := h.svc.ConfirmUpload(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, ErrIntentExpired):
			http.Error(w, "upload intent expired", http.StatusGone)
		case errors.Is(err, ErrIntentFailed):
			http.Error(w, "upload intent failed", http.StatusConflict)
		case errors.Is(err, ErrIntentNotFound):
			http.Error(w, "upload intent not found", http.StatusNotFound)
		case errors.Is(err, ErrIntentUnknown):
			h.logger.Error("confirm upload failed", "id", id, "err", err)
			http.Error(w, "upload intent in unknown state", http.StatusInternalServerError)
		default:
			h.logger.Error("confirm upload failed", "id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	writeJSON(w, http.StatusAccepted, ConfirmUploadResponse{
		ID:         intent.ID,
		FileName:   intent.FileName,
		Status:     intent.Status.String(),
		UploadedAt: intent.UploadedAt,
	})
}

func (h *Handlers) GetUploadStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	intent, status, err := h.svc.GetUploadStatus(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, ErrIntentNotFound):
			http.Error(w, "upload intent not found", http.StatusNotFound)
		case errors.Is(err, ErrIntentUnknown):
			h.logger.Error("get upload status failed", "id", id, "err", err)
			http.Error(w, "upload intent in unknown state", http.StatusInternalServerError)
		default:
			h.logger.Error("get upload status failed", "id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	resp := UploadStatusResponse{
		ID:         intent.ID,
		FileName:   intent.FileName,
		Status:     *status,
		UploadedAt: intent.UploadedAt,
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) extensionAllowed(ext string) bool {
	_, ok := h.allowedExtensions[ext]
	return ok
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
