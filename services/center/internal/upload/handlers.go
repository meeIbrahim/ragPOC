package upload

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type Handlers struct {
	svc    *Service
	logger *slog.Logger
}

func NewHandlers(svc *Service, logger *slog.Logger) *Handlers {
	return &Handlers{svc: svc, logger: logger}
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
	ID          string     `json:"id"`
	FileName    string     `json:"filename"`
	Status      string     `json:"status"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
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
			http.Error(w, "upload intent expired", http.StatusConflict)
		default:
			h.logger.Error("confirm upload failed", "id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	writeJSON(w, http.StatusOK, ConfirmUploadResponse{
		ID:          intent.ID,
		FileName:    intent.FileName,
		Status:      intent.Status.String(),
		CompletedAt: intent.CompletedAt,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
