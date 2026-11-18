// Package documents serves GET /documents: every received document once, newest first.
package documents

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"center-service/internal/domain"
)

// Lister reads the document list (sqlite.Store). truncated is true when more than limit
// entries exist.
type Lister interface {
	ListDocuments(ctx context.Context, limit int) (entries []domain.DocumentEntry, truncated bool, err error)
}

type Handlers struct {
	lister     Lister
	maxEntries int
	logger     *slog.Logger
}

// NewHandlers builds the GET /documents handler. maxEntries is [go.documents].max_entries.
func NewHandlers(lister Lister, maxEntries int, logger *slog.Logger) *Handlers {
	return &Handlers{lister: lister, maxEntries: maxEntries, logger: logger}
}

type documentJSON struct {
	ID            string    `json:"id"`
	UploadIDs     []string  `json:"upload_ids"`
	FileName      string    `json:"filename"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type listResponse struct {
	Documents []documentJSON `json:"documents"`
	Truncated bool           `json:"truncated"`
}

func (h *Handlers) List(w http.ResponseWriter, r *http.Request) {
	entries, truncated, err := h.lister.ListDocuments(r.Context(), h.maxEntries)
	if err != nil {
		h.logger.Error("list documents failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	resp := listResponse{Documents: make([]documentJSON, 0, len(entries)), Truncated: truncated}
	for _, e := range entries {
		d := documentJSON{ID: e.ID, UploadIDs: e.UploadIDs, FileName: e.FileName, Status: string(e.Status), CreatedAt: e.CreatedAt}
		// The store already pairs reasons with FAILED; checked again here because the response
		// contract (reason only on FAILED) is this handler's, whatever the Lister returns.
		if e.Status == domain.DocumentFailed {
			d.FailureReason = e.FailureReason
		}
		resp.Documents = append(resp.Documents, d)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Warn("write documents response failed", "err", err)
	}
}
