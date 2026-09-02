package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"ragingest/internal/db"
	"ragingest/internal/storage"
	"ragingest/internal/stream"
)

type IngestHandler struct {
	DB        *sql.DB
	Storage   storage.ObjectStore
	Publisher stream.Publisher
}

type initRequest struct {
	Filename string `json:"filename"`
}

type initResponse struct {
	UploadURL  string            `json:"upload_url"`
	Fields     map[string]string `json:"fields"`
	ObjectPath string            `json:"object_path"`
}

func (h *IngestHandler) Init(w http.ResponseWriter, r *http.Request) {
	var req initRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	objectKey := uuid.NewString()
	if req.Filename != "" {
		objectKey = objectKey + "-" + req.Filename
	}

	url, fields, err := h.Storage.PresignedUploadPolicy(r.Context(), objectKey, 15*time.Minute)
	if err != nil {
		http.Error(w, "failed to create upload url", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, initResponse{UploadURL: url, Fields: fields, ObjectPath: objectKey})
}

type confirmRequest struct {
	ObjectPath string `json:"object_path"`
}

type confirmResponse struct {
	HashID string `json:"hash_id"`
	Status string `json:"status"`
}

func (h *IngestHandler) Confirm(w http.ResponseWriter, r *http.Request) {
	var req confirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ObjectPath == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	hashID, err := h.Storage.HashObject(r.Context(), req.ObjectPath)
	if err != nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	inserted, err := db.InsertIfAbsent(h.DB, hashID, req.ObjectPath)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !inserted {
		writeJSON(w, http.StatusConflict, confirmResponse{HashID: hashID, Status: "duplicate"})
		return
	}

	// Best-effort inline publish. If this fails, the row stays unpublished
	// and the outbox poller (Task 12) retries it — the client still gets a
	// success response because the DB row is the durable proof of confirm.
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.Publisher.Publish(ctx, hashID, req.ObjectPath, "application/pdf"); err == nil {
		_ = db.MarkPublished(h.DB, hashID)
	}

	writeJSON(w, http.StatusOK, confirmResponse{HashID: hashID, Status: "uploaded"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
