package domain

import "errors"

// Sentinels shared by the stores and the feature packages. They live here so the sqlite
// store does not have to import upload or hashing to return them.
var (
	ErrIntentNotFound = errors.New("upload intent not found")
	// ErrLostClaim means another worker owns the row now; the caller must not touch it.
	ErrLostClaim = errors.New("lost claim")

	// ErrInvalidQuery means the RAG service rejected the question or the document selection
	// (empty, too long, too many document ids). The wrapped message is the service's own text,
	// for logs only.
	ErrInvalidQuery = errors.New("invalid query")
	// ErrRAGUnavailable means the RAG service, or something it depends on (Groq, Ollama,
	// Qdrant), could not answer: down, overloaded, or past [go.ask].deadline. Retrying later
	// may work.
	ErrRAGUnavailable = errors.New("rag service unavailable")
)
