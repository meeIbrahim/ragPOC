package ask

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"center-service/internal/domain"
)

// Store resolves upload ids to their documents. An id absent from the result is unknown.
type Store interface {
	ResolveUploads(ctx context.Context, ids []string) ([]domain.UploadRef, error)
}

// Limits are [rag].max_query_chars and [rag].max_document_ids. Python enforces the same
// limits; checking them here turns bad input into a 400 before any stream starts.
type Limits struct {
	MaxQueryChars  int
	MaxDocumentIDs int
}

// BadRequestError is input the client must fix. Message is safe to show to the user.
type BadRequestError struct {
	Message string
}

func (e *BadRequestError) Error() string { return e.Message }

// maxEchoedIDRunes caps how much of a client-supplied id a 400 body repeats back.
const maxEchoedIDRunes = 64

func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes]) + "..."
}

func badRequest(format string, args ...any) error {
	return &BadRequestError{Message: fmt.Sprintf(format, args...)}
}

// Plan is what an ask will do. With ShortCircuit set the selection holds no INDEXED document
// and the RAG service must not be called. Otherwise Hashes is the document_ids to send; empty
// means search every indexed document.
type Plan struct {
	ShortCircuit bool
	Hashes       []string
}

type Service struct {
	store  Store
	limits Limits
}

func NewService(store Store, limits Limits) *Service {
	return &Service{store: store, limits: limits}
}

// Prepare validates an ask and turns the selected upload ids into content hashes. Only INDEXED
// documents are kept: an upload that is not hashed yet, or whose document is PROCESSING or
// FAILED, is dropped silently. It returns a *BadRequestError for input the client must fix, or
// another error when the store fails.
func (s *Service) Prepare(ctx context.Context, question string, uploadIDs []string) (Plan, error) {
	if strings.TrimSpace(question) == "" {
		return Plan{}, badRequest("question is required")
	}
	// Python counts characters (code points) too, so both sides agree on the limit.
	if utf8.RuneCountInString(question) > s.limits.MaxQueryChars {
		return Plan{}, badRequest("question is longer than %d characters", s.limits.MaxQueryChars)
	}
	if len(uploadIDs) > s.limits.MaxDocumentIDs {
		return Plan{}, badRequest("at most %d upload_ids may be selected", s.limits.MaxDocumentIDs)
	}
	if len(uploadIDs) == 0 {
		return Plan{}, nil
	}

	refs, err := s.store.ResolveUploads(ctx, uploadIDs)
	if err != nil {
		return Plan{}, fmt.Errorf("ask: resolve uploads: %w", err)
	}
	byID := make(map[string]domain.UploadRef, len(refs))
	for _, r := range refs {
		byID[r.UploadID] = r
	}

	var hashes []string
	seen := make(map[string]bool)
	for _, id := range uploadIDs {
		ref, ok := byID[id]
		if !ok {
			return Plan{}, badRequest("unknown upload_id: %q", truncate(id, maxEchoedIDRunes))
		}
		if ref.DocStatus == domain.DocumentIndexed && !seen[ref.Hash] {
			seen[ref.Hash] = true
			hashes = append(hashes, ref.Hash)
		}
	}
	// An empty document_ids means "search everything", so a selection with nothing INDEXED
	// must not reach the RAG service.
	if len(hashes) == 0 {
		return Plan{ShortCircuit: true}, nil
	}
	return Plan{Hashes: hashes}, nil
}
