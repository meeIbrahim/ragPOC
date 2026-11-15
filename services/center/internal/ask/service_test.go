package ask

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"center-service/internal/domain"
)

var testLimits = Limits{MaxQueryChars: 10, MaxDocumentIDs: 3}

func newTestService(store *fakeStore) *Service {
	return NewService(store, testLimits)
}

func TestPrepareRejectsBadInput(t *testing.T) {
	store := &fakeStore{rows: map[string]domain.UploadRef{"known": indexed("known", "h1")}}
	tests := []struct {
		name     string
		question string
		ids      []string
		wantMsg  string
	}{
		{"empty question", "", nil, "question"},
		{"whitespace question (E1)", " \t\n ", nil, "question"},
		// 11 runes, 22 bytes: the limit counts characters, not bytes (E2).
		{"question one rune over the limit", strings.Repeat("é", 11), nil, "10"},
		{"too many ids, counted before dedupe", "q?", []string{"known", "known", "known", "known"}, "3"},
		{"unknown id (E3)", "q?", []string{"known", "nope", "also-nope"}, "nope"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestService(store).Prepare(context.Background(), tc.question, tc.ids)
			var bad *BadRequestError
			if !errors.As(err, &bad) {
				t.Fatalf("err = %v, want *BadRequestError: bad input must be a 400 before any stream", err)
			}
			if !strings.Contains(bad.Message, tc.wantMsg) {
				t.Errorf("message %q does not mention %q", bad.Message, tc.wantMsg)
			}
		})
	}
}

func TestPrepareNamesFirstUnknownID(t *testing.T) {
	store := &fakeStore{rows: map[string]domain.UploadRef{"known": indexed("known", "h1")}}
	_, err := newTestService(store).Prepare(context.Background(), "q?", []string{"known", "first-bad", "second-bad"})
	var bad *BadRequestError
	if !errors.As(err, &bad) || !strings.Contains(bad.Message, "first-bad") || strings.Contains(bad.Message, "second-bad") {
		t.Fatalf("err = %v, want a 400 naming only the first unknown id", err)
	}
}

func TestPrepareAcceptsLimits(t *testing.T) {
	store := &fakeStore{rows: map[string]domain.UploadRef{
		"a": indexed("a", "h1"), "b": indexed("b", "h2"), "c": indexed("c", "h3"),
	}}
	plan, err := newTestService(store).Prepare(context.Background(), strings.Repeat("é", 10), []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("question at max_query_chars and ids at max_document_ids must pass: %v", err)
	}
	if plan.ShortCircuit || !slices.Equal(plan.Hashes, []string{"h1", "h2", "h3"}) {
		t.Errorf("plan = %+v", plan)
	}
}

func TestPrepareEmptySelectionSearchesAll(t *testing.T) {
	store := &fakeStore{}
	plan, err := newTestService(store).Prepare(context.Background(), "q?", nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ShortCircuit || len(plan.Hashes) != 0 {
		t.Errorf("plan = %+v, want no hashes and no short-circuit: empty document_ids searches everything", plan)
	}
	if store.calls != 0 {
		t.Errorf("store called %d times for an empty selection", store.calls)
	}
}

func TestPrepareKeepsOnlyIndexedHashesDeduped(t *testing.T) {
	store := &fakeStore{rows: map[string]domain.UploadRef{
		"idx-1":      indexed("idx-1", "h1"),
		"idx-1-dup":  indexed("idx-1-dup", "h1"), // E5: second upload of the same content
		"processing": {UploadID: "processing", Hash: "h2", DocStatus: domain.DocumentProcessing},
		"failed":     {UploadID: "failed", Hash: "h3", DocStatus: domain.DocumentFailed},
		"unhashed":   {UploadID: "unhashed"},
		"idx-2":      indexed("idx-2", "h4"),
	}}
	ids := []string{"processing", "idx-1", "failed", "idx-1-dup", "unhashed", "idx-2"}
	plan, err := NewService(store, Limits{MaxQueryChars: 10, MaxDocumentIDs: 10}).Prepare(context.Background(), "q?", ids)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ShortCircuit {
		t.Fatal("short-circuit with INDEXED documents selected")
	}
	if !slices.Equal(plan.Hashes, []string{"h1", "h4"}) {
		t.Errorf("hashes = %v, want [h1 h4]: only INDEXED, deduped, in selection order (E5, E6)", plan.Hashes)
	}
}

// E4: a selection with nothing INDEXED must never become an empty document_ids (search all).
func TestPrepareShortCircuitsWhenNothingIndexed(t *testing.T) {
	store := &fakeStore{rows: map[string]domain.UploadRef{
		"processing": {UploadID: "processing", Hash: "h2", DocStatus: domain.DocumentProcessing},
		"unhashed":   {UploadID: "unhashed"},
	}}
	plan, err := newTestService(store).Prepare(context.Background(), "q?", []string{"processing", "unhashed"})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.ShortCircuit {
		t.Fatalf("plan = %+v: selection had no INDEXED document but would call the RAG service, and an empty document_ids searches everything", plan)
	}
}

func TestPrepareStoreErrorIsInternal(t *testing.T) {
	store := &fakeStore{err: errors.New("disk on fire")}
	_, err := newTestService(store).Prepare(context.Background(), "q?", []string{"a"})
	if err == nil {
		t.Fatal("store error swallowed")
	}
	var bad *BadRequestError
	if errors.As(err, &bad) {
		t.Fatalf("store error reported as bad request (%v): it must be a 500", err)
	}
}
