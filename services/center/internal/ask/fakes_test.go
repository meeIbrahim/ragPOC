package ask

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"center-service/internal/domain"
)

// fakeStore resolves upload ids from rows keyed by upload id, like the real join: an id with no
// row is unknown. err makes every call fail.
type fakeStore struct {
	rows map[string]domain.UploadRef
	err  error

	mu    sync.Mutex
	calls int
}

func (f *fakeStore) ResolveUploads(_ context.Context, ids []string) ([]domain.UploadRef, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	seen := map[string]bool{}
	var out []domain.UploadRef
	for _, id := range ids {
		if r, ok := f.rows[id]; ok && !seen[id] {
			seen[id] = true
			out = append(out, r)
		}
	}
	return out, nil
}

func indexed(id, hash string) domain.UploadRef {
	return domain.UploadRef{UploadID: id, Hash: hash, DocStatus: domain.DocumentIndexed}
}

// fakeAsker scripts an answer stream. It yields parts, then endErr; with block set it instead
// waits for its context to end and yields context.Canceled, like the real client. blocked is
// closed once it starts waiting, and ctx is the context it was given.
type fakeAsker struct {
	parts  []domain.AnswerPart
	endErr error
	block  bool

	mu      sync.Mutex
	calls   int
	yields  int
	ctx     context.Context
	hashes  []string
	blocked chan struct{}
}

func newFakeAsker(parts ...domain.AnswerPart) *fakeAsker {
	return &fakeAsker{parts: parts, blocked: make(chan struct{})}
}

func (f *fakeAsker) AskStream(ctx context.Context, _ string, hashes []string) iter.Seq2[domain.AnswerPart, error] {
	f.mu.Lock()
	f.calls++
	f.ctx = ctx
	f.hashes = hashes
	f.mu.Unlock()
	return func(yield func(domain.AnswerPart, error) bool) {
		for _, p := range f.parts {
			f.mu.Lock()
			f.yields++
			f.mu.Unlock()
			if !yield(p, nil) {
				return
			}
		}
		if f.block {
			close(f.blocked)
			<-ctx.Done()
			yield(domain.AnswerPart{}, fmt.Errorf("fake: %w", context.Canceled))
			return
		}
		if f.endErr != nil {
			yield(domain.AnswerPart{}, f.endErr)
		}
	}
}

func (f *fakeAsker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeAsker) yieldCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.yields
}

func (f *fakeAsker) context() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ctx
}

func sources(srcs ...domain.Source) domain.AnswerPart {
	if srcs == nil {
		srcs = []domain.Source{}
	}
	return domain.AnswerPart{Sources: srcs}
}

func delta(text string) domain.AnswerPart {
	return domain.AnswerPart{Delta: text}
}
