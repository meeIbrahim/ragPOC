package ask

import (
	"context"
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
