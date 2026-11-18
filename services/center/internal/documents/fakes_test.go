package documents

import (
	"context"

	"center-service/internal/domain"
)

// fakeLister returns fixed entries, or err from every call. It records the limit it was given.
type fakeLister struct {
	entries   []domain.DocumentEntry
	truncated bool
	err       error

	gotLimit int
}

func (f *fakeLister) ListDocuments(_ context.Context, limit int) ([]domain.DocumentEntry, bool, error) {
	f.gotLimit = limit
	if f.err != nil {
		return nil, false, f.err
	}
	return f.entries, f.truncated, nil
}
