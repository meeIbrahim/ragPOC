package upload

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"time"

	"center-service/internal/domain"
)

// fakeStore is an in-memory upload_intent/document_store with the same gate rules as the SQL,
// so tests assert the intent's end state rather than which calls were made.
type fakeStore struct {
	mu        sync.Mutex
	intents   map[string]*domain.Intent
	documents map[string]*domain.Document
	errs      map[string]error // method name -> error to return

	// beforeExpire runs inside UpdateIntentToExpired before the CAS; a test uses it to let a
	// concurrent confirm move the intent first.
	beforeExpire func(id string)
}

func newFakeStore() *fakeStore {
	return &fakeStore{intents: map[string]*domain.Intent{}, documents: map[string]*domain.Document{}, errs: map[string]error{}}
}

func (f *fakeStore) put(i domain.Intent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intents[i.ID] = &i
}

func (f *fakeStore) intent(id string) domain.Intent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.intents[id]
}

func (f *fakeStore) setStatus(id string, s domain.IntentState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intents[id].Status = s
}

func (f *fakeStore) CreateIntent(ctx context.Context, i *domain.Intent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["CreateIntent"]; err != nil {
		return err
	}
	copied := *i
	f.intents[i.ID] = &copied
	return nil
}

func (f *fakeStore) GetIntent(ctx context.Context, id string) (*domain.Intent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["GetIntent"]; err != nil {
		return nil, err
	}
	i, ok := f.intents[id]
	if !ok {
		return nil, domain.ErrIntentNotFound
	}
	copied := *i
	return &copied, nil
}

func (f *fakeStore) UpdateIntentToUploaded(ctx context.Context, id string, uploadedAt *time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["UpdateIntentToUploaded"]; err != nil {
		return 0, err
	}
	i, ok := f.intents[id]
	if !ok || i.Status != domain.IntentWaiting || !i.ExpiresAt.After(*uploadedAt) {
		return 0, nil
	}
	i.Status, i.UploadedAt = domain.IntentUploaded, uploadedAt
	return 1, nil
}

func (f *fakeStore) UpdateIntentToExpired(ctx context.Context, id string, now *time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.beforeExpire != nil {
		f.beforeExpire(id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["UpdateIntentToExpired"]; err != nil {
		return 0, err
	}
	i, ok := f.intents[id]
	if !ok || i.Status != domain.IntentWaiting || i.ExpiresAt.After(*now) {
		return 0, nil
	}
	i.Status = domain.IntentExpired
	return 1, nil
}

func (f *fakeStore) GetDocumentByHash(ctx context.Context, hash string) (*domain.Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["GetDocumentByHash"]; err != nil {
		return nil, err
	}
	d, ok := f.documents[hash]
	if !ok {
		return nil, nil
	}
	copied := *d
	return &copied, nil
}

type fakeBlob struct {
	err      error
	path     string
	duration time.Duration
}

func (b *fakeBlob) PresignedUploadURL(_ context.Context, objectPath string, expirationDuration time.Duration) (*url.URL, error) {
	if b.err != nil {
		return nil, b.err
	}
	b.path, b.duration = objectPath, expirationDuration
	return &url.URL{Scheme: "http", Host: "minio:9000", Path: "/upload-bucket/" + objectPath}, nil
}

var errBoom = errors.New("boom")
