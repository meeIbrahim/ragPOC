package hashing

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"center-service/internal/domain"
)

// fakeIntent is the state of one upload_intent row in fakeStore.
type fakeIntent struct {
	status    domain.IntentState
	claimedBy string
	retries   int
	hash      string
	tempPath  *string
}

// fakeStore is an in-memory upload_intent/document_store with the same claim and fence rules
// as the SQL, so tests assert where an intent ended up rather than which calls were made.
type fakeStore struct {
	mu        sync.Mutex
	intents   map[string]*fakeIntent
	order     []string // fetch order
	documents map[string]*domain.Document
	events    int // outbox rows queued

	errs map[string]error // method name -> error to return

	// onClaim runs inside UpdateIntentToHashing before the CAS; a test uses it to let
	// another worker win the race.
	onClaim func(id string)
	// onStartIngestion runs before StartIngestion's fence check.
	onStartIngestion func(id string)

	recoverCalls []recoverCall
	claims       int // claim attempts, cancelled ones included
}

type recoverCall struct {
	cutoff     time.Time
	maxRetries int
}

func newFakeStore() *fakeStore {
	return &fakeStore{intents: map[string]*fakeIntent{}, documents: map[string]*domain.Document{}, errs: map[string]error{}}
}

func (f *fakeStore) addUploaded(id string, tempPath *string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intents[id] = &fakeIntent{status: domain.IntentUploaded, tempPath: tempPath}
	f.order = append(f.order, id)
}

func (f *fakeStore) intent(id string) fakeIntent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.intents[id]
}

func (f *fakeStore) GetIntentsForHashing(ctx context.Context, limit int) ([]*domain.Intent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["GetIntentsForHashing"]; err != nil {
		return nil, err
	}
	var out []*domain.Intent
	for _, id := range f.order {
		if in := f.intents[id]; in.status == domain.IntentUploaded && len(out) < limit {
			out = append(out, &domain.Intent{ID: id, FileName: id + ".txt", TempBucketPath: in.tempPath, Status: in.status})
		}
	}
	return out, nil
}

func (f *fakeStore) UpdateIntentToHashing(ctx context.Context, id string, claimedBy *string, _ *time.Time) (int64, error) {
	f.mu.Lock()
	f.claims++
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.onClaim != nil {
		f.onClaim(id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["UpdateIntentToHashing:"+id]; err != nil {
		return 0, err
	}
	in := f.intents[id]
	if in.status != domain.IntentUploaded {
		return 0, nil
	}
	in.status, in.claimedBy = domain.IntentHashing, *claimedBy
	return 1, nil
}

// owned reports whether worker holds the HASHING claim on id. Callers hold f.mu.
func (f *fakeStore) owned(id, worker string) bool {
	in := f.intents[id]
	return in.status == domain.IntentHashing && in.claimedBy == worker
}

func (f *fakeStore) MarkIntentFailed(ctx context.Context, id string, claimedBy string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["MarkIntentFailed"]; err != nil {
		return 0, err
	}
	if !f.owned(id, claimedBy) {
		return 0, nil
	}
	in := f.intents[id]
	in.status, in.claimedBy = domain.IntentFailed, ""
	return 1, nil
}

func (f *fakeStore) ReleaseIntentClaim(ctx context.Context, id string, claimedBy string, maxRetries int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ReleaseIntentClaim"]; err != nil {
		return 0, err
	}
	if !f.owned(id, claimedBy) {
		return 0, nil
	}
	in := f.intents[id]
	in.retries++
	in.claimedBy = ""
	in.status = domain.IntentUploaded
	if in.retries >= maxRetries {
		in.status = domain.IntentFailed
	}
	return 1, nil
}

// RecoverStaleIntents only records its arguments: the sweeper tests pin the cutoff and limit,
// and the recovery SQL itself is covered by the store tests.
func (f *fakeStore) RecoverStaleIntents(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoverCalls = append(f.recoverCalls, recoverCall{cutoff: cutoff, maxRetries: maxRetries})
	return 0, f.errs["RecoverStaleIntents"]
}

func (f *fakeStore) recovers() []recoverCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recoverCall(nil), f.recoverCalls...)
}

func (f *fakeStore) StartIngestion(ctx context.Context, document *domain.Document, intentID string, claimedBy string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.onStartIngestion != nil {
		f.onStartIngestion(intentID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["StartIngestion"]; err != nil {
		return err
	}
	if !f.owned(intentID, claimedBy) {
		return domain.ErrLostClaim
	}
	if _, ok := f.documents[document.Hash]; !ok {
		d := *document
		f.documents[document.Hash] = &d
		f.events++
	}
	in := f.intents[intentID]
	in.status, in.hash = domain.IntentHashed, document.Hash
	return nil
}

// steal makes worker the owner of id's claim, as if the recovery sweep had handed it on.
func (f *fakeStore) steal(id, worker string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intents[id].claimedBy = worker
}

// fakeBlob is an in-memory upload bucket and ingestion bucket.
type fakeBlob struct {
	mu        sync.Mutex
	uploads   map[string][]byte
	ingestion map[string][]byte
	errs      map[string]error
	// blockHash makes HashUploadObject wait until its ctx ends, like a stalled download.
	blockHash bool
	// onHash runs at the start of HashUploadObject.
	onHash func(objectName string)
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{uploads: map[string][]byte{}, ingestion: map[string][]byte{}, errs: map[string]error{}}
}

func (b *fakeBlob) put(name string, content []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uploads[name] = content
}

func (b *fakeBlob) ExistsInUploadBucket(ctx context.Context, objectName string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.errs["ExistsInUploadBucket"]; err != nil {
		return false, err
	}
	_, ok := b.uploads[objectName]
	return ok, nil
}

func (b *fakeBlob) HashUploadObject(ctx context.Context, objectName string) (string, int64, error) {
	if b.onHash != nil {
		b.onHash(objectName)
	}
	if b.blockHash {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.errs["HashUploadObject"]; err != nil {
		return "", 0, err
	}
	content, ok := b.uploads[objectName]
	if !ok {
		return "", 0, errors.New("no such object")
	}
	sum, err := Sum256(bytes.NewReader(content))
	return sum, int64(len(content)), err
}

func (b *fakeBlob) MigrateToIngestion(ctx context.Context, objectName string, hash string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.errs["MigrateToIngestion"]; err != nil {
		return "", err
	}
	b.ingestion[hash] = b.uploads[objectName]
	delete(b.uploads, objectName)
	return hash[:2] + "/" + hash[2:4] + "/" + hash, nil
}
