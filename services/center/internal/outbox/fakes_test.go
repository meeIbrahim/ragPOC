package outbox

import (
	"context"
	"sync"
	"time"

	"center-service/internal/domain"
	"center-service/internal/events"
)

// fakeRow is the state of one outbox row in fakeStore.
type fakeRow struct {
	hash      string
	status    domain.OutboxStatus
	claimedBy string
	retries   int
}

// fakeStore is an in-memory outbox and document_store with the same claim and fence rules as
// the SQL, so tests assert where a row ended up rather than which calls were made. Every
// method fails on a done context, as ExecContext does.
type fakeStore struct {
	mu        sync.Mutex
	rows      map[int64]*fakeRow
	order     []int64
	documents map[string]*domain.Document

	errs map[string]error // method name -> error to return

	// onClaim runs inside ClaimOutboxMessage before the CAS; a test uses it to let another
	// worker win the race.
	onClaim func(id int64)

	recoverCalls []recoverCall
}

type recoverCall struct {
	cutoff     time.Time
	maxRetries int
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[int64]*fakeRow{}, documents: map[string]*domain.Document{}, errs: map[string]error{}}
}

// addPending seeds a PENDING row for a PROCESSING document with the given retries.
func (f *fakeStore) addPending(id int64, hash string, docRetries int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[id] = &fakeRow{hash: hash, status: domain.OutboxPending}
	f.order = append(f.order, id)
	f.documents[hash] = &domain.Document{
		Hash: hash, PermBucketPath: hash[:2] + "/" + hash[2:4] + "/" + hash, FileName: "a.pdf", Size: 42,
		Status: domain.DocumentProcessing, Retries: docRetries,
	}
}

func (f *fakeStore) row(id int64) fakeRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.rows[id]
}

func (f *fakeStore) FetchCandidates(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["FetchCandidates"]; err != nil {
		return nil, err
	}
	var out []*domain.OutboxMessage
	for _, id := range f.order {
		if r := f.rows[id]; r.status == domain.OutboxPending && len(out) < limit {
			out = append(out, &domain.OutboxMessage{Id: id, Hash: r.hash, Status: r.status, Retries: r.retries})
		}
	}
	return out, nil
}

func (f *fakeStore) ClaimOutboxMessage(ctx context.Context, claimedBy string, id int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if f.onClaim != nil {
		f.onClaim(id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ClaimOutboxMessage"]; err != nil {
		return false, err
	}
	r := f.rows[id]
	if r.status != domain.OutboxPending {
		return false, nil
	}
	r.status, r.claimedBy = domain.OutboxClaimed, claimedBy
	return true, nil
}

// owned reports whether worker holds the CLAIMED row id. Callers hold f.mu.
func (f *fakeStore) owned(id int64, worker string) bool {
	r := f.rows[id]
	return r.status == domain.OutboxClaimed && r.claimedBy == worker
}

func (f *fakeStore) MarkPublished(ctx context.Context, id int64, claimedBy string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["MarkPublished"]; err != nil {
		return false, err
	}
	if !f.owned(id, claimedBy) {
		return false, nil
	}
	r := f.rows[id]
	r.status, r.claimedBy = domain.OutboxPublished, ""
	return true, nil
}

func (f *fakeStore) RecoverStaleOutbox(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoverCalls = append(f.recoverCalls, recoverCall{cutoff: cutoff, maxRetries: maxRetries})
	return 0, f.errs["RecoverStaleOutbox"]
}

func (f *fakeStore) recovers() []recoverCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recoverCall(nil), f.recoverCalls...)
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

func (f *fakeStore) ReleaseClaim(ctx context.Context, id int64, claimedBy string, maxRetries int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ReleaseClaim"]; err != nil {
		return false, err
	}
	if !f.owned(id, claimedBy) {
		return false, nil
	}
	r := f.rows[id]
	r.retries++
	r.claimedBy = ""
	r.status = domain.OutboxPending
	if r.retries >= maxRetries {
		r.status = domain.OutboxBlocked
	}
	return true, nil
}

func (f *fakeStore) MarkBlocked(ctx context.Context, id int64, claimedBy string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["MarkBlocked"]; err != nil {
		return false, err
	}
	if !f.owned(id, claimedBy) {
		return false, nil
	}
	r := f.rows[id]
	r.status, r.claimedBy = domain.OutboxBlocked, ""
	return true, nil
}

// fakeProducer records published events; it fails on a done context like a Redis call.
type fakeProducer struct {
	mu        sync.Mutex
	published []events.StreamIngestionEvent
	err       error
	// blockPublish makes PublishIngestionEvent wait until its ctx ends, like a hung XADD.
	blockPublish bool
	// onPublish runs after a successful publish.
	onPublish func()
}

func (p *fakeProducer) PublishIngestionEvent(ctx context.Context, event *events.StreamIngestionEvent) (string, error) {
	if p.blockPublish {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	if p.err != nil {
		p.mu.Unlock()
		return "", p.err
	}
	p.published = append(p.published, *event)
	p.mu.Unlock()
	if p.onPublish != nil {
		p.onPublish()
	}
	return "1-0", nil
}

func (p *fakeProducer) events() []events.StreamIngestionEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]events.StreamIngestionEvent(nil), p.published...)
}
