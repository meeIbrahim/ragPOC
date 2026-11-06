package upload

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"center-service/internal/domain"
)

const docHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

var (
	discard = slog.New(slog.NewTextHandler(io.Discard, nil))
	now     = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
)

func newTestService(store *fakeStore, blob *fakeBlob) *Service {
	s := NewService(store, blob, discard)
	s.now = func() time.Time { return now }
	return s
}

// waiting is an intent inside its upload window unless expiresAt says otherwise.
func waiting(id string, expiresAt time.Time) domain.Intent {
	return domain.Intent{ID: id, FileName: id + ".pdf", Status: domain.IntentWaiting, CreatedAt: now.Add(-time.Hour), ExpiresAt: expiresAt}
}

// ---- CreateIntent

func TestCreateIntentStoresAWaitingIntentWithAPresignedURL(t *testing.T) {
	store, blob := newFakeStore(), &fakeBlob{}

	intent, u, err := newTestService(store, blob).CreateIntent(context.Background(), "report.pdf")
	if err != nil {
		t.Fatal(err)
	}
	stored := store.intent(intent.ID)
	if stored.Status != domain.IntentWaiting || stored.FileName != "report.pdf" ||
		!stored.CreatedAt.Equal(now) || !stored.ExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Errorf("stored intent = %+v; want WAITING, report.pdf, created now, expiring now + 2h (the presign window)", stored)
	}
	if stored.TempBucketPath == nil || *stored.TempBucketPath != intent.ID || blob.path != intent.ID {
		t.Errorf("temp path = %v, presigned path = %q; want both to be the intent id %s", stored.TempBucketPath, blob.path, intent.ID)
	}
	if blob.duration != 2*time.Hour || u == nil {
		t.Errorf("presign duration = %v, url = %v; want a 2h URL, matching expires_at", blob.duration, u)
	}
}

func TestCreateIntentPropagatesFailures(t *testing.T) {
	t.Run("presign", func(t *testing.T) {
		store := newFakeStore()
		if _, _, err := newTestService(store, &fakeBlob{err: errBoom}).CreateIntent(context.Background(), "a.pdf"); err == nil {
			t.Error("err = nil; want the presign failure")
		}
		if len(store.intents) != 0 {
			t.Errorf("stored %d intents; want none when no URL could be issued", len(store.intents))
		}
	})
	t.Run("store", func(t *testing.T) {
		store := newFakeStore()
		store.errs["CreateIntent"] = errBoom
		if _, _, err := newTestService(store, &fakeBlob{}).CreateIntent(context.Background(), "a.pdf"); !errors.Is(err, errBoom) {
			t.Errorf("err = %v; want the store failure wrapped", err)
		}
	})
}

// ---- ConfirmUpload

func TestConfirmUploadPassesTheGateOnce(t *testing.T) {
	store := newFakeStore()
	store.put(waiting("i1", now.Add(time.Hour)))

	got, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.IntentUploaded || got.UploadedAt == nil || !got.UploadedAt.Equal(now) {
		t.Errorf("returned %s/uploaded_at=%v; want UPLOADED at %v", got.Status, got.UploadedAt, now)
	}
	if s := store.intent("i1"); s.Status != domain.IntentUploaded {
		t.Errorf("stored status = %s; want UPLOADED", s.Status)
	}
}

// A client retrying confirm after the gate passed must get success, whatever the hash worker
// has done since, and the intent must not change.
func TestConfirmUploadIsIdempotentAfterTheGate(t *testing.T) {
	for _, st := range []domain.IntentState{domain.IntentUploaded, domain.IntentHashing, domain.IntentHashed} {
		t.Run(st.String(), func(t *testing.T) {
			store := newFakeStore()
			i := waiting("i1", now.Add(time.Hour))
			i.Status = st
			store.put(i)

			got, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1")
			if err != nil || got.Status != st {
				t.Errorf("repeat confirm = %v, %v; want the intent in %s and no error", got, err, st)
			}
			if s := store.intent("i1"); s.Status != st {
				t.Errorf("stored status = %s; want it unchanged at %s", s.Status, st)
			}
		})
	}
}

func TestConfirmUploadRefusals(t *testing.T) {
	cases := []struct {
		name    string
		intent  *domain.Intent
		wantErr error
		wantNow domain.IntentState
	}{
		{"expired in the store", ptrIntent(func(i *domain.Intent) { i.Status = domain.IntentExpired }), ErrIntentExpired, domain.IntentExpired},
		{"failed", ptrIntent(func(i *domain.Intent) { i.Status = domain.IntentFailed }), ErrIntentFailed, domain.IntentFailed},
		{"unknown state", ptrIntent(func(i *domain.Intent) { i.Status = domain.IntentUnknown }), ErrIntentUnknown, domain.IntentUnknown},
		{"unknown id", nil, ErrIntentNotFound, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStore()
			if c.intent != nil {
				store.put(*c.intent)
			}
			_, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1")
			if !errors.Is(err, c.wantErr) {
				t.Errorf("err = %v; want %v", err, c.wantErr)
			}
			if c.intent != nil && store.intent("i1").Status != c.wantNow {
				t.Errorf("stored status = %s; want %s", store.intent("i1").Status, c.wantNow)
			}
		})
	}
}

func ptrIntent(mod func(*domain.Intent)) *domain.Intent {
	i := waiting("i1", now.Add(time.Hour))
	mod(&i)
	return &i
}

// A WAITING intent past its window is expired lazily, by the confirm that finds it.
func TestConfirmUploadExpiresAWaitingIntentPastItsWindow(t *testing.T) {
	store := newFakeStore()
	store.put(waiting("i1", now.Add(-time.Second)))

	_, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1")
	if !errors.Is(err, ErrIntentExpired) {
		t.Errorf("err = %v; want ErrIntentExpired", err)
	}
	if s := store.intent("i1"); s.Status != domain.IntentExpired {
		t.Errorf("stored status = %s; want EXPIRED (lazy expiry on confirm)", s.Status)
	}
}

// The lazy expiry lost a race: a concurrent confirm (with an earlier clock) moved the intent
// first. The confirm must re-read and report the winner's state instead of expiring it.
func TestConfirmUploadReReadsWhenTheLazyExpiryLosesARace(t *testing.T) {
	store := newFakeStore()
	store.put(waiting("i1", now.Add(-time.Second)))
	store.beforeExpire = func(id string) { store.setStatus(id, domain.IntentUploaded) }

	got, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1")
	if err != nil || got.Status != domain.IntentUploaded {
		t.Errorf("confirm = %v, %v; want the UPLOADED intent the concurrent confirm produced, no error", got, err)
	}
	if s := store.intent("i1"); s.Status != domain.IntentUploaded {
		t.Errorf("stored status = %s; want UPLOADED: a lost expiry race must not overwrite the winner", s.Status)
	}
}

func TestConfirmUploadPropagatesStoreFailures(t *testing.T) {
	for _, method := range []string{"UpdateIntentToUploaded", "GetIntent", "UpdateIntentToExpired"} {
		t.Run(method, func(t *testing.T) {
			store := newFakeStore()
			store.put(waiting("i1", now.Add(-time.Second))) // expired, so every step runs
			store.errs[method] = errBoom

			if _, err := newTestService(store, &fakeBlob{}).ConfirmUpload(context.Background(), "i1"); !errors.Is(err, errBoom) {
				t.Errorf("err = %v; want the %s failure wrapped", err, method)
			}
		})
	}
}

// ---- GetUploadStatus

func TestGetUploadStatusReportsTheDocumentOnceHashed(t *testing.T) {
	for _, ds := range []domain.DocumentStatus{domain.DocumentProcessing, domain.DocumentIndexed, domain.DocumentFailed} {
		t.Run(string(ds), func(t *testing.T) {
			store := newFakeStore()
			i := waiting("i1", now.Add(-time.Hour)) // expiry no longer matters once hashed
			i.Status, i.Hash = domain.IntentHashed, ptr(docHash)
			store.put(i)
			store.documents[docHash] = &domain.Document{Hash: docHash, Status: ds}

			_, status, err := newTestService(store, &fakeBlob{}).GetUploadStatus(context.Background(), "i1")
			if err != nil || *status != string(ds) {
				t.Errorf("status = %v, %v; want the document's %s: once hashed, the poll follows the document", deref(status), err, ds)
			}
		})
	}
}

func TestGetUploadStatusBeforeTheDocumentExists(t *testing.T) {
	cases := []struct {
		name      string
		status    domain.IntentState
		expiresAt time.Time
		want      string
	}{
		{"waiting", domain.IntentWaiting, now.Add(time.Hour), "WAITING"},
		{"waiting past expiry", domain.IntentWaiting, now.Add(-time.Second), "EXPIRED"},
		{"waiting exactly at expiry", domain.IntentWaiting, now, "EXPIRED"}, // the confirm gate refuses here too
		{"uploaded", domain.IntentUploaded, now.Add(time.Hour), "UPLOADED"},
		{"hashing", domain.IntentHashing, now.Add(time.Hour), "PROCESSING"},
		{"hashed without a hash yet", domain.IntentHashed, now.Add(time.Hour), "PROCESSING"},
		{"expired", domain.IntentExpired, now.Add(-time.Hour), "EXPIRED"},
		{"failed", domain.IntentFailed, now.Add(time.Hour), "FAILED"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStore()
			i := waiting("i1", c.expiresAt)
			i.Status = c.status
			store.put(i)

			_, status, err := newTestService(store, &fakeBlob{}).GetUploadStatus(context.Background(), "i1")
			if err != nil || *status != c.want {
				t.Errorf("status = %v, %v; want %s", deref(status), err, c.want)
			}
		})
	}
}

func TestGetUploadStatusErrors(t *testing.T) {
	t.Run("unknown id", func(t *testing.T) {
		_, _, err := newTestService(newFakeStore(), &fakeBlob{}).GetUploadStatus(context.Background(), "nope")
		if !errors.Is(err, ErrIntentNotFound) {
			t.Errorf("err = %v; want ErrIntentNotFound", err)
		}
	})
	t.Run("unknown state", func(t *testing.T) {
		store := newFakeStore()
		i := waiting("i1", now.Add(time.Hour))
		i.Status = domain.IntentUnknown
		store.put(i)
		if _, _, err := newTestService(store, &fakeBlob{}).GetUploadStatus(context.Background(), "i1"); !errors.Is(err, ErrIntentUnknown) {
			t.Errorf("err = %v; want ErrIntentUnknown", err)
		}
	})
	t.Run("document read fails", func(t *testing.T) {
		store := newFakeStore()
		i := waiting("i1", now.Add(time.Hour))
		i.Status, i.Hash = domain.IntentHashed, ptr(docHash)
		store.put(i)
		store.errs["GetDocumentByHash"] = errBoom
		if _, _, err := newTestService(store, &fakeBlob{}).GetUploadStatus(context.Background(), "i1"); !errors.Is(err, errBoom) {
			t.Errorf("err = %v; want the document read failure", err)
		}
	})
}

func ptr[T any](v T) *T { return &v }

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
