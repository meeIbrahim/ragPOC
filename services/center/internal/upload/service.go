package upload

import (
	"center-service/internal/storage/minio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"
)

type IntentRepository interface {
	CreateIntent(ctx context.Context, i *Intent) error
	GetIntent(ctx context.Context, id string) (*Intent, error)
	UpdateIntentStatus(ctx context.Context, id string, status State, completedAt *time.Time) error
}

type DocumentRepository interface {
	GetDocumentByHash(ctx context.Context, hash string) (*DocumentStorage, error)
	CreateDocument(ctx context.Context, d *DocumentStorage) error
}

type Repository interface {
	IntentRepository
	DocumentRepository
}

type Service struct {
	repo   Repository
	minio  *minio.Client
	logger *slog.Logger
}

func NewService(repo Repository, minio *minio.Client, logger *slog.Logger) *Service {
	return &Service{repo: repo, minio: minio, logger: logger}
}

func (s *Service) CreateIntent(ctx context.Context, fileName string) (*Intent, *url.URL, error) {
	expirationDuration := 2 * time.Hour
	id := uuid.NewString()
	objectPath := id

	uploadURL, err := s.minio.PresignedUploadURL(ctx, objectPath, expirationDuration)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create url: %v", err)
	}
	intent := &Intent{
		ID:          id,
		FileName:    fileName,
		ObjectPath:  &objectPath,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(expirationDuration),
		CompletedAt: nil,
		Status:      StateWaiting,
	}

	if err := s.repo.CreateIntent(ctx, intent); err != nil {
		return nil, nil, fmt.Errorf("create upload intent: %w", err)
	}
	return intent, uploadURL, nil
}

func (s *Service) Get(ctx context.Context, id string) (*Intent, error) {
	intent, err := s.repo.GetIntent(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get upload_intent: %w", err)
	}
	return intent, nil
}

// ErrIntentExpired is returned by ConfirmUpload when the intent's presigned URL has expired.
var ErrIntentExpired = errors.New("upload intent expired")

// ConfirmUpload hashes the uploaded object, checks it against the document store, and
// either migrates it into the ingestion bucket or discards it as a duplicate. It is
// idempotent: calling it again on an already-completed or already-duplicate intent
// just returns the intent unchanged.
func (s *Service) ConfirmUpload(ctx context.Context, id string) (*Intent, error) {
	intent, err := s.repo.GetIntent(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get upload_intent: %w", err)
	}

	switch intent.Status {
	case StateCompleted, StateDuplicate:
		return intent, nil
	case StateExpired, StateFailed, StateVerificationFailed:
		return nil, fmt.Errorf("upload intent %s is in terminal state %s", id, intent.Status)
	}

	if time.Now().After(intent.ExpiresAt) {
		if err := s.repo.UpdateIntentStatus(ctx, id, StateExpired, nil); err != nil {
			s.logger.Error("mark upload_intent expired failed", "id", id, "err", err)
		}
		return nil, ErrIntentExpired
	}

	objectPath := *intent.ObjectPath
	hash, size, err := s.minio.HashUploadObject(ctx, objectPath)
	if err != nil {
		return nil, fmt.Errorf("hash uploaded object: %w", err)
	}

	now := time.Now()

	existing, err := s.repo.GetDocumentByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("check existing document: %w", err)
	}
	if existing != nil {
		if err := s.minio.RemoveFromUploadBucket(ctx, objectPath); err != nil {
			return nil, fmt.Errorf("remove duplicate object: %w", err)
		}
		if err := s.repo.UpdateIntentStatus(ctx, id, StateDuplicate, &now); err != nil {
			return nil, fmt.Errorf("mark upload_intent duplicate: %w", err)
		}
		intent.Status = StateDuplicate
		intent.CompletedAt = &now
		return intent, nil
	}

	destPath, err := s.minio.MigrateToIngestion(ctx, objectPath, hash)
	if err != nil {
		return nil, fmt.Errorf("migrate to ingestion bucket: %w", err)
	}
	doc := &DocumentStorage{
		ContentHash: hash,
		ObjectPath:  destPath,
		FileName:    intent.FileName,
		CreatedAt:   now,
		Size:        size,
	}
	if err := s.repo.CreateDocument(ctx, doc); err != nil {
		return nil, fmt.Errorf("create document: %w", err)
	}
	if err := s.repo.UpdateIntentStatus(ctx, id, StateCompleted, &now); err != nil {
		return nil, fmt.Errorf("mark upload_intent completed: %w", err)
	}
	intent.Status = StateCompleted
	intent.CompletedAt = &now
	return intent, nil
}
