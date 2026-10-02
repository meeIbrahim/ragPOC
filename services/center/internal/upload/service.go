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
	UpdateIntentToUploaded(ctx context.Context, id string, uploadedAt *time.Time) (int64, error)
	UpdateIntentToHashed(ctx context.Context, id string, hash *string, claimedBy *string) (int64, error)
	UpdateIntentToHashing(ctx context.Context, id string, claimedBy *string, claimedAt *time.Time) (int64, error)
	UpdateIntentToExpired(ctx context.Context, id string, now *time.Time) (int64, error)
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
	now := time.Now()
	id := uuid.NewString()
	objectPath := id

	uploadURL, err := s.minio.PresignedUploadURL(ctx, objectPath, expirationDuration)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create url: %v", err)
	}
	intent := &Intent{
		ID:             id,
		FileName:       fileName,
		Hash:           nil,
		TempBucketPath: &objectPath,
		CreatedAt:      now,
		ExpiresAt:      now.Add(expirationDuration),
		UploadedAt:     nil,
		Status:         StateWaiting,
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
var ErrIntentFailed = errors.New("upload intent failed")
var ErrIntentNotFound = errors.New("upload intent not found")
var ErrIntentUnknown = errors.New("upload intent in unknown state")

func (s *Service) ConfirmUpload(ctx context.Context, id string) (*Intent, error) {
	now := time.Now()
	rows, err := s.repo.UpdateIntentToUploaded(ctx, id, &now)
	if err != nil {
		return nil, fmt.Errorf("update upload_intent to uploaded: %w", err)
	}
	if rows == 0 {
		return s.resolveLostGate(ctx, id, now)
	}
	intent, err := s.repo.GetIntent(ctx, id)
	if err != nil {
		if errors.Is(err, ErrIntentNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("get upload_intent: %w", err)
	}
	intent.Status = StateUploaded
	intent.UploadedAt = &now
	return intent, nil
}

func (s *Service) resolveLostGate(ctx context.Context, id string, now time.Time) (*Intent, error) {
	intent, err := s.repo.GetIntent(ctx, id)
	if err != nil {
		if errors.Is(err, ErrIntentNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("get upload_intent: %w", err)
	}
	switch intent.Status {
	case StateExpired:
		return intent, ErrIntentExpired
	case StateUnknown:
		return intent, ErrIntentUnknown
	case StateHashed, StateHashing, StateUploaded:
		return intent, nil
	case StateFailed:
		return intent, ErrIntentFailed
	default:
		rows, err := s.repo.UpdateIntentToExpired(ctx, id, &now)
		if err != nil {
			return nil, fmt.Errorf("update upload_intent to expired: %w", err)
		}
		if rows == 0 {
			return s.resolveLostGate(ctx, id, now)
		}
		intent.Status = StateExpired
		return intent, ErrIntentExpired
	}
}
