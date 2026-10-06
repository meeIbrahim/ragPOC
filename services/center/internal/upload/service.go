package upload

import (
	"center-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"
)

type IntentStore interface {
	CreateIntent(ctx context.Context, i *domain.Intent) error
	GetIntent(ctx context.Context, id string) (*domain.Intent, error)
	UpdateIntentToUploaded(ctx context.Context, id string, uploadedAt *time.Time) (int64, error)
	UpdateIntentToExpired(ctx context.Context, id string, now *time.Time) (int64, error)
}

type Store interface {
	IntentStore
}

type Blob interface {
	PresignedUploadURL(ctx context.Context, objectPath string, expirationDuration time.Duration) (*url.URL, error)
}

type Service struct {
	repo    Store
	storage Blob
	logger  *slog.Logger
}

func NewService(repo Store, storage Blob, logger *slog.Logger) *Service {
	return &Service{repo: repo, storage: storage, logger: logger}
}

func (s *Service) CreateIntent(ctx context.Context, fileName string) (*domain.Intent, *url.URL, error) {
	expirationDuration := 2 * time.Hour
	now := time.Now().UTC()
	id := uuid.NewString()
	objectPath := id

	uploadURL, err := s.storage.PresignedUploadURL(ctx, objectPath, expirationDuration)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create url: %v", err)
	}
	intent := &domain.Intent{
		ID:             id,
		FileName:       fileName,
		Hash:           nil,
		TempBucketPath: &objectPath,
		CreatedAt:      now,
		ExpiresAt:      now.Add(expirationDuration),
		UploadedAt:     nil,
		Status:         domain.IntentWaiting,
	}

	if err := s.repo.CreateIntent(ctx, intent); err != nil {
		return nil, nil, fmt.Errorf("create upload intent: %w", err)
	}
	return intent, uploadURL, nil
}

func (s *Service) Get(ctx context.Context, id string) (*domain.Intent, error) {
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

func (s *Service) ConfirmUpload(ctx context.Context, id string) (*domain.Intent, error) {
	now := time.Now().UTC()
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
	intent.Status = domain.IntentUploaded
	intent.UploadedAt = &now
	return intent, nil
}

func (s *Service) resolveLostGate(ctx context.Context, id string, now time.Time) (*domain.Intent, error) {
	intent, err := s.repo.GetIntent(ctx, id)
	if err != nil {
		if errors.Is(err, ErrIntentNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("get upload_intent: %w", err)
	}
	switch intent.Status {
	case domain.IntentExpired:
		return intent, ErrIntentExpired
	case domain.IntentUnknown:
		return intent, ErrIntentUnknown
	case domain.IntentHashed, domain.IntentHashing, domain.IntentUploaded:
		return intent, nil
	case domain.IntentFailed:
		return intent, ErrIntentFailed
	default:
		rows, err := s.repo.UpdateIntentToExpired(ctx, id, &now)
		if err != nil {
			return nil, fmt.Errorf("update upload_intent to expired: %w", err)
		}
		if rows == 0 {
			return s.resolveLostGate(ctx, id, now)
		}
		intent.Status = domain.IntentExpired
		return intent, ErrIntentExpired
	}
}
