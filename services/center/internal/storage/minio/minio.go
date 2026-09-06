// Package minio wraps the object store buckets used to stage and ingest documents.
package minio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"center-service/internal/config"
)

// Client provides upload, migration, and lookup operations over the upload and ingestion buckets.
type Client struct {
	inner           *miniogo.Client
	uploadBucket    string
	ingestionBucket string
}

// New builds a Client from the minio section of the service config.
func New(cfg config.MinioConfig) (*Client, error) {
	inner, err := miniogo.New(cfg.URL, &miniogo.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: new client: %w", err)
	}
	return &Client{
		inner:           inner,
		uploadBucket:    cfg.UploadBucket,
		ingestionBucket: cfg.IngestionBucket,
	}, nil
}

// IngestionObjectKey returns the sharded ingestion bucket key for sha256Hash,
// e.g. "ab/c9/abc9...".
func IngestionObjectKey(sha256Hash string) string {
	return fmt.Sprintf("%s/%s/%s", sha256Hash[:2], sha256Hash[2:4], sha256Hash)
}

// UploadToUploadBucket writes reader's contents to the upload bucket under objectName.
func (c *Client) UploadToUploadBucket(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) (miniogo.UploadInfo, error) {
	info, err := c.inner.PutObject(ctx, c.uploadBucket, objectName, reader, size, miniogo.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return miniogo.UploadInfo{}, fmt.Errorf("minio: upload %s: %w", objectName, err)
	}
	return info, nil
}

// MigrateToIngestion copies uploadObjectName from the upload bucket into the ingestion
// bucket under sha256Hash's sharded key, verifies the copy landed, then removes the
// original object from the upload bucket. It returns the ingestion bucket key.
func (c *Client) MigrateToIngestion(ctx context.Context, uploadObjectName, sha256Hash string) (string, error) {
	destKey := IngestionObjectKey(sha256Hash)

	_, err := c.inner.CopyObject(ctx,
		miniogo.CopyDestOptions{Bucket: c.ingestionBucket, Object: destKey},
		miniogo.CopySrcOptions{Bucket: c.uploadBucket, Object: uploadObjectName},
	)
	if err != nil {
		return "", fmt.Errorf("minio: copy %s to ingestion bucket: %w", uploadObjectName, err)
	}

	exists, err := c.ExistsInIngestionBucket(ctx, sha256Hash)
	if err != nil {
		return "", fmt.Errorf("minio: verify migrated object %s: %w", destKey, err)
	}
	if !exists {
		return "", fmt.Errorf("minio: migrated object %s missing after copy", destKey)
	}

	if err := c.inner.RemoveObject(ctx, c.uploadBucket, uploadObjectName, miniogo.RemoveObjectOptions{}); err != nil {
		return "", fmt.Errorf("minio: remove original %s: %w", uploadObjectName, err)
	}

	return destKey, nil
}

// HashUploadObject streams objectName from the upload bucket, returning its sha256 hex digest and size.
func (c *Client) HashUploadObject(ctx context.Context, objectName string) (string, int64, error) {
	obj, err := c.inner.GetObject(ctx, c.uploadBucket, objectName, miniogo.GetObjectOptions{})
	if err != nil {
		return "", 0, fmt.Errorf("minio: get %s: %w", objectName, err)
	}
	defer obj.Close()

	h := sha256.New()
	size, err := io.Copy(h, obj)
	if err != nil {
		return "", 0, fmt.Errorf("minio: hash %s: %w", objectName, err)
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// RemoveFromUploadBucket deletes objectName from the upload bucket.
func (c *Client) RemoveFromUploadBucket(ctx context.Context, objectName string) error {
	if err := c.inner.RemoveObject(ctx, c.uploadBucket, objectName, miniogo.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("minio: remove %s: %w", objectName, err)
	}
	return nil
}

// ExistsInUploadBucket reports whether objectName exists in the upload bucket.
func (c *Client) ExistsInUploadBucket(ctx context.Context, objectName string) (bool, error) {
	return c.exists(ctx, c.uploadBucket, objectName)
}

// ExistsInIngestionBucket reports whether an object keyed by sha256Hash exists in the ingestion bucket.
func (c *Client) ExistsInIngestionBucket(ctx context.Context, sha256Hash string) (bool, error) {
	return c.exists(ctx, c.ingestionBucket, IngestionObjectKey(sha256Hash))
}

func (c *Client) exists(ctx context.Context, bucket, objectName string) (bool, error) {
	_, err := c.inner.StatObject(ctx, bucket, objectName, miniogo.StatObjectOptions{})
	if err != nil {
		if miniogo.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("minio: stat %s/%s: %w", bucket, objectName, err)
	}
	return true, nil
}

// PresignedUploadURL returns a presigned PUT URL for uploading objectName directly into the upload bucket.
func (c *Client) PresignedUploadURL(ctx context.Context, objectName string, expiry time.Duration) (*url.URL, error) {
	u, err := c.inner.PresignedPutObject(ctx, c.uploadBucket, objectName, expiry)
	if err != nil {
		return nil, fmt.Errorf("minio: presigned upload url %s: %w", objectName, err)
	}
	return u, nil
}

// PresignedIngestionURL returns a presigned GET URL for the ingestion bucket object identified by sha256Hash.
func (c *Client) PresignedIngestionURL(ctx context.Context, sha256Hash string, expiry time.Duration) (*url.URL, error) {
	u, err := c.inner.PresignedGetObject(ctx, c.ingestionBucket, IngestionObjectKey(sha256Hash), expiry, nil)
	if err != nil {
		return nil, fmt.Errorf("minio: presigned ingestion url %s: %w", sha256Hash, err)
	}
	return u, nil
}
