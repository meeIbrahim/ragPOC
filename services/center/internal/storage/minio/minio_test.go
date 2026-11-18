package minio

import (
	"context"
	"testing"
	"time"

	"center-service/internal/config"
)

func testConfig(publicURL string) config.MinioConfig {
	return config.MinioConfig{
		URL:             "minio:9000",
		PublicURL:       publicURL,
		AccessKey:       "test-access",
		SecretKey:       "test-secret",
		UploadBucket:    "upload-bucket",
		IngestionBucket: "ingestion-bucket",
	}
}

// F5 R3: the browser PUTs to upload_url, so it must name public_url's host, not the compose
// service name Go itself dials.
func TestPresignedUploadURLUsesThePublicURL(t *testing.T) {
	for _, tc := range []struct{ publicURL, scheme, host string }{
		{"http://localhost:9000", "http", "localhost:9000"},
		{"https://files.example.com/", "https", "files.example.com"},
	} {
		c, err := New(testConfig(tc.publicURL))
		if err != nil {
			t.Fatal(err)
		}
		u, err := c.PresignedUploadURL(context.Background(), "obj", time.Minute)
		if err != nil {
			t.Fatalf("%s: presign: %v", tc.publicURL, err)
		}
		if u.Scheme != tc.scheme || u.Host != tc.host || u.Path != "/upload-bucket/obj" {
			t.Errorf("%s: upload_url = %s; want %s://%s/upload-bucket/obj", tc.publicURL, u, tc.scheme, tc.host)
		}
	}
}

// F5 A2: presigning is local signing. Without a fixed Region, minio-go would first ask the
// server for the bucket location, and public_url is not reachable from inside compose.
func TestPresignedUploadURLNeverDialsThePublicURL(t *testing.T) {
	c, err := New(testConfig("http://10.255.255.1:9")) // unroutable: a dial would hang
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := c.PresignedUploadURL(ctx, "obj", time.Minute); err != nil {
		t.Fatalf("presign: %v; want local signing with no network call", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("presign took %v; want under 1s: it must not dial public_url", d)
	}
}
