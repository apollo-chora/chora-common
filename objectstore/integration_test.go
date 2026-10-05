package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// TestIntegration_RoundTrip exercises the real Store against a live
// S3-compatible endpoint (MinIO). It is skipped unless S3_INTEGRATION=1.
//
// Required environment (see ConfigFromEnv): S3_ENDPOINT, S3_ACCESS_KEY_ID
// (or S3_ACCESS_KEY), S3_SECRET_ACCESS_KEY (or S3_SECRET_KEY), S3_BUCKET.
// The bucket must exist or be creatable by the supplied credentials.
func TestIntegration_RoundTrip(t *testing.T) {
	if os.Getenv("S3_INTEGRATION") != "1" {
		t.Skip("set S3_INTEGRATION=1 (plus S3_ENDPOINT/S3_* credentials/S3_BUCKET) to run against MinIO")
	}

	cfg := ConfigFromEnv()
	cfg.UsePathStyle = true
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := store.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); err != nil && !isBucketAlreadyExists(err) {
		t.Fatalf("ensure bucket %q: %v", cfg.Bucket, err)
	}

	key := fmt.Sprintf("objectstore-itest/%d.txt", time.Now().UnixNano())
	payload := []byte("hello objectstore")
	t.Cleanup(func() { _ = store.Delete(context.Background(), key) })

	if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload)), "text/plain"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ok, err := store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Fatal("Exists=false after Put")
	}

	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if closeErr := rc.Close(); closeErr != nil {
		t.Errorf("Close body: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Get body=%q want %q", got, payload)
	}

	raw, err := store.PresignGet(ctx, key, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		t.Fatalf("build presigned request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch presigned URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned GET status=%d want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read presigned body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Errorf("presigned body=%q want %q", body, payload)
	}

	missing := key + ".missing"
	ok, err = store.Exists(ctx, missing)
	if err != nil {
		t.Fatalf("Exists(missing): %v", err)
	}
	if ok {
		t.Fatal("Exists=true for a key that was never written")
	}
	if _, err := store.Get(ctx, missing); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(missing) error=%v want ErrNotFound", err)
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, err = store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists after Delete: %v", err)
	}
	if ok {
		t.Fatal("Exists=true after Delete")
	}
}

// isBucketAlreadyExists reports whether err means the bucket is already owned
// by these credentials, which CreateBucket may return on a re-run.
func isBucketAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return true
		}
	}
	return false
}
