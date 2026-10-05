// Package objectstore is the cloud-neutral, S3-compatible blob storage client for
// the chora platform. HISTORY: it replaced the former Google Cloud Storage
// client (cloud.google.com/go/storage); the package is now a small,
// provider-neutral contract that works against MinIO locally and any
// S3-compatible endpoint in production.
//
// The package exposes one type, Store, constructed from a Config:
//
//	store, err := objectstore.New(objectstore.ConfigFromEnv())
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//
//	// Upload (size may be -1 for a seekable reader of unknown length).
//	if err := store.Put(ctx, "media/42/cover.png", r, size, "image/png"); err != nil {
//	    return err
//	}
//	// Download; the caller owns Close.
//	rc, err := store.Get(ctx, "media/42/cover.png")
//	if err != nil {
//	    return err
//	}
//	defer rc.Close()
//	// Time-limited link.
//	url, err := store.PresignGet(ctx, "media/42/cover.png", 15*time.Minute)
//
// Configuration is read from the environment (see ConfigFromEnv) so services
// keep zero inline config per the platform convention. The underlying client is
// the AWS SDK for Go v2 S3 client configured for path-style addressing, which
// MinIO and most self-hosted S3 servers require.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/env"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	// defaultRegion is the signing region used when Config.Region is empty.
	defaultRegion = "us-east-1"
	// defaultTimeout bounds a single non-streaming operation when
	// Config.Timeout is zero.
	defaultTimeout = 30 * time.Second
	// maxKeyLength is the S3 object key ceiling (1024 bytes).
	maxKeyLength = 1024
	// maxPresignTTL is the SigV4 ceiling for a presigned URL (7 days).
	maxPresignTTL = 7 * 24 * time.Hour
)

// Sentinel errors. Callers should test with errors.Is; every operation wraps
// the relevant sentinel so a single errors.Is check works.
var (
	// ErrEmptyEndpoint is returned by New when Config.Endpoint is empty.
	ErrEmptyEndpoint = errors.New("objectstore: endpoint is required")
	// ErrEmptyBucket is returned by New when Config.Bucket is empty.
	ErrEmptyBucket = errors.New("objectstore: bucket is required")
	// ErrMissingCredentials is returned by New when Config.AccessKey or
	// Config.SecretKey is empty.
	ErrMissingCredentials = errors.New("objectstore: access key and secret key are required")
	// ErrInvalidKey is returned by every keyed operation when the key fails
	// validation (empty, longer than 1024 bytes, a leading slash, or containing
	// a control character).
	ErrInvalidKey = errors.New("objectstore: invalid object key")
	// ErrNotFound is returned by Get when the object does not exist. Exists
	// reports a missing object as (false, nil) instead.
	ErrNotFound = errors.New("objectstore: object not found")
	// ErrInvalidTTL is returned by PresignGet when the TTL is not in
	// (0, 7d].
	ErrInvalidTTL = errors.New("objectstore: presign TTL must be between 1s and 7d")
)

// Config holds the S3-compatible connection settings. The zero value is not
// usable: Endpoint, Bucket, AccessKey and SecretKey are required. Prefer
// ConfigFromEnv so configuration stays out of source, per the platform
// convention.
type Config struct {
	// Endpoint is the S3-compatible server address. Include the scheme for
	// non-local endpoints, e.g. "https://s3.eu-west-1.amazonaws.com"; MinIO and
	// other plain-HTTP endpoints must include "http://", e.g.
	// "http://localhost:9000". A schemeless endpoint is assumed to be HTTPS.
	Endpoint string
	// AccessKey is the S3 access key ID.
	AccessKey string
	// SecretKey is the S3 secret access key.
	SecretKey string
	// Region is the SigV4 signing region. Empty defaults to "us-east-1".
	Region string
	// Bucket is the bucket used by every operation on the Store.
	Bucket string
	// UsePathStyle selects path-style addressing (…/bucket/key) instead of
	// virtual-host addressing (bucket.…/key). MinIO and most self-hosted S3
	// servers require true. Note the zero value is false, so callers that
	// construct Config directly for MinIO must set it explicitly;
	// ConfigFromEnv defaults it to true via S3_FORCE_PATH_STYLE.
	UsePathStyle bool
	// Timeout bounds a single non-streaming operation (Put, Delete, Exists,
	// PresignGet). Empty or zero uses defaultTimeout (30s); negative is
	// rejected by New. Get is not bounded by this value — see Get.
	Timeout time.Duration
}

// Store is an S3-compatible object store bound to a single bucket. It is safe
// for concurrent use.
type Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	timeout time.Duration
}

// New constructs a Store from cfg, validating the required fields and applying
// sane defaults (Region "us-east-1", Timeout 30s). It performs no network I/O,
// so a Store can be built even when the endpoint is unreachable.
func New(cfg Config) (*Store, error) {
	endpoint, err := normalizeEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	if cfg.Bucket == "" {
		return nil, ErrEmptyBucket
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, ErrMissingCredentials
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("objectstore: timeout must not be negative, got %s", cfg.Timeout)
	}

	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	creds := aws.NewCredentialsCache(aws.CredentialsProviderFunc(
		func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKey,
				SecretAccessKey: cfg.SecretKey,
				Source:          "objectstore/static",
			}, nil
		},
	))

	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: cfg.UsePathStyle,
		Credentials:  creds,
	})

	return &Store{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
		timeout: timeout,
	}, nil
}

// ConfigFromEnv builds a Config from the process environment. Recognised
// variables:
//
//	S3_ENDPOINT            endpoint, scheme included for non-local hosts
//	S3_ACCESS_KEY_ID       access key (falls back to S3_ACCESS_KEY)
//	S3_SECRET_ACCESS_KEY   secret key (falls back to S3_SECRET_KEY)
//	S3_REGION              signing region (default "us-east-1")
//	S3_BUCKET              bucket name
//	S3_FORCE_PATH_STYLE    path-style addressing (default true)
//
// The legacy S3_ACCESS_KEY / S3_SECRET_KEY names are accepted so existing
// .env files keep working. Validation is deferred to New.
func ConfigFromEnv() Config {
	return Config{
		Endpoint:     os.Getenv("S3_ENDPOINT"),
		AccessKey:    env.GetOrDefault("S3_ACCESS_KEY_ID", os.Getenv("S3_ACCESS_KEY")),
		SecretKey:    env.GetOrDefault("S3_SECRET_ACCESS_KEY", os.Getenv("S3_SECRET_KEY")),
		Region:       env.GetOrDefault("S3_REGION", defaultRegion),
		Bucket:       os.Getenv("S3_BUCKET"),
		UsePathStyle: boolFromEnv("S3_FORCE_PATH_STYLE", true),
		Timeout:      defaultTimeout,
	}
}

// Put uploads r to key. size is the exact byte length, or -1 for a seekable
// reader whose length is unknown. contentType is stored as the object's
// Content-Type and may be empty. A nil reader is rejected.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("objectstore: put %q: nil reader", key)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if size >= 0 {
		in.ContentLength = aws.Int64(size)
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}

	if _, err := s.client.PutObject(ctx, in); err != nil {
		return s.classify(err, key)
	}
	return nil
}

// Get opens key for reading. The caller owns the returned ReadCloser and must
// close it. Unlike the other operations, Get does not impose Config.Timeout on
// the transfer: the body streams under ctx, so pass a context whose deadline
// (if any) suits the object size. A missing object yields an error wrapping
// ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, s.classify(err, key)
	}
	if out.Body == nil {
		return nil, fmt.Errorf("objectstore: get %q: empty response body", key)
	}
	return out.Body, nil
}

// Delete removes key. S3 deletion is idempotent: deleting a key that does not
// exist succeeds and returns nil.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return s.classify(err, key)
	}
	return nil
}

// Exists reports whether key is present. A missing object is (false, nil); any
// other failure is returned as an error.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, s.classify(err, key)
	}
	return true, nil
}

// PresignGet returns a time-limited URL that grants anonymous GET access to
// key. ttl must be greater than zero and at most 7 days (the SigV4 ceiling).
// The URL is signed locally, so no network I/O is performed.
func (s *Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if ttl <= 0 || ttl > maxPresignTTL {
		return "", fmt.Errorf("%w: got %s", ErrInvalidTTL, ttl)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	out, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, func(o *s3.PresignOptions) {
		o.Expires = ttl
	})
	if err != nil {
		return "", s.classify(err, key)
	}
	return out.URL, nil
}

// Close releases resources held by the Store. The AWS SDK S3 client keeps no
// closable resources — its HTTP transport is process-wide and idle connections
// are pooled — so Close is a no-op provided for lifecycle symmetry with the
// Google Cloud Storage client it replaces. It always returns nil.
func (s *Store) Close() error { return nil }

// withTimeout derives a context bounded by the store timeout, or returns the
// parent unchanged (with a no-op cancel) when no timeout is configured.
func (s *Store) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.timeout)
}

// classify maps an SDK error to a package error, preserving ErrNotFound so
// callers can branch on it with errors.Is.
func (s *Store) classify(err error, key string) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	return fmt.Errorf("objectstore: operation on %q failed: %w", key, err)
}

// isNotFound reports whether err represents a missing object or bucket. The
// SDK surfaces HEAD misses as a generic 404, so both typed and smithy errors
// are inspected.
func isNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return true
		}
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}

// validateKey enforces the object-key invariants shared by every operation.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: key is empty", ErrInvalidKey)
	}
	if len(key) > maxKeyLength {
		return fmt.Errorf("%w: %d bytes exceeds the %d-byte limit", ErrInvalidKey, len(key), maxKeyLength)
	}
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("%w: key must not start with %q", ErrInvalidKey, "/")
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: key must not contain control characters", ErrInvalidKey)
		}
	}
	return nil
}

// normalizeEndpoint validates the endpoint and returns it as an absolute URL.
// A schemeless endpoint is assumed to be HTTPS so credentials are never sent in
// the clear by accident; plain-HTTP MinIO endpoints must say "http://".
func normalizeEndpoint(endpoint string) (string, error) {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		return "", ErrEmptyEndpoint
	}
	if !strings.Contains(e, "://") {
		e = "https://" + e
	}

	u, err := url.Parse(e)
	if err != nil {
		return "", fmt.Errorf("objectstore: invalid endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("objectstore: endpoint %q must use the http or https scheme", endpoint)
	}
	if u.Host == "" {
		return "", fmt.Errorf("objectstore: endpoint %q has no host", endpoint)
	}
	return strings.TrimRight(e, "/"), nil
}

// boolFromEnv parses a boolean env var, returning def when it is unset, empty
// or unparseable.
func boolFromEnv(key string, def bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return v
}
