package objectstore_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/objectstore"
)

const (
	defaultTimeout = 30 * time.Second
	defaultRegion  = "us-east-1"
)

// validConfig returns a Config that New accepts without touching the network.
func validConfig() objectstore.Config {
	return objectstore.Config{
		Endpoint:     "http://localhost:9000",
		AccessKey:    "chora",
		SecretKey:    "chora",
		Bucket:       "media",
		UsePathStyle: true,
	}
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  objectstore.Config
		want error
	}{
		{
			name: "empty endpoint",
			cfg:  objectstore.Config{},
			want: objectstore.ErrEmptyEndpoint,
		},
		{
			name: "missing credentials",
			cfg:  objectstore.Config{Endpoint: "http://localhost:9000", Bucket: "media"},
			want: objectstore.ErrMissingCredentials,
		},
		{
			name: "missing secret",
			cfg:  objectstore.Config{Endpoint: "http://localhost:9000", AccessKey: "a", Bucket: "media"},
			want: objectstore.ErrMissingCredentials,
		},
		{
			name: "missing bucket",
			cfg:  objectstore.Config{Endpoint: "http://localhost:9000", AccessKey: "a", SecretKey: "b"},
			want: objectstore.ErrEmptyBucket,
		},
		{
			name: "non-http scheme",
			cfg:  objectstore.Config{Endpoint: "ftp://host", AccessKey: "a", SecretKey: "b", Bucket: "media"},
		},
		{
			name: "endpoint without host",
			cfg:  objectstore.Config{Endpoint: "http://", AccessKey: "a", SecretKey: "b", Bucket: "media"},
		},
		{
			name: "negative timeout",
			cfg: objectstore.Config{
				Endpoint: "http://localhost:9000", AccessKey: "a", SecretKey: "b",
				Bucket: "media", Timeout: -time.Second,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, err := objectstore.New(c.cfg)
			if err == nil {
				t.Fatalf("New() = %v, want error", st)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Errorf("New() error = %v, want errors.Is %v", err, c.want)
			}
		})
	}
}

func TestNew_AcceptsValidConfig(t *testing.T) {
	st, err := objectstore.New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if st == nil {
		t.Fatal("New returned nil store")
	}
	if err := st.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestOperations_RejectInvalidKey(t *testing.T) {
	st, err := objectstore.New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	ops := []struct {
		name string
		call func() error
	}{
		{"Put", func() error { return st.Put(ctx, "", strings.NewReader("x"), 1, "text/plain") }},
		{"Get", func() error { _, err := st.Get(ctx, ""); return err }},
		{"Delete", func() error { return st.Delete(ctx, "") }},
		{"Exists", func() error { _, err := st.Exists(ctx, ""); return err }},
		{"PresignGet", func() error { _, err := st.PresignGet(ctx, "", time.Minute); return err }},
		{"Put leading slash", func() error { return st.Put(ctx, "/x", strings.NewReader("x"), 1, "") }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			err := op.call()
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, objectstore.ErrInvalidKey) {
				t.Errorf("error = %v, want errors.Is ErrInvalidKey", err)
			}
		})
	}
}

func TestPut_RejectsNilReader(t *testing.T) {
	st, err := objectstore.New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = st.Put(context.Background(), "valid/key", nil, 0, "")
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
	if errors.Is(err, objectstore.ErrInvalidKey) {
		t.Errorf("nil reader reported as invalid key: %v", err)
	}
}

func TestPresignGet_ValidatesTTL(t *testing.T) {
	st, err := objectstore.New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	for _, ttl := range []time.Duration{0, -time.Second, 8 * 24 * time.Hour} {
		if _, err := st.PresignGet(ctx, "a/b.txt", ttl); !errors.Is(err, objectstore.ErrInvalidTTL) {
			t.Errorf("PresignGet(ttl=%s) error = %v, want ErrInvalidTTL", ttl, err)
		}
	}

	// Exactly 7 days is the SigV4 ceiling and must be accepted.
	if _, err := st.PresignGet(ctx, "a/b.txt", 7*24*time.Hour); err != nil {
		t.Errorf("PresignGet(ttl=7d) error = %v, want nil", err)
	}
}

func TestPresignGet_ReturnsSignedPathStyleURL(t *testing.T) {
	st, err := objectstore.New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raw, err := st.PresignGet(context.Background(), "42/cover.png", 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("presigned URL %q is not parseable: %v", raw, err)
	}

	if u.Scheme != "http" {
		t.Errorf("scheme=%q want http", u.Scheme)
	}
	if u.Host != "localhost:9000" {
		t.Errorf("host=%q want localhost:9000", u.Host)
	}
	if u.Path != "/media/42/cover.png" {
		t.Errorf("path=%q want path-style /media/42/cover.png", u.Path)
	}
	q := u.Query()
	if q.Get("X-Amz-Signature") == "" {
		t.Error("presigned URL has no X-Amz-Signature")
	}
	if got := q.Get("X-Amz-Expires"); got != "900" {
		t.Errorf("X-Amz-Expires=%q want 900", got)
	}
	if !strings.Contains(q.Get("X-Amz-Credential"), "chora") {
		t.Errorf("X-Amz-Credential=%q does not contain the access key", q.Get("X-Amz-Credential"))
	}
}

// clearEnv unsets keys for the duration of the test and restores their previous
// values on cleanup. t.Setenv cannot express "unset", so it is done manually.
func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if old, ok := os.LookupEnv(k); ok {
			t.Cleanup(func() { _ = os.Setenv(k, old) })
		} else {
			t.Cleanup(func() { _ = os.Unsetenv(k) })
		}
		_ = os.Unsetenv(k)
	}
}

var allS3Env = []string{
	"S3_ENDPOINT", "S3_ACCESS_KEY_ID", "S3_ACCESS_KEY",
	"S3_SECRET_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION",
	"S3_BUCKET", "S3_FORCE_PATH_STYLE",
}

func TestConfigFromEnv_Defaults(t *testing.T) {
	clearEnv(t, allS3Env...)

	cfg := objectstore.ConfigFromEnv()
	if cfg.Endpoint != "" {
		t.Errorf("Endpoint=%q want empty", cfg.Endpoint)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		t.Errorf("credentials=%q/%q want empty", cfg.AccessKey, cfg.SecretKey)
	}
	if cfg.Region != defaultRegion {
		t.Errorf("Region=%q want %q", cfg.Region, defaultRegion)
	}
	if cfg.Bucket != "" {
		t.Errorf("Bucket=%q want empty", cfg.Bucket)
	}
	if !cfg.UsePathStyle {
		t.Error("UsePathStyle=false want default true")
	}
	if cfg.Timeout != defaultTimeout {
		t.Errorf("Timeout=%s want %s", cfg.Timeout, defaultTimeout)
	}
}

func TestConfigFromEnv_ReadsValues(t *testing.T) {
	clearEnv(t, allS3Env...)
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("S3_ACCESS_KEY_ID", "id-from-env")
	t.Setenv("S3_SECRET_ACCESS_KEY", "secret-from-env")
	t.Setenv("S3_REGION", "eu-central-1")
	t.Setenv("S3_BUCKET", "exports")
	t.Setenv("S3_FORCE_PATH_STYLE", "false")

	cfg := objectstore.ConfigFromEnv()
	if cfg.Endpoint != "http://minio:9000" {
		t.Errorf("Endpoint=%q", cfg.Endpoint)
	}
	if cfg.AccessKey != "id-from-env" {
		t.Errorf("AccessKey=%q", cfg.AccessKey)
	}
	if cfg.SecretKey != "secret-from-env" {
		t.Errorf("SecretKey=%q", cfg.SecretKey)
	}
	if cfg.Region != "eu-central-1" {
		t.Errorf("Region=%q", cfg.Region)
	}
	if cfg.Bucket != "exports" {
		t.Errorf("Bucket=%q", cfg.Bucket)
	}
	if cfg.UsePathStyle {
		t.Error("UsePathStyle=true want false from S3_FORCE_PATH_STYLE=false")
	}
}

func TestConfigFromEnv_LegacyCredentialFallback(t *testing.T) {
	clearEnv(t, allS3Env...)
	t.Setenv("S3_ACCESS_KEY", "legacy-id")
	t.Setenv("S3_SECRET_KEY", "legacy-secret")

	cfg := objectstore.ConfigFromEnv()
	if cfg.AccessKey != "legacy-id" {
		t.Errorf("AccessKey=%q want legacy fallback", cfg.AccessKey)
	}
	if cfg.SecretKey != "legacy-secret" {
		t.Errorf("SecretKey=%q want legacy fallback", cfg.SecretKey)
	}
}

func TestConfigFromEnv_PrimaryCredentialsWinOverLegacy(t *testing.T) {
	clearEnv(t, allS3Env...)
	t.Setenv("S3_ACCESS_KEY_ID", "primary-id")
	t.Setenv("S3_ACCESS_KEY", "legacy-id")
	t.Setenv("S3_SECRET_ACCESS_KEY", "primary-secret")
	t.Setenv("S3_SECRET_KEY", "legacy-secret")

	cfg := objectstore.ConfigFromEnv()
	if cfg.AccessKey != "primary-id" {
		t.Errorf("AccessKey=%q want primary", cfg.AccessKey)
	}
	if cfg.SecretKey != "primary-secret" {
		t.Errorf("SecretKey=%q want primary", cfg.SecretKey)
	}
}

func TestConfigFromEnv_PathStyleParsing(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"true", true},
		{"1", true},
		{"false", false},
		{"0", false},
		{"garbage", true}, // unparseable falls back to the default
	}
	for _, c := range cases {
		t.Run(c.val, func(t *testing.T) {
			clearEnv(t, allS3Env...)
			t.Setenv("S3_FORCE_PATH_STYLE", c.val)
			if got := objectstore.ConfigFromEnv().UsePathStyle; got != c.want {
				t.Errorf("UsePathStyle=%v want %v for %q", got, c.want, c.val)
			}
		})
	}
}
