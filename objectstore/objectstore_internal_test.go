package objectstore

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestNew_AppliesDefaultsAndNormalizesEndpoint(t *testing.T) {
	st, err := New(Config{
		Endpoint:  "http://localhost:9000/",
		AccessKey: "chora",
		SecretKey: "chora",
		Bucket:    "media",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if st.bucket != "media" {
		t.Errorf("bucket=%q want %q", st.bucket, "media")
	}
	if st.timeout != defaultTimeout {
		t.Errorf("timeout=%s want default %s", st.timeout, defaultTimeout)
	}

	opts := st.client.Options()
	if opts.Region != defaultRegion {
		t.Errorf("region=%q want default %q", opts.Region, defaultRegion)
	}
	if opts.BaseEndpoint == nil {
		t.Fatal("BaseEndpoint is nil")
	}
	if *opts.BaseEndpoint != "http://localhost:9000" {
		t.Errorf("BaseEndpoint=%q want trailing slash trimmed", *opts.BaseEndpoint)
	}
	if opts.UsePathStyle {
		t.Error("UsePathStyle=true for zero-value Config; want false (documented zero value)")
	}
}

func TestNew_PropagatesPathStyleAndRegion(t *testing.T) {
	st, err := New(Config{
		Endpoint:     "http://minio:9000",
		AccessKey:    "a",
		SecretKey:    "b",
		Bucket:       "exports",
		Region:       "eu-west-2",
		UsePathStyle: true,
		Timeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := st.client.Options()
	if !opts.UsePathStyle {
		t.Error("UsePathStyle not propagated")
	}
	if opts.Region != "eu-west-2" {
		t.Errorf("region=%q want %q", opts.Region, "eu-west-2")
	}
	if st.timeout != 5*time.Second {
		t.Errorf("timeout=%s want %s", st.timeout, 5*time.Second)
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"schemeless assumes https", "localhost:9000", "https://localhost:9000", false},
		{"http kept", "http://localhost:9000", "http://localhost:9000", false},
		{"trailing slash trimmed", "http://localhost:9000/", "http://localhost:9000", false},
		{"whitespace trimmed", "  http://minio:9000  ", "http://minio:9000", false},
		{"path prefix kept", "https://s3.example.com/prefix", "https://s3.example.com/prefix", false},
		{"empty rejected", "", "", true},
		{"whitespace-only rejected", "   ", "", true},
		{"non-http scheme rejected", "ftp://host", "", true},
		{"missing host rejected", "http://", "", true},
		{"garbage rejected", "://bad", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := normalizeEndpoint(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("normalizeEndpoint(%q)=%q want error", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeEndpoint(%q): %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("normalizeEndpoint(%q)=%q want %q", c.in, got, c.want)
			}
		})
	}
}

func TestValidateKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"nested path", "media/42/cover.png", false},
		{"single char", "a", false},
		{"unicode", "ünïcode/ok", false},
		{"at limit", strings.Repeat("a", maxKeyLength), false},
		{"over limit", strings.Repeat("a", maxKeyLength+1), true},
		{"empty", "", true},
		{"leading slash", "/leading", true},
		{"nul byte", "has\x00nul", true},
		{"newline", "has\nnewline", true},
		{"delete char", "has\x7fdel", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateKey(c.key)
			if c.wantErr {
				if err == nil {
					t.Fatalf("validateKey(%q) = nil, want error", c.key)
				}
				if !errors.Is(err, ErrInvalidKey) {
					t.Fatalf("validateKey(%q) error %v, want ErrInvalidKey", c.key, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateKey(%q): %v", c.key, err)
			}
		})
	}
}

func TestBoolFromEnv(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		val  string
		def  bool
		want bool
	}{
		{"unset uses default true", false, "", true, true},
		{"unset uses default false", false, "", false, false},
		{"true", true, "true", false, true},
		{"one", true, "1", false, true},
		{"false", true, "false", true, false},
		{"zero", true, "0", true, false},
		{"garbage uses default", true, "not-a-bool", true, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := "OBJECTSTORE_TEST_BOOL_" + string(rune('A'+i))
			if c.set {
				t.Setenv(key, c.val)
			} else {
				t.Setenv(key, "")
			}
			if got := boolFromEnv(key, c.def); got != c.want {
				t.Errorf("boolFromEnv(%q,%v)=%v want %v", c.val, c.def, got, c.want)
			}
		})
	}
}

func TestIsNotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"NoSuchKey", &types.NoSuchKey{}, true},
		{"NotFound", &types.NotFound{}, true},
		{"smithy NotFound code", &smithy.GenericAPIError{Code: "NotFound", Message: "x"}, true},
		{"smithy NoSuchBucket code", &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "x"}, true},
		{"smithy AccessDenied", &smithy.GenericAPIError{Code: "AccessDenied", Message: "x"}, false},
		{"plain error", errors.New("boom"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isNotFound(c.err); got != c.want {
				t.Errorf("isNotFound(%v)=%v want %v", c.err, got, c.want)
			}
		})
	}
}

func TestClose_IsNoOp(t *testing.T) {
	st, err := New(Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "a",
		SecretKey: "b",
		Bucket:    "c",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Errorf("Close()=%v want nil", err)
	}
}
