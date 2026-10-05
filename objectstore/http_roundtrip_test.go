package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/objectstore"
)

// fakeS3 is a minimal, in-process S3-compatible endpoint used to exercise the
// real HTTP path without a container. It ignores authentication and stores
// objects in memory keyed by URL path.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, ok := strings.Cut(trimmed, "/")
	if !ok || bucket == "" || key == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := r.URL.Path

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		f.objects[id] = body
		w.Header().Set("ETag", `"fake-etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		body, ok := f.objects[id]
		if !ok {
			writeNoSuchKey(w)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case http.MethodHead:
		body, ok := f.objects[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		delete(f.objects, id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeNoSuchKey(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
}

func TestStore_HTTPRoundTrip(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	store, err := objectstore.New(objectstore.Config{
		Endpoint:     srv.URL,
		AccessKey:    "test",
		SecretKey:    "test",
		Bucket:       "media",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	payload := []byte("round-trip payload")

	if err := store.Put(ctx, "dir/obj.bin", bytes.NewReader(payload), int64(len(payload)), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ok, err := store.Exists(ctx, "dir/obj.bin")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Fatal("Exists=false after Put")
	}

	rc, err := store.Get(ctx, "dir/obj.bin")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if closeErr := rc.Close(); closeErr != nil {
		t.Errorf("Close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Get body=%q want %q", got, payload)
	}

	ok, err = store.Exists(ctx, "dir/missing.bin")
	if err != nil {
		t.Fatalf("Exists(missing): %v", err)
	}
	if ok {
		t.Fatal("Exists=true for a missing key")
	}
	if _, err := store.Get(ctx, "dir/missing.bin"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("Get(missing) error=%v want ErrNotFound", err)
	}

	raw, err := store.PresignGet(ctx, "dir/obj.bin", time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if !strings.HasPrefix(raw, srv.URL+"/media/dir/obj.bin?") {
		t.Errorf("presigned URL=%q want prefix %q", raw, srv.URL+"/media/dir/obj.bin?")
	}

	if err := store.Delete(ctx, "dir/obj.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, err = store.Exists(ctx, "dir/obj.bin")
	if err != nil {
		t.Fatalf("Exists after Delete: %v", err)
	}
	if ok {
		t.Fatal("Exists=true after Delete")
	}
}
