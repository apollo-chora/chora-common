// Package httpclient — supplementary retry/sleep edge tests.
package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/httpclient"
)

func TestClient_Get_ContextCancelledDuringRetryBackoff(t *testing.T) {
	t.Parallel()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	cl, err := httpclient.New(srv.URL,
		httpclient.WithMaxRetries(2),
		httpclient.WithRetryBackoff(10*time.Second), // longer than ctx budget
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	start := time.Now()
	resp, err := cl.Get(ctx, "/x")
	elapsed := time.Since(start)
	if err == nil {
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatal("expected context error while sleeping between 5xx retries")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("sleepCtx ignored ctx cancellation: elapsed %s", elapsed)
	}
}

func TestClient_Get_ContextCancelledDuringNetworkRetrySleep(t *testing.T) {
	t.Parallel()
	// Point at an unlistened loopback port so the first attempt fails with
	// a network error (retryable), then cancel mid-backoff.
	cl, err := httpclient.New("http://127.0.0.1:59998",
		httpclient.WithMaxRetries(1),
		httpclient.WithRetryBackoff(10*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	start := time.Now()
	_, err = cl.Get(ctx, "/x")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error (refused conn + cancelled ctx)")
	}
	if elapsed > 3*time.Second {
		t.Errorf("network-retry sleep ignored ctx cancellation: elapsed %s", elapsed)
	}
}

func TestClient_Get_ZeroBackoffRetriesImmediately(t *testing.T) {
	t.Parallel()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "try again", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cl, err := httpclient.New(srv.URL,
		httpclient.WithMaxRetries(1),
		httpclient.WithRetryBackoff(0), // sleepCtx(0) → no sleep
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	resp, err := cl.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (retry with zero backoff)", resp.StatusCode)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}
