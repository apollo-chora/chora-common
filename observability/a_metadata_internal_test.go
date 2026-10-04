// a_metadata_internal_test.go — GCE metadata-server stub for
// gceMetadataProjectID's on-GCE branches.
//
// The metadata library honours GCE_METADATA_HOST, so an httptest stub can
// stand in for the real metadata server without any network. However the
// library ALSO memoizes OnGCEWithContext (sync.Once) and caches the
// project-id value process-lifetime (cachedValue), so the on-GCE
// branches are only observable in a FRESH process. This test therefore
// re-execs the test binary as a child with GCE_METADATA_HOST stubbed and
// asserts the stub response there; the child is run with
// -test.coverprofile so its statements can be merged into the package
// profile by the verification harness.
package observability

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestGCEMetadataProjectID_OnGCEWithStubbedHost(t *testing.T) {
	if os.Getenv("OBS_META_CHILD") == "1" {
		runMetadataStubInChild(t)
		return
	}

	// Parent: re-exec just this test in a fresh process so the
	// process-lifetime metadata memoization/cache is bypassed, and pass
	// -test.coverprofile ONLY when this binary is itself cover-
	// instrumented (the plain build would otherwise reject the flag).
	// The child's counter data is then merged by the verification harness
	// into the package profile (see report; normal-exit subprocess
	// counters are not folded in by `go test`).
	childArgs := []string{"-test.run=^TestGCEMetadataProjectID_OnGCEWithStubbedHost$"}
	for _, a := range os.Args {
		if strings.HasPrefix(a, "-test.coverprofile=") {
			childArgs = append(childArgs, "-test.coverprofile="+filepath.Join(os.TempDir(), "obs_child_cover.out"))
			break
		}
	}
	cmd := exec.Command(os.Args[0], childArgs...)
	cmd.Env = append(os.Environ(), "OBS_META_CHILD=1")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("metadata-stub child process failed: %v", err)
	}
}

func runMetadataStubInChild(t *testing.T) {
	t.Helper()
	// serveOK flips the project-id endpoint between 404 (error branch) and
	// 200 (success branch). The metadata lib only caches the value on
	// success, so a failed GET leaves the cache empty and the following
	// 200 GET genuinely hits the stub again.
	var (
		mu      sync.Mutex
		serveOK bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/", "/computeMetadata/v1/":
			w.WriteHeader(http.StatusOK)
		case "/computeMetadata/v1/project/project-id":
			mu.Lock()
			ok := serveOK
			mu.Unlock()
			if !ok {
				http.Error(w, "not defined", http.StatusNotFound)
				return
			}
			_, _ = fmt.Fprint(w, "chora-test-project")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// The metadata library builds "http://" + GCE_METADATA_HOST itself,
	// so the env value must be scheme-less host:port.
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(srv.URL, "http://"))

	// First: the server rejects the lookup — the error branch must yield
	// ("", false), and no value is cached.
	if id, ok := gceMetadataProjectID(context.Background()); ok || id != "" {
		t.Fatalf("metadata error branch: got id=%q ok=%v, want (\"\", false)", id, ok)
	}

	// Second: the server now serves the project id — the success branch
	// must yield the stubbed id.
	mu.Lock()
	serveOK = true
	mu.Unlock()
	id, ok := gceMetadataProjectID(context.Background())
	if !ok {
		t.Fatalf("gceMetadataProjectID ok=false, want true with a stubbed metadata host")
	}
	if id != "chora-test-project" {
		t.Errorf("project id = %q, want chora-test-project", id)
	}
}


