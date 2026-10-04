// mounts_contract_test.go — the registry and the services' actual mux mounts
// must agree, IN BOTH DIRECTIONS.
//
// This is the cross-service half of the CHO-2195 guard. The failure was not that
// someone wrote the wrong route: it was that TWO independently-maintained copies
// of one routing table were free to disagree, and nothing ever compared them. For
// chora-sharing they ended up perfectly inverted, and every Pub/Sub lane into that
// service dead-lettered for two weeks behind a fully green test suite.
//
// So: compare them. For each service, the set of inboxes the registry assigns must
// equal the set of `/api/internal/pubsub/{inbox}` routes it actually mounts.
//
//   - registry has it, service does not mount it  ⇒ the gateway forwards a push to
//     a 404. Pub/Sub retries 5× and dead-letters. (This was weakness-grown and
//     live-quiz-scores.)
//   - service mounts it, registry does not have it ⇒ the gateway REFUSES the inbox
//     (404 + loud log), so the route can never be reached. (This was
//     course-published, and weakness-review-pending in chora-consumption.)
//
// chora-sharing is deliberately NOT scanned here: it carries a strictly stronger,
// in-process guard — (*Handler).MissingInboxes, which main() turns into a refusal
// to boot, so an unmounted inbox cannot even start. The other three services
// should adopt that pattern; until they do, this static scan is their guard.
package pubsubinbox_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/pubsubinbox"
)

// scannedServices are the services whose mounts are checked by source scan.
// chora-sharing is covered in-process instead — see the package comment.
var scannedServices = map[pubsubinbox.Service]string{
	pubsubinbox.Delivery:    "chora-delivery",
	pubsubinbox.Consumption: "chora-consumption",
	pubsubinbox.Identity:    "chora-identity",
}

// A mount is a .Handle/.HandleFunc call carrying a literal inbox path. Matching
// the CALL — not merely the string — matters: the same path appears in doc
// comments and in tests, and a path in a comment is not a route. An earlier
// investigation conflated the two and concluded chora-sharing mounted an inbox it
// did not.
var mountRe = regexp.MustCompile(`\.(?:Handle|HandleFunc)\(\s*"(?:[A-Z]+ )?/api/internal/pubsub/([a-z0-9-]+)"`)

func TestRegistryMatchesServiceMounts(t *testing.T) {
	root := repoRoot(t)

	for svc, dir := range scannedServices {
		t.Run(string(svc), func(t *testing.T) {
			mounted := scanMounts(t, filepath.Join(root, "services", dir))
			assigned := pubsubinbox.InboxesFor(svc)

			for _, inbox := range assigned {
				if !mounted[inbox] {
					t.Errorf("registry assigns %q to %s, but %s MOUNTS NO SUCH ROUTE.\n"+
						"chora-gateway will forward this push to %s and get a 404; Pub/Sub retries "+
						"5x and dead-letters every message on the lane, silently.",
						inbox, svc, svc, pubsubinbox.Path(inbox))
				}
			}

			assignedSet := make(map[string]bool, len(assigned))
			for _, i := range assigned {
				assignedSet[i] = true
			}
			for inbox := range mounted {
				if assignedSet[inbox] {
					continue
				}
				owner, owned := pubsubinbox.Owner(inbox)
				switch {
				case !owned:
					t.Errorf("%s mounts %q, but NO SERVICE OWNS IT in the registry.\n"+
						"chora-gateway refuses an unowned inbox (404), so this route is unreachable "+
						"and its subscription would dead-letter. Register it.", svc, inbox)
				default:
					t.Errorf("%s mounts %q, but the registry assigns it to %s.\n"+
						"The gateway will route it to %s — this route is dead.", svc, inbox, owner, owner)
				}
			}
		})
	}
}

// scanMounts returns every inbox mounted anywhere under dir (excluding _test.go,
// which proves nothing about the running service).
func scanMounts(t *testing.T, dir string) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, m := range mountRe.FindAllSubmatch(src, -1) {
			found[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanning %s: %v", dir, err)
	}
	if len(found) == 0 {
		// Fail loud rather than pass vacuously. A scanner that silently finds
		// nothing is not a guard — it is a green light with the bulb removed.
		t.Fatalf("scanned %s and found ZERO mounted pubsub routes. Either the mount pattern "+
			"changed (update mountRe) or the path is wrong — this test cannot be allowed to "+
			"pass by finding nothing.", dir)
	}
	return found
}

// repoRoot walks up from the test's working directory to the go.work at the
// monorepo root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate the monorepo root (no go.work found walking up)")
	return ""
}

// The scanner must actually be able to SEE a real mount. Without this, a broken
// regex or a wrong path would make every check above pass by finding nothing —
// the F2 lesson: a guard that scans the wrong thing is not a guard.
func TestScanner_SeesAKnownRealMount(t *testing.T) {
	mounted := scanMounts(t, filepath.Join(repoRoot(t), "services", "chora-delivery"))
	if !mounted["payments-inbox"] {
		got := make([]string, 0, len(mounted))
		for k := range mounted {
			got = append(got, k)
		}
		sort.Strings(got)
		t.Fatalf("the scanner did not find chora-delivery's payments-inbox mount — it is "+
			"not reading real routes, so every other assertion here is vacuous. found=%v", got)
	}
}
