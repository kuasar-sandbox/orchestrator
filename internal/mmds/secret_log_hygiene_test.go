package mmds

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// mmdsLogHygienePackages are every package on the MMDS endpoint path that
// handles a store value, relay auth value, or relayed secret in cleartext.
// New packages added to that path should be added here too.
var mmdsLogHygienePackages = []string{
	"internal/mmds",
	"internal/mmdsauth",
	"internal/mmdsrelay",
	"internal/mmdsrpc",
	"internal/mmdscfg",
	"internal/proxyendpoints",
	"internal/configsock",
	"internal/orch",
}

// loggerCallRE finds one *slog.Logger call (Info/Warn/Error/Debug), captured
// up to its closing paren on the same or a following line via loggerCallEnd
// below — deliberately simple (this is a lint-style guard, not a full Go
// parser) since every call site in these packages is a single statement.
var loggerCallRE = regexp.MustCompile(`\.(?:Info|Warn|Error|Debug)\(`)

// forbiddenLogArg matches a bare identifier (as opposed to a string literal
// key like "value" or "sid") that plausibly carries secret material, passed
// directly as a logger argument: value, secret, plaintext, a header's
// injected value, or a response/request body. Field *names* like
// "auth_header_name" (the header NAME, not its value) or "backend" are not
// matched — only identifiers a reader would recognize as holding the
// material itself.
var forbiddenLogArg = regexp.MustCompile(`\b(value|Value|secret|Secret|plaintext|Plaintext|headerValue|body|Body|relayAuth|storeValue|authValue)\b`)

// TestNoMMDSPackageLogsSecretMaterial greps every non-test .go file in the
// MMDS endpoint packages for a logger call whose arguments reference a
// secret/value/plaintext/body identifier directly — the store value, relay
// auth value, and relayed secret plaintext must only ever be logged at
// redacted/length granularity (or not at all), never in full. This is a
// static best-effort guard (regex, not a real Go parser), so it can be
// fooled by sufficiently indirect code, but every call site in these
// packages today is a single, direct statement.
func TestNoMMDSPackageLogsSecretMaterial(t *testing.T) {
	root := repoRoot(t)

	for _, pkg := range mmdsLogHygienePackages {
		dir := filepath.Join(root, pkg)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || filepath.Ext(name) != ".go" || len(name) >= 8 && name[len(name)-8:] == "_test.go" {
				continue
			}
			path := filepath.Join(dir, name)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			for i, line := range splitLines(string(data)) {
				if !loggerCallRE.MatchString(line) {
					continue
				}
				if forbiddenLogArg.MatchString(line) {
					t.Errorf("%s:%d: logger call appears to reference secret material directly: %s", path, i+1, line)
				}
			}
		}
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// repoRoot walks up from this test file's own directory to find go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from test file")
		}
		dir = parent
	}
}
