package testsecret

import (
	"os"
	"os/exec"
	"regexp"
	"testing"
)

// InSubprocess isolates tests that exercise the process-wide default Secret
// plugin client. The client's configuration is intentionally fixed after its
// first reference; a fresh process also makes go test -count=N repeatable.
// Call this before Start and return when it returns false.
func InSubprocess(t *testing.T) bool {
	t.Helper()
	const marker = "KUASAR_SECRET_PLUGIN_TEST_CASE"
	if os.Getenv(marker) == t.Name() {
		return true
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.timeout=2m")
	command.Env = append(os.Environ(), marker+"="+t.Name())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Secret plugin subprocess: %v\n%s", err, output)
	}
	if testing.Verbose() && len(output) != 0 {
		t.Logf("%s", output)
	}
	return false
}
