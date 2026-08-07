package configsock

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

type stubMMDSRouteSecretAdmin struct {
	values map[string][]byte
	puts   int
	dels   int
}

func (s *stubMMDSRouteSecretAdmin) PutMMDSRouteSecretValue(_ context.Context, sandboxID, name string, value []byte) error {
	if sandboxID != "sandbox-1" {
		return api.ErrNotFound
	}
	if name != "declared" {
		return fmt.Errorf("undeclared: %w", api.ErrBadRequest)
	}
	s.puts++
	s.values[name] = append([]byte(nil), value...)
	return nil
}

func (s *stubMMDSRouteSecretAdmin) DeleteMMDSRouteSecretValue(_ context.Context, sandboxID, name string) error {
	if sandboxID != "sandbox-1" {
		return api.ErrNotFound
	}
	if name != "declared" {
		return fmt.Errorf("undeclared: %w", api.ErrBadRequest)
	}
	s.dels++
	delete(s.values, name)
	return nil
}

func TestMMDSRouteSecretAdminPutOverwriteDelete(t *testing.T) {
	admin := &stubMMDSRouteSecretAdmin{values: map[string][]byte{}}
	_, client := startTestServer(t, Deps{
		MMDSRouteSecretAdmin:         admin,
		MaxMMDSRouteSecretValueBytes: 16,
	})
	path := "/internal/admin/sandboxes/sandbox-1/mmds/secrets/declared"

	request := func(method, body string, headers http.Header) int {
		t.Helper()
		req, err := http.NewRequest(method, "http://unix"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = headers
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode == http.StatusNoContent && len(responseBody) != 0 {
			t.Fatal("successful mutation returned a body")
		}
		return response.StatusCode
	}

	if code := request(http.MethodPut, "one", http.Header{"Content-Type": []string{"application/json"}}); code != http.StatusNoContent {
		t.Fatalf("first PUT status = %d", code)
	}
	if code := request(http.MethodPut, "two", http.Header{"Content-Type": []string{"application/octet-stream"}}); code != http.StatusNoContent {
		t.Fatalf("overwrite status = %d", code)
	}
	if got := string(admin.values["declared"]); got != "two" {
		t.Fatal("stored value mismatch")
	}
	if code := request(http.MethodDelete, "", nil); code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", code)
	}
	if code := request(http.MethodDelete, "", nil); code != http.StatusNoContent {
		t.Fatalf("idempotent DELETE status = %d", code)
	}
	if _, exists := admin.values["declared"]; exists || admin.puts != 2 || admin.dels != 2 {
		t.Fatalf("admin calls: puts=%d dels=%d exists=%t", admin.puts, admin.dels, exists)
	}
}

func TestMMDSRouteSecretAdminValidation(t *testing.T) {
	admin := &stubMMDSRouteSecretAdmin{values: map[string][]byte{}}
	_, client := startTestServer(t, Deps{
		MMDSRouteSecretAdmin:         admin,
		MaxMMDSRouteSecretValueBytes: 3,
	})
	request := func(path, body string, header http.Header) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPut, "http://unix"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = header
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	base := "/internal/admin/sandboxes/sandbox-1/mmds/secrets/"
	if code := request(base+"undeclared", "x", nil); code != http.StatusBadRequest {
		t.Fatalf("undeclared status = %d", code)
	}
	if code := request(base+"declared", "four", nil); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d", code)
	}
	if code := request("/internal/admin/sandboxes/missing/mmds/secrets/declared", "x", nil); code != http.StatusNotFound {
		t.Fatalf("missing sandbox status = %d", code)
	}
}

func TestMMDSRouteSecretAdminPidfileGate(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "admin.pids")
	if err := os.WriteFile(pidfile, []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	admin := &stubMMDSRouteSecretAdmin{values: map[string][]byte{}}
	socket, client := startTestServer(t, Deps{MMDSRouteSecretAdmin: admin, AdminPidfile: pidfile})
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config socket mode = %o, want 600", info.Mode().Perm())
	}
	path := "http://unix/internal/admin/sandboxes/sandbox-1/mmds/secrets/declared"
	req, _ := http.NewRequest(http.MethodPut, path, strings.NewReader("x"))
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("excluded pid status = %d", response.StatusCode)
	}
	if err := os.WriteFile(pidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	req, _ = http.NewRequest(http.MethodPut, path, strings.NewReader("x"))
	response, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("allowlisted pid status = %d", response.StatusCode)
	}
}
