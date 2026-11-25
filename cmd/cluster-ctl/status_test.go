package main

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

func TestStatusUsesAuthenticatedReadAndProjectsResponse(t *testing.T) {
	t.Setenv("E2B_API_KEY", "private-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != registry.RouteLinkGroupStatusPath || r.URL.Query().Get("group") != "/g" || r.URL.Query().Get("node_id") != "node" || r.Header.Get("X-API-KEY") != "private-key" {
			t.Errorf("unexpected status request")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"group": "/g", "node_id": "node", "storage_readable": true, "published": false, "reason": "support_unavailable", "private_context": "must-not-print"})
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := statusCmd([]string{"--registry", server.URL, "--group", "/g", "--node-id", "node"}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "must-not-print") || strings.Contains(out.String(), "private-key") || !strings.Contains(out.String(), "support_unavailable") {
		t.Fatalf("unsafe/missing projection: %s", out.String())
	}
}

func TestStatusDoesNotForwardCredentialsOrEchoErrorBodies(t *testing.T) {
	t.Setenv("E2B_API_KEY", "private-key")
	for _, status := range []int{http.StatusFound, http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			redirect := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("redirect received credentials") }))
			defer redirect.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", redirect.URL)
				w.WriteHeader(status)
				_, _ = w.Write([]byte("private-database-details"))
			}))
			defer server.Close()
			var out bytes.Buffer
			err := statusCmd([]string{"--registry", server.URL, "--group", "g", "--node-id", "node"}, &out)
			if err == nil || strings.Contains(err.Error(), "private-database") || out.Len() != 0 {
				t.Fatalf("request error not isolated: %v %s", err, out.String())
			}
		})
	}
}

func TestStatusVerifiesTLSAndResponseIdentity(t *testing.T) {
	t.Setenv("E2B_API_KEY", "private-key")
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(registry.GroupStatus{Group: "g", NodeID: "node", StorageReadable: true, Reason: "support_unavailable"})
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := statusCmd([]string{"--registry", server.URL, "--group", "g", "--node-id", "node"}, &out); err == nil {
		t.Fatal("untrusted TLS accepted")
	}
	if requests.Load() != 0 {
		t.Fatal("untrusted TLS received a Group key")
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := statusCmd([]string{"--registry", server.URL, "--group", "g", "--node-id", "node", "--ca", ca}, &out); err != nil || requests.Load() != 1 {
		t.Fatalf("trusted TLS failed: %v requests=%d", err, requests.Load())
	}
	out.Reset()
	wrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(registry.GroupStatus{Group: "other", NodeID: "node", StorageReadable: true})
	}))
	defer wrong.Close()
	if err := statusCmd([]string{"--registry", wrong.URL, "--group", "g", "--node-id", "node"}, &out); err == nil || out.Len() != 0 {
		t.Fatalf("wrong Group projection accepted: %v %s", err, out.String())
	}
}
