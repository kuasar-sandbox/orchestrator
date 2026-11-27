// Package testsecret provides a real HTTP-over-UDS broker for integration tests.
package testsecret

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
)

// Broker serves mutable test values and counts requests by opaque key.
type Broker struct {
	mu       sync.Mutex
	values   map[string][]byte
	requests map[string]int
}

// Start configures the process default resolver's "test" plugin with caching
// disabled. Call it once per test process, before the first plugin reference;
// keep related scenarios as subtests because the SDK freezes process config.
func Start(t testing.TB, values map[string][]byte) *Broker {
	t.Helper()
	dir, err := os.MkdirTemp("", "kuasar-secret-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{values: make(map[string][]byte), requests: make(map[string]int)}
	for key, value := range values {
		b.Set(key, value)
	}
	server := &http.Server{Handler: http.HandlerFunc(b.serveHTTP)}
	t.Cleanup(func() { _ = server.Close() })
	go func() { _ = server.Serve(listener) }()
	t.Setenv(secret.Env, "test::"+socket+":cache_ttl=0")
	return b
}

// Set replaces a value. A nil value makes the key return HTTP 404.
func (b *Broker) Set(key string, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if value == nil {
		delete(b.values, key)
		return
	}
	b.values[key] = append([]byte(nil), value...)
}

func (b *Broker) Count(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requests[key]
}

func (b *Broker) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/secrets/resolve" {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.requests[req.Key]++
	value, ok := b.values[req.Key]
	value = append([]byte(nil), value...)
	b.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"key": req.Key, "value": "base64:" + base64.StdEncoding.EncodeToString(value),
	})
}
