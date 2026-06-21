package groupcfg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExternalKeyProviderAndCache(t *testing.T) {
	ctx := context.Background()
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pathKey && r.Header.Get(GroupHeader) == "/g" {
			atomic.AddInt64(&hits, 1)
			_ = json.NewEncoder(w).Encode(Key{ProjectID: "p1", ManifestKey: "abcd"})
			return
		}
		w.WriteHeader(http.StatusNotFound) // unknown group
	}))
	defer srv.Close()

	kp := NewExternalKey(strings.TrimPrefix(srv.URL, "http://"), nil, time.Minute)

	k, found, err := kp.Key(ctx, "/g")
	if err != nil || !found || k.ManifestKey != "abcd" || k.ProjectID != "p1" {
		t.Fatalf("external key: %+v found=%v err=%v", k, found, err)
	}
	// Second lookup is served from the TTL cache (no extra HTTP hit).
	if _, _, err := kp.Key(ctx, "/g"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt64(&hits); n != 1 {
		t.Fatalf("cache miss: %d HTTP hits, want 1", n)
	}
	// Unknown group → found=false (a 404), not an error.
	if _, found, err := kp.Key(ctx, "/nope"); err != nil || found {
		t.Fatalf("unknown group: found=%v err=%v", found, err)
	}
}

func TestExternalProviderUnavailableIsError(t *testing.T) {
	// A dead endpoint surfaces as an error (callers map it to 503), not found=false.
	kp := NewExternalKey("127.0.0.1:1", nil, time.Minute) // nothing listening
	if _, _, err := kp.Key(context.Background(), "/g"); err == nil {
		t.Fatal("expected an error from an unreachable provider")
	}
}
