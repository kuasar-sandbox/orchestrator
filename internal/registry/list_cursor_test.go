package registry

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

type countedListCursorKeys struct {
	metadata.PublicListCursorRepository
	calls int
	fail  error
}

func (k *countedListCursorKeys) GetOrCreatePublicListCursorKey(ctx context.Context) ([32]byte, error) {
	k.calls++
	if k.fail != nil {
		// Even a misbehaving provider's candidate key cannot be cached when
		// the initialization outcome is unknown.
		return [32]byte{1}, k.fail
	}
	return k.PublicListCursorRepository.GetOrCreatePublicListCursorKey(ctx)
}

func TestPublicListCursorPrivateAndPortable(t *testing.T) {
	stores := newTestStores(t)
	keys := &countedListCursorKeys{PublicListCursorRepository: stores.listCursors}
	stores.listCursors = keys
	r := New(stores, 0, nil)
	publishTestGroups(t, r, "node", "g", "h")
	apiKey := testAPIKeyValue()
	for i := range 3 {
		rec := testE2BSandboxRecord("g", fmt.Sprintf("private-foreign-route-%d", i), fmt.Sprintf("sb-%d", i), "node", StateReady)
		if i == 0 {
			rec.APISecret = strings.Repeat("2", 64)
		}
		if _, err := stores.PutSandbox(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
	}
	if keys.calls != 0 {
		t.Fatal("constructor or publication initialized the list key")
	}
	w, _ := requestRouteList(t, r, "not-an-api-key", url.Values{"limit": {"1"}})
	if w.Code != http.StatusForbidden || keys.calls != 0 {
		t.Fatal("unauthenticated request initialized the list key")
	}
	w, rows := requestRouteList(t, r, apiKey, url.Values{"limit": {"1"}})
	token := w.Header().Get("x-next-token")
	if w.Code != http.StatusOK || len(rows) != 0 || token == "" || keys.calls != 1 {
		t.Fatalf("private empty page: status=%d items=%d key reads=%d", w.Code, len(rows), keys.calls)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, publicListCursorPrefix))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-foreign-route") || strings.Contains(token, "private-foreign-route") {
		t.Fatal("opaque continuation exposed another root's route key")
	}
	for _, peer := range []*Registry{r, New(stores, 0, nil), New(stores, 0, nil)} {
		w, rows = requestRouteList(t, peer, apiKey, url.Values{"limit": {"2"}, "nextToken": {token}})
		if w.Code != http.StatusOK || len(rows) != 2 || rows[0].SandboxID != "sb-1" || rows[1].SandboxID != "sb-2" || w.Header().Get("x-next-token") != "" {
			t.Fatalf("Registry peer/restart lost cursor: status=%d rows=%+v", w.Code, rows)
		}
	}
	if keys.calls != 3 {
		t.Fatalf("successful codec not cached per Registry: key reads=%d", keys.calls)
	}
	for _, query := range []url.Values{
		{"limit": {"1"}, "nextToken": {token}, "group": {"h"}},
		{"limit": {"1"}, "nextToken": {token}, "state": {"paused"}},
	} {
		w, _ = requestRouteList(t, r, apiKey, query)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("cursor escaped authenticated scope: status=%d", w.Code)
		}
	}
	w, _ = requestRouteList(t, r, testAPIKeyValue(), url.Values{"nextToken": {token}})
	if w.Code != http.StatusBadRequest {
		t.Fatal("cursor was reusable with a different API token for the same root")
	}
	raw[len(raw)-1] ^= 1
	for _, invalid := range []string{
		publicListCursorPrefix + base64.RawURLEncoding.EncodeToString(raw),
		publicListCursorPrefix, publicListCursorPrefix + "AA", token[:len(token)-12], "cl2_" + token[4:],
		publicListCursorPrefix + strings.Repeat("a", maxPublicListCursorBytes),
	} {
		w, _ = requestRouteList(t, r, apiKey, url.Values{"nextToken": {invalid}})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid encrypted cursor accepted: length=%d status=%d", len(invalid), w.Code)
		}
	}
}

func TestPublicListCursorUnknownInitializationNotCached(t *testing.T) {
	stores := newTestStores(t)
	keys := &countedListCursorKeys{PublicListCursorRepository: stores.listCursors, fail: metadata.ErrUnknown}
	stores.listCursors = keys
	r := New(stores, 0, nil)
	internal := metadata.Cursor(base64.RawURLEncoding.EncodeToString([]byte("internal cursor")))
	if token, err := r.encodePublicListCursor(t.Context(), "g", "", "key", internal); token != "" || !errors.Is(err, metadata.ErrUnknown) || r.listCursor.aead != nil || keys.calls != 1 {
		t.Fatalf("unknown key was cached: token=%q error=%v reads=%d", token, err, keys.calls)
	}
	keys.fail = nil
	token, err := r.encodePublicListCursor(t.Context(), "g", "", "key", internal)
	if err != nil || token == "" || keys.calls != 2 {
		t.Fatalf("subsequent initialization failed: error=%v reads=%d", err, keys.calls)
	}
	peer := New(stores, 0, nil)
	got, err := peer.decodePublicListCursor(t.Context(), "g", "", "key", token)
	if err != nil || got != internal {
		t.Fatal("successful retry cursor does not use durable key")
	}
}

func TestPublicListCursorMaximumLogicalKey(t *testing.T) {
	r := New(newTestStores(t), 0, nil)
	// The internal decoder permits at most 9,016 bytes before base64 encoding.
	// Encryption must not make an otherwise valid maximum cursor unpageable.
	internal := metadata.Cursor(base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", 9016))))
	token, err := r.encodePublicListCursor(t.Context(), "g", "", "key", internal)
	if err != nil || len(token) > maxPublicListCursorBytes {
		t.Fatalf("maximum cursor cannot fit: length=%d err=%v", len(token), err)
	}
	got, err := r.decodePublicListCursor(t.Context(), "g", "", "key", token)
	if err != nil || got != internal {
		t.Fatalf("maximum cursor lost identity: %v", err)
	}
}

func TestPublicListLongRouteKeyPagination(t *testing.T) {
	r := New(newTestStores(t), 0, nil)
	publishTestGroups(t, r, "node", "g")
	for i, routeKey := range []string{strings.Repeat("x", 4096), strings.Repeat("y", 4096)} {
		record := testE2BSandboxRecord("g", routeKey, fmt.Sprintf("sb-%d", i), "node", StateReady)
		if _, err := r.stores.PutSandbox(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	key := testAPIKeyValue()
	w, rows := requestRouteList(t, r, key, url.Values{"limit": {"1"}})
	if w.Code != http.StatusOK || len(rows) != 1 || w.Header().Get("x-next-token") == "" {
		t.Fatalf("maximum route key first page: status=%d rows=%+v", w.Code, rows)
	}
	first := rows[0].SandboxID
	w, rows = requestRouteList(t, r, key, url.Values{"limit": {"1"}, "nextToken": {w.Header().Get("x-next-token")}})
	if w.Code != http.StatusOK || len(rows) != 1 || rows[0].SandboxID == first || w.Header().Get("x-next-token") != "" {
		t.Fatalf("maximum route key continuation: status=%d rows=%+v", w.Code, rows)
	}
}
