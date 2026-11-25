package registry

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

type listRouteReads struct {
	metadata.RouteRepository
	calls, rows int
	limits      []int
}

func (s *listRouteReads) ListRoutes(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.SandboxRecord]], error) {
	s.calls++
	s.limits = append(s.limits, limit)
	page, err := s.RouteRepository.ListRoutes(ctx, group, cursor, limit)
	s.rows += len(page.Items)
	return page, err
}

func requestRouteList(t *testing.T, r *Registry, key string, query url.Values) (*httptest.ResponseRecorder, []ListItem) {
	t.Helper()
	if !query.Has("group") {
		query.Set("group", "g")
	}
	req := httptest.NewRequest(http.MethodGet, RouteLinkListPath+"?"+query.Encode(), nil)
	req.Header.Set("X-API-KEY", key)
	w := httptest.NewRecorder()
	r.serveList(w, req)
	var out []ListItem
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || out == nil {
			t.Fatalf("list must return a JSON array: %s (%v)", w.Body.String(), err)
		}
	}
	return w, out
}

func TestRouteListBoundsDefaultAndMaximumPage(t *testing.T) {
	r := New(newTestStores(t), 0, nil)
	publishTestGroups(t, r, "node", "g")
	for i := range 1005 {
		rec := testE2BSandboxRecord("g", fmt.Sprintf("route-%04d", i), fmt.Sprintf("sb-%04d", i), "node", StateReady)
		if _, err := r.stores.PutSandbox(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
	}
	reads := &listRouteReads{RouteRepository: r.stores.routes}
	r.stores.routes = reads
	key := testAPIKeyValue()
	w, rows := requestRouteList(t, r, key, url.Values{})
	if w.Code != http.StatusOK || len(rows) != 100 || reads.rows != 100 || reads.calls != 1 || w.Header().Get("x-next-token") == "" {
		t.Fatalf("default page: status=%d items=%d reads=%+v", w.Code, len(rows), reads)
	}
	if rows[0].SandboxID != "sb-0000" || rows[99].SandboxID != "sb-0099" {
		t.Fatal("default page skipped or duplicated routes")
	}
	firstCursor := w.Header().Get("x-next-token")
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(firstCursor, publicListCursorPrefix))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{key, testAPISecret, testEnvdAccessToken, testTrafficAccessToken} {
		if strings.Contains(w.Body.String(), secret) || strings.Contains(string(decoded), secret) {
			t.Fatal("list body or continuation exposed protected credentials")
		}
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("authenticated list is cacheable")
	}
	reads.calls, reads.rows, reads.limits = 0, 0, nil
	w, rows = requestRouteList(t, r, key, url.Values{"limit": {"1000"}})
	if w.Code != http.StatusOK || len(rows) != 1000 || reads.rows != 1000 || reads.calls != 8 || w.Header().Get("x-next-token") == "" {
		t.Fatalf("maximum page: status=%d items=%d reads=%+v", w.Code, len(rows), reads)
	}
	for _, limit := range reads.limits {
		if limit > 128 {
			t.Fatalf("unbounded repository request: %d", limit)
		}
	}
	w, rows = requestRouteList(t, r, key, url.Values{"limit": {"1000"}, "nextToken": {firstCursor}})
	if w.Code != http.StatusOK || len(rows) != 905 || rows[0].SandboxID != "sb-0100" || rows[904].SandboxID != "sb-1004" || w.Header().Get("x-next-token") != "" {
		t.Fatalf("continuation lost or duplicated rows: status=%d items=%d next=%q", w.Code, len(rows), w.Header().Get("x-next-token"))
	}
}

func TestRouteListFilteredEmptyPagesKeepProgress(t *testing.T) {
	r := New(newTestStores(t), 0, nil)
	publishTestGroups(t, r, "node", "g")
	want := map[string]bool{}
	for i := range 173 {
		rec := testE2BSandboxRecord("g", fmt.Sprintf("route-%04d", i), fmt.Sprintf("sb-%04d", i), "node", StateReady)
		if i < 51 {
			// A candidate/full fingerprint alone is not authorization. A
			// mismatching secret must still fail complete token verification.
			rec.APISecret = strings.Repeat("2", 64)
		} else if i%3 == 0 {
			rec.State = StateDead
		} else {
			rec.State = StatePaused
			want[rec.SandboxID] = true
		}
		if _, err := r.stores.PutSandbox(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
	}
	reads := &listRouteReads{RouteRepository: r.stores.routes}
	r.stores.routes = reads
	seen, tokens := map[string]bool{}, map[string]bool{}
	key := testAPIKeyValue()
	next := ""
	for page := 0; ; page++ {
		before := reads.rows
		w, rows := requestRouteList(t, r, key, url.Values{"limit": {"17"}, "nextToken": {next}})
		if w.Code != http.StatusOK || reads.rows-before > 17 {
			t.Fatalf("filtered page=%d status=%d scanned=%d", page, w.Code, reads.rows-before)
		}
		if page == 0 && (len(rows) != 0 || w.Header().Get("x-next-token") == "") {
			t.Fatal("unauthorized empty page lost its continuation")
		}
		for _, item := range rows {
			if !want[item.SandboxID] || seen[item.SandboxID] || item.State != "paused" {
				t.Fatalf("unauthorized, wrong-state, or duplicate row: %+v", item)
			}
			seen[item.SandboxID] = true
		}
		next = w.Header().Get("x-next-token")
		if next == "" {
			break
		}
		if tokens[next] || page > 20 {
			t.Fatal("cursor failed to advance")
		}
		tokens[next] = true
	}
	if len(seen) != len(want) || reads.rows != 173 {
		t.Fatalf("filtered traversal: visible=%d want=%d scanned=%d", len(seen), len(want), reads.rows)
	}
}

func TestRouteListStateFilterAndInvalidInput(t *testing.T) {
	r := New(newTestStores(t), 0, nil)
	publishTestGroups(t, r, "node", "g", "h")
	for i, state := range []SandboxState{StateReady, StatePausing, StatePaused, StateReserved, StateDead, StateDeleting} {
		rec := testE2BSandboxRecord("g", fmt.Sprintf("route-%04d", i), fmt.Sprintf("sb-%04d", i), "node", state)
		if state == StateReserved {
			rec.APISecret = "" // retained Group root fallback remains authenticated
		}
		if _, err := r.stores.PutSandbox(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
		w, rows := requestRouteList(t, r, testAPIKeyValue(), url.Values{"state": {string(state)}})
		if w.Code != http.StatusOK || len(rows) != 1 || rows[0].State != string(state) {
			t.Fatalf("state %q: status=%d rows=%+v", state, w.Code, rows)
		}
	}
	w, rows := requestRouteList(t, r, testAPIKeyValue(), url.Values{"state": {"running"}})
	if w.Code != http.StatusOK || len(rows) != 1 || rows[0].State != "ready" {
		t.Fatal("running filter did not select the existing ready projection")
	}
	w, _ = requestRouteList(t, r, testAPIKeyValue(), url.Values{"limit": {"1"}})
	cursor := w.Header().Get("x-next-token")
	for _, query := range []url.Values{
		{"limit": {"0"}}, {"limit": {"-1"}}, {"limit": {"1001"}}, {"limit": {"x"}}, {"limit": {""}}, {"limit": {"1", "2"}},
		{"state": {"unknown"}}, {"state": {"paused", "dead"}}, {"nextToken": {"malformed"}}, {"nextToken": {cursor + "="}},
		{"nextToken": {cursor, cursor}}, {"group": {"h"}, "nextToken": {cursor}},
	} {
		w, _ := requestRouteList(t, r, testAPIKeyValue(), query)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid list input accepted: %v status=%d body=%s", query, w.Code, w.Body.String())
		}
	}
	key := testAPIKeyValue()
	bad, _ := hex.DecodeString(strings.TrimPrefix(key, "e2b_"))
	bad[len(bad)-1] ^= 1
	w, _ = requestRouteList(t, r, "e2b_"+hex.EncodeToString(bad), url.Values{"nextToken": {cursor}})
	if w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "sb-") {
		t.Fatal("continuation bypassed full API key MAC validation")
	}
}
