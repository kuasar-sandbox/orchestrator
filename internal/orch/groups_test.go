package orch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestGroupHTTPUDSAndNodeLinkShareAuthority(t *testing.T) {
	o := testOrch(t)
	key, _, _ := allowlistedBuildIdentity(t, o)
	handler := api.New(o, "test", api.Resources{}, o.log).Handler()
	socket := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	group := "/tenant/group"
	events, unsubscribe := o.SubscribeGroups()
	defer unsubscribe()
	request := func(method, path, body, token string, want int) map[string]any {
		t.Helper()
		req, err := http.NewRequest(method, "http://node"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("X-API-KEY", token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("%s: status=%d body=%s", method, resp.StatusCode, raw)
		}
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return out
	}
	path := "/groups?group=" + url.QueryEscape(group)
	request(http.MethodPut, path, `{"expected_version":0,"context":{}}`, "", 401)
	created := request(http.MethodPut, path, `{"expected_version":0,"context":{}}`, key, 200)
	version := int64(created["version"].(float64))
	if e := nextGroupEvent(t, events); e.Sequence != version || e.Support == nil || e.Support.DefaultContext != nil {
		t.Fatalf("create event=%+v", e)
	}
	updated := request(http.MethodPut, path, fmt.Sprintf(`{"expected_version":%d,"context":{"target_port":8080}}`, version), key, 200)
	version = int64(updated["version"].(float64))
	if e := nextGroupEvent(t, events); e.Sequence != version || e.Support.Entries[0].Routing.TargetPort != 8080 {
		t.Fatalf("update event=%+v", e)
	}
	request(http.MethodPut, path, fmt.Sprintf(`{"expected_version":%d,"context":{"target_port":8080}}`, version), key, 200)
	select {
	case e := <-events:
		t.Fatalf("no-op published delta: %+v", e)
	default:
	}
	ack := o.HandleCommand(context.Background(), &routesync.Command{CmdID: "get", Kind: routesync.CmdGroupGet, Group: &routesync.GroupCommand{Group: group, APIKey: key}})
	if ack.Status != routesync.AckAccepted || ack.GroupDetail == nil || ack.GroupDetail.Version != version {
		t.Fatalf("NodeLink did not read HTTP state: %+v", ack)
	}
	request(http.MethodPut, path, `{"expected_version":0,"context":{}}`, key, 409)
	for _, body := range []string{`{`, `{"expected_version":0,"context":{},"key_put":{}}`, `{"expected_version":0,"expected_version":1,"context":{}}`} {
		request(http.MethodPut, path, body, key, 400)
	}
	deleted := request(http.MethodDelete, path+fmt.Sprintf("&expected_version=%d", version), "", key, 200)
	if e := nextGroupEvent(t, events); e.Deleted != group || e.Sequence != int64(deleted["version"].(float64)) {
		t.Fatalf("delete event=%+v", e)
	}
	// The same handler is used by TCP. A valid-looking token with a bad MAC
	// must not receive the retained tombstone version.
	bad := key[:len(key)-1] + map[bool]string{true: "1", false: "0"}[strings.HasSuffix(key, "0")]
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-API-KEY", bad)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 || strings.Contains(w.Body.String(), `"version"`) {
		t.Fatalf("bad MAC learned Group state: %d %s", w.Code, w.Body.String())
	}
}

func nextGroupEvent(t *testing.T, events <-chan routesync.GroupEvent) routesync.GroupEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("missing Group event")
		return routesync.GroupEvent{}
	}
}
