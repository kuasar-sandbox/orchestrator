package placer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

type bootstrapReadinessSink struct {
	viewSink
	resetApplied, bookmarkApplied chan struct{}
}

func (s bootstrapReadinessSink) reset()    { s.viewSink.reset(); close(s.resetApplied) }
func (s bootstrapReadinessSink) bookmark() { s.viewSink.bookmark(); close(s.bookmarkApplied) }

func TestBootstrapMembershipDoesNotBypassWatchBookmark(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == clusterclient.MembershipPath {
			json.NewEncoder(w).Encode(clustercfg.MembershipConfig{Active: 1, Versions: []clustercfg.MembershipVersion{{Version: 1, Members: []clustercfg.MembershipMember{{ID: "r"}}}}, Owners: clustercfg.MembershipOwnerConfig{NodeList: 1}})
			return
		}
		writeViewFrameForTest(t, w, &registry.ViewEvent{Type: "reset"})
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		writeViewFrameForTest(t, w, &registry.ViewEvent{Type: "bookmark", Token: "registry.1.test:1"})
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	reg, err := clusterclient.NewRegistry(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	eps, err := reg.NodeListEndpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	label, err := reg.ActiveLabel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sink := bootstrapReadinessSink{svc.nodes.sourceWithNotifyLabel("node_list", label, nil), make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.subscribeOnce(ctx, RegistryLink{Name: eps[0].MemberID, BaseURL: eps[0].BaseURL, Client: eps[0].Client}, "/watch", "", sink)
	}()
	select {
	case <-sink.resetApplied:
	case <-ctx.Done():
		t.Fatal("reset not consumed")
	}
	if svc.ReadyForLabel(label) {
		t.Fatal("membership and reset incorrectly made placer ready")
	}
	close(release)
	select {
	case <-sink.bookmarkApplied:
	case <-ctx.Done():
		t.Fatal("bookmark not consumed")
	}
	if !svc.ReadyForLabel(label) {
		t.Fatal("bookmark did not establish ready_label")
	}
	cancel()
	<-done
}
