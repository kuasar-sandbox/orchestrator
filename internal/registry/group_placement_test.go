package registry

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	requestplacement "github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func registryGroupEntry(t *testing.T) (string, nodegroup.Entry) {
	t.Helper()
	raw, _ := hex.DecodeString(strings.Repeat("3", 64))
	key, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	return key, nodegroup.Entry{AuthRef: hex.EncodeToString(apikey.FullFingerprint(raw)), AuthMaterial: nodegroup.AuthMaterial{APISecret: hex.EncodeToString(raw)}, Routing: nodegroup.Routing{TargetPort: 49983}}
}

func reportRegistryGroup(t *testing.T, r *Registry, id string, labels map[string]string, group nodegroup.Support) metadata.Registration {
	t.Helper()
	node := &NodeRecord{NodeID: id, Labels: labels, APIEndpoint: "api", DataEndpoint: "data"}
	if err := r.stores.PutNode(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	stream := groupReportStream{}
	for _, msg := range []*routesync.Msg{
		{Type: routesync.TypeGroupBegin, Group: &routesync.GroupReport{SyncID: "sync-" + id, Sequence: group.Version, Total: 1}},
		{Type: routesync.TypeGroupBatch, Group: &routesync.GroupReport{SyncID: "sync-" + id, Sequence: group.Version, Groups: []nodegroup.Support{group}}},
		{Type: routesync.TypeGroupEnd, Group: &routesync.GroupReport{SyncID: "sync-" + id, Sequence: group.Version}},
	} {
		if err := stream.apply(t.Context(), r.stores.groups, node.Registration, msg); err != nil {
			t.Fatal(err)
		}
	}
	return node.Registration
}

func TestGroupPlacementAuthenticationDefaultExplicitAndRuntime(t *testing.T) {
	r := New(newTestStores(t), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	key, entry := registryGroupEntry(t)
	ref := "manifest://" + strings.Repeat("a", 64)
	legacy := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: ref}.String()
	annotated := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: ref + "@runtime:one.bundle:digest:" + strings.Repeat("b", 64)}.String()
	group := nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}, DefaultContext: &nodegroup.DefaultContext{TemplateID: annotated}}
	reportRegistryGroup(t, r, "runtime", map[string]string{requestplacement.RuntimeLabelPrefix + "one.bundle": "digest:" + strings.Repeat("b", 64), "pool": "runtime"}, group)
	group.DefaultContext = nil
	reportRegistryGroup(t, r, "explicit", map[string]string{"pool": "explicit", requestplacement.RuntimeLabelPrefix + "other.bundle": "digest:" + strings.Repeat("b", 64)}, group)
	request := PlaceRequest{Group: "g", APIKey: key}
	selected, err := r.selectGroupNode(t.Context(), request)
	if err != nil || selected.NodeID != "runtime" || selected.TemplateRef != annotated || selected.TemplateSource != "default" || selected.TargetPort != 49983 || selected.GroupAdmission == nil {
		t.Fatalf("default choice=%+v %v", selected, err)
	}
	request.TemplateID = &legacy
	request.Constraints = requestplacement.Request{NodeSelectors: []map[string]string{{"pool": "explicit"}}}
	selected, err = r.selectGroupNode(t.Context(), request)
	if err != nil || selected.NodeID != "explicit" || selected.TemplateRef != legacy || selected.TemplateSource != "explicit" {
		t.Fatalf("legacy explicit choice=%+v %v", selected, err)
	}
	request.TemplateID = &annotated
	if _, err := r.selectGroupNode(t.Context(), request); !errors.Is(err, ErrNoNode) {
		t.Fatalf("runtime accepted on another selected Node: %v", err)
	}
	request.Constraints = requestplacement.Request{NodeSelectors: []map[string]string{{}}}
	selected, err = r.selectGroupNode(t.Context(), request)
	if err != nil || selected.NodeID != "runtime" {
		t.Fatalf("empty OR branch relaxed runtime: %+v %v", selected, err)
	}
	request.RequiredNodeID = "explicit"
	if _, err := r.selectGroupNode(t.Context(), request); !errors.Is(err, ErrNoNode) {
		t.Fatalf("fixed Node bypassed runtime: %v", err)
	}
	request.RequiredNodeID, request.TemplateID, request.Build = "explicit", nil, true
	selected, err = r.selectGroupNode(t.Context(), request)
	if err != nil || selected.NodeID != "explicit" || selected.TemplateRef != "" {
		t.Fatalf("Build inherited default template filtering: %+v %v", selected, err)
	}
	request.Build, request.Migration = false, true
	selected, err = r.selectGroupNode(t.Context(), request)
	if err != nil || selected.NodeID != "explicit" || selected.TemplateRef != "" {
		t.Fatalf("opaque-token import inferred a default runtime: %+v %v", selected, err)
	}
	// Preserve the candidate fingerprint and change only the authentication MAC.
	request.APIKey = key[:len(key)-1] + map[bool]string{true: "1", false: "0"}[strings.HasSuffix(key, "0")]
	if _, err := r.selectGroupNode(t.Context(), request); !errors.Is(err, ErrReserveForbidden) {
		t.Fatalf("bad MAC authenticated: %v", err)
	}
}

func TestGroupReportRejectsLateRegistrationAndSequenceGap(t *testing.T) {
	r := New(newTestStores(t), time.Second, nil)
	_, entry := registryGroupEntry(t)
	group := nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}}
	old := reportRegistryGroup(t, r, "node", nil, group)
	stream := groupReportStream{syncID: "sync-node", sequence: 1, complete: true}
	group.Version = 3
	gap := &routesync.Msg{Type: routesync.TypeGroupUpsert, Group: &routesync.GroupReport{Sequence: 3, Groups: []nodegroup.Support{group}}}
	if err := stream.apply(t.Context(), r.stores.groups, old, gap); !errors.Is(err, metadata.ErrGroupResync) {
		t.Fatalf("missing sequence accepted: %v", err)
	}
	group.Version = 2
	fresh := reportRegistryGroup(t, r, "node", nil, group)
	if fresh == old {
		t.Fatal("replacement registration did not change")
	}
	late := &routesync.Msg{Type: routesync.TypeGroupDelete, Group: &routesync.GroupReport{Sequence: 2, Deleted: "g"}}
	if err := stream.apply(t.Context(), r.stores.groups, old, late); !errors.Is(err, metadata.ErrConflict) {
		t.Fatalf("old stream deleted new support: %v", err)
	}
	selected, err := r.stores.groups.ReadGroupSupport(t.Context(), "g", "node")
	if err != nil || !selected.Support.Exists || selected.Support.Value.GroupVersion != 2 || selected.Node.Registration != fresh {
		t.Fatalf("replacement report damaged: %+v %v", selected, err)
	}
}
