package placer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// The transport fixture calls the production SQLite Group transactions. Faults
// distinguish an unexecuted delivery from a committed delivery with a lost ACK.
type importNodeFixture struct {
	st                  *store.Store
	svc                 *Service
	registration        metadata.Registration
	puts                []routesync.GroupCommand
	loseACK, loseBefore bool
	beforePut           func()
}

func newImportNode(t *testing.T) *importNodeFixture {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	f := &importNodeFixture{st: st, registration: metadata.Registration{NodeID: "node", LinkOwner: "registry", Fence: metadata.Version{9: 1}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in registry.NodeGroupRequest
		if req.URL.Path != registry.PlacerLinkNodeGroupPath || json.NewDecoder(req.Body).Decode(&in) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		ack := &routesync.CmdAck{CmdID: "receipt", Status: routesync.AckAccepted}
		var err error
		switch in.Kind {
		case routesync.CmdGroupGet:
			detail, e := st.GroupGet(req.Context(), in.Group.APIKey, in.Group.Group)
			ack.GroupDetail, err = &detail, e
		case routesync.CmdGroupPut:
			f.puts = append(f.puts, in.Group)
			if f.loseBefore {
				f.loseBefore = false
				http.Error(w, "injected disconnect", 503)
				return
			}
			if f.beforePut != nil {
				f.beforePut()
			}
			result, _, e := st.GroupPut(req.Context(), in.Group.APIKey, in.Group.Group, *in.Group.ExpectedVersion, *in.Group.Context, in.Group.KeyPut)
			ack.GroupResult, err = &result, e
			if f.loseACK {
				f.loseACK = false
				http.Error(w, "injected lost ACK", 503)
				return
			}
		default:
			http.Error(w, "unexpected command", 400)
			return
		}
		if err != nil {
			ack.Status, ack.HTTPStatus = routesync.AckRejected, nodegroup.HTTPStatus(err)
			var version *nodegroup.VersionError
			if errors.As(err, &version) {
				ack.CurrentGroupVersion = &version.Version
			}
		}
		_ = json.NewEncoder(w).Encode(registry.NodeGroupResponse{NodeID: "node", Registration: f.registration, Ack: ack})
	}))
	t.Cleanup(srv.Close)
	f.svc = NewRemoteLinks([]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}}, clustercfg.PlacementConfig{}, nil)
	return f
}

func importTestCommand(t *testing.T, port int) routesync.GroupCommand {
	t.Helper()
	cmd, err := importCommand(clusterstate.SandboxGroupRecord{
		Group: "/group", TargetPort: port,
		APISecret:   clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("1", 64)},
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("2", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func (f *importNodeFixture) maintain(t *testing.T, cmd routesync.GroupCommand, apply bool) ImportResult {
	t.Helper()
	return f.svc.maintainGroup(t.Context(), importTarget{"source", "node", "/group"}, cmd, importLeaseToken{sourceID: "source"}, apply)
}

func TestGroupImportConditionalRenewalAndExplicitUpdate(t *testing.T) {
	f := newImportNode(t)
	cmd := importTestCommand(t, 123)
	created := f.maintain(t, cmd, false)
	if created.Status != "committed" || !created.KeyLeaseConfirmed {
		t.Fatalf("create: %+v", created)
	}
	same := f.maintain(t, cmd, false)
	if same.Status != "unchanged" || len(f.puts) != 1 {
		t.Fatalf("unneeded renewal: %+v puts=%d", same, len(f.puts))
	}
	changed := importTestCommand(t, 456)
	conflict := f.maintain(t, changed, false)
	if conflict.Status != "conflict" || len(f.puts) != 1 {
		t.Fatalf("background overwrite: %+v", conflict)
	}
	updated := f.maintain(t, changed, true)
	if updated.Status != "committed" || updated.Version <= created.Version {
		t.Fatalf("apply: %+v", updated)
	}
	f.registration.Fence[9]++
	renewed := f.maintain(t, changed, false)
	if renewed.Status != "committed" || renewed.Version != updated.Version || len(f.puts) != 3 {
		t.Fatalf("registration reused stale lease cache: %+v", renewed)
	}
	deleted, err := f.st.GroupDelete(t.Context(), cmd.APIKey, "/group", updated.Version)
	if err != nil {
		t.Fatal(err)
	}
	for _, apply := range []bool{false, true} {
		result := f.maintain(t, changed, apply)
		if result.Status != "conflict" || result.Version != deleted.Version || len(f.puts) != 3 {
			t.Fatalf("tombstone revived: %+v", result)
		}
	}
}

func TestGroupImportUnknownIsNotLeaseConfirmation(t *testing.T) {
	f := newImportNode(t)
	cmd := importTestCommand(t, 123)
	f.loseACK = true
	if got := f.maintain(t, cmd, false); got.Status != "unknown" || got.KeyLeaseConfirmed {
		t.Fatalf("lost ACK: %+v", got)
	}
	observed := f.maintain(t, cmd, false)
	if observed.Status != "observed" || observed.KeyLeaseConfirmed || len(f.puts) != 1 {
		t.Fatalf("GET invented lease ACK: %+v", observed)
	}
	renewed := f.maintain(t, cmd, false)
	if renewed.Status != "committed" || !renewed.KeyLeaseConfirmed || renewed.Version != observed.Version || len(f.puts) != 2 {
		t.Fatalf("safe equivalent renewal: %+v", renewed)
	}
}

func TestGroupImportUnknownRetryKeepsOriginalPayload(t *testing.T) {
	f := newImportNode(t)
	original := importTestCommand(t, 123)
	f.loseBefore = true
	if got := f.maintain(t, original, false); got.Status != "unknown" {
		t.Fatalf("lost delivery: %+v", got)
	}
	changed := importTestCommand(t, 456)
	changed.KeyPut.ExpiresUnix += 1000
	got := f.maintain(t, changed, true)
	if got.Status != "committed" || len(f.puts) != 2 {
		t.Fatalf("retry: %+v", got)
	}
	first, _ := json.Marshal(f.puts[0])
	second, _ := json.Marshal(f.puts[1])
	if string(first) != string(second) {
		t.Fatal("unknown retry changed original CAS/payload/expiry/identity")
	}
	detail, err := f.st.GroupGet(t.Context(), original.APIKey, "/group")
	if err != nil || detail.Context.TargetPort != 123 {
		t.Fatalf("unknown result rebased: %v", err)
	}
}

func TestGroupImportUnknownCannotOverwriteConcurrentDefinition(t *testing.T) {
	f := newImportNode(t)
	cmd := importTestCommand(t, 123)
	f.loseACK = true
	_ = f.maintain(t, cmd, false)
	detail, err := f.st.GroupGet(t.Context(), cmd.APIKey, "/group")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.st.GroupPut(t.Context(), cmd.APIKey, "/group", detail.Version, nodegroup.Context{TargetPort: 789}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := f.maintain(t, cmd, true)
	if got.Status != "conflict" || len(f.puts) != 1 {
		t.Fatalf("unknown auto-rebase: %+v", got)
	}
}

func TestGroupImportApplyStopsAfterSingleCASConflict(t *testing.T) {
	f := newImportNode(t)
	cmd := importTestCommand(t, 123)
	first := f.maintain(t, cmd, false)
	f.beforePut = func() {
		_, _, err := f.st.GroupPut(context.Background(), cmd.APIKey, "/group", first.Version, nodegroup.Context{TargetPort: 789}, nil)
		if err != nil {
			panic(err)
		}
	}
	got := f.maintain(t, importTestCommand(t, 456), true)
	if got.Status != "conflict" || len(f.puts) != 2 {
		t.Fatalf("apply re-read/rebased: %+v", got)
	}
}

func TestGroupImportLegacyMappingAndStaticTargets(t *testing.T) {
	legacy := "bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte("manifest://"+strings.Repeat("a", 64)))
	rec := clusterstate.SandboxGroupRecord{Group: "g", ProjectID: "project", TemplateRef: legacy, Config: map[string]string{"x": "y"},
		ImageRepo: "private/repo", RegistryAuth: clusterstate.Secret{Type: clusterstate.SecretInline, Value: "auth"},
		Metadata: map[string]string{"business": "data"}, TargetPort: 99, ManifestKey: clusterstate.Secret{Value: strings.Repeat("2", 64)}}
	cmd, err := importCommand(rec)
	if err != nil || cmd.Context.TemplateID == nil || *cmd.Context.TemplateID != legacy || cmd.Context.ProjectID != "project" || cmd.Context.RegistryAuth.Value != "auth" || cmd.KeyPut == nil {
		t.Fatalf("mapping: %v", err)
	}
	_, targets := importTargets("g", []*registry.NodeRecord{
		{NodeID: "hot", Draining: true, Zone: "critical", Labels: map[string]string{"pool": "p"}},
		{NodeID: "empty-runtime", Labels: map[string]string{"pool": "p"}},
		{NodeID: "other", Labels: map[string]string{"pool": "q"}},
	}, []map[string]string{{"pool": "p"}}, nil)
	if strings.Join(targets, ",") != "empty-runtime,hot" {
		t.Fatalf("static targets affected by creation admission: %v", targets)
	}
	rec.TemplateRef = ""
	if _, err := importCommand(rec); err == nil {
		t.Fatal("config without default template accepted")
	}
	rec.Config = nil
	cmd, err = importCommand(rec)
	if err != nil || cmd.Context.TemplateID != nil {
		t.Fatalf("Group without default rejected: %v", err)
	}
	rec.ManifestKey = clusterstate.Secret{Type: clusterstate.SecretRef, Value: "unresolved", Fingerprint: strings.Repeat("a", 64)}
	if _, err := importCommand(rec); err == nil {
		t.Fatal("unresolved secret accepted")
	}
}

func TestGroupImportStateBudgetKeepsUnknownWrites(t *testing.T) {
	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{}, nil)
	for i := 0; i < maxImportBindings; i++ {
		state, err := svc.importState(importTarget{group: time.Unix(int64(i), 0).String()})
		if err != nil {
			t.Fatal(err)
		}
		state.pending = &routesync.GroupCommand{}
	}
	if _, err := svc.importState(importTarget{group: "overflow"}); err == nil {
		t.Fatal("unbounded unknown cache")
	}
	if len(svc.importStates) != maxImportBindings {
		t.Fatal("unknown operation evicted")
	}
}
