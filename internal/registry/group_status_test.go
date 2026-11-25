package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	requestplacement "github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func readTestGroupStatus(t *testing.T, r *Registry, key, node, template string) (*httptest.ResponseRecorder, GroupStatus) {
	t.Helper()
	query := url.Values{"group": {"g"}, "node_id": {node}}
	if template != "" {
		query.Set("template_id", template)
	}
	req := httptest.NewRequest(http.MethodGet, RouteLinkGroupStatusPath+"?"+query.Encode(), nil)
	req.Header.Set("X-API-KEY", key)
	mux := http.NewServeMux()
	r.ServeRouteLink(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	var status GroupStatus
	if response.Code == http.StatusOK {
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
	}
	return response, status
}

func TestGroupStatusAuthenticatesExactSupportAndRedactsPrivateFields(t *testing.T) {
	r := testReg(t)
	key, entry := registryGroupEntry(t)
	group := nodegroup.Support{Group: "g", Version: 7, Entries: []nodegroup.Entry{entry}, DefaultContext: &nodegroup.DefaultContext{TemplateID: testTemplateRef}}
	reg := reportRegistryGroup(t, r, "node", nil, group)
	profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	profile.Value.ResumeToken = "private-resume-capability"
	profile.Value.APIEndpoint = "https://private-api.example"
	profile.Value.Labels = map[string]string{"private-label": "private-tenant-metadata"}
	if _, err := r.stores.nodes.UpdateProfile(t.Context(), profile.Registration, profile.Version, profile.Value); err != nil {
		t.Fatal(err)
	}
	response, status := readTestGroupStatus(t, r, key, "node", "")
	if response.Code != http.StatusOK || !status.StorageReadable || !status.Published || !status.Eligible || !status.RuntimeMatch || status.GroupVersion != 7 || status.Registration == nil || status.Registration.NodeID != reg.NodeID || status.Registration.Fence != reg.Fence || status.Registration.LinkOwner != reg.LinkOwner {
		t.Fatalf("status=%+v code=%d", status, response.Code)
	}
	for _, secret := range []string{key, entry.AuthMaterial.APISecret, entry.AuthRef, "private-resume-capability", "private-api.example", "private-tenant-metadata", "auth_ref", "context", "template_id"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("diagnostic exposed private field %q", secret)
		}
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("diagnostic is cacheable")
	}
	for _, invalid := range []string{"", "not-a-key", key[:len(key)-1] + map[bool]string{true: "1", false: "0"}[strings.HasSuffix(key, "0")]} {
		response, _ := readTestGroupStatus(t, r, invalid, "node", "")
		if response.Code != http.StatusForbidden || response.Body.String() != "forbidden\n" {
			t.Fatalf("invalid key exposed status: code=%d body=%s", response.Code, response.Body.String())
		}
	}
	// A different root for the same Group must not reveal this Node's support.
	publishTestGroups(t, r, "other-root-node", "g")
	response, status = readTestGroupStatus(t, r, testAPIKeyValue(), "node", "")
	if response.Code != http.StatusOK || status.Published || status.Registration != nil || status.GroupVersion != 0 || status.Reason != "support_unavailable" {
		t.Fatalf("foreign root exposed support: %+v code=%d", status, response.Code)
	}
}

func TestGroupStatusRequiresCompletedCurrentRegistration(t *testing.T) {
	r := testReg(t)
	key, entry := registryGroupEntry(t)
	group := nodegroup.Support{Group: "g", Version: 2, Entries: []nodegroup.Entry{entry}}
	first := reportRegistryGroup(t, r, "node", nil, group)
	// Begin a replacement snapshot. Retained roots still authenticate, but the
	// old publication must no longer be reported as current readiness.
	if err := r.stores.groups.BeginGroupSupport(t.Context(), first, "incomplete", 3, 1); err != nil {
		t.Fatal(err)
	}
	_, status := readTestGroupStatus(t, r, key, "node", "")
	if status.Published || status.GroupVersion != 0 || status.Registration != nil || status.Reason != "support_unavailable" {
		t.Fatalf("partial snapshot declared ready: %+v", status)
	}
	group.Version = 3
	if err := r.stores.groups.WriteGroupSupportBatch(t.Context(), first, "incomplete", 0, []nodegroup.Support{group}); err != nil {
		t.Fatal(err)
	}
	_, status = readTestGroupStatus(t, r, key, "node", "")
	if status.Published {
		t.Fatal("batch without end declared ready")
	}
	if _, err := r.stores.groups.FinishGroupSupport(t.Context(), first, "incomplete"); err != nil {
		t.Fatal(err)
	}
	_, status = readTestGroupStatus(t, r, key, "node", "")
	if !status.Published || status.GroupVersion != 3 || status.Registration.Fence != first.Fence {
		t.Fatalf("complete snapshot missing: %+v", status)
	}
	// Registering a new connection invalidates the previous publication even
	// if the old Group row and authentication root remain present.
	if err := r.stores.PutNode(t.Context(), &NodeRecord{NodeID: "node"}); err != nil {
		t.Fatal(err)
	}
	_, status = readTestGroupStatus(t, r, key, "node", "")
	if status.Published || status.Registration != nil {
		t.Fatalf("old registration declared ready: %+v", status)
	}
	fresh := reportRegistryGroup(t, r, "node", nil, group)
	_, status = readTestGroupStatus(t, r, key, "node", "")
	if !status.Published || status.Registration.Fence != fresh.Fence || fresh == first {
		t.Fatalf("fresh publication missing: %+v", status)
	}
	_, status = readTestGroupStatus(t, r, key, "unknown", "")
	if status.Published || status.Registration != nil || status.Reason != "support_unavailable" {
		t.Fatalf("unknown Node exposed inventory: %+v", status)
	}
	profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.stores.nodes.BeginReap(t.Context(), "node", profile.Version); err != nil {
		t.Fatal(err)
	}
	_, status = readTestGroupStatus(t, r, key, "node", "")
	if status.Published || status.Registration != nil || status.Reason != "support_unavailable" {
		t.Fatalf("reaped Node remained a published candidate: %+v", status)
	}
}

func TestGroupStatusExplainsCreateEligibility(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	annotated := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: ref + "@runtime:one.bundle:digest:" + strings.Repeat("b", 64)}.String()
	for _, tc := range []struct {
		name, reason string
		labels       map[string]string
		context      *nodegroup.DefaultContext
		template     string
		modify       func(*metadata.NodeProfileRecord)
	}{
		{name: "default", reason: "eligible", context: &nodegroup.DefaultContext{TemplateID: testTemplateRef}},
		{name: "missing_default", reason: "default_template_missing"},
		{name: "explicit", reason: "eligible", template: testTemplateRef},
		{name: "runtime_missing", reason: "runtime_unavailable", template: annotated},
		{name: "wrong_runtime_digest", reason: "runtime_unavailable", template: annotated, labels: map[string]string{requestplacement.RuntimeLabelPrefix + "one.bundle": "digest:" + strings.Repeat("c", 64)}},
		{name: "runtime_available", reason: "eligible", template: annotated, labels: map[string]string{requestplacement.RuntimeLabelPrefix + "one.bundle": "digest:" + strings.Repeat("b", 64)}},
		{name: "draining", reason: "draining", template: testTemplateRef, modify: func(n *metadata.NodeProfileRecord) { n.Draining = true }},
		{name: "zone", reason: "zone_rejected", template: testTemplateRef, modify: func(n *metadata.NodeProfileRecord) { n.Zone = "critical" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := testReg(t)
			key, entry := registryGroupEntry(t)
			reportRegistryGroup(t, r, "node", tc.labels, nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}, DefaultContext: tc.context})
			if tc.modify != nil {
				profile, err := r.stores.nodes.ReadNodeProfile(t.Context(), "node")
				if err != nil {
					t.Fatal(err)
				}
				tc.modify(&profile.Value)
				if _, err := r.stores.nodes.UpdateProfile(t.Context(), profile.Registration, profile.Version, profile.Value); err != nil {
					t.Fatal(err)
				}
			}
			response, status := readTestGroupStatus(t, r, key, "node", tc.template)
			if response.Code != http.StatusOK || !status.Published || status.Reason != tc.reason || status.Eligible != (tc.reason == "eligible") {
				t.Fatalf("status=%+v code=%d", status, response.Code)
			}
			request := PlaceRequest{Group: "g", APIKey: key, RequiredNodeID: "node"}
			if tc.template != "" {
				request.TemplateID = &tc.template
			}
			_, err := r.selectGroupNode(t.Context(), request)
			if (err == nil) != status.Eligible {
				t.Fatalf("diagnostic differs from selection: %+v selection error=%v", status, err)
			}
		})
	}
}

type failingGroupStatusRead struct {
	metadata.GroupSupportRepository
}

func (g failingGroupStatusRead) ReadGroupSupport(context.Context, string, string) (metadata.GroupSupportSelection, error) {
	return metadata.GroupSupportSelection{}, errors.New("private-database-connection-details")
}

func TestGroupStatusRejectsWritesAndRedactsStorageFailure(t *testing.T) {
	r := testReg(t)
	publishTestGroups(t, r, "node", "g")
	r.stores.groups = failingGroupStatusRead{r.stores.groups}
	response, _ := readTestGroupStatus(t, r, testAPIKeyValue(), "node", "")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private-database") {
		t.Fatalf("storage error leaked or passed: %d %s", response.Code, response.Body.String())
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		w := httptest.NewRecorder()
		r.serveGroupStatus(w, httptest.NewRequest(method, RouteLinkGroupStatusPath+"?group=g&node_id=node", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("write %s accepted: %d", method, w.Code)
		}
	}
}
