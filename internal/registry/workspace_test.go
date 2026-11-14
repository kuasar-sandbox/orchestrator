package registry

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
)

func workspaceInput(t *testing.T, token string) (map[string]string, []workspace.Entry) {
	t.Helper()
	raw, _ := json.Marshal([]map[string]any{{"name": "w", "mounts": []map[string]string{{"path": "/data"}}, "disk": map[string]string{"migrationToken": token}}})
	meta, entries, err := workspace.Extract(map[string]string{workspace.Namespace: string(raw)})
	if err != nil {
		t.Fatal(err)
	}
	return meta, entries
}

func TestWorkspaceReserveBindsFirstSourceToReservation(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	meta, entries := workspaceInput(t, "kwt1.first")
	r.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		if strings.Contains(req.Config[workspace.Namespace], "kwt1.") {
			t.Fatal("private source reached placer")
		}
		return &Placement{NodeID: "n1", TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint}, nil
	}))
	if err := r.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var commands []*routesync.Command
	r.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) {
		commands = append(commands, c)
		r.ackCommand(&routesync.CmdAck{CmdID: c.CmdID, Status: routesync.AckAccepted})
	}})
	place := func(key string, ws []workspace.Entry) error {
		return r.placeAndCreate(ctx, "/g", key, meta, nil, nil, true, false, createOptions{workspaces: ws})
	}
	if err := place("r", entries); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || len(commands[0].Workspaces) != 1 || !strings.Contains(string(commands[0].Workspaces[0].Disk), "kwt1.first") {
		t.Fatal("source lost")
	}
	rec, _, found, err := r.stores.GetSandbox(ctx, "/g", "r")
	if err != nil || !found || rec.WorkspacesDigest == "" {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(rec)
	if strings.Contains(string(wire), "kwt1.") {
		t.Fatal("raw token persisted")
	}
	_, second := workspaceInput(t, "kwt1.second")
	if err := place("r", second); err != nil || len(commands) != 1 {
		t.Fatalf("concurrent loser replaced workspace %v", err)
	}
	_, large := workspaceInput(t, strings.Repeat("a", 1<<20))
	if err := place("large", large); !errors.Is(err, errInvalidSandboxConfig) || len(commands) != 1 {
		t.Fatalf("oversize frame %v", err)
	}
	if _, _, found, err := r.stores.GetSandbox(ctx, "/g", "large"); err != nil || found {
		t.Fatal("dangling reservation after frame failure")
	}
}

func TestWorkspaceBuildReplayIdentity(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	r.SetPlacer(placementWithToken("n1"))
	owner := &recordingNodeOwner{ackErr: context.DeadlineExceeded}
	r.SetNodeOwner(owner)
	meta, entries := workspaceInput(t, "kwt1.private")
	req := BuildReserveReq{Group: "/g", BuildID: "workspace-build", TemplateID: "transient-workspace", Profile: types.ProfileBare, Resources: testWireBuildResources(), Metadata: meta, Workspaces: entries}
	if _, err := r.ReserveBuild(ctx, req); err == nil {
		t.Fatal("expected lost acknowledgment")
	}
	rec, found, err := r.stores.GetBuildInGroup(ctx, "/g", req.BuildID)
	if err != nil || !found {
		t.Fatal(err)
	}
	b, _ := json.Marshal(rec)
	if strings.Contains(string(b), "kwt1.private") || rec.RegistrationWorkspacesDigest == "" || len(owner.commands) != 1 || len(owner.commands[0].Workspaces) != 1 {
		t.Fatal("private build replay state")
	}
	_, req.Workspaces = workspaceInput(t, "kwt1.other")
	if _, err := r.ReserveBuild(ctx, req); err == nil || !strings.Contains(err.Error(), "immutable") || len(owner.commands) != 1 {
		t.Fatalf("changed source replay: %v", err)
	}
}
