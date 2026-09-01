package store

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxIdentityRoundTrip(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("1", "2")

	cluster := &types.Sandbox{
		ID: "stable-g3", Profile: types.ProfileBare,
		Cluster:       &types.ClusterSandboxContext{Group: "/tenant", RouteKey: "worker"},
		StableIDValue: "stable", TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		State: types.StateRunning, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(cluster)
	if err := st.Put(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, cluster.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile != types.ProfileBare || got.Cluster == nil ||
		got.Cluster.Group != "/tenant" || got.Cluster.RouteKey != "worker" ||
		got.StableIDValue != "stable" || got.StableID() != "stable" {
		t.Fatalf("cluster identity round trip = %+v", got)
	}

	standalone := &types.Sandbox{
		ID: "import-target", Profile: types.ProfileE2B, StableIDValue: "source-stable-id",
		TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("b", 64)}.String(), State: types.StatePaused,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 2,
	}
	setTestSandboxServiceCredentials(standalone)
	if err := st.Put(ctx, standalone); err != nil {
		t.Fatal(err)
	}
	got, err = st.Get(ctx, standalone.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cluster != nil || got.StableIDValue != "source-stable-id" || got.StableID() != "source-stable-id" {
		t.Fatalf("standalone imported identity round trip = %+v", got)
	}
}

func TestSandboxSchemaUsesStableIDHardCut(t *testing.T) {
	st := testStore(t)
	rows, err := st.db.QueryContext(context.Background(), `PRAGMA table_info(sandboxes)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !columns["stable_id"] {
		t.Fatal("sandboxes schema is missing stable_id")
	}
	legacyColumn := strings.Join([]string{"auth", "sandbox", "id"}, "_")
	if columns[legacyColumn] {
		t.Fatalf("sandboxes schema retained legacy column %q", legacyColumn)
	}
}

func TestSandboxStableIDIsNotUnique(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("7", "8")
	templateID := types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindSnp,
		Ref:     "manifest://" + strings.Repeat("e", 64),
	}.String()
	for _, id := range []string{"copy-a", "copy-b"} {
		sb := &types.Sandbox{
			ID:            id,
			Profile:       types.ProfileBare,
			StableIDValue: "shared-stable-id",
			TemplateID:    templateID,
			State:         types.StatePaused,
			APISecret:     pair.APISecret,
			ManifestKey:   pair.ManifestKey,
			CreatedUnix:   1,
		}
		setTestSandboxServiceCredentials(sb)
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatalf("InsertSandbox(%s): %v", id, err)
		}
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sandboxes WHERE stable_id=?`, "shared-stable-id").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rows sharing StableID = %d, want 2", count)
	}
}

func TestSandboxSystemIdentityIsInsertBound(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("3", "4")
	sb := &types.Sandbox{
		ID: "stable-g0", Profile: types.ProfileBare,
		Cluster:       &types.ClusterSandboxContext{Group: "/original", RouteKey: "route"},
		StableIDValue: "stable", TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String(),
		State: types.StateRunning, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	sb.Profile = types.ProfileE2B
	sb.Cluster = &types.ClusterSandboxContext{Group: "/replacement", RouteKey: "other"}
	sb.StableIDValue = "replacement"
	sb.State = types.StatePaused
	setTestSandboxServiceCredentials(sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile != types.ProfileBare || got.Cluster == nil ||
		got.Cluster.Group != "/original" || got.Cluster.RouteKey != "route" ||
		got.StableIDValue != "stable" || got.State != types.StatePaused {
		t.Fatalf("lifecycle upsert rebound system identity: %+v", got)
	}
}

func TestCASRunStateFencesStaleRunner(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("5", "6")
	sb := &types.Sandbox{
		ID: "stable-g0", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(), State: types.StateRunning,
		RunID: "sandbox-current", APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CASRunState(ctx, sb.ID, "sandbox-stale", types.StateRunning, types.StatePaused); err != nil || changed {
		t.Fatalf("stale runner state change = %v, %v", changed, err)
	}
	if changed, err := st.CASRunState(ctx, sb.ID, sb.RunID, types.StateRunning, types.StatePaused); err != nil || !changed {
		t.Fatalf("current runner state change = %v, %v", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil || got.State != types.StatePaused {
		t.Fatalf("sandbox after fenced state change = %+v, %v", got, err)
	}
}

func TestSandboxIdentityValidation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("5", "6")
	base := types.Sandbox{
		ID: "sandbox", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(), State: types.StateRunning,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(&base)

	for name, mutate := range map[string]func(*types.Sandbox){
		"empty profile":   func(sb *types.Sandbox) { sb.Profile = "" },
		"unknown profile": func(sb *types.Sandbox) { sb.Profile = "unknown" },
		"missing group": func(sb *types.Sandbox) {
			sb.Cluster = &types.ClusterSandboxContext{RouteKey: "route"}
		},
		"missing route key": func(sb *types.Sandbox) {
			sb.Cluster = &types.ClusterSandboxContext{Group: "/group"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			sb := base
			mutate(&sb)
			if err := st.Put(ctx, &sb); err == nil {
				t.Fatal("invalid sandbox identity was accepted")
			}
		})
	}

	if err := st.Put(ctx, &base); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sandboxes SET cluster_group='/group',cluster_route_key='' WHERE id=?`, base.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, base.ID); err == nil {
		t.Fatal("incomplete stored cluster context was accepted")
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sandboxes SET cluster_group='',profile='unknown' WHERE id=?`, base.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, base.ID); err == nil {
		t.Fatal("invalid stored profile was accepted")
	}
}

func TestSandboxListDefaultExcludesInternalLifecycleStates(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("7", "8")
	rows := []struct {
		id    string
		state types.State
	}{
		{id: "a-starting", state: types.StateStarting},
		{id: "b-running", state: types.StateRunning},
		{id: "c-paused", state: types.StatePaused},
		{id: "d-dead", state: types.StateDead},
	}
	for _, row := range rows {
		sb := &types.Sandbox{
			ID: row.id, Profile: types.ProfileBare,
			TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("e", 64)}.String(),
			State:      row.state, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 1,
		}
		setTestSandboxServiceCredentials(sb)
		if err := st.Put(ctx, sb); err != nil {
			t.Fatal(err)
		}
	}

	got, next, err := st.List(ctx, "", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if next != "" || len(got) != 2 || got[0].ID != "b-running" || got[1].ID != "c-paused" {
		t.Fatalf("default list = %+v next=%q, want running and paused only", got, next)
	}
	for _, row := range []struct {
		state types.State
		id    string
	}{
		{state: types.StateStarting, id: "a-starting"},
		{state: types.StateDead, id: "d-dead"},
	} {
		got, next, err = st.List(ctx, string(row.state), "", 100, "")
		if err != nil || next != "" || len(got) != 1 || got[0].ID != row.id {
			t.Fatalf("explicit %s list = %+v next=%q err=%v, want %s", row.state, got, next, err, row.id)
		}
	}
}
