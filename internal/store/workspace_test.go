package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspaceDefaultPinsPerGenerationAndClearsWithParent(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("workspace", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	first := &WorkspaceDefault{Disk: rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///trusted/original"}}
	other := &WorkspaceDefault{Disk: rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///trusted/new"}}
	for _, candidate := range []*WorkspaceDefault{first, other, nil} {
		got, err := st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, sb.RunID, "work", candidate)
		if err != nil || got.Disk.DiffTemplate != first.Disk.DiffTemplate {
			t.Fatal(got, err)
		}
	}
	var raw string
	if err := st.db.QueryRow("SELECT source_enc FROM workspace_defaults").Scan(&raw); err != nil || strings.Contains(raw, "trusted") {
		t.Fatal("unencrypted intent", err)
	}
	if _, err := st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, "obsolete", "work", other); !errors.Is(err, ErrWorkspaceGeneration) {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.db.QueryRow("SELECT count(*) FROM workspace_defaults").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
