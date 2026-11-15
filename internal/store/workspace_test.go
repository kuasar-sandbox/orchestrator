package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspaceDefaultPinsPerInstanceAndClearsWithParent(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("workspace", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	first := &WorkspaceDefault{Disk: rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///trusted/original"}}
	other := &WorkspaceDefault{Disk: rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///trusted/new"}}
	for i, candidate := range []*WorkspaceDefault{first, other, nil} {
		if i == 1 {
			sb.RunID = "after-resume"
			if err := st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
		}
		got, err := st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, "work", candidate)
		if err != nil || got.Disk.DiffTemplate != first.Disk.DiffTemplate {
			t.Fatal(got, err)
		}
	}
	var raw string
	if err := st.db.QueryRow("SELECT source_enc FROM workspace_defaults").Scan(&raw); err != nil || strings.Contains(raw, "trusted") {
		t.Fatal("unencrypted intent", err)
	}
	if _, err := st.WorkspaceDefault(ctx, sb.ID, "obsolete", "work", other); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.db.QueryRow("SELECT count(*) FROM workspace_defaults").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	replacement := sandboxInsertFixture(sb.ID, 1)
	if err := st.InsertSandbox(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, "work", first); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("deleted instance selected a disk for replacement: %v", err)
	}
	got, err := st.WorkspaceDefault(ctx, replacement.ID, replacement.InstanceID, "work", other)
	if err != nil || got == nil || got.Disk.DiffTemplate != other.Disk.DiffTemplate {
		t.Fatal(got, err)
	}
}
