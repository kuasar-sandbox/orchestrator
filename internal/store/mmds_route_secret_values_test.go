package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxInitialMMDSRouteSecretValuesAreAtomicAndEncrypted(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("mmds-initial", 0)
	sb.Metadata = map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`}
	digest := sandboxcfg.MMDSRoutesDigest(sb.Metadata[sandboxcfg.NsMMDS])
	secret := []byte("initial-route-secret-value")
	if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, digest, MMDSRouteSecretValues{"key": secret}); err != nil {
		t.Fatal(err)
	}

	gotSandbox, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotSandbox.Metadata[sandboxcfg.NsMMDS], string(secret)) || strings.Contains(gotSandbox.Metadata[sandboxcfg.NsMMDS], `"secrets"`) {
		t.Fatal("sandbox metadata contains secret material")
	}
	values, revision, found, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerSandbox, sb.ID, digest)
	if err != nil || !found || revision != 1 || !bytes.Equal(values["key"], secret) {
		t.Fatalf("secret lookup metadata mismatch: revision=%d found=%t err=%v", revision, found, err)
	}
	var ciphertext string
	if err := st.db.QueryRowContext(ctx, `SELECT ciphertext FROM sandbox_mmds_route_secret_values WHERE sandbox_id=?`, sb.ID).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ciphertext, string(secret)) {
		t.Fatal("SQLite ciphertext column contains plaintext")
	}
}

func TestSandboxAndInitialMMDSRouteSecretValuesRollbackTogether(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER reject_mmds_initial BEFORE INSERT ON sandbox_mmds_route_secret_values BEGIN SELECT RAISE(ABORT, 'reject'); END`); err != nil {
		t.Fatal(err)
	}
	sb := sandboxInsertFixture("mmds-rollback", 0)
	if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, "digest", MMDSRouteSecretValues{"key": []byte("value")}); err == nil {
		t.Fatal("atomic insert unexpectedly succeeded")
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("sandbox row survived failed secret insert")
	}
}

func TestBuildAndInitialMMDSRouteSecretValuesRollbackTogether(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER reject_build_mmds_initial BEFORE INSERT ON build_mmds_route_secret_values BEGIN SELECT RAISE(ABORT, 'reject'); END`); err != nil {
		t.Fatal(err)
	}
	build := &types.Build{
		BuildID: "mmds-build-rollback", TemplateID: "transient-mmds-build-rollback",
		APISecret: strings.Repeat("4", 64), ManifestKey: strings.Repeat("5", 64),
		Profile: types.ProfileE2B, Status: types.BuildBuilding, CreatedUnix: 1,
		ExecutionClaimed: true,
	}
	if err := st.InsertBuildWithMMDSRouteSecretValues(ctx, build, "digest", MMDSRouteSecretValues{"key": []byte("value")}); err == nil {
		t.Fatal("atomic build insert unexpectedly succeeded")
	}
	got, err := st.GetBuild(ctx, build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("build row survived failed secret insert")
	}
}

func TestMMDSRouteSecretValuesAADRejectsSubstitution(t *testing.T) {
	ctx := context.Background()
	newOwner := func(t *testing.T, st *Store, id, digest, value string) {
		t.Helper()
		sb := sandboxInsertFixture(id, 0)
		if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, digest, MMDSRouteSecretValues{"key": []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("owner", func(t *testing.T) {
		st := testStore(t)
		newOwner(t, st, "aad-owner-a", "digest", "owner-a-value")
		newOwner(t, st, "aad-owner-b", "digest", "owner-b-value")
		var ciphertext string
		if err := st.db.QueryRowContext(ctx, `SELECT ciphertext FROM sandbox_mmds_route_secret_values WHERE sandbox_id='aad-owner-a'`).Scan(&ciphertext); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx, `UPDATE sandbox_mmds_route_secret_values SET ciphertext=? WHERE sandbox_id='aad-owner-b'`, ciphertext); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerSandbox, "aad-owner-b", "digest"); err == nil {
			t.Fatal("owner-substituted ciphertext decrypted")
		}
	})

	t.Run("owner kind", func(t *testing.T) {
		st := testStore(t)
		ownerID := "aad-owner-kind"
		newOwner(t, st, ownerID, "digest", "sandbox-value")
		build := &types.Build{
			BuildID: ownerID, TemplateID: "transient-aad-owner-kind",
			APISecret: strings.Repeat("7", 64), ManifestKey: strings.Repeat("8", 64),
			Profile: types.ProfileE2B, Status: types.BuildRegistered, CreatedUnix: 1,
		}
		if err := st.InsertBuildWithMMDSRouteSecretValues(ctx, build, "digest", MMDSRouteSecretValues{"key": []byte("build-value")}); err != nil {
			t.Fatal(err)
		}
		var ciphertext string
		if err := st.db.QueryRowContext(ctx, `SELECT ciphertext FROM sandbox_mmds_route_secret_values WHERE sandbox_id=?`, ownerID).Scan(&ciphertext); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx, `UPDATE build_mmds_route_secret_values SET ciphertext=? WHERE build_id=?`, ciphertext, ownerID); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerBuild, ownerID, "digest"); err == nil {
			t.Fatal("owner-kind-substituted ciphertext decrypted")
		}
	})

	t.Run("routes digest", func(t *testing.T) {
		st := testStore(t)
		newOwner(t, st, "aad-digest", "digest-a", "digest-value")
		if _, err := st.db.ExecContext(ctx, `UPDATE sandbox_mmds_route_secret_values SET routes_digest='digest-b' WHERE sandbox_id='aad-digest'`); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerSandbox, "aad-digest", "digest-b"); err == nil {
			t.Fatal("config-substituted ciphertext decrypted")
		}
	})

	t.Run("revision", func(t *testing.T) {
		st := testStore(t)
		newOwner(t, st, "aad-revision", "digest", "revision-value")
		if _, err := st.db.ExecContext(ctx, `UPDATE sandbox_mmds_route_secret_values SET revision=revision+1 WHERE sandbox_id='aad-revision'`); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerSandbox, "aad-revision", "digest"); err == nil {
			t.Fatal("revision-substituted ciphertext decrypted")
		}
	})
}

func TestMMDSRouteSecretValuesConcurrentCAS(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("mmds-cas", 0)
	if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, "digest", nil); err != nil {
		t.Fatal(err)
	}
	const writers = 12
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := st.PutMMDSRouteSecretValue(ctx, MMDSRouteSecretOwnerSandbox, sb.ID, "digest", fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("value-%02d", i)))
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	values, revision, found, err := st.GetMMDSRouteSecretValues(ctx, MMDSRouteSecretOwnerSandbox, sb.ID, "digest")
	if err != nil || !found || len(values) != writers || revision != writers {
		t.Fatalf("CAS result: values=%d revision=%d found=%t err=%v", len(values), revision, found, err)
	}
}

func TestMMDSRouteSecretValuesSandboxDeleteCascades(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("mmds-delete", 0)
	if err := st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, "digest", MMDSRouteSecretValues{"key": []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sandbox_mmds_route_secret_values WHERE sandbox_id=?`, sb.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("secret rows after sandbox delete = %d", count)
	}
}

func TestBuildMMDSRouteSecretValuesTerminalCleanup(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	build := &types.Build{
		BuildID: "mmds-build", TemplateID: "transient-mmds-build",
		APISecret: strings.Repeat("2", 64), ManifestKey: strings.Repeat("3", 64),
		Profile: types.ProfileE2B, Status: types.BuildBuilding, CreatedUnix: 1,
		ExecutionClaimed: true,
		Metadata: map[string]string{
			sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
			"ordinary":        "preserved",
		},
	}
	if err := st.InsertBuildWithMMDSRouteSecretValues(ctx, build, "digest", MMDSRouteSecretValues{"key": []byte("build-value")}); err != nil {
		t.Fatal(err)
	}
	build.Status = types.BuildError
	if err := st.PutBuildTerminal(ctx, build); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM build_mmds_route_secret_values WHERE build_id=?`, build.BuildID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("build secret rows after terminal update = %d", count)
	}
	got, err := st.GetBuild(ctx, build.BuildID)
	if err != nil || got.Status != types.BuildError {
		t.Fatalf("terminal build status mismatch: err=%v", err)
	}
	if _, present := got.Metadata[sandboxcfg.NsMMDS]; present {
		t.Fatalf("terminal build retained builder-only MMDS routes: %+v", got.Metadata)
	}
	if got.Metadata["ordinary"] != "preserved" {
		t.Fatalf("terminal build lost ordinary metadata: %+v", got.Metadata)
	}
}
