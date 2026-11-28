package orch

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestNativeManifestRegistrationRejectsUppercase(t *testing.T) {
	o := testOrch(t)
	if _, _, _, err := o.AddKeyPair(t.Context(), strings.Repeat("AB", 32), "", "", 0, ""); err == nil || !strings.Contains(err.Error(), "lowercase hex") {
		t.Fatalf("admin registration accepted non-canonical native ManifestKey: %v", err)
	}
	rows, err := o.st.ListKeyPairs(t.Context())
	if err != nil || len(rows) != 0 {
		t.Fatalf("failed native registration wrote credentials: %v", err)
	}
}

func TestManifestPluginOrchestratorBoundaries(t *testing.T) {
	if !testsecret.InSubprocess(t) {
		return
	}
	material := bytes.Repeat([]byte{0xab}, 32)
	var raw [32]byte
	copy(raw[:], material)
	const reference = "@test/Tenant/Manifest_v1"
	broker := testsecret.Start(t, map[string][]byte{"Tenant/Manifest_v1": material})
	expectedFP := manifestkey.Fingerprint(raw)
	ctx := t.Context()

	t.Run("registration defaults persistence and task delivery", func(t *testing.T) {
		o := testOrch(t)
		before := broker.Count("Tenant/Manifest_v1")
		added, apiFP, manifestFP, err := o.AddKeyPair(ctx, reference, "", "plugin", 0, "")
		if err != nil || !added || manifestFP != expectedFP {
			t.Fatalf("register reference: added=%v fingerprint=%q err=%v", added, manifestFP, err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+1 {
			t.Fatal("default API secret and stored fingerprint did not share one resolved root")
		}
		pair, found, err := o.st.AllowedKeyPairByAPISecretFingerprint(ctx, apiFP)
		wantAPI := hex.EncodeToString(apikey.DeriveAPISecret(material))
		if err != nil || !found || pair.APISecret != wantAPI || pair.ManifestKey != reference || pair.ManifestKeyFingerprint != expectedFP {
			t.Fatalf("persisted credential pair mismatch: found=%v err=%v", found, err)
		}
		apiKey := mintTestAPIKey(t, pair.APISecret)
		registered, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileBare, Resources: testBuildResources()})
		if err != nil {
			t.Fatal(err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+2 {
			t.Fatal("Build registration did not reuse its bound ManifestKey for persisted identity")
		}
		persisted, err := o.st.GetBuild(ctx, registered.BuildID)
		if err != nil || persisted.ManifestKey != reference || persisted.ManifestKeyFingerprint != expectedFP {
			t.Fatalf("build lost original reference or material identity: %v", err)
		}
		for name, env := range map[string]map[string]string{
			"builder": buildTaskEnv(persisted),
			"sandbox": sandboxTaskEnv(&types.Sandbox{ManifestKey: pair.ManifestKey}),
		} {
			if env["MANIFEST_KEY"] != reference || !strings.HasPrefix(env[secret.Env], "test::") {
				t.Fatalf("%s task did not receive original reference and node plugin configuration", name)
			}
		}
		if _, _, _, err := o.AddKeyPair(ctx, reference, "@test/Tenant/Manifest_v1", "", 0, ""); err == nil {
			t.Fatal("Manifest plugin support expanded the APISecret input format")
		}
	})

	t.Run("NodeLink verifies the declared material fingerprint", func(t *testing.T) {
		o := testOrch(t)
		apiSecret := strings.Repeat("1", 64)
		apiFP, _ := store.APISecretHash(apiSecret)
		command := &routesync.Command{APISecret: apiSecret, APISecretFingerprint: apiFP, ManifestKey: reference, ManifestKeyFingerprint: strings.Repeat("0", 64)}
		if err := o.putClusterKeyPair(ctx, command); err == nil {
			t.Fatal("untrusted fingerprint accepted")
		}
		pairs, err := o.st.ListKeyPairs(ctx)
		if err != nil || len(pairs) != 0 {
			t.Fatalf("failed NodeLink registration changed storage: %v", err)
		}
		command.ManifestKeyFingerprint = expectedFP
		if err := o.putClusterKeyPair(ctx, command); err != nil {
			t.Fatal(err)
		}
		pair, found, err := o.st.AllowedKeyPairByAPISecretFingerprint(ctx, apiFP)
		if err != nil || !found || pair.ManifestKey != reference {
			t.Fatalf("NodeLink registration changed carrier: %v", err)
		}
	})

	t.Run("projection and both Builder route paths use API secret", func(t *testing.T) {
		o := testOrch(t)
		o.cfg.MMDS.Enabled = true
		apiSecret := strings.Repeat("1", 64)
		sb := &types.Sandbox{ID: "node-local", StableIDValue: "stable-global", APISecret: apiSecret, ManifestKey: reference, ManifestKeyFingerprint: expectedFP}
		before := broker.Count("Tenant/Manifest_v1")
		broker.Set("Tenant/Manifest_v1", nil)
		defer broker.Set("Tenant/Manifest_v1", material)
		entry, err := routeEntryBase(sb)
		if err != nil || entry.ManifestKeyFingerprint != expectedFP || entry.MmdsSecret != hex.EncodeToString(keys.MmdsSecret(apiSecret, sb.ID)) {
			t.Fatalf("route projection changed API-derived identity: %v", err)
		}
		if entry.MmdsSecret == hex.EncodeToString(keys.MmdsSecret(apiSecret, sb.StableID())) || len(entry.MmdsSecret) != 64 {
			t.Fatal("MMDS derivation changed local sandbox ID or route encoding")
		}
		sb.ManifestKey = "@test/unavailable-other-root"
		changedManifest, _ := routeEntryBase(sb)
		if changedManifest.MmdsSecret != entry.MmdsSecret {
			t.Fatal("MMDS still directly depends on ManifestKey")
		}
		sb.APISecret = strings.Repeat("2", 64)
		changedAPI, _ := routeEntryBase(sb)
		if changedAPI.MmdsSecret == entry.MmdsSecret {
			t.Fatal("MMDS did not change with APISecret")
		}
		for _, recovery := range []bool{false, true} {
			b := &types.Build{BuildID: "build-plugin", Profile: types.ProfileE2B, APISecret: apiSecret, ManifestKey: reference, ManifestKeyFingerprint: expectedFP,
				Builder: types.BuildOptions{Target: &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}}}
			var row *types.Sandbox
			if recovery {
				row = o.publishRecoveredBuildMMDS(b)
			} else {
				row = o.publishBuildFinal(&pendingBuild{build: b, handoff: newBuildTaskHandoff(false, "", types.ResumeSource{})}, &configsock.BuildSpec{})
			}
			if row == nil {
				t.Fatal("Builder route not published")
			}
			got, err := routeEntryBase(row)
			if err != nil || got.MmdsSecret != hex.EncodeToString(keys.MmdsSecret(apiSecret, buildMMDSID(b.BuildID))) || got.ManifestKeyFingerprint != expectedFP {
				t.Fatalf("Builder recovery=%v route: %v", recovery, err)
			}
			o.uncache(row.ID)
			o.setMMDSBuildOwner(row.ID, "")
		}
		if broker.Count("Tenant/Manifest_v1") != before {
			t.Fatal("route projection contacted the broker")
		}
	})

	t.Run("migration preserves references and binds each operation once", func(t *testing.T) {
		dir := t.TempDir()
		o := migrationOrchestrator(t, dir, []byte("runtime"))
		literal := hex.EncodeToString(material)
		source := migrationSandbox(t, dir, "source", literal, "manifest://"+strings.Repeat("b", 64))
		source.ManifestKey, source.ManifestKeyFingerprint = reference, expectedFP
		source.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: source.ResumeSource.SandboxRef}
		if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: source.APISecret, ManifestKey: reference}, "", 0, ""); err != nil {
			t.Fatal(err)
		}
		before := broker.Count("Tenant/Manifest_v1")
		token, err := o.mintSandboxTokenContext(ctx, source, source.ResumeSource)
		if err != nil {
			t.Fatal(err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+1 {
			t.Fatal("migration seal used more than one resolved root")
		}
		payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{APISecret: source.APISecret, ManifestKey: literal}, token)
		if err != nil || payload.ManifestKeyFingerprint != expectedFP {
			t.Fatalf("plugin migration token changed legacy material identity: %v", err)
		}
		before = broker.Count("Tenant/Manifest_v1")
		apiKey := mintTestAPIKey(t, source.APISecret)
		target, err := o.prepareStandaloneTargetWithMMDS(ctx, "plugin-target", apiKey, token, nil, nil, api.ImportOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+1 {
			t.Fatal("standalone import did not share one root across repeated token checks")
		}
		persisted, err := o.st.Get(ctx, target.ID)
		if err != nil || persisted == nil || persisted.ManifestKey != reference || persisted.ManifestKeyFingerprint != expectedFP {
			t.Fatalf("migration import lost original reference or retained fingerprint: %v", err)
		}
		broker.Set("Tenant/Manifest_v1", bytes.Repeat([]byte{0xcd}, 32))
		defer broker.Set("Tenant/Manifest_v1", material)
		if _, err := o.mintSandboxTokenContext(ctx, source, source.ResumeSource); err == nil {
			t.Fatal("migration export accepted a changed retained root")
		}
		if _, err := o.ImportSandbox(ctx, apiKey, token, "changed-root-target", api.ImportOptions{}); err == nil {
			t.Fatal("migration import accepted a changed retained root")
		}
		if row, err := o.st.Get(ctx, "changed-root-target"); err != nil || row != nil {
			t.Fatalf("failed migration import wrote a target: %v", err)
		}
	})

	t.Run("workspace report and token share one resolved root", func(t *testing.T) {
		dir := t.TempDir()
		o := migrationOrchestrator(t, dir, []byte("runtime"))
		sb := migrationSandbox(t, dir, "plugin-workspace", hex.EncodeToString(material), "manifest://"+strings.Repeat("e", 64))
		apiKey := mintTestAPIKey(t, sb.APISecret)
		sb.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}}}
		sb.State = types.StatePaused
		sb.ManifestKey, sb.ManifestKeyFingerprint = reference, expectedFP
		sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("e", 64)}
		if err := o.st.Put(ctx, sb); err != nil {
			t.Fatal(err)
		}
		size := int64(8192)
		disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64), BaseFromRefs: []string{"manifest://" + strings.Repeat("b", 64)}}
		report := artifact.DiskReport{Refs: true, DiskRefs: []string{disk.Base, disk.BaseFromRefs[0]}, RemovedRefs: []string{"file://old.overlay"}}
		result := workspacedisk.Result{Disk: &disk, Report: &report}
		file := installWorkspaceResult(t, o, result)
		before := broker.Count("Tenant/Manifest_v1")
		out, err := o.ExportWorkspace(ctx, sb.ID, apiKey, "project")
		if err != nil {
			t.Fatal(err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+1 {
			t.Fatal("workspace token did not bind exactly one resolved root")
		}
		decoded, err := migrationtoken.OpenWorkspace(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: hex.EncodeToString(material)}, out.Result)
		if err != nil || decoded.Base != disk.Base || !slices.Equal(decoded.BaseFromRefs, disk.BaseFromRefs) || !out.Refs || !slices.Equal(out.DiskRefs, report.DiskRefs) || !slices.Equal(out.RemovedRefs, report.RemovedRefs) {
			t.Fatalf("plugin workspace token/report mismatch: %v", err)
		}
		writeResult := func(value workspacedisk.Result) {
			t.Helper()
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		writeResult(workspacedisk.Result{Disk: &disk})
		before = broker.Count("Tenant/Manifest_v1")
		if got, err := o.ExportWorkspace(ctx, sb.ID, apiKey, "project"); !errors.Is(err, api.ErrWorkspaceUnavailable) || got.Result != "" {
			t.Fatalf("invalid report produced a workspace token: %v", err)
		}
		if broker.Count("Tenant/Manifest_v1") != before {
			t.Fatal("invalid workspace report reached key resolution")
		}
		writeResult(result)
		broker.Set("Tenant/Manifest_v1", bytes.Repeat([]byte{0xcd}, 32))
		defer broker.Set("Tenant/Manifest_v1", material)
		if got, err := o.ExportWorkspace(ctx, sb.ID, apiKey, "project"); err == nil || got.Result != "" {
			t.Fatal("workspace export accepted a changed retained root")
		}
		stored, err := o.st.Get(ctx, sb.ID)
		if err != nil || stored.ManifestKey != reference || stored.ManifestKeyFingerprint != expectedFP || stored.ResumeSource != sb.ResumeSource || stored.State != types.StatePaused {
			t.Fatalf("workspace export changed retained identity or source: %v", err)
		}
	})

	t.Run("pull token uses canonical material and retained identity", func(t *testing.T) {
		o := testOrch(t)
		want := regcreds.Creds{Username: "plugin-user", Password: "plugin-password"}
		token, err := regcreds.Seal(hex.EncodeToString(material), want)
		if err != nil {
			t.Fatal(err)
		}
		b := &types.Build{ManifestKey: reference, ManifestKeyFingerprint: expectedFP}
		before := broker.Count("Tenant/Manifest_v1")
		value, err := o.resolveBuildCreds(ctx, b, token, "", "")
		if err != nil {
			t.Fatal(err)
		}
		var got regcreds.Creds
		if err := json.Unmarshal([]byte(value), &got); err != nil || got != want {
			t.Fatalf("legacy pull token could not use plugin material: %v", err)
		}
		if broker.Count("Tenant/Manifest_v1") != before+1 {
			t.Fatal("pull token resolution repeated its broker request")
		}
		b.ManifestKeyFingerprint = strings.Repeat("0", 64)
		if _, err := o.resolveBuildCreds(ctx, b, token, "", ""); err == nil {
			t.Fatal("pull token resolution ignored retained manifest identity")
		}
	})

	t.Run("Builder request digest retains canonical text key", func(t *testing.T) {
		pair, err := store.ResolveKeyPair(ctx, store.KeyPair{ManifestKey: reference})
		if err != nil {
			t.Fatal(err)
		}
		bound, _ := pair.ManifestKeyBytes()
		got, err := clusterBuildRegistrationRequestDigest(bound, &routesync.Command{BuildID: "build"})
		if err != nil {
			t.Fatal(err)
		}
		// This exact legacy JSON shape is authenticated with 64 lowercase hex
		// bytes, not the 32-byte root or the plugin reference text.
		payload := `{"BuildID":"build","TemplateID":"","Profile":"","APISecretFingerprint":"","ImageRepo":"","RegistryAuth":"","Resources":null,"Config":null,"Env":null,"Secure":false,"Credentials":null,"MMDSSecrets":null}`
		mac := hmac.New(sha256.New, []byte(hex.EncodeToString(material)))
		_, _ = mac.Write([]byte(payload))
		if got != hex.EncodeToString(mac.Sum(nil)) {
			t.Fatal("Builder request digest changed the legacy text-key protocol")
		}
	})
}
