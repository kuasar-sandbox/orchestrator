package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func admissionBuild(id string, resources types.BuildResources) *types.Build {
	return &types.Build{
		BuildID: id, TemplateID: "transient-" + id,
		APISecret: strings.Repeat("2", 64), ManifestKey: strings.Repeat("1", 64),
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildRegistered,
		Resources: resources, CreatedUnix: time.Now().Unix(),
	}
}

func TestRegistrationAdmissionChecksEveryDimensionAndReleasesAtTerminal(t *testing.T) {
	ctx := context.Background()
	for name, limit := range map[string]types.BuildAdmissionLimit{
		"count":   {MaxBuilds: 1},
		"cpu":     {Resources: types.BuildResources{CPU: 2000}},
		"memory":  {Resources: types.BuildResources{Memory: 2 << 30}},
		"storage": {Resources: types.BuildResources{Storage: 20 << 30}},
		"all": {MaxBuilds: 1, Resources: types.BuildResources{
			CPU: 2000, Memory: 2 << 30, Storage: 20 << 30,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			st := testStore(t)
			resources := types.BuildResources{CPU: 2000, Memory: 2 << 30, Storage: 20 << 30}
			first := admissionBuild("first-"+name, resources)
			if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, first, limit, "", nil); err != nil || !inserted {
				t.Fatalf("first register: inserted=%v err=%v", inserted, err)
			}
			second := admissionBuild("second-"+name, resources)
			if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, second, limit, "", nil); !errors.Is(err, ErrBuildRegistrationCapacity) {
				t.Fatalf("second register error = %v", err)
			}

			first.Status = types.BuildBuilding
			first.ExecutionClaimed = true
			if err := st.PutBuild(ctx, first); err != nil {
				t.Fatal(err)
			}
			first.Status = types.BuildError
			first.Reason = "done"
			if err := st.PutBuildTerminal(ctx, first); err != nil {
				t.Fatal(err)
			}
			if _, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, second, limit, "", nil); err != nil || !inserted {
				t.Fatalf("register after terminal release: inserted=%v err=%v", inserted, err)
			}
		})
	}
}

func TestRegistrationAdmissionConcurrentDoesNotOversubscribe(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	limit := types.BuildAdmissionLimit{MaxBuilds: 1, Resources: types.BuildResources{CPU: 1000, Memory: 1 << 30}}
	start := make(chan struct{})
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx,
				admissionBuild(fmt.Sprintf("concurrent-%02d", i), types.BuildResources{CPU: 1000, Memory: 1 << 30}),
				limit, "", nil)
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	success, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrBuildRegistrationCapacity):
			rejected++
		default:
			t.Fatalf("unexpected concurrent register error: %v", err)
		}
	}
	if success != 1 || rejected != cap(results)-1 {
		t.Fatalf("success=%d rejected=%d", success, rejected)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.RegistrationBuilds != 1 || usage.Registration.CPU != 1000 || usage.Registration.Memory != 1<<30 {
		t.Fatalf("usage = %+v", usage)
	}
}

func TestRegistrationReplayIsIdempotentAndConflictIsImmutable(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	limit := types.BuildAdmissionLimit{MaxBuilds: 1}
	build := admissionBuild("replay", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	build.RegistrationImageRepo = "registry.test/repo"
	build.RegistrationRegistryAuth = `{"auths":{"registry.test":{"auth":"first"}}}`
	initialValues := MMDSRouteSecretValues{"token": []byte("initial")}
	registered, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, build, limit, "routes-v1", initialValues)
	if err != nil || !inserted {
		t.Fatalf("register: %v %v", inserted, err)
	}
	replayed, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, build, limit, "routes-v1", initialValues)
	if err != nil || inserted || replayed.BuildID != registered.BuildID {
		t.Fatalf("replay: inserted=%v build=%+v err=%v", inserted, replayed, err)
	}
	conflict := *build
	conflict.Resources.Memory++
	if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, &conflict, limit, "routes-v1", initialValues); !errors.Is(err, ErrBuildRegistrationConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	for name, mutate := range map[string]func(*types.Build){
		"image repo":    func(b *types.Build) { b.RegistrationImageRepo = "registry.test/other" },
		"registry auth": func(b *types.Build) { b.RegistrationRegistryAuth = `{"auths":{"registry.test":{"auth":"other"}}}` },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := *build
			mutate(&candidate)
			if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, &candidate, limit, "routes-v1", initialValues); !errors.Is(err, ErrBuildRegistrationConflict) {
				t.Fatalf("conflict error = %v", err)
			}
		})
	}
	changedValues := MMDSRouteSecretValues{"token": []byte("changed")}
	if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, build, limit, "routes-v1", changedValues); !errors.Is(err, ErrBuildRegistrationConflict) {
		t.Fatalf("MMDS value conflict error = %v", err)
	}
	// Registration retries can arrive after Trigger. Lifecycle-owned fields must
	// not turn the same immutable definition into a false conflict.
	mutated := *registered
	mutated.Status = types.BuildBuilding
	mutated.ExecutionClaimed = true
	mutated.Kind = types.KindSnp
	mutated.FromImage = "registry.test/triggered:latest"
	mutated.StartCmd = "serve"
	mutated.Steps = []types.TemplateStep{{Type: "RUN", Args: []string{"echo", "triggered"}}}
	if err := st.PutBuild(ctx, &mutated); err != nil {
		t.Fatal(err)
	}
	replayed, inserted, err = st.RegisterBuildWithMMDSRouteSecretValues(ctx, build, limit, "routes-v1", initialValues)
	if err != nil || inserted || replayed.Status != types.BuildBuilding || replayed.FromImage != mutated.FromImage {
		t.Fatalf("post-trigger replay: inserted=%v build=%+v err=%v", inserted, replayed, err)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || usage.RegistrationBuilds != 1 {
		t.Fatalf("usage after replay/conflict = %+v, %v", usage, err)
	}
}

func TestExecutionAdmissionVectorClaimAndFIFO(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	resources := types.BuildResources{CPU: 2000, Memory: 2 << 30, Storage: 8 << 30}
	first := admissionBuild("fifo-first", resources)
	first.Status, first.WaitingUnix, first.WaitingSequence = types.BuildWaiting, 10, 1
	second := admissionBuild("fifo-second", resources)
	second.Status, second.WaitingUnix, second.WaitingSequence = types.BuildWaiting, 20, 2
	if err := st.PutBuild(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := st.PutBuild(ctx, first); err != nil {
		t.Fatal(err)
	}
	waiting, err := st.BuildsByStatus(ctx, types.BuildWaiting)
	if err != nil || len(waiting) != 2 || waiting[0].BuildID != first.BuildID || waiting[1].BuildID != second.BuildID {
		t.Fatalf("FIFO = %+v, %v", waiting, err)
	}
	limit := types.BuildAdmissionLimit{MaxBuilds: 2, Resources: types.BuildResources{CPU: 3000, Memory: 3 << 30, Storage: 12 << 30}}
	if won, err := st.ClaimBuildExecution(ctx, first.BuildID, limit, time.Now()); err != nil || !won {
		t.Fatalf("claim first: won=%v err=%v", won, err)
	}
	if won, err := st.ClaimBuildExecution(ctx, second.BuildID, limit, time.Now()); err != nil || won {
		t.Fatalf("claim over vector: won=%v err=%v", won, err)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || usage.ExecutionBuilds != 1 || usage.Execution != resources || usage.WaitingBuilds != 1 {
		t.Fatalf("usage = %+v, %v", usage, err)
	}
}

func TestGetClaimedBuildIDByRunIDReturnsOnlyLiveDurableAssignment(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	runID := "br-durable-assignment"
	claimed := admissionBuild("durable-assignment", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	claimed.Status = types.BuildBuilding
	claimed.ExecutionClaimed = true
	claimed.RunID = runID
	if err := st.PutBuild(ctx, claimed); err != nil {
		t.Fatal(err)
	}

	got, found, err := st.GetClaimedBuildIDByRunID(ctx, runID)
	if err != nil || !found || got != claimed.BuildID {
		t.Fatalf("claimed assignment = %q, %t, %v", got, found, err)
	}
	if got, found, err := st.GetClaimedBuildIDByRunID(ctx, "br-unknown"); err != nil || found || got != "" {
		t.Fatalf("unknown assignment = %q, %t, %v", got, found, err)
	}
	duplicate := admissionBuild("duplicate-run-owner", claimed.Resources)
	duplicate.Status, duplicate.ExecutionClaimed, duplicate.RunID = types.BuildBuilding, true, runID
	if err := st.PutBuild(ctx, duplicate); err != nil {
		t.Fatal(err)
	}
	if got, found, err := st.GetClaimedBuildIDByRunID(ctx, runID); err == nil || found || got != "" {
		t.Fatalf("ambiguous assignment = %q, %t, %v", got, found, err)
	}
	if _, err := st.db.ExecContext(ctx, `DELETE FROM builds WHERE build_id=?`, duplicate.BuildID); err != nil {
		t.Fatal(err)
	}

	claimed.ExecutionClaimed = false
	claimed.Status = types.BuildError
	if err := st.PutBuild(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	if got, found, err := st.GetClaimedBuildIDByRunID(ctx, runID); err != nil || found || got != "" {
		t.Fatalf("terminal assignment replay = %q, %t, %v", got, found, err)
	}
}

func TestExecutionAdmissionConcurrentClaimsDoNotOversubscribe(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	const candidates = 32
	resources := types.BuildResources{CPU: 1000, Memory: 1 << 30, Storage: 4 << 30}
	for i := 0; i < candidates; i++ {
		build := admissionBuild(fmt.Sprintf("execution-race-%02d", i), resources)
		build.Status, build.WaitingUnix = types.BuildWaiting, int64(i+1)
		if err := st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}

	limit := types.BuildAdmissionLimit{MaxBuilds: 1, Resources: resources}
	start := make(chan struct{})
	results := make(chan bool, candidates)
	errs := make(chan error, candidates)
	var wg sync.WaitGroup
	for i := 0; i < candidates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			won, err := st.ClaimBuildExecution(ctx, fmt.Sprintf("execution-race-%02d", i), limit, time.Now())
			results <- won
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent execution claim: %v", err)
		}
	}
	winners := 0
	for won := range results {
		if won {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("execution claim winners = %d, want 1", winners)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.ExecutionBuilds != 1 || usage.Execution != resources || usage.WaitingBuilds != candidates-1 {
		t.Fatalf("usage after concurrent claims = %+v", usage)
	}
}

func TestTerminalRegistrationReplayReturnsOriginalWithoutReacquiringCapacity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	limit := types.BuildAdmissionLimit{MaxBuilds: 1}
	definition := admissionBuild("terminal-replay", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	definition.RegistrationImageRepo = "registry.test/repo"
	definition.RegistrationRegistryAuth = `{"auths":{"registry.test":{"auth":"immutable"}}}`
	mmdsRoutes := `{"routes":[{"path":"/identity","data":"registered"}]}`
	definition.Metadata = map[string]string{sandboxcfg.NsMMDS: mmdsRoutes, "ordinary": "preserved"}
	definition.RegistrationMMDSRoutesDigest = sandboxcfg.MMDSRoutesDigest(mmdsRoutes)
	initialValues := MMDSRouteSecretValues{"identity": []byte("terminal-secret")}
	registered, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, definition, limit, definition.RegistrationMMDSRoutesDigest, initialValues)
	if err != nil || !inserted {
		t.Fatalf("register: inserted=%v err=%v", inserted, err)
	}
	terminal := *registered
	terminal.Status = types.BuildBuilding
	terminal.ExecutionClaimed = true
	if err := st.PutBuild(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	terminal.Status = types.BuildError
	terminal.Reason = "completed before delayed registration ACK"
	if err := st.PutBuildTerminal(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	terminalStored, err := st.GetBuild(ctx, definition.BuildID)
	if err != nil || terminalStored == nil {
		t.Fatalf("terminal GetBuild = %+v, %v", terminalStored, err)
	}
	if _, present := terminalStored.Metadata[sandboxcfg.NsMMDS]; present ||
		terminalStored.RegistrationMMDSRoutesDigest != definition.RegistrationMMDSRoutesDigest ||
		terminalStored.RegistrationMMDSValuesDigest == "" ||
		terminalStored.RegistrationMMDSValuesDigest != definition.RegistrationMMDSValuesDigest {
		t.Fatalf("terminal registration identity = metadata %+v route digest %q value digest %q",
			terminalStored.Metadata, terminalStored.RegistrationMMDSRoutesDigest, terminalStored.RegistrationMMDSValuesDigest)
	}
	if terminalStored.RegistrationMMDSValuesDigest == "terminal-secret" || len(terminalStored.RegistrationMMDSValuesDigest) != 64 {
		t.Fatalf("terminal MMDS value identity is not an irreversible digest: %q", terminalStored.RegistrationMMDSValuesDigest)
	}
	var secretRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM build_mmds_route_secret_values WHERE build_id=?`, definition.BuildID).Scan(&secretRows); err != nil || secretRows != 0 {
		t.Fatalf("terminal MMDS value rows = %d, err=%v", secretRows, err)
	}

	// Tightening both count and resource limits after acceptance must not turn a
	// delayed exact retry into a new admission decision.
	tightened := types.BuildAdmissionLimit{
		MaxBuilds: 1,
		Resources: types.BuildResources{CPU: 1, Memory: 1, Storage: 1},
	}
	replayed, inserted, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, definition, tightened, definition.RegistrationMMDSRoutesDigest, initialValues)
	if err != nil || inserted {
		t.Fatalf("terminal replay: inserted=%v err=%v", inserted, err)
	}
	if replayed.Status != types.BuildError || replayed.Reason != terminal.Reason {
		t.Fatalf("terminal replay = %+v", replayed)
	}
	changed := *definition
	changed.Metadata = map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/identity","data":"changed"}]}`, "ordinary": "preserved"}
	changed.RegistrationMMDSRoutesDigest = sandboxcfg.MMDSRoutesDigest(changed.Metadata[sandboxcfg.NsMMDS])
	if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, &changed, tightened, changed.RegistrationMMDSRoutesDigest, nil); !errors.Is(err, ErrBuildRegistrationConflict) {
		t.Fatalf("terminal MMDS identity conflict = %v", err)
	}
	changedValues := MMDSRouteSecretValues{"identity": []byte("different-terminal-secret")}
	if _, _, err := st.RegisterBuildWithMMDSRouteSecretValues(ctx, definition, tightened, definition.RegistrationMMDSRoutesDigest, changedValues); !errors.Is(err, ErrBuildRegistrationConflict) {
		t.Fatalf("terminal MMDS value identity conflict = %v", err)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.RegistrationBuilds != 0 || usage.ExecutionBuilds != 0 {
		t.Fatalf("terminal replay reacquired admission: %+v", usage)
	}
}

func TestBuildExpiryReleasesRegistrationAndRuntimeOwnershipIsEncrypted(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	mmdsRoutes := `{"routes":[{"path":"/identity","data":"expires"}]}`
	mmdsDigest := sandboxcfg.MMDSRoutesDigest(mmdsRoutes)
	registered := admissionBuild("expires-registered", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	waiting := admissionBuild("expires-waiting", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	waiting.Status, waiting.WaitingUnix = types.BuildWaiting, 2
	for _, build := range []*types.Build{registered, waiting} {
		build.Metadata = map[string]string{sandboxcfg.NsMMDS: mmdsRoutes, "ordinary": "preserved"}
		build.RegistrationMMDSRoutesDigest = mmdsDigest
		if err := st.InsertBuildWithMMDSRouteSecretValues(ctx, build, mmdsDigest, MMDSRouteSecretValues{"identity": []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	if expired, err := st.ExpireBuild(ctx, registered.BuildID, types.BuildRegistered, "registration TTL"); err != nil || !expired {
		t.Fatalf("expire registered: %v %v", expired, err)
	}
	if expired, err := st.ExpireBuild(ctx, waiting.BuildID, types.BuildWaiting, "queue TTL"); err != nil || !expired {
		t.Fatalf("expire waiting: %v %v", expired, err)
	}
	usage, err := st.BuildUsage(ctx)
	if err != nil || usage.RegistrationBuilds != 0 || usage.WaitingBuilds != 0 {
		t.Fatalf("usage after expiry = %+v, %v", usage, err)
	}
	for _, buildID := range []string{registered.BuildID, waiting.BuildID} {
		expired, err := st.GetBuild(ctx, buildID)
		if err != nil || expired == nil {
			t.Fatalf("expired build %s = %+v, %v", buildID, expired, err)
		}
		if expired.Metadata["ordinary"] != "preserved" {
			t.Fatalf("expired build %s lost ordinary metadata: %+v", buildID, expired.Metadata)
		}
		if _, present := expired.Metadata[sandboxcfg.NsMMDS]; present || expired.RegistrationMMDSRoutesDigest != mmdsDigest {
			t.Fatalf("expired build %s retained MMDS routes or lost identity: metadata=%+v digest=%q", buildID, expired.Metadata, expired.RegistrationMMDSRoutesDigest)
		}
		var secretRows int
		if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM build_mmds_route_secret_values WHERE build_id=?`, buildID).Scan(&secretRows); err != nil || secretRows != 0 {
			t.Fatalf("expired build %s secret rows = %d, err=%v", buildID, secretRows, err)
		}
	}

	runtime := admissionBuild("runtime-owner", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	runtime.Status, runtime.ExecutionClaimed, runtime.RunID = types.BuildBuilding, true, "br-runtime-owner"
	if err := st.PutBuild(ctx, runtime); err != nil {
		t.Fatal(err)
	}
	prepareJSON := `{"schema_version":1}`
	owned, err := st.SetBuildRuntimePreparation(ctx, runtime.BuildID, runtime.RunID,
		"7", "192.0.2.7", "02:00:00:00:00:07", "secret-token", prepareJSON)
	if err != nil || !owned {
		t.Fatalf("set runtime ownership: %v %v", owned, err)
	}
	var ciphertext string
	if err := st.db.QueryRowContext(ctx, `SELECT runtime_envd_access_token_enc FROM builds WHERE build_id=?`, runtime.BuildID).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if ciphertext == "" || ciphertext == "secret-token" {
		t.Fatalf("runtime token stored without encryption: %q", ciphertext)
	}
	loaded, err := st.GetBuild(ctx, runtime.BuildID)
	if err != nil || loaded.RuntimeEnvdAccessToken != "secret-token" || loaded.RuntimePrepareJSON != prepareJSON {
		t.Fatalf("runtime ownership round trip = %+v, %v", loaded, err)
	}
	if found, err := st.BuildingTaskIdentity(ctx, runtime.BuildID, "br-stale"); err != nil || found {
		t.Fatalf("stale task identity = %t, %v", found, err)
	}
	if found, err := st.BuildingTaskIdentity(ctx, runtime.BuildID, runtime.RunID); err != nil || !found {
		t.Fatalf("exact task identity = %t, %v", found, err)
	}
	if owned, err := st.SetBuildRuntimePreparation(ctx, runtime.BuildID, runtime.RunID,
		"8", "192.0.2.8", "02:00:00:00:00:08", "other-token", prepareJSON); err != nil || owned {
		t.Fatalf("second runtime preparation = %t, %v", owned, err)
	}
	if cleared, err := st.ClearBuildRuntimeOwnership(ctx, runtime.BuildID); err != nil || !cleared {
		t.Fatalf("clear runtime preparation = %t, %v", cleared, err)
	}
	loaded, err = st.GetBuild(ctx, runtime.BuildID)
	if err != nil || loaded.RuntimeVswitchPort != "" || loaded.RuntimeEnvdAccessToken != "" || loaded.RuntimePrepareJSON != "" {
		t.Fatalf("cleared runtime preparation = %+v, %v", loaded, err)
	}
}

func TestBuildUsageFailsClosedOnOverflow(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i, cpu := range []int64{math.MaxInt64, 1} {
		build := admissionBuild(fmt.Sprintf("overflow-%d", i), types.BuildResources{CPU: cpu, Memory: 1})
		if err := st.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.BuildUsage(ctx); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("overflow usage error = %v", err)
	}
}

func TestAcceptBuildResultIsDurableIdempotentAndClaimBound(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	b := admissionBuild("durable-result", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	b.Status, b.ExecutionClaimed, b.RunID = types.BuildBuilding, true, "br-durable-result"
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	result := types.BuildResult{
		Error: "snapshot.cfg is malformed", FailureStage: "artifact_prepare",
	}
	if inserted, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, result); err != nil || !inserted {
		t.Fatalf("first result acceptance = inserted %v err %v", inserted, err)
	}
	if inserted, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, result); err != nil || inserted {
		t.Fatalf("idempotent result replay = inserted %v err %v", inserted, err)
	}
	changed := result
	changed.Error = "snapshot.cfg is missing"
	if _, err := st.AcceptBuildResult(ctx, b.BuildID, b.RunID, changed); !errors.Is(err, ErrBuildResultConflict) {
		t.Fatalf("conflicting result replay error = %v", err)
	}
	if _, err := st.AcceptBuildResult(ctx, b.BuildID, "br-wrong-owner", result); !errors.Is(err, ErrBuildExecutionOwnership) {
		t.Fatalf("wrong-run result error = %v", err)
	}
	loaded, err := st.GetBuild(ctx, b.BuildID)
	if err != nil || loaded.ExecutionResult == nil || *loaded.ExecutionResult != result {
		t.Fatalf("durable result = %+v, err=%v", loaded, err)
	}
	loaded.Status = types.BuildError
	loaded.RuntimeVswitchPort = "7"
	loaded.RuntimeFloatingIP = "192.0.2.7"
	loaded.RuntimePortMAC = "02:00:00:00:00:07"
	loaded.RuntimeEnvdAccessToken = "runtime-token"
	loaded.RuntimePrepareJSON = `{"schema_version":1}`
	if err := st.PutBuildTerminal(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	terminal, err := st.GetBuild(ctx, b.BuildID)
	if err != nil || terminal.ExecutionResult != nil || terminal.ExecutionClaimed ||
		terminal.RuntimeVswitchPort != "" || terminal.RuntimeEnvdAccessToken != "" || terminal.RuntimePrepareJSON != "" {
		t.Fatalf("terminal result cleanup = %+v, err=%v", terminal, err)
	}
}

func TestBuildPhaseTransitionsFenceStaleReports(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	b := admissionBuild("phase-owner", types.BuildResources{CPU: 1000, Memory: 1 << 30})
	b.Status, b.ExecutionClaimed = types.BuildBuilding, true
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "a", "bp-a-owner", "starting"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "a", "bp-a-owner", "finished"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "a", "bp-a-owner", "finished"); err != nil {
		t.Fatalf("idempotent finished replay: %v", err)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "b", "bp-b-owner", "starting"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "a", "bp-a-owner", "failed"); err == nil {
		t.Fatal("stale phase A failure cleared or replaced phase B")
	}
	loaded, err := st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != "b" || loaded.PhaseSandboxID != "bp-b-owner" {
		t.Fatalf("active phase after stale report = %q %q", loaded.Phase, loaded.PhaseSandboxID)
	}
	if err := st.SetBuildPhase(ctx, b.BuildID, "b", "bp-b-owner", "failed"); err != nil {
		t.Fatal(err)
	}
	loaded, err = st.GetBuild(ctx, b.BuildID)
	if err != nil || loaded.Phase != "b" || loaded.PhaseSandboxID != "bp-b-owner" {
		t.Fatalf("failed phase remains locatable = %+v, %v", loaded, err)
	}
}
