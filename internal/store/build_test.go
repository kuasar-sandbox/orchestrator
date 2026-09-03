package store

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func buildTriggerFixture(id string, status types.BuildState) *types.Build {
	enabled := true
	build := &types.Build{
		BuildID:      id,
		TemplateID:   "transient-original",
		APISecret:    strings.Repeat("2", 64),
		ManifestKey:  strings.Repeat("1", 64),
		Profile:      types.ProfileE2B,
		FromImage:    "registry.test/original:latest",
		FromTemplate: "",
		RegistryAuth: "original-registry-auth",
		StartCmd:     "original-start",
		ReadyCmd:     "original-ready",
		Steps:        []types.TemplateStep{{Type: "RUN", Args: []string{"original"}}},
		Status:       status,
		Reason:       "original reason",
		RunID:        "builder-original",
		Names:        []string{"original-name"},
		Aliases:      []string{"original-alias"},
		Metadata:     map[string]string{"original": "metadata"},
		Builder: types.BuildOptions{Referer: &types.BuildRefererOptions{
			Enabled: &enabled,
		}},
		CreatedUnix: 12345,
	}
	if status == types.BuildReady {
		build.Kind = types.KindImg
		build.PersistID = "e2b:img:manifest://original"
	}
	return build
}

func buildTriggerCandidate(base *types.Build, label string) *types.Build {
	disabled := false
	candidate := *base
	// These registration/lifecycle fields deliberately differ. The trigger
	// commit must never write them.
	candidate.TemplateID = "transient-candidate-" + label
	candidate.PersistID = "persist-candidate-" + label
	candidate.APISecret = "api-candidate-" + label
	candidate.ManifestKey = "manifest-candidate-" + label
	candidate.Profile = types.ProfileBare
	candidate.Reason = "reason-candidate-" + label
	candidate.RunID = "run-candidate-" + label
	candidate.Names = []string{"name-candidate-" + label}
	candidate.Aliases = []string{"alias-candidate-" + label}
	candidate.CreatedUnix = 99999
	candidate.Kind = ""

	candidate.FromImage = "registry.test/image-" + label + ":latest"
	candidate.FromTemplate = "template-" + label
	candidate.RegistryAuth = "registry-auth-" + label
	candidate.StartCmd = "start-" + label
	candidate.ReadyCmd = "ready-" + label
	candidate.Steps = []types.TemplateStep{{Type: "RUN", Args: []string{"steps-" + label}, Force: true}}
	candidate.Metadata = map[string]string{"metadata": label}
	candidate.Builder = types.BuildOptions{Referer: &types.BuildRefererOptions{Enabled: &disabled}}
	candidate.Status = types.BuildWaiting
	return &candidate
}

func applyTriggerWorkOrder(dst, src *types.Build) {
	dst.FromImage = src.FromImage
	dst.FromTemplate = src.FromTemplate
	dst.RegistryAuth = src.RegistryAuth
	dst.StartCmd = src.StartCmd
	dst.ReadyCmd = src.ReadyCmd
	dst.Steps = src.Steps
	dst.Status = types.BuildWaiting
}

func TestCommitBuildTriggerStateMatrix(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	states := []types.BuildState{
		types.BuildRegistered,
		types.BuildWaiting,
		types.BuildBuilding,
		types.BuildReady,
		types.BuildError,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			original := buildTriggerFixture("build-"+string(state), state)
			if err := st.PutBuild(ctx, original); err != nil {
				t.Fatal(err)
			}
			before, err := st.GetBuild(ctx, original.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			var registryAuthBefore string
			if err := st.db.QueryRowContext(ctx, `SELECT registry_auth_enc FROM builds WHERE build_id=?`, original.BuildID).Scan(&registryAuthBefore); err != nil {
				t.Fatal(err)
			}
			candidate := buildTriggerCandidate(before, string(state))
			committed, err := st.CommitBuildTrigger(ctx, candidate)
			if err != nil {
				t.Fatalf("CommitBuildTrigger: %v", err)
			}
			if committed != (state == types.BuildRegistered) {
				t.Fatalf("committed = %t for state %q", committed, state)
			}
			after, err := st.GetBuild(ctx, original.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			want := before
			if state == types.BuildRegistered {
				if after.WaitingSequence <= 0 {
					t.Fatalf("committed trigger has invalid FIFO sequence %d", after.WaitingSequence)
				}
				wantCopy := *before
				applyTriggerWorkOrder(&wantCopy, candidate)
				wantCopy.WaitingSequence = after.WaitingSequence
				want = &wantCopy
			}
			if !reflect.DeepEqual(after, want) {
				t.Fatalf("stored build after commit:\n got: %#v\nwant: %#v", after, want)
			}
			var registryAuthAfter string
			if err := st.db.QueryRowContext(ctx, `SELECT registry_auth_enc FROM builds WHERE build_id=?`, original.BuildID).Scan(&registryAuthAfter); err != nil {
				t.Fatal(err)
			}
			if state == types.BuildRegistered {
				if registryAuthAfter == candidate.RegistryAuth || registryAuthAfter == registryAuthBefore {
					t.Fatalf("committed registry auth was not newly encrypted: before=%q after=%q", registryAuthBefore, registryAuthAfter)
				}
			} else if registryAuthAfter != registryAuthBefore {
				t.Fatal("failed conditional commit rewrote encrypted registry auth")
			}
		})
	}
}

func TestCommitBuildTriggerConcurrentHasCompleteSingleWinner(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	original := buildTriggerFixture("build-concurrent-trigger", types.BuildRegistered)
	if err := st.PutBuild(ctx, original); err != nil {
		t.Fatal(err)
	}
	before, err := st.GetBuild(ctx, original.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	candidates := []*types.Build{
		buildTriggerCandidate(before, "a"),
		buildTriggerCandidate(before, "b"),
	}
	type result struct {
		candidate int
		committed bool
		err       error
	}
	results := make(chan result, len(candidates))
	start := make(chan struct{})
	for i := range candidates {
		go func(candidate int) {
			<-start
			committed, err := st.CommitBuildTrigger(ctx, candidates[candidate])
			results <- result{candidate: candidate, committed: committed, err: err}
		}(i)
	}
	close(start)

	winner := -1
	for range candidates {
		result := <-results
		if result.err != nil {
			t.Fatalf("candidate %d: %v", result.candidate, result.err)
		}
		if result.committed {
			if winner != -1 {
				t.Fatalf("candidates %d and %d both committed", winner, result.candidate)
			}
			winner = result.candidate
		}
	}
	if winner == -1 {
		t.Fatal("no trigger candidate committed")
	}

	got, err := st.GetBuild(ctx, original.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	want := *before
	applyTriggerWorkOrder(&want, candidates[winner])
	if got.WaitingSequence <= 0 {
		t.Fatalf("winning trigger has invalid FIFO sequence %d", got.WaitingSequence)
	}
	want.WaitingSequence = got.WaitingSequence
	if !reflect.DeepEqual(got, &want) {
		t.Fatalf("stored trigger is mixed or incomplete:\n got: %#v\nwant: %#v", got, &want)
	}
}

func TestCommitBuildTriggerRejectsPreResolvedKind(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	original := buildTriggerFixture("build-pre-resolved-kind", types.BuildRegistered)
	if err := st.PutBuild(ctx, original); err != nil {
		t.Fatal(err)
	}
	candidate := buildTriggerCandidate(original, "pre-resolved")
	candidate.Kind = types.KindSnp
	if committed, err := st.CommitBuildTrigger(ctx, candidate); err == nil || committed || !strings.Contains(err.Error(), "kind must remain unset") {
		t.Fatalf("CommitBuildTrigger pre-resolved kind: committed=%t err=%v", committed, err)
	}
	stored, err := st.GetBuild(ctx, original.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildRegistered || stored.Kind != "" {
		t.Fatalf("rejected trigger mutated build: %+v", stored)
	}
}

func TestCommitBuildTriggerAssignsDurableFIFOSequence(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	var candidates []*types.Build
	for _, id := range []string{"z-triggered-first", "a-triggered-second"} {
		registered := buildTriggerFixture(id, types.BuildRegistered)
		if err := st.PutBuild(ctx, registered); err != nil {
			t.Fatal(err)
		}
		candidate := buildTriggerCandidate(registered, id)
		candidate.WaitingUnix = 100 // deliberately identical second-resolution time
		candidates = append(candidates, candidate)
	}
	for _, candidate := range candidates {
		if committed, err := st.CommitBuildTrigger(ctx, candidate); err != nil || !committed {
			t.Fatalf("CommitBuildTrigger(%s): committed=%t err=%v", candidate.BuildID, committed, err)
		}
	}
	waiting, err := st.BuildsByStatus(ctx, types.BuildWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(waiting) != 2 || waiting[0].BuildID != candidates[0].BuildID || waiting[1].BuildID != candidates[1].BuildID {
		t.Fatalf("durable FIFO order = %+v", waiting)
	}
	if waiting[0].WaitingSequence <= 0 || waiting[1].WaitingSequence != waiting[0].WaitingSequence+1 {
		t.Fatalf("FIFO sequences = %d, %d", waiting[0].WaitingSequence, waiting[1].WaitingSequence)
	}
}

func TestSetBuildRunIDPreservesBuildingStatus(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	b := &types.Build{
		BuildID: "build-1", TemplateID: "transient-1",
		APISecret:   strings.Repeat("2", 64),
		ManifestKey: strings.Repeat("1", 64),
		Profile:     types.ProfileE2B,
		Status:      types.BuildWaiting, CreatedUnix: 1,
	}
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	won, err := st.CASBuildStatus(ctx, b.BuildID, types.BuildWaiting, types.BuildBuilding)
	if err != nil || !won {
		t.Fatalf("CASBuildStatus: won=%v err=%v", won, err)
	}
	if err := st.SetBuildRunID(ctx, b.BuildID, "br-test"); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.BuildBuilding || got.RunID != "br-test" {
		t.Fatalf("build after assignment = status %q run_id %q", got.Status, got.RunID)
	}
}

func TestBuildInstanceConfigIsEncryptedAndRoundTrips(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	b := buildTriggerFixture("build-instance-config", types.BuildRegistered)
	b.Env = map[string]string{"VISIBLE_NAME": "distinct-instance-env-value"}
	b.Secure = true
	b.ServiceSecret = "distinct-service-secret"
	b.EnvdAccessToken = "distinct-envd-token"
	b.TrafficAccessToken = "distinct-traffic-token"
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}

	var ciphertext string
	if err := st.db.QueryRowContext(ctx,
		`SELECT instance_config_enc FROM builds WHERE build_id=?`, b.BuildID,
	).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if ciphertext == "" {
		t.Fatal("instance config ciphertext is empty")
	}
	for _, plaintext := range []string{
		"distinct-instance-env-value",
		b.ServiceSecret,
		b.EnvdAccessToken,
		b.TrafficAccessToken,
	} {
		if strings.Contains(ciphertext, plaintext) {
			t.Fatalf("instance config ciphertext contains plaintext %q", plaintext)
		}
	}

	got, err := st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Env, b.Env) || got.Secure != b.Secure ||
		got.ServiceSecret != b.ServiceSecret || got.EnvdAccessToken != b.EnvdAccessToken ||
		got.TrafficAccessToken != b.TrafficAccessToken {
		t.Fatalf("instance config round trip:\n got: %#v\nwant: %#v", got, b)
	}
}
