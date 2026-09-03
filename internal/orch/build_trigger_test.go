package orch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func registerTriggerTestBuild(t *testing.T, o *Orchestrator, apiKey string) *types.Build {
	t.Helper()
	b, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Name:      "trigger-test",
		Tags:      []string{"trigger-test-alias"},
		Profile:   types.ProfileE2B,
		Resources: types.BuildResources{CPU: 2000, Memory: 2 << 30},
	})
	if err != nil {
		t.Fatalf("RegisterBuild: %v", err)
	}
	return b
}

func TestRegisteredAndWaitingBuildDoNotCreateObjectDirectories(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = t.TempDir()
	o.cfg.Paths.BaseRoot = t.TempDir()
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)

	requireBuildDirectoriesAbsent := func(state types.BuildState) {
		t.Helper()
		for _, path := range []string{
			nodepath.BuildRunDir(o.cfg.Paths.RunRoot, b.BuildID),
			nodepath.BuildBaseDir(o.cfg.Paths.BaseRoot, b.BuildID),
		} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s Build created %s before execution claim: %v", state, path, err)
			}
		}
	}
	requireBuildDirectoriesAbsent(types.BuildRegistered)

	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID,
		api.TriggerSpec{FromImage: "registry.test/no-early-directory:latest"}, api.BuildAuth{}); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	requireBuildDirectoriesAbsent(types.BuildWaiting)
}

func TestClusterWaitingProjectionPrecedesConcurrentBuildClaimWithoutExtension(t *testing.T) {
	o := testOrch(t)
	o.vs = failingCreateVS{err: errors.New("stop after execution claim")}
	ctx := context.Background()
	apiKey, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterBuildRegisterCommand("waiting-before-building", fingerprint)
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("BuildRegister ack = %+v", ack)
	}
	buildEvents, cancelBuildEvents := o.SubscribeBuilds()
	defer cancelBuildEvents()

	committed := make(chan struct{})
	releaseTrigger := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseTrigger)
			released = true
		}
	}()
	o.commitBuildTrigger = func(ctx context.Context, candidate *types.Build) (bool, error) {
		won, err := o.st.CommitBuildTrigger(ctx, candidate)
		if err == nil && won {
			close(committed)
			<-releaseTrigger
		}
		return won, err
	}
	triggerDone := make(chan error, 1)
	go func() {
		triggerDone <- o.TriggerBuild(ctx, apiKey, cmd.TemplateRef, cmd.BuildID,
			api.TriggerSpec{FromImage: "registry.test/waiting-order:latest"}, api.BuildAuth{})
	}()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("TriggerBuild did not commit waiting")
	}

	poolCtx, cancelPool := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		o.BuildPool(poolCtx, time.Hour)
		close(poolDone)
	}()
	defer func() {
		if !released {
			close(releaseTrigger)
			released = true
		}
		cancelPool()
		<-poolDone
		if err := o.DrainBuilds(context.Background()); err != nil {
			t.Errorf("DrainBuilds: %v", err)
		}
	}()

	// Wait until BuildPool has selected the durable waiting row and is queued
	// behind TriggerBuild's publication fence. This makes the old
	// waiting-after-building race deterministic rather than timing-dependent.
	deadline := time.Now().Add(5 * time.Second)
	for {
		o.buildEventFences.mu.Lock()
		lock := o.buildEventFences.locks[cmd.BuildID]
		refs := 0
		if lock != nil {
			refs = lock.refs
		}
		o.buildEventFences.mu.Unlock()
		if refs >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("BuildPool did not wait on the committed Build publication fence")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseTrigger)
	released = true
	if err := <-triggerDone; err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}

	for index, want := range []types.BuildState{types.BuildWaiting, types.BuildBuilding} {
		select {
		case event := <-buildEvents:
			if event.Kind != routesync.BuildUpsert || event.BuildID != cmd.BuildID || event.State != string(want) {
				t.Fatalf("Build event %d = %+v, want %s upsert", index, event, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for Build event %d (%s)", index, want)
		}
	}
}

func TestDirectBuildEndpointsRejectInvalidBuildIDAtBoundary(t *testing.T) {
	o := testOrch(t)
	for _, buildID := range []string{strings.Repeat("x", 49), "build/id"} {
		if err := o.TriggerBuild(context.Background(), "unused", "unused", buildID,
			api.TriggerSpec{}, api.BuildAuth{}); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("TriggerBuild(%q) error = %v, want ErrBadRequest", buildID, err)
		}
		if _, err := o.BuildStatus(context.Background(), "unused", "unused", buildID); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("BuildStatus(%q) error = %v, want ErrBadRequest", buildID, err)
		}
		if _, err := o.BuildLogs(context.Background(), "unused", "unused", buildID, 0); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("BuildLogs(%q) error = %v, want ErrBadRequest", buildID, err)
		}
	}
}

func requireBuildStateConflict(t *testing.T, err error, want types.BuildState) {
	t.Helper()
	var conflict *api.BuildStateConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %v, want BuildStateConflictError", err)
	}
	if conflict.State != want {
		t.Fatalf("conflict state = %q, want %q", conflict.State, want)
	}
}

func TestTriggerBuildStateMatrix(t *testing.T) {
	states := []types.BuildState{
		types.BuildRegistered,
		types.BuildWaiting,
		types.BuildBuilding,
		types.BuildReady,
		types.BuildError,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			apiKey, _, _ := allowlistedBuildIdentity(t, o)
			registered := registerTriggerTestBuild(t, o, apiKey)
			registered.Status = state
			registered.PersistID = "persist-before"
			registered.Reason = "reason-before"
			registered.RunID = "run-before"
			registered.Names = []string{"name-before"}
			registered.Aliases = []string{"alias-before"}
			if err := o.st.PutBuild(ctx, registered); err != nil {
				t.Fatal(err)
			}
			before, err := o.st.GetBuild(ctx, registered.BuildID)
			if err != nil {
				t.Fatal(err)
			}

			spec := api.TriggerSpec{
				FromImage: "registry.test/trigger:latest",
				Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"echo", "triggered"}}},
				StartCmd:  "serve",
				ReadyCmd:  "ready",
			}
			err = o.TriggerBuild(ctx, apiKey, registered.TemplateID, registered.BuildID, spec, api.BuildAuth{
				RegistryUsername: "trigger-user",
				RegistryPassword: "trigger-password",
			})
			after, getErr := o.st.GetBuild(ctx, registered.BuildID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if state == types.BuildRegistered {
				if err != nil {
					t.Fatalf("TriggerBuild: %v", err)
				}
				if after.Status != types.BuildWaiting || after.Kind != "" ||
					after.FromImage != spec.FromImage || after.StartCmd != spec.StartCmd ||
					after.ReadyCmd != spec.ReadyCmd || !reflect.DeepEqual(after.Steps, spec.Steps) {
					t.Fatalf("committed trigger work order = %#v", after)
				}
				var creds regcreds.Creds
				if err := json.Unmarshal([]byte(after.RegistryAuth), &creds); err != nil {
					t.Fatalf("decode registry auth: %v", err)
				}
				if creds.Username != "trigger-user" || creds.Password != "trigger-password" {
					t.Fatalf("registry credentials = %#v", creds)
				}
				if after.TemplateID != before.TemplateID || after.PersistID != before.PersistID ||
					after.Profile != before.Profile || after.CreatedUnix != before.CreatedUnix ||
					after.Reason != before.Reason || after.RunID != before.RunID ||
					!reflect.DeepEqual(after.Names, before.Names) || !reflect.DeepEqual(after.Aliases, before.Aliases) {
					t.Fatalf("trigger changed non-work-order fields:\n before: %#v\n after: %#v", before, after)
				}
				return
			}
			requireBuildStateConflict(t, err, state)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("conflicting trigger changed build:\n before: %#v\n after: %#v", before, after)
			}
		})
	}
}

func TestTriggerBuildChecksOwnershipBeforeState(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)
	b.Status = types.BuildReady
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	_, otherAPIKey := defaultTestCredentials(t, strings.Repeat("6", 64))

	for name, keyAndTemplate := range map[string][2]string{
		"wrong owner":    {otherAPIKey, b.TemplateID},
		"wrong template": {apiKey, "transient-other"},
	} {
		t.Run(name, func(t *testing.T) {
			err := o.TriggerBuild(ctx, keyAndTemplate[0], keyAndTemplate[1], b.BuildID,
				api.TriggerSpec{FromImage: "registry.test/retry:latest"}, api.BuildAuth{})
			if !errors.Is(err, api.ErrNotFound) {
				t.Fatalf("error = %v, want ErrNotFound", err)
			}
			var conflict *api.BuildStateConflictError
			if errors.As(err, &conflict) {
				t.Fatalf("authorization failure leaked state %q", conflict.State)
			}
		})
	}
}

func TestTriggerBuildResourceAssertionPrecedesSideEffects(t *testing.T) {
	value := func(v int64) *int64 { return &v }
	for _, test := range []struct {
		name      string
		assertion buildcfg.ResourcePatch
	}{
		{name: "cpu increase", assertion: buildcfg.ResourcePatch{CPU: value(2001)}},
		{name: "cpu decrease", assertion: buildcfg.ResourcePatch{CPU: value(1999)}},
		{name: "memory increase", assertion: buildcfg.ResourcePatch{Memory: value((2 << 30) + 1)}},
		{name: "memory decrease", assertion: buildcfg.ResourcePatch{Memory: value((2 << 30) - 1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testOrch(t)
			ctx := context.Background()
			apiKey, _, _ := allowlistedBuildIdentity(t, o)
			b := registerTriggerTestBuild(t, o, apiKey)
			before, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			// The deliberately invalid pull token proves the immutable resource
			// assertion is checked before credential resolution.
			err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
				FromImage:         "registry.test/assertion:latest",
				ResourceAssertion: test.assertion,
			}, api.BuildAuth{PullToken: "not-a-sealed-token"})
			if !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "does not match registered value") {
				t.Fatalf("TriggerBuild error = %v, want resource assertion ErrBadRequest", err)
			}
			after, err := o.st.GetBuild(ctx, b.BuildID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected assertion changed durable build:\n before: %#v\n after: %#v", before, after)
			}
		})
	}

	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/assertion-equal:latest",
		ResourceAssertion: buildcfg.ResourcePatch{
			CPU: value(2000), Memory: value(2 << 30),
		},
	}, api.BuildAuth{}); err != nil {
		t.Fatalf("equal assertion: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored.Status != types.BuildWaiting || stored.Resources != b.Resources {
		t.Fatalf("equal assertion result = %+v, %v", stored, err)
	}
}

func TestTriggerBuildConcurrentHasSingleCompleteWinner(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)

	type candidate struct {
		label string
		spec  api.TriggerSpec
		auth  api.BuildAuth
	}
	candidates := []candidate{
		{
			label: "a",
			spec: api.TriggerSpec{
				FromImage: "registry.test/concurrent-a:latest",
				Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"steps-a"}}},
				StartCmd:  "start-a",
				ReadyCmd:  "ready-a",
			},
			auth: api.BuildAuth{RegistryUsername: "user-a", RegistryPassword: "password-a"},
		},
		{
			label: "b",
			spec: api.TriggerSpec{
				FromImage: "registry.test/concurrent-b:latest",
				Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"steps-b"}}},
				StartCmd:  "start-b",
				ReadyCmd:  "ready-b",
			},
			auth: api.BuildAuth{RegistryUsername: "user-b", RegistryPassword: "password-b"},
		},
	}

	arrived := make(chan struct{}, len(candidates))
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	o.commitBuildTrigger = func(ctx context.Context, candidate *types.Build) (bool, error) {
		arrived <- struct{}{}
		<-release
		return o.st.CommitBuildTrigger(ctx, candidate)
	}
	type result struct {
		candidate int
		err       error
	}
	results := make(chan result, len(candidates))
	for i := range candidates {
		go func(candidate int) {
			request := candidates[candidate]
			results <- result{candidate: candidate, err: o.TriggerBuild(
				ctx, apiKey, b.TemplateID, b.BuildID, request.spec, request.auth,
			)}
		}(i)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first concurrent trigger did not reach the commit barrier")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		o.buildEventFences.mu.Lock()
		lock := o.buildEventFences.locks[b.BuildID]
		refs := 0
		if lock != nil {
			refs = lock.refs
		}
		o.buildEventFences.mu.Unlock()
		if refs >= len(candidates) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second concurrent trigger did not reach the Build event fence")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-arrived:
		t.Fatal("second concurrent trigger bypassed the first trigger publication fence")
	default:
	}
	close(release)
	released = true
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("second concurrent trigger did not reach the commit after publication fence release")
	}

	winner := -1
	for range candidates {
		result := <-results
		if result.err == nil {
			if winner != -1 {
				t.Fatalf("candidates %d and %d both triggered", winner, result.candidate)
			}
			winner = result.candidate
			continue
		}
		requireBuildStateConflict(t, result.err, types.BuildWaiting)
	}
	if winner == -1 {
		t.Fatal("neither concurrent trigger succeeded")
	}

	got, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	want := candidates[winner]
	if got.Status != types.BuildWaiting || got.Kind != "" ||
		got.FromImage != want.spec.FromImage || got.FromTemplate != "" ||
		got.StartCmd != want.spec.StartCmd || got.ReadyCmd != want.spec.ReadyCmd ||
		!reflect.DeepEqual(got.Steps, want.spec.Steps) {
		t.Fatalf("stored concurrent work order is mixed or incomplete: %#v", got)
	}
	var creds regcreds.Creds
	if err := json.Unmarshal([]byte(got.RegistryAuth), &creds); err != nil {
		t.Fatal(err)
	}
	if creds.Username != want.auth.RegistryUsername || creds.Password != want.auth.RegistryPassword {
		t.Fatalf("stored registry credentials = %#v, want candidate %s", creds, want.label)
	}
	if got.Builder.Referer != nil || len(got.Metadata) != 0 {
		t.Fatalf("trigger modified sealed build definition: metadata=%#v builder=%#v", got.Metadata, got.Builder)
	}
}

func TestTriggerBuildCannotOverwritePoolClaim(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)
	first := api.TriggerSpec{
		FromImage: "registry.test/first:latest",
		Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"first"}}},
		StartCmd:  "first-start",
	}
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, first, api.BuildAuth{
		RegistryUsername: "first-user", RegistryPassword: "first-password",
	}); err != nil {
		t.Fatalf("first TriggerBuild: %v", err)
	}

	execution, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	limit, _ := configresolve.BuilderExecutionLimit(o.cfg.Builder)
	if won, err := o.st.ClaimBuildExecution(ctx, execution.BuildID, limit, time.Now()); err != nil || !won {
		t.Fatalf("ClaimBuildExecution: won=%t err=%v", won, err)
	}
	if err := o.st.SetBuildRunID(ctx, b.BuildID, "run-active"); err != nil {
		t.Fatal(err)
	}
	execution, err = o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(execution, claimed) {
		t.Fatalf("execution work order differs from claimed row:\n execution: %#v\n row: %#v", execution, claimed)
	}

	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/second:latest",
		Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"second"}}},
		StartCmd:  "second-start",
	}, api.BuildAuth{RegistryUsername: "second-user", RegistryPassword: "second-password"})
	requireBuildStateConflict(t, err, types.BuildBuilding)
	after, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, claimed) {
		t.Fatalf("second trigger overwrote claimed work order:\n before: %#v\n after: %#v", claimed, after)
	}
}

func TestTriggerBuildTerminalConflictPreservesTemplateAndAllowsNewRegistration(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	ready := registerTriggerTestBuild(t, o, apiKey)
	ready.Status = types.BuildReady
	ready.PersistID = "e2b:img:manifest://ready"
	ready.Reason = "ready reason"
	if err := o.st.PutBuild(ctx, ready); err != nil {
		t.Fatal(err)
	}
	requireBuildStateConflict(t, o.TriggerBuild(ctx, apiKey, ready.TemplateID, ready.BuildID,
		api.TriggerSpec{FromImage: "registry.test/retry-ready:latest"}, api.BuildAuth{}), types.BuildReady)
	templates, err := o.ListTemplates(ctx, apiKey)
	if err != nil || len(templates) != 1 || templates[0].BuildID != ready.BuildID {
		t.Fatalf("ListTemplates after ready conflict = %#v, err=%v", templates, err)
	}

	failed := registerTriggerTestBuild(t, o, apiKey)
	failed.Status = types.BuildError
	failed.Reason = "original deterministic failure"
	if err := o.st.PutBuild(ctx, failed); err != nil {
		t.Fatal(err)
	}
	requireBuildStateConflict(t, o.TriggerBuild(ctx, apiKey, failed.TemplateID, failed.BuildID,
		api.TriggerSpec{FromImage: "registry.test/retry-error:latest"}, api.BuildAuth{}), types.BuildError)
	storedFailed, err := o.st.GetBuild(ctx, failed.BuildID)
	if err != nil || storedFailed.Reason != failed.Reason {
		t.Fatalf("error build after conflict = %#v, err=%v", storedFailed, err)
	}

	replacement := registerTriggerTestBuild(t, o, apiKey)
	if replacement.BuildID == ready.BuildID || replacement.BuildID == failed.BuildID {
		t.Fatalf("replacement reused build id %q", replacement.BuildID)
	}
	if err := o.TriggerBuild(ctx, apiKey, replacement.TemplateID, replacement.BuildID,
		api.TriggerSpec{FromImage: "registry.test/replacement:latest"}, api.BuildAuth{}); err != nil {
		t.Fatalf("replacement TriggerBuild: %v", err)
	}
}
