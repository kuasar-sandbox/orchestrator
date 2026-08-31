package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRangeSandboxesStableOrderAndCallbackError(t *testing.T) {
	storage := testStore(t)
	ctx := context.Background()
	for _, sandbox := range []*types.Sandbox{
		sandboxInsertFixture("sandbox-c", 2),
		sandboxInsertFixture("sandbox-b", 1),
		sandboxInsertFixture("sandbox-a", 0),
	} {
		if sandbox.ID == "sandbox-b" {
			sandbox.CreatedUnix = 1000
		}
		if err := storage.InsertSandbox(ctx, sandbox); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	if err := storage.RangeSandboxes(ctx, func(sandbox *types.Sandbox) error {
		ids = append(ids, sandbox.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"sandbox-a", "sandbox-b", "sandbox-c"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("RangeSandboxes order=%v, want %v", ids, want)
	}
	wantErr := errors.New("stop range")
	calls := 0
	if err := storage.RangeSandboxes(ctx, func(*types.Sandbox) error {
		calls++
		return wantErr
	}); !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("callback error=%v calls=%d", err, calls)
	}
}

func TestRangeBuildsStreamsOnlyCurrentSetInStableOrder(t *testing.T) {
	storage := testStore(t)
	ctx := context.Background()
	fixtures := []*types.Build{
		buildTriggerFixture("build-ready", types.BuildReady),
		buildTriggerFixture("build-error", types.BuildError),
		buildTriggerFixture("build-building", types.BuildBuilding),
		buildTriggerFixture("build-waiting", types.BuildWaiting),
		buildTriggerFixture("build-registered-b", types.BuildRegistered),
		buildTriggerFixture("build-registered-a", types.BuildRegistered),
	}
	created := map[string]int64{
		"build-registered-a": 1,
		"build-registered-b": 1,
		"build-waiting":      2,
		"build-building":     3,
		"build-error":        4,
		"build-ready":        5,
	}
	for _, build := range fixtures {
		build.CreatedUnix = created[build.BuildID]
		if err := storage.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	if err := storage.RangeBuilds(ctx, func(build *types.Build) error {
		ids = append(ids, build.BuildID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"build-registered-a", "build-registered-b", "build-waiting", "build-building", "build-ready"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("RangeBuilds order/current set=%v, want %v", ids, want)
	}
}
