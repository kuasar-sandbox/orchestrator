package store

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestHoldConditionalWriteAndStaleUpsert(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("hold", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	type result struct {
		value int64
		ok    bool
		err   error
	}
	out := make(chan result, 2)
	for _, n := range []int64{2, 3} {
		go func() {
			v, changed, err := st.SetHoldValue(ctx, sb.ID, sb.InstanceID, sb.RunID, n, &zero)
			out <- result{v, changed, err}
		}()
	}
	first, second := <-out, <-out
	if !first.ok {
		first, second = second, first
	}
	if !first.ok || first.err != nil || second.ok || !errors.Is(second.err, ErrHoldNotMatched) || first.value != second.value {
		t.Fatalf("concurrent value CAS: %+v, %+v", first, second)
	}
	if actual, _, err := st.SetHoldValue(ctx, sb.ID, sb.InstanceID, sb.RunID, first.value, &zero); !errors.Is(err, ErrHoldNotMatched) || actual != first.value {
		t.Fatal("equal target bypassed mismatched condition", actual, err)
	}
	if err := st.Put(ctx, sb); err != nil { // stale parent snapshot from before either write
		t.Fatal(err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got.HoldValue == nil || *got.HoldValue != first.value {
		t.Fatal("ordinary Put overwrote current hold", got, err)
	}
	// Template/import are inserts; neither executes a writable copy of the hold.
	child := sandboxInsertFixture("child", 1)
	child.HoldValue = got.HoldValue
	if err := st.InsertSandbox(ctx, child); err != nil {
		t.Fatal(err)
	}
	fresh, err := st.Get(ctx, child.ID)
	if err != nil || fresh.HoldValue != nil {
		t.Fatal("new parent inherited hold", err)
	}
	if _, _, err := st.SetHoldValue(ctx, sb.ID, sb.InstanceID, "old-run", 0, nil); !errors.Is(err, ErrHoldState) {
		t.Fatal("wrong run accepted", err)
	}
	if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, sandboxRunResult(sb.ID, sb.RunID, 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.SetHoldValue(ctx, sb.ID, sb.InstanceID, sb.RunID, 0, nil); !errors.Is(err, ErrHoldState) {
		t.Fatal("accepted a write after this run terminated", err)
	}
	if ok, err := st.CommitSandboxDead(ctx, sb); !ok || err != nil {
		t.Fatal(ok, err)
	}
	dead, _ := st.Get(ctx, sb.ID)
	if ok, err := st.BeginSandboxReset(ctx, dead); !ok || err != nil {
		t.Fatal(ok, err)
	}
	resetting, _ := st.Get(ctx, sb.ID)
	if ok, err := st.CommitSandboxReset(ctx, resetting, types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///base/current.sandbox"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	paused, err := st.Get(ctx, sb.ID)
	if err != nil || paused.HoldValue == nil || *paused.HoldValue != first.value {
		t.Fatal("Reset cleared the same local parent's hold", err)
	}
}
