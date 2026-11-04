package store

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"reflect"
	"testing"
)

func TestIdlePolicySurvivesStoreAndReset(t *testing.T) {
	for _, v := range []*int64{nil, new(int64(0)), new(int64(5))} {
		st := testStore(t)
		ctx := context.Background()
		sb := sandboxInsertFixture("idle", 0)
		sb.IdleTimeout = v
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		saved, err := st.Get(ctx, sb.ID)
		if err != nil || !reflect.DeepEqual(saved.IdleTimeout, v) {
			t.Fatal(saved, err)
		}
		result := sandboxRunResult(sb.ID, sb.RunID, 7)
		if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil {
			t.Fatal(err)
		}
		if ok, err := st.CommitSandboxDead(ctx, sb); err != nil || !ok {
			t.Fatal(ok, err)
		}
		dead, _ := st.Get(ctx, sb.ID)
		if ok, err := st.BeginSandboxReset(ctx, dead); err != nil || !ok {
			t.Fatal(ok, err)
		}
		resetting, _ := st.Get(ctx, sb.ID)
		if ok, err := st.CommitSandboxReset(ctx, resetting, types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///base/current.sandbox"}); err != nil || !ok {
			t.Fatal(ok, err)
		}
		paused, _ := st.Get(ctx, sb.ID)
		if !reflect.DeepEqual(paused.IdleTimeout, v) {
			t.Fatal("Reset changed idle policy")
		}
	}
}
