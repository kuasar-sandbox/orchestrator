package orch

import (
	"context"
	"errors"
	"fmt"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCheckpointQuotaDirectAndClusterCreate(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, value := range []int{-2, 0, 1, 3, -1} {
			t.Run(fmt.Sprintf("cluster=%t/value=%d", cluster, value), func(t *testing.T) {
				// -2 denotes an omitted field; explicit zero must be rejected.
				var quota *int
				if value != -2 {
					v := value
					quota = &v
				}
				want := value
				if quota == nil {
					want = 1
				}
				valid := want > 0
				gate := make(chan struct{})
				o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, &countingLauncher{startGate: gate})
				defer close(gate)
				var sid string
				if cluster {
					_, _, fingerprint := allowlistedBuildIdentity(t, o)
					cmd := clusterCreateCommand(fingerprint, "checkpoint-quota")
					cmd.MaxCheckpoints = quota
					ack := o.HandleCommand(ctx, cmd)
					if (ack.Status == routesync.AckAccepted) != valid {
						t.Fatalf("cluster acceptance: %+v", ack)
					}
					sid = cmd.SID
				} else {
					req := createRequestFixture(t, o, "1")
					req.MaxCheckpoints = quota
					accepted, err := o.Create(ctx, req)
					if (err == nil) != valid {
						t.Fatalf("direct acceptance: %+v %v", accepted, err)
					}
					if accepted != nil {
						sid = accepted.ID
						if accepted.MaxCheckpoints != want {
							t.Fatalf("accepted quota=%d want=%d", accepted.MaxCheckpoints, want)
						}
					}
				}
				if valid {
					stored, err := o.st.Get(ctx, sid)
					if err != nil || stored == nil || stored.State != types.StateStarting || stored.MaxCheckpoints != want {
						t.Fatalf("durable quota: %+v %v", stored, err)
					}
				} else if sid != "" {
					if stored, err := o.st.Get(ctx, sid); err != nil || stored != nil {
						t.Fatalf("rejected quota created a parent: %+v %v", stored, err)
					}
				}
			})
		}
	}
}

func TestCheckpointQuotaCreateMetadata(t *testing.T) {
	for _, test := range []struct {
		name          string
		raw           string
		internalQuota *int
		wantQuota     int
		wantMetadata  string
		invalid       bool
	}{
		{name: "quota only", raw: `{"max_checkpoints":3}`, wantQuota: 3},
		{name: "snapshot defaults retained", raw: ` { "max_checkpoints": 4, "drop_caches": true, "merge_ref": false } `, wantQuota: 4, wantMetadata: `{"merge_ref":false,"drop_caches":true}`},
		{name: "null uses default", raw: `{"max_checkpoints":null,"merge_ref":false}`, wantQuota: 1, wantMetadata: `{"merge_ref":false}`},
		{name: "unset uses default", raw: `{}`, wantQuota: 1},
		{name: "same internal quota", raw: `{"max_checkpoints":3}`, internalQuota: checkpointQuotaCreateInt(3), wantQuota: 3},
		{name: "null preserves internal quota", raw: `{"max_checkpoints":null}`, internalQuota: checkpointQuotaCreateInt(4), wantQuota: 4},
		{name: "zero rejected", raw: `{"max_checkpoints":0}`, invalid: true},
		{name: "negative rejected", raw: `{"max_checkpoints":-1}`, invalid: true},
		{name: "unknown field rejected", raw: `{"max_checkpoints":3,"unknown":true}`, invalid: true},
		{name: "conflicting internal quota rejected", raw: `{"max_checkpoints":3}`, internalQuota: checkpointQuotaCreateInt(2), invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := make(chan struct{})
			launcher := &countingLauncher{startGate: gate}
			o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, launcher)
			defer close(gate)
			req := createRequestFixture(t, o, "1")
			req.Metadata[sandboxcfg.NsCheckpoint] = test.raw
			req.MaxCheckpoints = test.internalQuota

			accepted, err := o.Create(ctx, req)
			if req.Metadata[sandboxcfg.NsCheckpoint] != test.raw {
				t.Fatalf("Create mutated request metadata: %+v", req.Metadata)
			}
			if test.invalid {
				if !errors.Is(err, api.ErrBadRequest) || accepted != nil {
					t.Fatalf("invalid quota Create = %+v, %v", accepted, err)
				}
				rows, _, listErr := o.st.List(ctx, "", "", 100, "")
				if listErr != nil || len(rows) != 0 || launcher.starts.Load() != 0 {
					t.Fatalf("rejected quota side effects: rows=%+v, err=%v, starts=%d", rows, listErr, launcher.starts.Load())
				}
				return
			}
			if err != nil || accepted == nil {
				t.Fatalf("Create = %+v, %v", accepted, err)
			}
			stored, err := o.st.Get(ctx, accepted.ID)
			if err != nil || stored == nil {
				t.Fatalf("Get = %+v, %v", stored, err)
			}
			for _, sb := range []*types.Sandbox{accepted, stored} {
				if sb.MaxCheckpoints != test.wantQuota {
					t.Fatalf("quota=%d want=%d", sb.MaxCheckpoints, test.wantQuota)
				}
				got, present := sb.Metadata[sandboxcfg.NsCheckpoint]
				if got != test.wantMetadata || present != (test.wantMetadata != "") {
					t.Fatalf("snapshot metadata=%q present=%t, want=%q", got, present, test.wantMetadata)
				}
				if sb.Metadata["test-key"] != "test-value" {
					t.Fatalf("unrelated metadata changed: %+v", sb.Metadata)
				}
				if _, err := checkpointMetadataPolicy(sb.Metadata); err != nil {
					t.Fatalf("stored snapshot policy is not valid for Pause: %v", err)
				}
			}
		})
	}
}

func TestCheckpointQuotaCreateHookBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		hookRaw string
		invalid bool
	}{
		{name: "snapshot defaults remain writable", hookRaw: `{"merge_ref":true,"drop_caches":false}`},
		{name: "quota cannot change", hookRaw: `{"max_checkpoints":2}`, invalid: true},
		{name: "quota cannot be reintroduced", hookRaw: `{"max_checkpoints":4}`, invalid: true},
		{name: "null quota cannot be reintroduced", hookRaw: `{"max_checkpoints":null}`, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := make(chan struct{})
			launcher := &countingLauncher{startGate: gate}
			o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, launcher)
			defer close(gate)
			req := createRequestFixture(t, o, "1")
			req.Metadata[sandboxcfg.NsCheckpoint] = `{"max_checkpoints":4,"merge_ref":false}`
			called := false
			o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
				called = true
				if got := operation.Create.Metadata[sandboxcfg.NsCheckpoint]; got != `{"merge_ref":false}` {
					t.Fatalf("Hook received quota carrier: %q", got)
				}
				operation.Create.Metadata[sandboxcfg.NsCheckpoint] = test.hookRaw
				return nil
			}), nil)

			accepted, err := o.Create(ctx, req)
			if !called {
				t.Fatal("Create did not invoke Hook")
			}
			if test.invalid {
				if !errors.Is(err, api.ErrBadRequest) || accepted != nil {
					t.Fatalf("quota mutation Create = %+v, %v", accepted, err)
				}
				rows, _, listErr := o.st.List(ctx, "", "", 100, "")
				if listErr != nil || len(rows) != 0 || launcher.starts.Load() != 0 {
					t.Fatalf("rejected Hook quota side effects: rows=%+v, err=%v, starts=%d", rows, listErr, launcher.starts.Load())
				}
				return
			}
			if err != nil || accepted == nil {
				t.Fatalf("Create = %+v, %v", accepted, err)
			}
			stored, err := o.st.Get(ctx, accepted.ID)
			if err != nil || stored == nil || stored.MaxCheckpoints != 4 || stored.Metadata[sandboxcfg.NsCheckpoint] != test.hookRaw {
				t.Fatalf("Hook changed core quota or lost snapshot defaults: %+v, %v", stored, err)
			}
		})
	}
}

func checkpointQuotaCreateInt(value int) *int {
	return &value
}
