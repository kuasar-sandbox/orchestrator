package orch

import (
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
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
