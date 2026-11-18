package orch

import (
	"context"
	"errors"
	"fmt"
	connector "github.com/kuasar-sandbox/connector/pkg/vswitch"
	"github.com/kuasar-sandbox/orchestrator/internal/hostmetrics"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestNativePortTrafficProjectionKeepsUnknownDistinctFromZero(t *testing.T) {
	now := time.Now()
	zero := uint64(0)
	value := uint64(17)
	got := networkCounterSamples("platform", api.TrafficCounters{RXPackets: &zero, RXBytes: &value}, now)
	if len(got) != 2 || got[0].Name != "sandbox.traffic.platform.rx.packets" || got[0].IntValue == nil || *got[0].IntValue != 0 || got[1].Name != "sandbox.traffic.platform.rx.bytes" || got[1].IntValue == nil || *got[1].IntValue != 17 {
		t.Fatalf("native observation projection %+v", got)
	}
	if len(networkCounterSamples("transit", api.TrafficCounters{}, now)) != 0 {
		t.Fatal("unknown must not become zero")
	}
}

func TestNetworkMetricLifecycleCoverage(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePausing, types.StatePaused} {
		if !networkMetricEligible(&types.Sandbox{VswitchPort: "2", State: state}) {
			t.Fatalf("state %s lost network stats", state)
		}
	}
	for _, state := range []types.State{types.StateDead, types.StateDeleting} {
		if networkMetricEligible(&types.Sandbox{VswitchPort: "2", State: state}) {
			t.Fatalf("stale state %s observed", state)
		}
	}
	if networkMetricEligible(&types.Sandbox{State: types.StateRunning}) {
		t.Fatal("no-port sandbox should not be queried")
	}
}

func testHostObserver(t *testing.T, o *Orchestrator) *conductorHostObserver {
	t.Helper()
	path := filepath.Join(t.TempDir(), "host.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	t.Setenv("KS_HOST_OTLP_SOCKET", path)
	h := &conductorHostObserver{publisher: hostmetrics.New("node-ctl", "conductor", nil), events: make(chan hostmetrics.Group, 8), states: make(map[string]observedSandboxState)}
	o.hostObserver = h
	return h
}

func TestHostLifecyclePublishesTransitionsAndCommittedDeletion(t *testing.T) {
	o := testOrch(t)
	h := testHostObserver(t, o)
	ids := batchSandboxes(t, o, 1)
	sb, err := o.st.Get(t.Context(), ids[0])
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	o.publishUpsert(sb)
	o.publishUpsert(sb)
	sb.State = types.StateDeleting
	o.publishUpsert(sb)
	o.publishDelete(sb.ID)
	o.publishDelete("missing")
	o.observeSandboxDelete(sb)
	if len(h.events) != 3 {
		t.Fatalf("got %d observations", len(h.events))
	}
	for _, state := range []string{"running", "deleting", "deleted"} {
		group := <-h.events
		sample := group.Samples[0]
		if group.Resource["sandbox.id"] != sb.ID || sample.Name != "sandbox.lifecycle.state" || sample.Attributes["state"] != state || sample.Timestamp.Before(before) {
			t.Fatalf("bad transition %+v", group)
		}
	}
	if len(h.states) != 0 {
		t.Fatalf("retained state for a deleted sandbox: %+v", h.states)
	}
	for range cap(h.events) {
		h.offer(hostmetrics.Group{})
	}
	if h.offer(hostmetrics.Group{}) {
		t.Fatal("full queue accepted another observation")
	}
}

type hostFinalSwitch struct {
	*trafficSwitchStub
	detach func(context.Context, string) error
}

func (s *hostFinalSwitch) Detach(ctx context.Context, port string) error { return s.detach(ctx, port) }

func TestHostFinalNetworkBeforeDetach(t *testing.T) {
	for _, test := range []string{"exact", "stale", "missing-mac", "wrong-mac", "busy", "pending"} {
		t.Run(test, func(t *testing.T) {
			o := testOrch(t)
			ids := trafficBindings(t, o, 1)
			h := testHostObserver(t, o)
			sb, err := o.st.Get(t.Context(), ids[0])
			if err != nil {
				t.Fatal(err)
			}
			read, detach := 0, 0
			o.vs = &hostFinalSwitch{&trafficSwitchStub{vsClient: o.vs, read: func(_ context.Context, ports []int) (*connector.StatsOutput, error) {
				read++
				if detach != 0 {
					t.Fatal("read after detach")
				}
				rows := trafficRows(o, ports)
				rows.Ports[0].Generation = 0
				rows.Ports[0].PortMAC = sb.PortMAC
				if test == "wrong-mac" {
					rows.Ports[0].PortMAC = "00:11:22:33:44:55"
				}
				if test == "missing-mac" {
					rows.Ports[0].PortMAC = ""
				}
				return rows, nil
			}}, func(context.Context, string) error { detach++; return nil }}
			switch test {
			case "stale":
				sb.RunID = "old-run"
			case "busy":
				for range cap(o.nativeStatsSlots) {
					o.nativeStatsSlots <- struct{}{}
				}
			case "pending":
				o.detachedPortsPending[sb.VswitchPort] = struct{}{}
			}
			if err := o.detachSandboxPort(t.Context(), sb.VswitchPort, sb); err != nil {
				t.Fatal(err)
			}
			if detach != 1 {
				t.Fatal("telemetry prevented detach")
			}
			if test == "exact" {
				if len(h.events) != 1 {
					t.Fatal("no final packet snapshot")
				}
				g := <-h.events
				if g.Resource["port"] != "1" || g.Resource["switch"] != "test-traffic" || g.Resource["sandbox.run_id"] != sb.RunID || g.Resource["attachment.generation"] != "0" ||
					len(g.Samples) != 8 || *g.Samples[1].IntValue != 9007199254740993 || *g.Samples[2].IntValue != 0 {
					t.Fatalf("counter precision, observed zero, or identity lost: %+v", g)
				}
			} else if len(h.events) != 0 {
				t.Fatalf("incorrectly attributed: %+v", <-h.events)
			}
			if (test == "busy" || test == "stale" || test == "pending") && read != 0 {
				t.Fatalf("invalid read reached connector: %d", read)
			}
		})
	}
}

func TestHostNetworkBatchOnlySplitsBindingFailures(t *testing.T) {
	o := testOrch(t)
	ids := trafficBindings(t, o, 8)
	objects := make([]*types.Sandbox, 0, len(ids))
	for _, id := range ids {
		sb, err := o.st.Get(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, sb)
	}
	calls := 0
	var fail error = errors.New("unavailable switch")
	o.vs = &trafficSwitchStub{vsClient: o.vs, read: func(_ context.Context, ports []int) (*connector.StatsOutput, error) {
		calls++
		if fail != nil {
			return nil, fail
		}
		rows := trafficRows(o, ports)
		for i, port := range ports {
			rows.Ports[i].PortMAC = fmt.Sprintf("02:00:00:00:01:%02x", port)
		}
		return rows, nil
	}}
	if groups := o.readHostNetworkBatch(t.Context(), objects); calls != 1 || len(groups) != 0 {
		t.Fatalf("switch failure calls=%d, groups=%d", calls, len(groups))
	}
	fail = nil
	objects[4].RunID = "superseded"
	groups := o.readHostNetworkBatch(t.Context(), objects)
	if len(groups) != len(objects)-1 {
		t.Fatalf("dropped healthy rows alongside stale binding: %d", len(groups))
	}
	for _, g := range groups {
		if g.Resource["sandbox.id"] == objects[4].ID {
			t.Fatal("stale row attributed")
		}
	}
}
