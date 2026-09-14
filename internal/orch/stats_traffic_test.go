package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	connector "github.com/kuasar-sandbox/connector/pkg/vswitch"
	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type trafficSwitchStub struct {
	vsClient
	read func(context.Context, []int) (*connector.StatsOutput, error)
}

func (s *trafficSwitchStub) Stats(ctx context.Context, ports []int) (*connector.StatsOutput, error) {
	return s.read(ctx, ports)
}

type trafficBatchFunc func(context.Context, []proxystats.TrafficQuery) ([]proxystats.BatchResult, error)

func (f trafficBatchFunc) SandboxTrafficStatsBatch(ctx context.Context, queries []proxystats.TrafficQuery) ([]proxystats.BatchResult, error) {
	return f(ctx, queries)
}

func (f trafficBatchFunc) SandboxTrafficStats(context.Context, string, string, types.Profile, types.State) (*api.TrafficStats, error) {
	return nil, errors.New("batch provider was queried per sandbox")
}

func trafficBindings(t *testing.T, o *Orchestrator, count int) []string {
	t.Helper()
	o.cfg.Sandbox.Network.Switch = "test-traffic"
	ids := batchSandboxes(t, o, count)
	for i, id := range ids {
		sb, err := o.st.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		sb.RunID, sb.VswitchPort, sb.FloatingIP = "internal-fence", strconv.Itoa(i+1), fmt.Sprintf("198.18.0.%d", i+1)
		sb.InnerIP = "169.254.1.1/31"
		if err := o.st.Put(context.Background(), sb); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func trafficRows(o *Orchestrator, ports []int) *connector.StatsOutput {
	result := &connector.StatsOutput{Switch: o.cfg.Sandbox.Network.Switch}
	for _, port := range ports {
		result.Ports = append(result.Ports, connector.PortStatsOutput{Port: uint32(port), FloatingIP: fmt.Sprintf("198.18.0.%d", port), InnerIP: "169.254.1.1",
			MgmtRxPackets: uint64(port), MgmtRxBytes: 9007199254740993, MgmtTxPackets: 0, MgmtTxBytes: 7,
			TransitRxPackets: 11, TransitRxBytes: 12, TransitTxPackets: 13, TransitTxBytes: 14})
	}
	return result
}

func TestTrafficFlatBatchUsesCurrentSwitchAndProxyBatches(t *testing.T) {
	o := testOrch(t)
	ids := trafficBindings(t, o, conductorextension.MaxStatsSandboxes)
	var networkCalls, ingressCalls int
	o.vs = &trafficSwitchStub{vsClient: o.vs, read: func(_ context.Context, ports []int) (*connector.StatsOutput, error) {
		networkCalls++
		if len(ports) != len(ids) {
			t.Fatalf("connector received %d ports", len(ports))
		}
		// Connector order is independent of sandbox order.
		rows := trafficRows(o, ports)
		for i, j := 0, len(rows.Ports)-1; i < j; i, j = i+1, j-1 {
			rows.Ports[i], rows.Ports[j] = rows.Ports[j], rows.Ports[i]
		}
		return rows, nil
	}}
	stamp := time.Unix(123, 0).UTC()
	o.SetSandboxTrafficProvider(trafficBatchFunc(func(_ context.Context, queries []proxystats.TrafficQuery) ([]proxystats.BatchResult, error) {
		ingressCalls++
		result := make([]proxystats.BatchResult, len(queries))
		for i, q := range queries {
			if q.SandboxID != ids[i] || q.RunID != "internal-fence" || q.State != types.StateRunning {
				t.Fatal("lost internal Proxy fence", q)
			}
			result[i] = proxystats.BatchResult{SandboxID: q.SandboxID, Stats: &api.TrafficStats{State: "running", IdleSince: &stamp,
				Services: map[string]api.ServiceTrafficStats{"forward": {IdleSince: &stamp}, "exec": {IdleSince: &stamp}}}}
		}
		return result, nil
	}))
	rows, err := o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{"traffic"}})
	if err != nil || networkCalls != 1 || ingressCalls != 1 || len(rows) != len(ids) {
		t.Fatal("traffic batch", len(rows), networkCalls, ingressCalls, err)
	}
	for i, row := range rows {
		stats := row.Traffic
		if row.SandboxID != ids[i] || *stats.Platform.RXPackets != uint64(i+1) || *stats.Platform.RXBytes != 9007199254740993 || *stats.Platform.TXPackets != 0 || *stats.Transit.RXBytes != 12 || *stats.Transit.TXPackets != 13 || !stats.IdleSince.Equal(stamp) {
			t.Fatal("flat counters, valid zero, identity or ingress-only idle changed", row)
		}
	}
	raw, err := json.Marshal(rows[0].Traffic)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != 8 || string(object["egress"]) != "{}" || string(object["inflight"]) != `{"parking":0,"connected":0}` || !strings.Contains(string(object["services"]), `"connected":0`) || strings.Contains(string(object["services"]), "egress") {
		t.Fatal("public traffic field contract", string(raw))
	}
	for _, forbidden := range []string{"proxy", "vswitch", "sources", "runID", "run_id"} {
		if _, found := object[forbidden]; found || strings.Contains(string(raw), "internal-fence") {
			t.Fatal("private/source-group field", string(raw))
		}
	}
}

func TestTrafficReadRejectsIncompleteOrChangedBindings(t *testing.T) {
	for _, kind := range []string{"source", "nil", "missing", "duplicate", "switch", "floating-ip", "inner-ip", "invalid-inner-cidr", "reset", "changed-run", "changed-port", "changed-inner-ip", "pending-detach", "control-lock"} {
		t.Run(kind, func(t *testing.T) {
			o := testOrch(t)
			ids := trafficBindings(t, o, 2)
			var calls int
			o.SetSandboxTrafficProvider(nativeTrafficFunc(func(context.Context, string) (*api.TrafficStats, error) {
				return &api.TrafficStats{State: "running"}, nil
			}))
			o.vs = &trafficSwitchStub{vsClient: o.vs, read: func(_ context.Context, ports []int) (*connector.StatsOutput, error) {
				calls++
				result := trafficRows(o, ports)
				switch kind {
				case "source":
					return nil, errors.New("map read failed")
				case "reset":
					return nil, connector.ErrStatsUnavailable
				case "nil":
					return nil, nil
				case "missing":
					result.Ports = result.Ports[:1]
				case "duplicate":
					result.Ports[1] = result.Ports[0]
				case "switch":
					result.Switch = "other"
				case "floating-ip":
					result.Ports[0].FloatingIP = "198.18.0.99"
				case "inner-ip":
					result.Ports[0].InnerIP = "169.254.1.99"
				case "changed-run", "changed-port", "changed-inner-ip":
					sb, err := o.st.Get(context.Background(), ids[0])
					if err != nil {
						t.Fatal(err)
					}
					if kind == "changed-run" {
						sb.RunID = "replacement"
					} else if kind == "changed-port" {
						sb.VswitchPort = "3"
					} else {
						sb.InnerIP = "169.254.1.99/31"
					}
					if err := o.st.Put(context.Background(), sb); err != nil {
						t.Fatal(err)
					}
				}
				return result, nil
			}}
			if kind == "invalid-inner-cidr" {
				sb, err := o.st.Get(context.Background(), ids[0])
				if err != nil {
					t.Fatal(err)
				}
				sb.InnerIP = "invalid"
				if err := o.st.Put(context.Background(), sb); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "pending-detach" {
				o.detachedPortsPending["1"] = struct{}{}
			}
			if kind == "control-lock" {
				o.networkAllocationMu.Lock()
				defer o.networkAllocationMu.Unlock()
			}
			rows, err := o.ReadStats(context.Background(), conductorextension.StatsRequest{SandboxIDs: ids, Sections: []string{"traffic"}})
			if rows != nil || !errors.Is(err, api.ErrStatsUnavailable) {
				t.Fatal("incomplete or previous-owner stats published", rows, err)
			}
			if (kind == "pending-detach" || kind == "control-lock") && calls != 0 {
				t.Fatal("fenced read reached connector")
			}
			if kind != "pending-detach" && kind != "control-lock" && calls != 1 {
				t.Fatal("source failure case did not execute connector read", calls)
			}
		})
	}
}

func TestTrafficNoPortAndSingleSourceFailure(t *testing.T) {
	o := testOrch(t)
	ids := batchSandboxes(t, o, 1)
	sb, err := o.st.Get(context.Background(), ids[0])
	if err != nil {
		t.Fatal(err)
	}
	provider := &trafficStatsProviderStub{stats: &api.TrafficStats{State: "running", Inflight: api.TrafficInflight{Connected: 1}}}
	o.SetSandboxTrafficProvider(provider)
	o.vs = &trafficSwitchStub{vsClient: o.vs, read: func(context.Context, []int) (*connector.StatsOutput, error) {
		t.Fatal("unattached sandbox triggered a connector read")
		return nil, nil
	}}
	key := mintTestAPIKey(t, sb.APISecret)
	result, err := o.TrafficStats(context.Background(), sb.ID, key)
	if err != nil || !reflect.DeepEqual(result.Platform, api.TrafficCounters{}) || !reflect.DeepEqual(result.Transit, api.TrafficCounters{}) || result.Inflight.Connected != 1 || result.IdleSince != nil {
		t.Fatal("unattached traffic invented values", result, err)
	}
	provider.err = errors.New("configured Proxy read failed")
	if _, err := o.TrafficStats(context.Background(), sb.ID, key); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatal("configured failure not unavailable", err)
	}
}
