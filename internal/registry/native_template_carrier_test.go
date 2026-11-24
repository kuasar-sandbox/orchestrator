package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRegistryNativeMaximumRuntimeTemplateCarriers(t *testing.T) {
	base := "file://" + strings.Repeat("a", 64) + ".snapshot@location:"
	base += strings.Repeat("l", types.MaxPortableBaseRefBytes-len(base))
	name := strings.Repeat("r", 255-len(".bundle")) + ".bundle"
	digest := strings.Repeat("b", 64)
	ref := base + "@runtime:" + name + ":digest:" + digest
	template := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: ref}.String()
	if len(ref) != 592 || len(template) != 799 {
		t.Fatal("fixture did not reach carrier limits")
	}
	for backend, factory := range registryNativeBackends {
		t.Run(backend, func(t *testing.T) {
			open := factory(t)
			r, _ := newNativeFaultRegistry(t, open)
			r.parkTimeout = 5 * time.Second
			reportRegistryGroup(t, r, "n1", map[string]string{placement.RuntimeLabelPrefix + name: "digest:" + digest}, nodegroup.Support{
				Group: "/g", Version: 1, Entries: []nodegroup.Entry{{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}},
				DefaultContext: &nodegroup.DefaultContext{TemplateID: template},
			})
			commands := make(chan *routesync.Command, 1)
			owner := &groupCommandOwner{}
			owner.call = func(_ context.Context, _ string, command *routesync.Command) (*routesync.CmdAck, error) {
				var wire bytes.Buffer
				if err := routesync.WriteMsg(&wire, &routesync.Msg{Type: routesync.TypeCommand, Cmd: command}); err != nil {
					return nil, err
				}
				decoded, err := routesync.ReadMsg(&wire)
				if err != nil {
					return nil, err
				}
				commands <- decoded.Cmd
				return &routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckAccepted}, nil
			}
			r.SetNodeOwner(owner)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				req := testCreateReserve("/g", "max-carrier", nil)
				req.TemplateID = &template
				_, err := r.ReserveSandbox(ctx, req)
				done <- err
			}()
			var command *routesync.Command
			select {
			case command = <-commands:
			case err := <-done:
				t.Fatalf("maximum template rejected: %v", err)
			case <-ctx.Done():
				t.Fatal("maximum template did not dispatch")
			}
			if command.TemplateRef != template {
				t.Fatal("NodeLink command truncated template")
			}
			event := testE2BRoute(command.Cluster.StableID, routesync.StateRunning)
			event.SandboxID, event.Profile, event.TemplateID = command.SID, string(types.ProfileBare), template
			event.EnvdAccessToken, event.TrafficAccessToken = "", ""
			var wire bytes.Buffer
			if err := routesync.WriteMsg(&wire, &routesync.Msg{Type: routesync.TypeUpsert, Route: &event}); err != nil {
				t.Fatal(err)
			}
			decoded, err := routesync.ReadMsg(&wire)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.applyRoute(testNodeSession(ctx, r.stores, "n1"), "n1", decoded.Route); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			fresh, _ := newNativeFaultRegistry(t, open)
			record, _, found, err := fresh.stores.GetSandbox(ctx, "/g", "max-carrier")
			if err != nil || !found || record.TemplateID != template || record.CreateTemplateID != template {
				t.Fatalf("native decision/report readback failed: %v", err)
			}
			mux := http.NewServeMux()
			fresh.ServeRouteLink(mux)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, RouteLinkRoutePath+"?group=/g&route_key=max-carrier&sid="+record.SandboxID, nil))
			var route RouteResolve
			if response.Code != 200 {
				t.Fatalf("RouteLink status=%d", response.Code)
			}
			if err := json.Unmarshal(response.Body.Bytes(), &route); err != nil || route.TemplateID != template {
				t.Fatal("RouteLink template changed")
			}
			table, err := proxyshm.Create(t.TempDir()+"/routes.shm", 16)
			if err != nil {
				t.Fatal(err)
			}
			defer table.Close()
			if err := table.Upsert(*decoded.Route); err != nil {
				t.Fatal(err)
			}
			got, ok := table.Lookup(command.SID)
			if !ok || got.TemplateID != template {
				t.Fatal("mmap template changed")
			}
			t.Log("C12: 592-byte reference / 799-byte template survived selection, framed NodeLink command/report, native commit/readback, RouteLink response and actual mmap")
		})
	}
}
