package registry

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type routeProjectionFaults struct {
	metadata.RouteRepository
	reportErr, deleteErr error
	reportFailures       int
	commitBeforeError    bool
	beforeReport         func()
	reports, deletes     int
	registrations        []metadata.Registration
	refs                 []metadata.NodeSandboxRef
}

func (f *routeProjectionFaults) ApplyRouteReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeSandboxRef, event routesync.RouteEntry) (metadata.RouteReportResult, error) {
	f.reports++
	f.registrations = append(f.registrations, registration)
	f.refs = append(f.refs, ref)
	if f.beforeReport != nil {
		before := f.beforeReport
		f.beforeReport = nil
		before()
	}
	fail := f.reportErr != nil && (f.reportFailures < 0 || f.reports <= f.reportFailures)
	if fail && !f.commitBeforeError {
		return metadata.RouteReportResult{}, f.reportErr
	}
	result, err := f.RouteRepository.ApplyRouteReport(ctx, registration, ref, event)
	if err != nil || !fail {
		return result, err
	}
	return metadata.RouteReportResult{}, f.reportErr
}

func (f *routeProjectionFaults) ApplyRouteDelete(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeSandboxRef, restore *metadata.SandboxRecord) error {
	f.deletes++
	if f.deleteErr != nil && !f.commitBeforeError {
		return f.deleteErr
	}
	if err := f.RouteRepository.ApplyRouteDelete(ctx, registration, expected, ref, restore); err != nil {
		return err
	}
	return f.deleteErr
}

func TestRouteReportConflictReevaluatesSameReport(t *testing.T) {
	for _, state := range []string{routesync.StateRunning, routesync.StatePaused} {
		t.Run(state, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx := t.Context()
			route := testSandboxIdentityRecord("/g", "report-conflict", "sb-report", "n1", StateReserved)
			if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
				t.Fatalf("seed: %v %v", ok, err)
			}
			call := &reserveCall{done: make(chan struct{}), expectedSandboxID: route.SandboxID}
			reg.inflight[flightKey(route.Group, route.RouteKey)] = call
			faults := &routeProjectionFaults{RouteRepository: reg.stores.routes, reportErr: metadata.ErrConflict, reportFailures: 1}
			faults.beforeReport = func() {
				// A concurrent settings update invalidates the report's first
				// pinned version without changing its stream or sandbox identity.
				next := *route
				next.AutoReset = true
				if _, err := reg.stores.PutSandbox(ctx, &next); err != nil {
					t.Fatal(err)
				}
			}
			reg.stores.routes = faults
			event := testE2BRoute(route.SandboxID, state)
			if err := reg.applyRoute(testNodeSession(ctx, reg.stores, "n1"), "n1", &event); err != nil {
				t.Fatal(err)
			}
			if faults.reports != 2 || faults.registrations[0] != faults.registrations[1] || faults.refs[0] != faults.refs[1] {
				t.Fatalf("retry changed authority: calls=%d registrations=%+v refs=%+v", faults.reports, faults.registrations, faults.refs)
			}
			actual, _, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey)
			want := StateReady
			if state == routesync.StatePaused {
				want = StatePaused
			}
			if err != nil || !found || actual.State != want || !actual.AutoReset {
				t.Fatalf("report lost concurrent settings: %+v %v %v", actual, found, err)
			}
			if want == StateReady {
				select {
				case <-call.done:
					if call.err != nil || call.result == nil || call.result.Route.SandboxID != route.SandboxID {
						t.Fatalf("READY waiter: %+v %v", call.result, call.err)
					}
				default:
					t.Fatal("committed READY did not finish waiter")
				}
			}
		})
	}
}

// serveProjectionStream runs the real node-link reader, including its final
// bookmark, so a swallowed mutation error would visibly skip the failed frame.
func serveProjectionStream(t *testing.T, reg *Registry, route *SandboxRecord, kind string) (*routesync.Hello, error) {
	t.Helper()
	var input, output bytes.Buffer
	frames := []*routesync.Msg{{Type: routesync.TypeBuildSyncBegin}, {Type: routesync.TypeBuildSyncEnd}}
	switch kind {
	case "upsert":
		event := testE2BRoute(route.SandboxID, routesync.StateRunning)
		frames = append(frames, &routesync.Msg{Type: routesync.TypeUpsert, Route: &event})
	case "delete":
		frames = append(frames, &routesync.Msg{Type: routesync.TypeDelete, SID: route.NodeSandboxID})
	case "full_snapshot":
	default:
		t.Fatalf("unknown projection kind %q", kind)
	}
	frames = append(frames, &routesync.Msg{Type: routesync.TypeBookmark, RevToken: "node-fp:2", FullSync: true})
	for _, frame := range frames {
		if err := routesync.WriteMsg(&input, frame); err != nil {
			t.Fatal(err)
		}
	}
	err := reg.serveNodeLinkLocal(t.Context(), &output, func() {}, &input, &routesync.NodeRegister{NodeID: route.NodeID})
	hello, readErr := routesync.ReadMsg(&output)
	if readErr != nil || hello == nil || hello.Hello == nil {
		t.Fatalf("missing hello: %+v %v", hello, readErr)
	}
	return hello.Hello, err
}

func seedProjectionStream(t *testing.T) (*Registry, *SandboxRecord) {
	t.Helper()
	reg, _ := registryWithTypedFixture(t)
	reg.reaperCtx = t.Context()
	route := testSandboxIdentityRecord("/g", "projection", "sb-projection", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(t.Context(), route, metadata.Version{}); err != nil || !ok {
		t.Fatalf("seed: %v %v", ok, err)
	}
	reg.updateNodeResume(testNodeSession(t.Context(), reg.stores, "n1"), "n1", "node-fp:1")
	return reg, route
}

func assertProjectionBookmark(t *testing.T, reg *Registry, want string) {
	t.Helper()
	node, found, err := reg.stores.GetNode(t.Context(), "n1")
	if err != nil || !found || node.ResumeToken != want {
		t.Fatalf("bookmark: %+v %v %v, want %q", node, found, err, want)
	}
}

func TestNodeLinkProjectionFailurePreservesBookmarkForReconnect(t *testing.T) {
	for _, kind := range []string{"upsert", "delete", "full_snapshot"} {
		for _, fault := range []struct {
			name      string
			err       error
			committed bool
		}{
			{"conflict", metadata.ErrConflict, false},
			{"unavailable", &metadata.Unavailable{Cause: errors.New("disconnected")}, false},
			{"unknown_uncommitted", &metadata.Unknown{Cause: errors.New("reply lost")}, false},
			{"unknown_committed", &metadata.Unknown{Cause: errors.New("reply lost")}, true},
		} {
			t.Run(kind+"/"+fault.name, func(t *testing.T) {
				reg, route := seedProjectionStream(t)
				faults := &routeProjectionFaults{RouteRepository: reg.stores.routes, reportErr: fault.err, deleteErr: fault.err, reportFailures: -1, commitBeforeError: fault.committed}
				reg.stores.routes = faults
				_, err := serveProjectionStream(t, reg, route, kind)
				if !errors.Is(err, fault.err) {
					t.Fatalf("projection error was swallowed: %v", err)
				}
				assertProjectionBookmark(t, reg, "node-fp:1")
				calls := faults.reports + faults.deletes
				wantCalls := 1
				if kind == "upsert" && errors.Is(fault.err, metadata.ErrConflict) {
					wantCalls = 5
				}
				if calls != wantCalls {
					t.Fatalf("wrong retry policy: calls=%d want=%d", calls, wantCalls)
				}
				faults.reportErr, faults.deleteErr = nil, nil
				hello, err := serveProjectionStream(t, reg, route, kind)
				if err != nil || hello.ResumeFrom != "node-fp:1" {
					t.Fatalf("reconnect did not replay from safe bookmark: %+v %v", hello, err)
				}
				assertProjectionBookmark(t, reg, "node-fp:2")
				actual, _, found, err := reg.stores.GetSandbox(t.Context(), route.Group, route.RouteKey)
				if err != nil || (kind == "upsert" && (!found || actual.State != StateReady)) || (kind != "upsert" && found) {
					t.Fatalf("replayed projection did not converge: %+v %v %v", actual, found, err)
				}
			})
		}
	}
}

type unavailableProjectionOwner struct {
	metadata.OwnershipRepository
}

func (f unavailableProjectionOwner) ReadSandboxOwner(context.Context, string, string) (metadata.Versioned[metadata.NodeSandboxRef], error) {
	return metadata.Versioned[metadata.NodeSandboxRef]{}, &metadata.Unavailable{Cause: errors.New("ownership read failed")}
}

func TestNodeLinkOwnershipReadFailurePreservesBookmark(t *testing.T) {
	for _, kind := range []string{"upsert", "delete", "full_snapshot"} {
		t.Run(kind, func(t *testing.T) {
			reg, route := seedProjectionStream(t)
			reg.stores.ownership = unavailableProjectionOwner{reg.stores.ownership}
			_, err := serveProjectionStream(t, reg, route, kind)
			if !errors.Is(err, metadata.ErrUnavailable) {
				t.Fatalf("ownership error was swallowed: %v", err)
			}
			assertProjectionBookmark(t, reg, "node-fp:1")
		})
	}
}

func TestNodeLinkUnownedProjectionMayAdvanceBookmark(t *testing.T) {
	for _, kind := range []string{"upsert", "delete", "full_snapshot"} {
		t.Run(kind, func(t *testing.T) {
			reg, route := seedProjectionStream(t)
			// A known absent owner makes the frame irrelevant, rather than an
			// unapplied storage failure that must prevent the next bookmark.
			if err := reg.stores.RemoveNodeSandboxRef(t.Context(), "n1", route.NodeSandboxID); err != nil {
				t.Fatal(err)
			}
			if _, err := serveProjectionStream(t, reg, route, kind); err != nil {
				t.Fatal(err)
			}
			assertProjectionBookmark(t, reg, "node-fp:2")
		})
	}
}
