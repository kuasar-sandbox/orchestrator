package proxyext

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	internalproxy "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
)

type workerHostRouter struct {
	lookups int
	target  internalproxy.ConnectTarget
}

func (r *workerHostRouter) LookupRoute(_ context.Context, _ string, target internalproxy.ConnectTarget) (internalproxy.RouteBinding, bool, error) {
	r.lookups++
	r.target = target
	return internalproxy.RouteBinding{}, false, nil
}

func (*workerHostRouter) ActivateRoute(context.Context, internalproxy.RouteBinding) (internalproxy.Route, bool, error) {
	return internalproxy.Route{}, false, nil
}

func TestWorkerHostProcessGetRouteAndPublicProjection(t *testing.T) {
	table, err := proxyshm.Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	route := testRoute("s1", "run-1")
	route.EnvdUDS = "/run/s1/envd.sock"
	route.CiUDS = "/run/s1/ci.sock"
	if err := table.Upsert(route); err != nil {
		t.Fatal(err)
	}
	process := proxyextension.Process{Role: proxyextension.RoleWorker, WorkerID: "proxy-2", WorkerEpoch: 7}
	host := NewWorkerHost(process, table, nil)
	if got := host.Process(); got != process {
		t.Fatalf("Process=%+v want=%+v", got, process)
	}
	view, found := host.GetRoute("s1")
	if !found || view.SandboxID != "s1" || view.StableID != "stable-s1" || view.RunID != "run-1" ||
		view.EnvdUDS != route.EnvdUDS || view.CIUDS != route.CiUDS || view.Revision == 0 ||
		view.APISecretFingerprint != route.APISecretFingerprint || view.ManifestKeyFingerprint != route.ManifestKeyFingerprint {
		t.Fatalf("GetRoute=%+v found=%v", view, found)
	}
	view.RunID = "mutated"
	again, found := host.GetRoute("s1")
	if !found || again.RunID != "run-1" {
		t.Fatalf("GetRoute shared mutable state: %+v found=%v", again, found)
	}
	if _, found := host.GetRoute("missing"); found {
		t.Fatal("missing route was found")
	}
}

func TestWorkerHostForwardAuthorizedStrictTargetConversion(t *testing.T) {
	router := &workerHostRouter{}
	core := internalproxy.New(router, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	host := NewWorkerHost(proxyextension.Process{Role: proxyextension.RoleWorker, WorkerID: "proxy-0", WorkerEpoch: 1}, nil, core)
	request := httptest.NewRequest(http.MethodGet, "http://private/", nil)

	invalid := httptest.NewRecorder()
	host.ForwardAuthorized(invalid, request, proxyextension.ForwardRequest{
		SandboxID: "s1", Target: proxyextension.ConnectTarget{Service: "unknown", Port: 8080},
	})
	if invalid.Code != http.StatusBadRequest || invalid.Header().Get(internalproxy.HeaderProxyError) != internalproxy.ProxyErrorBadRequest || router.lookups != 0 {
		t.Fatalf("invalid response=%d headers=%v lookups=%d", invalid.Code, invalid.Header(), router.lookups)
	}
	invalidPort := httptest.NewRecorder()
	host.ForwardAuthorized(invalidPort, request, proxyextension.ForwardRequest{
		SandboxID: "s1", Target: proxyextension.ConnectTarget{Service: proxyextension.ConnectServiceForward, Port: 0},
	})
	if invalidPort.Code != http.StatusBadRequest || router.lookups != 0 {
		t.Fatalf("invalid port response=%d lookups=%d", invalidPort.Code, router.lookups)
	}

	exec := httptest.NewRecorder()
	host.ForwardAuthorized(exec, request, proxyextension.ForwardRequest{
		SandboxID: "s1", Target: proxyextension.ConnectTarget{Service: proxyextension.ConnectServiceExec},
	})
	if exec.Code != http.StatusNotImplemented || exec.Header().Get(internalproxy.HeaderProxyError) != internalproxy.ProxyErrorDenied || router.lookups != 0 {
		t.Fatalf("exec response=%d headers=%v lookups=%d", exec.Code, exec.Header(), router.lookups)
	}

	notFound := httptest.NewRecorder()
	host.ForwardAuthorized(notFound, request, proxyextension.ForwardRequest{
		SandboxID: "s1", Target: proxyextension.ConnectTarget{Service: proxyextension.ConnectServiceForward, Port: 8080},
	})
	if notFound.Code != http.StatusNotFound || router.lookups != 1 ||
		router.target != (internalproxy.ConnectTarget{Service: internalproxy.ConnectServiceForward, Port: 8080}) {
		t.Fatalf("valid response=%d lookups=%d target=%+v", notFound.Code, router.lookups, router.target)
	}
}
