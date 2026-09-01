package proxy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

type compileMasterExtension struct{}

func (compileMasterExtension) Start(ctx context.Context, host proxy.MasterHost) error {
	_, _, _ = host.Routes().Get(ctx, "sandbox")
	_, _ = host.Traffic().Get(ctx, "sandbox")
	_ = host.Routes().SyncState()
	return nil
}

func (compileMasterExtension) WrapManagement(next http.Handler) http.Handler { return next }

type compileWorkerExtension struct{}

func (compileWorkerExtension) Start(context.Context, proxy.WorkerHost) error { return nil }
func (compileWorkerExtension) WrapIngress(next http.Handler) http.Handler    { return next }

func TestPublicAPICompilesWithoutInternalTypes(t *testing.T) {
	app := proxy.New(proxy.Hooks{
		Configure: func(context.Context, *proxy.Config) error { return nil },
		BindRuntime: func(_ context.Context, process proxy.Process, runtime *proxy.Runtime) error {
			_ = process.Role
			runtime.MasterExtension = compileMasterExtension{}
			runtime.WorkerExtension = compileWorkerExtension{}
			runtime.TLS = proxy.TLSMaterialProviderFunc(func(context.Context) (proxy.TLSMaterial, error) {
				return proxy.TLSMaterial{}, nil
			})
			return nil
		},
	})
	if app == nil {
		t.Fatal("New returned nil")
	}
	for _, value := range []any{proxy.RoleMaster, proxy.RoleWorker, proxy.Process{}, proxy.TLSMaterial{}, (*proxy.App)(nil)} {
		_ = value
	}
	for _, value := range []any{
		proxy.RouteView{SandboxID: "node-sandbox", StableID: "stable-sandbox"}, proxy.RouteEvent{}, proxy.TrafficView{}, proxy.ProfileBare,
		proxy.RouteStateRunning, proxy.RouteSyncSynced, proxy.RouteSyncLost,
		proxy.ErrTrafficUnavailable, proxy.ErrTrafficConflict,
		proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}, proxy.ForwardRequest{},
	} {
		_ = value
	}
}

func TestRuntimeCannotBeSerializedByValueOrPointer(t *testing.T) {
	for _, value := range []any{proxy.Runtime{}, &proxy.Runtime{}} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatalf("json.Marshal(%T) succeeded", value)
		}
	}
	var runtime proxy.Runtime
	if err := json.Unmarshal([]byte(`{}`), &runtime); err == nil {
		t.Fatal("Runtime JSON decode succeeded")
	}
}
