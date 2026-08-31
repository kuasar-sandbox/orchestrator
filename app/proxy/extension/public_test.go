package extension_test

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
)

type compileExtension struct{}

func (compileExtension) Start(context.Context, extension.MasterHost) error { return nil }
func (compileExtension) WrapManagement(next http.Handler) http.Handler     { return next }

type compileWorkerExtension struct{}

func (compileWorkerExtension) Start(_ context.Context, host extension.WorkerHost) error {
	_ = host.Process()
	_, _ = host.GetRoute("sandbox")
	host.ForwardAuthorized(nil, nil, extension.ForwardRequest{
		SandboxID: "sandbox",
		Target: extension.ConnectTarget{
			Service: extension.ConnectServiceForward,
			Port:    8080,
		},
	})
	return nil
}

func (compileWorkerExtension) WrapIngress(next http.Handler) http.Handler { return next }

func TestPublicContractsCompileOutsidePackage(t *testing.T) {
	var master extension.MasterExtension = compileExtension{}
	var wrapper extension.ManagementWrapper = compileExtension{}
	var worker extension.WorkerExtension = compileWorkerExtension{}
	var ingress extension.IngressWrapper = compileWorkerExtension{}
	if master == nil || wrapper == nil || worker == nil || ingress == nil {
		t.Fatal("public extension contracts are nil")
	}
	for _, value := range []any{
		extension.RouteView{}, extension.RouteEvent{}, extension.TrafficView{},
		extension.ProfileBare, extension.RouteStateRunning,
		extension.RouteSyncInitializing, extension.RouteSyncLost,
		extension.ErrTrafficUnavailable, extension.ErrTrafficConflict,
	} {
		_ = value
	}
	for _, value := range []any{
		extension.RoleMaster, extension.RoleWorker, extension.Process{},
		extension.ConnectServiceForward, extension.ConnectTarget{}, extension.ForwardRequest{},
	} {
		_ = value
	}
	workerHost := reflect.TypeOf((*extension.WorkerHost)(nil)).Elem()
	if _, found := workerHost.MethodByName("Watch"); found {
		t.Fatal("WorkerHost unexpectedly exposes Watch")
	}
	if _, found := workerHost.MethodByName("Routes"); found {
		t.Fatal("WorkerHost unexpectedly exposes a route source")
	}
}
