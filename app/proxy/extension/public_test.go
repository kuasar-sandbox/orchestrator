package extension_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
)

type compileExtension struct{}

func (compileExtension) Start(context.Context, extension.MasterHost) error { return nil }
func (compileExtension) WrapManagement(next http.Handler) http.Handler     { return next }

func TestPublicContractsCompileOutsidePackage(t *testing.T) {
	var master extension.MasterExtension = compileExtension{}
	var wrapper extension.ManagementWrapper = compileExtension{}
	if master == nil || wrapper == nil {
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
}
