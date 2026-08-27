package proxy_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/proxy"
)

func TestPublicAPICompilesWithoutInternalTypes(t *testing.T) {
	app := proxy.New(proxy.Hooks{
		Configure: func(context.Context, *proxy.Config) error { return nil },
		BindRuntime: func(_ context.Context, process proxy.Process, runtime *proxy.Runtime) error {
			_ = process.Role
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
