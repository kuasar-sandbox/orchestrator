package conductor_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/app/conductor"
)

func TestPublicConductorAPICompilesWithoutInternalTypes(t *testing.T) {
	app := conductor.New(conductor.Hooks{Configure: func(_ context.Context, cfg *conductor.Config, runtime *conductor.Runtime) error {
		cfg.API.Domain = "custom.test"
		runtime.EncryptionKeys = conductor.EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) {
			return [][]byte{make([]byte, 32)}, nil
		})
		return nil
	}})
	if app == nil {
		t.Fatal("New returned nil")
	}
	if _, err := json.Marshal(&conductor.Runtime{}); err == nil {
		t.Fatal("process-local Runtime was serializable")
	}

	seen := map[reflect.Type]bool{}
	for _, value := range []any{
		conductor.Config{}, conductor.Hooks{}, conductor.Runtime{}, conductor.TLSMaterial{},
		conductor.ObjectStoreCredentials{}, (*conductor.App)(nil),
		(*conductor.TLSMaterialProvider)(nil), (*conductor.EncryptionKeyProvider)(nil),
		(*conductor.ObjectStoreCredentialsProvider)(nil),
	} {
		assertNoInternalType(t, reflect.TypeOf(value), seen)
	}
}

func assertNoInternalType(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	if typ == nil {
		return
	}
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Map {
		assertNoInternalType(t, typ.Key(), seen)
		assertNoInternalType(t, typ.Elem(), seen)
		return
	}
	if seen[typ] {
		return
	}
	seen[typ] = true
	if strings.Contains(typ.PkgPath(), "/internal/") {
		t.Fatalf("public conductor API exposes internal type %s", typ)
	}
	for methodIndex := 0; methodIndex < typ.NumMethod(); methodIndex++ {
		method := typ.Method(methodIndex)
		for index := 0; index < method.Type.NumIn(); index++ {
			assertNoInternalType(t, method.Type.In(index), seen)
		}
		for index := 0; index < method.Type.NumOut(); index++ {
			assertNoInternalType(t, method.Type.Out(index), seen)
		}
	}
	switch typ.Kind() {
	case reflect.Struct:
		for index := 0; index < typ.NumField(); index++ {
			if typ.Field(index).IsExported() {
				assertNoInternalType(t, typ.Field(index).Type, seen)
			}
		}
	case reflect.Interface:
		for index := 0; index < typ.NumMethod(); index++ {
			method := typ.Method(index)
			for input := 0; input < method.Type.NumIn(); input++ {
				assertNoInternalType(t, method.Type.In(input), seen)
			}
			for output := 0; output < method.Type.NumOut(); output++ {
				assertNoInternalType(t, method.Type.Out(output), seen)
			}
		}
	case reflect.Func:
		for index := 0; index < typ.NumIn(); index++ {
			assertNoInternalType(t, typ.In(index), seen)
		}
		for index := 0; index < typ.NumOut(); index++ {
			assertNoInternalType(t, typ.Out(index), seen)
		}
	}
}
