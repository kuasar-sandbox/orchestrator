package conductor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
)

// Exercise the real shared core entry, including the final Configure result.
func TestAppRejectsInvalidEffectiveAdmissionBeforeOpeningStore(t *testing.T) {
	for _, field := range []string{"rate", "burst"} {
		t.Run(field, func(t *testing.T) {
			bootstrap := testBootstrap(t)
			db := filepath.Join(t.TempDir(), "must-not-open.db")
			app := New(Hooks{Configure: func(_ context.Context, cfg *Config, runtime *Runtime) error {
				completeConfig(cfg)
				cfg.Paths.DBPath = db
				cfg.ResourceListen = &publicconfig.ResourceListenConfig{Enabled: true}
				if field == "rate" {
					cfg.ResourceListen.Admission.Rate = -1
				} else {
					cfg.ResourceListen.Admission.Burst = -1
				}
				runtime.EncryptionKeys = EncryptionKeyProviderFunc(func(context.Context) ([][]byte, error) { return [][]byte{make([]byte, 32)}, nil })
				return nil
			}})
			app.receive = func() (*componentexec.Bootstrap, error) { return bootstrap, nil }
			if err := app.RunContext(context.Background()); err == nil || !strings.Contains(err.Error(), "resource_listen.admission."+field) {
				t.Fatalf("effective %s error=%v", field, err)
			}
			if _, err := os.Stat(db); !os.IsNotExist(err) {
				t.Fatalf("store opened before resource validation: %v", err)
			}
		})
	}
}
