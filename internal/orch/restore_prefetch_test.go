package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCreateRejectsInvalidRestoreBeforeLaunchSideEffects(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	req := api.CreateReq{
		APIKey:     apiKey,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		TimeoutSec: 60,
		Metadata: map[string]string{
			sandboxcfg.NsRestore: `{"prefetch":"eager"}`,
		},
	}
	if _, err := o.Create(context.Background(), req); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("invalid restore created %s: stat error = %v", path, statErr)
		}
	}
	items, _, err := o.List(context.Background(), apiKey, "", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("invalid restore persisted sandboxes: %+v", items)
	}
}
