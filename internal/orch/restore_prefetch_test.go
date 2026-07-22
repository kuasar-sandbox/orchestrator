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
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)
	persistID := "bare-img-" + strings.Repeat("b", 64)
	if err := o.st.PutBuild(context.Background(), &types.Build{
		BuildID:     "invalid-restore-template",
		TemplateID:  "transient-invalid-restore-template",
		PersistID:   persistID,
		ManifestKey: manifestKey,
		Profile:     types.ProfileBare,
		Kind:        types.KindImg,
		Status:      types.BuildReady,
		Metadata: map[string]string{
			sandboxcfg.NsRestore: `{"file_refs":"trust"}`,
		},
		CreatedUnix: 1,
	}); err != nil {
		t.Fatal(err)
	}

	requests := map[string]api.CreateReq{
		"create metadata": {
			APIKey:     apiKey,
			TemplateID: "bare-img-" + strings.Repeat("a", 64),
			TimeoutSec: 60,
			Metadata: map[string]string{
				sandboxcfg.NsRestore: `{"file_refs":"trust"}`,
			},
		},
		"template default": {
			APIKey:     apiKey,
			TemplateID: persistID,
			TimeoutSec: 60,
		},
	}
	for name, req := range requests {
		t.Run(name, func(t *testing.T) {
			_, err := o.Create(context.Background(), req)
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("Create error = %v, want ErrBadRequest", err)
			}
		})
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
