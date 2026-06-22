package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
)

// sandboxGroupCmd implements `cluster-ctl sandbox-group {upsert|get}` — sandbox-group
// config admin (cluster.md §6.2 store provider). It opens the same durable state DB
// the registry uses (sqlite shares the file), so a running registry reads the writes
// at the next reserve.
func sandboxGroupCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cluster-ctl sandbox-group {upsert|get} [flags]")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("sandbox-group", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/registry.yaml", "registry config (for state.dsn + sandbox_group encryption_key)")
	stateDSN := fs.String("state", "", "override state.dsn")
	group := fs.String("group", "", "group path (e.g. /cell/proj/app/g1)")
	templateRef := fs.String("template-ref", "", "snapshot template ref for new sandboxes")
	manifestKey := fs.String("manifest-key", "", "tenant manifest key (hex)")
	projectID := fs.String("project-id", "", "project id")
	imageRepo := fs.String("image-repo", "", "tenant image repo")
	_ = fs.Parse(rest)

	cfg, err := clustercfg.LoadRegistry(*cfgPath)
	if err != nil {
		if *stateDSN == "" {
			return err
		}
		cfg = &clustercfg.RegistryConfig{} // state-only admin: --state provided, config absent
	}
	if *stateDSN != "" {
		cfg.State.DSN = *stateDSN
	}
	if cfg.State.DSN == "" {
		return fmt.Errorf("sandbox-group: state.dsn required (--state or --config)")
	}

	kv, err := clusterstore.Open(cfg.State.DSN, 0)
	if err != nil {
		return err
	}
	defer kv.Close()
	var box *secretbox.Box
	if cfg.SandboxGroup.EncryptionKey != "" {
		if box, err = secretbox.NewFromColonHex(cfg.SandboxGroup.EncryptionKey); err != nil {
			return err
		}
	}
	stores := registry.NewStores(kv, box)
	ctx := context.Background()

	switch sub {
	case "upsert":
		if *group == "" {
			return fmt.Errorf("sandbox-group upsert: --group required")
		}
		g := &registry.GroupConfig{
			Group: *group, ProjectID: *projectID, ManifestKey: *manifestKey,
			TemplateRef: *templateRef, ImageRepo: *imageRepo,
		}
		if err := stores.PutGroup(ctx, g); err != nil {
			return err
		}
		fmt.Printf("upserted group %s\n", *group)
	case "get":
		if *group == "" {
			return fmt.Errorf("sandbox-group get: --group required")
		}
		g, found, err := stores.GetGroupByID(ctx, *group)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("group %s not found", *group)
		}
		mk := ""
		if g.ManifestKey != "" {
			mk = "(set)"
		}
		fmt.Printf("group: %s\nproject_id: %s\ntemplate_ref: %s\nimage_repo: %s\nmanifest_key: %s\n",
			g.Group, g.ProjectID, g.TemplateRef, g.ImageRepo, mk)
	default:
		return fmt.Errorf("unknown sandbox-group subcommand %q (upsert|get)", sub)
	}
	return nil
}
