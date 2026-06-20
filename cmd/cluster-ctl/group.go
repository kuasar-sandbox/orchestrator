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

// groupCmd implements `cluster-ctl group {upsert|get}` — sandbox-group config
// admin (cluster.md §6.2 store provider). It opens the same durable store the
// registry uses (sqlite shares the file), so a running registry reads the writes
// at the next reserve.
func groupCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cluster-ctl group {upsert|get} [flags]")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("group", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config (for store.dsn + encryption_key)")
	storeDSN := fs.String("store", "", "override store.dsn")
	group := fs.String("group", "", "group path (e.g. /cell/proj/app/g1)")
	templateRef := fs.String("template-ref", "", "snapshot template ref for new sandboxes")
	manifestKey := fs.String("manifest-key", "", "tenant manifest key (hex)")
	projectID := fs.String("project-id", "", "project id")
	imageRepo := fs.String("image-repo", "", "tenant image repo")
	_ = fs.Parse(rest)

	cfg, err := clustercfg.Load(*cfgPath)
	if err != nil {
		if *storeDSN == "" {
			return err
		}
		cfg = &clustercfg.Config{} // store-only admin: --store provided, config absent
	}
	if *storeDSN != "" {
		cfg.Store.DSN = *storeDSN
	}
	if cfg.Store.DSN == "" {
		return fmt.Errorf("group: store.dsn required (--store or --config)")
	}

	kv, err := clusterstore.Open(cfg.Store.DSN, 0)
	if err != nil {
		return err
	}
	defer kv.Close()
	var box *secretbox.Box
	if cfg.GroupConfig.EncryptionKey != "" {
		if box, err = secretbox.NewFromColonHex(cfg.GroupConfig.EncryptionKey); err != nil {
			return err
		}
	}
	stores := registry.NewStores(kv, box)
	ctx := context.Background()

	switch sub {
	case "upsert":
		if *group == "" {
			return fmt.Errorf("group upsert: --group required")
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
			return fmt.Errorf("group get: --group required")
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
		return fmt.Errorf("unknown group subcommand %q (upsert|get)", sub)
	}
	return nil
}
