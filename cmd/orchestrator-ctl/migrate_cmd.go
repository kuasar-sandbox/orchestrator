package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/orch"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/store"
)

// exportSandboxCmd implements `orchestrator-ctl export-sandbox <sid>`. With
// --to-template it promotes the paused sandbox's snapshot to a remote manifest and
// prints the persist template id (fork/fan-out; usable via `e2b sandbox create`).
// Otherwise it prints a one-line base64 migration token for `import-sandbox` on
// another node (same-sid move). Auth: E2B_API_KEY env (must own the sandbox).
func exportSandboxCmd(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("export-sandbox", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "orchestrator config YAML (or ORCHESTRATOR_CONFIG env)")
	toTemplate := fs.Bool("to-template", false, "promote + print the persist template id (fork) instead of a migration token")
	keepSource := fs.Bool("keep-source", false, "keep the source sandbox (copy) instead of relinquishing it (move)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sid := fs.Arg(0)
	if sid == "" {
		return fmt.Errorf("usage: orchestrator-ctl export-sandbox <sid> [--to-template] [--keep-source] --config <cfg>")
	}
	o, st, err := liteOrch(*cfgPath, log)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := o.ExportSandbox(context.Background(), os.Getenv("E2B_API_KEY"), sid, *toTemplate, *keepSource)
	if err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

// importSandboxCmd implements `orchestrator-ctl import-sandbox <token>`: recreate a
// paused sandbox from a migration token on this node (which must share the remote
// store and have the tenant manifest-key added). Auth: E2B_API_KEY env.
func importSandboxCmd(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("import-sandbox", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "orchestrator config YAML (or ORCHESTRATOR_CONFIG env)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	tok := fs.Arg(0)
	if tok == "" {
		return fmt.Errorf("usage: orchestrator-ctl import-sandbox <token> --config <cfg>")
	}
	o, st, err := liteOrch(*cfgPath, log)
	if err != nil {
		return err
	}
	defer st.Close()
	id, err := o.ImportSandbox(context.Background(), os.Getenv("E2B_API_KEY"), tok)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "imported %s — resume on this node with: e2b sandbox resume %s\n", id, id)
	fmt.Println(id)
	return nil
}

// liteOrch builds an Orchestrator with only the store + config wired (no launcher
// or vswitch — export/import touch neither). It runs alongside a live serve daemon
// (SQLite WAL), like `manifest-key`.
func liteOrch(cfgPath string, log *slog.Logger) (*orch.Orchestrator, *store.Store, error) {
	if cfgPath == "" {
		cfgPath = os.Getenv("ORCHESTRATOR_CONFIG")
	}
	if cfgPath == "" {
		return nil, nil, fmt.Errorf("--config <file> (or ORCHESTRATOR_CONFIG) is required")
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, err
	}
	box, err := secretbox.NewFromColonHex(cfg.EncryptionKeySpec())
	if err != nil {
		return nil, nil, err
	}
	st, err := store.Open(cfg.Paths.DBPath, box)
	if err != nil {
		return nil, nil, err
	}
	return orch.New(cfg, st, nil, nil, log), st, nil
}
