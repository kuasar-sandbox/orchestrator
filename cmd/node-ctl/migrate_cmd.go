package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

// exportSandboxCmd implements `node-ctl export-sandbox <sid>` as a client of
// the running serve daemon's api plane (POST /sandboxes/{id}/export over the local
// control socket). With --to-template it promotes the paused sandbox's snapshot to a
// remote manifest and prints the persist template id (fork; usable via `e2b sandbox
// create`). Otherwise it prints a one-line base64 migration token for `import-sandbox`
// on another node (the import allocates a fresh sandbox id). Auth: E2B_API_KEY env
// (must own the sandbox).
func exportSandboxCmd(args []string, _ *slog.Logger) error {
	sid, rest := leadingPositional(args)
	fs := flag.NewFlagSet("export-sandbox", flag.ContinueOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	toTemplate := fs.Bool("to-template", false, "promote + print the persist template id (fork) instead of a migration token")
	keepSource := fs.Bool("keep-source", false, "keep the source sandbox (copy) instead of relinquishing it (move)")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if sid == "" {
		sid = fs.Arg(0)
	}
	if sid == "" {
		return fmt.Errorf("usage: node-ctl export-sandbox <sid> [--to-template] [--keep-source] [--socket S]")
	}
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("export-sandbox: E2B_API_KEY is required")
	}
	body := map[string]any{"toTemplate": *toTemplate, "keepSource": *keepSource}
	code, resp, err := udsDo(resolveSocket(*socket), http.MethodPost,
		"/sandboxes/"+sid+"/export", map[string]string{"X-API-KEY": apiKey}, body)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("export-sandbox: %s", apiMessage(resp))
	}
	var out struct {
		Result string `json:"result"`
	}
	_ = json.Unmarshal(resp, &out)
	fmt.Println(out.Result)
	return nil
}

// importSandboxCmd implements `node-ctl import-sandbox <token>` as a client of
// the daemon's api plane (POST /sandboxes/import): recreate a paused sandbox from a
// migration token on this node (which must share the remote store and have the tenant
// matching node key lease installed). Auth: E2B_API_KEY env.
func importSandboxCmd(args []string, _ *slog.Logger) error {
	tok, rest := leadingPositional(args)
	fs := flag.NewFlagSet("import-sandbox", flag.ContinueOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if tok == "" {
		tok = fs.Arg(0)
	}
	if tok == "" {
		return fmt.Errorf("usage: node-ctl import-sandbox <token> [--socket S]")
	}
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("import-sandbox: E2B_API_KEY is required")
	}
	code, resp, err := udsDo(resolveSocket(*socket), http.MethodPost,
		"/sandboxes/import", map[string]string{"X-API-KEY": apiKey}, map[string]any{"token": tok})
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("import-sandbox: %s", apiMessage(resp))
	}
	var out struct {
		SandboxID string `json:"sandboxID"`
	}
	_ = json.Unmarshal(resp, &out)
	fmt.Fprintf(os.Stderr, "imported %s — resume on this node with: e2b sandbox resume %s\n", out.SandboxID, out.SandboxID)
	fmt.Println(out.SandboxID)
	return nil
}
