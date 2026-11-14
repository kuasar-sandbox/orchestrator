package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

func workspaceCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: node-ctl workspace {list|attach|export} <sid> [flags]")
	}
	action := args[0]
	sid, rest := leadingPositional(args[1:])
	fs := flag.NewFlagSet("workspace "+action, flag.ContinueOnError)
	socket := fs.String("socket", "", "control socket (or NODE_CTL_SOCKET)")
	var name, generation, tokenFile, diskFile string
	var asJSON bool
	timeout := 30 * time.Second
	switch action {
	case "list":
		fs.BoolVar(&asJSON, "json", false, "print JSON")
	case "export":
		fs.StringVar(&name, "name", "", "workspace name")
		fs.BoolVar(&asJSON, "json", false, "print JSON")
		fs.DurationVar(&timeout, "timeout", 20*time.Minute, "maximum export request duration")
	case "attach":
		fs.StringVar(&name, "name", "", "workspace name")
		fs.StringVar(&generation, "if-generation", "", "require this run generation")
		fs.StringVar(&diskFile, "disk-spec", "", "DiskV1 JSON file, or - for stdin")
		fs.StringVar(&tokenFile, "token-file", "", "migration token file, or - for stdin")
	default:
		return fmt.Errorf("unknown workspace operation %q", action)
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if sid == "" && fs.NArg() == 1 {
		sid = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return fmt.Errorf("workspace %s accepts one sandbox ID", action)
	}
	if sid == "" || sid == "." || sid == ".." || strings.Contains(sid, "/") {
		return fmt.Errorf("workspace %s requires a sandbox ID", action)
	}
	if action != "list" && name == "" {
		return fmt.Errorf("--name is required")
	}
	if timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	headers := map[string]string{"X-API-KEY": os.Getenv("E2B_API_KEY")}
	if headers["X-API-KEY"] == "" {
		return fmt.Errorf("E2B_API_KEY is required")
	}
	path := "/sandboxes/" + url.PathEscape(sid) + "/workspaces"
	method := http.MethodGet
	var body any
	if action != "list" {
		path += "/" + action + "?" + url.Values{"name": {name}}.Encode()
		method = http.MethodPost
		body = struct{}{}
	}
	if action == "attach" {
		if generation != "" {
			headers[api.RunGenerationHeader] = generation
		}
		disk, err := workspaceDiskInput(diskFile, tokenFile)
		if err != nil {
			return err
		}
		body = map[string]any{"disk": disk}
	}
	code, resp, err := udsDoWithTimeout(resolveSocket(*socket), method, path, headers, body, timeout)
	if err != nil {
		return err
	}
	if action == "attach" && code == http.StatusNoContent {
		return nil
	}
	if code != http.StatusOK || action == "attach" {
		return fmt.Errorf("workspace %s (%d): %s", action, code, apiMessage(resp))
	}
	if action == "export" {
		var out struct {
			Token string `json:"token"`
		}
		if strictjson.Decode(resp, &out) != nil || out.Token == "" {
			return fmt.Errorf("invalid workspace export response")
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(out)
		}
		_, err = fmt.Fprintln(os.Stdout, out.Token)
		return err
	}
	var out api.WorkspaceList
	if err := json.Unmarshal(resp, &out); err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATE\tBYTES\tMOUNTS")
	for _, ws := range out.Workspaces {
		size := "-"
		if ws.SizeBytes != nil {
			size = fmt.Sprint(*ws.SizeBytes)
		}
		var paths []string
		for _, m := range ws.Mounts {
			paths = append(paths, m.Path)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", ws.Name, ws.State, size, strings.Join(paths, ","))
	}
	return tw.Flush()
}

func workspaceDiskInput(diskFile, tokenFile string) (any, error) {
	if diskFile != "" && tokenFile != "" {
		return nil, fmt.Errorf("--disk-spec and --token-file are mutually exclusive")
	}
	path := firstNonEmpty(diskFile, tokenFile)
	if path == "" {
		return struct{}{}, nil
	}
	var input io.Reader = os.Stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		input = f
	}
	const max = 16 << 20
	b, err := io.ReadAll(io.LimitReader(input, max+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, fmt.Errorf("workspace input too large")
	}
	b = bytes.TrimSpace(b)
	if tokenFile != "" {
		if len(b) == 0 || strings.ContainsAny(string(b), " \t\r\n") {
			return nil, fmt.Errorf("expected a single workspace token")
		}
		return map[string]string{"migrationToken": string(b)}, nil
	}
	if len(b) == 0 || b[0] != '{' || !json.Valid(b) {
		return nil, fmt.Errorf("expected a disk JSON object")
	}
	return json.RawMessage(b), nil
}
