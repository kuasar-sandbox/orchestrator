package main

// Shared client plumbing for the CLIs that talk to a running `node-ctl
// serve` daemon over its local control socket (key-lease, export-sandbox,
// import-sandbox). These commands never read the orchestrator config file — the
// daemon owns the store + keys; they only need the socket path.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

const defaultSocket = "/run/sandbox/node-ctl.socket"

// resolveSocket picks the control-socket path: --socket flag, else NODE_CTL_SOCKET
// env, else the default (matches config_socket's default).
func resolveSocket(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if v := os.Getenv("NODE_CTL_SOCKET"); v != "" {
		return v
	}
	return defaultSocket
}

// udsDo performs one HTTP request to the control socket and returns the status code
// and raw response body. hdr and reqBody are optional (reqBody is JSON-encoded).
func udsDo(socket, method, path string, hdr map[string]string, reqBody any) (int, []byte, error) {
	var rdr io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://localhost"+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := configsock.HTTPClient(socket).Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("reach orchestrator at %s: %w (is `node-ctl conductor serve` running?)", socket, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

// apiMessage extracts the e2b api error {"message": "..."} from body, falling back
// to the trimmed raw body.
func apiMessage(body []byte) string {
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &e) == nil && e.Message != "" {
		return e.Message
	}
	return strings.TrimSpace(string(body))
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// leadingPositional pulls a leading non-flag arg (a sandbox id or token) out of args
// so the remaining flags still parse — Go's flag package stops at the first positional,
// so `export-sandbox <sid> --socket …` would otherwise drop the flags. Returns the
// value (or "") and the remaining args to hand to flag.Parse; a trailing positional is
// still recoverable via fs.Arg(0).
func leadingPositional(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
}
