// Test-only, loopback NodeLink relay. It changes delivery/ACK order while the
// actual Node process, SQLite database and VM implementation remain unchanged.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type event struct {
	At      string `json:"at"`
	Action  string `json:"action"`
	Type    string `json:"type"`
	Command string `json:"command,omitempty"`
	SID     string `json:"sid,omitempty"`
	State   string `json:"state,omitempty"`
}

type relay struct {
	target, modeFile string
	client           *http.Client
	log              *json.Encoder
	mu               sync.Mutex
}

func (r *relay) record(action string, msg *routesync.Msg) {
	e := event{At: time.Now().UTC().Format(time.RFC3339Nano), Action: action, Type: msg.Type}
	if msg.Cmd != nil {
		e.Command, e.SID = msg.Cmd.CmdID, msg.Cmd.SID
	}
	if msg.Ack != nil {
		e.Command = msg.Ack.CmdID
	}
	if msg.Route != nil {
		e.SID, e.State = msg.Route.SandboxID, msg.Route.State
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Deliberately exclude all commands, contexts, credentials and route tokens.
	if err := r.log.Encode(e); err != nil {
		panic(err)
	}
}

func (r *relay) mode() (string, error) {
	raw, err := os.ReadFile(r.modeFile)
	if err != nil {
		return "", err
	}
	mode := strings.TrimSpace(string(raw))
	switch mode {
	case "normal", "drop-create-ack", "drop-create-results", "error-after-ready", "pause-create", "disconnect":
		return mode, nil
	default:
		return "", fmt.Errorf("invalid fixture mode")
	}
}

func (r *relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if req.Method != http.MethodPut || req.URL.Path != routesync.NodeLinkPath {
		http.NotFound(w, req)
		return
	}
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	var mu sync.Mutex
	creates := map[string]string{}
	createdSID := map[string]bool{}
	held := map[string]*routesync.Msg{}
	running := map[string]bool{}
	go func() {
		var terminal error
		defer func() { writer.CloseWithError(terminal); cancel() }()
		for {
			msg, err := routesync.ReadMsg(req.Body)
			if err != nil {
				terminal = err
				return
			}
			mode, err := r.mode()
			if err != nil || mode == "disconnect" {
				terminal = errors.New("fixture disconnected session")
				return
			}
			mu.Lock()
			sid := ""
			if msg.Ack != nil {
				sid = creates[msg.Ack.CmdID]
			}
			mu.Unlock()
			if sid != "" && (mode == "drop-create-ack" || mode == "drop-create-results") {
				r.record("drop", msg)
				continue
			}
			if sid != "" && mode == "error-after-ready" && msg.Ack.Status == routesync.AckAccepted {
				msg.Ack.Status, msg.Ack.HTTPStatus = routesync.AckRejected, http.StatusConflict
				msg.Ack.ErrorCode, msg.Ack.Reason = "", "fixture late generic rejection"
				if !running[sid] {
					held[sid] = msg
					r.record("hold-error", msg)
					continue
				}
			}
			if msg.Route != nil {
				mu.Lock()
				target := createdSID[msg.Route.SandboxID]
				mu.Unlock()
				if target && mode == "drop-create-results" {
					r.record("drop", msg)
					continue
				}
			}
			if err = routesync.WriteMsg(writer, msg); err != nil {
				terminal = err
				return
			}
			r.record("forward-up", msg)
			mu.Lock()
			tracked := msg.Route != nil && createdSID[msg.Route.SandboxID]
			mu.Unlock()
			if tracked && msg.Route.State == "running" {
				sid := msg.Route.SandboxID
				running[sid] = true
				if delayed := held[sid]; delayed != nil {
					if err = routesync.WriteMsg(writer, delayed); err != nil {
						terminal = err
						return
					}
					r.record("forward-late-error", delayed)
					delete(held, sid)
				}
			}
		}
	}()
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPut, r.target, reader)
	if err != nil {
		http.Error(w, "invalid fixture target", 500)
		return
	}
	upstream.Header = req.Header.Clone()
	response, err := r.client.Do(upstream)
	if err != nil {
		http.Error(w, "fixture upstream failed", 502)
		return
	}
	defer response.Body.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(response.StatusCode)
	flush := w.(http.Flusher)
	flush.Flush()
	for {
		msg, err := routesync.ReadMsg(response.Body)
		if err != nil {
			return
		}
		if msg.Cmd != nil && msg.Cmd.Kind == routesync.CmdCreate {
			mu.Lock()
			if len(creates) >= 256 {
				mu.Unlock()
				return
			}
			creates[msg.Cmd.CmdID] = msg.Cmd.SID
			createdSID[msg.Cmd.SID] = true
			mu.Unlock()
			r.record("create-received", msg)
			for {
				mode, err := r.mode()
				if err != nil || mode == "disconnect" {
					return
				}
				if mode != "pause-create" {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		if err = routesync.WriteMsg(w, msg); err != nil {
			return
		}
		flush.Flush()
		r.record("forward-down", msg)
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "loopback listener")
	target := flag.String("target", "", "Registry NodeLink URL")
	mode := flag.String("mode-file", "", "atomically replaced fault mode file")
	logPath := flag.String("events", "", "credential-free JSONL receipt")
	flag.Parse()
	file, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	r := &relay{target: *target, modeFile: *mode, log: json.NewEncoder(file)}
	if _, err = r.mode(); err != nil {
		panic(err)
	}
	r.client = &http.Client{Transport: &http2.Transport{AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}}}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	server := &http.Server{Handler: h2c.NewHandler(r, &http2.Server{})}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); server.Close() }()
	fmt.Println(listener.Addr())
	if err = server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
