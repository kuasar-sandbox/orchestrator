// Command node-stub-ctl runs controllable node-link stubs for cluster e2e tests.
// It starts real node_link clients and exposes local admin/data HTTP endpoints,
// but does not launch microVMs.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	sandboxctl "github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

var version = "0.1.0-dev"

const (
	defaultCreateDelay = 10 * time.Millisecond
	defaultBuildDelay  = 10 * time.Millisecond
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:], log)
	case "nodes":
		err = runGet(os.Args[2:], "/v1/nodes")
	case "events":
		err = runGet(os.Args[2:], "/v1/events")
	case "data-hits":
		err = runGet(os.Args[2:], "/v1/data-hits")
	case "commands":
		err = runGet(os.Args[2:], "/v1/commands")
	case "node":
		err = runNode(os.Args[2:])
	case "sandbox":
		err = runSandbox(os.Args[2:])
	case "build":
		err = runBuild(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println("node-stub-ctl", version)
	default:
		usage()
	}
	if err != nil {
		log.Error("node-stub-ctl", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  node-stub-ctl serve --node-link HOST:PORT [--nodes N] [--node-prefix stub] [--label k=v]
  node-stub-ctl nodes|events|data-hits|commands --admin http://HOST:PORT
  node-stub-ctl node {restart-link|reboot-empty|crash|start|drain|undrain} NODE --admin http://HOST:PORT
  node-stub-ctl sandbox list --admin http://HOST:PORT [--node NODE]
  node-stub-ctl sandbox create --admin http://HOST:PORT --node NODE [--sid SID] [--metadata k=v ...]
  node-stub-ctl sandbox delete --admin http://HOST:PORT --node NODE --sid SID
  node-stub-ctl sandbox orphan --admin http://HOST:PORT --node NODE --sid SID [--metadata k=v ...]
  node-stub-ctl build list --admin http://HOST:PORT [--node NODE]
  node-stub-ctl version`)
	os.Exit(2)
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func runServe(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	nodeLink := fs.String("node-link", "", "registry node_link endpoint")
	nodes := fs.Int("nodes", 1, "number of simulated nodes")
	prefix := fs.String("node-prefix", "stub", "simulated node id prefix")
	adminListen := fs.String("admin-listen", "127.0.0.1:0", "admin API listen address")
	dataListen := fs.String("data-listen", "127.0.0.1:0", "data-plane stub listen address")
	capacity := fs.Int("capacity", 100, "sandbox capacity per node")
	registrationBuildCPU := fs.Int("registration-build-cpu", 4000, "registration Build CPU capacity in milli-cores")
	registrationBuildMem := fs.Int64("registration-build-mem-bytes", 4<<30, "registration Build memory capacity in bytes")
	registrationBuildStorage := fs.Int64("registration-build-storage-bytes", 0, "registration Build storage capacity in bytes")
	executionBuildCPU := fs.Int("execution-build-cpu", 4000, "execution Build CPU capacity in milli-cores")
	executionBuildMem := fs.Int64("execution-build-mem-bytes", 4<<30, "execution Build memory capacity in bytes")
	executionBuildStorage := fs.Int64("execution-build-storage-bytes", 0, "execution Build storage capacity in bytes")
	heartbeat := fs.Duration("heartbeat", time.Second, "node heartbeat interval")
	runtimeDigest := fs.String("runtime-digest", "runtime-stub", "runtime digest reported by each node")
	strictKeys := fs.Bool("strict-keys", true, "reject builds when the referenced key is not installed; creates always require inline API secret material")
	createDelay := fs.Duration("create-delay", defaultCreateDelay, "default create-to-running delay")
	buildDelay := fs.Duration("build-delay", defaultBuildDelay, "default build event delay")
	var labels multiFlag
	fs.Var(&labels, "label", "label k=v applied to every simulated node; repeatable")
	_ = fs.Parse(args)
	if *nodeLink == "" {
		return fmt.Errorf("--node-link is required")
	}
	if *nodes <= 0 {
		return fmt.Errorf("--nodes must be positive")
	}
	baseLabels, err := parseLabels(labels)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc := newService(*nodeLink, log)
	dataLn, err := net.Listen("tcp", *dataListen)
	if err != nil {
		return fmt.Errorf("data listen %s: %w", *dataListen, err)
	}
	defer dataLn.Close()
	adminLn, err := net.Listen("tcp", *adminListen)
	if err != nil {
		return fmt.Errorf("admin listen %s: %w", *adminListen, err)
	}
	defer adminLn.Close()
	svc.dataEndpoint = publicAddr(dataLn.Addr().String())
	svc.adminURL = "http://" + publicAddr(adminLn.Addr().String())

	dataSrv := &http.Server{Handler: http.HandlerFunc(svc.serveData)}
	adminSrv := &http.Server{Handler: svc}
	go func() {
		if err := dataSrv.Serve(dataLn); err != nil && err != http.ErrServerClosed {
			log.Error("node-stub data server", "err", err)
		}
	}()
	go func() {
		if err := adminSrv.Serve(adminLn); err != nil && err != http.ErrServerClosed {
			log.Error("node-stub admin server", "err", err)
		}
	}()
	defer dataSrv.Close()
	defer adminSrv.Close()

	for i := 1; i <= *nodes; i++ {
		labels := cloneStringMap(baseLabels)
		labels["node"] = fmt.Sprintf("%s-%d", *prefix, i)
		if _, ok := labels["slot"]; !ok {
			labels["slot"] = strconv.Itoa(i)
		}
		node := newStubNode(stubNodeOptions{
			ID:           fmt.Sprintf("%s-%d", *prefix, i),
			NodeLink:     *nodeLink,
			DataEndpoint: svc.dataEndpoint,
			Labels:       labels,
			Capacity:     *capacity,
			BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{
				CPU: int64(*registrationBuildCPU), Memory: *registrationBuildMem, Storage: *registrationBuildStorage,
			}},
			BuildExecutionCapacity: &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{
				CPU: int64(*executionBuildCPU), Memory: *executionBuildMem, Storage: *executionBuildStorage,
			}},
			RuntimeDigest:     *runtimeDigest,
			StrictKeys:        *strictKeys,
			HeartbeatInterval: *heartbeat,
			CreateDelay:       *createDelay,
			BuildDelay:        *buildDelay,
		}, svc)
		svc.addNode(node)
		node.start(ctx)
	}

	ready := map[string]any{"admin": svc.adminURL, "data": svc.dataEndpoint, "nodes": *nodes}
	_ = json.NewEncoder(os.Stdout).Encode(ready)
	log.Info("node-stub-ctl serve", "admin", svc.adminURL, "data", svc.dataEndpoint, "nodes", *nodes, "node_link", *nodeLink)
	<-ctx.Done()
	return nil
}

func parseLabels(vals []string) (map[string]string, error) {
	out := map[string]string{}
	for _, v := range vals {
		k, val, ok := strings.Cut(v, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("label %q must be k=v", v)
		}
		out[k] = val
	}
	return out, nil
}

func publicAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "::" || host == "0.0.0.0" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

type service struct {
	nodeLink     string
	adminURL     string
	dataEndpoint string
	log          *slog.Logger

	mu       sync.Mutex
	nodes    map[string]*stubNode
	events   []eventLog
	dataHits []dataHit
	seq      int64
}

func newService(nodeLink string, log *slog.Logger) *service {
	return &service{nodeLink: nodeLink, log: log, nodes: map[string]*stubNode{}}
}

func (s *service) addNode(n *stubNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[n.ID] = n
}

func (s *service) getNode(id string) (*stubNode, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	return n, ok
}

func (s *service) sortedNodes() []*stubNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*stubNode, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *service) logEvent(nodeID, kind string, detail any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.events = append(s.events, eventLog{Seq: s.seq, Time: time.Now().UTC().Format(time.RFC3339Nano), NodeID: nodeID, Type: kind, Detail: detail})
}

func (s *service) appendDataHit(hit dataHit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	hit.Seq = s.seq
	hit.Time = time.Now().UTC().Format(time.RFC3339Nano)
	s.dataHits = append(s.dataHits, hit)
}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	parts := splitPath(r.URL.Path)
	if len(parts) == 0 || parts[0] != "v1" {
		http.NotFound(w, r)
		return
	}
	switch {
	case len(parts) == 2 && parts[1] == "nodes" && r.Method == http.MethodGet:
		s.writeJSON(w, s.nodeSnapshots(""))
	case len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet:
		s.mu.Lock()
		out := append([]eventLog(nil), s.events...)
		s.mu.Unlock()
		s.writeJSON(w, out)
	case len(parts) == 2 && parts[1] == "data-hits" && r.Method == http.MethodGet:
		s.mu.Lock()
		out := append([]dataHit(nil), s.dataHits...)
		s.mu.Unlock()
		s.writeJSON(w, out)
	case len(parts) == 2 && parts[1] == "commands" && r.Method == http.MethodGet:
		s.writeJSON(w, s.allCommands())
	case len(parts) == 2 && parts[1] == "sandboxes" && r.Method == http.MethodGet:
		s.writeJSON(w, s.allSandboxes())
	case len(parts) == 2 && parts[1] == "builds" && r.Method == http.MethodGet:
		s.writeJSON(w, s.allBuilds())
	case len(parts) >= 3 && parts[1] == "nodes":
		s.serveNode(w, r, parts[2:])
	default:
		http.NotFound(w, r)
	}
}

func (s *service) serveNode(w http.ResponseWriter, r *http.Request, parts []string) {
	nodeID := parts[0]
	n, ok := s.getNode(nodeID)
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.writeJSON(w, n.snapshot())
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "restart-link":
			n.restartLink()
		case "reboot-empty":
			n.rebootEmpty()
		case "crash":
			n.crash()
		case "start":
			n.start(context.Background())
		case "drain":
			n.setDraining(true)
		case "undrain":
			n.setDraining(false)
		default:
			http.NotFound(w, r)
			return
		}
		s.writeJSON(w, map[string]any{"ok": true})
		return
	}
	if len(parts) == 2 && parts[1] == "commands" && r.Method == http.MethodGet {
		s.writeJSON(w, n.commandsSnapshot())
		return
	}
	if len(parts) >= 2 && parts[1] == "sandboxes" {
		s.serveSandboxes(w, r, n, parts[2:])
		return
	}
	if len(parts) >= 2 && parts[1] == "routes" {
		s.serveRoutes(w, r, n, parts[2:])
		return
	}
	if len(parts) >= 2 && parts[1] == "builds" {
		s.serveBuilds(w, r, n, parts[2:])
		return
	}
	http.NotFound(w, r)
}

func (s *service) serveSandboxes(w http.ResponseWriter, r *http.Request, n *stubNode, parts []string) {
	if len(parts) == 0 && r.Method == http.MethodGet {
		s.writeJSON(w, n.sandboxesSnapshot())
		return
	}
	if len(parts) == 0 && r.Method == http.MethodPost {
		var req sandboxAdminRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sb, err := n.createAdminSandbox(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.writeJSON(w, sb)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		n.deleteSandbox(parts[0], true)
		s.writeJSON(w, map[string]any{"ok": true})
		return
	}
	http.NotFound(w, r)
}

func (s *service) serveRoutes(w http.ResponseWriter, r *http.Request, n *stubNode, parts []string) {
	if len(parts) == 1 && parts[0] == "orphan" && r.Method == http.MethodPost {
		var req sandboxAdminRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.SID == "" {
			http.Error(w, "sid is required", http.StatusBadRequest)
			return
		}
		req.Metadata = observableSandboxMetadata(req.Metadata)
		n.publishRoute(routesync.RouteEntry{SandboxID: req.SID, State: routesync.StateRunning})
		s.logEvent(n.ID, "orphan_route", req)
		s.writeJSON(w, map[string]any{"ok": true})
		return
	}
	http.NotFound(w, r)
}

func (s *service) serveBuilds(w http.ResponseWriter, r *http.Request, n *stubNode, parts []string) {
	if len(parts) == 0 && r.Method == http.MethodGet {
		s.writeJSON(w, n.buildsSnapshot())
		return
	}
	if len(parts) == 2 && parts[1] == "state" && r.Method == http.MethodPost {
		var req struct {
			State      string `json:"state"`
			TemplateID string `json:"template_id,omitempty"`
			Reason     string `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := n.setBuildState(parts[0], req.State, req.TemplateID, req.Reason, true); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		s.writeJSON(w, map[string]any{"ok": true})
		return
	}
	http.NotFound(w, r)
}

func (s *service) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *service) nodeSnapshots(filter string) []nodeSnapshot {
	nodes := s.sortedNodes()
	out := make([]nodeSnapshot, 0, len(nodes))
	for _, n := range nodes {
		if filter == "" || n.ID == filter {
			out = append(out, n.snapshot())
		}
	}
	return out
}

func (s *service) allCommands() []commandLog {
	var out []commandLog
	for _, n := range s.sortedNodes() {
		out = append(out, n.commandsSnapshot()...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func (s *service) allSandboxes() []sandboxSnapshot {
	var out []sandboxSnapshot
	for _, n := range s.sortedNodes() {
		out = append(out, n.sandboxesSnapshot()...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID == out[j].NodeID {
			return out[i].SID < out[j].SID
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

func (s *service) allBuilds() []buildSnapshot {
	var out []buildSnapshot
	for _, n := range s.sortedNodes() {
		out = append(out, n.buildsSnapshot()...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID == out[j].NodeID {
			return out[i].BuildID < out[j].BuildID
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

func (s *service) serveData(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.HasPrefix(host, "api.") {
		s.serveControlStub(w, r)
		return
	}
	execService := r.Header.Get("E2b-Sandbox-Service") == "exec"
	if execService && r.Method != http.MethodConnect {
		w.Header().Set("Allow", http.MethodConnect)
		http.Error(w, "exec requires CONNECT", http.StatusMethodNotAllowed)
		return
	}
	sid := r.Header.Get("E2b-Sandbox-Id")
	if sid == "" {
		sid = sidFromHost(host)
	}
	if sid == "" {
		http.Error(w, "bad sandbox host", http.StatusBadRequest)
		return
	}
	n, sb := s.findSandbox(sid)
	if sb == nil {
		w.Header().Set("X-Kuasar-Proxy-Error", "not_found")
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	var execClaims keys.ExecAccessClaims
	var execPrograms *execadmission.ProgramSet
	if execService {
		var err error
		execClaims, err = keys.ParseAndVerifyExecAccessToken(
			r.Header.Get("X-Access-Token"), sb.ServiceSecret, sb.StableID, time.Now(),
		)
		if err != nil {
			http.Error(w, "invalid access token", http.StatusUnauthorized)
			return
		}
		compiler, err := execadmission.Default()
		if err != nil {
			http.Error(w, "exec admission unavailable", http.StatusNotImplemented)
			return
		}
		execPrograms, err = compiler.Compile(execClaims.Conditions)
		if err != nil {
			http.Error(w, "invalid exec conditions", http.StatusBadRequest)
			return
		}
	}
	if r.Method == http.MethodConnect {
		if execService {
			s.serveExecDataConnect(w, r, n, sb, execClaims, execPrograms)
			return
		}
		s.serveDataConnect(w, r, n, sb)
		return
	}
	status, body := sb.response()
	s.appendDataHit(dataHit{
		NodeID: n.ID, SandboxID: sb.SID, Cluster: cloneStubClusterContext(sb.Cluster), Metadata: observableSandboxMetadata(sb.Metadata),
		Host: r.Host, Path: r.URL.Path, Method: r.Method,
	})
	if status == 0 {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
	if body != "" {
		_, _ = io.WriteString(w, body)
	}
}

func (s *service) serveExecDataConnect(
	w http.ResponseWriter,
	r *http.Request,
	n *stubNode,
	sb *stubSandbox,
	claims keys.ExecAccessClaims,
	programs *execadmission.ProgramSet,
) {
	tunnelCtx := r.Context()
	cancelTunnel := func() {}
	if r.ProtoMajor != 2 {
		tunnelCtx = context.WithoutCancel(tunnelCtx)
		if deadline, ok := r.Context().Deadline(); ok {
			tunnelCtx, cancelTunnel = context.WithDeadline(tunnelCtx, deadline)
		}
	}
	defer cancelTunnel()
	_ = sandboxctl.ServeExecTunnel(tunnelCtx, sandboxctl.ExecTunnelOptions{
		Authorize: func(context.Context) error { return nil },
		AcceptDownstream: func(context.Context) (io.ReadWriteCloser, error) {
			return proxypkg.AcceptConnectStream(w, r)
		},
		AuthorizeRequest: func(ctx context.Context, frame *sandboxctl.ExecRequestFrame) error {
			if claims.ExpiresUnix != nil && time.Now().Unix() >= *claims.ExpiresUnix {
				return errors.New("exec capability expired")
			}
			return programs.Evaluate(ctx, frame.Request.Exec)
		},
		DialBackend: func(_ context.Context, frame *sandboxctl.ExecRequestFrame) (io.ReadWriteCloser, error) {
			client, backend := net.Pipe()
			raw := append([]byte(nil), frame.Raw...)
			go func() {
				defer backend.Close()
				got := make([]byte, len(raw))
				if _, err := io.ReadFull(backend, got); err != nil || !bytes.Equal(got, raw) {
					return
				}
				s.appendDataHit(dataHit{
					NodeID: n.ID, SandboxID: sb.SID,
					Cluster: cloneStubClusterContext(sb.Cluster), Metadata: observableSandboxMetadata(sb.Metadata),
					Host: "exec", Path: "/exec-admitted", Method: "EXEC",
				})
				_, _ = io.WriteString(backend, "exec-admitted")
			}()
			return client, nil
		},
		FirstRequestTimeout: limits.FirstRequestTimeout,
	})
}

func (s *service) serveDataConnect(w http.ResponseWriter, r *http.Request, n *stubNode, sb *stubSandbox) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connect unsupported", http.StatusInternalServerError)
		return
	}
	conn, br, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	inner, err := http.ReadRequest(br.Reader)
	if err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if inner.Body != nil {
		defer inner.Body.Close()
	}
	status, body := sb.response()
	if status == 0 {
		status = http.StatusNoContent
	}
	s.appendDataHit(dataHit{
		NodeID: n.ID, SandboxID: sb.SID, Cluster: cloneStubClusterContext(sb.Cluster), Metadata: observableSandboxMetadata(sb.Metadata),
		Host: inner.Host, Path: inner.URL.Path, Method: inner.Method,
	})
	resp := &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       inner,
	}
	_ = resp.Write(conn)
}

func (s *service) serveControlStub(w http.ResponseWriter, r *http.Request) {
	if buildID := extractBuildID(r.URL.Path); buildID != "" {
		for _, n := range s.sortedNodes() {
			if b := n.getBuild(buildID); b != nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(b)
				return
			}
		}
		http.Error(w, "build not found", http.StatusNotFound)
		return
	}
	if sid := extractSandboxID(r.URL.Path); sid != "" {
		n, sb := s.findSandbox(sid)
		if sb == nil {
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodDelete {
			n.deleteSandbox(sid, true)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/connect") {
			_ = json.NewEncoder(w).Encode(sb.connectResponse())
		} else {
			_ = json.NewEncoder(w).Encode(sb.publicDetailResponse(n.ID))
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *service) findSandbox(sid string) (*stubNode, *stubSandbox) {
	for _, n := range s.sortedNodes() {
		if sb := n.getSandbox(sid); sb != nil && sb.State == routesync.StateRunning {
			return n, sb
		}
	}
	return nil, nil
}

type stubNodeOptions struct {
	ID                        string
	NodeLink                  string
	DataEndpoint              string
	Labels                    map[string]string
	Capacity                  int
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit
	RuntimeDigest             string
	StrictKeys                bool
	HeartbeatInterval         time.Duration
	CreateDelay               time.Duration
	BuildDelay                time.Duration
}

type stubNode struct {
	stubNodeOptions
	svc *service
	log *slog.Logger

	mu           sync.Mutex
	online       bool
	cancel       context.CancelFunc
	draining     bool
	session      int64
	linkEndpoint string
	redirectTo   routesync.NodeLinkTarget
	sandboxes    map[string]*stubSandbox
	builds       map[string]*stubBuild
	buildSeq     int64
	executionSeq int64
	keyPairs     map[string]stubKeyPair
	commands     []commandLog
	cmdSeq       int64
	subs         map[int]chan routesync.Event
	subSeq       int
	buildEvents  chan *routesync.BuildEvent
}

func newStubNode(opts stubNodeOptions, svc *service) *stubNode {
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = time.Second
	}
	if opts.CreateDelay <= 0 {
		opts.CreateDelay = defaultCreateDelay
	}
	if opts.BuildDelay <= 0 {
		opts.BuildDelay = defaultBuildDelay
	}
	return &stubNode{
		stubNodeOptions: opts,
		svc:             svc,
		log:             svc.log.With("node", opts.ID),
		sandboxes:       map[string]*stubSandbox{},
		builds:          map[string]*stubBuild{},
		keyPairs:        map[string]stubKeyPair{},
		subs:            map[int]chan routesync.Event{},
		buildEvents:     make(chan *routesync.BuildEvent, 256),
	}
}

func (n *stubNode) start(parent context.Context) {
	n.mu.Lock()
	if n.online {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	n.cancel = cancel
	n.online = true
	n.session++
	identity := routesync.NodeRegister{
		NodeID: n.ID, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(n.BuildRegistrationCapacity),
		BuildExecutionCapacity:    cloneBuildAdmissionLimit(n.BuildExecutionCapacity),
		DataEndpoint:              n.DataEndpoint, RuntimeDigest: n.RuntimeDigest,
	}
	n.mu.Unlock()

	dial := func(ctx context.Context, endpoint string) (net.Conn, error) {
		if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
			if u, err := http.NewRequest(http.MethodGet, endpoint, nil); err == nil && u.URL.Host != "" {
				endpoint = u.URL.Host
			}
		}
		if strings.HasPrefix(endpoint, "/") {
			return (&net.Dialer{}).DialContext(ctx, "unix", endpoint)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	}
	client := nodelink.NewWithEndpoint(n.NodeLink, dial, identity, n, n.HeartbeatInterval, nil, n.log, true)
	go client.Run(ctx)
	n.svc.logEvent(n.ID, "node_start", map[string]any{"session": n.session})
}

func (n *stubNode) crash() {
	n.mu.Lock()
	cancel := n.cancel
	n.cancel = nil
	n.online = false
	n.linkEndpoint = ""
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	n.svc.logEvent(n.ID, "node_crash", nil)
}

func (n *stubNode) restartLink() {
	n.crash()
	time.Sleep(50 * time.Millisecond)
	n.start(context.Background())
	n.svc.logEvent(n.ID, "node_restart_link", nil)
}

func (n *stubNode) rebootEmpty() {
	n.mu.Lock()
	var old []routesync.RouteEntry
	for _, sb := range n.sandboxes {
		old = append(old, routesync.RouteEntry{SandboxID: sb.SID, Profile: sb.Profile, State: routesync.StateDead})
	}
	n.sandboxes = map[string]*stubSandbox{}
	n.builds = map[string]*stubBuild{}
	n.keyPairs = map[string]stubKeyPair{}
	n.mu.Unlock()
	for _, r := range old {
		n.publishRoute(r)
	}
	time.Sleep(50 * time.Millisecond)
	n.restartLink()
	n.svc.logEvent(n.ID, "node_reboot_empty", map[string]any{"deleted": len(old)})
}

func (n *stubNode) setDraining(v bool) {
	n.mu.Lock()
	n.draining = v
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "node_drain", map[string]any{"draining": v})
}

func (n *stubNode) NodeLinkSession(endpoint string) {
	n.mu.Lock()
	n.linkEndpoint = endpoint
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "node_link_session", map[string]string{"endpoint": endpoint})
}

func (n *stubNode) NodeLinkRedirect(target routesync.NodeLinkTarget) {
	n.mu.Lock()
	n.redirectTo = target
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "node_link_redirect", target)
}

func (n *stubNode) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	n.mu.Lock()
	routes := make([]routesync.RouteEntry, 0, len(n.sandboxes))
	for _, sb := range n.sandboxes {
		if sb.State == routesync.StateRunning || sb.State == routesync.StatePaused {
			routes = append(routes, sb.routeEntry())
		}
	}
	n.mu.Unlock()
	sort.Slice(routes, func(i, j int) bool { return routes[i].SandboxID < routes[j].SandboxID })
	for _, r := range routes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := fn(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *stubNode) Subscribe() (<-chan routesync.Event, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subSeq++
	id := n.subSeq
	ch := make(chan routesync.Event, 256)
	n.subs[id] = ch
	cancel := func() {
		n.mu.Lock()
		if c, ok := n.subs[id]; ok {
			delete(n.subs, id)
			close(c)
		}
		n.mu.Unlock()
	}
	return ch, cancel
}

func (n *stubNode) OnWake(ctx context.Context, sid string) {}
func (n *stubNode) Policy() routesync.Policy               { return routesync.Policy{} }

func (n *stubNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	if cmd == nil {
		return nil
	}
	n.recordCommand(cmd)
	if cmd.Kind != routesync.CmdExecSession && cmd.ExecConditionsSpecified() {
		return ack(cmd, routesync.AckRejected, "command contains exec conditions")
	}
	if cmd.Kind == routesync.CmdExecSession && cmd.ExecConditionsSpecified() && len(cmd.ExecConditions) == 0 {
		return ack(cmd, routesync.AckRejected, "command contains non-canonical exec conditions")
	}
	switch cmd.Kind {
	case routesync.CmdKeyPut:
		pair, err := stubKeyPairFromCommand(cmd)
		if err != nil {
			return ack(cmd, routesync.AckRejected, err.Error())
		}
		n.mu.Lock()
		if existing, found := n.keyPairs[pair.APISecretFingerprint]; found && !sameStubKeyPair(existing, pair) {
			n.mu.Unlock()
			return ack(cmd, routesync.AckRejected, "API secret fingerprint is already bound to a different key pair")
		}
		n.keyPairs[pair.APISecretFingerprint] = pair
		n.mu.Unlock()
		return ack(cmd, routesync.AckAccepted, "")
	case routesync.CmdKeyDrop:
		if !validStubFingerprint(cmd.APISecretFingerprint) {
			return ack(cmd, routesync.AckRejected, "API secret fingerprint must be 64 lowercase hex characters")
		}
		n.mu.Lock()
		delete(n.keyPairs, cmd.APISecretFingerprint)
		n.mu.Unlock()
		return ack(cmd, routesync.AckAccepted, "")
	case routesync.CmdCreate:
		return n.handleCreate(cmd)
	case routesync.CmdConnect:
		return n.handleConnect(cmd)
	case routesync.CmdExecSession:
		return n.handleExecSession(cmd)
	case routesync.CmdDelete:
		return n.handleDelete(cmd)
	case routesync.CmdBuildRegister:
		return n.handleBuildRegisterContext(ctx, cmd)
	default:
		return ack(cmd, routesync.AckRejected, "unknown command kind")
	}
}

func (n *stubNode) handleCreate(cmd *routesync.Command) *routesync.CmdAck {
	if cmd.SID == "" {
		return ack(cmd, routesync.AckRejected, "sid is required")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return ack(cmd, routesync.AckRejected, "valid profile is required")
	}
	if cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		return ack(cmd, routesync.AckRejected, "complete cluster sandbox context is required")
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil || tmpl.Profile != profile {
		return ack(cmd, routesync.AckRejected, "template and command profile must match")
	}
	credentials, metadata, err := sandboxcfg.ExtractCredentials(cmd.Config)
	if err != nil {
		return ack(cmd, routesync.AckRejected, "invalid sandbox credentials")
	}
	if err := sandboxcfg.ValidateCredentialsForProfile(profile, credentials); err != nil {
		return ack(cmd, routesync.AckRejected, "sandbox credentials do not match profile")
	}
	pair, found := n.keyPair(cmd.APISecretFingerprint)
	if !found {
		return ack(cmd, routesync.AckRejected, "credential pair not installed")
	}
	if pair.APISecretType != "inline" || pair.APISecret == "" {
		return ack(cmd, routesync.AckRejected, "API secret material unavailable")
	}
	beh := behaviorFromConfig(metadata, n.CreateDelay, n.BuildDelay)
	if beh.CreateResult == "reject" {
		return ack(cmd, routesync.AckRejected, "stub create rejected")
	}
	metadata = cloneStringMap(metadata)
	delete(metadata, clusterstate.ObjectMetadataKey)
	stableID := cmd.Cluster.StableID
	if stableID == "" {
		stableID = cmd.SID
	}
	materialized, err := materializeStubCredentials(profile, pair.APISecret, stableID, credentials)
	if err != nil {
		return ack(cmd, routesync.AckRejected, "sandbox credential materialization failed")
	}
	clusterContext := *cmd.Cluster
	sb := &stubSandbox{
		SID: cmd.SID, Profile: string(profile), Metadata: metadata, State: "creating",
		TemplateID:             cmd.TemplateRef,
		StableID:               stableID,
		APISecret:              pair.APISecret,
		APISecretFingerprint:   pair.APISecretFingerprint,
		ManifestKeyFingerprint: pair.ManifestKeyFingerprint,
		ServiceSecret:          materialized.ServiceSecret,
		EnvdAccessToken:        materialized.EnvdAccessToken,
		TrafficAccessToken:     materialized.TrafficAccessToken,
		ForwardAccessToken:     materialized.ForwardAccessToken,
		Cluster:                &clusterContext,
		Behavior:               beh, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	n.mu.Lock()
	n.sandboxes[sb.SID] = sb
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "sandbox_create", sb.snapshot(n.ID))
	if beh.CreateResult == "timeout" {
		return ack(cmd, routesync.AckAccepted, "")
	}
	go func() {
		time.Sleep(beh.CreateDelay)
		n.mu.Lock()
		cur := n.sandboxes[sb.SID]
		if cur == nil {
			n.mu.Unlock()
			return
		}
		cur.State = routesync.StateRunning
		entry := cur.routeEntry()
		n.mu.Unlock()
		n.publishRoute(entry)
		if beh.Duration > 0 {
			time.Sleep(beh.Duration)
			n.deleteSandbox(sb.SID, true)
		}
	}()
	return ack(cmd, routesync.AckAccepted, "")
}

func (n *stubNode) handleConnect(cmd *routesync.Command) *routesync.CmdAck {
	n.mu.Lock()
	sb := n.sandboxes[cmd.SID]
	if sb == nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox not found")
	}
	if cmd.APISecretFingerprint != sb.APISecretFingerprint {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox credential binding mismatch")
	}
	if cmd.Profile != sb.Profile || !sameStubClusterContext(cmd.Cluster, sb.Cluster) {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox context binding mismatch")
	}
	result, err := stubConnectResult(sb)
	if err != nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, err.Error())
	}
	if cmd.TimeoutSeconds > 0 {
		sb.DeadlineUnix = time.Now().Add(time.Duration(cmd.TimeoutSeconds) * time.Second).Unix()
	}
	sb.State = routesync.StateRunning
	entry := sb.routeEntry()
	n.mu.Unlock()
	n.publishRoute(entry)
	accepted := ack(cmd, routesync.AckAccepted, "")
	accepted.Connect = result
	return accepted
}

func (n *stubNode) handleExecSession(cmd *routesync.Command) *routesync.CmdAck {
	if err := validateStubExecSessionEnvelope(cmd); err != nil {
		return ack(cmd, routesync.AckRejected, err.Error())
	}
	compiler, err := execadmission.Default()
	if err != nil {
		return ack(cmd, routesync.AckRejected, "exec admission is unavailable")
	}
	if _, err := compiler.Compile(cmd.ExecConditions); err != nil {
		return ack(cmd, routesync.AckRejected, "invalid exec conditions")
	}
	if _, err := execsession.ExpiryUnix(time.Now().Unix(), cmd.TTLSeconds); err != nil {
		return ack(cmd, routesync.AckRejected, "invalid exec session ttl")
	}

	n.mu.Lock()
	sb := n.sandboxes[cmd.SID]
	if sb == nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox not found")
	}
	if cmd.APISecretFingerprint != sb.APISecretFingerprint {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox credential binding mismatch")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil || cmd.Cluster == nil || cmd.Cluster.Group == "" || cmd.Cluster.RouteKey == "" {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox context binding is incomplete")
	}
	stableID := cmd.Cluster.StableID
	if stableID == "" {
		stableID = cmd.SID
	}
	if sb.Profile != string(profile) || sb.Cluster == nil ||
		sb.Cluster.Group != cmd.Cluster.Group || sb.Cluster.RouteKey != cmd.Cluster.RouteKey ||
		sb.StableID != stableID {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox context binding mismatch")
	}
	if sb.State != routesync.StateRunning && sb.State != routesync.StatePaused {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox not found")
	}
	template, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil || template.Profile != profile {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox template and profile are inconsistent")
	}
	expiresUnix, err := execsession.ExpiryUnix(time.Now().Unix(), cmd.TTLSeconds)
	if err != nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "invalid exec session ttl")
	}
	token, err := keys.MintExecAccessTokenWithConditions(
		sb.ServiceSecret, sb.StableID, expiresUnix, cmd.ExecConditions,
	)
	if err != nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox exec credentials are invalid")
	}
	paused := sb.State == routesync.StatePaused
	n.mu.Unlock()

	accepted := ack(cmd, routesync.AckAccepted, "")
	accepted.ExecSession = &routesync.ExecSessionResult{ExecAccessToken: token}
	if paused {
		n.scheduleExecResume(cmd.SID, sb)
	}
	return accepted
}

func validateStubExecSessionEnvelope(cmd *routesync.Command) error {
	if cmd == nil || cmd.Kind != routesync.CmdExecSession || cmd.CmdID == "" ||
		!types.ValidLocalSandboxID(cmd.SID) || cmd.APISecretFingerprint == "" {
		return errors.New("exec session command identity is incomplete")
	}
	if cmd.TTLSeconds < 0 || cmd.TimeoutSeconds != 0 || cmd.TemplateRef != "" || len(cmd.Config) != 0 ||
		cmd.APISecretType != "" || cmd.APISecret != "" || cmd.APISecretRef != "" ||
		cmd.ManifestKeyFingerprint != "" || cmd.ManifestKeyType != "" || cmd.ManifestKey != "" || cmd.ManifestKeyRef != "" ||
		cmd.ExpiresUnix != 0 || cmd.BuildID != "" || cmd.BuildResources != nil || cmd.ImageRepo != "" || cmd.RegistryAuth != "" ||
		len(cmd.BuildMMDSSecrets) != 0 {
		return errors.New("exec session command contains fields for another operation")
	}
	return nil
}

func (n *stubNode) scheduleExecResume(sid string, expected *stubSandbox) {
	go func() {
		time.Sleep(n.CreateDelay)
		n.mu.Lock()
		current := n.sandboxes[sid]
		if current != expected || current.State != routesync.StatePaused {
			n.mu.Unlock()
			return
		}
		current.State = routesync.StateRunning
		entry := current.routeEntry()
		n.mu.Unlock()
		n.publishRoute(entry)
	}()
}

func (n *stubNode) handleDelete(cmd *routesync.Command) *routesync.CmdAck {
	n.mu.Lock()
	sb := n.sandboxes[cmd.SID]
	if sb == nil {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox not found")
	}
	if cmd.APISecretFingerprint != sb.APISecretFingerprint {
		n.mu.Unlock()
		return ack(cmd, routesync.AckRejected, "sandbox credential binding mismatch")
	}
	delete(n.sandboxes, cmd.SID)
	n.mu.Unlock()
	n.publishDelete(cmd.SID)
	n.svc.logEvent(n.ID, "sandbox_delete", map[string]any{"sid": cmd.SID, "found": true})
	return ack(cmd, routesync.AckAccepted, "")
}

func (n *stubNode) handleBuildRegister(cmd *routesync.Command) *routesync.CmdAck {
	return n.handleBuildRegisterContext(context.Background(), cmd)
}

func (n *stubNode) handleBuildRegisterContext(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	if cmd.BuildID == "" || cmd.TemplateRef == "" {
		return ackHTTP(cmd, routesync.AckRejected, "build_id and template_ref are required", http.StatusBadRequest)
	}
	if !types.Profile(cmd.Profile).Valid() {
		return ackHTTP(cmd, routesync.AckRejected, "valid profile is required", http.StatusBadRequest)
	}
	resources := cmd.BuildResources.Types()
	if err := resources.ValidateRequired(); err != nil {
		return ackHTTP(cmd, routesync.AckRejected, err.Error(), http.StatusBadRequest)
	}
	beh := behaviorFromConfig(cmd.Config, n.CreateDelay, n.BuildDelay)
	b := &stubBuild{
		BuildID: cmd.BuildID, Profile: cmd.Profile, APISecretFingerprint: cmd.APISecretFingerprint,
		Metadata: cloneStringMap(cmd.Config), State: "registered", TemplateID: cmd.TemplateRef,
		Resources: cloneBuildResources(cmd.BuildResources), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Behavior: beh,
		RegistrationImageRepo: cmd.ImageRepo, RegistrationRegistryAuth: cmd.RegistryAuth,
		RegistrationMMDSSecrets: cloneStringMap(cmd.BuildMMDSSecrets),
	}
	n.mu.Lock()
	if existing := n.builds[b.BuildID]; existing != nil {
		conflict := !sameStubBuildRegistration(existing, b)
		terminal := existing.State == "ready" || existing.State == "error"
		event := &routesync.BuildEvent{
			BuildID: existing.BuildID, State: existing.State,
			TemplateID: existing.TemplateID, Reason: existing.Reason,
		}
		n.mu.Unlock()
		if conflict {
			return ackHTTP(cmd, routesync.AckRejected, "build immutable definition conflicts with existing build", http.StatusConflict)
		}
		if terminal {
			select {
			case n.buildEvents <- event:
				n.svc.logEvent(n.ID, "build_event", event)
			case <-ctx.Done():
				return nil
			}
		}
		return ack(cmd, routesync.AckAccepted, "")
	}
	// An exact replay after an ambiguous/lost ACK must be recognized before the
	// mutable credential lease and current admission policy are consulted. The
	// Registry may otherwise treat key withdrawal as proof that this node had no
	// side effect and place the same BuildID elsewhere.
	if n.StrictKeys {
		if _, ok := n.keyPairLocked(cmd.APISecretFingerprint); !ok {
			n.mu.Unlock()
			return ackHTTP(cmd, routesync.AckRejected, "credential pair not installed", http.StatusBadRequest)
		}
	}
	if beh.BuildResult == "reject" {
		n.mu.Unlock()
		return ackHTTP(cmd, routesync.AckRejected, "stub build rejected", http.StatusTooManyRequests)
	}
	if !stubAdmissionLimit(n.BuildExecutionCapacity).AllowsOne(resources) {
		n.mu.Unlock()
		return ackHTTP(cmd, routesync.AckRejected, "build resources cannot fit execution admission", http.StatusBadRequest)
	}
	var usedBuilds int64
	var used types.BuildResources
	for _, existing := range n.builds {
		if existing.State != "registered" && existing.State != "building" {
			continue
		}
		if usedBuilds == math.MaxInt64 {
			n.mu.Unlock()
			return ackHTTP(cmd, routesync.AckRejected, "registration admission usage overflow", http.StatusTooManyRequests)
		}
		usedBuilds++
		var err error
		used, err = used.Add(existing.Resources.Types())
		if err != nil {
			n.mu.Unlock()
			return ackHTTP(cmd, routesync.AckRejected, "registration admission usage overflow", http.StatusTooManyRequests)
		}
	}
	if !stubAdmissionLimit(n.BuildRegistrationCapacity).AllowsAdd(usedBuilds, used, resources) {
		n.mu.Unlock()
		return ackHTTP(cmd, routesync.AckRejected, "registration admission capacity exceeded", http.StatusTooManyRequests)
	}
	if n.buildSeq == math.MaxInt64 {
		n.mu.Unlock()
		return ackHTTP(cmd, routesync.AckRejected, "build registration sequence overflow", http.StatusTooManyRequests)
	}
	n.buildSeq++
	b.RegistrationSeq = n.buildSeq
	n.builds[b.BuildID] = b
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "build_register", b.snapshot(n.ID))
	go func() {
		if beh.BuildResult == "timeout" {
			return
		}
		time.Sleep(beh.BuildDelay)
		state := beh.BuildResult
		if state == "" || state == "registered" {
			state = "building"
		}
		_ = n.setBuildState(b.BuildID, state, b.TemplateID, "", true)
	}()
	return ack(cmd, routesync.AckAccepted, "")
}

func sameStubBuildRegistration(a, b *stubBuild) bool {
	if a == nil || b == nil || a.Resources == nil || b.Resources == nil {
		return false
	}
	return a.Profile == b.Profile && a.TemplateID == b.TemplateID &&
		a.APISecretFingerprint == b.APISecretFingerprint && *a.Resources == *b.Resources &&
		maps.Equal(a.Metadata, b.Metadata) && a.RegistrationImageRepo == b.RegistrationImageRepo &&
		hmac.Equal([]byte(a.RegistrationRegistryAuth), []byte(b.RegistrationRegistryAuth)) &&
		maps.Equal(a.RegistrationMMDSSecrets, b.RegistrationMMDSSecrets)
}

func stubAdmissionLimit(limit *routesync.BuildAdmissionLimit) types.BuildAdmissionLimit {
	if limit == nil {
		return types.BuildAdmissionLimit{}
	}
	out := types.BuildAdmissionLimit{MaxBuilds: limit.MaxBuilds}
	if limit.Resources != nil {
		out.Resources = limit.Resources.Types()
	}
	return out
}

func (n *stubNode) Heartbeat() *routesync.Heartbeat {
	n.mu.Lock()
	defer n.mu.Unlock()
	var counts int
	var registration, execution routesync.BuildAdmissionUsage
	registration.Resources = &routesync.BuildResources{}
	execution.Resources = &routesync.BuildResources{}
	for _, sb := range n.sandboxes {
		if sb.State == routesync.StateRunning || sb.State == routesync.StatePaused || sb.State == "creating" {
			counts++
		}
	}
	for _, b := range n.builds {
		if b.State == "registered" || b.State == "building" {
			registration.Builds++
			addBuildResourcesFailClosed(registration.Resources, b.Resources)
		}
		if b.State == "building" {
			execution.Builds++
			addBuildResourcesFailClosed(execution.Resources, b.Resources)
		}
	}
	zone := n.Labels["zone"]
	return &routesync.Heartbeat{Zone: zone, Counts: counts, Draining: n.draining,
		BuildRegistrationUsage: &registration, BuildExecutionUsage: &execution}
}

func (n *stubNode) BuildEvents() <-chan *routesync.BuildEvent { return n.buildEvents }

func (n *stubNode) publishRoute(entry routesync.RouteEntry) {
	if entry.State == "" {
		entry.State = routesync.StateRunning
	}
	ev := routesync.Event{Kind: routesync.TypeUpsert, Route: entry}
	n.publish(ev)
	n.svc.logEvent(n.ID, "route_upsert", routeObservation(entry))
}

func (n *stubNode) publishDelete(sid string) {
	n.publish(routesync.Event{Kind: routesync.TypeDelete, SID: sid})
	n.svc.logEvent(n.ID, "route_delete", map[string]string{"sid": sid})
}

func (n *stubNode) publish(ev routesync.Event) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, ch := range n.subs {
		select {
		case ch <- ev:
		default:
			close(ch)
			delete(n.subs, id)
		}
	}
}

func (n *stubNode) keyPair(apiSecretFingerprint string) (stubKeyPair, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.keyPairLocked(apiSecretFingerprint)
}

func (n *stubNode) keyPairLocked(apiSecretFingerprint string) (stubKeyPair, bool) {
	pair, ok := n.keyPairs[apiSecretFingerprint]
	if !ok {
		return stubKeyPair{}, false
	}
	if pair.ExpiresUnix > 0 && pair.ExpiresUnix <= time.Now().Unix() {
		delete(n.keyPairs, apiSecretFingerprint)
		return stubKeyPair{}, false
	}
	return pair, true
}

func (n *stubNode) recordCommand(cmd *routesync.Command) {
	metadata := observableSandboxMetadata(cmd.Config)
	n.mu.Lock()
	n.cmdSeq++
	log := commandLog{
		Seq: n.cmdSeq, Time: time.Now().UTC().Format(time.RFC3339Nano), NodeID: n.ID,
		CmdID: cmd.CmdID, Kind: cmd.Kind, SID: cmd.SID, Metadata: metadata,
		BuildID: cmd.BuildID, Profile: cmd.Profile, Cluster: cloneStubClusterContext(cmd.Cluster),
		APISecretFingerprint: cmd.APISecretFingerprint,
	}
	n.commands = append(n.commands, log)
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "command", log)
}

func (n *stubNode) createAdminSandbox(req sandboxAdminRequest) (*sandboxSnapshot, error) {
	if req.SID == "" {
		req.SID = "sb-admin-" + randHex(4)
	}
	state := req.State
	if state == "" {
		state = routesync.StateRunning
	}
	beh := behaviorFromMap(req.Behavior, n.CreateDelay, n.BuildDelay)
	sb := &stubSandbox{
		SID: req.SID, Metadata: observableSandboxMetadata(req.Metadata), State: state,
		TemplateID: req.TemplateID,
		Behavior:   beh, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	n.mu.Lock()
	n.sandboxes[sb.SID] = sb
	n.mu.Unlock()
	if state == routesync.StateRunning || state == routesync.StatePaused {
		n.publishRoute(sb.routeEntry())
	}
	snap := sb.snapshot(n.ID)
	return &snap, nil
}

func (n *stubNode) deleteSandbox(sid string, publish bool) {
	n.mu.Lock()
	_, found := n.sandboxes[sid]
	delete(n.sandboxes, sid)
	n.mu.Unlock()
	if publish && found {
		n.publishDelete(sid)
	}
	n.svc.logEvent(n.ID, "sandbox_delete", map[string]any{"sid": sid, "found": found})
}

func (n *stubNode) setBuildState(buildID, state, templateID, reason string, publish bool) error {
	if state == "" {
		state = "building"
	}
	if state == "building" {
		return n.requestBuildExecution(buildID)
	}
	n.mu.Lock()
	b := n.builds[buildID]
	if b == nil {
		n.mu.Unlock()
		return fmt.Errorf("build not found")
	}
	wasBuilding := b.State == "building"
	b.State = state
	b.ExecutionReady = false
	if templateID != "" {
		b.TemplateID = templateID
	}
	if reason != "" {
		b.Reason = reason
	}
	ev := &routesync.BuildEvent{BuildID: b.BuildID, State: b.State, TemplateID: b.TemplateID, Reason: b.Reason}
	n.mu.Unlock()
	if publish {
		n.publishBuildEvent(ev)
	}
	if wasBuilding {
		n.scheduleBuildExecutions()
	}
	return nil
}

func (n *stubNode) requestBuildExecution(buildID string) error {
	n.mu.Lock()
	b := n.builds[buildID]
	if b == nil {
		n.mu.Unlock()
		return fmt.Errorf("build not found")
	}
	if b.State != "registered" {
		n.mu.Unlock()
		return nil
	}
	if !b.ExecutionReady {
		if n.executionSeq == math.MaxInt64 {
			n.mu.Unlock()
			return fmt.Errorf("build execution sequence overflow")
		}
		n.executionSeq++
		b.ExecutionReady = true
		b.ExecutionSeq = n.executionSeq
	}
	n.mu.Unlock()
	n.scheduleBuildExecutions()
	return nil
}

// scheduleBuildExecutions models the real node's durable FIFO execution
// admission closely enough for cluster and fault-injection tests: registered
// work remains queued until the aggregate count/CPU/memory/storage vector fits.
func (n *stubNode) scheduleBuildExecutions() {
	n.mu.Lock()
	limit := stubAdmissionLimit(n.BuildExecutionCapacity)
	var usedBuilds int64
	var used types.BuildResources
	queued := make([]*stubBuild, 0)
	for _, b := range n.builds {
		switch {
		case b.State == "building":
			if usedBuilds == math.MaxInt64 {
				n.mu.Unlock()
				return
			}
			usedBuilds++
			var err error
			used, err = used.Add(b.Resources.Types())
			if err != nil {
				n.mu.Unlock()
				return
			}
		case b.State == "registered" && b.ExecutionReady:
			queued = append(queued, b)
		}
	}
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].ExecutionSeq == queued[j].ExecutionSeq {
			return queued[i].BuildID < queued[j].BuildID
		}
		return queued[i].ExecutionSeq < queued[j].ExecutionSeq
	})
	events := make([]*routesync.BuildEvent, 0, len(queued))
	for _, b := range queued {
		resources := b.Resources.Types()
		if !limit.AllowsAdd(usedBuilds, used, resources) {
			break // strict FIFO: do not bypass the oldest ready Build
		}
		next, err := used.Add(resources)
		if err != nil || usedBuilds == math.MaxInt64 {
			break
		}
		used, usedBuilds = next, usedBuilds+1
		b.State = "building"
		b.ExecutionReady = false
		events = append(events, &routesync.BuildEvent{
			BuildID: b.BuildID, State: b.State, TemplateID: b.TemplateID, Reason: b.Reason,
		})
	}
	n.mu.Unlock()
	for _, event := range events {
		n.publishBuildEvent(event)
	}
}

func (n *stubNode) publishBuildEvent(event *routesync.BuildEvent) {
	select {
	case n.buildEvents <- event:
	default:
	}
	n.svc.logEvent(n.ID, "build_event", event)
}

func (n *stubNode) getSandbox(sid string) *stubSandbox {
	n.mu.Lock()
	defer n.mu.Unlock()
	if sb := n.sandboxes[sid]; sb != nil {
		cp := *sb
		cp.Metadata = cloneStringMap(sb.Metadata)
		cp.Cluster = cloneStubClusterContext(sb.Cluster)
		return &cp
	}
	return nil
}

func (n *stubNode) getBuild(buildID string) *stubBuild {
	n.mu.Lock()
	defer n.mu.Unlock()
	if b := n.builds[buildID]; b != nil {
		cp := *b
		cp.Metadata = cloneStringMap(b.Metadata)
		cp.Resources = cloneBuildResources(b.Resources)
		return &cp
	}
	return nil
}

func (n *stubNode) snapshot() nodeSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return nodeSnapshot{
		NodeID: n.ID, Online: n.online, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest, Draining: n.draining,
		LinkEndpoint: n.linkEndpoint, RedirectMemberID: n.redirectTo.MemberID, RedirectEndpoint: n.redirectTo.Endpoint,
		Sandboxes: n.sandboxSnapshotsLocked(), Builds: n.buildSnapshotsLocked(), KeyPairs: n.keyPairSnapshotsLocked(),
		CommandCounts: n.commandCountsLocked(),
	}
}

func (n *stubNode) sandboxesSnapshot() []sandboxSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sandboxSnapshotsLocked()
}

func (n *stubNode) buildsSnapshot() []buildSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.buildSnapshotsLocked()
}

func (n *stubNode) commandsSnapshot() []commandLog {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := append([]commandLog(nil), n.commands...)
	return out
}

func (n *stubNode) sandboxSnapshotsLocked() []sandboxSnapshot {
	out := make([]sandboxSnapshot, 0, len(n.sandboxes))
	for _, sb := range n.sandboxes {
		out = append(out, sb.snapshot(n.ID))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SID < out[j].SID })
	return out
}

func (n *stubNode) buildSnapshotsLocked() []buildSnapshot {
	out := make([]buildSnapshot, 0, len(n.builds))
	for _, b := range n.builds {
		out = append(out, b.snapshot(n.ID))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BuildID < out[j].BuildID })
	return out
}

func (n *stubNode) keyPairSnapshotsLocked() []stubKeyPairSnapshot {
	out := make([]stubKeyPairSnapshot, 0, len(n.keyPairs))
	for _, pair := range n.keyPairs {
		out = append(out, stubKeyPairSnapshot{
			APISecretFingerprint:   pair.APISecretFingerprint,
			APISecretType:          pair.APISecretType,
			ManifestKeyFingerprint: pair.ManifestKeyFingerprint,
			ManifestKeyType:        pair.ManifestKeyType,
			ExpiresUnix:            pair.ExpiresUnix,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].APISecretFingerprint < out[j].APISecretFingerprint
	})
	return out
}

func (n *stubNode) commandCountsLocked() map[string]int {
	out := map[string]int{}
	for _, c := range n.commands {
		out[c.Kind]++
	}
	return out
}

type stubSandbox struct {
	SID                    string
	Profile                string
	Metadata               map[string]string
	State                  string
	TemplateID             string
	StableID               string
	APISecret              string `json:"-"`
	APISecretFingerprint   string
	ManifestKeyFingerprint string
	ServiceSecret          string `json:"-"`
	EnvdAccessToken        string `json:"-"`
	TrafficAccessToken     string `json:"-"`
	ForwardAccessToken     string `json:"-"`
	DeadlineUnix           int64
	Cluster                *routesync.ClusterSandboxContext
	Behavior               stubBehavior
	CreatedAt              string
}

func stubConnectResult(s *stubSandbox) (*routesync.ConnectResult, error) {
	if s == nil || s.SID == "" || s.TemplateID == "" || s.ForwardAccessToken == "" {
		return nil, errors.New("sandbox connect result is incomplete")
	}
	template, err := types.ParseTemplateID(s.TemplateID)
	if err != nil || string(template.Profile) != s.Profile {
		return nil, errors.New("sandbox template and profile are inconsistent")
	}
	result := &routesync.ConnectResult{
		NodeSandboxID:      s.SID,
		TemplateID:         s.TemplateID,
		Profile:            s.Profile,
		ForwardAccessToken: s.ForwardAccessToken,
	}
	switch types.Profile(s.Profile) {
	case types.ProfileBare:
		if s.EnvdAccessToken != "" || s.TrafficAccessToken != "" {
			return nil, errors.New("bare sandbox contains e2b access tokens")
		}
	case types.ProfileE2B:
		if s.EnvdAccessToken == "" || s.TrafficAccessToken == "" {
			return nil, errors.New("e2b sandbox access tokens are required")
		}
		result.EnvdAccessToken = s.EnvdAccessToken
		result.TrafficAccessToken = s.TrafficAccessToken
	default:
		return nil, errors.New("sandbox profile is invalid")
	}
	return result, nil
}

func (s *stubSandbox) routeEntry() routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID: s.SID, State: s.State,
		TemplateID:             s.TemplateID,
		Profile:                s.Profile,
		StableID:               s.StableID,
		APISecret:              s.APISecret,
		APISecretFingerprint:   s.APISecretFingerprint,
		ManifestKeyFingerprint: s.ManifestKeyFingerprint,
		ServiceSecret:          s.ServiceSecret,
		EnvdAccessToken:        s.EnvdAccessToken,
		TrafficAccessToken:     s.TrafficAccessToken,
		ForwardAccessToken:     s.ForwardAccessToken,
	}
}

func (s *stubSandbox) connectResponse() map[string]any {
	response := map[string]any{
		"sandboxID":  s.SID,
		"templateID": s.TemplateID,
	}
	if s.ForwardAccessToken != "" {
		response["forwardAccessToken"] = s.ForwardAccessToken
	}
	if types.Profile(s.Profile) == types.ProfileE2B {
		response["envdAccessToken"] = s.EnvdAccessToken
		response["trafficAccessToken"] = s.TrafficAccessToken
	}
	return response
}

func (s *stubSandbox) publicDetailResponse(nodeID string) map[string]any {
	return map[string]any{
		"sandboxID":  s.SID,
		"templateID": s.TemplateID,
		"clientID":   nodeID,
		"state":      s.State,
		"metadata":   observableSandboxMetadata(s.Metadata),
	}
}

type sandboxRouteObservation struct {
	SID              string `json:"sid"`
	Profile          string `json:"profile,omitempty"`
	TemplateID       string `json:"template_id,omitempty"`
	State            string `json:"state,omitempty"`
	SnapshotLocation string `json:"snap_loc,omitempty"`
}

func routeObservation(entry routesync.RouteEntry) sandboxRouteObservation {
	return sandboxRouteObservation{
		SID:              entry.SandboxID,
		Profile:          entry.Profile,
		TemplateID:       entry.TemplateID,
		State:            entry.State,
		SnapshotLocation: entry.SnapshotLocation,
	}
}

type stubCredentials struct {
	ServiceSecret      string
	EnvdAccessToken    string
	TrafficAccessToken string
	ForwardAccessToken string
}

func materializeStubCredentials(profile types.Profile, apiSecret, stableID string, overrides sandboxcfg.Credentials) (stubCredentials, error) {
	if err := sandboxcfg.ValidateCredentialsForProfile(profile, overrides); err != nil {
		return stubCredentials{}, err
	}
	serviceSecret := overrides.ServiceSecret
	var err error
	if serviceSecret == "" {
		serviceSecret, err = keys.DeriveServiceSecret(apiSecret, stableID)
		if err != nil {
			return stubCredentials{}, err
		}
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, stableID)
	if err != nil {
		return stubCredentials{}, err
	}
	credentials := stubCredentials{
		ServiceSecret:      serviceSecret,
		ForwardAccessToken: forwardAccessToken,
	}
	if profile == types.ProfileBare {
		return credentials, nil
	}
	credentials.EnvdAccessToken = overrides.EnvdAccessToken
	if credentials.EnvdAccessToken == "" {
		credentials.EnvdAccessToken, err = keys.MintToken()
		if err != nil {
			return stubCredentials{}, err
		}
	}
	credentials.TrafficAccessToken = overrides.TrafficAccessToken
	if credentials.TrafficAccessToken == "" {
		credentials.TrafficAccessToken, err = keys.MintToken()
		if err != nil {
			return stubCredentials{}, err
		}
	}
	return credentials, nil
}

func sameStubClusterContext(a, b *routesync.ClusterSandboxContext) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Group == b.Group && a.RouteKey == b.RouteKey && a.StableID == b.StableID
}

func (s *stubSandbox) snapshot(nodeID string) sandboxSnapshot {
	return sandboxSnapshot{
		NodeID: nodeID, SID: s.SID, Profile: s.Profile, Cluster: cloneStubClusterContext(s.Cluster),
		Metadata: observableSandboxMetadata(s.Metadata), State: s.State,
		TemplateID:           s.TemplateID,
		APISecretFingerprint: s.APISecretFingerprint,
		Behavior:             s.Behavior, CreatedAt: s.CreatedAt,
	}
}

func observableSandboxMetadata(metadata map[string]string) map[string]string {
	out := cloneStringMap(metadata)
	delete(out, sandboxcfg.NsCredentials)
	return out
}

func cloneStubClusterContext(in *routesync.ClusterSandboxContext) *routesync.ClusterSandboxContext {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (s *stubSandbox) response() (int, string) {
	status := s.Behavior.HTTPStatus
	if status == 0 {
		status = http.StatusNoContent
	}
	return status, s.Behavior.HTTPBody
}

type stubBuild struct {
	BuildID                  string                    `json:"build_id"`
	Profile                  string                    `json:"profile"`
	APISecretFingerprint     string                    `json:"-"`
	Metadata                 map[string]string         `json:"metadata,omitempty"`
	State                    string                    `json:"state"`
	TemplateID               string                    `json:"template_id,omitempty"`
	Reason                   string                    `json:"reason,omitempty"`
	Resources                *routesync.BuildResources `json:"resources,omitempty"`
	RegistrationImageRepo    string                    `json:"-"`
	RegistrationRegistryAuth string                    `json:"-"`
	RegistrationMMDSSecrets  map[string]string         `json:"-"`
	Behavior                 stubBehavior              `json:"behavior,omitempty"`
	CreatedAt                string                    `json:"created_at,omitempty"`
	RegistrationSeq          int64                     `json:"-"`
	ExecutionReady           bool                      `json:"-"`
	ExecutionSeq             int64                     `json:"-"`
}

func (b *stubBuild) snapshot(nodeID string) buildSnapshot {
	return buildSnapshot{
		NodeID: nodeID, BuildID: b.BuildID, Profile: b.Profile, Metadata: cloneStringMap(b.Metadata), State: b.State, TemplateID: b.TemplateID, Reason: b.Reason,
		Resources: cloneBuildResources(b.Resources), Behavior: b.Behavior, CreatedAt: b.CreatedAt,
	}
}

type stubBehavior struct {
	CreateDelay  time.Duration `json:"-"`
	Duration     time.Duration `json:"-"`
	BuildDelay   time.Duration `json:"-"`
	CreateResult string        `json:"create_result,omitempty"`
	BuildResult  string        `json:"build_result,omitempty"`
	HTTPStatus   int           `json:"http_status,omitempty"`
	HTTPBody     string        `json:"http_body,omitempty"`
}

func behaviorFromConfig(cfg map[string]string, createDelay, buildDelay time.Duration) stubBehavior {
	return behaviorFromMap(cfg, createDelay, buildDelay)
}

func behaviorFromMap(cfg map[string]string, createDelay, buildDelay time.Duration) stubBehavior {
	b := stubBehavior{CreateDelay: createDelay, BuildDelay: buildDelay, CreateResult: "running", BuildResult: "building", HTTPStatus: http.StatusNoContent}
	if cfg == nil {
		return b
	}
	if v := cfg["stub.create_delay_ms"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			b.CreateDelay = time.Duration(n) * time.Millisecond
		}
	}
	if v := cfg["stub.duration_ms"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			b.Duration = time.Duration(n) * time.Millisecond
		}
	}
	if v := cfg["stub.build_delay_ms"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			b.BuildDelay = time.Duration(n) * time.Millisecond
		}
	}
	if v := cfg["stub.create_result"]; v != "" {
		b.CreateResult = v
	}
	if v := cfg["stub.build_result"]; v != "" {
		b.BuildResult = v
	}
	if v := cfg["stub.http_status"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			b.HTTPStatus = n
		}
	}
	if v := cfg["stub.http_body"]; v != "" {
		b.HTTPBody = v
	}
	return b
}

type stubKeyPair struct {
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	APISecretType          string `json:"api_secret_type,omitempty"`
	APISecret              string `json:"api_secret,omitempty"`
	APISecretRef           string `json:"api_secret_ref,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	ManifestKeyType        string `json:"manifest_key_type,omitempty"`
	ManifestKey            string `json:"manifest_key,omitempty"`
	ManifestKeyRef         string `json:"manifest_key_ref,omitempty"`
	ExpiresUnix            int64  `json:"expires_unix,omitempty"`
}

// stubKeyPairSnapshot intentionally excludes inline material and secret-provider
// references. The stub admin endpoint is observability-only and may listen beyond
// loopback, so it exposes only non-secret identity and lease metadata.
type stubKeyPairSnapshot struct {
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	APISecretType          string `json:"api_secret_type,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	ManifestKeyType        string `json:"manifest_key_type,omitempty"`
	ExpiresUnix            int64  `json:"expires_unix,omitempty"`
}

func stubKeyPairFromCommand(cmd *routesync.Command) (stubKeyPair, error) {
	pair := stubKeyPair{
		APISecretFingerprint:   cmd.APISecretFingerprint,
		APISecretType:          cmd.APISecretType,
		APISecret:              cmd.APISecret,
		APISecretRef:           cmd.APISecretRef,
		ManifestKeyFingerprint: cmd.ManifestKeyFingerprint,
		ManifestKeyType:        cmd.ManifestKeyType,
		ManifestKey:            cmd.ManifestKey,
		ManifestKeyRef:         cmd.ManifestKeyRef,
		ExpiresUnix:            cmd.ExpiresUnix,
	}
	if !validStubFingerprint(pair.APISecretFingerprint) {
		return stubKeyPair{}, fmt.Errorf("API secret fingerprint must be 64 lowercase hex characters")
	}
	if !validStubFingerprint(pair.ManifestKeyFingerprint) {
		return stubKeyPair{}, fmt.Errorf("manifest key fingerprint must be 64 lowercase hex characters")
	}
	var err error
	pair.APISecretType, err = normalizeStubSecret(
		"API secret", pair.APISecretType, pair.APISecret, pair.APISecretRef, pair.APISecretFingerprint,
	)
	if err != nil {
		return stubKeyPair{}, err
	}
	pair.ManifestKeyType, err = normalizeStubSecret(
		"manifest key", pair.ManifestKeyType, pair.ManifestKey, pair.ManifestKeyRef, pair.ManifestKeyFingerprint,
	)
	if err != nil {
		return stubKeyPair{}, err
	}
	return pair, nil
}

func normalizeStubSecret(name, typ, value, ref, fingerprint string) (string, error) {
	if typ == "" && value != "" && ref == "" {
		typ = "inline"
	}
	switch typ {
	case "inline":
		if value == "" || ref != "" {
			return "", fmt.Errorf("%s inline carrier is incomplete", name)
		}
		if !validStubSecret(value) {
			return "", fmt.Errorf("%s must be 64 lowercase hex characters", name)
		}
		raw, _ := hex.DecodeString(value)
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != fingerprint {
			return "", fmt.Errorf("%s fingerprint does not match inline material", name)
		}
	case "ref":
		if ref == "" || value != "" {
			return "", fmt.Errorf("%s ref carrier is incomplete", name)
		}
	default:
		return "", fmt.Errorf("unsupported %s carrier type %q", name, typ)
	}
	return typ, nil
}

func validStubSecret(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validStubFingerprint(fingerprint string) bool {
	return validStubSecret(fingerprint)
}

func sameStubKeyPair(a, b stubKeyPair) bool {
	apiSecretEqual := hmac.Equal([]byte(a.APISecret), []byte(b.APISecret))
	apiRefEqual := hmac.Equal([]byte(a.APISecretRef), []byte(b.APISecretRef))
	manifestKeyEqual := hmac.Equal([]byte(a.ManifestKey), []byte(b.ManifestKey))
	manifestRefEqual := hmac.Equal([]byte(a.ManifestKeyRef), []byte(b.ManifestKeyRef))
	return a.APISecretFingerprint == b.APISecretFingerprint &&
		a.APISecretType == b.APISecretType && apiSecretEqual && apiRefEqual &&
		a.ManifestKeyFingerprint == b.ManifestKeyFingerprint &&
		a.ManifestKeyType == b.ManifestKeyType && manifestKeyEqual && manifestRefEqual
}

type eventLog struct {
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
	NodeID string `json:"node_id,omitempty"`
	Type   string `json:"type"`
	Detail any    `json:"detail,omitempty"`
}

type commandLog struct {
	Seq                  int64                            `json:"seq"`
	Time                 string                           `json:"time"`
	NodeID               string                           `json:"node_id"`
	CmdID                string                           `json:"cmd_id"`
	Kind                 string                           `json:"kind"`
	SID                  string                           `json:"sid,omitempty"`
	Metadata             map[string]string                `json:"metadata,omitempty"`
	BuildID              string                           `json:"build_id,omitempty"`
	Profile              string                           `json:"profile,omitempty"`
	Cluster              *routesync.ClusterSandboxContext `json:"cluster,omitempty"`
	APISecretFingerprint string                           `json:"api_secret_fingerprint,omitempty"`
}

type dataHit struct {
	Seq       int64                            `json:"seq"`
	Time      string                           `json:"time"`
	NodeID    string                           `json:"node_id"`
	SandboxID string                           `json:"sid"`
	Cluster   *routesync.ClusterSandboxContext `json:"cluster,omitempty"`
	Metadata  map[string]string                `json:"metadata,omitempty"`
	Host      string                           `json:"host"`
	Path      string                           `json:"path"`
	Method    string                           `json:"method"`
}

type nodeSnapshot struct {
	NodeID           string                `json:"node_id"`
	Online           bool                  `json:"online"`
	Labels           map[string]string     `json:"labels,omitempty"`
	Capacity         int                   `json:"capacity,omitempty"`
	DataEndpoint     string                `json:"data_endpoint,omitempty"`
	RuntimeDigest    string                `json:"runtime_digest,omitempty"`
	Draining         bool                  `json:"draining,omitempty"`
	LinkEndpoint     string                `json:"link_endpoint,omitempty"`
	RedirectMemberID string                `json:"redirect_member_id,omitempty"`
	RedirectEndpoint string                `json:"redirect_endpoint,omitempty"`
	Sandboxes        []sandboxSnapshot     `json:"sandboxes,omitempty"`
	Builds           []buildSnapshot       `json:"builds,omitempty"`
	KeyPairs         []stubKeyPairSnapshot `json:"keys,omitempty"`
	CommandCounts    map[string]int        `json:"command_counts,omitempty"`
}

type sandboxSnapshot struct {
	NodeID               string                           `json:"node_id,omitempty"`
	SID                  string                           `json:"sid"`
	Profile              string                           `json:"profile"`
	Cluster              *routesync.ClusterSandboxContext `json:"cluster,omitempty"`
	Metadata             map[string]string                `json:"metadata,omitempty"`
	State                string                           `json:"state"`
	TemplateID           string                           `json:"template_id,omitempty"`
	APISecretFingerprint string                           `json:"api_secret_fingerprint,omitempty"`
	Behavior             stubBehavior                     `json:"behavior,omitempty"`
	CreatedAt            string                           `json:"created_at,omitempty"`
}

type buildSnapshot struct {
	NodeID     string                    `json:"node_id,omitempty"`
	BuildID    string                    `json:"build_id"`
	Profile    string                    `json:"profile"`
	Metadata   map[string]string         `json:"metadata,omitempty"`
	State      string                    `json:"state"`
	TemplateID string                    `json:"template_id,omitempty"`
	Reason     string                    `json:"reason,omitempty"`
	Resources  *routesync.BuildResources `json:"resources,omitempty"`
	Behavior   stubBehavior              `json:"behavior,omitempty"`
	CreatedAt  string                    `json:"created_at,omitempty"`
}

type sandboxAdminRequest struct {
	SID        string            `json:"sid"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	State      string            `json:"state,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Behavior   map[string]string `json:"behavior,omitempty"`
}

func ack(cmd *routesync.Command, status, reason string) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: status, Reason: reason}
}

func ackHTTP(cmd *routesync.Command, status, reason string, httpStatus int) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: status, Reason: reason, HTTPStatus: httpStatus}
}

func sidFromHost(host string) string {
	left := host
	if i := strings.IndexByte(left, '.'); i >= 0 {
		left = left[:i]
	}
	i := strings.IndexByte(left, '-')
	if i < 0 || i+1 >= len(left) {
		return ""
	}
	return left[i+1:]
}

func extractBuildID(path string) string {
	i := strings.Index(path, "/builds/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/builds/"):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

func extractSandboxID(path string) string {
	i := strings.Index(path, "/sandboxes/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/sandboxes/"):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneBuildResources(in *routesync.BuildResources) *routesync.BuildResources {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

func cloneBuildAdmissionLimit(in *routesync.BuildAdmissionLimit) *routesync.BuildAdmissionLimit {
	if in == nil {
		return nil
	}
	out := *in
	out.Resources = cloneBuildResources(in.Resources)
	return &out
}

func addBuildResourcesFailClosed(dst *routesync.BuildResources, src *routesync.BuildResources) {
	if src == nil {
		return
	}
	next, err := dst.Types().Add(src.Types())
	if err != nil {
		// The stub feeds the same placement path as a real node. Never let a
		// malformed test fixture or arithmetic wrap advertise false headroom.
		dst.CPU = math.MaxInt64
		dst.Memory = math.MaxInt64
		dst.Storage = math.MaxInt64
		return
	}
	*dst = *routesync.BuildResourcesFromTypes(next)
}

func randHex(n int) string {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw)
}

// --- Thin admin CLI wrappers ------------------------------------------------

func runGet(args []string, path string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	admin := fs.String("admin", "http://127.0.0.1:18080", "node-stub admin URL")
	_ = fs.Parse(args)
	return printRequest(http.MethodGet, strings.TrimRight(*admin, "/")+path, nil)
}

func runNode(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: node-stub-ctl node ACTION NODE --admin URL")
	}
	action, nodeID := args[0], args[1]
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	admin := fs.String("admin", "http://127.0.0.1:18080", "node-stub admin URL")
	_ = fs.Parse(args[2:])
	switch action {
	case "restart-link", "reboot-empty", "crash", "start", "drain", "undrain":
	default:
		return fmt.Errorf("unknown node action %q", action)
	}
	return printRequest(http.MethodPost, fmt.Sprintf("%s/v1/nodes/%s/%s", strings.TrimRight(*admin, "/"), nodeID, action), nil)
}

func runSandbox(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: node-stub-ctl sandbox {list|create|delete|orphan}")
	}
	action := args[0]
	fs := flag.NewFlagSet("sandbox", flag.ExitOnError)
	admin := fs.String("admin", "http://127.0.0.1:18080", "node-stub admin URL")
	nodeID := fs.String("node", "", "node id")
	sid := fs.String("sid", "", "sandbox id")
	state := fs.String("state", routesync.StateRunning, "sandbox state")
	var metadata multiFlag
	fs.Var(&metadata, "metadata", "sandbox metadata k=v; repeatable")
	_ = fs.Parse(args[1:])
	metadataMap, err := parseLabels(metadata)
	if err != nil {
		return err
	}
	baseAdmin := strings.TrimRight(*admin, "/")
	base := fmt.Sprintf("%s/v1/nodes/%s", baseAdmin, *nodeID)
	switch action {
	case "list":
		if *nodeID == "" {
			return printRequest(http.MethodGet, baseAdmin+"/v1/sandboxes", nil)
		}
		return printRequest(http.MethodGet, base+"/sandboxes", nil)
	case "create":
		if *nodeID == "" {
			return fmt.Errorf("--node is required")
		}
		body := sandboxAdminRequest{SID: *sid, Metadata: metadataMap, State: *state}
		return printRequest(http.MethodPost, base+"/sandboxes", body)
	case "delete":
		if *nodeID == "" {
			return fmt.Errorf("--node is required")
		}
		if *sid == "" {
			return fmt.Errorf("--sid is required")
		}
		return printRequest(http.MethodDelete, base+"/sandboxes/"+*sid, nil)
	case "orphan":
		if *nodeID == "" {
			return fmt.Errorf("--node is required")
		}
		body := sandboxAdminRequest{SID: *sid, Metadata: metadataMap}
		return printRequest(http.MethodPost, base+"/routes/orphan", body)
	default:
		return fmt.Errorf("unknown sandbox action %q", action)
	}
}

func runBuild(args []string) error {
	if len(args) < 1 || args[0] != "list" {
		return fmt.Errorf("usage: node-stub-ctl build list --admin URL [--node NODE]")
	}
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	admin := fs.String("admin", "http://127.0.0.1:18080", "node-stub admin URL")
	nodeID := fs.String("node", "", "node id")
	_ = fs.Parse(args[1:])
	if *nodeID != "" {
		return printRequest(http.MethodGet, fmt.Sprintf("%s/v1/nodes/%s/builds", strings.TrimRight(*admin, "/"), *nodeID), nil)
	}
	return printRequest(http.MethodGet, strings.TrimRight(*admin, "/")+"/v1/builds", nil)
}

func printRequest(method, url string, body any) error {
	var rd io.Reader
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
		rd = &buf
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	if len(out) > 0 {
		_, _ = os.Stdout.Write(out)
	}
	return nil
}

var _ nodelink.Node = (*stubNode)(nil)
