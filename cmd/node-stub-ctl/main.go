// Command node-stub-ctl runs controllable node-link stubs for cluster e2e tests.
// It starts real node_link clients and exposes local admin/data HTTP endpoints,
// but does not launch microVMs.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/nodelink"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/transportauth"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var version = "0.1.0-dev"

const (
	defaultCreateDelay = 10 * time.Millisecond
	defaultBuildDelay  = 10 * time.Millisecond
	headerAccessToken  = "X-Access-Token"
	stubDiskSizeMB     = 1024
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
  node-stub-ctl serve --node-link https://HOST:PORT --tls-cert FILE --tls-key FILE --tls-ca FILE [--nodes N] [--node-prefix stub] [--label k=v]
  node-stub-ctl nodes|events|data-hits|commands --admin http://HOST:PORT
  node-stub-ctl node {restart-link|reboot-empty|crash|start|drain|undrain} NODE --admin http://HOST:PORT
  node-stub-ctl sandbox list --admin http://HOST:PORT [--node NODE]
  node-stub-ctl sandbox create --admin http://HOST:PORT --node NODE [--sid SID] [--metadata k=v ...]
  node-stub-ctl sandbox delete --admin http://HOST:PORT --node NODE --sid SID
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
	buildCPU := fs.Int("build-cpu", 4000, "build CPU capacity in milli-cores")
	buildMem := fs.Int64("build-mem-bytes", 4<<30, "build memory capacity in bytes")
	buildStorage := fs.Int64("build-storage-bytes", 0, "build storage capacity in bytes")
	heartbeat := fs.Duration("heartbeat", time.Second, "node heartbeat interval")
	runtimeDigest := fs.String("runtime-digest", "runtime-stub", "runtime digest reported by each node")
	createDelay := fs.Duration("create-delay", defaultCreateDelay, "default create-to-running delay")
	buildDelay := fs.Duration("build-delay", defaultBuildDelay, "default build event delay")
	stateDir := fs.String("state-dir", "", "durable simulated-node state directory")
	tlsCert := fs.String("tls-cert", "", "node mTLS certificate")
	tlsKey := fs.String("tls-key", "", "node mTLS private key")
	tlsCA := fs.String("tls-ca", "", "cluster CA certificate")
	var labels multiFlag
	fs.Var(&labels, "label", "label k=v applied to every simulated node; repeatable")
	_ = fs.Parse(args)
	if *nodeLink == "" {
		return fmt.Errorf("--node-link is required")
	}
	if *nodes <= 0 {
		return fmt.Errorf("--nodes must be positive")
	}
	if *tlsCert == "" || *tlsKey == "" || *tlsCA == "" {
		return errors.New("--tls-cert, --tls-key, and --tls-ca are required")
	}
	tlsConfig, err := (clustercfg.TLS{Cert: *tlsCert, Key: *tlsKey, CA: *tlsCA}).ClientConfig("")
	if err != nil {
		return fmt.Errorf("node-link TLS: %w", err)
	}
	serverTLS, err := (clustercfg.TLS{Cert: *tlsCert, Key: *tlsKey, CA: *tlsCA}).ServerConfig()
	if err != nil {
		return fmt.Errorf("node data TLS: %w", err)
	}
	baseLabels, err := parseLabels(labels)
	if err != nil {
		return err
	}
	removeStateDir := false
	if *stateDir == "" {
		*stateDir, err = os.MkdirTemp("", "kuasar-node-stub-")
		if err != nil {
			return err
		}
		removeStateDir = true
	} else if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		return err
	}
	if removeStateDir {
		defer os.RemoveAll(*stateDir)
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

	dataSrv := &http.Server{
		Handler:   transportauth.Middleware(transportauth.RoleRouter, http.HandlerFunc(svc.serveData)),
		TLSConfig: serverTLS,
	}
	adminSrv := &http.Server{Handler: svc}
	go func() {
		if err := dataSrv.ServeTLS(dataLn, "", ""); err != nil && err != http.ErrServerClosed {
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
		node, err := newStubNode(stubNodeOptions{
			ID:                fmt.Sprintf("%s-%d", *prefix, i),
			NodeLink:          *nodeLink,
			DataEndpoint:      svc.dataEndpoint,
			StatePath:         filepath.Join(*stateDir, fmt.Sprintf("%s-%d.db", *prefix, i)),
			TLSConfig:         tlsConfig,
			Labels:            labels,
			Capacity:          *capacity,
			BuildCapacity:     &routesync.BuildResources{CPU: *buildCPU, Mem: *buildMem, Storage: *buildStorage},
			RuntimeDigest:     *runtimeDigest,
			HeartbeatInterval: *heartbeat,
			CreateDelay:       *createDelay,
			BuildDelay:        *buildDelay,
		}, svc)
		if err != nil {
			return err
		}
		svc.addNode(node)
		node.start(ctx)
	}
	defer svc.close()

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

func (s *service) close() {
	for _, node := range s.sortedNodes() {
		node.close()
	}
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
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	if r.Method == http.MethodConnect {
		if status, kind, err := n.validateDataFence(r, sb); err != nil {
			w.Header().Set(proxypkg.HeaderProxyError, kind)
			http.Error(w, err.Error(), status)
			return
		}
		s.serveDataConnect(w, r, n, sb)
		return
	}
	w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorBadRequest)
	http.Error(w, "node data endpoint requires CONNECT", http.StatusBadRequest)
}

func (n *stubNode) validateDataFence(request *http.Request, sandbox *stubSandbox) (int, string, error) {
	n.mu.Lock()
	currentEpoch := n.nodeEpoch
	n.mu.Unlock()
	if request.Header.Get(proxypkg.HeaderNodeID) != n.ID {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("wrong node identity")
	}
	epoch, err := strconv.ParseUint(request.Header.Get(proxypkg.HeaderNodeEpoch), 10, 64)
	if err != nil || epoch == 0 || epoch != currentEpoch {
		return http.StatusConflict, proxypkg.ProxyErrorWrongNodeEpoch, errors.New("wrong node epoch")
	}
	if sandbox.State != routesync.StateRunning {
		return http.StatusConflict, proxypkg.ProxyErrorRouteInactive, errors.New("route is not READY")
	}
	if request.Header.Get(proxypkg.HeaderRegistryGeneration) == "" ||
		request.Header.Get(proxypkg.HeaderRegistryGeneration) != sandbox.RegistryGeneration ||
		request.Header.Get(proxypkg.HeaderBindingDigest) == "" ||
		request.Header.Get(proxypkg.HeaderBindingDigest) != sandbox.BindingDigest {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("wrong execution Binding")
	}
	if sandbox.AccessToken != "" && request.Header.Get(proxypkg.HeaderAccessToken) != sandbox.AccessToken {
		return http.StatusUnauthorized, proxypkg.ProxyErrorUnauthorized, errors.New("invalid access token")
	}
	return 0, "", nil
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
		NodeID: n.ID, SandboxID: sb.SID, Metadata: cloneStringMap(sb.Metadata),
		Host: inner.Host, Path: inner.URL.Path, Method: inner.Method, AccessToken: inner.Header.Get(headerAccessToken),
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
	nodeID := r.Header.Get(proxypkg.HeaderNodeID)
	n, found := s.getNode(nodeID)
	if !found {
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorWrongBinding)
		http.Error(w, "node not found", http.StatusConflict)
		return
	}
	kind := r.Header.Get(clusterstate.DirectHeaderExecutionKind)
	objectID := r.Header.Get(clusterstate.DirectHeaderObjectID)
	if status, proxyError, err := n.validateDirectControl(r, kind, objectID); err != nil {
		w.Header().Set(proxypkg.HeaderProxyError, proxyError)
		http.Error(w, err.Error(), status)
		return
	}
	if kind == "build" {
		switch {
		case r.Method == http.MethodPost && extractBuildID(r.URL.Path) == objectID && !strings.HasSuffix(r.URL.Path, "/status"):
			n.triggerBuild(w, r, objectID)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/files/"):
			http.Error(w, "stub files storage is not configured", http.StatusNotImplemented)
		default:
			n.writeBuildStatus(w, r, objectID)
		}
		return
	}
	if kind == "sandbox" {
		sb := n.getSandbox(objectID)
		if sb == nil {
			w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodDelete {
			if err := n.deleteFinalSandbox(r.Context(), objectID); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sb)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *stubNode) triggerBuild(w http.ResponseWriter, request *http.Request, buildID string) {
	raw, err := io.ReadAll(io.LimitReader(request.Body, 1<<20+1))
	if err != nil || len(raw) > 1<<20 {
		http.Error(w, "invalid Build trigger", http.StatusBadRequest)
		return
	}
	build, err := n.store.GetBuild(request.Context(), buildID)
	if err != nil || build == nil {
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
		http.Error(w, "build not found", http.StatusNotFound)
		return
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("kuasar-node-stub-build-trigger-v1\x00"))
	_, _ = hash.Write(raw)
	_, _ = hash.Write([]byte("\x00" + request.Header.Get("X-Kuasar-Pull-Token")))
	digest := hex.EncodeToString(hash.Sum(nil))
	accepted, err := n.store.CommitBuildTrigger(request.Context(), build, digest)
	if errors.Is(err, store.ErrBuildTriggerConflict) {
		http.Error(w, "Build ID already has a different trigger", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if accepted {
		n.mu.Lock()
		if current := n.builds[buildID]; current != nil {
			current.State = string(types.BuildWaiting)
		}
		n.mu.Unlock()
		n.svc.logEvent(n.ID, "build_trigger", map[string]string{"build_id": buildID})
	}
	w.WriteHeader(http.StatusAccepted)
}

func (n *stubNode) writeBuildStatus(w http.ResponseWriter, request *http.Request, buildID string) {
	build, err := n.store.GetBuild(request.Context(), buildID)
	if err != nil || build == nil {
		w.Header().Set(proxypkg.HeaderProxyError, proxypkg.ProxyErrorNotFound)
		http.Error(w, "build not found", http.StatusNotFound)
		return
	}
	templateID := build.TemplateID
	if build.PersistID != "" {
		templateID = build.PersistID
	}
	response := map[string]any{
		"templateID": templateID, "buildID": build.BuildID, "profile": build.Profile,
		"status": build.Status.SDKStatus(), "logs": []string{}, "logEntries": []any{},
	}
	if build.Reason != "" {
		response["reason"] = map[string]string{"message": build.Reason}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (n *stubNode) validateDirectControl(request *http.Request, kindName, objectID string) (int, string, error) {
	var kind clusterstate.ExecutionKind
	switch kindName {
	case "sandbox":
		kind = clusterstate.ExecutionKindSandbox
		if extractSandboxID(request.URL.Path) != objectID {
			return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("Sandbox URL identifies another execution")
		}
	case "build":
		kind = clusterstate.ExecutionKindBuild
		build, err := n.store.GetBuild(request.Context(), objectID)
		if err != nil || build == nil {
			return http.StatusNotFound, proxypkg.ProxyErrorNotFound, errors.Join(err, errors.New("Build object is missing"))
		}
		pathBuildID := extractBuildID(request.URL.Path)
		pathTemplateID := extractTemplateID(request.URL.Path)
		if pathTemplateID != build.TemplateID || pathBuildID != "" && pathBuildID != objectID {
			return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("Build URL identifies another execution")
		}
	default:
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("invalid execution kind")
	}
	record, err := n.store.GetNodeWorkflow(request.Context(), kind, objectID)
	if err != nil || record == nil {
		return http.StatusNotFound, proxypkg.ProxyErrorNotFound, errors.Join(err, errors.New("workflow is missing"))
	}
	n.mu.Lock()
	currentEpoch := n.nodeEpoch
	n.mu.Unlock()
	epoch, epochErr := strconv.ParseUint(request.Header.Get(proxypkg.HeaderNodeEpoch), 10, 64)
	if epochErr != nil || epoch != currentEpoch || record.NodeEpoch != currentEpoch {
		return http.StatusConflict, proxypkg.ProxyErrorWrongNodeEpoch, errors.New("wrong NodeEpoch")
	}
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, err
	}
	if record.BindingDigest != request.Header.Get(proxypkg.HeaderBindingDigest) ||
		binding.RegistryGeneration != request.Header.Get(proxypkg.HeaderRegistryGeneration) ||
		binding.NodeID != n.ID || binding.ObjectID != objectID ||
		binding.Group != request.Header.Get(clusterstate.DirectHeaderGroup) {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("wrong execution Binding")
	}
	if kind == clusterstate.ExecutionKindSandbox && binding.RouteKey != request.Header.Get(clusterstate.DirectHeaderRouteKey) {
		return http.StatusConflict, proxypkg.ProxyErrorWrongBinding, errors.New("wrong Route key")
	}
	return 0, "", nil
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
	ID                string
	NodeLink          string
	DataEndpoint      string
	StatePath         string
	TLSConfig         *tls.Config
	Labels            map[string]string
	Capacity          int
	BuildCapacity     *routesync.BuildResources
	RuntimeDigest     string
	HeartbeatInterval time.Duration
	CreateDelay       time.Duration
	BuildDelay        time.Duration
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
	nodeEpoch    uint64
	linkEndpoint string
	redirectTo   routesync.NodeLinkTarget
	sandboxes    map[string]*stubSandbox
	builds       map[string]*stubBuild
	commands     []commandLog
	cmdSeq       int64
	current      nodeexec.LocalSessionIdentity
	store        *store.Store
	authority    *nodeexec.Authority
	active       map[string]struct{}
}

func newStubNode(opts stubNodeOptions, svc *service) (*stubNode, error) {
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = time.Second
	}
	if opts.CreateDelay <= 0 {
		opts.CreateDelay = defaultCreateDelay
	}
	if opts.BuildDelay <= 0 {
		opts.BuildDelay = defaultBuildDelay
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		return nil, err
	}
	st, err := store.Open(opts.StatePath, box)
	if err != nil {
		return nil, err
	}
	if err := st.ConfigureSandboxSlotAdmission(uint64(opts.Capacity), 256); err != nil {
		st.Close()
		return nil, err
	}
	node := &stubNode{
		stubNodeOptions: opts,
		svc:             svc,
		log:             svc.log.With("node", opts.ID),
		sandboxes:       map[string]*stubSandbox{},
		builds:          map[string]*stubBuild{},
		nodeEpoch:       1,
		store:           st,
		active:          map[string]struct{}{},
	}
	authority, err := nodeexec.NewAuthority(
		st, st, node.currentIdentity, node.buildCapacity, node.buildObject, node.sandboxObject, node.sandboxDemand,
	)
	if err != nil {
		st.Close()
		return nil, err
	}
	node.authority = authority
	return node, nil
}

func (n *stubNode) close() {
	n.crash()
	if n.store != nil {
		_ = n.store.Close()
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
	if n.BuildCapacity != nil && n.BuildCapacity.Slots <= 0 {
		n.BuildCapacity.Slots = 2
	}
	identity := routesync.NodeRegister{
		NodeID: n.ID, EnrollmentID: "stub-enrollment-" + n.ID,
		LoadModelVersion: placement.LoadModelVersion,
		Labels:           cloneStringMap(n.Labels), Capabilities: stubCapabilities(n.Capacity, n.BuildCapacity),
		Capacity:      n.Capacity,
		BuildCapacity: cloneBuildResources(n.BuildCapacity), DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest,
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
	client := nodelink.New(n.NodeLink, dial, identity, n, n, n.store, n.HeartbeatInterval, n.TLSConfig, n.log)
	client.SetEventReplayLimits(64, 1<<20, 50*time.Millisecond)
	go client.Run(ctx)
	go n.runWorkflows(ctx)
	n.svc.logEvent(n.ID, "node_start", map[string]any{"session": n.session})
}

func stubCapabilities(sandboxCapacity int, buildCapacity *routesync.BuildResources) map[string]bool {
	capabilities := make(map[string]bool, 2)
	if sandboxCapacity > 0 {
		capabilities["sandbox"] = true
	}
	if buildCapacity != nil && buildCapacity.Slots > 0 {
		capabilities["build"] = true
	}
	return capabilities
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
	deleted := len(n.sandboxes)
	n.sandboxes = map[string]*stubSandbox{}
	n.builds = map[string]*stubBuild{}
	n.nodeEpoch++
	n.session = 0
	n.current = nodeexec.LocalSessionIdentity{}
	n.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	n.restartLink()
	n.svc.logEvent(n.ID, "node_reboot_empty", map[string]any{"deleted": deleted})
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

func (n *stubNode) NextSession(context.Context) (routesync.SessionTuple, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.session++
	n.current = nodeexec.LocalSessionIdentity{
		NodeID: n.ID, NodeEpoch: n.nodeEpoch, SessionSeq: uint64(n.session), DataEndpoint: n.DataEndpoint,
	}
	return routesync.SessionTuple{NodeEpoch: n.nodeEpoch, SessionSeq: uint64(n.session)}, nil
}

func (n *stubNode) currentIdentity(context.Context) (nodeexec.LocalSessionIdentity, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.current.Validate(); err != nil {
		return nodeexec.LocalSessionIdentity{}, err
	}
	return n.current, nil
}

func (n *stubNode) buildCapacity(context.Context) (nodeexec.BuildCapacity, string, error) {
	capacity := n.BuildCapacity
	if capacity == nil || capacity.Slots <= 0 {
		return nodeexec.BuildCapacity{}, "build_capacity_unavailable", nil
	}
	return nodeexec.BuildCapacity{
		Slots: uint64(capacity.Slots), CPU: uint64(max(capacity.CPU, 0)),
		Memory: uint64(max(capacity.Mem, 0)), Storage: uint64(max(capacity.Storage, 0)), QueueLimit: 256,
	}, "", nil
}

func (n *stubNode) buildObject(ctx context.Context, record nodeexec.DispatchRecord) (*types.Build, error) {
	spec, err := clusterstate.ParseBuildDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nil, err
	}
	lease, found, err := n.store.KeyLeaseByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	)
	if err != nil || !found {
		return nil, errors.Join(err, errors.New("exact node key lease is unavailable"))
	}
	register, err := api.ParseBuildRegisterEnvelope(spec.Request)
	if err != nil || register.Profile != spec.Profile || register.CPUCount != spec.CPUCount ||
		register.MemoryMB != spec.MemoryMB || !slices.Equal(stubNonEmpty(register.Name), spec.Names) ||
		!slices.Equal(register.Tags, spec.Aliases) || !reflect.DeepEqual(register.Metadata, spec.Metadata) {
		return nil, errors.Join(err, errors.New("Build request envelope does not match dispatch spec"))
	}
	return &types.Build{
		BuildID: record.ObjectID, TemplateID: spec.TemplateID,
		AuthKey: lease.AuthKey, ManifestKey: lease.ManifestKey,
		Profile: spec.Profile, CPUCount: spec.CPUCount, MemoryMB: spec.MemoryMB,
		Kind:  types.KindImg,
		Names: append([]string(nil), spec.Names...), Aliases: append([]string(nil), spec.Aliases...),
		Metadata: clusterstate.WithoutSystemMetadata(spec.Metadata), RegistryAuth: lease.RegistryAuth,
		Status: types.BuildRegistered, CreatedUnix: time.Now().Unix(),
	}, nil
}

func stubNonEmpty(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

func (n *stubNode) sandboxObject(ctx context.Context, record nodeexec.DispatchRecord) (*types.Sandbox, error) {
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nil, err
	}
	lease, found, err := n.store.KeyLeaseByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	)
	if err != nil || !found {
		return nil, errors.Join(err, errors.New("exact node key lease is unavailable"))
	}
	create, err := api.ParseSandboxCreateEnvelope(spec.Request)
	if err != nil || create.TemplateID != spec.TemplateRef || create.TimeoutSec != spec.TimeoutSeconds ||
		!reflect.DeepEqual(create.Metadata, spec.Config) {
		return nil, errors.Join(err, errors.New("Sandbox request envelope does not match dispatch spec"))
	}
	now := time.Now()
	deadline := now.Unix()
	if spec.TimeoutSeconds > 0 {
		deadline = now.Add(time.Duration(spec.TimeoutSeconds) * time.Second).Unix()
	}
	return &types.Sandbox{
		ID: record.ObjectID, TemplateID: spec.TemplateRef, State: types.StateStarting,
		AuthKey: lease.AuthKey, ManifestKey: lease.ManifestKey,
		EnvdAccessToken: spec.AccessToken, TrafficAccessToken: "traffic-" + record.ObjectID,
		Metadata: clusterstate.WithoutSystemMetadata(create.Metadata), Env: create.EnvVars,
		CreatedUnix: now.Unix(), DeadlineUnix: deadline,
	}, nil
}

func stubSandboxPresentation(
	record *nodeexec.WorkflowRecord,
	sandbox *types.Sandbox,
) (*clusterstate.SandboxPresentationV1, error) {
	if record == nil || sandbox == nil {
		return nil, errors.New("stub Sandbox presentation requires a workflow and object")
	}
	normalized, err := placement.ParseNormalizedDemand(record.NormalizedDemand)
	if err != nil || normalized.Sandbox == nil {
		return nil, errors.Join(err, errors.New("stub Sandbox presentation requires normalized demand"))
	}
	memoryBytes := max(
		normalized.Sandbox.StartupBudgetMemory,
		normalized.Sandbox.FloorMemory,
		normalized.Sandbox.AllocatableAtSnapshot,
		uint64(1<<30),
	)
	end := sandbox.DeadlineUnix
	if end < sandbox.CreatedUnix {
		end = sandbox.CreatedUnix
	}
	presentation := &clusterstate.SandboxPresentationV1{
		CPUCount: 1, MemoryMB: int(memoryBytes >> 20), DiskSizeMB: stubDiskSizeMB,
		EnvdVersion: api.EnvdVersion(sandbox.Profile()), StartedAt: sandbox.CreatedUnix, EndAt: end,
		Metadata: clusterstate.WithoutSystemMetadata(sandbox.Metadata),
	}
	return presentation, presentation.Validate()
}

func (n *stubNode) sandboxDemand(ctx context.Context, record nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nodectl.SandboxAdmissionDemand{}, err
	}
	if _, found, err := n.store.KeyLeaseByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	); err != nil || !found {
		return nodectl.SandboxAdmissionDemand{}, errors.Join(err, errors.New("exact node key lease is unavailable"))
	}
	normalized, err := placement.ParseNormalizedDemand(record.NormalizedDemand)
	if err != nil || normalized.Sandbox == nil {
		return nodectl.SandboxAdmissionDemand{}, errors.Join(err, errors.New("Sandbox normalized demand is missing"))
	}
	demand := normalized.Sandbox
	capacityMemory := max(demand.StartupBudgetMemory, demand.FloorMemory, demand.AllocatableAtSnapshot, uint64(1<<30))
	return nodectl.SandboxAdmissionDemand{
		SlotUnits: demand.SlotUnits, CapacityMemoryBytes: capacityMemory, CapacityCPU: 1,
		FloorMemoryBytes: demand.FloorMemory, StartupBudgetMemory: demand.StartupBudgetMemory,
		AllocatableAtSnapshot: demand.AllocatableAtSnapshot,
	}, nil
}

func (n *stubNode) runWorkflows(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := n.reconcileWorkflows(ctx); err != nil && ctx.Err() == nil {
			n.log.Warn("reconcile final stub workflows", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-n.authority.WorkWake():
		case <-n.store.WorkflowWake():
		case <-n.authority.SandboxAdmissionWake():
		case <-ticker.C:
		}
	}
}

func (n *stubNode) reconcileWorkflows(ctx context.Context) error {
	var joined error
	if err := n.authority.PromoteSandboxQueue(ctx); err != nil {
		joined = errors.Join(joined, err)
	}
	if _, err := n.authority.PromoteBuildQueue(ctx); err != nil {
		joined = errors.Join(joined, err)
	}
	for _, kind := range []clusterstate.ExecutionKind{clusterstate.ExecutionKindSandbox, clusterstate.ExecutionKindBuild} {
		records, err := n.authority.Launchable(ctx, kind, "", 256)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		for _, record := range records {
			n.startWorkflow(ctx, record)
		}
	}
	return joined
}

func (n *stubNode) startWorkflow(ctx context.Context, record *nodeexec.WorkflowRecord) {
	if record == nil {
		return
	}
	key := fmt.Sprintf("%d/%s", record.Kind, record.ObjectID)
	n.mu.Lock()
	if _, running := n.active[key]; running {
		n.mu.Unlock()
		return
	}
	n.active[key] = struct{}{}
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			delete(n.active, key)
			n.mu.Unlock()
		}()
		var err error
		if record.Kind == clusterstate.ExecutionKindSandbox {
			err = n.executeSandbox(ctx, record)
		} else {
			err = n.executeBuild(ctx, record)
		}
		if err != nil && ctx.Err() == nil {
			n.log.Warn("execute final stub workflow", "kind", record.Kind, "object", record.ObjectID, "err", err)
		}
	}()
}

func (n *stubNode) executeSandbox(ctx context.Context, record *nodeexec.WorkflowRecord) error {
	claimed, err := n.authority.ClaimSandbox(ctx, record)
	if err != nil {
		return err
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(claimed.DispatchSpec)
	if err != nil {
		return n.authority.FailSandbox(ctx, claimed, err.Error())
	}
	behavior := behaviorFromConfig(spec.Config, n.CreateDelay, n.BuildDelay)
	if behavior.CreateResult == "timeout" {
		return nil
	}
	if !sleepContext(ctx, behavior.CreateDelay) {
		return ctx.Err()
	}
	sandbox, err := n.store.Get(ctx, claimed.ObjectID)
	if err != nil || sandbox == nil {
		return errors.Join(err, errors.New("accepted Sandbox object is missing"))
	}
	sandbox.State = types.StateRunning
	if behavior.CreateResult == "reject" {
		terminal, commitErr := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{State: "ERROR", Reason: "stub create rejected"})
		if commitErr != nil {
			return commitErr
		}
		return n.authority.ReleaseSandboxResources(ctx, terminal, "stub create rejected")
	}
	presentation, err := stubSandboxPresentation(claimed, sandbox)
	if err != nil {
		return err
	}
	committed, err := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: spec.TargetPort, Presentation: presentation,
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.sandboxes[sandbox.ID] = &stubSandbox{
		SID: sandbox.ID, Metadata: cloneStringMap(sandbox.Metadata), State: routesync.StateRunning,
		TemplateID: sandbox.TemplateID, AccessToken: sandbox.EnvdAccessToken,
		TrafficAccessToken: sandbox.TrafficAccessToken, Behavior: behavior,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Binding: committed.OpaqueBinding,
		BindingDigest: committed.BindingDigest, RegistryGeneration: committed.LatestEvent.RegistryGeneration,
	}
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "sandbox_ready", map[string]string{"sid": sandbox.ID, "binding_digest": committed.BindingDigest})
	if behavior.Duration > 0 && sleepContext(ctx, behavior.Duration) {
		return n.deleteFinalSandbox(ctx, claimed.ObjectID)
	}
	return nil
}

func (n *stubNode) executeBuild(ctx context.Context, record *nodeexec.WorkflowRecord) error {
	claimed, err := n.authority.ClaimBuild(ctx, record)
	if err != nil {
		return err
	}
	build, err := n.store.GetBuild(ctx, claimed.ObjectID)
	if err != nil || build == nil {
		return errors.Join(err, errors.New("accepted Build object is missing"))
	}
	spec, err := clusterstate.ParseBuildDispatchSpec(claimed.DispatchSpec)
	if err != nil {
		return err
	}
	behavior := behaviorFromMap(spec.Metadata, n.CreateDelay, n.BuildDelay)
	if build.Status == types.BuildWaiting {
		if _, err := n.store.CommitClusterBuildState(ctx, build, nodeexec.EventUpdate{State: string(types.BuildBuilding)}); err != nil {
			return err
		}
	} else if build.Status != types.BuildBuilding {
		return fmt.Errorf("stub Build %s is not launchable from %s", build.BuildID, build.Status)
	}
	n.mu.Lock()
	n.builds[build.BuildID] = &stubBuild{
		BuildID: build.BuildID, Profile: string(build.Profile), Metadata: cloneStringMap(spec.Metadata),
		State: string(types.BuildBuilding), TemplateID: build.TemplateID,
		Resources: &routesync.BuildResources{Slots: int(claimed.BuildDemand.Slots), CPU: int(claimed.BuildDemand.CPU), Mem: int64(claimed.BuildDemand.Memory), Storage: int64(claimed.BuildDemand.Storage)},
		Behavior:  behavior, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	n.mu.Unlock()
	if behavior.BuildResult == "timeout" {
		<-ctx.Done()
		return ctx.Err()
	}
	if !sleepContext(ctx, behavior.BuildDelay) {
		return ctx.Err()
	}
	if behavior.BuildResult == "reject" || behavior.BuildResult == "error" {
		return n.setBuildState(build.BuildID, string(types.BuildError), "", "stub build rejected", true)
	}
	artifactHash := sha256.Sum256([]byte(build.BuildID))
	artifact := "e2b-img-" + hex.EncodeToString(artifactHash[:])
	return n.setBuildState(build.BuildID, string(types.BuildReady), artifact, "", true)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (n *stubNode) PlacementLoad(ctx context.Context) (*routesync.PlacementLoadSnapshot, error) {
	identity, err := n.currentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	sandboxUsage, err := n.store.SandboxSlotAdmissionUsage(ctx)
	if err != nil {
		return nil, err
	}
	buildUsage, _, err := n.store.BuildAdmissionUsage(ctx, identity.NodeID, identity.NodeEpoch)
	if err != nil {
		return nil, err
	}
	buildQueueDepth, err := n.store.BuildQueueDepth(ctx, identity.NodeID, identity.NodeEpoch)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	draining := n.draining
	n.mu.Unlock()
	return &routesync.PlacementLoadSnapshot{
		WaterZone: "green", Draining: draining,
		SandboxSlotUsed: sandboxUsage.SlotUsed, SandboxSlotHardLimit: uint64(max(n.Capacity, 0)),
		SandboxQueueDepth: sandboxUsage.QueueDepth, SandboxQueueLimit: 256, SandboxRateTokenAvailable: true,
		BuildSlotsUsed: buildUsage.Slots, BuildCPUUsed: buildUsage.CPU, BuildMemoryUsed: buildUsage.Memory,
		BuildStorageUsed: buildUsage.Storage, BuildQueueDepth: buildQueueDepth,
		BuildSlotHardLimit: uint64(max(n.BuildCapacity.Slots, 0)), BuildQueueLimit: 256,
		BuildRateTokenAvailable: true,
	}, nil
}

func (n *stubNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	if cmd == nil {
		return &routesync.CmdAck{Status: routesync.AckRejected, Outcome: routesync.DispatchUnknown, Reason: "empty command"}
	}
	n.recordCommand(cmd)
	switch cmd.Kind {
	case routesync.CmdKeyPut:
		local, err := n.currentIdentity(ctx)
		if err != nil || cmd.NodeEpoch != local.NodeEpoch || cmd.SessionSeq != local.SessionSeq {
			return finalStubReject(cmd, routesync.DispatchSessionMoved, errors.Join(err, errors.New("stale node-link tuple")))
		}
		if cmd.KeyLease == nil || cmd.KeyLease.Validate() != nil ||
			cmd.KeyLease.AuthKey.Type != routesync.KeyMaterialInline ||
			cmd.KeyLease.ManifestKey.Type != routesync.KeyMaterialInline ||
			cmd.KeyLease.ExpiresUnix <= time.Now().Unix() ||
			cmd.AuthKeyFingerprint != cmd.KeyLease.AuthKey.Fingerprint ||
			cmd.ManifestKeyFingerprint != cmd.KeyLease.ManifestKey.Fingerprint {
			return finalStubReject(cmd, routesync.DispatchConflict, errors.New("invalid inline key lease"))
		}
		registryAuth := ""
		switch cmd.KeyLease.RegistryAuth.Type {
		case "":
		case routesync.KeyMaterialInline:
			registryAuth = cmd.KeyLease.RegistryAuth.Value
		default:
			return finalStubReject(cmd, routesync.DispatchConflict, errors.New("stub cannot resolve registry auth reference"))
		}
		if _, err := n.store.PutKeyLease(ctx, store.KeyLease{
			Group: cmd.KeyLease.Group, AuthKey: cmd.KeyLease.AuthKey.Value,
			ManifestKey: cmd.KeyLease.ManifestKey.Value, RegistryAuth: registryAuth,
			Label: "cluster", ExpiresUnix: cmd.KeyLease.ExpiresUnix,
		}); err != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, err)
		}
		ref := routesync.NodeKeyLeaseRefV1{
			Version: routesync.NodeKeyLeaseVersionV1, Group: cmd.KeyLease.Group,
			AuthKeyFingerprint:     cmd.KeyLease.AuthKey.Fingerprint,
			ManifestKeyFingerprint: cmd.KeyLease.ManifestKey.Fingerprint,
		}
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, KeyLeaseRef: &ref}
	case routesync.CmdKeyDrop:
		local, err := n.currentIdentity(ctx)
		if err != nil || cmd.NodeEpoch != local.NodeEpoch || cmd.SessionSeq != local.SessionSeq {
			return finalStubReject(cmd, routesync.DispatchSessionMoved, errors.Join(err, errors.New("stale node-link tuple")))
		}
		if cmd.KeyLeaseRef == nil || cmd.KeyLeaseRef.Validate() != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, errors.New("invalid key lease reference"))
		}
		ref := *cmd.KeyLeaseRef
		if _, err := n.store.DropKeyLeaseRef(ctx, ref.Group, ref.AuthKeyFingerprint, ref.ManifestKeyFingerprint); err != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, err)
		}
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, KeyLeaseRef: &ref}
	case routesync.CmdSandboxAdmitDispatch, routesync.CmdBuildAdmitDispatch:
		local, err := n.currentIdentity(ctx)
		if err != nil {
			return finalStubReject(cmd, routesync.DispatchSessionMoved, err)
		}
		command, err := nodeexec.DispatchCommandFromWire(cmd, local)
		if err != nil {
			return finalStubReject(cmd, routesync.DispatchWrongBinding, err)
		}
		reply, err := n.authority.AdmitAndDispatch(ctx, command)
		if err != nil {
			return finalStubReject(cmd, routesync.DispatchUnknown, err)
		}
		return &routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Outcome: string(reply.Outcome), Reason: reply.Reason,
		}
	case routesync.CmdSandboxResume:
		if err := n.verifyFinalCommand(ctx, cmd, clusterstate.ExecutionKindSandbox, cmd.SID); err != nil {
			return finalStubReject(cmd, routesync.DispatchWrongBinding, err)
		}
		if err := n.resumeFinalSandbox(ctx, cmd.SID); err != nil {
			return finalStubReject(cmd, routesync.DispatchUnknown, err)
		}
		return finalStubAccept(cmd)
	case routesync.CmdSandboxDelete:
		if err := n.verifyFinalCommand(ctx, cmd, clusterstate.ExecutionKindSandbox, cmd.SID); err != nil {
			return finalStubReject(cmd, routesync.DispatchWrongBinding, err)
		}
		if err := n.deleteFinalSandbox(ctx, cmd.SID); err != nil {
			return finalStubReject(cmd, routesync.DispatchUnknown, err)
		}
		return finalStubAccept(cmd)
	case routesync.CmdRebindExecution:
		return n.rebindFinalExecution(ctx, cmd)
	case routesync.CmdAckRecoveryEvent:
		return n.ackFinalRecoveryEvent(ctx, cmd)
	case routesync.CmdCollectRecovery:
		page, err := n.collectFinalRecovery(ctx, cmd)
		if err != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, err)
		}
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, Recovery: page}
	case routesync.CmdFinalizeWorkflow:
		kind, objectID, err := commandExecutionIdentity(cmd)
		if err != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, err)
		}
		if err := n.authority.FinalizeWorkflow(ctx, kind, objectID, cmd.BindingDigest); err != nil {
			return finalStubReject(cmd, routesync.DispatchConflict, err)
		}
		return finalStubAccept(cmd)
	default:
		return finalStubReject(cmd, routesync.DispatchUnknown, fmt.Errorf("final node-link forbids command kind %q", cmd.Kind))
	}
}

func finalStubAccept(command *routesync.Command) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckAccepted}
}

func finalStubReject(command *routesync.Command, outcome string, err error) *routesync.CmdAck {
	return &routesync.CmdAck{
		CmdID: command.CmdID, Status: routesync.AckRejected, Outcome: outcome, Reason: err.Error(),
	}
}

func commandExecutionIdentity(command *routesync.Command) (clusterstate.ExecutionKind, string, error) {
	switch {
	case command.SID != "" && command.BuildID == "":
		return clusterstate.ExecutionKindSandbox, command.SID, nil
	case command.BuildID != "" && command.SID == "":
		return clusterstate.ExecutionKindBuild, command.BuildID, nil
	default:
		return 0, "", errors.New("command requires exactly one Sandbox or Build ID")
	}
}

func (n *stubNode) verifyFinalCommand(
	ctx context.Context,
	command *routesync.Command,
	kind clusterstate.ExecutionKind,
	objectID string,
) error {
	if objectID == "" {
		return errors.New("command object ID is required")
	}
	local, err := n.currentIdentity(ctx)
	if err != nil {
		return err
	}
	if command.NodeEpoch != local.NodeEpoch || command.SessionSeq != local.SessionSeq {
		return errors.New("command carries a stale node-link tuple")
	}
	record, err := n.store.GetNodeWorkflow(ctx, kind, objectID)
	if err != nil {
		return err
	}
	if record == nil || record.NodeID != local.NodeID || record.NodeEpoch != local.NodeEpoch ||
		record.BindingDigest != command.BindingDigest {
		return nodeexec.ErrWorkflowConflict
	}
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.RegistryGeneration != command.RegistryGeneration {
		return nodeexec.ErrWorkflowConflict
	}
	return nil
}

func (n *stubNode) resumeFinalSandbox(ctx context.Context, sandboxID string) error {
	record, err := n.store.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sandboxID)
	if err != nil || record == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	sandbox, err := n.store.Get(ctx, sandboxID)
	if err != nil || sandbox == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return err
	}
	presentation, err := stubSandboxPresentation(record, sandbox)
	if err != nil {
		return err
	}
	_, err = n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: spec.TargetPort, Presentation: presentation,
	})
	if err == nil {
		n.mu.Lock()
		if current := n.sandboxes[sandboxID]; current != nil {
			current.State = routesync.StateRunning
		}
		n.mu.Unlock()
	}
	return err
}

func (n *stubNode) deleteFinalSandbox(ctx context.Context, sandboxID string) error {
	record, err := n.store.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sandboxID)
	if err != nil || record == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	if record.ObjectState == "DELETED" {
		return nil
	}
	sandbox, err := n.store.Get(ctx, sandboxID)
	if err != nil || sandbox == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	terminal, err := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{State: "DELETED"})
	if err != nil {
		return err
	}
	if err := n.authority.ReleaseSandboxResources(ctx, terminal, "deleted"); err != nil {
		return err
	}
	n.mu.Lock()
	delete(n.sandboxes, sandboxID)
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "sandbox_deleted", map[string]string{"sid": sandboxID})
	return nil
}

func (n *stubNode) rebindFinalExecution(ctx context.Context, command *routesync.Command) *routesync.CmdAck {
	kind, objectID, err := commandExecutionIdentity(command)
	if err != nil {
		return finalStubReject(command, routesync.DispatchConflict, err)
	}
	local, err := n.currentIdentity(ctx)
	if err != nil || command.NodeEpoch != local.NodeEpoch || command.SessionSeq != local.SessionSeq {
		return finalStubReject(command, routesync.DispatchSessionMoved, errors.Join(err, errors.New("stale node-link tuple")))
	}
	replaced, err := n.store.CASExecutionBinding(ctx, kind, objectID, command.OldBindingDigest, command.Binding)
	if err != nil {
		return finalStubReject(command, routesync.DispatchWrongBinding, err)
	}
	if !replaced {
		return finalStubReject(command, routesync.DispatchWrongBinding, errors.New("current Binding does not match rebind CAS"))
	}
	record, err := n.store.GetNodeWorkflow(ctx, kind, objectID)
	if err != nil || record == nil {
		return finalStubReject(command, routesync.DispatchConflict, errors.Join(err, errors.New("rebound workflow is missing")))
	}
	if record.BindingDigest != command.BindingDigest || record.OpaqueBinding != command.Binding {
		return finalStubReject(command, routesync.DispatchWrongBinding, errors.New("rebound workflow differs from target Binding"))
	}
	fact, err := n.store.RecoveryExecutionFact(ctx, kind, objectID, command.RegistryGeneration)
	if err != nil {
		return finalStubReject(command, routesync.DispatchConflict, errors.Join(err, errors.New("rebound workflow has no durable object")))
	}
	if kind == clusterstate.ExecutionKindSandbox {
		n.mu.Lock()
		if sandbox := n.sandboxes[objectID]; sandbox != nil {
			sandbox.Binding = record.OpaqueBinding
			sandbox.BindingDigest = record.BindingDigest
			sandbox.RegistryGeneration = record.LatestEvent.RegistryGeneration
		}
		n.mu.Unlock()
	}
	object := fact.Object
	return &routesync.CmdAck{
		CmdID: command.CmdID, Status: routesync.AckAccepted, RebindObject: &object,
		RebindAdmissionState: fact.AdmissionState, RebindResourceClaimed: fact.ResourceClaimed,
	}
}

func (n *stubNode) ackFinalRecoveryEvent(ctx context.Context, command *routesync.Command) *routesync.CmdAck {
	kind, objectID, err := commandExecutionIdentity(command)
	if err != nil || command.EventAck == nil {
		return finalStubReject(command, routesync.DispatchConflict, errors.Join(err, errors.New("recovery event ACK command is incomplete")))
	}
	if kind != clusterstate.ExecutionKindSandbox {
		return finalStubReject(command, routesync.DispatchConflict, errors.New("Build recovery has no cluster event ACK"))
	}
	ack := *command.EventAck
	if ack.ObjectKind != "sandbox" || ack.ObjectID != objectID || ack.EventSeq == 0 {
		return finalStubReject(command, routesync.DispatchConflict, errors.New("recovery event ACK identifies another execution"))
	}
	if err := n.verifyFinalCommand(ctx, command, kind, objectID); err != nil {
		return finalStubReject(command, routesync.DispatchWrongBinding, err)
	}
	record, err := n.store.GetNodeWorkflow(ctx, kind, objectID)
	if err != nil || record == nil || record.LatestEvent == nil || record.EventSeq < ack.EventSeq ||
		record.LatestEvent.RegistryGeneration != command.RegistryGeneration ||
		record.LatestEvent.BindingDigest != command.BindingDigest {
		return finalStubReject(command, routesync.DispatchConflict, errors.Join(err, errors.New("recovery event ACK does not match the durable target event")))
	}
	local, err := n.currentIdentity(ctx)
	if err != nil {
		return finalStubReject(command, routesync.DispatchSessionMoved, err)
	}
	if err := n.store.AckExecutionEvent(ctx, local.NodeID, local.NodeEpoch, ack); err != nil {
		return finalStubReject(command, routesync.DispatchConflict, err)
	}
	object := recoveryObjectSnapshot(*record.LatestEvent)
	return &routesync.CmdAck{
		CmdID: command.CmdID, Status: routesync.AckAccepted, RebindObject: &object,
		RebindAdmissionState: string(record.AdmissionState), RebindResourceClaimed: record.ResourceClaimed,
	}
}

func recoveryObjectSnapshot(event routesync.ExecutionEvent) routesync.RecoveryObjectSnapshot {
	return routesync.RecoveryObjectSnapshot{
		ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, NodeID: event.NodeID, NodeEpoch: event.NodeEpoch,
		RegistryGeneration: event.RegistryGeneration, Binding: event.Binding, BindingDigest: event.BindingDigest,
		EventSeq: event.EventSeq, State: event.State, DataEndpoint: event.DataEndpoint,
		TargetPort: event.TargetPort, AccessToken: event.AccessToken,
		TrafficAccessToken: event.TrafficAccessToken, TemplateRef: event.TemplateRef,
		SnapshotRef: event.SnapshotRef, SnapshotLocation: event.SnapshotLocation,
		ArtifactRef: event.ArtifactRef, Reason: event.Reason,
		Presentation: cloneStubPresentation(event.Presentation),
	}
}

func cloneStubPresentation(source *clusterstate.SandboxPresentationV1) *clusterstate.SandboxPresentationV1 {
	if source == nil {
		return nil
	}
	clone := source.Clone()
	return &clone
}

func (n *stubNode) collectFinalRecovery(ctx context.Context, command *routesync.Command) (*routesync.RecoveryReportPage, error) {
	if command.Recovery == nil {
		return nil, errors.New("recovery report command is incomplete")
	}
	request := *command.Recovery
	if err := request.Validate(); err != nil {
		return nil, err
	}
	local, err := n.currentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if command.NodeEpoch != local.NodeEpoch || command.SessionSeq != local.SessionSeq {
		return nil, errors.New("recovery report command carries a stale node-link tuple")
	}
	objects, err := n.store.RecoveryExecutionReport(ctx, local.NodeID, local.NodeEpoch, request.SourceRegistryGeneration)
	if err != nil {
		return nil, err
	}
	digest, err := routesync.CanonicalRecoveryReportDigest(objects)
	if err != nil {
		return nil, err
	}
	if request.ExpectedReportDigest != "" && request.ExpectedReportDigest != digest {
		return nil, errors.New("durable recovery report changed between pages")
	}
	if request.Offset > uint64(len(objects)) {
		return nil, errors.New("recovery report offset is beyond the snapshot")
	}
	end := min(request.Offset+uint64(request.Limit), uint64(len(objects)))
	pageObjects := append([]routesync.RecoveryExecutionFact(nil), objects[request.Offset:end]...)
	for len(pageObjects) > 0 {
		encoded, encodeErr := json.Marshal(pageObjects)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if uint32(len(encoded)) <= request.MaxBytes {
			break
		}
		pageObjects = pageObjects[:len(pageObjects)-1]
		end--
	}
	if request.Offset < uint64(len(objects)) && len(pageObjects) == 0 {
		return nil, errors.New("one recovery object exceeds the requested page byte limit")
	}
	page := &routesync.RecoveryReportPage{
		RecoveryEpoch: request.RecoveryEpoch, SourceClusterID: request.SourceClusterID,
		SourceRegistryGeneration: request.SourceRegistryGeneration, SourceRegistryLayoutDigest: request.SourceRegistryLayoutDigest,
		TargetRegistryGeneration: request.TargetRegistryGeneration, TargetRegistryLayoutDigest: request.TargetRegistryLayoutDigest,
		NodeID: local.NodeID, NodeEpoch: local.NodeEpoch, SessionSeq: local.SessionSeq,
		ReportDigest: digest, TotalObjects: uint64(len(objects)), Offset: request.Offset,
		NextOffset: end, Complete: end == uint64(len(objects)), Objects: pageObjects,
	}
	if err := page.ValidateFor(request, local.NodeID, local.NodeEpoch, local.SessionSeq); err != nil {
		return nil, err
	}
	return page, nil
}

func (n *stubNode) recordCommand(cmd *routesync.Command) {
	n.mu.Lock()
	n.cmdSeq++
	log := commandLog{
		Seq: n.cmdSeq, Time: time.Now().UTC().Format(time.RFC3339Nano), NodeID: n.ID,
		CmdID: cmd.CmdID, Kind: cmd.Kind, SID: cmd.SID, BuildID: cmd.BuildID,
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
		SID: req.SID, Metadata: cloneStringMap(req.Metadata), State: state,
		TemplateID: req.TemplateID, AccessToken: req.AccessToken,
		TrafficAccessToken: "traffic-" + req.SID,
		Behavior:           beh, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	n.mu.Lock()
	n.sandboxes[sb.SID] = sb
	n.mu.Unlock()
	snap := sb.snapshot(n.ID)
	return &snap, nil
}

func (n *stubNode) deleteSandbox(sid string, _ bool) {
	n.mu.Lock()
	_, found := n.sandboxes[sid]
	delete(n.sandboxes, sid)
	n.mu.Unlock()
	n.svc.logEvent(n.ID, "sandbox_delete", map[string]any{"sid": sid, "found": found})
}

func (n *stubNode) setBuildState(buildID, state, templateID, reason string, publish bool) error {
	if state == "" {
		state = string(types.BuildBuilding)
	}
	build, err := n.store.GetBuild(context.Background(), buildID)
	if err != nil || build == nil {
		return errors.Join(err, errors.New("build not found"))
	}
	update := nodeexec.EventUpdate{State: state, Reason: reason}
	if state == string(types.BuildReady) {
		if templateID == "" {
			digest := sha256.Sum256([]byte(buildID))
			templateID = "e2b-img-" + hex.EncodeToString(digest[:])
		}
		build.PersistID = templateID
		build.Names = appendUniqueString(build.Names, templateID)
		build.Aliases = appendUniqueString(build.Aliases, templateID)
		update.ArtifactRef = templateID
	}
	_, err = n.store.CommitClusterBuildState(context.Background(), build, update)
	if err != nil {
		return err
	}
	n.mu.Lock()
	b := n.builds[buildID]
	if b == nil {
		b = &stubBuild{BuildID: buildID, Profile: string(build.Profile), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		n.builds[buildID] = b
	}
	displayTemplate := build.TemplateID
	if build.PersistID != "" {
		displayTemplate = build.PersistID
	}
	b.State, b.TemplateID, b.Reason = state, displayTemplate, reason
	n.mu.Unlock()
	if publish {
		n.svc.logEvent(n.ID, "build_state", map[string]string{"build_id": buildID, "state": state})
	}
	return nil
}

func appendUniqueString(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func (n *stubNode) getSandbox(sid string) *stubSandbox {
	n.mu.Lock()
	defer n.mu.Unlock()
	if sb := n.sandboxes[sid]; sb != nil {
		cp := *sb
		cp.Metadata = cloneStringMap(sb.Metadata)
		return &cp
	}
	return nil
}

func (n *stubNode) snapshot() nodeSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return nodeSnapshot{
		NodeID: n.ID, Online: n.online, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		NodeEpoch: n.nodeEpoch, SessionSeq: uint64(n.session),
		DataEndpoint: n.DataEndpoint, RuntimeDigest: n.RuntimeDigest, Draining: n.draining,
		LinkEndpoint: n.linkEndpoint, RedirectMemberID: n.redirectTo.MemberID, RedirectEndpoint: n.redirectTo.Endpoint,
		Sandboxes: n.sandboxSnapshotsLocked(), Builds: n.buildSnapshotsLocked(),
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

func (n *stubNode) commandCountsLocked() map[string]int {
	out := map[string]int{}
	for _, c := range n.commands {
		out[c.Kind]++
	}
	return out
}

type stubSandbox struct {
	SID                string            `json:"sid"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	State              string            `json:"state"`
	TemplateID         string            `json:"template_id,omitempty"`
	AccessToken        string            `json:"access_token,omitempty"`
	TrafficAccessToken string            `json:"traffic_access_token,omitempty"`
	Behavior           stubBehavior      `json:"behavior,omitempty"`
	CreatedAt          string            `json:"created_at,omitempty"`
	Binding            string            `json:"-"`
	BindingDigest      string            `json:"binding_digest,omitempty"`
	RegistryGeneration string            `json:"registry_generation,omitempty"`
}

func (s *stubSandbox) snapshot(nodeID string) sandboxSnapshot {
	return sandboxSnapshot{
		NodeID: nodeID, SID: s.SID, Metadata: cloneStringMap(s.Metadata), State: s.State,
		TemplateID: s.TemplateID, AccessToken: s.AccessToken, TrafficAccessToken: s.TrafficAccessToken,
		Behavior: s.Behavior, CreatedAt: s.CreatedAt,
		RegistryGeneration: s.RegistryGeneration, BindingDigest: s.BindingDigest,
	}
}

func (s *stubSandbox) response() (int, string) {
	status := s.Behavior.HTTPStatus
	if status == 0 {
		status = http.StatusNoContent
	}
	return status, s.Behavior.HTTPBody
}

type stubBuild struct {
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

type eventLog struct {
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
	NodeID string `json:"node_id,omitempty"`
	Type   string `json:"type"`
	Detail any    `json:"detail,omitempty"`
}

type commandLog struct {
	Seq     int64  `json:"seq"`
	Time    string `json:"time"`
	NodeID  string `json:"node_id"`
	CmdID   string `json:"cmd_id"`
	Kind    string `json:"kind"`
	SID     string `json:"sid,omitempty"`
	BuildID string `json:"build_id,omitempty"`
}

type dataHit struct {
	Seq         int64             `json:"seq"`
	Time        string            `json:"time"`
	NodeID      string            `json:"node_id"`
	SandboxID   string            `json:"sid"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Host        string            `json:"host"`
	Path        string            `json:"path"`
	Method      string            `json:"method"`
	AccessToken string            `json:"access_token,omitempty"`
}

type nodeSnapshot struct {
	NodeID           string            `json:"node_id"`
	Online           bool              `json:"online"`
	NodeEpoch        uint64            `json:"node_epoch"`
	SessionSeq       uint64            `json:"session_seq"`
	Labels           map[string]string `json:"labels,omitempty"`
	Capacity         int               `json:"capacity,omitempty"`
	DataEndpoint     string            `json:"data_endpoint,omitempty"`
	RuntimeDigest    string            `json:"runtime_digest,omitempty"`
	Draining         bool              `json:"draining,omitempty"`
	LinkEndpoint     string            `json:"link_endpoint,omitempty"`
	RedirectMemberID string            `json:"redirect_member_id,omitempty"`
	RedirectEndpoint string            `json:"redirect_endpoint,omitempty"`
	Sandboxes        []sandboxSnapshot `json:"sandboxes,omitempty"`
	Builds           []buildSnapshot   `json:"builds,omitempty"`
	CommandCounts    map[string]int    `json:"command_counts,omitempty"`
}

type sandboxSnapshot struct {
	NodeID             string            `json:"node_id,omitempty"`
	SID                string            `json:"sid"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	State              string            `json:"state"`
	RegistryGeneration string            `json:"registry_generation,omitempty"`
	BindingDigest      string            `json:"binding_digest,omitempty"`
	TemplateID         string            `json:"template_id,omitempty"`
	AccessToken        string            `json:"access_token,omitempty"`
	TrafficAccessToken string            `json:"traffic_access_token,omitempty"`
	Behavior           stubBehavior      `json:"behavior,omitempty"`
	CreatedAt          string            `json:"created_at,omitempty"`
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
	SID         string            `json:"sid"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	State       string            `json:"state,omitempty"`
	TemplateID  string            `json:"template_id,omitempty"`
	AccessToken string            `json:"access_token,omitempty"`
	Behavior    map[string]string `json:"behavior,omitempty"`
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

func extractTemplateID(path string) string {
	i := strings.Index(path, "/templates/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/templates/"):]
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
		return fmt.Errorf("usage: node-stub-ctl sandbox {list|create|delete}")
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
