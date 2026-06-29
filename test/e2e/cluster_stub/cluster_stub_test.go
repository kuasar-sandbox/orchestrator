package cluster_stub_test

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/router"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
)

const (
	testDomain  = "cluster.stub.local"
	testGroup   = "/cell/proj/app/g1"
	testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	testMK      = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
)

type harness struct {
	ctx    context.Context
	cancel context.CancelFunc

	reg       *registry.Registry
	links     *httptest.Server
	router    *httptest.Server
	node      *nodeStub
	apiKey    string
	dataHits  chan *http.Request
	dataToken chan string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	kv := clusterstore.OpenMemory(1000)
	t.Cleanup(func() { kv.Close() })
	stores := registry.NewStores(kv)

	reg := registry.New(stores, nil, 5*time.Second, log)
	placer := registry.NewHTTPScalePlacer(reg, 1, 2*time.Second)
	reg.SetPlacer(placer)
	reg.SetScalerMemberlistLabel("scaler.default")
	reg.SetScaleReadyLabel("registry.1.test")
	go reg.RunKeyDistributor(ctx, time.Hour)

	regHub := membergroup.NewHub()
	mux := http.NewServeMux()
	regHub.Mount(mux)
	reg.ServeRouteLink(mux)
	reg.ServeScaleLink(mux)
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	links := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	linkAddr := strings.TrimPrefix(links.URL, "http://")
	observer, err := membergroup.New(membergroup.Options{
		Label: "scaler.default", Name: "observer.registry", Hub: regHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RoleObserver, ID: "observer.registry",
			APIAdvertise: links.URL, MemberlistAdvertise: links.URL,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reg.SetScalerSeedJoiner(func(ctx context.Context, id, label, advertise string) error {
		observer.AddSeed(id, advertise)
		_, err := observer.Join(id)
		return err
	})
	reg.SetScalerPeerSource(func(label string) []registry.ScalerPeer {
		metas := observer.ReadyScalers(label)
		out := make([]registry.ScalerPeer, 0, len(metas))
		for _, meta := range metas {
			out = append(out, registry.ScalerPeer{ID: meta.ID, Advertise: meta.APIAdvertise, ReadyLabel: meta.ReadyLabel})
		}
		return out
	})

	groupDir := t.TempDir()
	writeStubGroup(t, groupDir)
	groupSource, err := scaler.NewFileGroupSource("stub", groupDir)
	if err != nil {
		t.Fatal(err)
	}
	svc := scaler.NewRemoteLinksWithGroups(
		[]scaler.RegistryLink{{Name: "registry", BaseURL: "http://" + linkAddr, Client: http.DefaultClient}},
		groupSource, groupSource,
		clustercfg.PlacementConfig{Candidates: 1, ZoneAdmitMax: "yellow"}, 30, log,
	)
	scalerHub := membergroup.NewHub()
	scalerMux := http.NewServeMux()
	scalerHub.Mount(scalerMux)
	svc.ServeScaleLink(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	scalerGroup, err := membergroup.New(membergroup.Options{
		Label: "scaler.default", Name: "s1", Hub: scalerHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RoleScaler, ID: "s1",
			APIAdvertise: scalerSrv.URL, MemberlistAdvertise: scalerSrv.URL,
			Ready: true, ReadyLabel: "registry.1.test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	svc.Start(ctx)
	go svc.RegisterLoop(ctx, "s1", scalerSrv.URL, "scaler.default")

	dataHits := make(chan *http.Request, 16)
	dataToken := make(chan string, 16)
	nodeHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataHits <- r.Clone(r.Context())
		dataToken <- r.Header.Get(router.HeaderAccessTok)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(nodeHTTP.Close)

	node := startNodeStub(t, ctx, links.URL, strings.TrimPrefix(nodeHTTP.URL, "http://"))
	waitForPlacement(t, ctx, placer, links.URL)
	rt := router.New(linkAddr, testDomain, time.Minute, nil, log)
	rt.SetDataPlaneAuth("off")
	routerSrv := httptest.NewServer(rt.Handler())

	rawAuth, err := hex.DecodeString(testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err := apikey.Mint(rawAuth)
	if err != nil {
		t.Fatal(err)
	}

	h := &harness{
		ctx: ctx, cancel: cancel, reg: reg, links: links, router: routerSrv,
		node: node, apiKey: apiKey, dataHits: dataHits, dataToken: dataToken,
	}
	t.Cleanup(func() {
		cancel()
		_ = observer.Shutdown()
		_ = scalerGroup.Shutdown()
		node.close()
		scalerSrv.Close()
		routerSrv.Close()
		links.Close()
	})
	return h
}

func writeStubGroup(t *testing.T, dir string) {
	t.Helper()
	raw, err := json.Marshal(clusterstate.SandboxGroupRecord{
		Group: testGroup, ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testMK},
		AuthKey:       clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
		TemplateRef:   "tmpl-1",
		NodeSelectors: []map[string]string{{"pool": "stub"}},
		Config:        map[string]string{"from_group": "yes"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "group.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitForPlacement(t *testing.T, ctx context.Context, placer registry.Placer, linksURL string) {
	t.Helper()
	var last error
	for i := 0; i < 300; i++ {
		if placement, err := placer.Place(ctx, registry.PlaceRequest{Group: testGroup, RouteKey: "probe"}); err == nil && placement.NodeID == "n1" {
			return
		} else {
			last = err
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("scaler placement did not become ready: %v; node_list=%s", last,
		readScaleSnapshot(t, linksURL+registry.ScaleLinkNodeListWatchPath))
}

func readScaleSnapshot(t *testing.T, u string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	var frames []string
	for i := 0; i < 4; i++ {
		m, err := readViewFrame(resp.Body)
		if err != nil {
			frames = append(frames, "err="+err.Error())
			break
		}
		frames = append(frames, fmt.Sprintf("%s:%s", m.Type, strings.TrimSpace(string(m.Value))))
		if m.Type == "bookmark" {
			break
		}
	}
	return strings.Join(frames, "|")
}

func readViewFrame(r io.Reader) (*registry.ViewEvent, error) {
	var lenb [4]byte
	if _, err := io.ReadFull(r, lenb[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenb[:])
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("invalid view frame length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var ev registry.ViewEvent
	if err := json.Unmarshal(buf, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

func TestClusterStubReserveAndDataPlane(t *testing.T) {
	h := newHarness(t)

	h.waitForNodeKeyCache(t, "n1")
	h.node.sendHeartbeat(t)
	h.node.waitCommand(t, routesync.CmdKeyPut)

	resp := h.doDataByKey(t, "u1:s1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("data by key status=%d, want 204", resp.StatusCode)
	}
	create := h.node.waitCommand(t, routesync.CmdCreate)
	if create.Group != testGroup || create.RouteKey != "u1:s1" || create.TemplateRef != "tmpl-1" || create.Config["from_group"] != "yes" {
		t.Fatalf("create command = %+v", create)
	}
	if create.KeyFingerprint == "" || create.AccessToken == "" {
		t.Fatalf("create missing key fingerprint or access token: %+v", create)
	}

	select {
	case req := <-h.dataHits:
		if req.Host != "49983-"+create.SID+"."+testDomain {
			t.Fatalf("forwarded Host=%q, want synthesized sandbox host", req.Host)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("router never forwarded to node data endpoint")
	}
	select {
	case tok := <-h.dataToken:
		if tok != create.AccessToken {
			t.Fatalf("node saw access token %q, want %q", tok, create.AccessToken)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not receive access token")
	}

	resp = h.doDataByKey(t, "u1:s1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("second data by key status=%d, want 204", resp.StatusCode)
	}
	if got := h.node.countKind(routesync.CmdCreate); got != 1 {
		t.Fatalf("create commands=%d, want 1 (route cache should avoid Reserve)", got)
	}
}

func (h *harness) waitForNodeKeyCache(t *testing.T, nodeID string) {
	t.Helper()
	for i := 0; i < 300; i++ {
		node, found, err := h.reg.Stores().GetNode(h.ctx, nodeID)
		if err == nil && found && len(node.ManifestKeys) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s did not receive manifest key cache", nodeID)
}

func TestClusterStubBuildRegister(t *testing.T) {
	h := newHarness(t)

	req, _ := http.NewRequest(http.MethodPost, h.router.URL+"/v3/templates", strings.NewReader(`{"name":"tmpl"}`))
	req.Host = "api." + testDomain
	req.Header.Set(router.HeaderGroup, testGroup)
	req.Header.Set(router.HeaderAPIKey, h.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("build register status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	cmd := h.node.waitCommand(t, routesync.CmdBuildRegister)
	if cmd.Group != testGroup || cmd.BuildID == "" || cmd.TemplateRef == "" || cmd.KeyFingerprint == "" {
		t.Fatalf("build_register command = %+v", cmd)
	}
}

func TestClusterStubOrphanReportDeletesNodeSandbox(t *testing.T) {
	h := newHarness(t)

	h.node.sendRoute(t, routesync.RouteEntry{
		SandboxID: "sb-orphan", Group: testGroup, RouteKey: "missing", State: routesync.StateRunning,
	})
	cmd := h.node.waitCommand(t, routesync.CmdDelete)
	if cmd.SID != "sb-orphan" {
		t.Fatalf("orphan delete command=%+v, want sid sb-orphan", cmd)
	}
}

func (h *harness) doDataByKey(t *testing.T, routeKey string) *http.Response {
	t.Helper()
	var lastStatus int
	var lastBody string
	for i := 0; i < 200; i++ {
		req, _ := http.NewRequest(http.MethodGet, h.router.URL+"/health", nil)
		req.Host = "data." + testDomain
		req.Header.Set(router.HeaderGroup, testGroup)
		req.Header.Set(router.HeaderRouteKey, routeKey)
		req.Header.Set(router.HeaderAPIKey, h.apiKey)
		req.Header.Set("E2b-Sandbox-Port", "49983")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusNoContent {
			return resp
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastStatus = resp.StatusCode
		lastBody = strings.TrimSpace(string(body))
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("data by key never succeeded; last status=%d body=%s", lastStatus, lastBody)
	return nil
}

type nodeStub struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	pw     *io.PipeWriter
	resp   *http.Response

	writeMu sync.Mutex
	mu      sync.Mutex
	cmds    []*routesync.Command
	cmdCh   chan *routesync.Command
}

func startNodeStub(t *testing.T, ctx context.Context, controlURL, dataEndpoint string) *nodeStub {
	t.Helper()
	nctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	req, err := http.NewRequestWithContext(nctx, http.MethodPut, controlURL+routesync.NodeLinkPath, pr)
	if err != nil {
		t.Fatal(err)
	}
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	stub := &nodeStub{t: t, ctx: nctx, cancel: cancel, pw: pw, cmdCh: make(chan *routesync.Command, 64)}
	stub.write(t, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{
		NodeID: "n1", Labels: map[string]string{"pool": "stub"}, Capacity: 10,
		BuildCapacity: &routesync.BuildResources{CPU: 4000, Mem: 4 << 30},
		DataEndpoint:  dataEndpoint,
		RuntimeDigest: "runtime-stub",
	}})
	var resp *http.Response
	select {
	case resp = <-respCh:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("node-link did not connect")
	}
	stub.resp = resp
	hello, err := routesync.ReadMsg(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if hello.Type != routesync.TypeHello {
		t.Fatalf("node-link first response=%+v, want hello", hello)
	}
	go stub.readLoop()
	return stub
}

func (n *nodeStub) readLoop() {
	for {
		m, err := routesync.ReadMsg(n.resp.Body)
		if err != nil {
			return
		}
		if m.Type != routesync.TypeCommand || m.Cmd == nil {
			continue
		}
		cmd := *m.Cmd
		n.mu.Lock()
		n.cmds = append(n.cmds, &cmd)
		n.mu.Unlock()
		select {
		case n.cmdCh <- &cmd:
		default:
		}
		n.write(n.t, &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}})
		switch cmd.Kind {
		case routesync.CmdCreate, routesync.CmdConnect:
			n.sendRoute(n.t, routesync.RouteEntry{
				SandboxID: cmd.SID, Group: cmd.Group, RouteKey: cmd.RouteKey,
				State: routesync.StateRunning, AccessToken: cmd.AccessToken, TemplateID: cmd.TemplateRef,
			})
		case routesync.CmdBuildRegister:
			n.write(n.t, &routesync.Msg{Type: routesync.TypeBuildEvent, Build: &routesync.BuildEvent{
				Group: cmd.Group, BuildID: cmd.BuildID, State: string(registry.BuildBuilding),
			}})
		}
	}
}

func (n *nodeStub) waitCommand(t *testing.T, kind string) *routesync.Command {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		n.mu.Lock()
		for _, cmd := range n.cmds {
			if cmd.Kind == kind {
				cp := *cmd
				n.mu.Unlock()
				return &cp
			}
		}
		n.mu.Unlock()
		select {
		case cmd := <-n.cmdCh:
			if cmd.Kind == kind {
				cp := *cmd
				return &cp
			}
		case <-deadline:
			t.Fatalf("timed out waiting for command kind %s; saw %v", kind, n.commandKinds())
		}
	}
}

func (n *nodeStub) countKind(kind string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	var ncmd int
	for _, cmd := range n.cmds {
		if cmd.Kind == kind {
			ncmd++
		}
	}
	return ncmd
}

func (n *nodeStub) commandKinds() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.cmds))
	for _, cmd := range n.cmds {
		out = append(out, fmt.Sprintf("%s(%s)", cmd.Kind, cmd.SID))
	}
	return out
}

func (n *nodeStub) sendRoute(t *testing.T, entry routesync.RouteEntry) {
	t.Helper()
	n.write(t, &routesync.Msg{Type: routesync.TypeUpsert, Route: &entry})
}

func (n *nodeStub) sendHeartbeat(t *testing.T) {
	t.Helper()
	n.write(t, &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: &routesync.Heartbeat{Counts: 0}})
}

func (n *nodeStub) write(t *testing.T, msg *routesync.Msg) {
	t.Helper()
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if err := routesync.WriteMsg(n.pw, msg); err != nil {
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		t.Fatalf("node write %s: %v", msg.Type, err)
	}
}

func (n *nodeStub) close() {
	n.cancel()
	_ = n.pw.Close()
	if n.resp != nil {
		_ = n.resp.Body.Close()
	}
}
