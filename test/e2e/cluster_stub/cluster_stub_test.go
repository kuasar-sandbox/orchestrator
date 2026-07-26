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

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/router"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	testDomain          = "cluster.stub.local"
	testGroup           = "/cell/proj/app/g1"
	testAPISecret       = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	testMK              = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	testEnvdAccessToken = "opaque-envd-access-token-from-node"
)

func fullFingerprint(t *testing.T, secretHex string) string {
	t.Helper()
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(apikey.FullFingerprint(raw))
}

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
	stores := registry.NewStores()

	reg := registry.New(stores, nil, 5*time.Second, log)
	httpPlacer := registry.NewHTTPPlacer(reg, 1, 2*time.Second)
	reg.SetPlacer(httpPlacer)
	reg.SetPlacerMemberlistLabel("placer.default")
	reg.SetPlacerReadyLabel("registry.1.test")

	regHub := membergroup.NewHub()
	mux := http.NewServeMux()
	regHub.Mount(mux)
	reg.ServeRouteLink(mux)
	reg.ServePlacerLink(mux)
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	links := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	linkAddr := strings.TrimPrefix(links.URL, "http://")
	observer, err := membergroup.New(membergroup.Options{
		Label: "placer.default", Name: "observer.registry", Hub: regHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RoleObserver, ID: "observer.registry", Advertise: links.URL,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reg.SetPlacerSeedJoiner(func(ctx context.Context, id, label, advertise string) error {
		observer.AddSeed(id, advertise)
		_, err := observer.Join(id)
		return err
	})
	reg.SetPlacerPeerSource(func(label string) []registry.PlacerPeer {
		metas := observer.ReadyPlacers(label)
		out := make([]registry.PlacerPeer, 0, len(metas))
		for _, meta := range metas {
			out = append(out, registry.PlacerPeer{ID: meta.ID, Advertise: meta.Advertise, ReadyLabel: meta.ReadyLabel})
		}
		return out
	})

	groupDir := t.TempDir()
	writeStubGroup(t, groupDir)
	groupSource, err := placer.NewFileGroupSource("stub", groupDir)
	if err != nil {
		t.Fatal(err)
	}
	svc := placer.NewRemoteLinksWithGroups(
		[]placer.RegistryLink{{Name: "registry", BaseURL: "http://" + linkAddr, Client: http.DefaultClient}},
		groupSource, []placer.ImportSource{{SourceID: "stub", Importer: groupSource}},
		clustercfg.PlacementConfig{Candidates: 1, ZoneAdmitMax: "yellow"}, log,
	)
	placerHub := membergroup.NewHub()
	placerMux := http.NewServeMux()
	placerHub.Mount(placerMux)
	svc.ServePlacerLink(placerMux)
	placerSrv := httptest.NewServer(placerMux)
	placerGroup, err := membergroup.New(membergroup.Options{
		Label: "placer.default", Name: "s1", Hub: placerHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RolePlacer, ID: "s1", Advertise: placerSrv.URL,
			Ready: true, ReadyLabel: "registry.1.test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	svc.Start(ctx)
	go svc.RegisterLoop(ctx, "s1", placerSrv.URL, "placer.default")

	dataHits := make(chan *http.Request, 16)
	dataToken := make(chan string, 16)
	nodeHTTP := newClusterDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataHits <- r.Clone(r.Context())
		dataToken <- r.Header.Get(router.HeaderAccessTok)
		w.WriteHeader(http.StatusNoContent)
	}))

	node := startNodeStub(t, ctx, links.URL, strings.TrimPrefix(nodeHTTP.URL, "http://"))
	waitForPlacement(t, ctx, httpPlacer, links.URL)
	rt := router.New(linkAddr, testDomain, time.Minute, nil, log)
	rt.SetDataPlaneAuth("off")
	routerSrv := httptest.NewServer(rt.Handler())

	rawAPISecret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err := apikey.Mint(rawAPISecret)
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
		_ = placerGroup.Shutdown()
		node.close()
		placerSrv.Close()
		routerSrv.Close()
		links.Close()
	})
	return h
}

func newClusterDataTunnelServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			h(w, r)
			return
		}
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
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		inner, err := http.ReadRequest(br.Reader)
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		rec := httptest.NewRecorder()
		h(rec, inner)
		resp := rec.Result()
		defer resp.Body.Close()
		_ = resp.Write(conn)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeStubGroup(t *testing.T, dir string) {
	t.Helper()
	raw, err := json.Marshal(clusterstate.SandboxGroupRecord{
		Group: testGroup, ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testMK},
		APISecret:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
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
	t.Fatalf("placer placement did not become ready: %v; node_list=%s", last,
		readPlacerSnapshot(t, linksURL+registry.PlacerLinkNodeListWatchPath))
}

func readPlacerSnapshot(t *testing.T, u string) string {
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
	keyPut := h.node.waitCommand(t, routesync.CmdKeyPut)
	if keyPut.APISecretFingerprint != fullFingerprint(t, testAPISecret) || keyPut.APISecret != testAPISecret ||
		keyPut.ManifestKeyFingerprint != fullFingerprint(t, testMK) || keyPut.ManifestKey != testMK {
		t.Fatalf("key_put did not carry the complete credential pair: %+v", keyPut)
	}

	resp := h.doDataByKey(t, "u1:s1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("data by key status=%d, want 204", resp.StatusCode)
	}
	create := h.node.waitCommand(t, routesync.CmdCreate)
	location, err := clusterstate.ObjectLocationFromMetadata(create.Config)
	if err != nil {
		t.Fatalf("create command metadata: %v", err)
	}
	if location.Group != testGroup || location.RouteKey != "u1:s1" || create.TemplateRef != "tmpl-1" || create.Config["from_group"] != "yes" {
		t.Fatalf("create command = %+v", create)
	}
	if create.APISecretFingerprint != fullFingerprint(t, testAPISecret) {
		t.Fatalf("create APISecretFingerprint=%q, want group API secret fingerprint", create.APISecretFingerprint)
	}
	reserved, err := h.reg.ReserveSandbox(h.ctx, testGroup, "u1:s1", nil)
	if err != nil {
		t.Fatalf("ready Reserve: %v", err)
	}
	if reserved.AccessToken != testEnvdAccessToken {
		t.Fatalf("ready Reserve token=%q, want node-reported token", reserved.AccessToken)
	}
	resolved, found, err := h.reg.ResolveSID(h.ctx, testGroup, "u1:s1", create.SID)
	if err != nil || !found || resolved.AccessToken != testEnvdAccessToken {
		t.Fatalf("resolved route=%+v found=%v err=%v, want node-reported token", resolved, found, err)
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
		if tok != testEnvdAccessToken {
			t.Fatalf("node saw access token %q, want node-reported %q", tok, testEnvdAccessToken)
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
	for i := 0; i < 1000; i++ {
		node, found, err := h.reg.Stores().GetNode(h.ctx, nodeID)
		if err == nil && found && len(node.KeyPairs) == 1 &&
			node.KeyPairs[0].APISecretFingerprint == fullFingerprint(t, testAPISecret) &&
			node.KeyPairs[0].APISecret == testAPISecret &&
			node.KeyPairs[0].ManifestKeyFingerprint == fullFingerprint(t, testMK) &&
			node.KeyPairs[0].ManifestKey == testMK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s did not receive the complete credential pair cache", nodeID)
}

func TestClusterStubBuildRegister(t *testing.T) {
	h := newHarness(t)

	req, _ := http.NewRequest(http.MethodPost, h.router.URL+"/v3/templates", strings.NewReader(`{"name":"tmpl","profile":"bare"}`))
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
	var registered struct {
		Profile string `json:"profile"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registered); err != nil || registered.Profile != "bare" {
		t.Fatalf("build register response profile=%q err=%v", registered.Profile, err)
	}
	cmd := h.node.waitCommand(t, routesync.CmdBuildRegister)
	location, err := clusterstate.ObjectLocationFromMetadata(cmd.Config)
	if err != nil {
		t.Fatalf("build command metadata: %v", err)
	}
	if location.Group != testGroup || cmd.BuildID == "" || cmd.TemplateRef == "" ||
		cmd.APISecretFingerprint != fullFingerprint(t, testAPISecret) || cmd.Profile != "bare" {
		t.Fatalf("build_register command = %+v", cmd)
	}
}

func TestClusterStubUnownedReportDoesNotDeleteNodeSandbox(t *testing.T) {
	h := newHarness(t)

	h.node.sendRoute(t, routesync.RouteEntry{SandboxID: "sb-orphan", State: routesync.StateRunning})
	h.node.assertNoCommand(t, routesync.CmdDelete, "sb-orphan", 500*time.Millisecond)
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
				SandboxID: cmd.SID, State: routesync.StateRunning,
				AccessToken: testEnvdAccessToken, TemplateID: cmd.TemplateRef,
			})
		case routesync.CmdBuildRegister:
			n.write(n.t, &routesync.Msg{Type: routesync.TypeBuildEvent, Build: &routesync.BuildEvent{
				BuildID: cmd.BuildID, State: string(registry.BuildBuilding),
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

func (n *nodeStub) assertNoCommand(t *testing.T, kind, sid string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		n.mu.Lock()
		for _, cmd := range n.cmds {
			if cmd.Kind == kind && cmd.SID == sid {
				n.mu.Unlock()
				t.Fatalf("unexpected command kind=%s sid=%s", kind, sid)
			}
		}
		n.mu.Unlock()
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
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
