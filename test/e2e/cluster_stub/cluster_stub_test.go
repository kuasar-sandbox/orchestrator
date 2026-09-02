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
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/router"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	testDomain             = "cluster.stub.local"
	testGroup              = "/cell/proj/app/g1"
	testAPISecret          = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	testMK                 = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	testTemplateRef        = "e2b-img-bWFuaWZlc3Q6Ly9hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh"
	testEnvdAccessToken    = "opaque-envd-access-token-from-node"
	testTrafficAccessToken = "opaque-traffic-access-token-from-node"
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

	reg              *registry.Registry
	links            *httptest.Server
	router           *httptest.Server
	node             *nodeStub
	apiKey           string
	apiHits          chan string
	dataHits         chan string
	apiNodeEndpoint  string
	dataNodeEndpoint string
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

	dataHits := make(chan string, 16)
	dataHTTP := newClusterDataTunnelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataHits <- r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	apiHits := make(chan string, 16)
	apiHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHits <- r.Method + " " + r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		sid := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": sid})
	}))
	t.Cleanup(apiHTTP.Close)

	node := startNodeStub(
		t, ctx, links.URL,
		strings.TrimPrefix(apiHTTP.URL, "http://"),
		strings.TrimPrefix(dataHTTP.URL, "http://"),
	)
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
		node: node, apiKey: apiKey, apiHits: apiHits, dataHits: dataHits,
		apiNodeEndpoint:  strings.TrimPrefix(apiHTTP.URL, "http://"),
		dataNodeEndpoint: strings.TrimPrefix(dataHTTP.URL, "http://"),
	}
	t.Cleanup(func() {
		cancel()
		_ = observer.Shutdown()
		_ = placerGroup.Shutdown()
		h.node.close()
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
		TemplateRef:   testTemplateRef,
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

func TestClusterStubCreateAndDataPlane(t *testing.T) {
	h := newHarness(t)
	const routeKey = "u1:s1"

	h.waitForNodeKeyCache(t, "n1")
	h.node.sendHeartbeat(t)
	keyPut := h.node.waitCommand(t, routesync.CmdKeyPut)
	if keyPut.APISecretFingerprint != fullFingerprint(t, testAPISecret) || keyPut.APISecret != testAPISecret ||
		keyPut.ManifestKeyFingerprint != fullFingerprint(t, testMK) || keyPut.ManifestKey != testMK {
		t.Fatalf("key_put did not carry the complete credential pair: %+v", keyPut)
	}

	req, _ := http.NewRequest(http.MethodPost, h.router.URL+"/sandboxes", nil)
	req.Host = "api." + testDomain
	req.Header.Set(router.HeaderGroup, testGroup)
	req.Header.Set(router.HeaderRouteKey, routeKey)
	req.Header.Set(router.HeaderAPIKey, h.apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		SandboxID          string `json:"sandboxID"`
		RouteKey           string `json:"routeKey"`
		EnvdAccessToken    string `json:"envdAccessToken"`
		TrafficAccessToken string `json:"trafficAccessToken"`
		ForwardAccessToken string `json:"forwardAccessToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d, want 201", resp.StatusCode)
	}
	if created.SandboxID == "" || created.RouteKey != routeKey ||
		created.EnvdAccessToken != testEnvdAccessToken ||
		created.TrafficAccessToken != testTrafficAccessToken || created.ForwardAccessToken == "" {
		t.Fatal("create response did not return SID and the explicit e2b/forward tokens")
	}
	create := h.node.waitCommand(t, routesync.CmdCreate)
	if create.Cluster == nil || create.Cluster.Group != testGroup || create.Cluster.RouteKey != routeKey ||
		create.Cluster.StableID != created.SandboxID ||
		create.SID != registry.EncodeNodeSandboxID(created.SandboxID, 0) || create.Profile != "e2b" ||
		create.TemplateRef != testTemplateRef || create.Config["from_group"] != "yes" {
		t.Fatalf("create command = %+v", create)
	}
	if created.SandboxID == create.SID {
		t.Fatalf("create exposed node command SID=%q as the stable public ID", create.SID)
	}
	if _, found := create.Config[clusterstate.ObjectMetadataKey]; found {
		t.Fatalf("create command leaked cluster context into user config: %+v", create.Config)
	}
	if create.APISecretFingerprint != fullFingerprint(t, testAPISecret) {
		t.Fatalf("create APISecretFingerprint=%q, want group API secret fingerprint", create.APISecretFingerprint)
	}
	reserved, err := h.reg.ReserveSandbox(h.ctx, registry.SandboxReserveRequest{
		Operation: registry.ReserveCreate,
		Group:     testGroup,
		RouteKey:  routeKey,
		APIKey:    h.apiKey,
	})
	if err != nil {
		t.Fatalf("ready Reserve: %v", err)
	}
	if reserved.Connect != nil {
		t.Fatalf("ready create Reserve returned a connect result: %+v", reserved.Connect)
	}
	reservedRoute := &reserved.Route
	serviceSecret, err := keys.DeriveServiceSecret(testAPISecret, create.Cluster.StableID)
	if err != nil {
		t.Fatal(err)
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, create.Cluster.StableID)
	if err != nil {
		t.Fatal(err)
	}
	if reservedRoute.RouteRevision <= 0 || reservedRoute.SandboxID != created.SandboxID || reservedRoute.NodeSandboxID != create.SID ||
		reservedRoute.StableID != create.Cluster.StableID || reservedRoute.APISecret != testAPISecret ||
		reservedRoute.APISecretFingerprint != fullFingerprint(t, testAPISecret) ||
		reservedRoute.ManifestKeyFingerprint != fullFingerprint(t, testMK) ||
		reservedRoute.ServiceSecret != serviceSecret || reservedRoute.EnvdAccessToken != testEnvdAccessToken ||
		reservedRoute.TrafficAccessToken != testTrafficAccessToken || reservedRoute.ForwardAccessToken != forwardAccessToken {
		t.Fatal("ready Reserve did not preserve explicit node-reported credentials")
	}
	if created.ForwardAccessToken != forwardAccessToken {
		t.Fatal("create response ForwardAccessToken did not match the node-reported route")
	}
	resolved, found, err := h.reg.ResolveSID(h.ctx, testGroup, routeKey, created.SandboxID)
	if err != nil || !found || resolved.SandboxID != created.SandboxID ||
		resolved.NodeSandboxID != create.SID || resolved.StableID != create.Cluster.StableID ||
		resolved.APISecret != testAPISecret || resolved.APISecretFingerprint != fullFingerprint(t, testAPISecret) ||
		resolved.ManifestKeyFingerprint != fullFingerprint(t, testMK) || resolved.ServiceSecret != serviceSecret ||
		resolved.EnvdAccessToken != testEnvdAccessToken || resolved.TrafficAccessToken != testTrafficAccessToken ||
		resolved.ForwardAccessToken != forwardAccessToken {
		t.Fatalf("resolved route did not preserve explicit node-reported credentials: found=%v err=%v", found, err)
	}

	controlReq, _ := http.NewRequest(http.MethodGet, h.router.URL+"/sandboxes/"+created.SandboxID, nil)
	controlReq.Host = "api." + testDomain
	controlReq.Header.Set(router.HeaderGroup, testGroup)
	controlReq.Header.Set(router.HeaderRouteKey, routeKey)
	controlReq.Header.Set(router.HeaderAPIKey, h.apiKey)
	controlResp, err := http.DefaultClient.Do(controlReq)
	if err != nil {
		t.Fatal(err)
	}
	controlResp.Body.Close()
	if controlResp.StatusCode != http.StatusOK {
		t.Fatalf("sandbox control status=%d, want 200", controlResp.StatusCode)
	}
	select {
	case hit := <-h.apiHits:
		if hit != "GET /sandboxes/"+create.SID {
			t.Fatalf("node API hit=%q, want rewritten node sandbox ID", hit)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("router never forwarded sandbox control to node API endpoint")
	}

	resp = h.doDataBySID(t, created.SandboxID, routeKey, created.EnvdAccessToken)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("sandbox-host data status=%d, want 204", resp.StatusCode)
	}
	select {
	case host := <-h.dataHits:
		if host != "49983-"+create.SID+"."+testDomain {
			t.Fatalf("forwarded Host=%q, want explicit sandbox host", host)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("router never forwarded to node data endpoint")
	}

	resp = h.doDataBySID(t, created.SandboxID, routeKey, created.EnvdAccessToken)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("second sandbox-host data status=%d, want 204", resp.StatusCode)
	}
	if got := h.node.countKind(routesync.CmdCreate); got != 1 {
		t.Fatalf("create commands=%d, want exactly the explicit create", got)
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

	req, _ := http.NewRequest(http.MethodPost, h.router.URL+"/v3/templates", strings.NewReader(`{"name":"tmpl","profile":"bare","cpuCount":2,"memoryMB":2048}`))
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
	if cmd.BuildResources == nil || cmd.BuildResources.CPU != 2000 || cmd.BuildResources.Memory != 2<<30 {
		t.Fatalf("build_register resources = %+v, want cpu=2000m memory=2GiB", cmd.BuildResources)
	}
}

func TestClusterStubBuildReconnectFullSyncRepairsLostDelete(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.router.URL+"/v3/templates", strings.NewReader(`{"name":"ttl-reconnect","profile":"bare","cpuCount":1,"memoryMB":1024}`))
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
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("build register status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	cmd := h.node.waitCommand(t, routesync.CmdBuildRegister)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if resolved, found := h.reg.ResolveBuild(h.ctx, testGroup, cmd.BuildID); found && resolved.BuildID == cmd.BuildID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Build %s projection did not become visible", cmd.BuildID)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Model a node-side TTL delete whose live BuildDelete was lost with the
	// connection. The replacement session's empty Build snapshot is authoritative.
	h.node.close()
	h.node = startNodeStub(t, h.ctx, h.links.URL, h.apiNodeEndpoint, h.dataNodeEndpoint)
	deadline = time.Now().Add(3 * time.Second)
	for {
		_, found := h.reg.ResolveBuild(h.ctx, testGroup, cmd.BuildID)
		_, refFound, refErr := h.reg.Stores().GetNodeBuildRef(h.ctx, "n1", cmd.BuildID)
		if !found && !refFound && refErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost BuildDelete did not converge after reconnect: projection=%v ref=%v ref_err=%v", found, refFound, refErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClusterStubUnownedReportDoesNotDeleteNodeSandbox(t *testing.T) {
	h := newHarness(t)

	h.node.sendRoute(t, routesync.RouteEntry{SandboxID: "sb-orphan", State: routesync.StateRunning})
	h.node.assertNoCommand(t, routesync.CmdDelete, "sb-orphan", 500*time.Millisecond)
}

func (h *harness) doDataBySID(t *testing.T, sid, routeKey, accessToken string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.router.URL+"/health", nil)
	req.Host = "49983-" + sid + "." + testDomain
	req.Header.Set(router.HeaderGroup, testGroup)
	req.Header.Set(router.HeaderRouteKey, routeKey)
	req.Header.Set(router.HeaderAccessTok, accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
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

func startNodeStub(t *testing.T, ctx context.Context, controlURL, apiEndpoint, dataEndpoint string) *nodeStub {
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
		BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{CPU: 4000, Memory: 4 << 30}},
		BuildExecutionCapacity:    &routesync.BuildAdmissionLimit{Resources: &routesync.BuildResources{CPU: 4000, Memory: 4 << 30}},
		APIEndpoint:               apiEndpoint,
		DataEndpoint:              dataEndpoint,
		RuntimeDigest:             "runtime-stub",
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
	stub.write(t, &routesync.Msg{Type: routesync.TypeBuildSyncBegin})
	stub.write(t, &routesync.Msg{Type: routesync.TypeBuildSyncEnd})
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
			n.sendRoute(n.t, routeForCommand(n.t, &cmd))
		case routesync.CmdBuildRegister:
			n.write(n.t, &routesync.Msg{Type: routesync.TypeBuildUpsert, Build: &routesync.BuildEvent{
				Kind: routesync.BuildUpsert, BuildID: cmd.BuildID, State: string(registry.BuildBuilding),
			}})
		}
	}
}

func routeForCommand(t *testing.T, cmd *routesync.Command) routesync.RouteEntry {
	t.Helper()
	stableID := cmd.SID
	if cmd.Cluster != nil && cmd.Cluster.StableID != "" {
		stableID = cmd.Cluster.StableID
	}
	serviceSecret, err := keys.DeriveServiceSecret(testAPISecret, stableID)
	if err != nil {
		t.Fatal(err)
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, stableID)
	if err != nil {
		t.Fatal(err)
	}
	entry := routesync.RouteEntry{
		SandboxID:              cmd.SID,
		TemplateID:             cmd.TemplateRef,
		Profile:                cmd.Profile,
		State:                  routesync.StateRunning,
		StableID:               stableID,
		APISecret:              testAPISecret,
		APISecretFingerprint:   fullFingerprint(t, testAPISecret),
		ManifestKeyFingerprint: fullFingerprint(t, testMK),
		ServiceSecret:          serviceSecret,
		ForwardAccessToken:     forwardAccessToken,
	}
	if cmd.Profile == "e2b" {
		entry.EnvdAccessToken = testEnvdAccessToken
		entry.TrafficAccessToken = testTrafficAccessToken
	}
	return entry
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
