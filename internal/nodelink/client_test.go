package nodelink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const (
	testAPISecret          = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	testManifestKey        = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	testEnvdAccessToken    = "node-link-envd-token"
	testTrafficAccessToken = "node-link-traffic-token"
)

func testFingerprint(secretHex string) string {
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type testPlacer struct{}

func (testPlacer) Place(ctx context.Context, req registry.PlaceRequest) (*registry.Placement, error) {
	return &registry.Placement{
		NodeID:               "n1",
		TemplateRef:          "e2b-img-bWFuaWZlc3Q6Ly9hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh",
		APISecretFingerprint: testFingerprint(testAPISecret),
	}, nil
}

// fakeNode implements Node: it streams its routes and, on a create command,
// "boots" the sandbox by adding a running route + emitting an upsert event — the
// same path a real node takes (the registry's Reserve waits on that route).
type fakeNode struct {
	mu                   sync.Mutex
	routes               map[string]routesync.RouteEntry
	keyPairs             map[string]routesync.Command
	createAPIFingerprint string
	createConfig         map[string]string
	events               chan routesync.Event
}

func newFakeNode() *fakeNode {
	return &fakeNode{
		routes:   map[string]routesync.RouteEntry{},
		keyPairs: map[string]routesync.Command{},
		events:   make(chan routesync.Event, 16),
	}
}

func (n *fakeNode) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, e := range n.routes {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}
func (n *fakeNode) Subscribe() (<-chan routesync.Event, func()) { return n.events, func() {} }
func (n *fakeNode) OnWake(ctx context.Context, sid string)      {}
func (n *fakeNode) Policy() routesync.Policy                    { return routesync.Policy{} }
func (n *fakeNode) Heartbeat() *routesync.Heartbeat             { return &routesync.Heartbeat{} }
func (n *fakeNode) BuildEvents() <-chan *routesync.BuildEvent   { return nil }

func (n *fakeNode) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
	var keyPair routesync.Command
	switch cmd.Kind {
	case routesync.CmdKeyPut:
		n.mu.Lock()
		n.keyPairs[cmd.APISecretFingerprint] = *cmd
		n.mu.Unlock()
		return ack
	case routesync.CmdCreate:
		n.mu.Lock()
		var installed bool
		keyPair, installed = n.keyPairs[cmd.APISecretFingerprint]
		n.createAPIFingerprint = cmd.APISecretFingerprint
		n.createConfig = cmd.Config
		n.mu.Unlock()
		if !installed {
			ack.Status = routesync.AckRejected
			ack.Reason = "credential pair not installed"
			return ack
		}
	default:
		return ack
	}
	authSandboxID := cmd.SID
	if cmd.Cluster != nil && cmd.Cluster.AuthSandboxID != "" {
		authSandboxID = cmd.Cluster.AuthSandboxID
	}
	serviceSecret, err := keys.DeriveServiceSecret(keyPair.APISecret, authSandboxID)
	if err != nil {
		ack.Status = routesync.AckRejected
		ack.Reason = "invalid service credentials"
		return ack
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, authSandboxID)
	if err != nil {
		ack.Status = routesync.AckRejected
		ack.Reason = "invalid forward credentials"
		return ack
	}
	e := routesync.RouteEntry{
		SandboxID:              cmd.SID,
		Profile:                cmd.Profile,
		State:                  routesync.StateRunning,
		AuthSandboxID:          authSandboxID,
		APISecret:              keyPair.APISecret,
		APISecretFingerprint:   keyPair.APISecretFingerprint,
		ManifestKeyFingerprint: keyPair.ManifestKeyFingerprint,
		ServiceSecret:          serviceSecret,
		ForwardAccessToken:     forwardAccessToken,
	}
	if cmd.Profile == "e2b" {
		e.EnvdAccessToken = testEnvdAccessToken
		e.TrafficAccessToken = testTrafficAccessToken
	}
	n.mu.Lock()
	n.routes[cmd.SID] = e
	n.mu.Unlock()
	n.events <- routesync.Event{Kind: routesync.TypeUpsert, Route: e}
	return ack
}

func (n *fakeNode) installedKeyPair(fingerprint string) (routesync.Command, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	cmd, ok := n.keyPairs[fingerprint]
	return cmd, ok
}

func (n *fakeNode) createFingerprint() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.createAPIFingerprint
}

func (n *fakeNode) createConfigSnapshot() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.createConfig
}

func TestNodeLinkReserveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New(registry.NewStores(), testPlacer{}, 5*time.Second, log)
	rawAPISecret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err := apikey.Mint(rawAPISecret)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	mux.HandleFunc(registry.PlacerLinkVerifyKeyPath, func(w http.ResponseWriter, req *http.Request) {
		parsed, parseErr := apikey.Parse(req.Header.Get("X-API-KEY"))
		if parseErr != nil || !apikey.Verify(parsed, rawAPISecret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	reg.SetPlacerPeerSource(func(string) []registry.PlacerPeer {
		return []registry.PlacerPeer{{ID: "test-placer", Advertise: srv.URL}}
	})
	addr := srv.Listener.Addr().String()

	node := newFakeNode()
	client := New(
		func(ctx context.Context) (net.Conn, error) { return net.Dial("tcp", addr) },
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, nil, log,
	)
	go client.Run(ctx)

	// Wait for the node to register over the channel.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, found, _ := reg.Stores().GetNode(ctx, "n1"); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never registered over node-link")
		}
		time.Sleep(10 * time.Millisecond)
	}

	apiFingerprint := testFingerprint(testAPISecret)
	manifestFingerprint := testFingerprint(testManifestKey)
	if err := reg.Stores().UpsertNodeKeyPair(ctx, "n1", clusterstate.NodeKeyPair{
		APISecretFingerprint:   apiFingerprint,
		APISecretType:          clusterstate.SecretInline,
		APISecret:              testAPISecret,
		ManifestKeyFingerprint: manifestFingerprint,
		ManifestKeyType:        clusterstate.SecretInline,
		ManifestKey:            testManifestKey,
		ExpiresUnix:            time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	keyDeadline := time.Now().Add(3 * time.Second)
	for {
		if keyPut, ok := node.installedKeyPair(apiFingerprint); ok {
			if keyPut.APISecret != testAPISecret || keyPut.ManifestKey != testManifestKey ||
				keyPut.ManifestKeyFingerprint != manifestFingerprint {
				t.Fatalf("distributed credential pair: %+v", keyPut)
			}
			break
		}
		if time.Now().After(keyDeadline) {
			t.Fatal("credential pair was not distributed over node-link")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var res *registry.ReserveResult
	reserveDeadline := time.Now().Add(3 * time.Second)
	for {
		res, err = reg.ReserveSandbox(ctx, registry.SandboxReserveRequest{
			Operation: registry.ReserveCreate,
			Group:     "/cell/proj/app/g1",
			RouteKey:  "u1:sess1",
			APIKey:    apiKey,
		})
		if err == nil {
			break
		}
		if !errors.Is(err, registry.ErrNodeGone) || time.Now().After(reserveDeadline) {
			t.Fatalf("reserve over node-link: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if res.Connect != nil {
		t.Fatalf("create reserve returned a connect result: %+v", res.Connect)
	}
	route := &res.Route
	if route.RouteRevision <= 0 || route.NodeID != "n1" || route.SandboxID == "" ||
		route.NodeSandboxID != registry.EncodeNodeSandboxID(route.SandboxID, 0) {
		t.Fatalf("reserve result: %+v", res)
	}
	if got := node.createFingerprint(); got != apiFingerprint {
		t.Fatalf("create APISecretFingerprint=%q, placement fingerprint=%q", got, apiFingerprint)
	}
	if route.AuthSandboxID != route.SandboxID || route.APISecret != testAPISecret ||
		route.APISecretFingerprint != apiFingerprint || route.ManifestKeyFingerprint != manifestFingerprint ||
		route.EnvdAccessToken != testEnvdAccessToken || route.TrafficAccessToken != testTrafficAccessToken {
		t.Fatal("reserve result did not preserve explicit route credentials")
	}
	if err := keys.VerifyForwardAccessToken(route.ForwardAccessToken, route.ServiceSecret, route.AuthSandboxID); err != nil {
		t.Fatalf("reserve ForwardAccessToken: %v", err)
	}
}

// TestNodeLinkReserveRoundTripPreservesMMDSConfig closes the gap between the
// registry-level ("does Reserve accept an mmds config key") and
// precheckCluster-level ("does precheckCluster correctly process a
// hand-built Command.Config") tests: it drives the SAME real wire as
// TestNodeLinkReserveRoundTrip (registry.ReserveSandbox -> node-link h2c
// stream -> nodelink.Client -> HandleCommand) with a Config map carrying
// kuasar-sandbox.mmds, and asserts the node process receives it byte-for-byte
// after a real JSON-over-the-wire round trip. It does not include the
// internal/router HTTP layer (covered separately by
// TestCreateSandboxMetadataMMDS*) or a real *orch.Orchestrator in place of
// fakeNode (no test in this repo stands one up; see mmds_routes_test.go's
// TestPrecheckClusterAppliesMMDSPolicyAndCanonicalizes for that hop).
func TestNodeLinkReserveRoundTripPreservesMMDSConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New(registry.NewStores(), testPlacer{}, 5*time.Second, log)
	rawAPISecret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err := apikey.Mint(rawAPISecret)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	mux.HandleFunc(registry.PlacerLinkVerifyKeyPath, func(w http.ResponseWriter, req *http.Request) {
		parsed, parseErr := apikey.Parse(req.Header.Get("X-API-KEY"))
		if parseErr != nil || !apikey.Verify(parsed, rawAPISecret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	reg.SetPlacerPeerSource(func(string) []registry.PlacerPeer {
		return []registry.PlacerPeer{{ID: "test-placer", Advertise: srv.URL}}
	})
	addr := srv.Listener.Addr().String()

	node := newFakeNode()
	client := New(
		func(ctx context.Context) (net.Conn, error) { return net.Dial("tcp", addr) },
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, nil, log,
	)
	go client.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, found, _ := reg.Stores().GetNode(ctx, "n1"); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never registered over node-link")
		}
		time.Sleep(10 * time.Millisecond)
	}

	apiFingerprint := testFingerprint(testAPISecret)
	manifestFingerprint := testFingerprint(testManifestKey)
	if err := reg.Stores().UpsertNodeKeyPair(ctx, "n1", clusterstate.NodeKeyPair{
		APISecretFingerprint:   apiFingerprint,
		APISecretType:          clusterstate.SecretInline,
		APISecret:              testAPISecret,
		ManifestKeyFingerprint: manifestFingerprint,
		ManifestKeyType:        clusterstate.SecretInline,
		ManifestKey:            testManifestKey,
		ExpiresUnix:            time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	keyDeadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := node.installedKeyPair(apiFingerprint); ok {
			break
		}
		if time.Now().After(keyDeadline) {
			t.Fatal("credential pair was not distributed over node-link")
		}
		time.Sleep(10 * time.Millisecond)
	}

	const mmdsSpec = `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`
	reserveDeadline := time.Now().Add(3 * time.Second)
	for {
		_, err = reg.ReserveSandbox(ctx, registry.SandboxReserveRequest{
			Operation: registry.ReserveCreate,
			Group:     "/cell/proj/app/g1",
			RouteKey:  "u1:sess1",
			APIKey:    apiKey,
					Config:    map[string]string{sandboxcfg.NsMMDS: mmdsSpec},
		})
		if err == nil {
			break
		}
		if !errors.Is(err, registry.ErrNodeGone) || time.Now().After(reserveDeadline) {
			t.Fatalf("reserve over node-link: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	got := node.createConfigSnapshot()
			if got[sandboxcfg.NsMMDS] != mmdsSpec {
		t.Fatalf("mmds specification did not survive the registry -> node-link wire round trip: got %+v", got)
	}
}

func TestNodeLinkClientFollowsRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New(registry.NewStores(), testPlacer{}, 5*time.Second, log)

	ownerMux := http.NewServeMux()
	ownerMux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	ownerSrv := httptest.NewServer(h2c.NewHandler(ownerMux, &http2.Server{}))
	defer ownerSrv.Close()

	ingressMux := http.NewServeMux()
	ingressMux.HandleFunc(routesync.NodeLinkPath, func(w http.ResponseWriter, req *http.Request) {
		first, err := routesync.ReadMsg(req.Body)
		if err != nil || first.Type != routesync.TypeNodeRegister || first.NodeReg == nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		if !first.NodeReg.AcceptRedirect {
			http.Error(w, "redirect not accepted", http.StatusConflict)
			return
		}
		if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{
			Version: routesync.Version,
			Redirect: &routesync.NodeLinkRedirect{Targets: []routesync.NodeLinkTarget{{
				MemberID: "owner", Endpoint: ownerSrv.URL,
			}}},
		}}); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	})
	ingressSrv := httptest.NewServer(h2c.NewHandler(ingressMux, &http2.Server{}))
	defer ingressSrv.Close()

	node := newFakeNode()
	client := NewWithEndpoint(
		ingressSrv.URL,
		func(ctx context.Context, endpoint string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		},
		routesync.NodeRegister{NodeID: "n1", DataEndpoint: "10.0.0.1:8443"},
		node, 50*time.Millisecond, nil, log, true,
	)
	go client.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if got, found, _ := reg.Stores().GetNode(ctx, "n1"); found && got.DataEndpoint == "10.0.0.1:8443" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never registered after redirect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNodeLinkOutboxPrioritizesHighPriorityFramesBeforeHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := make(chan *routesync.Msg, 2)
	highOut := make(chan *routesync.Msg, 1)
	hbUpdate := make(chan struct{}, 1)
	var hbMu sync.Mutex
	heartbeat := &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: &routesync.Heartbeat{Counts: 7}}
	latestHeartbeat := heartbeat

	highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{CmdID: "cmd-1", Status: routesync.AckAccepted}}
	hbUpdate <- struct{}{}
	go runNodeLinkOutbox(ctx, outbox, highOut, hbUpdate, &hbMu, &latestHeartbeat)

	first := receiveOutboxMsg(t, outbox)
	if first.Type != routesync.TypeCmdAck || first.Ack == nil || first.Ack.CmdID != "cmd-1" {
		t.Fatalf("first outbox msg=%+v, want cmd_ack before heartbeat", first)
	}
	second := receiveOutboxMsg(t, outbox)
	if second.Type != routesync.TypeHeartbeat || second.Beat == nil || second.Beat.Counts != 7 {
		t.Fatalf("second outbox msg=%+v, want latest heartbeat", second)
	}
}

func receiveOutboxMsg(t *testing.T, ch <-chan *routesync.Msg) *routesync.Msg {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for outbox msg")
	}
	return nil
}
