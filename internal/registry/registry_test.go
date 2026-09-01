package registry

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testRegWithBox(t *testing.T) *Registry {
	t.Helper()
	return New(NewStores(), nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

const testMK = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
const testAPISecret = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testAPIFingerprint = "5df404c22ba4e956e7ef06b6499f07ee62894450c25c928a7f5db26f6ea499a4"
const testEnvdAccessToken = "test-envd-access-token"
const testTrafficAccessToken = "test-traffic-access-token"
const testTemplateRef = "e2b-snp-bWFuaWZlc3Q6Ly9hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh"

func testE2BRoute(sandboxID, state string) routesync.RouteEntry {
	nodeSandboxID := EncodeNodeSandboxID(sandboxID, 0)
	serviceSecret, err := keys.DeriveServiceSecret(testAPISecret, sandboxID)
	if err != nil {
		panic(err)
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, sandboxID)
	if err != nil {
		panic(err)
	}
	return routesync.RouteEntry{
		SandboxID: nodeSandboxID, Profile: "e2b", State: state,
		StableID: sandboxID, APISecret: testAPISecret,
		APISecretFingerprint: testAPIFingerprint, ManifestKeyFingerprint: fullFingerprint(testMK),
		ServiceSecret: serviceSecret, EnvdAccessToken: testEnvdAccessToken,
		TrafficAccessToken: testTrafficAccessToken, ForwardAccessToken: forwardAccessToken,
	}
}

func testE2BSandboxRecord(group, routeKey, sid, nodeID string, state SandboxState) *SandboxRecord {
	route := testE2BRoute(sid, routesync.StateRunning)
	return &SandboxRecord{
		Group: group, RouteKey: routeKey, SandboxID: sid, NodeSandboxID: route.SandboxID,
		SandboxGeneration: 0, NextSandboxGeneration: 1,
		NodeID: nodeID, State: state, Profile: route.Profile, TemplateID: testTemplateRef,
		StableID: route.StableID, APISecret: route.APISecret,
		APISecretFingerprint: route.APISecretFingerprint, ManifestKeyFingerprint: route.ManifestKeyFingerprint,
		ServiceSecret: route.ServiceSecret, EnvdAccessToken: route.EnvdAccessToken,
		TrafficAccessToken: route.TrafficAccessToken, ForwardAccessToken: route.ForwardAccessToken,
	}
}

func testNodeSandboxRef(group, routeKey, sandboxID, profile, apiSecretFingerprint string) clusterstate.NodeSandboxRef {
	return clusterstate.NodeSandboxRef{
		Group: group, RouteKey: routeKey, SandboxID: sandboxID,
		SandboxGeneration: 0, NodeSandboxID: EncodeNodeSandboxID(sandboxID, 0),
		Profile: profile, APISecretFingerprint: apiSecretFingerprint,
	}
}

func testSandboxIdentityRecord(group, routeKey, sandboxID, nodeID string, state SandboxState) *SandboxRecord {
	return &SandboxRecord{
		Group: group, RouteKey: routeKey, SandboxID: sandboxID,
		SandboxGeneration: 0, NodeSandboxID: EncodeNodeSandboxID(sandboxID, 0), NextSandboxGeneration: 1,
		NodeID: nodeID, State: state, Profile: "e2b", APISecretFingerprint: testAPIFingerprint,
	}
}

func testRouteFromRecord(record *SandboxRecord, sid, state string) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID: sid, Profile: record.Profile, State: state,
		StableID: record.StableID, APISecret: record.APISecret,
		APISecretFingerprint:   record.APISecretFingerprint,
		ManifestKeyFingerprint: record.ManifestKeyFingerprint,
		ServiceSecret:          record.ServiceSecret, EnvdAccessToken: record.EnvdAccessToken,
		TrafficAccessToken: record.TrafficAccessToken,
		ForwardAccessToken: record.ForwardAccessToken,
	}
}

func updateHeartbeatAndRefreshKeyPairs(reg *Registry, ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	reg.updateHeartbeat(ctx, nodeID, hb)
	rec, found, err := reg.getNodeForLinkUpdate(ctx, nodeID)
	if err == nil && found {
		reg.refreshNodeKeyPairs(ctx, rec)
	}
}

type placementFunc func(context.Context, PlaceRequest) (*Placement, error)

func (f placementFunc) Place(ctx context.Context, req PlaceRequest) (*Placement, error) {
	placement, err := f(ctx, req)
	if err == nil && placement != nil && !req.Build && placement.TemplateRef == "" {
		placement.TemplateRef = testTemplateRef
	}
	return placement, err
}

func placementWithToken(nodeID string) Placer {
	return placementFunc(func(ctx context.Context, req PlaceRequest) (*Placement, error) {
		return &Placement{
			NodeID: nodeID, TemplateRef: testTemplateRef, Config: mergeConfig(map[string]string{"a": "1"}, req.Config),
			APISecretFingerprint: fullFingerprint(testAPISecret), ImageRepo: "repo", RegistryAuth: "auth-json",
		}, nil
	})
}

func TestSelectorPatchRefreshesNodeLinkKeyCache(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) {
		cmds = append(cmds, c)
		reg.ackCommand(&routesync.CmdAck{CmdID: c.CmdID, Status: routesync.AckAccepted})
	}})

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err != nil {
		t.Fatalf("push selector patch: %v", err)
	}

	if len(cmds) != 0 {
		t.Fatalf("selector patch should only update node_link cache, got commands %+v", cmds)
	}
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyPut ||
		cmds[0].APISecretFingerprint != fullFingerprint(testAPISecret) || cmds[0].APISecret != testAPISecret ||
		cmds[0].ManifestKeyFingerprint != fullFingerprint(testMK) || cmds[0].ManifestKey != testMK {
		t.Fatalf("expected heartbeat key_put with the group key, got %+v", cmds)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found || len(node.KeyPairs) != 1 {
		t.Fatalf("node key state found=%v err=%v node=%+v", found, err, node)
	}
	if node.KeyPairs[0].AckedExpiresUnix != cmds[0].ExpiresUnix || node.KeyPairs[0].AckedExpiresUnix <= time.Now().Unix() {
		t.Fatalf("acknowledged key lease not persisted in node_link: key=%+v cmd=%+v", node.KeyPairs[0], cmds[0])
	}

	cmds = nil
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	if len(cmds) != 0 {
		t.Fatalf("heartbeat resent manifest key before stored TTL expired: %+v", cmds)
	}
}

func TestHeartbeatRenewsManifestKeyBeforeLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	now := time.Now().Unix()
	key := testNodeKeyPair(testAPISecret, testMK, now+int64(keyLeaseTTL.Seconds()))
	key.AckedExpiresUnix = now + int64((keyRenewBefore / 2).Seconds())
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
		t.Fatal(err)
	}
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) {
		cmds = append(cmds, c)
		reg.ackCommand(&routesync.CmdAck{CmdID: c.CmdID, Status: routesync.AckAccepted})
	}})

	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})

	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdKeyPut || cmds[0].APISecretFingerprint != key.APISecretFingerprint {
		t.Fatalf("heartbeat did not renew key approaching expiry: %+v", cmds)
	}

	cmds = nil
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	node.KeyPairs[0].AckedExpiresUnix = now + int64((keyRenewBefore + time.Hour).Seconds())
	if err := reg.stores.PutNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	if len(cmds) != 0 {
		t.Fatalf("heartbeat renewed key before renew window: %+v", cmds)
	}
}

func TestHeartbeatRetriesRejectedManifestKey(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	key := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(keyLeaseTTL).Unix())
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
		t.Fatal(err)
	}
	sends := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		sends++
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "not installed"})
	}})

	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})

	got, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found || len(got.KeyPairs) != 1 {
		t.Fatalf("node found=%v err=%v state=%+v", found, err, got)
	}
	if sends != 2 || got.KeyPairs[0].AckedExpiresUnix != 0 {
		t.Fatalf("rejected key sends=%d state=%+v, want two attempts and no acknowledgement", sends, got.KeyPairs[0])
	}
}

func TestRejectedManifestKeyDoesNotBlockOtherKeys(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	expires := time.Now().Add(keyLeaseTTL).Unix()
	keys := []clusterstate.NodeKeyPair{
		testNodeKeyPair(testAPISecret, testMK, expires),
		testNodeKeyPair("1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222", expires),
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", KeyPairs: keys}); err != nil {
		t.Fatal(err)
	}
	var sends []string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		sends = append(sends, cmd.APISecretFingerprint)
		status := routesync.AckAccepted
		if cmd.APISecretFingerprint == keys[0].APISecretFingerprint {
			status = routesync.AckRejected
		}
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: status})
	}})

	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})

	got, _, _ := reg.stores.GetNode(ctx, "n1")
	seen := map[string]bool{}
	for _, fingerprint := range sends {
		seen[fingerprint] = true
	}
	if len(sends) != 2 || !seen[keys[0].APISecretFingerprint] || !seen[keys[1].APISecretFingerprint] {
		t.Fatalf("delivery order=%v, want both keys", sends)
	}
	acked := map[string]int64{}
	for _, pair := range got.KeyPairs {
		acked[pair.APISecretFingerprint] = pair.AckedExpiresUnix
	}
	if len(got.KeyPairs) != 2 || acked[keys[0].APISecretFingerprint] != 0 || acked[keys[1].APISecretFingerprint] != expires {
		t.Fatalf("key states=%+v, want rejected first pair and acknowledged second pair", got.KeyPairs)
	}
}

func TestHeartbeatRetriesManifestKeyAfterMissingAck(t *testing.T) {
	reg := testRegWithBox(t)
	key := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(keyLeaseTTL).Unix())
	if err := reg.stores.PutNode(context.Background(), &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
		t.Fatal(err)
	}
	sends := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { sends++ }})
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	updateHeartbeatAndRefreshKeyPairs(reg, waitCtx, "n1", &routesync.Heartbeat{})

	got, found, err := reg.stores.GetNode(context.Background(), "n1")
	if err != nil || !found || len(got.KeyPairs) != 1 {
		t.Fatalf("node found=%v err=%v state=%+v", found, err, got)
	}
	if got.KeyPairs[0].AckedExpiresUnix != 0 {
		t.Fatalf("missing ACK advanced acknowledged lease: %+v", got.KeyPairs[0])
	}
	reg.mu.Lock()
	waiters := len(reg.acks)
	reg.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("missing ACK left %d waiter(s)", waiters)
	}

	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		sends++
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})
	updateHeartbeatAndRefreshKeyPairs(reg, context.Background(), "n1", &routesync.Heartbeat{})
	got, _, _ = reg.stores.GetNode(context.Background(), "n1")
	if sends != 2 || got.KeyPairs[0].AckedExpiresUnix != key.ExpiresUnix {
		t.Fatalf("retry sends=%d state=%+v, want accepted second delivery", sends, got.KeyPairs[0])
	}
}

func TestMissingManifestKeyAckStopsCurrentRefreshBatch(t *testing.T) {
	reg := testRegWithBox(t)
	expires := time.Now().Add(keyLeaseTTL).Unix()
	keys := []clusterstate.NodeKeyPair{
		testNodeKeyPair(testAPISecret, testMK, expires),
		testNodeKeyPair("1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222", expires),
	}
	if err := reg.stores.PutNode(context.Background(), &NodeRecord{NodeID: "n1", KeyPairs: keys}); err != nil {
		t.Fatal(err)
	}
	sends := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { sends++ }})
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	updateHeartbeatAndRefreshKeyPairs(reg, waitCtx, "n1", &routesync.Heartbeat{})

	if sends != 1 {
		t.Fatalf("missing ACK sent %d keys, want one before ending the batch", sends)
	}
	got, _, _ := reg.stores.GetNode(context.Background(), "n1")
	if len(got.KeyPairs) != 2 || got.KeyPairs[0].AckedExpiresUnix != 0 || got.KeyPairs[1].AckedExpiresUnix != 0 {
		t.Fatalf("missing ACK advanced key state: %+v", got.KeyPairs)
	}
}

func TestHeartbeatRetriesManifestKeyAfterDisconnect(t *testing.T) {
	reg := testRegWithBox(t)
	key := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(keyLeaseTTL).Unix())
	if err := reg.stores.PutNode(context.Background(), &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
		t.Fatal(err)
	}
	linkCtx, disconnect := context.WithCancel(context.Background())
	sends := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) {
		sends++
		disconnect()
	}})
	updateHeartbeatAndRefreshKeyPairs(reg, linkCtx, "n1", &routesync.Heartbeat{})

	got, _, _ := reg.stores.GetNode(context.Background(), "n1")
	if got.KeyPairs[0].AckedExpiresUnix != 0 {
		t.Fatalf("disconnected delivery advanced acknowledged lease: %+v", got.KeyPairs[0])
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		sends++
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})
	updateHeartbeatAndRefreshKeyPairs(reg, context.Background(), "n1", &routesync.Heartbeat{})
	got, _, _ = reg.stores.GetNode(context.Background(), "n1")
	if sends != 2 || got.KeyPairs[0].AckedExpiresUnix != key.ExpiresUnix {
		t.Fatalf("reconnected retry sends=%d state=%+v", sends, got.KeyPairs[0])
	}
}

func TestStaleManifestKeyAckDoesNotOverwriteNewDesiredLease(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	initial := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(2*time.Hour).Unix())
	newer := initial
	newer.ExpiresUnix = initial.ExpiresUnix + int64(time.Hour.Seconds())
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{initial}}); err != nil {
		t.Fatal(err)
	}
	var expiries []int64
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		expiries = append(expiries, cmd.ExpiresUnix)
		if len(expiries) == 1 {
			if err := reg.stores.UpsertNodeKeyPair(ctx, "n1", newer); err != nil {
				t.Fatalf("update desired lease: %v", err)
			}
		}
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})

	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	got, _, _ := reg.stores.GetNode(ctx, "n1")
	if got.KeyPairs[0].ExpiresUnix != newer.ExpiresUnix || got.KeyPairs[0].AckedExpiresUnix != 0 {
		t.Fatalf("stale ACK changed newer desired lease: %+v", got.KeyPairs[0])
	}
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	got, _, _ = reg.stores.GetNode(ctx, "n1")
	if len(expiries) != 2 || expiries[0] != initial.ExpiresUnix || expiries[1] != newer.ExpiresUnix || got.KeyPairs[0].AckedExpiresUnix != newer.ExpiresUnix {
		t.Fatalf("delivery expiries=%v state=%+v", expiries, got.KeyPairs[0])
	}
}

func TestManifestKeyAckWaitDoesNotBlockOtherNode(t *testing.T) {
	reg := testRegWithBox(t)
	expires := time.Now().Add(keyLeaseTTL).Unix()
	for _, nodeID := range []string{"n1", "n2"} {
		apiSecret, manifestKey := testAPISecret, testMK
		if nodeID == "n2" {
			apiSecret = "1111111111111111111111111111111111111111111111111111111111111111"
			manifestKey = "2222222222222222222222222222222222222222222222222222222222222222"
		}
		key := testNodeKeyPair(apiSecret, manifestKey, expires)
		if err := reg.stores.PutNode(context.Background(), &NodeRecord{NodeID: nodeID, KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{})
	blockedCtx, unblock := context.WithCancel(context.Background())
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { close(started) }})
	blockedDone := make(chan struct{})
	go func() {
		updateHeartbeatAndRefreshKeyPairs(reg, blockedCtx, "n1", &routesync.Heartbeat{})
		close(blockedDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first node did not start key delivery")
	}

	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(cmd *routesync.Command) {
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})
	updateHeartbeatAndRefreshKeyPairs(reg, context.Background(), "n2", &routesync.Heartbeat{})
	got, _, _ := reg.stores.GetNode(context.Background(), "n2")
	if got.KeyPairs[0].AckedExpiresUnix != expires {
		t.Fatalf("second node was blocked by first node's ACK wait: %+v", got.KeyPairs[0])
	}
	unblock()
	select {
	case <-blockedDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled ACK wait did not return")
	}
}

func TestManifestKeyAckWaitDoesNotDelayHeartbeatState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg := testRegWithBox(t)
	key := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(keyLeaseTTL).Unix())
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{key}}); err != nil {
		t.Fatal(err)
	}
	keyStarted := make(chan struct{})
	var once sync.Once
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) {
		once.Do(func() { close(keyStarted) })
	}})
	heartbeats := make(chan *routesync.Heartbeat, 2)
	done := make(chan struct{})
	go func() {
		reg.runNodeHeartbeatUpdates(ctx, "n1", heartbeats)
		close(done)
	}()
	heartbeats <- &routesync.Heartbeat{Allocated: 1, Pool: 10}
	select {
	case <-keyStarted:
	case <-time.After(time.Second):
		t.Fatal("manifest-key delivery did not start")
	}

	heartbeats <- &routesync.Heartbeat{Allocated: 7, Pool: 10, Draining: true}
	deadline := time.Now().Add(time.Second)
	for {
		got, found, err := reg.stores.GetNode(context.Background(), "n1")
		if err == nil && found && got.Allocated == 7 && got.Draining {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat state did not advance while key ACK was pending: found=%v err=%v state=%+v", found, err, got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat updater did not stop")
	}
}

func TestDeleteSandboxRouteSendsNodeLinkCommand(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-1", "n1", StateReady)); err != nil {
		t.Fatal(err)
	}
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	deleted, err := reg.DeleteSandboxRoute(ctx, "/g", "rk", "sb-old")
	if err != nil || deleted {
		t.Fatalf("stale sid delete deleted=%v err=%v", deleted, err)
	}
	if len(cmds) != 0 {
		t.Fatalf("stale sid sent commands: %+v", cmds)
	}

	deleted, err = reg.DeleteSandboxRoute(ctx, "/g", "rk", "sb-1")
	if err != nil || !deleted {
		t.Fatalf("delete route deleted=%v err=%v", deleted, err)
	}
	if len(cmds) != 1 || cmds[0].Kind != routesync.CmdDelete || cmds[0].SID != EncodeNodeSandboxID("sb-1", 0) {
		t.Fatalf("delete command=%+v", cmds)
	}
}

func TestSelectorPatchRetriesFailedManifestKeyCacheWrite(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}}); err != nil {
		t.Fatal(err)
	}
	owner := &flakyKeyPairOwner{remoteLifecycleOwner: remoteLifecycleOwner{reg: reg}}
	reg.SetNodeOwner(owner)

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err == nil {
		t.Fatal("first selector patch should fail when node_link cache write fails")
	}
	if owner.putCalls != 1 {
		t.Fatalf("first patch put calls=%d, want 1", owner.putCalls)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 0 {
		t.Fatalf("failed key cache write was persisted: %+v", node.KeyPairs)
	}

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err != nil {
		t.Fatalf("second selector patch should retry successfully: %v", err)
	}
	if owner.putCalls != 2 {
		t.Fatalf("second patch put calls=%d, want retry after failed cache write", owner.putCalls)
	}
	node, found, err = reg.stores.GetNode(ctx, "n1")
	if err != nil || !found || len(node.KeyPairs) != 1 {
		t.Fatalf("retry did not persist key cache found=%v err=%v node=%+v", found, err, node)
	}
}

func TestSelectorPatchWritesManifestKeyTargetsConcurrently(t *testing.T) {
	reg := testRegWithBox(t)
	owner := &concurrentKeyPairOwner{want: 4, ready: make(chan struct{})}
	owner.reg = reg
	reg.SetNodeOwner(owner)

	if err := pushSelectorPatch(reg, "/g", []string{"n1", "n2", "n3", "n4"}, testMK); err != nil {
		t.Fatalf("selector patch fan-out: %v", err)
	}
	owner.mu.Lock()
	started := owner.started
	owner.mu.Unlock()
	if started != 4 {
		t.Fatalf("manifest key writes started=%d, want 4", started)
	}
}

func TestKeyDropOnLeave(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) {
		cmds = append(cmds, c)
		reg.ackCommand(&routesync.CmdAck{CmdID: c.CmdID, Status: routesync.AckAccepted})
	}})

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err != nil {
		t.Fatalf("push selector patch: %v", err)
	}
	updateHeartbeatAndRefreshKeyPairs(reg, ctx, "n1", &routesync.Heartbeat{})
	if err := pushSelectorPatch(reg, "/g", nil, ""); err != nil {
		t.Fatalf("empty selector patch: %v", err)
	}
	cmds = nil

	if len(cmds) != 0 {
		t.Fatalf("empty patch should not push key_drop; node TTL handles expiry, got %+v", cmds)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 1 {
		t.Fatalf("node_link key cache should remain until TTL expiry: %+v", node.KeyPairs)
	}
}

func TestSelectorPatchWritesOnlySelectedNodeKeyCache(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Labels: map[string]string{"zone": "east"}})
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n2", Labels: map[string]string{"zone": "west"}})
	cmds := map[string][]*routesync.Command{}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds["n1"] = append(cmds["n1"], c) }})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(c *routesync.Command) { cmds["n2"] = append(cmds["n2"], c) }})

	if err := pushSelectorPatch(reg, "/g", []string{"n2"}, testMK); err != nil {
		t.Fatalf("push selector patch: %v", err)
	}

	if len(cmds["n1"]) != 0 {
		t.Fatalf("selector-matching n1 got commands despite selector patch targeting n2: %+v", cmds["n1"])
	}
	if len(cmds["n2"]) != 0 {
		t.Fatalf("selector patch should not push n2 commands immediately: %+v", cmds["n2"])
	}
	n2, found, err := reg.stores.GetNode(ctx, "n2")
	if err != nil || !found || len(n2.KeyPairs) != 1 {
		t.Fatalf("n2 node_link cache=%+v found=%v err=%v", n2, found, err)
	}

	cmds = map[string][]*routesync.Command{}
	if err := pushSelectorPatch(reg, "/g", nil, ""); err != nil {
		t.Fatalf("empty selector patch: %v", err)
	}
	if len(cmds["n2"]) != 0 {
		t.Fatalf("empty selector patch should not push key_drop, commands=%+v", cmds["n2"])
	}
	n2, _, _ = reg.stores.GetNode(ctx, "n2")
	if len(n2.KeyPairs) != 1 {
		t.Fatalf("empty selector patch should leave n2 cache until TTL expiry: %+v", n2.KeyPairs)
	}
}

func TestSelectorPatchDoesNotDropRemovedTargetAndRotatesKey(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(c *routesync.Command) { cmds = append(cmds, c) }})

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err != nil {
		t.Fatalf("push selector patch: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("initial patch commands=%+v, want none", cmds)
	}

	cmds = nil
	if err := pushSelectorPatchPair(reg, "/g", []string{"n1"},
		"1111111111111111111111111111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222222222222222222222222222"); err != nil {
		t.Fatalf("rotate selector patch key: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("rotated key patch commands=%+v, want none", cmds)
	}
	node, _, _ := reg.stores.GetNode(ctx, "n1")
	if len(node.KeyPairs) != 2 {
		t.Fatalf("rotated key cache=%+v, want old and new keys until TTL expiry", node.KeyPairs)
	}

	cmds = nil
	if err := pushSelectorPatch(reg, "/g", nil, ""); err != nil {
		t.Fatalf("empty selector patch: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("removed selector patch target commands=%+v, want none", cmds)
	}
	node, _, _ = reg.stores.GetNode(ctx, "n1")
	if len(node.KeyPairs) != 2 {
		t.Fatalf("removed selector patch target cache=%+v, want unchanged until TTL expiry", node.KeyPairs)
	}
}

func TestSelectorPatchCachesKeysInNodeLink(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})

	if err := pushSelectorPatch(reg, "/g", []string{"n1"}, testMK); err != nil {
		t.Fatalf("push selector patch: %v", err)
	}

	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 1 || node.KeyPairs[0].APISecretFingerprint != fullFingerprint(testAPISecret) ||
		node.KeyPairs[0].APISecret != testAPISecret || node.KeyPairs[0].ManifestKey != testMK {
		t.Fatalf("node_link key-pair cache=%+v", node.KeyPairs)
	}

	registered := &routesync.NodeRegister{NodeID: "n1", Capacity: 10, DataEndpoint: "10.0.0.1:8443"}
	if _, err := reg.updateNodeRegister(ctx, registered); err != nil {
		t.Fatalf("updateNodeRegister: %v", err)
	}
	node, found, err = reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node after register found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 1 || node.KeyPairs[0].APISecretFingerprint != fullFingerprint(testAPISecret) {
		t.Fatalf("register cleared node_link key-pair cache: %+v", node.KeyPairs)
	}
}

func TestManifestKeyTTLExpiresNodeLinkCache(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})

	if err := reg.stores.UpsertNodeKeyPair(ctx, "n1", testNodeKeyPair(testAPISecret, testMK, time.Now().Unix()-1)); err != nil {
		t.Fatalf("upsert expired key: %v", err)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found || len(node.KeyPairs) != 1 {
		t.Fatalf("initial key cache=%+v found=%v err=%v", node, found, err)
	}

	if err := reg.stores.PruneExpiredNodeKeyPairs(ctx, "n1", time.Now().Unix()); err != nil {
		t.Fatalf("prune expired keys: %v", err)
	}
	node, found, err = reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 0 {
		t.Fatalf("expired manifest key kept node_link key cache: %+v", node.KeyPairs)
	}
}

func TestSelectorPatchRequiresCurrentImportSourceLease(t *testing.T) {
	reg := testReg(t)
	ctx := context.Background()
	resp, err := reg.acquireImportSourceLease(ctx, ImportSourceLeaseRequest{
		SourceID: "source-a", OwnerID: "s1", RunID: "run-1", TTLMillis: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := resp.Lease
	if lease.Term == 0 {
		t.Fatalf("lease term was not assigned: %+v", lease)
	}
	missing := selectorPatchForTest("/g", []string{"n1"}, testNodeKeyPair(testAPISecret, testMK, 0))
	if err := reg.applySelectorPatch(ctx, missing); !errors.Is(err, errMissingImportSourceLease) {
		t.Fatalf("missing source lease patch err=%v, want errMissingImportSourceLease", err)
	}
	stale := selectorPatchForTest("/g", []string{"n1"}, testNodeKeyPair(testAPISecret, testMK, 0))
	stale.ImportSourceID, stale.ImportOwnerID, stale.ImportRunID, stale.ImportTerm = "source-a", "s2", "run-2", lease.Term
	if err := reg.applySelectorPatch(ctx, stale); !errors.Is(err, errStaleImportSourceLease) {
		t.Fatalf("stale source owner patch err=%v, want errStaleImportSourceLease", err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	if node, found, err := reg.stores.GetNode(ctx, "n1"); err != nil || !found || len(node.KeyPairs) != 0 {
		t.Fatalf("stale patch changed node_link key cache: node=%+v found=%v err=%v", node, found, err)
	}
	pair := testNodeKeyPair(testAPISecret, testMK, 0)
	good := selectorPatchForTest("/g", []string{"n1"}, pair)
	good.ImportSourceID, good.ImportOwnerID, good.ImportRunID, good.ImportTerm = "source-a", lease.OwnerID, lease.RunID, lease.Term
	if err := reg.applySelectorPatch(ctx, good); err != nil {
		t.Fatalf("current source owner patch was rejected: %v", err)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found || len(node.KeyPairs) != 1 ||
		node.KeyPairs[0].APISecretFingerprint != pair.APISecretFingerprint || node.KeyPairs[0].ManifestKey != testMK {
		t.Fatalf("current patch not applied to node_link: node=%+v found=%v err=%v", node, found, err)
	}
}

func testReg(t *testing.T) *Registry {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := New(NewStores(), nil, 5*time.Second, log)
	enableTestCreateAuth(t, reg)
	return reg
}

func testCreateReserve(group, routeKey string, config map[string]string) SandboxReserveRequest {
	return SandboxReserveRequest{
		Operation: ReserveCreate, Group: group, RouteKey: routeKey,
		APIKey: testAPIKeyValue(), Config: config,
	}
}

func testAPIKeyValue() string {
	secret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		panic(err)
	}
	apiKey, err := apikey.Mint(secret)
	if err != nil {
		panic(err)
	}
	return apiKey
}

func enableTestCreateAuth(t *testing.T, reg *Registry) {
	t.Helper()
	secret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		parsed, parseErr := apikey.Parse(req.Header.Get("X-API-KEY"))
		if parseErr != nil || !apikey.Verify(parsed, secret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	reg.SetPlacerPeerSource(func(string) []PlacerPeer {
		return []PlacerPeer{{ID: "test-placer", Advertise: server.URL}}
	})
}

func pushSelectorPatch(reg *Registry, group string, nodes []string, manifestKey string) error {
	if manifestKey == "" {
		return pushSelectorPatchPair(reg, group, nodes, "", "")
	}
	return pushSelectorPatchPair(reg, group, nodes, testAPISecret, manifestKey)
}

func pushSelectorPatchPair(reg *Registry, group string, nodes []string, apiSecret, manifestKey string) error {
	ctx := context.Background()
	resp, err := reg.acquireImportSourceLease(ctx, ImportSourceLeaseRequest{
		SourceID: "test-source", OwnerID: "test-placer", RunID: "test-run", TTLMillis: 1000,
	})
	if err != nil {
		return err
	}
	patch := &routesync.SelectorPatch{Group: group, NodeIDs: nodes}
	if apiSecret != "" || manifestKey != "" {
		patch = selectorPatchForTest(group, nodes, testNodeKeyPair(apiSecret, manifestKey, 0))
	}
	patch.ImportSourceID = resp.Lease.SourceID
	patch.ImportOwnerID = resp.Lease.OwnerID
	patch.ImportRunID = resp.Lease.RunID
	patch.ImportTerm = resp.Lease.Term
	return reg.applySelectorPatch(ctx, patch)
}

func selectorPatchForTest(group string, nodes []string, pair clusterstate.NodeKeyPair) *routesync.SelectorPatch {
	return &routesync.SelectorPatch{
		Group: group, NodeIDs: nodes,
		APISecretFingerprint: pair.APISecretFingerprint, APISecretType: pair.APISecretType,
		APISecret: pair.APISecret, APISecretRef: pair.APISecretRef,
		ManifestKeyFingerprint: pair.ManifestKeyFingerprint, ManifestKeyType: pair.ManifestKeyType,
		ManifestKey: pair.ManifestKey, ManifestKeyRef: pair.ManifestKeyRef,
	}
}

// fakeConn implements nodeConn; its onCmd hook lets a test simulate the node
// reacting to a command (e.g. reporting the sandbox running).
type fakeConn struct {
	nodeID string
	onCmd  func(*routesync.Command)
	err    error
}

func (c *fakeConn) id() string { return c.nodeID }
func (c *fakeConn) send(cmd *routesync.Command) error {
	if c.err != nil {
		return c.err
	}
	if c.onCmd != nil {
		c.onCmd(cmd)
	}
	return nil
}

type remoteLifecycleOwner struct {
	reg      *Registry
	commands int
}

type remoteRouteWriteOwner struct {
	node         *NodeRecord
	onCreate     func(*routesync.Command)
	connectedErr error
	runtimeErr   error
	ackErr       error
	commands     int
}

func (o *remoteRouteWriteOwner) PutKeyPair(context.Context, string, clusterstate.NodeKeyPair) error {
	return nil
}

func (o *remoteRouteWriteOwner) DropKeyPair(context.Context, string, string) error { return nil }

func (o *remoteRouteWriteOwner) AdmitBuild(context.Context, string, string, *routesync.BuildResources) bool {
	return true
}

func (o *remoteRouteWriteOwner) ReleaseBuild(context.Context, string, string) {}

func (o *remoteRouteWriteOwner) Connected(context.Context, string) error {
	if o.connectedErr != nil {
		return o.connectedErr
	}
	if o.node == nil {
		return ErrNodeGone
	}
	return nil
}

func (o *remoteRouteWriteOwner) Runtime(context.Context, string) (*NodeRecord, bool, error) {
	if o.runtimeErr != nil {
		return nil, false, o.runtimeErr
	}
	return o.node, o.node != nil, nil
}

func (o *remoteRouteWriteOwner) DeleteSandbox(context.Context, string, string, string) error {
	return nil
}

func (o *remoteRouteWriteOwner) SendCommand(context.Context, string, *routesync.Command) error {
	return nil
}

func (o *remoteRouteWriteOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	o.commands++
	if o.onCreate != nil && cmd != nil && cmd.Kind == routesync.CmdCreate {
		o.onCreate(cmd)
	}
	if o.ackErr != nil {
		return nil, o.ackErr
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}

type flakyKeyPairOwner struct {
	remoteLifecycleOwner
	putCalls int
}

func (o *flakyKeyPairOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	o.putCalls++
	if o.putCalls == 1 {
		return errors.New("temporary key cache failure")
	}
	return o.reg.stores.UpsertNodeKeyPair(ctx, nodeID, pair)
}

type concurrentKeyPairOwner struct {
	remoteLifecycleOwner
	mu      sync.Mutex
	started int
	want    int
	ready   chan struct{}
	once    sync.Once
}

func (o *concurrentKeyPairOwner) PutKeyPair(context.Context, string, clusterstate.NodeKeyPair) error {
	o.mu.Lock()
	o.started++
	if o.started >= o.want {
		o.once.Do(func() { close(o.ready) })
	}
	o.mu.Unlock()
	select {
	case <-o.ready:
		return nil
	case <-time.After(time.Second):
		return errors.New("key-pair writes were serialized")
	}
}

func (o *remoteLifecycleOwner) Connected(ctx context.Context, nodeID string) error {
	_, found, err := o.Runtime(ctx, nodeID)
	if err != nil {
		return err
	}
	if !found {
		return ErrNodeGone
	}
	return nil
}

func (o *remoteLifecycleOwner) PutKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	return nil
}

func (o *remoteLifecycleOwner) DropKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	return nil
}

func (o *remoteLifecycleOwner) AdmitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	return true
}

func (o *remoteLifecycleOwner) ReleaseBuild(ctx context.Context, nodeID, buildID string) {}

func (o *remoteLifecycleOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return o.reg.stores.GetNode(ctx, nodeID)
}

func (o *remoteLifecycleOwner) DeleteSandbox(ctx context.Context, nodeID, sid, apiSecretFingerprint string) error {
	return nil
}

func (o *remoteLifecycleOwner) SendCommand(ctx context.Context, nodeID string, cmd *routesync.Command) error {
	o.commands++
	return nil
}

func (o *remoteLifecycleOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	o.commands++
	if cmd.Kind == routesync.CmdCreate {
		if cmd.Cluster == nil {
			return nil, errors.New("missing cluster sandbox context")
		}
		record := testE2BSandboxRecord(cmd.Cluster.Group, cmd.Cluster.RouteKey, cmd.Cluster.StableID, nodeID, StateReady)
		_, _ = o.reg.stores.PutSandbox(ctx, record)
	}
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
}

func TestReserveSandboxCreateFlow(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	// The fake node, on create, reports the sandbox running (the node is the
	// route authority; the registry's Reserve waits on this).
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		if cmd.AutoPauseMemory == nil || !*cmd.AutoPauseMemory {
			t.Errorf("default create AutoPauseMemory = %v, want true", cmd.AutoPauseMemory)
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/c/p/a/g1", "u1:s1", nil))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	want := testEnvdAccessToken
	if res.Route.NodeID != "n1" || res.Route.SandboxID == "" || res.Route.NodeSandboxID != EncodeNodeSandboxID(res.Route.SandboxID, 0) || res.Route.EnvdAccessToken != want {
		t.Fatalf("reserve result: %+v", res)
	}

	// The sandbox is now READY; a second reserve for the same key returns it
	// directly (session affinity) without re-placing.
	res2, err := reg.ReserveSandbox(ctx, testCreateReserve("/c/p/a/g1", "u1:s1", nil))
	if err != nil {
		t.Fatalf("re-reserve: %v", err)
	}
	if res2.Route.SandboxID != res.Route.SandboxID || res2.Route.NodeSandboxID != res.Route.NodeSandboxID || res2.Route.NodeID != "n1" {
		t.Fatalf("re-reserve mismatch: %+v vs %+v", res2, res)
	}

	// SandboxStore reflects READY keyed by (group, route_key).
	rec, _, found, err := reg.stores.GetSandbox(ctx, "/c/p/a/g1", "u1:s1")
	if err != nil || !found || rec.State != StateReady {
		t.Fatalf("stored record: %+v found=%v err=%v", rec, found, err)
	}
	if !rec.AutoPauseMemory {
		t.Fatal("default create record lost AutoPauseMemory=true")
	}
}

func TestReserveSandboxCreatePropagatesAutoPauseMemoryFalse(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		if cmd.AutoPauseMemory == nil || *cmd.AutoPauseMemory {
			t.Errorf("create AutoPauseMemory = %v, want explicit false", cmd.AutoPauseMemory)
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}
	reg.addNode(conn)

	request := testCreateReserve("/g", "auto-pause-false", nil)
	value := false
	request.AutoPauseMemory = &value
	if _, err := reg.ReserveSandbox(ctx, request); err != nil {
		t.Fatal(err)
	}
	record, _, found, err := reg.stores.GetSandbox(ctx, "/g", "auto-pause-false")
	if err != nil || !found || record.State != StateReady || record.AutoPauseMemory {
		t.Fatalf("stored explicit false record = %+v, found=%t, err=%v", record, found, err)
	}
}

func TestReserveSandboxJoinerWakesWhenReadyObservedByQuorumRead(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	commandSeen := make(chan *routesync.Command, 1)
	releaseReady := make(chan struct{})
	var once sync.Once
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		once.Do(func() { commandSeen <- cmd })
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		go func() {
			<-releaseReady
			if cmd.Cluster == nil {
				t.Error("create command is missing cluster context")
				return
			}
			record := testE2BSandboxRecord(cmd.Cluster.Group, cmd.Cluster.RouteKey, cmd.Cluster.StableID, "n1", StateReady)
			_, _ = reg.stores.PutSandbox(context.Background(), record)
		}()
	}})

	type reserveOut struct {
		res *ReserveResult
		err error
	}
	leader := make(chan reserveOut, 1)
	go func() {
		res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
		leader <- reserveOut{res: res, err: err}
	}()

	select {
	case <-commandSeen:
	case <-time.After(time.Second):
		t.Fatal("reserve did not send create command")
	}

	joinCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	joiner := make(chan reserveOut, 1)
	go func() {
		res, err := reg.ReserveSandbox(joinCtx, testCreateReserve("/g", "rk", nil))
		joiner <- reserveOut{res: res, err: err}
	}()
	time.Sleep(25 * time.Millisecond)
	close(releaseReady)

	var lout reserveOut
	select {
	case lout = <-leader:
	case <-time.After(time.Second):
		t.Fatal("leader reserve did not complete")
	}
	if lout.err != nil || lout.res == nil || lout.res.Route.SandboxID == "" || lout.res.Route.NodeSandboxID == "" {
		t.Fatalf("leader reserve = %+v err=%v", lout.res, lout.err)
	}
	select {
	case jout := <-joiner:
		if jout.err != nil || jout.res == nil || jout.res.Route.SandboxID != lout.res.Route.SandboxID || jout.res.Route.NodeSandboxID != lout.res.Route.NodeSandboxID {
			t.Fatalf("joiner reserve = %+v err=%v, leader=%+v", jout.res, jout.err, lout.res)
		}
	case <-time.After(time.Second):
		t.Fatal("joiner reserve was not woken by ready quorum read")
	}
}

func TestReserveSandboxWakesFromRemoteRouteLinkWrite(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"a", "b", "c"}, 3, 1, 1, 1)
	nodeID := "node-remote"
	waiter := New(cluster["a"], placementWithToken(nodeID), 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reporter := New(cluster["b"], nil, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, waiter)
	owner := &remoteRouteWriteOwner{
		node: &NodeRecord{NodeID: nodeID, DataEndpoint: "127.0.0.1:12345"},
		onCreate: func(cmd *routesync.Command) {
			go func() {
				time.Sleep(20 * time.Millisecond)
				route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
				route.TemplateID = cmd.TemplateRef
				reporter.applyRoute(context.Background(), nodeID, &route)
			}()
		},
	}
	waiter.SetNodeOwner(owner)

	res, err := waiter.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("ReserveSandbox: %v", err)
	}
	if res.Route.NodeID != nodeID || res.Route.SandboxID == "" || res.Route.NodeSandboxID != EncodeNodeSandboxID(res.Route.SandboxID, 0) || res.Route.DataEndpoint != "127.0.0.1:12345" {
		t.Fatalf("reserve result=%+v", res)
	}
	rec, _, found, err := cluster["c"].GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || rec.State != StateReady || rec.SandboxID != res.Route.SandboxID || rec.NodeSandboxID != res.Route.NodeSandboxID {
		t.Fatalf("replicated ready route=%+v found=%v err=%v", rec, found, err)
	}
}

func TestReserveSandboxUsesNodeReportedCredentials(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "u1:s1", nil))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	want := testEnvdAccessToken
	if res.Route.EnvdAccessToken != want || res.Route.TrafficAccessToken != testTrafficAccessToken || res.Route.ForwardAccessToken == "" {
		t.Fatalf("route credentials mismatch result=%+v", res)
	}
	if res.Route.Profile != "e2b" {
		t.Fatalf("reserve profile=%q, want e2b", res.Route.Profile)
	}
}

func TestReadyRoutePreservesCredentials(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.addNode(&fakeConn{nodeID: "n1"})
	want := testEnvdAccessToken
	reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-ready", "n1", StateReady))

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.Route.SandboxID != "sb-ready" || res.Route.NodeSandboxID != EncodeNodeSandboxID("sb-ready", 0) || res.Route.EnvdAccessToken != want || res.Route.Profile != "e2b" {
		t.Fatalf("ready reserve result=%+v, want e2b profile and node-reported token %q", res, want)
	}
	rr, found, err := reg.ResolveSID(ctx, "/g", "rk", "sb-ready")
	if err != nil || !found {
		t.Fatalf("resolve found=%v err=%v", found, err)
	}
	if rr.SandboxID != "sb-ready" || rr.NodeSandboxID != EncodeNodeSandboxID("sb-ready", 0) || rr.EnvdAccessToken != want || rr.Profile != "e2b" {
		t.Fatalf("resolve result=%+v, want e2b profile and node-reported token %q", rr, want)
	}
	if _, found, err := reg.ResolveSID(ctx, "/other", "rk", "sb-ready"); err != nil || found {
		t.Fatalf("wrong-group resolve found=%v err=%v", found, err)
	}
	if _, found, err := reg.ResolveSID(ctx, "/g", "other-rk", "sb-ready"); err != nil || found {
		t.Fatalf("wrong-route-key resolve found=%v err=%v", found, err)
	}
}

func TestMaterializedRouteReadsRejectCorruptCredentials(t *testing.T) {
	for _, state := range []SandboxState{StateReady, StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			record := testE2BSandboxRecord("/g", "rk", "sb-corrupt", "n1", state)
			record.ForwardAccessToken = ""
			if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
				t.Fatal(err)
			}

			if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); err == nil {
				t.Fatal("Reserve accepted a corrupt materialized route")
			}
			if _, found, err := reg.ResolveSID(ctx, "/g", "rk", "sb-corrupt"); err == nil || found {
				t.Fatalf("ResolveSID found=%v err=%v, want credential validation failure", found, err)
			}
		})
	}
}

func TestReserveSandboxCreateUsesPlacementMaterial(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	basePlacer := placementWithToken("n1")
	reg.SetPlacer(placementFunc(func(ctx context.Context, req PlaceRequest) (*Placement, error) {
		placement, err := basePlacer.Place(ctx, req)
		if err != nil {
			return nil, err
		}
		placement.Config[sandboxcfg.NsRestore] = `{"prefetch":"memory"}`
		placement.Config[sandboxcfg.NsCheckpoint] = `{"merge_ref":true}`
		return placement, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var got *routesync.Command
	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		cp := *cmd
		got = &cp
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}
	reg.addNode(conn)

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", map[string]string{"b": "2"})); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("create command not sent")
	}
	if got.TemplateRef != testTemplateRef || got.Config["a"] != "1" || got.Config["b"] != "2" {
		t.Fatalf("create command did not use placement config: %+v", got)
	}
	if got.Profile != "e2b" || got.Cluster == nil || got.Cluster.Group != "/g" || got.Cluster.RouteKey != "rk" ||
		got.Cluster.StableID == got.SID || got.SID != EncodeNodeSandboxID(got.Cluster.StableID, 0) {
		t.Fatalf("create command identity context=%+v, want StableID and g0 node ID", got)
	}
	if _, ok := got.Config[clusterstate.ObjectMetadataKey]; ok {
		t.Fatalf("cluster identity leaked into create metadata: %+v", got.Config)
	}
	if _, ok := got.Config[sandboxcfg.NsRestore]; ok {
		t.Fatalf("placement restore leaked without an explicit create value: %+v", got.Config)
	}
	if _, ok := got.Config[sandboxcfg.NsCheckpoint]; ok {
		t.Fatalf("placement checkpoint policy leaked without an explicit create value: %+v", got.Config)
	}
	if got.APISecretFingerprint != fullFingerprint(testAPISecret) {
		t.Fatalf("key fingerprint=%q, want provider key fp", got.APISecretFingerprint)
	}

	for _, mode := range []string{"off", "memory"} {
		got = nil
		if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk-"+mode, map[string]string{
			sandboxcfg.NsRestore: `{"prefetch":"` + mode + `"}`,
		})); err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Config[sandboxcfg.NsRestore] != `{"prefetch":"`+mode+`"}` {
			t.Fatalf("explicit create restore %q did not reach command: %+v", mode, got)
		}
	}

	got = nil
	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk-checkpoint", map[string]string{
		sandboxcfg.NsCheckpoint: ` { "merge_ref" : false, "drop_caches" : null } `,
	})); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Config[sandboxcfg.NsCheckpoint] != `{"merge_ref":false}` {
		t.Fatalf("explicit Create checkpoint policy did not reach command canonically: %+v", got)
	}
}

func TestReserveSandboxLeafMergesPlacementAndRequestResources(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	basePlacer := placementWithToken("n1")
	reg.SetPlacer(placementFunc(func(ctx context.Context, req PlaceRequest) (*Placement, error) {
		placement, err := basePlacer.Place(ctx, req)
		if err != nil {
			return nil, err
		}
		placement.Config[sandboxcfg.NsResource] = `{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":0.5}}`
		return placement, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var command *routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		copy := *cmd
		copy.Config = cloneStringMap(cmd.Config)
		command = &copy
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}})

	_, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "resource-leaves", map[string]string{
		sandboxcfg.NsResource: `{"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if command == nil {
		t.Fatal("create command not sent")
	}
	patch, err := sandboxcfg.ParseResourcePatch(command.Config[sandboxcfg.NsResource])
	if err != nil {
		t.Fatal(err)
	}
	if patch.Capacity == nil || patch.Capacity.CPU == nil || *patch.Capacity.CPU != 4 ||
		patch.Capacity.Memory == nil || *patch.Capacity.Memory != "8GiB" ||
		patch.Allocatable == nil || patch.Allocatable.CPU == nil || *patch.Allocatable.CPU != 0.5 ||
		patch.Allocatable.Memory == nil || *patch.Allocatable.Memory != "512MiB" ||
		patch.Startup == nil || patch.Startup.Memory == nil || *patch.Startup.Memory != "1GiB" {
		t.Fatalf("merged resource patch = %+v", patch)
	}
}

func TestReserveSandboxDoesNotHideInvalidPlacementResourceWithRequest(t *testing.T) {
	reg := testReg(t)
	commands := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		return &Placement{
			NodeID: "n1", TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint,
			Config: map[string]string{sandboxcfg.NsResource: `{"control":{}}`},
		}, nil
	}))
	if err := reg.stores.PutNode(context.Background(), &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands++ }})

	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "invalid-placement-resource", map[string]string{
		sandboxcfg.NsResource: `{"capacity":{"cpu":8,"memory":"16GiB"},"allocatable":{"memory":"1GiB"}}`,
	}))
	if !errors.Is(err, errInvalidSandboxConfig) || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("ReserveSandbox error = %v", err)
	}
	if commands != 0 {
		t.Fatalf("invalid lower-priority resource sent %d commands", commands)
	}
}

func TestReserveSandboxRejectsInvalidRestoreBeforePlacement(t *testing.T) {
	placements := 0
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}))

	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsRestore: `{"prefetch":"disk"}`,
	}))
	if err == nil {
		t.Fatal("invalid restore should be rejected")
	}
	if placements != 0 {
		t.Fatalf("invalid restore reached placement %d times", placements)
	}
	if _, _, found, getErr := reg.stores.GetSandbox(context.Background(), "/g", "rk"); getErr != nil || found {
		t.Fatalf("invalid restore wrote route state: found=%v err=%v", found, getErr)
	}
}

func TestReserveSandboxRejectsInvalidCredentialsBeforeReadyFastPath(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "sb-ready", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}

	_, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsCredentials: `{"service_secret":"not-hex"}`,
	}))
	if !errors.Is(err, errInvalidSandboxConfig) {
		t.Fatalf("ReserveSandbox error=%v, want invalid sandbox config", err)
	}
	stored, _, found, getErr := reg.stores.GetSandbox(ctx, "/g", "rk")
	if getErr != nil || !found || stored.SandboxID != original.SandboxID || stored.NodeSandboxID != original.NodeSandboxID || stored.State != original.State {
		t.Fatalf("invalid credentials changed READY route: stored=%+v found=%v err=%v", stored, found, getErr)
	}
}

func TestReserveSandboxRejectsInvalidPlacementTemplate(t *testing.T) {
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		return &Placement{
			NodeID: "n1", TemplateRef: "e2b-snp-invalid", APISecretFingerprint: testAPIFingerprint,
		}, nil
	}))

	if _, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "rk", nil)); err == nil ||
		!strings.Contains(err.Error(), "invalid placement template") {
		t.Fatalf("ReserveSandbox error=%v, want invalid placement template", err)
	}
	if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
		t.Fatalf("invalid template wrote route: found=%v err=%v", found, err)
	}
}

func TestReserveSandboxJoinsExistingRouteFlight(t *testing.T) {
	result := &ReserveResult{Route: RouteResolve{SandboxID: "s1", NodeSandboxID: EncodeNodeSandboxID("s1", 0), NodeID: "n1", DataEndpoint: "node:1"}}
	call := &reserveCall{done: make(chan struct{}), result: result}
	close(call.done)
	reg := testReg(t)
	reg.inflight[flightKey("/g", "rk")] = call

	got, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsRestore: `{"prefetch":"memory"}`,
	}))
	if err != nil || got != result {
		t.Fatalf("existing route flight result=%+v err=%v", got, err)
	}
}

func TestReserveSandboxCreateUsesRemoteNodeOwner(t *testing.T) {
	ctx := context.Background()
	reg := New(NewStores(), placementWithToken("n-remote"), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n-remote", LinkOwner: "remote", DataEndpoint: "10.0.0.2:8443"}); err != nil {
		t.Fatal(err)
	}
	owner := &remoteLifecycleOwner{reg: reg}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": owner})

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("reserve via remote owner: %v", err)
	}
	if res.Route.NodeID != "n-remote" || res.Route.DataEndpoint != "10.0.0.2:8443" || owner.commands != 1 {
		t.Fatalf("res=%+v commands=%d", res, owner.commands)
	}
}

func TestReserveSandboxReadyRouteUsesRemoteNodeOwnerRuntime(t *testing.T) {
	ctx := context.Background()
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		t.Fatal("ready route should not call placer")
		return nil, nil
	}), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n-remote", LinkOwner: "remote", DataEndpoint: "10.0.0.2:8443"}); err != nil {
		t.Fatal(err)
	}
	owner := &remoteLifecycleOwner{reg: reg}
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": owner})
	_, _ = reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-ready", "n-remote", StateReady))

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("reserve ready via remote owner: %v", err)
	}
	if res.Route.NodeID != "n-remote" || res.Route.DataEndpoint != "10.0.0.2:8443" || owner.commands != 0 {
		t.Fatalf("res=%+v commands=%d", res, owner.commands)
	}
}

func TestReserveSandboxDoesNotReplaceReadyRouteOnRuntimeError(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "replacement", APISecretFingerprint: testAPIFingerprint}, nil
	}), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-ready", "n1", StateReady)); err != nil {
		t.Fatal(err)
	}
	runtimeErr := errors.New("node owner temporarily unavailable")
	owner := &remoteRouteWriteOwner{node: &NodeRecord{NodeID: "n1"}, runtimeErr: runtimeErr}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, runtimeErr) {
		t.Fatalf("ReserveSandbox err=%v, want runtime error", err)
	}
	if placements != 0 || owner.commands != 0 {
		t.Fatalf("runtime error replaced READY route: placements=%d commands=%d", placements, owner.commands)
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || got.SandboxID != "sb-ready" || got.NodeSandboxID != EncodeNodeSandboxID("sb-ready", 0) || got.State != StateReady {
		t.Fatalf("READY route=%+v found=%v err=%v", got, found, err)
	}
}

func TestReserveSandboxNoNode(t *testing.T) {
	reg := testReg(t)
	if _, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/c/p/a/g1", "u1:s1", nil)); err == nil {
		t.Fatal("expected error with no nodes")
	}
}

func TestSendAndWaitAck(t *testing.T) {
	reg := testReg(t)
	conn := &fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}}
	ack, err := reg.sendAndWait(context.Background(), conn, &routesync.Command{CmdID: "c1", Kind: routesync.CmdKeyPut}, 2*time.Second)
	if err != nil {
		t.Fatalf("sendAndWait: %v", err)
	}
	if ack.Status != routesync.AckAccepted {
		t.Fatalf("ack: %+v", ack)
	}
}

func TestSendAndWaitTimeout(t *testing.T) {
	reg := testReg(t)
	conn := &fakeConn{nodeID: "n1"} // never acks
	if _, err := reg.sendAndWait(context.Background(), conn, &routesync.Command{CmdID: "c2", Kind: routesync.CmdKeyPut}, 100*time.Millisecond); err == nil {
		t.Fatal("expected timeout waiting for an ack")
	}
}

func TestCreateRejectFastFails(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	conn := &fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdCreate {
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "bad template"})
		}
	}}
	reg.addNode(conn)

	start := time.Now()
	_, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err == nil {
		t.Fatal("expected reserve to fail on a rejected create")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reserve took %v; expected fast-fail on reject (park_timeout is 5s)", elapsed)
	}
}

func TestCreateRejectRestoresPreexistingRoute(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("n1"))
	orig := testE2BSandboxRecord("/g", "rk", "sb-existing", "n-gone", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdCreate {
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "reject"})
		}
	}})

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); err == nil {
		t.Fatal("expected reserve to fail on rejected replacement")
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || got.SandboxID != orig.SandboxID || got.NodeSandboxID != orig.NodeSandboxID || got.NodeID != orig.NodeID || got.State != orig.State {
		t.Fatalf("restored route=%+v found=%v err=%v, want %+v", got, found, err, orig)
	}
}

func TestDeadReportDeletesRoute(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-dead", "n1", StateReady))
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "sb-dead", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b",
		SandboxID: EncodeNodeSandboxID("sb-dead", 0), State: routesync.StateDead,
	})

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("dead report should delete route")
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID("sb-dead", 0)); err != nil || found {
		t.Fatalf("dead report left node sandbox ref found=%v err=%v", found, err)
	}
}

func TestStartingReportPreservesRegistryRouteState(t *testing.T) {
	for _, state := range []SandboxState{StateReserved, StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			sid := "sb-starting-" + string(state)
			record := testE2BSandboxRecord("/g", "rk", sid, "n1", state)
			initialRevision, err := reg.stores.PutSandbox(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			ref := testNodeSandboxRef("/g", "rk", sid, "e2b", testAPIFingerprint)
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
				t.Fatal(err)
			}

			route := testE2BRoute(sid, routesync.StateStarting)
			reg.applyRoute(ctx, "n1", &route)

			stored, revision, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found || revision != initialRevision || stored.State != state ||
				stored.NodeSandboxID != record.NodeSandboxID {
				t.Fatalf("route after starting report = %+v revision=%d found=%v err=%v", stored, revision, found, err)
			}
			storedRef, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", record.NodeSandboxID)
			if err != nil || !found || storedRef != ref {
				t.Fatalf("node ref after starting report = %+v found=%v err=%v", storedRef, found, err)
			}
		})
	}
}

func TestLateDeadReportDoesNotDeleteReplacementRoute(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-new", "n1", StateReady)); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: EncodeNodeSandboxID("sb-old", 0), State: routesync.StateDead})

	rec, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || rec.SandboxID != "sb-new" {
		t.Fatalf("replacement route=%+v found=%v err=%v", rec, found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID("sb-old", 0)); err != nil || found {
		t.Fatalf("stale owner ref found=%v err=%v", found, err)
	}
}

func TestStaleLiveReportRetainsOwnershipUntilDead(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-new", "n1", StateReady)); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	var deletes int
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdDelete && cmd.SID == EncodeNodeSandboxID("sb-old", 0) {
			deletes++
		}
	}})

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: EncodeNodeSandboxID("sb-old", 0), State: routesync.StateRunning})
	if deletes != 1 {
		t.Fatalf("stale live report sent %d delete commands, want 1", deletes)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID("sb-old", 0)); err != nil || !found {
		t.Fatalf("stale live ownership was removed before DEAD: found=%v err=%v", found, err)
	}

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: EncodeNodeSandboxID("sb-old", 0), State: routesync.StateDead})
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID("sb-old", 0)); err != nil || found {
		t.Fatalf("stale ownership remained after DEAD: found=%v err=%v", found, err)
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || got.SandboxID != "sb-new" {
		t.Fatalf("replacement route=%+v found=%v err=%v", got, found, err)
	}
}

func TestRouteReportProfileMustMatchOwnership(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/g", "rk", "sb-profile", "n1", StateReserved)); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "sb-profile", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	var deletes int
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdDelete && cmd.SID == EncodeNodeSandboxID("sb-profile", 0) {
			deletes++
		}
	}})

	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{
		SandboxID: EncodeNodeSandboxID("sb-profile", 0), Profile: "bare", State: routesync.StateRunning,
	})

	rec, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || rec.State != StateReserved || rec.Profile != "e2b" {
		t.Fatalf("mismatched report changed route=%+v found=%v err=%v", rec, found, err)
	}
	if deletes != 1 {
		t.Fatalf("mismatched report sent %d delete commands, want 1", deletes)
	}
}

func TestValidateRouteCredentials(t *testing.T) {
	valid := testE2BRoute("sb-credentials", routesync.StateRunning)
	if err := validateRouteCredentials(&valid, testAPIFingerprint, "sb-credentials"); err != nil {
		t.Fatalf("valid e2b route credentials: %v", err)
	}

	bare := testE2BRoute("sb-bare", routesync.StateRunning)
	bare.Profile = "bare"
	bare.EnvdAccessToken = ""
	bare.TrafficAccessToken = ""
	if err := validateRouteCredentials(&bare, testAPIFingerprint, "sb-bare"); err != nil {
		t.Fatalf("valid bare route credentials: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*routesync.RouteEntry)
	}{
		{name: "API secret", mutate: func(route *routesync.RouteEntry) { route.APISecret = strings.Repeat("0", 64) }},
		{name: "API fingerprint", mutate: func(route *routesync.RouteEntry) { route.APISecretFingerprint = strings.Repeat("0", 64) }},
		{name: "manifest fingerprint", mutate: func(route *routesync.RouteEntry) { route.ManifestKeyFingerprint = "bad" }},
		{name: "stable ID", mutate: func(route *routesync.RouteEntry) { route.StableID = "" }},
		{name: "service secret", mutate: func(route *routesync.RouteEntry) { route.ServiceSecret = "bad" }},
		{name: "forward token", mutate: func(route *routesync.RouteEntry) { route.ForwardAccessToken = "bad" }},
		{name: "missing envd token", mutate: func(route *routesync.RouteEntry) { route.EnvdAccessToken = "" }},
		{name: "missing traffic token", mutate: func(route *routesync.RouteEntry) { route.TrafficAccessToken = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := valid
			tt.mutate(&candidate)
			if err := validateRouteCredentials(&candidate, testAPIFingerprint, "sb-credentials"); err == nil {
				t.Fatal("invalid route credentials were accepted")
			}
		})
	}

	bare.EnvdAccessToken = "e2b-only"
	if err := validateRouteCredentials(&bare, testAPIFingerprint, "sb-bare"); err == nil {
		t.Fatal("bare route accepted an e2b credential")
	}
}

func TestReservedRouteWithMaterializedCredentialsRejectsRebinding(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	current := testE2BSandboxRecord("/g", "rk", "sb-resume", "n1", StateReserved)
	if _, err := reg.stores.PutSandbox(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "sb-resume", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	var deletes int
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdDelete {
			deletes++
		}
	}})

	rebound := testE2BRoute("sb-resume", routesync.StateRunning)
	rebound.ServiceSecret = strings.Repeat("1", 64)
	var err error
	rebound.ForwardAccessToken, err = keys.MintForwardAccessToken(rebound.ServiceSecret, rebound.StableID)
	if err != nil {
		t.Fatal(err)
	}
	reg.applyRoute(ctx, "n1", &rebound)

	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.State != StateReserved || !sameRouteCredentials(stored, current) {
		t.Fatalf("credential rebinding changed reserved route=%+v found=%v err=%v", stored, found, err)
	}
	if deletes != 0 {
		t.Fatalf("credential rebinding triggered %d destructive commands", deletes)
	}
}

func TestUnownedRouteReportDoesNotDeleteNodeLocalSandbox(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var cmds []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		cmds = append(cmds, cmd)
	}})

	for _, state := range []string{routesync.StateRunning, routesync.StatePaused, routesync.StateDead} {
		reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: "sb-local", State: state})
	}
	if len(cmds) != 0 {
		t.Fatalf("unowned node-local sandbox received cluster command: %+v", cmds)
	}
}

func TestRouteEventsResolveSameIDByNodeOwnerTable(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	for _, tc := range []struct {
		node     string
		group    string
		routeKey string
	}{
		{node: "n1", group: "/g1", routeKey: "rk1"},
		{node: "n2", group: "/g2", routeKey: "rk2"},
	} {
		record := testE2BSandboxRecord(tc.group, tc.routeKey, "same-sandbox", tc.node, StateReserved)
		if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
			t.Fatal(err)
		}
		if err := reg.stores.AddNodeSandboxRef(ctx, tc.node, testNodeSandboxRef(tc.group, tc.routeKey, "same-sandbox", "e2b", testAPIFingerprint)); err != nil {
			t.Fatal(err)
		}
	}

	route := testE2BRoute("same-sandbox", routesync.StateRunning)
	reg.applyRoute(ctx, "n1", &route)
	g1, _, found, err := reg.stores.GetSandbox(ctx, "/g1", "rk1")
	if err != nil || !found || g1.State != StateReady || g1.EnvdAccessToken != testEnvdAccessToken {
		t.Fatalf("g1 route=%+v found=%v err=%v", g1, found, err)
	}
	g2, _, found, err := reg.stores.GetSandbox(ctx, "/g2", "rk2")
	if err != nil || !found || g2.State != StateReserved {
		t.Fatalf("n1 event changed g2 route=%+v found=%v err=%v", g2, found, err)
	}
}

func TestReserveRetriesSameNodeAfterSandboxIDCollision(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var firstSID string
	placeCalls := 0
	basePlacer := placementWithToken("n1")
	reg.SetPlacer(placementFunc(func(ctx context.Context, req PlaceRequest) (*Placement, error) {
		placeCalls++
		if placeCalls == 1 {
			firstSID = req.SandboxID
			if _, err := reg.stores.PutSandbox(ctx, testE2BSandboxRecord("/existing", "rk-existing", firstSID, "n1", StateReady)); err != nil {
				return nil, err
			}
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/existing", "rk-existing", firstSID, "e2b", testAPIFingerprint)); err != nil {
				return nil, err
			}
		}
		return basePlacer.Place(ctx, req)
	}))
	var createSIDs []string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		createSIDs = append(createSIDs, cmd.SID)
		go func() {
			reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
			route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
			route.SandboxID = cmd.SID
			reg.applyRoute(ctx, "n1", &route)
		}()
	}})

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/new", "rk-new", nil))
	if err != nil {
		t.Fatalf("ReserveSandbox: %v", err)
	}
	if placeCalls != 2 || firstSID == "" || res.Route.SandboxID != firstSID || res.Route.NodeSandboxID == EncodeNodeSandboxID(firstSID, 0) {
		t.Fatalf("placeCalls=%d firstSID=%q result=%+v", placeCalls, firstSID, res)
	}
	if len(createSIDs) != 1 || createSIDs[0] != res.Route.NodeSandboxID {
		t.Fatalf("create commands=%v result=%+v", createSIDs, res)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID(firstSID, 0)); err != nil || !found || ref.Group != "/existing" || ref.RouteKey != "rk-existing" {
		t.Fatalf("original ownership=%+v found=%v err=%v", ref, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", res.Route.NodeSandboxID); err != nil || !found || ref.Group != "/new" || ref.RouteKey != "rk-new" || ref.SandboxID != res.Route.SandboxID || ref.SandboxGeneration != 1 {
		t.Fatalf("replacement ownership=%+v found=%v err=%v", ref, found, err)
	}
}

func TestParkTimeoutRollback(t *testing.T) {
	ctx := context.Background()
	reg := New(NewStores(), nil, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	reg.SetPlacer(placementWithToken("n1"))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	var sid string
	var deletes int
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		switch cmd.Kind {
		case routesync.CmdCreate:
			sid = cmd.SID
			go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		case routesync.CmdDelete:
			if cmd.SID == sid {
				deletes++
			}
		}
	}}) // accepts create but never reports running

	_, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err == nil {
		t.Fatal("expected park timeout error")
	}
	// The RESERVED record (never reached READY) must be rolled back, not stranded.
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("RESERVED record stranded after park timeout (not rolled back)")
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", sid); err != nil || !found {
		t.Fatalf("ambiguous create ownership was dropped: sid=%q found=%v err=%v", sid, found, err)
	}

	// A create that completed after the caller timed out is still recognized as
	// registry-owned, deleted, and retained until the node confirms DEAD.
	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: sid, State: routesync.StateRunning})
	if deletes != 1 {
		t.Fatalf("late running route sent %d delete commands, want 1", deletes)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", sid); err != nil || !found {
		t.Fatalf("late running ownership was removed before DEAD: found=%v err=%v", found, err)
	}
	reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: sid, State: routesync.StateDead})
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", sid); err != nil || found {
		t.Fatalf("late sandbox ownership remained after DEAD: found=%v err=%v", found, err)
	}
}

func TestRollbackReserveDropsReplacementNodeRef(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	reserved := testE2BSandboxRecord("/g", "rk", orig.SandboxID, "new", StateReserved)
	reserved.SandboxGeneration = 1
	reserved.NodeSandboxID = EncodeNodeSandboxID(orig.SandboxID, 1)
	reserved.NextSandboxGeneration = 2
	reservedRev, err := reg.stores.PutSandbox(ctx, reserved)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "new"}); err != nil {
		t.Fatal(err)
	}
	ref := testNodeSandboxRef("/g", "rk", orig.SandboxID, "e2b", testAPIFingerprint)
	ref.SandboxGeneration, ref.NodeSandboxID = 1, reserved.NodeSandboxID
	if err := reg.stores.AddNodeSandboxRef(ctx, "new", ref); err != nil {
		t.Fatal(err)
	}

	if !reg.rollbackReservedAtRevision(ctx, "/g", "rk", reserved, reservedRev, orig, true, true) {
		t.Fatal("rollback did not restore original route")
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || got.SandboxID != "sb-old" || got.NodeSandboxID != orig.NodeSandboxID || got.NodeID != "old" {
		t.Fatalf("restored route=%+v found=%v err=%v", got, found, err)
	}
	node, found, err := reg.stores.GetNode(ctx, "new")
	if err != nil || !found {
		t.Fatalf("replacement node=%+v found=%v err=%v", node, found, err)
	}
	if len(node.Sandboxes) != 0 {
		t.Fatalf("replacement node retained sandbox ref: %+v", node.Sandboxes)
	}
}

func TestRollbackReserveRestoresOriginalSameNodeRef(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "n1", StateReady)
	reserved := testE2BSandboxRecord("/g", "rk", orig.SandboxID, "n1", StateReserved)
	reserved.SandboxGeneration = 1
	reserved.NodeSandboxID = EncodeNodeSandboxID(orig.SandboxID, 1)
	reserved.NextSandboxGeneration = 2
	reservedRev, err := reg.stores.PutSandbox(ctx, reserved)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	ref := testNodeSandboxRef("/g", "rk", orig.SandboxID, "e2b", testAPIFingerprint)
	ref.SandboxGeneration, ref.NodeSandboxID = 1, reserved.NodeSandboxID
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
		t.Fatal(err)
	}

	if !reg.rollbackReservedAtRevision(ctx, "/g", "rk", reserved, reservedRev, orig, true, true) {
		t.Fatal("rollback did not restore original route")
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node=%+v found=%v err=%v", node, found, err)
	}
	if len(node.Sandboxes) != 1 || node.Sandboxes[0].SandboxID != "sb-old" || node.Sandboxes[0].NodeSandboxID != orig.NodeSandboxID {
		t.Fatalf("same-node rollback refs=%+v, want sb-old", node.Sandboxes)
	}
}

func TestRollbackReserveRestoresOriginalCredentialBinding(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	orig := testE2BSandboxRecord("/g", "rk", "same-sandbox", "n1", StateReady)
	reserved := testE2BSandboxRecord("/g", "rk", "same-sandbox", "n1", StateReserved)
	reserved.SandboxGeneration = 1
	reserved.NodeSandboxID = EncodeNodeSandboxID(orig.SandboxID, 1)
	reserved.NextSandboxGeneration = 2
	reserved.APISecretFingerprint = strings.Repeat("b", 64)
	reservedRev, err := reg.stores.PutSandbox(ctx, reserved)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	ref := testNodeSandboxRef("/g", "rk", reserved.SandboxID, "e2b", reserved.APISecretFingerprint)
	ref.SandboxGeneration, ref.NodeSandboxID = 1, reserved.NodeSandboxID
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
		t.Fatal(err)
	}

	if !reg.rollbackReservedAtRevision(ctx, "/g", "rk", reserved, reservedRev, orig, true, true) {
		t.Fatal("rollback did not restore original credential binding")
	}

	route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || route.APISecretFingerprint != orig.APISecretFingerprint {
		t.Fatalf("restored route=%+v found=%v err=%v", route, found, err)
	}
	ref, found, err = reg.stores.GetNodeSandboxRef(ctx, "n1", orig.NodeSandboxID)
	if err != nil || !found || ref.APISecretFingerprint != orig.APISecretFingerprint {
		t.Fatalf("restored ref=%+v found=%v err=%v", ref, found, err)
	}
}

func TestRollbackReserveRevisionFencesConcurrentWinner(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(fmt.Sprintf("restore=%v", restore), func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			reserved := testE2BSandboxRecord("/g", "rk", "sb-reserved", "candidate", StateReserved)
			if _, err := reg.stores.PutSandbox(ctx, reserved); err != nil {
				t.Fatal(err)
			}
			if err := reg.stores.AddNodeSandboxRef(ctx, "candidate", testNodeSandboxRef("/g", "rk", reserved.SandboxID, "e2b", testAPIFingerprint)); err != nil {
				t.Fatal(err)
			}
			observed, rev, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found {
				t.Fatalf("read reserved found=%v err=%v", found, err)
			}
			winner := testE2BSandboxRecord("/g", "rk", "sb-winner", "winner", StateReady)
			if _, err := reg.stores.PutSandbox(ctx, winner); err != nil {
				t.Fatal(err)
			}
			original := testE2BSandboxRecord("/g", "rk", "sb-original", "original", StatePaused)

			if reg.rollbackReservedAtRevision(ctx, "/g", "rk", observed, rev, original, restore, true) {
				t.Fatal("stale rollback replaced a concurrently committed winner")
			}
			got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found || got.SandboxID != winner.SandboxID || got.NodeSandboxID != winner.NodeSandboxID || got.State != StateReady {
				t.Fatalf("winner=%+v found=%v err=%v", got, found, err)
			}
			if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "candidate", reserved.NodeSandboxID); err != nil || !found {
				t.Fatalf("stale rollback mutated ownership: found=%v err=%v", found, err)
			}
		})
	}
}

func TestReserveFinishIsSingleAssignment(t *testing.T) {
	reg := testReg(t)
	call := &reserveCall{done: make(chan struct{})}
	reg.inflight["/g\x00rk"] = call
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			sandboxID := fmt.Sprintf("sb-%d", i)
			reg.finish("/g\x00rk", &ReserveResult{Route: RouteResolve{SandboxID: sandboxID, NodeSandboxID: EncodeNodeSandboxID(sandboxID, 0)}}, nil)
		}(i)
	}
	close(start)
	wg.Wait()
	select {
	case <-call.done:
	default:
		t.Fatal("reserve call was not completed")
	}
	if call.result == nil || call.result.Route.SandboxID == "" || call.result.Route.NodeSandboxID == "" {
		t.Fatalf("reserve result=%+v", call.result)
	}
}

func TestReadyReplacementFailureRestoresOriginalGeneration(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		if placements == 1 {
			return &Placement{NodeID: "candidate", APISecretFingerprint: testAPIFingerprint}, nil
		}
		return nil, ErrNoNode
	}))
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"old", "candidate"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "old", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "candidate", err: ErrNodeGone})

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("ReserveSandbox err=%v, want ErrNodeGone", err)
	}
	if placements != 2 {
		t.Fatalf("placements=%d, want failed candidate followed by no-node result", placements)
	}
	restored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || restored.SandboxID != "sb-old" || restored.NodeSandboxID != orig.NodeSandboxID || restored.SandboxGeneration != 0 || restored.NextSandboxGeneration != 2 || restored.NodeID != "old" || restored.State != StateReady {
		t.Fatalf("restored route=%+v found=%v err=%v", restored, found, err)
	}
	oldNode, found, err := reg.stores.GetNode(ctx, "old")
	if err != nil || !found || len(oldNode.Sandboxes) != 1 || oldNode.Sandboxes[0].SandboxID != "sb-old" {
		t.Fatalf("restored old node=%+v found=%v err=%v", oldNode, found, err)
	}
	candidate, found, err := reg.stores.GetNode(ctx, "candidate")
	if err != nil || !found || len(candidate.Sandboxes) != 0 {
		t.Fatalf("failed candidate=%+v found=%v err=%v", candidate, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "old", orig.NodeSandboxID); err != nil || !found || ref.RouteKey != "rk" {
		t.Fatalf("failed replacement did not restore old owner ref: ref=%+v found=%v err=%v", ref, found, err)
	}
}

func TestReadyReplacementDeleteRestoresOriginalRoute(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.parkTimeout = 100 * time.Millisecond
	reg.SetPlacer(placementWithToken("new"))
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"old", "new"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "old", testNodeSandboxRef("/g", "rk", orig.SandboxID, "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	deleted := make(chan struct{})
	var replacementSID string
	reg.addNode(&fakeConn{nodeID: "new", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		replacementSID = cmd.SID
		go func() {
			reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
			reg.applyDeleteBySID(context.Background(), "new", cmd.SID)
			close(deleted)
		}()
	}})

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); err == nil {
		t.Fatal("replacement create Delete unexpectedly completed Reserve")
	}
	select {
	case <-deleted:
	case <-time.After(time.Second):
		t.Fatal("replacement node did not report Delete")
	}
	restored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || restored.SandboxID != orig.SandboxID || restored.NodeSandboxID != orig.NodeSandboxID ||
		restored.SandboxGeneration != orig.SandboxGeneration || restored.NextSandboxGeneration != 2 ||
		restored.NodeID != orig.NodeID || restored.State != StateReady {
		t.Fatalf("route after replacement Delete = %+v found=%v err=%v", restored, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "old", orig.NodeSandboxID); err != nil || !found || ref.RouteKey != orig.RouteKey {
		t.Fatalf("original owner ref after replacement Delete = %+v found=%v err=%v", ref, found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "new", replacementSID); err != nil || found {
		t.Fatalf("failed replacement owner ref remained: found=%v err=%v", found, err)
	}
}

func TestReadyReplacementRejectsCredentialBindingChange(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		return &Placement{NodeID: "candidate", APISecretFingerprint: strings.Repeat("b", 64)}, nil
	}))
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "old", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}

	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, nil, orig, true); err == nil ||
		!strings.Contains(err.Error(), "credential binding mismatch") {
		t.Fatalf("placeAndCreate error = %v; want binding mismatch", err)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.SandboxID != orig.SandboxID || stored.NodeSandboxID != orig.NodeSandboxID || stored.APISecretFingerprint != testAPIFingerprint {
		t.Fatalf("original route=%+v found=%v err=%v", stored, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "old", orig.NodeSandboxID); err != nil || !found ||
		ref.APISecretFingerprint != testAPIFingerprint {
		t.Fatalf("original ref=%+v found=%v err=%v", ref, found, err)
	}
}

func TestReadyReplacementRejectsProfileChange(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		return &Placement{
			NodeID: "candidate", TemplateRef: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
			APISecretFingerprint: testAPIFingerprint,
		}, nil
	}))
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "old", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}

	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, nil, orig, true); err == nil ||
		!strings.Contains(err.Error(), "replacement profile mismatch") {
		t.Fatalf("placeAndCreate error = %v; want profile mismatch", err)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.SandboxID != orig.SandboxID || stored.NodeSandboxID != orig.NodeSandboxID || stored.Profile != orig.Profile {
		t.Fatalf("original route=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReadyReplacementPreservesMaterializedCredentials(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	original.AutoPauseMemory = false
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "new"}); err != nil {
		t.Fatal(err)
	}
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		return &Placement{
			NodeID: "new", TemplateRef: testTemplateRef,
			APISecretFingerprint: original.APISecretFingerprint,
		}, nil
	}))
	var command *routesync.Command
	var reserved *SandboxRecord
	reg.addNode(&fakeConn{nodeID: "new", onCmd: func(cmd *routesync.Command) {
		copy := *cmd
		copy.Config = cloneStringMap(cmd.Config)
		command = &copy
		reserved, _, _, _ = reg.stores.GetSandbox(ctx, "/g", "rk")
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := routesync.RouteEntry{
			SandboxID: cmd.SID, Profile: original.Profile, State: routesync.StateRunning,
			StableID: original.StableID, APISecret: original.APISecret,
			APISecretFingerprint:   original.APISecretFingerprint,
			ManifestKeyFingerprint: original.ManifestKeyFingerprint,
			ServiceSecret:          original.ServiceSecret, EnvdAccessToken: original.EnvdAccessToken,
			TrafficAccessToken: original.TrafficAccessToken,
			ForwardAccessToken: original.ForwardAccessToken,
		}
		go reg.applyRoute(context.Background(), "new", &route)
	}})

	requestOverride := strings.Repeat("2", 64)
	result, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsCredentials: `{"service_secret":"` + requestOverride + `","envd_access_token":"new-envd","traffic_access_token":"new-traffic"}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if command == nil || command.Cluster == nil || command.Cluster.StableID != original.StableID {
		t.Fatal("replacement create did not preserve StableID")
	}
	if command.AutoPauseMemory == nil || *command.AutoPauseMemory {
		t.Fatalf("replacement create changed durable AutoPauseMemory: %v", command.AutoPauseMemory)
	}
	createCredentials, _, err := sandboxcfg.ExtractCredentials(command.Config)
	if err != nil {
		t.Fatal(err)
	}
	if createCredentials.ServiceSecret != original.ServiceSecret ||
		createCredentials.EnvdAccessToken != original.EnvdAccessToken ||
		createCredentials.TrafficAccessToken != original.TrafficAccessToken {
		t.Fatal("replacement create did not preserve the original credential overrides")
	}
	if reserved == nil || reserved.SandboxID != original.SandboxID || reserved.NodeSandboxID != command.SID || reserved.SandboxGeneration != 1 || reserved.CreateCredentials == nil || reserved.AutoPauseMemory ||
		!sameRouteCredentials(reserved, original) {
		t.Fatal("replacement RESERVED row did not retain materialized credentials and TTL capture policy")
	}
	if result.Route.SandboxID != original.SandboxID || result.Route.NodeSandboxID == original.NodeSandboxID || result.Route.StableID != original.StableID ||
		result.Route.ServiceSecret != original.ServiceSecret || result.Route.EnvdAccessToken != original.EnvdAccessToken ||
		result.Route.TrafficAccessToken != original.TrafficAccessToken ||
		result.Route.ForwardAccessToken != original.ForwardAccessToken {
		t.Fatal("replacement READY result changed materialized credentials")
	}
	ready, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || ready.CreateCredentials != nil || ready.AutoPauseMemory || !sameRouteCredentials(ready, original) {
		t.Fatal("replacement READY row changed credentials or TTL capture policy, or retained create overrides")
	}
}

func TestMaterializedReservedRetryPreservesCredentialBinding(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "stable-auth-sid", "old", StateReserved)
	original.AutoPauseMemory = false
	original.CreateCredentials = &sandboxcfg.Credentials{
		ServiceSecret: original.ServiceSecret, EnvdAccessToken: original.EnvdAccessToken,
		TrafficAccessToken: original.TrafficAccessToken,
	}
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		nodeID := "n1"
		if placements == 2 {
			nodeID = "n2"
		}
		return &Placement{NodeID: nodeID, TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint}, nil
	}))
	var commands []*routesync.Command
	for _, nodeID := range []string{"n1", "n2"} {
		nodeID := nodeID
		reg.addNode(&fakeConn{nodeID: nodeID, onCmd: func(cmd *routesync.Command) {
			copy := *cmd
			copy.Config = cloneStringMap(cmd.Config)
			commands = append(commands, &copy)
			status := routesync.AckRejected
			if nodeID == "n2" {
				status = routesync.AckAccepted
			}
			reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: status})
		}})
	}

	newOverride := &sandboxcfg.Credentials{
		ServiceSecret: strings.Repeat("4", 64), EnvdAccessToken: "new-envd", TrafficAccessToken: "new-traffic",
	}
	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, newOverride, nil, true); err != nil {
		t.Fatal(err)
	}
	if placements != 2 || len(commands) != 2 {
		t.Fatalf("placements=%d commands=%d, want two attempts", placements, len(commands))
	}
	for _, cmd := range commands {
		if cmd.Cluster == nil || cmd.Cluster.StableID != original.StableID ||
			cmd.APISecretFingerprint != original.APISecretFingerprint {
			t.Fatal("replacement retry changed the stable ID or credential binding")
		}
		if cmd.AutoPauseMemory == nil || *cmd.AutoPauseMemory {
			t.Fatalf("replacement retry changed durable AutoPauseMemory: %v", cmd.AutoPauseMemory)
		}
		credentials, _, err := sandboxcfg.ExtractCredentials(cmd.Config)
		if err != nil {
			t.Fatal(err)
		}
		if credentials.ServiceSecret != original.ServiceSecret ||
			credentials.EnvdAccessToken != original.EnvdAccessToken ||
			credentials.TrafficAccessToken != original.TrafficAccessToken {
			t.Fatal("replacement retry changed materialized credential overrides")
		}
	}
	reserved, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || reserved.State != StateReserved || reserved.NodeID != "n2" ||
		reserved.AutoPauseMemory || !sameRouteCredentials(reserved, original) {
		t.Fatal("replacement retry did not persist the original credential binding")
	}
	route := testRouteFromRecord(original, reserved.NodeSandboxID, routesync.StateRunning)
	reg.applyRoute(ctx, "n2", &route)
	ready, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || ready.State != StateReady || ready.CreateCredentials != nil ||
		!sameRouteCredentials(ready, original) {
		t.Fatal("replacement retry READY event changed the credential binding")
	}
}

func TestMaterializedReservedRetryRejectsBindingChange(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "stable-auth-sid", "old", StateReserved)
	original.CreateCredentials = &sandboxcfg.Credentials{
		ServiceSecret: original.ServiceSecret, EnvdAccessToken: original.EnvdAccessToken,
		TrafficAccessToken: original.TrafficAccessToken,
	}
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		fingerprint := testAPIFingerprint
		if placements == 2 {
			fingerprint = strings.Repeat("b", 64)
		}
		return &Placement{
			NodeID: fmt.Sprintf("n%d", placements), TemplateRef: testTemplateRef,
			APISecretFingerprint: fingerprint,
		}, nil
	}))
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		commands++
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected})
	}})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(cmd *routesync.Command) { commands++ }})

	err := reg.placeAndCreate(ctx, "/g", "rk", nil, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "credential binding mismatch") {
		t.Fatalf("placeAndCreate error=%v, want credential binding mismatch", err)
	}
	if placements != 2 || commands != 1 {
		t.Fatalf("placements=%d commands=%d, binding change reached a node", placements, commands)
	}
}

func TestCreateRejectsProfileInvalidCredentialsBeforeRouteMutation(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		if _, found := req.Config[sandboxcfg.NsCredentials]; found {
			t.Fatalf("credentials leaked into placement config: %+v", req.Config)
		}
		return &Placement{
			NodeID: "candidate", TemplateRef: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
			APISecretFingerprint: testAPIFingerprint,
		}, nil
	}))
	err := reg.placeAndCreate(ctx, "/g", "rk", nil, &sandboxcfg.Credentials{EnvdAccessToken: "envd"}, nil, true)
	if err == nil || !strings.Contains(err.Error(), "not valid for bare") {
		t.Fatalf("placeAndCreate error = %v", err)
	}
	if route, _, found, getErr := reg.stores.GetSandbox(ctx, "/g", "rk"); getErr != nil || found {
		t.Fatalf("invalid credentials mutated route: route=%+v found=%v err=%v", route, found, getErr)
	}
}

func TestNormalizeSandboxReserveConfigSeparatesCreateCredentials(t *testing.T) {
	secret := strings.Repeat("1", 64)
	original := map[string]string{
		sandboxcfg.NsRestore:     ` { "prefetch" : "memory" } `,
		sandboxcfg.NsCredentials: ` { "service_secret" : "` + secret + `" } `,
		sandboxcfg.NsCheckpoint:  ` { "merge_ref" : false, "drop_caches" : null } `,
	}
	cleaned, credentials, err := normalizeSandboxReserveConfig(original)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := cleaned[sandboxcfg.NsCredentials]; found {
		t.Fatalf("credentials remained in ordinary config: %+v", cleaned)
	}
	if cleaned[sandboxcfg.NsRestore] != `{"prefetch":"memory"}` {
		t.Fatalf("restore was not canonicalized: %+v", cleaned)
	}
	if cleaned[sandboxcfg.NsCheckpoint] != `{"merge_ref":false}` {
		t.Fatalf("checkpoint policy was not canonicalized: %+v", cleaned)
	}
	if credentials == nil || credentials.ServiceSecret != secret {
		t.Fatalf("separated credentials=%+v", credentials)
	}
	if original[sandboxcfg.NsCredentials] == "" {
		t.Fatal("normalization mutated the caller's config")
	}

	cleaned, credentials, err = normalizeSandboxReserveConfig(map[string]string{
		sandboxcfg.NsCredentials: `{}`,
	})
	if err != nil || credentials == nil || *credentials != (sandboxcfg.Credentials{}) {
		t.Fatalf("explicit empty credentials: cleaned=%+v credentials=%+v err=%v", cleaned, credentials, err)
	}
	cleaned, credentials, err = normalizeSandboxReserveConfig(nil)
	if err != nil || cleaned != nil || credentials != nil {
		t.Fatalf("absent credentials: cleaned=%+v credentials=%+v err=%v", cleaned, credentials, err)
	}
}

func TestReserveSandboxRejectsInvalidCheckpointBeforePlacement(t *testing.T) {
	placements := 0
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}))

	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "rk-checkpoint-invalid", map[string]string{
		sandboxcfg.NsCheckpoint: `{"merge_ref":"false"}`,
	}))
	if err == nil {
		t.Fatal("invalid checkpoint policy should be rejected")
	}
	if placements != 0 {
		t.Fatalf("invalid checkpoint policy reached placement %d times", placements)
	}
	if _, _, found, getErr := reg.stores.GetSandbox(context.Background(), "/g", "rk-checkpoint-invalid"); getErr != nil || found {
		t.Fatalf("invalid checkpoint policy wrote route state: found=%v err=%v", found, getErr)
	}
}

func TestPlaceAndCreateRejectsUnseparatedCredentials(t *testing.T) {
	placements := 0
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return nil, ErrNoNode
	}))
	const privateValue = "must-not-appear"
	err := reg.placeAndCreate(context.Background(), "/g", "rk", map[string]string{
		sandboxcfg.NsCredentials: `{"envd_access_token":"` + privateValue + `"}`,
	}, nil, nil, true)
	if err == nil || strings.Contains(err.Error(), privateValue) {
		t.Fatalf("unseparated credentials error=%q", err)
	}
	if placements != 0 {
		t.Fatalf("unseparated credentials reached placement %d times", placements)
	}
}

func TestAttachCreateCredentialsCanonicalizesAndPreservesOrdinaryConfig(t *testing.T) {
	secret := strings.Repeat("1", 64)
	original := map[string]string{
		sandboxcfg.NsCredentials: ` { "traffic_access_token": "traffic", "service_secret": "` + secret + `" } `,
		"keep":                   "value",
	}
	credentials, cleaned, err := sandboxcfg.ExtractCredentials(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := attachCreateCredentials(cleaned, types.ProfileE2B, &credentials)
	if err != nil {
		t.Fatal(err)
	}
	if got[sandboxcfg.NsCredentials] != `{"service_secret":"`+secret+`","traffic_access_token":"traffic"}` || got["keep"] != "value" {
		t.Fatalf("normalized create config = %+v", got)
	}
	if original[sandboxcfg.NsCredentials] == got[sandboxcfg.NsCredentials] {
		t.Fatal("normalization mutated or aliased the original credentials value")
	}
	explicitEmpty, err := attachCreateCredentials(nil, types.ProfileE2B, &sandboxcfg.Credentials{})
	if err != nil || explicitEmpty[sandboxcfg.NsCredentials] != `{}` {
		t.Fatalf("explicit empty credentials config=%+v err=%v", explicitEmpty, err)
	}
	absent, err := attachCreateCredentials(nil, types.ProfileE2B, nil)
	if err != nil || absent != nil {
		t.Fatalf("absent credentials config=%+v err=%v", absent, err)
	}
}

func TestCreateCredentialsPersistWithReservedAndClearOnReady(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	secret := strings.Repeat("1", 64)
	credentials := &sandboxcfg.Credentials{
		ServiceSecret:      secret,
		EnvdAccessToken:    "envd-private",
		TrafficAccessToken: "traffic-private",
	}
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		if _, found := req.Config[sandboxcfg.NsCredentials]; found {
			t.Fatalf("credentials reached placer config: %+v", req.Config)
		}
		return &Placement{
			NodeID: "n1", TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint,
			Config: map[string]string{sandboxcfg.NsCredentials: `{"envd_access_token":"placement-fallback"}`},
		}, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var command *routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		copy := *cmd
		copy.Config = cloneStringMap(cmd.Config)
		command = &copy
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})

	if err := reg.placeAndCreate(ctx, "/g", "rk", map[string]string{"ordinary": "value"}, credentials, nil, true); err != nil {
		t.Fatal(err)
	}
	reserved, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || reserved.State != StateReserved || reserved.CreateCredentials == nil ||
		*reserved.CreateCredentials != *credentials {
		t.Fatalf("reserved route=%+v found=%v err=%v", reserved, found, err)
	}
	if command == nil || command.Config["ordinary"] != "value" ||
		command.Config[sandboxcfg.NsCredentials] != `{"service_secret":"`+secret+`","envd_access_token":"envd-private","traffic_access_token":"traffic-private"}` {
		t.Fatalf("create command config=%+v", command)
	}
	resolved, found, err := reg.ResolveSID(ctx, "/g", "rk", reserved.SandboxID)
	if err != nil || !found {
		t.Fatalf("resolve reserved route found=%v err=%v", found, err)
	}
	if resolved.SandboxID != reserved.SandboxID || resolved.NodeSandboxID != reserved.NodeSandboxID {
		t.Fatalf("resolve identity=%+v, want stable=%q node=%q", resolved, reserved.SandboxID, reserved.NodeSandboxID)
	}
	publicJSON, err := json.Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(publicJSON, []byte("create_credentials")) || bytes.Contains(publicJSON, []byte("private")) {
		t.Fatalf("ordinary resolve exposed create credentials: %s", publicJSON)
	}

	route := testE2BRoute(reserved.SandboxID, routesync.StateRunning)
	route.SandboxID = reserved.NodeSandboxID
	reg.applyRoute(ctx, "n1", &route)
	ready, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || ready.State != StateReady || ready.CreateCredentials != nil {
		t.Fatalf("ready route retained create credentials: route=%+v found=%v err=%v", ready, found, err)
	}
}

func TestCreateCredentialsCASRetryUsesCommittedWinner(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	winner := &sandboxcfg.Credentials{EnvdAccessToken: "winner-envd"}
	later := &sandboxcfg.Credentials{EnvdAccessToken: "later-envd"}
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		if _, found := req.Config[sandboxcfg.NsCredentials]; found {
			t.Fatalf("credentials reached placement attempt %d: %+v", placements, req.Config)
		}
		if placements == 1 {
			winnerRecord := testSandboxIdentityRecord("/g", "rk", "sb-winner", "winner-node", StateReserved)
			winnerRecord.CreateCredentials = cloneCreateCredentials(winner)
			if _, err := reg.stores.PutSandbox(ctx, winnerRecord); err != nil {
				t.Fatal(err)
			}
		}
		return &Placement{NodeID: "n1", TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint}, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var commandConfig map[string]string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		commandConfig = cloneStringMap(cmd.Config)
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})

	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, later, nil, true); err != nil {
		t.Fatal(err)
	}
	if placements != 2 {
		t.Fatalf("placement attempts=%d, want CAS retry", placements)
	}
	if commandConfig[sandboxcfg.NsCredentials] != `{"envd_access_token":"winner-envd"}` {
		t.Fatalf("CAS retry adopted later credentials: %+v", commandConfig)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.CreateCredentials == nil || *stored.CreateCredentials != *winner {
		t.Fatalf("CAS winner credentials=%+v found=%v err=%v", stored, found, err)
	}
}

func TestCreateCredentialsCASRetryDropsRolledBackWinner(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	rolledBackState := testSandboxIdentityRecord("/g", "rk", "sb-original", "original-node", StatePaused)
	winner := &sandboxcfg.Credentials{EnvdAccessToken: "rolled-back-winner"}
	otherCandidate := testSandboxIdentityRecord("/g", "rk", "sb-other-candidate", "other-node", StateReserved)
	otherCandidate.CreateCredentials = cloneCreateCredentials(winner)
	if _, err := reg.stores.PutSandbox(ctx, otherCandidate); err != nil {
		t.Fatal(err)
	}
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		if placements == 1 {
			if _, err := reg.stores.PutSandbox(ctx, rolledBackState); err != nil {
				t.Fatal(err)
			}
		}
		return &Placement{NodeID: "n1", TemplateRef: testTemplateRef, APISecretFingerprint: testAPIFingerprint}, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var commandConfig map[string]string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		commandConfig = cloneStringMap(cmd.Config)
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})

	requestCredentials := &sandboxcfg.Credentials{EnvdAccessToken: "request-winner"}
	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, requestCredentials, nil, true); err != nil {
		t.Fatal(err)
	}
	if placements != 2 {
		t.Fatalf("placement attempts=%d, want CAS retry", placements)
	}
	if commandConfig[sandboxcfg.NsCredentials] != `{"envd_access_token":"request-winner"}` {
		t.Fatalf("CAS retry retained rolled-back credentials: %+v", commandConfig)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.CreateCredentials == nil || *stored.CreateCredentials != *requestCredentials {
		t.Fatalf("post-rollback winner=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReservedWithoutCreateCredentialsRejectsLaterFallback(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if _, err := reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "rk", "sb-winner", "winner-node", StateReserved)); err != nil {
		t.Fatal(err)
	}
	reg.SetPlacer(placementWithToken("n1"))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	var commandConfig map[string]string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		commandConfig = cloneStringMap(cmd.Config)
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})

	later := &sandboxcfg.Credentials{EnvdAccessToken: "later-envd"}
	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, later, nil, true); err != nil {
		t.Fatal(err)
	}
	if _, found := commandConfig[sandboxcfg.NsCredentials]; found {
		t.Fatalf("later fallback reached create command: %+v", commandConfig)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || stored.CreateCredentials != nil {
		t.Fatalf("later fallback replaced absent winner: route=%+v found=%v err=%v", stored, found, err)
	}
}

func TestRollbackReservedRestoresOrDeletesCreateCredentialsAtomically(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testSandboxIdentityRecord("/restore", "rk", "sb-original", "original-node", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	reserved := testSandboxIdentityRecord("/restore", "rk", "sb-candidate", "candidate-node", StateReserved)
	reserved.CreateCredentials = &sandboxcfg.Credentials{EnvdAccessToken: "candidate"}
	rev, err := reg.stores.PutSandbox(ctx, reserved)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.rollbackReservedAtRevision(ctx, "/restore", "rk", reserved, rev, original, true, false) {
		t.Fatal("rollback did not restore the original route")
	}
	restored, _, found, err := reg.stores.GetSandbox(ctx, "/restore", "rk")
	if err != nil || !found || restored.CreateCredentials != nil {
		t.Fatalf("restored route=%+v found=%v err=%v", restored, found, err)
	}

	fresh := testSandboxIdentityRecord("/delete", "rk", "sb-new", "new-node", StateReserved)
	fresh.CreateCredentials = &sandboxcfg.Credentials{EnvdAccessToken: "delete-with-route"}
	rev, err = reg.stores.PutSandbox(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.rollbackReservedAtRevision(ctx, "/delete", "rk", fresh, rev, nil, false, false) {
		t.Fatal("rollback did not delete a fresh route")
	}
	if route, _, found, err := reg.stores.GetSandbox(ctx, "/delete", "rk"); err != nil || found {
		t.Fatalf("fresh route survived rollback: route=%+v found=%v err=%v", route, found, err)
	}
}

func TestReadyReplacementCASRetryPreservesConcurrentCredentialBinding(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	orig := testE2BSandboxRecord("/g", "rk", "same-sandbox", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "candidate"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "candidate"})
	currentFingerprint := strings.Repeat("b", 64)
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		if placements == 1 {
			concurrent := *orig
			concurrent.APISecretFingerprint = currentFingerprint
			if _, err := reg.stores.PutSandbox(ctx, &concurrent); err != nil {
				t.Fatal(err)
			}
		}
		return &Placement{NodeID: "candidate", APISecretFingerprint: testAPIFingerprint}, nil
	}))

	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, nil, orig, true); err != nil {
		t.Fatal(err)
	}
	if placements != 1 {
		t.Fatalf("placements=%d, want one stale placement attempt", placements)
	}
	route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || route.APISecretFingerprint != currentFingerprint || route.State != StateReady {
		t.Fatalf("concurrent route=%+v found=%v err=%v", route, found, err)
	}
}

func TestReadyReplacementRetainsOldOwnershipAndRollbackDropsNewRef(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacer(placementWithToken("new"))
	orig := testE2BSandboxRecord("/g", "rk", "sb-old", "old", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, orig); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"old", "new"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "old", testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	var replacementSID string
	reg.addNode(&fakeConn{nodeID: "new", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdCreate {
			replacementSID = cmd.SID
			reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		}
	}})
	if err := reg.placeAndCreate(ctx, "/g", "rk", nil, nil, orig, true); err != nil {
		t.Fatalf("placeAndCreate: %v", err)
	}
	reserved, reservedRev, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || reserved.State != StateReserved || reserved.SandboxID != orig.SandboxID || reserved.NodeSandboxID != replacementSID || reserved.SandboxGeneration != 1 || reserved.NextSandboxGeneration != 2 {
		t.Fatalf("replacement route=%+v found=%v err=%v", reserved, found, err)
	}
	oldNode, found, err := reg.stores.GetNode(ctx, "old")
	if err != nil || !found || len(oldNode.Sandboxes) != 1 || oldNode.Sandboxes[0].SandboxID != "sb-old" {
		t.Fatalf("old node after replacement=%+v found=%v err=%v", oldNode, found, err)
	}
	newNode, found, err := reg.stores.GetNode(ctx, "new")
	if err != nil || !found || len(newNode.Sandboxes) != 1 || newNode.Sandboxes[0].SandboxID != orig.SandboxID || newNode.Sandboxes[0].NodeSandboxID != replacementSID || newNode.Sandboxes[0].SandboxGeneration != 1 {
		t.Fatalf("new node after replacement=%+v found=%v err=%v", newNode, found, err)
	}

	if !reg.rollbackReservedAtRevision(ctx, "/g", "rk", reserved, reservedRev, orig, true, true) {
		t.Fatal("rollback did not restore ready replacement")
	}
	restored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || restored.SandboxID != "sb-old" || restored.NodeSandboxID != orig.NodeSandboxID || restored.NodeID != "old" || restored.NextSandboxGeneration != 2 {
		t.Fatalf("restored route=%+v found=%v err=%v", restored, found, err)
	}
	oldNode, found, err = reg.stores.GetNode(ctx, "old")
	if err != nil || !found || len(oldNode.Sandboxes) != 1 || oldNode.Sandboxes[0].SandboxID != "sb-old" {
		t.Fatalf("restored old node=%+v found=%v err=%v", oldNode, found, err)
	}
	newNode, found, err = reg.stores.GetNode(ctx, "new")
	if err != nil || !found || len(newNode.Sandboxes) != 0 {
		t.Fatalf("new node after rollback=%+v found=%v err=%v", newNode, found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "new", replacementSID); err != nil || found {
		t.Fatalf("rollback retained replacement owner ref: found=%v err=%v", found, err)
	}
}

func TestStaleNodeDeleteDoesNotDeleteReplacementGeneration(t *testing.T) {
	tests := []struct {
		name   string
		report func(context.Context, *Registry)
	}{
		{name: "delete by SID", report: func(ctx context.Context, reg *Registry) {
			reg.applyDeleteBySID(ctx, "n1", EncodeNodeSandboxID("same-sandbox", 0))
		}},
		{name: "dead route", report: func(ctx context.Context, reg *Registry) {
			reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b",
				SandboxID: EncodeNodeSandboxID("same-sandbox", 0), State: routesync.StateDead,
			})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			current := testE2BSandboxRecord("/g", "rk", "same-sandbox", "n1", StateReady)
			current.SandboxGeneration = 1
			current.NodeSandboxID = EncodeNodeSandboxID(current.SandboxID, 1)
			current.NextSandboxGeneration = 2
			if _, err := reg.stores.PutSandbox(ctx, current); err != nil {
				t.Fatal(err)
			}
			if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
				t.Fatal(err)
			}
			currentRef := testNodeSandboxRef("/g", "rk", "same-sandbox", "e2b", testAPIFingerprint)
			currentRef.SandboxGeneration, currentRef.NodeSandboxID = 1, current.NodeSandboxID
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", currentRef); err != nil {
				t.Fatal(err)
			}
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "same-sandbox", "e2b", testAPIFingerprint)); err != nil {
				t.Fatal(err)
			}

			tt.report(ctx, reg)
			got, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found || got.SandboxID != "same-sandbox" || got.NodeSandboxID != current.NodeSandboxID || got.SandboxGeneration != 1 {
				t.Fatalf("replacement route=%+v found=%v err=%v", got, found, err)
			}
			node, found, err := reg.stores.GetNode(ctx, "n1")
			if err != nil || !found || len(node.Sandboxes) != 1 || node.Sandboxes[0].NodeSandboxID != current.NodeSandboxID {
				t.Fatalf("replacement node ref=%+v found=%v err=%v", node, found, err)
			}
			if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", EncodeNodeSandboxID("same-sandbox", 0)); err != nil || found {
				t.Fatalf("stale owner ref found=%v err=%v", found, err)
			}
		})
	}
}

func TestLateRouteEventsDoNotCrossCredentialBinding(t *testing.T) {
	for _, state := range []string{routesync.StateDead, routesync.StateRunning} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			owner := &recordingNodeOwner{allow: true}
			reg.SetNodeOwner(owner)
			oldFingerprint := testAPIFingerprint
			currentFingerprint := strings.Repeat("b", 64)
			if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
				t.Fatal(err)
			}
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "same-sandbox", "e2b", oldFingerprint)); err != nil {
				t.Fatal(err)
			}
			current := testSandboxIdentityRecord("/g", "rk", "same-sandbox", "n1", StateReady)
			current.SandboxGeneration = 1
			current.NodeSandboxID = EncodeNodeSandboxID(current.SandboxID, 1)
			current.NextSandboxGeneration = 2
			current.APISecretFingerprint = currentFingerprint
			if _, err := reg.stores.PutSandbox(ctx, current); err != nil {
				t.Fatal(err)
			}

			reg.applyRoute(ctx, "n1", &routesync.RouteEntry{Profile: "e2b", SandboxID: EncodeNodeSandboxID("same-sandbox", 0), State: state})

			route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found || route.APISecretFingerprint != currentFingerprint {
				t.Fatalf("current route=%+v found=%v err=%v", route, found, err)
			}
			if state == routesync.StateRunning {
				want := "n1/" + EncodeNodeSandboxID("same-sandbox", 0) + "/" + oldFingerprint
				if len(owner.deleted) != 1 || owner.deleted[0] != want {
					t.Fatalf("orphan deletes=%v, want [%s]", owner.deleted, want)
				}
			}
		})
	}
}

func TestReplaceOnRejectSucceeds(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	const credentialsJSON = `{"envd_access_token":"same-on-every-attempt"}`
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		for _, nodeID := range req.ExcludeNodeIDs {
			if nodeID == "n1" {
				return &Placement{NodeID: "n2", APISecretFingerprint: testAPIFingerprint}, nil
			}
		}
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}))
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n2"})
	creates := 0
	var createCredentials []string
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		creates++
		createCredentials = append(createCredentials, cmd.Config[sandboxcfg.NsCredentials])
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "transient"})
	}})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		creates++
		createCredentials = append(createCredentials, cmd.Config[sandboxcfg.NsCredentials])
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
		route.SandboxID = cmd.SID
		go reg.applyRoute(context.Background(), "n2", &route)
	}})

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsCredentials: credentialsJSON,
	}))
	if err != nil {
		t.Fatalf("reserve should succeed after excluding the rejected node: %v", err)
	}
	if res.Route.NodeID != "n2" || creates != 2 {
		t.Fatalf("expected n1 reject then n2 success; got %d creates, res=%+v", creates, res)
	}
	if len(createCredentials) != 2 || createCredentials[0] != credentialsJSON || createCredentials[1] != credentialsJSON {
		t.Fatalf("re-place changed create credentials: %v", createCredentials)
	}
}

func TestReserveSandboxSkipsDisconnectedCatalogNode(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var sawExclusion bool
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		for _, nodeID := range req.ExcludeNodeIDs {
			if nodeID == "stale" {
				sawExclusion = true
				return &Placement{NodeID: "live", APISecretFingerprint: testAPIFingerprint}, nil
			}
		}
		return &Placement{NodeID: "stale", APISecretFingerprint: testAPIFingerprint}, nil
	}))
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "stale"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "live"}); err != nil {
		t.Fatal(err)
	}
	original := testE2BSandboxRecord("/g", "rk", "sb-stale", "stale", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "live", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdCreate {
			return
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testRouteFromRecord(original, cmd.SID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "live", &route)
	}})

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
	if err != nil {
		t.Fatalf("ReserveSandbox: %v", err)
	}
	if res.Route.NodeID != "live" || !sawExclusion {
		t.Fatalf("result=%+v saw stale exclusion=%v", res, sawExclusion)
	}
}

func TestReserveSandboxDoesNotExcludeOnConnectionCheckError(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	checkErr := errors.New("node owner temporarily unavailable")
	owner := &remoteRouteWriteOwner{
		node: &NodeRecord{NodeID: "n1"}, connectedErr: checkErr,
	}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, checkErr) {
		t.Fatalf("ReserveSandbox err=%v, want connection check error", err)
	}
	if placements != 1 || owner.commands != 0 {
		t.Fatalf("connection check error retried/executed: placements=%d commands=%d", placements, owner.commands)
	}
}

func TestReserveSandboxDoesNotRePlaceAfterCreateAckTimeout(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}), 30*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	owner := &remoteRouteWriteOwner{
		node:   &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:12345"},
		ackErr: context.DeadlineExceeded,
	}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReserveSandbox err=%v, want park timeout", err)
	}
	if placements != 1 || owner.commands != 1 {
		t.Fatalf("ambiguous create was retried: placements=%d commands=%d", placements, owner.commands)
	}
}

func TestReserveSandboxParksAfterAmbiguousCommandError(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
	}), 30*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	owner := &remoteRouteWriteOwner{
		node:   &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:12345"},
		ackErr: errors.New("node-owner response lost"),
	}
	reg.SetNodeOwner(owner)

	if _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReserveSandbox err=%v, want park timeout after ambiguous delivery", err)
	}
	if placements != 1 || owner.commands != 1 {
		t.Fatalf("ambiguous create was retried: placements=%d commands=%d", placements, owner.commands)
	}
}

func TestReservePausedResume(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"})
	want := testEnvdAccessToken
	// Resume keeps both the stable public ID and the current node-local ID, and
	// ignores create-only credential overrides.
	original := testE2BSandboxRecord("/g", "rk", "stable-auth-sid", "n1", StatePaused)
	reg.stores.PutSandbox(ctx, original)

	conn := &fakeConn{nodeID: "n1"}
	conn.onCmd = func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdConnect {
			return
		}
		if cmd.APISecretFingerprint != testAPIFingerprint {
			t.Errorf("connect fingerprint=%q", cmd.APISecretFingerprint)
		}
		if cmd.Profile != "e2b" || cmd.Cluster == nil || cmd.Cluster.Group != "/g" ||
			cmd.Cluster.RouteKey != "rk" || cmd.Cluster.StableID != original.StableID {
			t.Errorf("connect identity context=%+v", cmd)
		}
		if len(cmd.Config) != 0 {
			t.Errorf("connect carried create-only config")
		}
		if cmd.Memory != nil {
			t.Errorf("implicit connect memory presence = %v, want nil", cmd.Memory)
		}
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		route := testRouteFromRecord(original, original.NodeSandboxID, routesync.StateRunning)
		go reg.applyRoute(context.Background(), "n1", &route)
	}
	reg.addNode(conn)

	res, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", map[string]string{
		sandboxcfg.NsCredentials: `{"service_secret":"` + strings.Repeat("3", 64) + `"}`,
	}))
	if err != nil {
		t.Fatalf("resume reserve: %v", err)
	}
	if res.Route.SandboxID != original.SandboxID || res.Route.NodeSandboxID != original.NodeSandboxID || res.Route.Profile != "e2b" || res.Route.StableID != original.StableID ||
		res.Route.EnvdAccessToken != want || !constantTimeStringEqual(res.Route.ForwardAccessToken, original.ForwardAccessToken) {
		t.Fatalf("resume result: %+v", res)
	}
}

func TestNodeListWatchProjectsLowFrequencyFields(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	lastBeat := time.Now().Unix()
	reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "n1", Labels: map[string]string{"pool": "p"}, Capacity: 10,
		BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{MaxBuilds: 16, Resources: &routesync.BuildResources{CPU: 64000}},
		BuildExecutionCapacity:    &routesync.BuildAdmissionLimit{MaxBuilds: 4, Resources: &routesync.BuildResources{CPU: 16000}},
		DataEndpoint:              "10.0.0.1:8443", RuntimeDigest: "rt1", LastHeartbeatUnix: lastBeat,
		Allocated: 99, Pool: 100, Counts: 7,
		BuildRegistrationUsage: &routesync.BuildAdmissionUsage{Builds: 3},
		BuildExecutionUsage:    &routesync.BuildAdmissionUsage{Builds: 2},
	})

	mux := http.NewServeMux()
	reg.ServePlacerLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + PlacerLinkNodeListWatchPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if rs := readViewFrame(t, resp.Body); rs.Type != "reset" {
		t.Fatalf("expected reset, got %+v", rs)
	}
	put := readViewFrame(t, resp.Body)
	if put.Type != "put" || put.Key != "n1" {
		t.Fatalf("node_list put: %+v", put)
	}
	var raw map[string]any
	if err := json.Unmarshal(put.Value, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["node_id"] != "n1" || raw["data_endpoint"] == "" || raw["runtime_digest"] != "rt1" {
		t.Fatalf("node_list value = %v", raw)
	}
	if _, ok := raw["build_registration_capacity"].(map[string]any); !ok {
		t.Fatalf("node_list missing build registration capacity: %v", raw)
	}
	for _, field := range []string{"last_heartbeat_unix", "allocated", "pool", "counts", "build_registration_usage", "build_execution_usage"} {
		if _, ok := raw[field]; ok {
			t.Fatalf("node_list exposed high-frequency field %q: %v", field, raw)
		}
	}
	if bm := readViewFrame(t, resp.Body); bm.Type != "bookmark" {
		t.Fatalf("expected bookmark, got %+v", bm)
	}
}

func TestNodeListWatchTokenResumesOnlyMatchingMembershipLabel(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacerReadyLabel("registry.1.test")
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServePlacerLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + PlacerLinkNodeListWatchPath)
	if err != nil {
		t.Fatal(err)
	}
	reset := readViewFrame(t, resp.Body)
	if reset.Type != "reset" {
		t.Fatalf("initial first frame = %+v, want reset", reset)
	}
	_ = readViewFrame(t, resp.Body) // n1 snapshot
	bookmark := readViewFrame(t, resp.Body)
	if bookmark.Type != "bookmark" || bookmark.Token == "" {
		t.Fatalf("initial bookmark missing watch token: %+v", bookmark)
	}

	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n2", LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	live := readViewFrame(t, resp.Body)
	if live.Type != "put" || live.Key != "n2" || live.Token == "" {
		t.Fatalf("live event should carry token, got %+v", live)
	}
	resp.Body.Close()

	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n3", LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(srv.URL + PlacerLinkNodeListWatchPath + "?from=" + url.QueryEscape(live.Token))
	if err != nil {
		t.Fatal(err)
	}
	delta := readViewFrame(t, resp.Body)
	if delta.Type != "put" || delta.Key != "n3" {
		t.Fatalf("live token should replay later delta without reset, got %+v", delta)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + PlacerLinkNodeListWatchPath + "?from=" + url.QueryEscape(bookmark.Token))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	delta = readViewFrame(t, resp.Body)
	if delta.Type != "put" || delta.Key != "n2" {
		t.Fatalf("matching token should replay delta without reset, got %+v", delta)
	}

	resp2, err := http.Get(srv.URL + PlacerLinkNodeListWatchPath + "?from=" + url.QueryEscape("registry.2.test:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if got := readViewFrame(t, resp2.Body); got.Type != "reset" {
		t.Fatalf("mismatched token should force full snapshot reset, got %+v", got)
	}
}

func TestNodeListWatchTokenResetsAcrossRegistryRestart(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServePlacerLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + PlacerLinkNodeListWatchPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := readViewFrame(t, resp.Body); got.Type != "reset" {
		t.Fatalf("initial frame=%+v, want reset", got)
	}
	_ = readViewFrame(t, resp.Body)
	bookmark := readViewFrame(t, resp.Body)
	resp.Body.Close()
	if bookmark.Token == "" {
		t.Fatalf("bookmark missing token: %+v", bookmark)
	}

	restarted := testReg(t)
	restartedMux := http.NewServeMux()
	restarted.ServePlacerLink(restartedMux)
	restartedSrv := httptest.NewServer(restartedMux)
	defer restartedSrv.Close()
	resp, err = http.Get(restartedSrv.URL + PlacerLinkNodeListWatchPath + "?from=" + url.QueryEscape(bookmark.Token))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := readViewFrame(t, resp.Body); got.Type != "reset" {
		t.Fatalf("stale token across restart frame=%+v, want reset", got)
	}
}

func TestSelectorPatchHTTPRejectsMissingImportSourceLease(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServePlacerLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body, err := json.Marshal(&routesync.SelectorPatch{
		Group: "/g", NodeIDs: []string{"n1"},
		APISecretFingerprint: "missing", ManifestKeyType: clusterstate.SecretInline, ManifestKey: "mk-missing",
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+PlacerLinkSelectorPatchPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.KeyPairs) != 0 {
		t.Fatalf("missing lease patch changed key cache: %+v", node.KeyPairs)
	}
}

func TestRunCompactorWithNilLogger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := New(NewStores(), nil, time.Second, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.RunCompactor(ctx, time.Millisecond)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("compactor did not stop")
	}
}

func TestNodeListWatchIgnoresHeartbeatWatermarks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg := testReg(t)
	now := time.Now().Unix()
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", LastHeartbeatUnix: now}); err != nil {
		t.Fatal(err)
	}
	rev, err := reg.stores.NodeListRev(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := reg.stores.WatchNodeList(ctx, rev)
	if err != nil {
		t.Fatal(err)
	}

	reg.updateHeartbeat(ctx, "n1", &routesync.Heartbeat{Allocated: 100, Pool: 200, Counts: 5})
	select {
	case ev := <-ch:
		t.Fatalf("watermark-only heartbeat emitted node_list event: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	reg.updateHeartbeat(ctx, "n1", &routesync.Heartbeat{Allocated: 100, Pool: 200, Counts: 5, Draining: true})
	select {
	case ev := <-ch:
		if ev.Type != WatchEventPut || ev.Key != "n1" {
			t.Fatalf("draining update event=%+v", ev)
		}
		var raw map[string]any
		if err := json.Unmarshal(ev.Value, &raw); err != nil {
			t.Fatal(err)
		}
		if raw["draining"] != true {
			t.Fatalf("draining update value=%v", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("draining heartbeat did not emit node_list event")
	}
}

func TestNodeLinkResumeTokenReturnedOnReconnect(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	open := func(bookmark string) string {
		t.Helper()
		tr := &http2.Transport{AllowHTTP: true}
		tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
		defer tr.CloseIdleConnections()
		pr, pw := io.Pipe()
		defer pw.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://registry"+routesync.NodeLinkPath, pr)
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() {
			errc <- routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &routesync.NodeRegister{NodeID: "n1"}})
		}()
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
		msg, err := routesync.ReadMsg(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if bookmark != "" {
			if err := routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeBookmark, RevToken: bookmark}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				node, found, _ := reg.stores.GetNode(ctx, "n1")
				got := ""
				if found {
					got = node.ResumeToken
				}
				if got == bookmark {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("registry did not record resume token %q", bookmark)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		if msg.Type != routesync.TypeHello || msg.Hello == nil {
			t.Fatalf("first frame=%+v, want hello", msg)
		}
		return msg.Hello.ResumeFrom
	}

	if got := open("node-fp:7"); got != "" {
		t.Fatalf("first connect resume_from=%q, want empty", got)
	}
	if got := open(""); got != "node-fp:7" {
		t.Fatalf("reconnect resume_from=%q, want node-fp:7", got)
	}
}

func readViewFrame(t *testing.T, r io.Reader) *ViewEvent {
	t.Helper()
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, binary.LittleEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	var ev ViewEvent
	if err := json.Unmarshal(buf, &ev); err != nil {
		t.Fatal(err)
	}
	return &ev
}

func TestSandboxKeyNoCollision(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// Nested groups: a per-group range over /a must NOT bleed into /a/b.
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/a", "x", "sb-a", "n1", StateReady))
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/a/b", "y", "sb-ab", "n1", StateReady))
	var got []string
	reg.stores.RangeSandboxes(ctx, "/a", func(s *SandboxRecord) error { got = append(got, s.SandboxID); return nil })
	if len(got) != 1 || got[0] != "sb-a" {
		t.Fatalf("RangeSandboxes(/a) leaked across groups: %v (want [sb-a])", got)
	}
	// Aliasing across the group/route_key boundary must not overwrite:
	// (/a, "b/y") and (/a/b, "y") must be distinct records.
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/a", "b/y", "sb-1", "n1", StateReady))
	r1, _, _, _ := reg.stores.GetSandbox(ctx, "/a", "b/y")
	rab, _, _, _ := reg.stores.GetSandbox(ctx, "/a/b", "y")
	if r1 == nil || r1.SandboxID != "sb-1" || rab == nil || rab.SandboxID != "sb-ab" {
		t.Fatalf("key aliasing overwrote a different tenant: (/a,b/y)=%v (/a/b,y)=%v", r1, rab)
	}
}

func TestRangeSandboxesWarmsGroupViewAndPublishesRouteWatch(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"r1", "r2", "r3"}, 3, 1, 1, 1)
	stores := cluster["r1"]
	seed := testSandboxIdentityRecord("/g", "rk", "sb-warm", "n1", StateReady)
	seedRouteShardRecord(t, ctx, cluster["r2"], seed, shardkv.Ballot{Round: 7, Writer: shardkv.MemberID("r2")}, 1)
	seedRouteShardRecord(t, ctx, cluster["r3"], seed, shardkv.Ballot{Round: 7, Writer: shardkv.MemberID("r2")}, 1)

	ch, err := stores.WatchRouteGroup(ctx, "/g", 0)
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := stores.RangeSandboxes(ctx, "/g", func(s *SandboxRecord) error {
		got = append(got, s.SandboxID)
		return nil
	}); err != nil {
		t.Fatalf("range warm: %v", err)
	}
	if len(got) != 1 || got[0] != "sb-warm" {
		t.Fatalf("range got %v, want [sb-warm]", got)
	}
	select {
	case ev := <-ch:
		if ev.Type != WatchEventPut || ev.Key != "rk" {
			t.Fatalf("route watch event=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("route watch did not receive warmup repair")
	}
}

func TestNodeFullSnapshotDeletesMissingSandboxRefs(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	keepRef := testNodeSandboxRef("/g", "keep", "sb-keep", "e2b", testAPIFingerprint)
	goneRef := testNodeSandboxRef("/g", "gone", "sb-gone", "e2b", testAPIFingerprint)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", Sandboxes: []clusterstate.NodeSandboxRef{keepRef, goneRef}}); err != nil {
		t.Fatal(err)
	}
	_, _ = reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "keep", "sb-keep", "n1", StateReady))
	_, _ = reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "gone", "sb-gone", "n1", StateReady))

	reg.applyNodeFullSnapshot(ctx, "n1", []clusterstate.NodeSandboxRef{keepRef, goneRef}, map[string]struct{}{keepRef.NodeSandboxID: {}})

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "gone"); found {
		t.Fatal("full snapshot should delete route missing from node range")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "keep"); !found {
		t.Fatal("full snapshot deleted present route")
	}
	node, found, err := reg.stores.GetNode(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node found=%v err=%v", found, err)
	}
	if len(node.Sandboxes) != 1 || node.Sandboxes[0].RouteKey != "keep" {
		t.Fatalf("node refs after full snapshot=%+v", node.Sandboxes)
	}
}

func TestNodeFullSnapshotKeepsAssignmentAddedAfterBaseline(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	baseline := testNodeSandboxRef("/old", "rk", "same-sandbox", "e2b", testAPIFingerprint)
	current := testNodeSandboxRef("/new", "rk", "same-sandbox", "e2b", testAPIFingerprint)
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", baseline); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.RemoveNodeSandboxRef(ctx, "n1", baseline.NodeSandboxID); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", current); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.PutSandbox(ctx, testSandboxIdentityRecord(current.Group, current.RouteKey, current.SandboxID, "n1", StateReserved)); err != nil {
		t.Fatal(err)
	}

	reg.applyNodeFullSnapshot(ctx, "n1", []clusterstate.NodeSandboxRef{baseline}, nil)

	ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", current.NodeSandboxID)
	if err != nil || !found || ref.Group != current.Group {
		t.Fatalf("current ref=%+v found=%v err=%v", ref, found, err)
	}
	if route, _, found, err := reg.stores.GetSandbox(ctx, current.Group, current.RouteKey); err != nil || !found || route.SandboxID != current.SandboxID || route.NodeSandboxID != current.NodeSandboxID {
		t.Fatalf("current route=%+v found=%v err=%v", route, found, err)
	}
}

func TestNodeFullSnapshotKeepsReboundCredentialIdentity(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	baseline := testNodeSandboxRef("/g", "rk", "same-sandbox", "e2b", testAPIFingerprint)
	current := baseline
	current.APISecretFingerprint = strings.Repeat("b", 64)
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", baseline); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.RemoveNodeSandboxRef(ctx, "n1", baseline.NodeSandboxID); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", current); err != nil {
		t.Fatal(err)
	}
	currentRoute := testSandboxIdentityRecord(current.Group, current.RouteKey, current.SandboxID, "n1", StateReserved)
	currentRoute.APISecretFingerprint = current.APISecretFingerprint
	if _, err := reg.stores.PutSandbox(ctx, currentRoute); err != nil {
		t.Fatal(err)
	}

	reg.applyNodeFullSnapshot(ctx, "n1", []clusterstate.NodeSandboxRef{baseline}, nil)

	ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", current.NodeSandboxID)
	if err != nil || !found || ref.APISecretFingerprint != current.APISecretFingerprint {
		t.Fatalf("current ref=%+v found=%v err=%v", ref, found, err)
	}
	route, _, found, err := reg.stores.GetSandbox(ctx, current.Group, current.RouteKey)
	if err != nil || !found || route.APISecretFingerprint != current.APISecretFingerprint {
		t.Fatalf("current route=%+v found=%v err=%v", route, found, err)
	}
}

func TestStoresRouteLinkUsesQuorumRepair(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"r1", "r2", "r3"}, 3, 1, 1, 1)
	stores := cluster["r1"]
	reg := New(stores, nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	seed := testSandboxIdentityRecord("/g", "rk", "sb-q", "n1", StateReady)
	seedRouteShardRecord(t, ctx, cluster["r1"], seed, shardkv.Ballot{Round: 7, Writer: shardkv.MemberID("seed")}, 3)
	seedRouteShardRecord(t, ctx, cluster["r3"], seed, shardkv.Ballot{Round: 7, Writer: shardkv.MemberID("seed")}, 3)
	if !localShardHasRecord(t, ctx, cluster["r1"], shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard("/g"), clusterstate.RecordSetRouteSandbox, clusterstate.RouteSandboxRecordKey("rk")) {
		t.Fatal("seed route did not land on local shard")
	}

	got, rev, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("GetSandbox found=%v err=%v", found, err)
	}
	if got.SandboxID != "sb-q" || got.NodeSandboxID != EncodeNodeSandboxID("sb-q", 0) || got.State != StateReady || rev == 0 {
		t.Fatalf("route from quorum = %+v rev=%d", got, rev)
	}
	if !localShardHasRecord(t, ctx, cluster["r2"], shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard("/g"), clusterstate.RecordSetRouteSandbox, clusterstate.RouteSandboxRecordKey("rk")) {
		t.Fatalf("lagging route owner was not repaired")
	}
}

func TestStoresNodeLinkUsesQuorumRepair(t *testing.T) {
	ctx := context.Background()
	cluster := newShardStoreCluster(t, []string{"n1", "n2", "n3"}, 1, 3, 1, 1)
	stores := cluster["n1"]
	reg := New(stores, nil, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	seed := &NodeRecord{NodeID: "node-q", Labels: map[string]string{"pool": "p"}, DataEndpoint: "10.0.0.1:8443"}
	seedNodeProfileShardRecord(t, ctx, cluster["n1"], seed, shardkv.Ballot{Round: 9, Writer: shardkv.MemberID("seed")}, 4)
	seedNodeProfileShardRecord(t, ctx, cluster["n3"], seed, shardkv.Ballot{Round: 9, Writer: shardkv.MemberID("seed")}, 4)
	if !localShardHasRecord(t, ctx, cluster["n1"], shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard("node-q"), clusterstate.RecordSetNodeProfile, clusterstate.NodeLinkProfileRecord) {
		t.Fatal("seed node did not land on local shard")
	}

	got, found, err := reg.stores.GetNode(ctx, "node-q")
	if err != nil || !found {
		t.Fatalf("GetNode found=%v err=%v", found, err)
	}
	if got.DataEndpoint != "10.0.0.1:8443" || got.Labels["pool"] != "p" {
		t.Fatalf("node from quorum = %+v", got)
	}
	if !localShardHasRecord(t, ctx, cluster["n2"], shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard("node-q"), clusterstate.RecordSetNodeProfile, clusterstate.NodeLinkProfileRecord) {
		t.Fatalf("lagging node owner was not repaired")
	}
}

func seedRouteShardRecord(t *testing.T, ctx context.Context, store *Stores, route *SandboxRecord, ballot shardkv.Ballot, rev uint64) {
	t.Helper()
	value, err := clusterstate.EncodeShardValue(route)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ShardStore().Handle(ctx, shardkv.Request{
		Op:        shardkv.OpRepair,
		Namespace: shardkv.Namespace(clusterstate.NamespaceRouteLink),
		Shard:     clusterstate.RouteLinkShard(route.Group),
		RecordSet: clusterstate.RecordSetRouteSandbox,
		Record: shardkv.Record{
			Namespace: shardkv.Namespace(clusterstate.NamespaceRouteLink),
			Shard:     clusterstate.RouteLinkShard(route.Group),
			RecordSet: clusterstate.RecordSetRouteSandbox,
			Key:       clusterstate.RouteSandboxRecordKey(route.RouteKey),
			Value:     value,
			Meta:      shardkv.RecordMeta{Ballot: ballot, Rev: rev, UpdatedAt: time.Now()},
		},
	})
	if err != nil {
		t.Fatalf("seed route shard: %v", err)
	}
}

func seedNodeProfileShardRecord(t *testing.T, ctx context.Context, store *Stores, node *NodeRecord, ballot shardkv.Ballot, rev uint64) {
	t.Helper()
	value, err := clusterstate.EncodeShardValue(nodeProfileFromRegistry(node))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ShardStore().Handle(ctx, shardkv.Request{
		Op:        shardkv.OpRepair,
		Namespace: shardkv.Namespace(clusterstate.NamespaceNodeLink),
		Shard:     clusterstate.NodeLinkShard(node.NodeID),
		RecordSet: clusterstate.RecordSetNodeProfile,
		Record: shardkv.Record{
			Namespace: shardkv.Namespace(clusterstate.NamespaceNodeLink),
			Shard:     clusterstate.NodeLinkShard(node.NodeID),
			RecordSet: clusterstate.RecordSetNodeProfile,
			Key:       clusterstate.NodeLinkProfileRecord,
			Value:     value,
			Meta:      shardkv.RecordMeta{Ballot: ballot, Rev: rev, UpdatedAt: time.Now()},
		},
	})
	if err != nil {
		t.Fatalf("seed node shard: %v", err)
	}
}

func TestMergeConfig(t *testing.T) {
	merged := mergeConfig(map[string]string{"a": "1", "b": "2"}, map[string]string{"b": "X", "c": "3"})
	if merged["a"] != "1" || merged["b"] != "X" || merged["c"] != "3" {
		t.Fatalf("mergeConfig: %v", merged)
	}
}

func TestSweepKeepsInflightReserved(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	liveRef := testNodeSandboxRef("/g", "live", "sb-r", "e2b", testAPIFingerprint)
	staleRef := testNodeSandboxRef("/g", "stale", "sb-s", "e2b", testAPIFingerprint)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(), Sandboxes: []clusterstate.NodeSandboxRef{liveRef, staleRef}})
	// A RESERVED row whose single-flight is still in flight must survive the sweep.
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "live", "sb-r", "dead", StateReserved))
	reg.mu.Lock()
	reg.inflight[flightKey("/g", "live")] = &reserveCall{done: make(chan struct{})}
	reg.mu.Unlock()
	// A RESERVED row with no in-flight reserve is stale → swept.
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "stale", "sb-s", "dead", StateReserved))

	reg.sweepNode(ctx, "dead", 30*time.Second)

	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "live"); !found {
		t.Fatal("swept a RESERVED row owned by an in-flight reserve")
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "dead", liveRef.NodeSandboxID); err != nil || !found || ref.RouteKey != "live" {
		t.Fatalf("in-flight ownership ref=%+v found=%v err=%v", ref, found, err)
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "stale"); found {
		t.Fatal("did not sweep a stale RESERVED row on a dead node")
	}
}

func TestClaimedNodeProfileRejectsStaleRuntimeWriters(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	const nodeID = "reaping"
	oldRef := testNodeSandboxRef("/g", "rk", "sb-old", "e2b", testAPIFingerprint)
	if err := reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: nodeID, LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(),
		Sandboxes: []clusterstate.NodeSandboxRef{oldRef},
	}); err != nil {
		t.Fatal(err)
	}
	stale, found, err := reg.getNodeForLinkUpdate(ctx, nodeID)
	if err != nil || !found || len(stale.Sandboxes) != 1 {
		t.Fatalf("stale node=%+v found=%v err=%v", stale, found, err)
	}
	claimed, err := reg.stores.claimNodeProfileReapShard(ctx, nodeID, stale.Meta.Rev)
	if err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	reg.updateHeartbeat(ctx, nodeID, &routesync.Heartbeat{Allocated: 9})
	reg.updateNodeResume(ctx, nodeID, "stale-resume")
	if _, found, err := reg.stores.GetNodeProfile(ctx, nodeID); err != nil || found {
		t.Fatalf("runtime update recreated claimed profile: found=%v err=%v", found, err)
	}
	stale.DataEndpoint = "stale"
	if _, ok, err := reg.stores.casNodeProfileShard(ctx, stale, stale.Meta.Rev); err != nil || ok {
		t.Fatalf("stale registration CAS ok=%v err=%v", ok, err)
	}
	registered, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: nodeID, DataEndpoint: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if registered.DataEndpoint != "fresh" || len(registered.Sandboxes) != 0 {
		t.Fatalf("registration reused stale node view: %+v", registered)
	}
}

func TestRuntimeProfileWriterDoesNotRetryIntoReconnectGeneration(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	const nodeID = "runtime-generation-fence"
	if _, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: nodeID, DataEndpoint: "old"}); err != nil {
		t.Fatal(err)
	}
	var interleaveErr error
	interleaved := false
	_, found, err := reg.updateNodeProfile(ctx, nodeID, false, func(rec *NodeRecord) {
		if !interleaved {
			interleaved = true
			claimed, claimErr := reg.stores.claimNodeProfileReapShard(ctx, nodeID, rec.Meta.Rev)
			if claimErr != nil || !claimed {
				interleaveErr = fmt.Errorf("claim=%v err=%w", claimed, claimErr)
				return
			}
			_, interleaveErr = reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: nodeID, DataEndpoint: "fresh"})
		}
		rec.ResumeToken = "stale-session"
	})
	if interleaveErr != nil {
		t.Fatal(interleaveErr)
	}
	if !errors.Is(err, shardkv.ErrConflict) || found {
		t.Fatalf("stale runtime writer found=%v err=%v", found, err)
	}
	profile, found, err := reg.stores.GetNodeProfile(ctx, nodeID)
	if err != nil || !found || profile.DataEndpoint != "fresh" || profile.ResumeToken != "" {
		t.Fatalf("fresh profile=%+v found=%v err=%v", profile, found, err)
	}
}

func TestSweepDeadNodes(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// Disconnected node with a stale heartbeat + a READY sandbox → both swept.
	deadRef := testNodeSandboxRef("/g", "rk", "sb-1", "e2b", testAPIFingerprint)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(), Sandboxes: []clusterstate.NodeSandboxRef{deadRef}})
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g", "rk", "sb-1", "dead", StateReady))
	// Connected node with a stale heartbeat → NOT swept (a live channel isn't dead).
	liveRef := testNodeSandboxRef("/g2", "rk", "sb-2", "e2b", testAPIFingerprint)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "live", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(), Sandboxes: []clusterstate.NodeSandboxRef{liveRef}})
	reg.addNode(&fakeConn{nodeID: "live"})
	reg.stores.PutSandbox(ctx, testSandboxIdentityRecord("/g2", "rk", "sb-2", "live", StateReady))

	reg.sweepNode(ctx, "dead", 30*time.Second)
	reg.sweepNode(ctx, "live", 30*time.Second)

	if _, found, _ := reg.stores.GetNode(ctx, "dead"); found {
		t.Fatal("dead node not swept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g", "rk"); found {
		t.Fatal("dead node's sandbox not reset")
	}
	if _, found, _ := reg.stores.GetNode(ctx, "live"); !found {
		t.Fatal("connected node wrongly swept")
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, "/g2", "rk"); !found {
		t.Fatal("connected node's sandbox wrongly reset")
	}
}

func TestSweepDeadNodeDoesNotDeleteReboundCredentialIdentity(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	oldFingerprint := testAPIFingerprint
	currentFingerprint := strings.Repeat("b", 64)
	oldRef := testNodeSandboxRef("/g", "rk", "same-sandbox", "e2b", oldFingerprint)
	if err := reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(),
		Sandboxes: []clusterstate.NodeSandboxRef{oldRef},
	}); err != nil {
		t.Fatal(err)
	}
	current := testSandboxIdentityRecord("/g", "rk", "same-sandbox", "dead", StateReady)
	current.SandboxGeneration = 1
	current.NodeSandboxID = EncodeNodeSandboxID(current.SandboxID, 1)
	current.NextSandboxGeneration = 2
	current.APISecretFingerprint = currentFingerprint
	if _, err := reg.stores.PutSandbox(ctx, current); err != nil {
		t.Fatal(err)
	}

	reg.sweepNode(ctx, "dead", 30*time.Second)

	route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found || route.APISecretFingerprint != currentFingerprint {
		t.Fatalf("current route=%+v found=%v err=%v", route, found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, "dead", oldRef.NodeSandboxID); err != nil || found {
		t.Fatalf("stale ownership ref found=%v err=%v", found, err)
	}
}

func TestSweepDeadNodeRetainsBuildRecordOwnership(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	owner := &recordingNodeOwner{allow: true}
	reg.SetNodeOwner(owner)
	refs := []clusterstate.NodeBuildRef{
		{Group: "/g", BuildID: "active"},
		{Group: "/g", BuildID: "terminal"},
		{Group: "/g", BuildID: "orphan"},
		{Group: "/g", BuildID: "moved"},
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{
		NodeID: "dead", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(), Builds: refs,
	}); err != nil {
		t.Fatal(err)
	}
	for _, build := range []*BuildRecord{
		{Group: "/g", BuildID: "active", NodeID: "dead", State: BuildStarting,
			RegistrationImageRepo: "registry.test/private", RegistrationRegistryAuth: `{"auths":{"registry.test":{"auth":"opaque"}}}`},
		{Group: "/g", BuildID: "terminal", NodeID: "dead", State: BuildReady},
		{Group: "/g", BuildID: "moved", NodeID: "other", State: BuildBuilding},
	} {
		if err := reg.stores.PutBuild(ctx, build); err != nil {
			t.Fatal(err)
		}
	}

	reg.sweepNode(ctx, "dead", 30*time.Second)

	active, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "active")
	if err != nil || !found || active.State != BuildError || active.Reason != "node disconnected" {
		t.Fatalf("active build=%+v found=%v err=%v", active, found, err)
	}
	if active.RegistrationImageRepo != "" || active.RegistrationRegistryAuth != "" {
		t.Fatalf("reaped terminal build retained registration credentials: %+v", active)
	}
	terminal, found, err := reg.stores.GetBuildInGroup(ctx, "/g", "terminal")
	if err != nil || !found || terminal.State != BuildReady {
		t.Fatalf("terminal build=%+v found=%v err=%v", terminal, found, err)
	}
	for _, buildID := range []string{"active", "terminal"} {
		ref, found, err := reg.stores.GetNodeBuildRef(ctx, "dead", buildID)
		if err != nil || !found || ref.Group != "/g" {
			t.Fatalf("retained ref %s=%+v found=%v err=%v", buildID, ref, found, err)
		}
	}
	for _, buildID := range []string{"orphan", "moved"} {
		if ref, found, err := reg.stores.GetNodeBuildRef(ctx, "dead", buildID); err != nil || found {
			t.Fatalf("stale ref %s=%+v found=%v err=%v", buildID, ref, found, err)
		}
	}
}

func TestHTTPPlacerHonorsMinReadyPlacers(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.SetPlacerPeerSource(func(string) []PlacerPeer {
		return []PlacerPeer{{ID: "s1", Advertise: "http://127.0.0.1:1"}}
	})
	placer := NewHTTPPlacerWithMinReady(reg, 1, 2, 100*time.Millisecond)
	if _, err := placer.Place(ctx, PlaceRequest{Group: "/g", RouteKey: "rk"}); err != ErrNoNode {
		t.Fatalf("Place with one ready placer and min_ready=2 err=%v, want ErrNoNode", err)
	}
	if _, err := reg.VerifyAPIKeyWithMinReady(ctx, "/g", "key", 1, 2, 100*time.Millisecond); err != ErrNoNode {
		t.Fatalf("VerifyAPIKey with one ready placer and min_ready=2 err=%v, want ErrNoNode", err)
	}
}

func TestHTTPPlacerPreservesInvalidSandboxConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(routesync.PlaceResult{
			Error: "resource leaves conflict", InvalidConfig: true,
		})
	}))
	defer server.Close()
	placer := &HTTPPlacer{timeout: time.Second, client: server.Client()}
	_, err := placer.placeOne(context.Background(), PlacerPeer{Advertise: server.URL}, PlaceRequest{Group: "/g"})
	if !errors.Is(err, errInvalidSandboxConfig) {
		t.Fatalf("placeOne error = %v, want invalid sandbox config", err)
	}
}

func TestReadyPlacerPeersUseMemberlistSource(t *testing.T) {
	reg := testReg(t)
	reg.SetPlacerReadyLabel("registry.1.test")
	reg.SetPlacerPeerSource(func(label string) []PlacerPeer {
		if label != "registry.1.test" {
			t.Fatalf("ready label passed to source = %q", label)
		}
		return []PlacerPeer{{ID: "s1", Advertise: "http://placer-1", ReadyLabel: label}}
	})
	peers := reg.readyPlacerPeers(time.Minute)
	if len(peers) != 1 || peers[0].ID != "s1" || peers[0].Advertise != "http://placer-1" {
		t.Fatalf("ready placer peers=%+v", peers)
	}
}
