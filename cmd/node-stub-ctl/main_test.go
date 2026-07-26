package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestBuildRegisterReplayPreservesIdentityAndState(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecretFingerprint := strings.Repeat("a", 64)
	cmd := &routesync.Command{
		CmdID: "c1", Kind: routesync.CmdBuildRegister,
		BuildID: "b1", TemplateRef: "transient-1", Profile: "bare", APISecretFingerprint: apiSecretFingerprint,
		Config: map[string]string{"stub.build_result": "timeout"},
	}
	if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
		t.Fatalf("first build_register ack = %+v", got)
	}
	node.mu.Lock()
	node.builds[cmd.BuildID].State = "ready"
	node.mu.Unlock()

	replay := *cmd
	replay.CmdID = "c2"
	if got := node.handleBuildRegister(&replay); got.Status != routesync.AckAccepted {
		t.Fatalf("identical replay ack = %+v", got)
	}
	for name, mutate := range map[string]func(*routesync.Command){
		"profile":  func(c *routesync.Command) { c.Profile = "e2b" },
		"template": func(c *routesync.Command) { c.TemplateRef = "transient-2" },
		"tenant":   func(c *routesync.Command) { c.APISecretFingerprint = strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			conflict := replay
			mutate(&conflict)
			if got := node.handleBuildRegister(&conflict); got.Status != routesync.AckRejected {
				t.Fatalf("conflicting replay ack = %+v", got)
			}
		})
	}

	node.mu.Lock()
	stored := node.builds[cmd.BuildID]
	buildCount := len(node.builds)
	node.mu.Unlock()
	if buildCount != 1 || stored.Profile != "bare" || stored.TemplateID != "transient-1" || stored.State != "ready" {
		t.Fatalf("replay changed stored build: count=%d build=%+v", buildCount, stored)
	}
	svc.mu.Lock()
	eventCount := len(svc.events)
	svc.mu.Unlock()
	if eventCount != 1 {
		t.Fatalf("replay restarted build state machine: events=%d", eventCount)
	}
}

func TestKeyPutStoresPairAndStrictLifecycleUsesAPISecretFingerprint(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1", StrictKeys: true}, svc)
	apiSecret := strings.Repeat("11", 32)
	manifestKey := strings.Repeat("22", 32)
	apiSecretFingerprint := testStubFingerprint(t, apiSecret)
	manifestKeyFingerprint := testStubFingerprint(t, manifestKey)
	expiresUnix := time.Now().Add(time.Hour).Unix()

	put := &routesync.Command{
		CmdID: "key-1", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: manifestKeyFingerprint, ManifestKeyType: "inline", ManifestKey: manifestKey,
		ExpiresUnix: expiresUnix,
	}
	if got := node.HandleCommand(context.Background(), put); got.Status != routesync.AckAccepted {
		t.Fatalf("key_put ack = %+v", got)
	}

	node.mu.Lock()
	pair, found := node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if !found || pair.APISecret != apiSecret || pair.ManifestKey != manifestKey || pair.ExpiresUnix != expiresUnix {
		t.Fatalf("stored key pair = %+v, found=%v", pair, found)
	}

	create := &routesync.Command{
		CmdID: "create-1", Kind: routesync.CmdCreate, SID: "sb1",
		TemplateRef: "bare-img-" + strings.Repeat("b", 64), Profile: "bare",
		Cluster:              &routesync.ClusterSandboxContext{Group: "/g", RouteKey: "rk", AuthSandboxID: "sb1"},
		APISecretFingerprint: apiSecretFingerprint,
		Config: map[string]string{
			"stub.create_result":           "timeout",
			clusterstate.ObjectMetadataKey: `{"group":"forged","route_key":"forged"}`,
		},
	}
	if got := node.HandleCommand(context.Background(), create); got.Status != routesync.AckAccepted {
		t.Fatalf("create ack = %+v", got)
	}
	node.mu.Lock()
	storedSandbox := node.sandboxes[create.SID]
	accessToken := storedSandbox.AccessToken
	node.mu.Unlock()
	if accessToken != "stub-access-"+create.SID {
		t.Fatalf("stub access token = %q", accessToken)
	}
	if storedSandbox.Profile != create.Profile || !sameStubClusterContext(storedSandbox.Cluster, create.Cluster) {
		t.Fatalf("stub sandbox context = %+v", storedSandbox)
	}
	if _, found := storedSandbox.Metadata[clusterstate.ObjectMetadataKey]; found {
		t.Fatalf("stub sandbox retained reserved cluster metadata: %+v", storedSandbox.Metadata)
	}

	build := &routesync.Command{
		CmdID: "build-1", Kind: routesync.CmdBuildRegister, BuildID: "b1",
		TemplateRef: "template-1", Profile: "bare", APISecretFingerprint: apiSecretFingerprint,
		Config: map[string]string{"stub.build_result": "timeout"},
	}
	if got := node.HandleCommand(context.Background(), build); got.Status != routesync.AckAccepted {
		t.Fatalf("build_register ack = %+v", got)
	}

	missing := *create
	missing.CmdID = "create-2"
	missing.SID = "sb2"
	missing.APISecretFingerprint = strings.Repeat("f", 64)
	if got := node.HandleCommand(context.Background(), &missing); got.Status != routesync.AckRejected {
		t.Fatalf("create with missing pair ack = %+v", got)
	}

	if got := node.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "drop-1", Kind: routesync.CmdKeyDrop, APISecretFingerprint: apiSecretFingerprint,
	}); got.Status != routesync.AckAccepted {
		t.Fatalf("key_drop ack = %+v", got)
	}
	node.mu.Lock()
	node.sandboxes[create.SID].State = routesync.StatePaused
	node.mu.Unlock()
	wrongConnect := &routesync.Command{
		CmdID: "connect-wrong", Kind: routesync.CmdConnect, SID: create.SID,
		APISecretFingerprint: strings.Repeat("f", 64),
	}
	if got := node.HandleCommand(context.Background(), wrongConnect); got.Status != routesync.AckRejected {
		t.Fatalf("connect with wrong binding ack = %+v", got)
	}
	node.mu.Lock()
	state := node.sandboxes[create.SID].State
	node.mu.Unlock()
	if state != routesync.StatePaused {
		t.Fatalf("rejected connect changed state to %q", state)
	}
	wrongContext := *wrongConnect
	wrongContext.CmdID = "connect-wrong-context"
	wrongContext.APISecretFingerprint = apiSecretFingerprint
	wrongContext.Profile = create.Profile
	wrongContext.Cluster = &routesync.ClusterSandboxContext{Group: "/other", RouteKey: "rk", AuthSandboxID: "sb1"}
	if got := node.HandleCommand(context.Background(), &wrongContext); got.Status != routesync.AckRejected {
		t.Fatalf("connect with wrong context ack = %+v", got)
	}
	connect := *wrongConnect
	connect.CmdID = "connect-1"
	connect.APISecretFingerprint = apiSecretFingerprint
	connect.Profile = create.Profile
	connect.Cluster = create.Cluster
	if got := node.HandleCommand(context.Background(), &connect); got.Status != routesync.AckAccepted {
		t.Fatalf("connect after key drop ack = %+v", got)
	}

	wrongDelete := &routesync.Command{
		CmdID: "delete-wrong", Kind: routesync.CmdDelete, SID: create.SID,
		APISecretFingerprint: strings.Repeat("f", 64),
	}
	if got := node.HandleCommand(context.Background(), wrongDelete); got.Status != routesync.AckRejected {
		t.Fatalf("delete with wrong binding ack = %+v", got)
	}
	if node.getSandbox(create.SID) == nil {
		t.Fatal("rejected delete removed sandbox")
	}
	deleteCmd := *wrongDelete
	deleteCmd.CmdID = "delete-1"
	deleteCmd.APISecretFingerprint = apiSecretFingerprint
	if got := node.HandleCommand(context.Background(), &deleteCmd); got.Status != routesync.AckAccepted {
		t.Fatalf("delete after key drop ack = %+v", got)
	}
	if node.getSandbox(create.SID) != nil {
		t.Fatal("accepted delete kept sandbox")
	}
}

func TestNodeSnapshotRedactsCredentialMaterial(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecret := strings.Repeat("66", 32)
	manifestKey := strings.Repeat("77", 32)
	apiRef := "secret://tenant/api-root"
	manifestRef := "secret://tenant/manifest-root"
	node.keyPairs["inline"] = stubKeyPair{
		APISecretFingerprint: testStubFingerprint(t, apiSecret), APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: testStubFingerprint(t, manifestKey), ManifestKeyType: "inline", ManifestKey: manifestKey,
	}
	node.keyPairs["ref"] = stubKeyPair{
		APISecretFingerprint: strings.Repeat("8", 64), APISecretType: "ref", APISecretRef: apiRef,
		ManifestKeyFingerprint: strings.Repeat("9", 64), ManifestKeyType: "ref", ManifestKeyRef: manifestRef,
	}

	raw, err := json.Marshal(node.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{apiSecret, manifestKey, apiRef, manifestRef} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("node snapshot leaked credential material %q: %s", secret, raw)
		}
	}
	if !strings.Contains(string(raw), testStubFingerprint(t, apiSecret)) ||
		!strings.Contains(string(raw), testStubFingerprint(t, manifestKey)) {
		t.Fatalf("node snapshot omitted credential fingerprints: %s", raw)
	}
}

func TestKeyPutRejectsHalfPairAndFingerprintRebinding(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecret := strings.Repeat("33", 32)
	manifestKey := strings.Repeat("44", 32)
	apiSecretFingerprint := testStubFingerprint(t, apiSecret)
	manifestKeyFingerprint := testStubFingerprint(t, manifestKey)

	half := &routesync.Command{
		CmdID: "key-half", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
	}
	if got := node.HandleCommand(context.Background(), half); got.Status != routesync.AckRejected {
		t.Fatalf("half key pair ack = %+v", got)
	}

	put := &routesync.Command{
		CmdID: "key-1", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: manifestKeyFingerprint, ManifestKeyType: "inline", ManifestKey: manifestKey,
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
	if got := node.HandleCommand(context.Background(), put); got.Status != routesync.AckAccepted {
		t.Fatalf("initial key_put ack = %+v", got)
	}

	rebound := *put
	rebound.CmdID = "key-2"
	rebound.ManifestKey = strings.Repeat("55", 32)
	rebound.ManifestKeyFingerprint = testStubFingerprint(t, rebound.ManifestKey)
	if got := node.HandleCommand(context.Background(), &rebound); got.Status != routesync.AckRejected {
		t.Fatalf("rebound key_put ack = %+v", got)
	}
	node.mu.Lock()
	stored := node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if stored.ManifestKeyFingerprint != manifestKeyFingerprint || stored.ManifestKey != manifestKey {
		t.Fatalf("rejected rebind changed stored pair: %+v", stored)
	}

	renewed := *put
	renewed.CmdID = "key-3"
	renewed.ExpiresUnix++
	if got := node.HandleCommand(context.Background(), &renewed); got.Status != routesync.AckAccepted {
		t.Fatalf("renewed key_put ack = %+v", got)
	}
	node.mu.Lock()
	stored = node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if stored.ExpiresUnix != renewed.ExpiresUnix {
		t.Fatalf("renewal expiry = %d, want %d", stored.ExpiresUnix, renewed.ExpiresUnix)
	}
}

func testStubFingerprint(t *testing.T, secretHex string) string {
	t.Helper()
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatalf("decode test secret: %v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
