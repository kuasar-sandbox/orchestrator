package orch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestDirectBuildRequestUsesProtectedObjectBinding(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	binding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1", Kind: clusterstate.ExecutionKindBuild,
		ObjectID: "build-1", Group: "/group", NodeID: "node-1", NodeEpoch: 7,
	}
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := clusterstate.WithExecutionBinding(map[string]string{"user": "value"}, opaque)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutBuild(context.Background(), &types.Build{
		BuildID: "build-1", TemplateID: "transient-build-1", ManifestKey: strings.Repeat("1", 64),
		Profile: types.ProfileBare, Kind: types.KindImg, Status: types.BuildRegistered,
		Metadata: metadata, CreatedUnix: 1,
	}); err != nil {
		t.Fatal(err)
	}
	session := &ClusterSession{current: nodeexec.LocalSessionIdentity{
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 3, DataEndpoint: "node-1:8443",
	}}
	node := &FinalClusterNode{store: st, session: session}
	request := httptest.NewRequest(http.MethodPost,
		"https://api.example.test/v2/templates/transient-build-1/builds/build-1", nil)
	setDirectBuildFence(request, digest)
	if status, kind, err := node.verifyDirectAPIRequest(request); err != nil || status != 0 || kind != "" {
		t.Fatalf("valid direct Build fence = %d %q %v", status, kind, err)
	}

	request.Header.Set(clusterstate.DirectHeaderRouteKey, "route-1")
	if status, kind, err := node.verifyDirectAPIRequest(request); err == nil ||
		status != http.StatusConflict || kind != proxypkg.ProxyErrorWrongBinding {
		t.Fatalf("Build Route key fence = %d %q %v", status, kind, err)
	}
	request.Header.Del(clusterstate.DirectHeaderRouteKey)
	request.URL.Path = "/v2/templates/another-template/builds/build-1"
	if status, kind, err := node.verifyDirectAPIRequest(request); err == nil ||
		status != http.StatusConflict || kind != proxypkg.ProxyErrorWrongBinding {
		t.Fatalf("Build path fence = %d %q %v", status, kind, err)
	}
}

func setDirectBuildFence(request *http.Request, digest string) {
	request.Header.Set(clusterstate.DirectHeaderExecutionKind, "build")
	request.Header.Set(clusterstate.DirectHeaderObjectID, "build-1")
	request.Header.Set(clusterstate.DirectHeaderGroup, "/group")
	request.Header.Set(proxypkg.HeaderNodeID, "node-1")
	request.Header.Set(proxypkg.HeaderNodeEpoch, "7")
	request.Header.Set(proxypkg.HeaderRegistryGeneration, "generation-1")
	request.Header.Set(proxypkg.HeaderBindingDigest, digest)
}
