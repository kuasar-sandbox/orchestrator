package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterclient"
)

func TestScaleLinkResolverUsesCachedMembership(t *testing.T) {
	var hits atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != clusterclient.MembershipPath {
			http.NotFound(w, req)
			return
		}
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(clustercfg.MembershipConfig{
			Active: 1,
			Versions: []clustercfg.MembershipVersion{{
				Version: 1,
				Members: []clustercfg.MembershipMember{
					{ID: "a", Advertise: srv.URL},
					{ID: "b", Advertise: srv.URL},
					{ID: "c", Advertise: srv.URL},
				},
			}},
			Owners: clustercfg.MembershipOwnerConfig{RouteLink: 1, NodeLink: 1, ScaleLink: 2, NodeList: 1},
		})
	}))
	defer srv.Close()

	regClient := clusterclient.NewRegistryWithClient(srv.URL, srv.Client())
	if err := regClient.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	hits.Store(0)
	resolver := newScaleLinkResolver(regClient)

	for _, source := range []string{"source-a", "source-b", "source-c"} {
		links, err := resolver(context.Background(), string(clusterstate.ScaleImportSourceShard(source)))
		if err != nil {
			t.Fatalf("resolve %s: %v", source, err)
		}
		if len(links) == 0 {
			t.Fatalf("resolve %s returned no links", source)
		}
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("resolver performed %d membership refreshes; want cached local LocateN only", got)
	}
}
