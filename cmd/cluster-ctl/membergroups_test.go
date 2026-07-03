package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

func TestPlacerLinkRegisterSeedsPlacerObserverMemberlist(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	regHub := membergroup.NewHub()
	regMux := http.NewServeMux()
	regHub.Mount(regMux)
	regSrv := httptest.NewServer(regMux)
	defer regSrv.Close()

	cfg := clustercfg.DefaultRegistry()
	cfg.Member.ID = "registry-1"
	cfg.Membership.Versions[0].Members[0].ID = "registry-1"
	cfg.Membership.Versions[0].Members[0].Advertise = regSrv.URL
	cfg.PlacerLink.PlacerLabel = "placer.default"
	observer, err := newPlacerObserverRuntime(&cfg, regHub, log)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.group.Shutdown()
	reg := registry.New(registry.NewStores(), nil, time.Second, log)
	reg.SetPlacerMemberlistLabel("placer.default")
	reg.SetPlacerSeedJoiner(observer.JoinSeed)
	reg.SetPlacerPeerSource(observer.ReadyPlacers)
	reg.SetPlacerReadyLabel("registry.1.test")
	reg.ServePlacerLink(regMux)

	placerHub := membergroup.NewHub()
	placerMux := http.NewServeMux()
	placerHub.Mount(placerMux)
	placerSrv := httptest.NewServer(placerMux)
	defer placerSrv.Close()
	placerGroup, err := membergroup.New(membergroup.Options{
		Label: "placer.default", Name: "placer-1", Hub: placerHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RolePlacer, ID: "placer-1",
			Advertise: placerSrv.URL,
			Ready:     true, ReadyLabel: "registry.1.test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer placerGroup.Shutdown()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(registry.PlacerRegister{
		ID: "placer-1", Advertise: placerSrv.URL,
		MemberlistLabel: "placer.default",
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(regSrv.URL+registry.PlacerLinkRegisterPath, "application/json", &body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("register status=%s", resp.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		peers := observer.ReadyPlacers("registry.1.test")
		if len(peers) == 1 && peers[0].ID == "placer-1" && peers[0].Advertise == placerSrv.URL {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("observer did not learn ready placer: %+v", observer.ReadyPlacers("registry.1.test"))
}
