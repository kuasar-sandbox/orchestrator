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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func TestScaleLinkRegisterSeedsScalerObserverMemberlist(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	regHub := membergroup.NewHub()
	regMux := http.NewServeMux()
	regHub.Mount(regMux)
	regSrv := httptest.NewServer(regMux)
	defer regSrv.Close()

	cfg := clustercfg.DefaultRegistry()
	cfg.Member.ID = "registry-1"
	cfg.Member.Advertise = regSrv.URL
	cfg.ScaleLink.ScalerLabel = "scaler.default"
	observer, err := newScalerObserverRuntime(&cfg, regHub, log)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.group.Shutdown()

	kv := clusterstore.OpenMemory(100)
	defer kv.Close()
	reg := registry.New(registry.NewStores(kv), nil, time.Second, log)
	reg.SetScalerMemberlistLabel("scaler.default")
	reg.SetScalerSeedJoiner(observer.JoinSeed)
	reg.SetScalerPeerSource(observer.ReadyScalers)
	reg.SetScaleReadyLabel("registry.1.test")
	reg.ServeScaleLink(regMux)

	scalerHub := membergroup.NewHub()
	scalerMux := http.NewServeMux()
	scalerHub.Mount(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	defer scalerSrv.Close()
	scalerGroup, err := membergroup.New(membergroup.Options{
		Label: "scaler.default", Name: "scaler-1", Hub: scalerHub, FastTimers: true,
		Meta: membergroup.Meta{
			Role: membergroup.RoleScaler, ID: "scaler-1",
			APIAdvertise: scalerSrv.URL, MemberlistAdvertise: scalerSrv.URL,
			Ready: true, ReadyLabel: "registry.1.test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scalerGroup.Shutdown()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(registry.ScalerRegister{
		ID: "scaler-1", Advertise: scalerSrv.URL,
		MemberlistLabel: "scaler.default", MemberlistAdvertise: scalerSrv.URL,
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(regSrv.URL+registry.ScaleLinkRegisterPath, "application/json", &body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("register status=%s", resp.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		peers := observer.ReadyScalers("registry.1.test")
		if len(peers) == 1 && peers[0].ID == "scaler-1" && peers[0].Advertise == scalerSrv.URL {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("observer did not learn ready scaler: %+v", observer.ReadyScalers("registry.1.test"))
}
