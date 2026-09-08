package proxystats

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestInvalidPolicyStatsUnavailableWithoutPoisoningOtherSandbox(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	server := NewStatsServer(master, func() bool { return true }, func(sid string) (RouteIdentity, bool) {
		identity := RouteIdentity{RunID: "run", Profile: types.ProfileBare, State: types.StatePaused}
		identity.TrafficPolicyInvalid = sid == "bad"
		if !identity.TrafficPolicyInvalid {
			identity.MaxInflight = config.MaxInflight{Total: 9}
		}
		return identity, sid == "bad" || sid == "good"
	}, nil)
	for _, sids := range [][]string{{"bad"}, {"good"}, {"good", "bad"}, {"bad", "good"}} {
		request := BatchRequest{}
		for _, sid := range sids {
			request.Sandboxes = append(request.Sandboxes, TrafficQuery{SandboxID: sid, RunID: "run", Profile: types.ProfileBare, State: types.StatePaused})
		}
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, BatchPath, bytes.NewReader(raw)))
		if len(sids) == 1 && sids[0] == "good" {
			var got BatchResponse
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if response.Code != 200 || len(got.Sandboxes) != 1 || got.Sandboxes[0].Stats.MaxInflight.Total != 9 {
				t.Fatalf("good response=%d %s", response.Code, response.Body)
			}
		} else if response.Code != 503 || bytes.Contains(response.Body.Bytes(), []byte("sandboxes")) {
			t.Fatalf("invalid batch leaked partial/effective data: %d %s", response.Code, response.Body)
		}
	}
}
