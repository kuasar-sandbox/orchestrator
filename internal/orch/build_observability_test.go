package orch

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuilderAdmissionStatusUsesDurableUsageAndReportsHeadroom(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	b := registerTriggerTestBuild(t, o, apiKey)

	mx := metrics.New()
	o.SetMetrics(mx)
	o.recordRegistrationRejection("capacity")
	o.recordExecutionWouldWait()
	status, err := o.BuilderAdmissionStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Registration.UsedBuilds != 1 || status.Registration.Used != b.Resources ||
		status.Registration.Available.MaxBuilds == nil || *status.Registration.Available.MaxBuilds != 1 ||
		status.Execution.UsedBuilds != 0 || status.RegistrationReject["capacity"] != 1 ||
		status.ExecutionWouldWait != 1 {
		t.Fatalf("builder admission status = %+v", status)
	}

	response := httptest.NewRecorder()
	mx.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	text := response.Body.String()
	for _, metric := range []string{
		"builder_registration_used_builds 1",
		"builder_registration_used_cpu_milli 2000",
		"builder_registration_rejections_total{reason=\"capacity\"} 1",
		"builder_execution_would_wait_total 1",
	} {
		if !strings.Contains(text, metric) {
			t.Fatalf("metrics missing %q:\n%s", metric, text)
		}
	}
}

func TestBuilderAdmissionStatusCountsExecutionFitRegistrationRejection(t *testing.T) {
	executionCPU := config.CPUCores("1")
	o := testOrchCfg(t, &config.Config{Builder: config.BuilderConfig{
		Admission: config.BuilderAdmissionConfig{Execution: &config.BuildAdmissionLimitConfig{
			Resources: config.BuildAdmissionResourcesConfig{CPU: &executionCPU},
		}},
	}})
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	_, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Profile:   types.ProfileE2B,
		Resources: types.BuildResources{CPU: 2000, Memory: 2 << 30},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild error = %v, want ErrBadRequest", err)
	}
	status, err := o.BuilderAdmissionStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.RegistrationReject["execution_fit"] != 1 || status.Registration.UsedBuilds != 0 {
		t.Fatalf("builder admission status = %+v", status)
	}
}
