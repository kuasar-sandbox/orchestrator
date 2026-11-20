package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

type diskQoSPolicyProvider struct {
	stubProvider
	execution rtconfig.ResourcesConfig
	target    rtconfig.ResourcesConfig
}

func (p diskQoSPolicyProvider) BuildTaskSpecFor(ctx context.Context, buildID, runID string) (*BuildTaskSpec, bool, error) {
	spec, ok, err := p.stubProvider.BuildTaskSpecFor(ctx, buildID, runID)
	if spec != nil && spec.Final != nil {
		spec.Final.Resources, spec.Final.SandboxResources = p.execution, p.target
	}
	return spec, ok, err
}

func (p diskQoSPolicyProvider) CompleteBuildPrepare(ctx context.Context, buildID, runID string, summary ArtifactPrepareSummary) (*BuildSpec, error) {
	spec, err := p.stubProvider.CompleteBuildPrepare(ctx, buildID, runID, summary)
	if spec != nil {
		spec.Resources, spec.SandboxResources = p.execution, p.target
	}
	return spec, err
}

func TestBuildBootstrapDiskQoSVersionGate(t *testing.T) {
	testBuildDiskQoSVersionGate(t, PathTaskBuildBootstrap)
}

func TestBuildPrepareDiskQoSVersionGate(t *testing.T) {
	testBuildDiskQoSVersionGate(t, PathTaskBuildPrepare)
}

func testBuildDiskQoSVersionGate(t *testing.T, path string) {
	t.Helper()
	// Version 7 decodes BuildSpec into an older DiffCOWConfig and silently
	// drops rate and per-disk dirty limits before rendering Host YAML. Both
	// entry points must reject it before loading or completing any task.
	const beforeDiskQoS = 7
	for _, tc := range []struct {
		version int
		status  int
	}{
		{beforeDiskQoS, http.StatusBadRequest},
		{BuildTaskSchemaVersion, http.StatusOK},
	} {
		t.Run(fmt.Sprintf("v%d", tc.version), func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "builder.pid")
			mustWrite(t, pidFile, strconv.Itoa(os.Getpid()))
			var authCalls, specCalls, prepareCalls atomic.Int32
			zero, targetIOPS := uint64(0), uint64(37)
			provider := diskQoSPolicyProvider{
				stubProvider: stubProvider{
					pidFile: pidFile, buildAuthHits: &authCalls,
					buildSpecHits: &specCalls, buildPrepHits: &prepareCalls,
				},
				execution: rtconfig.ResourcesConfig{DiffCOW: rtconfig.DiffCOWConfig{
					MaxDirtySize: "0", ReadBandwidth: "50MiB", ReadIOPS: &zero,
					MaxDirtySizePerDisk: "8MiB",
				}},
				target: rtconfig.ResourcesConfig{DiffCOW: rtconfig.DiffCOWConfig{
					MaxDirtySize: "0", ReadBandwidthPerDisk: "10MiB", ReadIOPSPerDisk: &targetIOPS,
				}},
			}
			var request any = BuildTaskRequest{BuildID: "x", RunID: "br-test", Version: tc.version}
			if path == PathTaskBuildPrepare {
				request = BuildPrepareRequest{
					BuildID: "x", RunID: "br-test", Version: tc.version,
					Summary: ArtifactPrepareSummary{SchemaVersion: ArtifactPrepareSchemaVersion},
				}
			}
			raw, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
			req = req.WithContext(context.WithValue(req.Context(), peerPIDKey{}, os.Getpid()))
			response := httptest.NewRecorder()
			New("", Deps{Provider: provider}, slog.Default()).router().ServeHTTP(response, req)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			// Both responses carry the same Final/Error envelope. Decode it as a
			// client does to check the actual JSON wire, including explicit zero.
			var out BuildPrepareResponse
			if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if tc.status != http.StatusOK {
				if out.Error == "" || out.Final != nil {
					t.Fatalf("unsupported worker received a task: %+v", out)
				}
				if IsRetryableError(buildResponseError(response.Code, out.Error)) {
					t.Fatal("unsupported worker received a retryable error")
				}
				if authCalls.Load() != 0 || specCalls.Load() != 0 || prepareCalls.Load() != 0 {
					t.Fatalf("unsupported worker reached providers: auth=%d spec=%d prepare=%d",
						authCalls.Load(), specCalls.Load(), prepareCalls.Load())
				}
				return
			}
			if out.Error != "" || out.Final == nil ||
				!reflect.DeepEqual(out.Final.Resources, provider.execution) ||
				!reflect.DeepEqual(out.Final.SandboxResources, provider.target) {
				t.Fatalf("current worker lost execution or target QoS: %+v", out)
			}
			if authCalls.Load() != 1 || specCalls.Load()+prepareCalls.Load() != 1 {
				t.Fatalf("current worker provider calls: auth=%d spec=%d prepare=%d",
					authCalls.Load(), specCalls.Load(), prepareCalls.Load())
			}
		})
	}
}
