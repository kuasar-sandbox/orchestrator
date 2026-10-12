package configsock

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestBuildTaskRejectsWorkerWithoutRuntimeTimeoutPolicy(t *testing.T) {
	// Version 6 workers silently discard runtime_timeouts while decoding a
	// BuildSpec. Reject them before loading a secret-bearing task or completing
	// preparation, rather than running with the SDK default after an upgrade.
	const beforeRuntimeTimeoutPolicy = 6
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	for _, tc := range []struct {
		path    string
		request any
	}{
		{PathTaskBuildBootstrap, BuildTaskRequest{
			BuildID: "x", RunID: "br-test", Version: beforeRuntimeTimeoutPolicy,
		}},
		{PathTaskBuildPrepare, BuildPrepareRequest{
			BuildID: "x", RunID: "br-test", Version: beforeRuntimeTimeoutPolicy,
			Summary: ArtifactPrepareSummary{SchemaVersion: ArtifactPrepareSchemaVersion},
		}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			var authCalls, specCalls, prepareCalls atomic.Int32
			_, client := startTestServer(t, Deps{Provider: stubProvider{
				pidFile: pf, buildAuthHits: &authCalls, buildSpecHits: &specCalls,
				buildPrepHits: &prepareCalls,
			}})
			status, _ := rawPost(t, client, tc.path, tc.request)
			if status != http.StatusBadRequest {
				t.Errorf("pre-timeout-policy worker status = %d, want %d", status, http.StatusBadRequest)
			}
			if authCalls.Load() != 0 || specCalls.Load() != 0 || prepareCalls.Load() != 0 {
				t.Errorf("unsupported worker reached providers: auth=%d spec=%d prepare=%d",
					authCalls.Load(), specCalls.Load(), prepareCalls.Load())
			}
		})
	}
}

type timeoutPolicyProvider struct{ stubProvider }

func (p timeoutPolicyProvider) BuildTaskSpecFor(ctx context.Context, buildID, runID string) (*BuildTaskSpec, bool, error) {
	spec, ok, err := p.stubProvider.BuildTaskSpecFor(ctx, buildID, runID)
	if spec != nil && spec.Final != nil {
		spec.Final.RuntimeTimeouts = rtconfig.TimeoutsConfig{AppStart: "3s"}
	}
	return spec, ok, err
}

func (p timeoutPolicyProvider) CompleteBuildPrepare(ctx context.Context, buildID, runID string, summary ArtifactPrepareSummary) (*BuildSpec, error) {
	spec, err := p.stubProvider.CompleteBuildPrepare(ctx, buildID, runID, summary)
	if spec != nil {
		spec.RuntimeTimeouts = rtconfig.TimeoutsConfig{AppStart: "3s"}
	}
	return spec, err
}

func TestBuildTaskRuntimeTimeoutPolicyRoundTrip(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: timeoutPolicyProvider{stubProvider{pidFile: pf}}})
	spec, err := FetchBuildTaskSpec(context.Background(), sock, "x", "br-test")
	if err != nil {
		t.Fatal(err)
	}
	if spec == nil || spec.Final == nil || spec.Final.RuntimeTimeouts.AppStart != "3s" {
		t.Fatalf("bootstrap lost runtime timeout policy: %+v", spec)
	}
	final, err := CompleteBuildPrepare(context.Background(), sock, "x", "br-test", ArtifactPrepareSummary{
		SchemaVersion: ArtifactPrepareSchemaVersion, ResolutionDigest: strings.Repeat("0", 64), RequiredRefCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if final == nil || final.RuntimeTimeouts.AppStart != "3s" {
		t.Fatalf("prepare completion lost runtime timeout policy: %+v", final)
	}
}
