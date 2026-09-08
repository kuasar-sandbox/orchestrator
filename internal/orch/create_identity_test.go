package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStandaloneCreateIdentitySelection(t *testing.T) {
	for _, test := range []struct{ name, raw, id, stable string }{
		{name: "absent"},
		{name: "empty object", raw: `{}`},
		{name: "empty fields", raw: `{"id":"","stable_id":""}`},
		{name: "local", raw: `{"id":"worker-42"}`, id: "worker-42"},
		{name: "stable", raw: `{"stable_id":"worker-42"}`, stable: "worker-42"},
		{name: "both", raw: `{"id":"instance-3","stable_id":"worker-42"}`, id: "instance-3", stable: "worker-42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{}
			o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
			blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
			o.vs = blocked
			req := createRequestFixture(t, o, "4")
			if test.raw != "" {
				req.Metadata[sandboxcfg.NsIdentity] = test.raw
			}
			original := cloneStringMap(req.Metadata)
			events, stop := o.Subscribe()
			defer stop()
			sb, err := o.Create(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if test.id != "" && sb.ID != test.id {
				t.Fatalf("local ID = %q", sb.ID)
			}
			if test.id == "" {
				id, err := uuid.Parse(sb.ID)
				if err != nil || id.Version() != 7 {
					t.Fatalf("default ID is not UUIDv7: %q", sb.ID)
				}
			}
			stable := test.stable
			if stable == "" {
				stable = sb.ID
			}
			if sb.StableIDValue != test.stable || sb.StableID() != stable || sb.Cluster != nil {
				t.Fatalf("unexpected stable/cluster identity: %q / %q / %+v", sb.StableIDValue, sb.StableID(), sb.Cluster)
			}
			if sb.RunDir != nodepath.SandboxRunDir(cfg.Paths.RunRoot, sb.ID) || sb.BaseDir != nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sb.ID) {
				t.Fatal("paths did not use the selected local ID")
			}
			if _, found := sb.Metadata[sandboxcfg.NsIdentity]; found {
				t.Fatal("identity persisted as metadata")
			}
			if !reflect.DeepEqual(req.Metadata, original) {
				t.Fatal("request metadata mutated")
			}
			if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, sb.ServiceSecret, stable); err != nil {
				t.Fatal(err)
			}
			stored, err := o.st.Get(ctx, sb.ID)
			if err != nil || stored == nil || stored.StableIDValue != test.stable || stored.Cluster != nil {
				t.Fatalf("identity readback failed: %v", err)
			}
			event := receiveCreateRouteEvent(t, events, routesync.TypeUpsert)
			if event.Route.SandboxID != sb.ID || event.Route.StableID != stable {
				t.Fatal("route identity mismatch")
			}
			close(blocked.gate)
			waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "identity create running")
		})
	}
}

func TestCreateIdentityHTTPInputsAndConflict(t *testing.T) {
	for _, carrier := range []string{"metadata", "header", "header-over-body"} {
		t.Run(carrier, func(t *testing.T) {
			o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, &countingLauncher{})
			blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
			o.vs = blocked
			req := createRequestFixture(t, o, "5")
			rawIdentity := `{"id":"http-instance","stable_id":"http-stable"}`
			wantStable := "http-stable"
			if carrier == "metadata" {
				req.Metadata[sandboxcfg.NsIdentity] = rawIdentity
			}
			if carrier == "header-over-body" {
				req.Metadata[sandboxcfg.NsIdentity] = `{"id":"body-instance","stable_id":"body-stable"}`
				rawIdentity, wantStable = `{"id":"http-instance"}`, "http-instance"
			}
			body, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			handler := api.New(o, "sandbox.test", api.Resources{}, o.log).Handler()
			create := func() *httptest.ResponseRecorder {
				r := httptest.NewRequest(http.MethodPost, "/sandboxes", bytes.NewReader(body))
				r.Header.Set("X-API-KEY", req.APIKey)
				if carrier != "metadata" {
					r.Header.Set("X-Kuasar-Sandbox-Identity", rawIdentity)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				return w
			}
			w := create()
			if w.Code != http.StatusCreated {
				t.Fatalf("Create = %d %s", w.Code, w.Body.String())
			}
			var response struct {
				SandboxID string `json:"sandboxID"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.SandboxID != "http-instance" {
				t.Fatal("response did not use requested ID")
			}
			stored, err := o.st.Get(ctx, response.SandboxID)
			if err != nil || stored == nil || stored.StableID() != wantStable {
				t.Fatalf("HTTP identity readback: %v", err)
			}
			if _, found := stored.Metadata[sandboxcfg.NsIdentity]; found {
				t.Fatal("HTTP identity retained in metadata")
			}
			if w := create(); w.Code != http.StatusConflict {
				t.Fatalf("duplicate HTTP Create = %d %s", w.Code, w.Body.String())
			}
			close(blocked.gate)
			waitForSandbox(t, o, ctx, stored.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "HTTP identity create running")
		})
	}
}

func TestCreateIdentityRetainedStateConflicts(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused, types.StateDeleting, types.StateDead} {
		t.Run(string(state), func(t *testing.T) {
			lc := &countingLauncher{}
			o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, lc)
			vs := &checkpointVS{}
			o.vs = vs
			req := createRequestFixture(t, o, "6")
			pair, err := o.resolveAllowed(ctx, req.APIKey)
			if err != nil {
				t.Fatal(err)
			}
			existing := &types.Sandbox{
				ID: "retained-id", StableIDValue: "original-stable", Profile: types.ProfileBare,
				TemplateID: req.TemplateID, State: state, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
				Metadata: map[string]string{"keep": "original"},
			}
			switch state {
			case types.StateStarting:
				existing.LaunchMode = types.LaunchImage
			case types.StatePaused:
				existing.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64)}
			case types.StateDead:
				existing.DeadUnix = 1
			}
			materializeTestSandboxCredentials(t, existing)
			if err := o.st.InsertSandbox(ctx, existing); err != nil {
				t.Fatal(err)
			}
			before, err := o.st.Get(ctx, existing.ID)
			if err != nil {
				t.Fatal(err)
			}
			req.Metadata[sandboxcfg.NsIdentity] = `{"id":"retained-id","stable_id":"replacement"}`
			if _, err := o.Create(ctx, req); !errors.Is(err, api.ErrAlreadyExists) || !errors.Is(err, store.ErrSandboxExists) {
				t.Fatalf("conflict = %v", err)
			}
			after, err := o.st.Get(ctx, existing.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("conflicting Create changed existing object")
			}
			if _, found := o.launches.Lookup(existing.ID); found {
				t.Fatal("conflicting Create leaked its claim")
			}
			if lc.starts.Load() != 0 || vs.attaches.Load() != 0 {
				t.Fatal("conflict caused host side effects")
			}
		})
	}
}

func TestCreateIdentityConcurrentAndReuseAfterDelete(t *testing.T) {
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, lc)
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	req := createRequestFixture(t, o, "7")
	req.Metadata[sandboxcfg.NsIdentity] = `{"id":"concurrent-id","stable_id":"logical"}`
	type result struct {
		sb  *types.Sandbox
		err error
	}
	const callers = 8
	results := make(chan result, callers)
	for i := 0; i < callers; i++ {
		go func() { sb, err := o.Create(ctx, req); results <- result{sb, err} }()
	}
	accepted := 0
	for i := 0; i < callers; i++ {
		res := <-results
		if res.err == nil {
			accepted++
			continue
		}
		if !errors.Is(res.err, api.ErrAlreadyExists) {
			t.Fatalf("concurrent loser = %v", res.err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d concurrent creates", accepted)
	}
	if _, found := o.launches.Lookup("concurrent-id"); !found {
		t.Fatal("loser canceled winner")
	}
	close(blocked.gate)
	waitForSandbox(t, o, ctx, "concurrent-id", func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "concurrent winner running")
	if _, err := o.launches.Wait(ctx, "concurrent-id"); err != nil {
		t.Fatal(err)
	}
	if lc.starts.Load() != 1 {
		t.Fatalf("launches = %d", lc.starts.Load())
	}
	if _, err := o.Create(ctx, req); !errors.Is(err, store.ErrSandboxExists) || !errors.Is(err, api.ErrAlreadyExists) {
		t.Fatalf("durable conflict = %v", err)
	}
	other := createRequestFixture(t, o, "8")
	other.Metadata[sandboxcfg.NsIdentity] = `{"id":"concurrent-id","stable_id":"other-tenant"}`
	if _, err := o.Create(ctx, other); !errors.Is(err, api.ErrAlreadyExists) {
		t.Fatalf("cross-tenant conflict = %v", err)
	}
	if ok, err := o.Kill(ctx, "concurrent-id", req.APIKey); err != nil || !ok {
		t.Fatalf("Kill = %t, %v", ok, err)
	}
	waitForSandboxAbsent(t, o, ctx, "concurrent-id", "identity final deletion")
	sb, err := o.Create(ctx, req)
	if err != nil || sb.ID != "concurrent-id" || sb.StableID() != "logical" {
		t.Fatalf("Create after final deletion = %v", err)
	}
	waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "reused ID running")
}

func TestCreateIdentityHookBoundary(t *testing.T) {
	for _, mode := range []string{"credentials", "envelope", "metadata"} {
		t.Run(mode, func(t *testing.T) {
			o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, &countingLauncher{})
			blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
			o.vs = blocked
			req := createRequestFixture(t, o, "9")
			req.Metadata[sandboxcfg.NsIdentity] = `{"id":"hook-id","stable_id":"hook-stable"}`
			secret := strings.Repeat("f", 64)
			o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
				if operation.SandboxID != "hook-id" || operation.Origin != conductorextension.SandboxOriginDirect {
					t.Fatal("Hook saw wrong identity/origin")
				}
				if _, present := operation.Create.Metadata[sandboxcfg.NsIdentity]; present {
					t.Fatal("Hook received writable identity input")
				}
				switch mode {
				case "envelope":
					operation.SandboxID = "changed"
				case "metadata":
					operation.Create.Metadata[sandboxcfg.NsIdentity] = `{}`
				case "credentials":
					operation.Create.Metadata[sandboxcfg.NsCredentials] = `{"service_secret":"` + secret + `"}`
				}
				return nil
			}), nil)
			sb, err := o.Create(ctx, req)
			if mode != "credentials" {
				if !errors.Is(err, api.ErrBadRequest) || sb != nil {
					t.Fatalf("identity mutation = %v", err)
				}
				if stored, err := o.st.Get(ctx, "hook-id"); err != nil || stored != nil {
					t.Fatal("identity mutation persisted")
				}
				return
			}
			if err != nil || sb.ServiceSecret != secret || sb.StableID() != "hook-stable" {
				t.Fatalf("final Hook credentials = %v", err)
			}
			if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, secret, "hook-stable"); err != nil {
				t.Fatal(err)
			}
			close(blocked.gate)
			waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "Hook credentials running")
		})
	}
}

func TestClusterCreatePreservesOriginalCredentialInput(t *testing.T) {
	o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, &countingLauncher{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(fingerprint, "cluster-identity")
	cmd.Config = map[string]string{sandboxcfg.NsCredentials: `{"service_secret":"` + strings.Repeat("a", 64) + `"}`, "keep": "value"}
	original := cloneStringMap(cmd.Config)
	finalSecret := strings.Repeat("b", 64)
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		if operation.Origin != conductorextension.SandboxOriginCluster {
			t.Fatal("wrong cluster Hook origin")
		}
		operation.Create.Metadata[sandboxcfg.NsCredentials] = `{"service_secret":"` + finalSecret + `"}`
		return nil
	}), nil)
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster ACK = %+v", ack)
	}
	if !reflect.DeepEqual(cmd.Config, original) {
		t.Fatal("command input mutated")
	}
	stored, err := o.st.Get(ctx, cmd.SID)
	if err != nil || stored == nil || stored.ServiceSecret != finalSecret || stored.Cluster == nil || stored.StableID() != cmd.Cluster.StableID {
		t.Fatalf("cluster identity/credentials: %v", err)
	}
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusConflict {
		t.Fatalf("cluster duplicate ACK = %+v", ack)
	}
	if !reflect.DeepEqual(cmd.Config, original) {
		t.Fatal("repeated command input mutated")
	}
	close(blocked.gate)
	waitForSandbox(t, o, ctx, cmd.SID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "cluster identity running")
}

func TestCreateIdentityFailureRetainsID(t *testing.T) {
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, lc)
	vs := &checkpointVS{}
	o.vs = vs
	req := createRequestFixture(t, o, "a")
	req.Metadata[sandboxcfg.NsIdentity] = `{"id":"failed-identity"}`
	barrier := newFakeRouteBarrier(errors.New("injected apply failure"), nil)
	barrier.release()
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	if _, err := o.Create(ctx, req); !errors.Is(err, api.ErrProxyUnavailable) {
		t.Fatalf("barrier failure = %v", err)
	}
	stored, err := o.st.Get(ctx, "failed-identity")
	if err != nil || stored == nil || stored.State != types.StateDead {
		t.Fatalf("failure did not retain dead identity: %v", err)
	}
	ready := newFakeRouteBarrier(nil, nil)
	ready.release()
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: ready})
	if _, err := o.Create(ctx, req); !errors.Is(err, api.ErrAlreadyExists) {
		t.Fatalf("dead ID retry = %v", err)
	}
	if lc.starts.Load() != 0 || vs.attaches.Load() != 0 {
		t.Fatal("pre-launch failure/retry acquired host resources")
	}
}

func TestIdentityInvalidAndWrongOriginRejectedBeforeEffects(t *testing.T) {
	o := testOrch(t)
	req := createRequestFixture(t, o, "b")
	for _, raw := range []string{`{"id":"../x"}`, `{"stable_id":null}`, `{"id":"` + strings.Repeat("a", 58) + `"}`} {
		req.Metadata[sandboxcfg.NsIdentity] = raw
		if _, err := o.Create(context.Background(), req); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("invalid identity = %v", err)
		}
	}
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(fingerprint, "cluster-reject")
	cmd.Config = map[string]string{sandboxcfg.NsIdentity: `{}`}
	if ack := o.HandleCommand(context.Background(), cmd); ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("cluster identity injection = %+v", ack)
	}
	request := buildRegisterRequest(api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources(), Metadata: map[string]string{sandboxcfg.NsIdentity: `{}`}}, "")
	if _, err := o.normalizeBuildRegistration(request, nil); !errors.Is(err, api.ErrBadRequest) || !strings.Contains(fmt.Sprint(err), sandboxcfg.NsIdentity) {
		t.Fatalf("Build identity injection = %v", err)
	}
}
