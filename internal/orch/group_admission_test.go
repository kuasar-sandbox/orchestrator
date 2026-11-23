package orch

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"reflect"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const testGroupRegistryAuth = `{"auths":{"registry.test":{"auth":"opaque"}}}`

func clusterGroupFixture(t *testing.T, o *Orchestrator, fingerprint, group string) (string, int64) {
	t.Helper()
	ctx := context.Background()
	pair, err := o.resolveByFingerprint(ctx, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	key, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	if detail, err := o.GroupGet(ctx, key, group); err == nil {
		return key, detail.Version
	}
	c := nodegroup.Context{}
	if group == "/test" {
		c.ImageRepo = "registry.test/repo"
		c.RegistryAuth = &keys.Secret{Type: keys.SecretInline, Value: testGroupRegistryAuth}
	}
	result, err := o.GroupPut(ctx, key, group, nodegroup.PutRequest{Context: c})
	if errors.Is(err, nodegroup.ErrConflict) {
		// Concurrent test setup can share the already installed fixture.
		detail, getErr := o.GroupGet(ctx, key, group)
		if getErr != nil {
			t.Fatal(getErr)
		}
		return key, detail.Version
	}
	if err != nil {
		t.Fatal(err)
	}
	return key, result.Version
}

func TestGroupCreateDefaultAndExplicitInputs(t *testing.T) {
	o := testOrch(t)
	_, _, fp := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(t, o, fp, "group-template")
	template := cmd.TemplateRef
	defaults := map[string]string{"inherited": "group", "overridden": "group", sandboxcfg.NsCheckpoint: `{"merge_ref":true}`,
		sandboxcfg.NsResource: `{"capacity":{"cpu":4,"memory":"8GiB"},"allocatable":{"cpu":0.5}}`,
		sandboxcfg.NsTraffic:  `{"max_inflight":{"total":20,"forward":8}}`,
	}
	result, err := o.GroupPut(context.Background(), cmd.GroupAPIKey, cmd.Cluster.Group, nodegroup.PutRequest{
		ExpectedVersion: cmd.ExpectedGroupVersion, Context: nodegroup.Context{TemplateID: &template, SandboxConfig: defaults},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd.ExpectedGroupVersion = result.Version
	cmd.Config = map[string]string{"overridden": "request", sandboxcfg.NsCheckpoint: `{"merge_ref":false}`,
		sandboxcfg.NsResource: `{"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}`,
		sandboxcfg.NsTraffic:  `{"max_inflight":{"forward":0,"exec":2}}`,
	}
	wantRequest := cloneStringMap(cmd.Config)
	for _, explicit := range []bool{false, true} {
		cmd.TemplateRef = ""
		if explicit {
			cmd.TemplateRef = template
		}
		_, actual, normalized, err := o.precheckCluster(context.Background(), cmd)
		if err != nil {
			t.Fatal(err)
		}
		if actual.String() != template || normalized.Metadata["overridden"] != "request" || normalized.Metadata[sandboxcfg.NsCheckpoint] != `{"merge_ref":false}` {
			t.Fatalf("explicit=%v: template=%s config=%v", explicit, actual.String(), normalized.Metadata)
		}
		if _, inherited := normalized.Metadata["inherited"]; inherited == explicit {
			t.Fatalf("explicit=%v inherited Group default: %v", explicit, normalized.Metadata)
		}
		resource, err := sandboxcfg.ParseResourcePatch(normalized.Metadata[sandboxcfg.NsResource])
		if err != nil || resource.Allocatable == nil || resource.Allocatable.Memory == nil || *resource.Allocatable.Memory != "512MiB" || resource.Startup == nil || resource.Startup.Memory == nil || *resource.Startup.Memory != "1GiB" {
			t.Fatalf("request resource leaves lost: %+v %v", resource, err)
		}
		if explicit {
			if resource.Capacity != nil || resource.Allocatable.CPU != nil || normalized.Metadata[sandboxcfg.NsTraffic] != `{"max_inflight":{"forward":0,"exec":2}}` {
				t.Fatal("explicit template inherited Group resource/traffic defaults")
			}
		} else if resource.Capacity == nil || resource.Capacity.CPU == nil || *resource.Capacity.CPU != 4 || resource.Allocatable.CPU == nil || *resource.Allocatable.CPU != 0.5 || normalized.Metadata[sandboxcfg.NsTraffic] != `{"max_inflight":{"total":20,"forward":0,"exec":2}}` {
			t.Fatal("default template did not merge Group resource/traffic leaves")
		}
	}
	if !reflect.DeepEqual(cmd.Config, wantRequest) {
		t.Fatal("Group merge mutated the request")
	}
}

func TestGroupCreateKeepsCopiedInputsAcrossConcurrentUpdate(t *testing.T) {
	cfg := &config.Config{}
	cfg.Sandbox.TimeoutSec = 60
	gate := make(chan struct{})
	defer close(gate)
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{startGate: gate})
	_, _, fp := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(t, o, fp, "group-hook-race")
	o.SetExtensionHooks(sandboxHookFunc(func(ctx context.Context, _ *conductorextension.SandboxOperation) error {
		_, err := o.GroupPut(ctx, cmd.GroupAPIKey, cmd.Cluster.Group, nodegroup.PutRequest{
			ExpectedVersion: cmd.ExpectedGroupVersion, Context: nodegroup.Context{TargetPort: 8080},
		})
		return err
	}), nil)
	ack := o.HandleCommand(ctx, cmd)
	if ack.Status != routesync.AckAccepted {
		t.Fatalf("copied Group inputs were revoked during core admission: %+v", ack)
	}
	if sb, err := o.st.Get(ctx, cmd.SID); err != nil || sb == nil || sb.TemplateID != cmd.TemplateRef {
		t.Fatalf("copied Group input did not reach durable acceptance: %v %v", sb, err)
	}
	next := *cmd
	next.SID = "group-hook-next"
	ack = o.HandleCommand(ctx, &next)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusConflict || ack.ErrorCode != routesync.ErrorGroupVersionConflict {
		t.Fatalf("new invocation reused stale revision: %+v", ack)
	}
}

func TestGroupAcceptedCreateIsNotNonexecutionAfterWithdrawal(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	o, ctx := newAsyncConnectTestOrchestrator(t, &config.Config{}, &countingLauncher{startGate: gate})
	_, _, fp := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(t, o, fp, "group-lost-ack")
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("initial acceptance: %+v", ack)
	}
	if _, err := o.GroupDelete(ctx, cmd.GroupAPIKey, cmd.Cluster.Group, cmd.ExpectedGroupVersion); err != nil {
		t.Fatal(err)
	}
	if err := o.dropClusterKey(ctx, fp); err != nil {
		t.Fatal(err)
	}
	ack := o.HandleCommand(ctx, cmd)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusConflict || ack.ErrorCode != "" {
		t.Fatalf("accepted identity lost after withdrawal: %+v", ack)
	}
	if sb, err := o.st.Get(ctx, cmd.SID); err != nil || sb == nil {
		t.Fatalf("accepted create disappeared: %v %v", sb, err)
	}
}

func TestGroupBuildUsesPrivateContextAndReplaysAfterWithdrawal(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	_, _, fp := allowlistedBuildIdentity(t, o)
	cmd := clusterBuildRegisterCommand(t, o, "group-build-lost-ack", fp)
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("initial acceptance: %+v", ack)
	}
	b, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil || b == nil || b.RegistrationImageRepo != "registry.test/repo" || b.RegistrationRegistryAuth != testGroupRegistryAuth {
		t.Fatalf("private Build inputs were not retained: build=%v err=%v", b != nil, err)
	}
	if _, err := o.GroupDelete(ctx, cmd.GroupAPIKey, "/test", cmd.ExpectedGroupVersion); err != nil {
		t.Fatal(err)
	}
	if err := o.dropClusterKey(ctx, fp); err != nil {
		t.Fatal(err)
	}
	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("lost-ACK replay after Group withdrawal: %+v", ack)
	}
	changed := *cmd
	changed.ExpectedGroupVersion++
	ack := o.HandleCommand(ctx, &changed)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusConflict || ack.ErrorCode != "" {
		t.Fatalf("different accepted request was treated as nonexecution: %+v", ack)
	}
}
