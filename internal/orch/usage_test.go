package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/usage"
)

func usageSandbox(t *testing.T, o *Orchestrator) (*types.Sandbox, usage.Record, []byte) {
	t.Helper()
	sb := &types.Sandbox{ID: "usage-sid", StableIDValue: "stable-alias", Profile: types.ProfileBare, State: types.StatePaused,
		TemplateID:   types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		ResumeSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64)},
		BaseDir:      t.TempDir(), APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	record := usage.Record{Sequence: 1, SavedUTC: 99, Snapshot: usage.Snapshot{SandboxID: sb.ID, RunEpoch: "native-epoch", StartedUTC: 1, SampleInterval: 1e9, FlushInterval: 5e9,
		Counters: []usage.Counter{{Name: "guest.cpu", Source: "boot/pid/start", KnownTotal: usage.Uint128{Hi: 4, Lo: 9007199254740993}, LastRaw: 456, Hertz: 100, SourceKnown: true, Complete: true, Status: usage.OK}},
		Gauges:   []usage.Gauge{{Name: "guest.memory", Source: "ram", IntegralTotal: usage.Uint128{Hi: 8, Lo: 9007199254740993}, SpanTotal: 10, CoveredTotal: 9, Status: usage.Missing}}}}
	raw, err := usage.EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sb.BaseDir, sb.ID+".usage"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return sb, record, raw
}
func TestUsageStatsPausedExactIdentityHistoryAndNoReadEffects(t *testing.T) {
	o := testOrch(t)
	sb, record, before := usageSandbox(t, o)
	ctx := context.Background()
	key := mintTestAPIKey(t, sb.APISecret)
	for _, view := range []string{"current", "saved"} {
		raw, err := o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{View: view})
		if err != nil {
			t.Fatal(err)
		}
		var got usage.View
		if err := json.Unmarshal(raw, &got); err != nil || got.Live != nil || !reflect.DeepEqual(got.Saved, &record) {
			t.Fatal("native snapshot changed", string(raw), err)
		}
	}
	raw, err := o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{View: "history", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Records []usage.Record `json:"records"`
		Next    int64          `json:"next_cursor,string"`
	}
	if err := json.Unmarshal(raw, &page); err != nil || len(page.Records) != 1 || !reflect.DeepEqual(page.Records[0], record) || page.Next != int64(len(before)) {
		t.Fatal("history loss", string(raw), err)
	}
	raw, err = o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{View: "history", Cursor: page.Next})
	if err != nil || !bytes.Contains(raw, []byte(`"records":[]`)) {
		t.Fatal(string(raw), err)
	}
	for _, id := range []string{"stable-alias", "other"} {
		if _, err := o.UsageStats(ctx, id, key, conductorextension.UsageQuery{}); !errors.Is(err, api.ErrNotFound) {
			t.Fatal("alias/fallback", err)
		}
	}
	if _, err := o.UsageStats(ctx, sb.ID, mintTestAPIKey(t, strings.Repeat("3", 64)), conductorextension.UsageQuery{}); !errors.Is(err, api.ErrNotFound) {
		t.Fatal("ownership", err)
	}
	after, err := os.ReadFile(filepath.Join(sb.BaseDir, sb.ID+".usage"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read changed native file", err)
	}
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil || current.State != types.StatePaused || current.RunID != "" {
		t.Fatal("usage read woke sandbox", err)
	}
	rows, err := o.ReadStats(ctx, conductorextension.StatsRequest{SandboxIDs: []string{sb.ID}, Sections: []string{"usage"}})
	if err != nil || len(rows) != 1 || rows[0].SandboxID != sb.ID || rows[0].StableID != "stable-alias" {
		t.Fatal("trusted paused usage", rows, err)
	}
	var got usage.View
	if err := json.Unmarshal(rows[0].Usage, &got); err != nil || !reflect.DeepEqual(got.Saved, &record) {
		t.Fatal("batch uses different native reader", err)
	}
}

func TestUsageStatsLiveOwnerErrorsLocksAndCancellation(t *testing.T) {
	o := testOrch(t)
	sb, _, before := usageSandbox(t, o)
	ctx := context.Background()
	key := mintTestAPIKey(t, sb.APISecret)
	sb.State, sb.RunDir, sb.RunID = types.StateRunning, t.TempDir(), "internal-run"
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	manager, err := usage.Open(sb.BaseDir, sb.ID, "next-native-epoch", time.Now(), time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(ctx, time.Now())
	server := &ctl.Server{Path: filepath.Join(sb.RunDir, "ctl.sock"), SnapshotHandler: func(ctl.Request) (ctl.Response, error) {
		t.Error("query triggered snapshot")
		return ctl.Response{}, errors.New("unexpected")
	}, UsageHandler: func(ctl.Request) (ctl.Response, error) {
		body, err := json.Marshal(manager.View())
		return ctl.Response{SandboxID: sb.ID, Usage: body}, err
	}}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	ownerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Serve(ownerCtx) }()
	defer func() { stop(); <-done }()
	raw, err := o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{})
	if err != nil {
		t.Fatal(err)
	}
	var view usage.View
	if err := json.Unmarshal(raw, &view); err != nil || !reflect.DeepEqual(view, manager.View()) {
		t.Fatal("live source changed", string(raw), err)
	}
	raw, err = o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{View: "saved"})
	if err != nil {
		t.Fatal(err)
	}
	// Decode a fresh value: an omitted Live must not inherit the previous decode.
	var saved usage.View
	if err := json.Unmarshal(raw, &saved); err != nil || saved.Live != nil || saved.Saved == nil {
		t.Fatal("saved view lost", string(raw), err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := o.UsageStats(canceled, sb.ID, key, conductorextension.UsageQuery{}); err == nil {
		t.Fatal("canceled read succeeded")
	}
	sb.State, sb.RunDir = types.StatePaused, ""
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.UsageStats(ctx, sb.ID, key, conductorextension.UsageQuery{}); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatal("offline read bypassed owner lock", err)
	}
	after, err := os.ReadFile(filepath.Join(sb.BaseDir, sb.ID+".usage"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("query sampled or saved", err)
	}
}

func TestUsageStatsRejectsReplacedBaseBinding(t *testing.T) {
	o := testOrch(t)
	sb, record, _ := usageSandbox(t, o)
	sb.State, sb.RunDir, sb.RunID = types.StateRunning, t.TempDir(), "same-runtime"
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	replacement := *sb
	replacement.BaseDir = t.TempDir()
	server := &ctl.Server{Path: filepath.Join(sb.RunDir, "ctl.sock"), UsageHandler: func(ctl.Request) (ctl.Response, error) {
		if err := o.st.Put(context.Background(), &replacement); err != nil {
			return ctl.Response{}, err
		}
		body, err := json.Marshal(usage.View{Saved: &record})
		return ctl.Response{SandboxID: sb.ID, Usage: body}, err
	}, SnapshotHandler: func(ctl.Request) (ctl.Response, error) { return ctl.Response{}, errors.New("unexpected snapshot") }}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() { cancel(); <-done }()
	if body, err := o.UsageStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret), conductorextension.UsageQuery{}); !errors.Is(err, api.ErrStatsUnavailable) || body != nil {
		t.Fatal("accepted a sample after its base binding changed", string(body), err)
	}
}

func TestUsageStatsRejectsPausedIdentityReuse(t *testing.T) {
	for _, change := range []string{"tenant", "stable-label", "cluster-owner"} {
		t.Run(change, func(t *testing.T) {
			o := testOrch(t)
			expected, _, before := usageSandbox(t, o)
			ctx := context.Background()
			key := mintTestAPIKey(t, expected.APISecret)
			if !ownsSandbox(expected, key) {
				t.Fatal("initial read was not authorized")
			}
			// Resume a read after its initial ownership check. A delete and
			// insert may reuse every path and the second-resolution timestamp;
			// the saved file remains readable throughout the replacement.
			replacement := *expected
			switch change {
			case "tenant":
				replacement.APISecret = strings.Repeat("3", 64)
			case "stable-label":
				replacement.StableIDValue = "replacement-label"
			case "cluster-owner":
				replacement.Cluster = &types.ClusterSandboxContext{Group: "/replacement", RouteKey: "route"}
			}
			materializeTestSandboxCredentials(t, &replacement)
			if err := o.st.Delete(ctx, expected.ID); err != nil {
				t.Fatal(err)
			}
			if err := o.st.Put(ctx, &replacement); err != nil {
				t.Fatal(err)
			}
			if body, err := o.readUsageStats(ctx, expected, conductorextension.UsageQuery{}); body != nil || !errors.Is(err, api.ErrStatsUnavailable) {
				t.Fatal("published usage under a replaced identity", string(body), err)
			}
			if change == "tenant" {
				if _, err := o.UsageStats(ctx, expected.ID, key, conductorextension.UsageQuery{}); !errors.Is(err, api.ErrNotFound) {
					t.Fatal("old tenant still owns reused SandboxID", err)
				}
			}
			if _, err := o.UsageStats(ctx, replacement.ID, mintTestAPIKey(t, replacement.APISecret), conductorextension.UsageQuery{}); err != nil {
				t.Fatal("current owner could not read saved usage", err)
			}
			after, err := os.ReadFile(filepath.Join(expected.BaseDir, expected.ID+".usage"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("read changed saved usage", err)
			}
		})
	}
}
