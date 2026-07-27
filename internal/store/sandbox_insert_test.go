package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func sandboxInsertFixture(id string, candidate int) *types.Sandbox {
	hexDigit := fmt.Sprintf("%x", candidate+1)
	sb := &types.Sandbox{
		ID:                 id,
		Profile:            types.ProfileE2B,
		Cluster:            &types.ClusterSandboxContext{Group: fmt.Sprintf("/group-%d", candidate), RouteKey: fmt.Sprintf("route-%d", candidate)},
		AuthSandboxIDValue: fmt.Sprintf("stable-%d", candidate),
		TemplateID:         "e2b-img-" + strings.Repeat(hexDigit, 64),
		State:              types.StateRunning,
		DeadlineUnix:       int64(100 + candidate),
		RunDir:             fmt.Sprintf("/run/%d", candidate),
		BaseDir:            fmt.Sprintf("/base/%d", candidate),
		RunID:              fmt.Sprintf("run-%d", candidate),
		EnvdUDS:            fmt.Sprintf("/envd/%d.sock", candidate),
		CiUDS:              fmt.Sprintf("/ci/%d.sock", candidate),
		FloatingIP:         fmt.Sprintf("192.0.2.%d", candidate+1),
		VswitchPort:        fmt.Sprintf("port-%d", candidate),
		InnerIP:            fmt.Sprintf("198.51.100.%d", candidate+1),
		PortMAC:            fmt.Sprintf("02:00:00:00:00:%02x", candidate),
		APISecret:          strings.Repeat(hexDigit, 64),
		ManifestKey:        strings.Repeat(fmt.Sprintf("%x", candidate+9), 64),
		SnapshotRef:        fmt.Sprintf("snapshot-%d", candidate),
		Metadata:           map[string]string{"candidate": fmt.Sprint(candidate), "metadata": fmt.Sprintf("value-%d", candidate)},
		Env:                map[string]string{"CANDIDATE": fmt.Sprint(candidate), "ENV": fmt.Sprintf("value-%d", candidate)},
		CreatedUnix:        int64(1000 + candidate),
		ServiceSecret:      strings.Repeat(fmt.Sprintf("%x", candidate+2), 64),
		EnvdAccessToken:    fmt.Sprintf("envd-%d", candidate),
		TrafficAccessToken: fmt.Sprintf("traffic-%d", candidate),
	}
	forwardToken, err := keys.MintForwardAccessToken(sb.ServiceSecret, sb.AuthSandboxID())
	if err != nil {
		panic(err)
	}
	sb.ForwardAccessToken = forwardToken
	return sb
}

func TestInsertSandboxConflictDoesNotChangeExistingRecord(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	original := sandboxInsertFixture("insert-conflict", 0)
	if err := st.InsertSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}

	replacement := sandboxInsertFixture(original.ID, 1)
	replacement.Profile = types.ProfileBare
	replacement.EnvdAccessToken = ""
	replacement.TrafficAccessToken = ""
	if err := st.InsertSandbox(ctx, replacement); !errors.Is(err, ErrSandboxExists) {
		t.Fatalf("conflicting insert error = %v, want ErrSandboxExists", err)
	}

	got, err := st.Get(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatalf("conflicting insert changed existing record:\n got: %#v\nwant: %#v", got, original)
	}
}

func TestInsertSandboxConcurrentConflictHasSingleWinner(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	const contenders = 6

	candidates := make([]*types.Sandbox, contenders)
	results := make(chan struct {
		candidate int
		err       error
	}, contenders)
	start := make(chan struct{})
	for i := range candidates {
		candidates[i] = sandboxInsertFixture("concurrent-insert", i)
		go func(candidate int) {
			<-start
			results <- struct {
				candidate int
				err       error
			}{candidate: candidate, err: st.InsertSandbox(ctx, candidates[candidate])}
		}(i)
	}
	close(start)

	winner := -1
	for range contenders {
		result := <-results
		switch {
		case result.err == nil:
			if winner != -1 {
				t.Fatalf("candidates %d and %d both inserted successfully", winner, result.candidate)
			}
			winner = result.candidate
		case errors.Is(result.err, ErrSandboxExists):
		default:
			t.Fatalf("candidate %d insert error = %v", result.candidate, result.err)
		}
	}
	if winner == -1 {
		t.Fatal("no concurrent insert succeeded")
	}

	got, err := st.Get(ctx, candidates[winner].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, candidates[winner]) {
		t.Fatalf("stored concurrent winner:\n got: %#v\nwant: %#v", got, candidates[winner])
	}
}
