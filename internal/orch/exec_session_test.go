package orch

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExecSessionMintsTokenForAuthenticatedStableSubject(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID:                 "stable-g2",
		Profile:            types.ProfileBare,
		AuthSandboxIDValue: "stable",
		TemplateID:         "bare-img-" + strings.Repeat("b", 64),
		State:              types.StateRunning,
		APISecret:          deriveTestAPISecret(t, manifestKey),
		ManifestKey:        manifestKey,
		RunDir:             filepath.Join(t.TempDir(), "run"),
		BaseDir:            filepath.Join(t.TempDir(), "lib"),
		CreatedUnix:        1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Unix()
	token, err := o.ExecSession(ctx, sb.ID, apiKey, "ignored-for-existing-target", 37)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().Unix()
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(before+36, 0)); err != nil {
		t.Fatalf("minted token before expiry: %v", err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(after+38, 0)); err == nil {
		t.Fatal("minted token remained valid after ttl")
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.ID, time.Unix(before, 0)); err == nil {
		t.Fatal("minted token accepted node-local ID instead of stable AuthSandboxID")
	}
}

func TestExecSessionWithoutTTLIsLongLived(t *testing.T) {
	sb := &types.Sandbox{
		ID: "bare-1", ServiceSecret: strings.Repeat("1", 64),
	}
	token, err := mintExecSessionToken(sb, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.ID, time.Unix(math.MaxInt64, 0)); err != nil {
		t.Fatalf("long-lived token at distant time: %v", err)
	}
}

func TestExecSessionRejectsUnauthorizedAndInvalidTTL(t *testing.T) {
	o := testOrch(t)
	if _, err := o.ExecSession(context.Background(), "missing", "invalid", "", 0); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("missing target error = %v, want not found", err)
	}
	if _, err := o.ExecSession(context.Background(), "overflow-target", "invalid", "kmt1.not-opened", math.MaxInt64); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("overflow request error = %v, want bad request", err)
	}
	if sb, err := o.st.Get(context.Background(), "overflow-target"); err != nil || sb != nil {
		t.Fatalf("overflow request reached import: sandbox=%+v err=%v", sb, err)
	}
	for _, test := range []struct {
		name string
		now  int64
		ttl  int64
	}{
		{name: "negative", now: 1, ttl: -1},
		{name: "overflow", now: math.MaxInt64 - 1, ttl: 2},
		{name: "invalid clock", now: 0, ttl: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := execSessionExpiry(test.now, test.ttl); !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("execSessionExpiry() error = %v, want bad request", err)
			}
		})
	}
}
