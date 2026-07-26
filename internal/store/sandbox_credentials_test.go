package store

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testSandboxWithServiceCredentials(id string, profile types.Profile) *types.Sandbox {
	sb := &types.Sandbox{
		ID:          id,
		Profile:     profile,
		TemplateID:  string(profile) + "-img-" + strings.Repeat("a", 64),
		State:       types.StateRunning,
		APISecret:   strings.Repeat("1", 64),
		ManifestKey: strings.Repeat("2", 64),
		CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(sb)
	return sb
}

func TestSandboxServiceCredentialsAreEncryptedAndRoundTrip(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandboxWithServiceCredentials("sandbox-service-credentials", types.ProfileE2B)

	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	var serviceEnc, envdEnc, trafficEnc, forwardEnc string
	if err := st.db.QueryRowContext(ctx, `
		SELECT service_secret_enc,envd_access_token_enc,traffic_access_token_enc,forward_access_token_enc
		FROM sandboxes WHERE id=?`, sb.ID,
	).Scan(&serviceEnc, &envdEnc, &trafficEnc, &forwardEnc); err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string]struct {
		ciphertext string
		plaintext  string
	}{
		"service secret":       {serviceEnc, sb.ServiceSecret},
		"envd access token":    {envdEnc, sb.EnvdAccessToken},
		"traffic access token": {trafficEnc, sb.TrafficAccessToken},
		"forward access token": {forwardEnc, sb.ForwardAccessToken},
	} {
		if pair.ciphertext == "" || pair.ciphertext == pair.plaintext {
			t.Fatalf("%s was not encrypted", name)
		}
		plaintext, err := st.box.DecryptString(pair.ciphertext)
		if err != nil || plaintext != pair.plaintext {
			t.Fatalf("decrypt %s = %q, err=%v", name, plaintext, err)
		}
	}

	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ServiceSecret != sb.ServiceSecret ||
		got.EnvdAccessToken != sb.EnvdAccessToken ||
		got.TrafficAccessToken != sb.TrafficAccessToken ||
		got.ForwardAccessToken != sb.ForwardAccessToken {
		t.Fatalf("service credential round trip = %+v", got)
	}
}

func TestSandboxServiceCredentialsAreInsertBound(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandboxWithServiceCredentials("sandbox-insert-bound", types.ProfileE2B)
	originalService := sb.ServiceSecret
	originalEnvd := sb.EnvdAccessToken
	originalTraffic := sb.TrafficAccessToken
	originalForward := sb.ForwardAccessToken
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	sb.ServiceSecret = strings.Repeat("3", 64)
	sb.EnvdAccessToken = "replacement-envd-access-token"
	sb.TrafficAccessToken = "replacement-traffic-access-token"
	replacementForward, err := keys.MintForwardAccessToken(sb.ServiceSecret, sb.AuthSandboxID())
	if err != nil {
		t.Fatal(err)
	}
	sb.ForwardAccessToken = replacementForward
	sb.State = types.StatePaused
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	got, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ServiceSecret != originalService ||
		got.EnvdAccessToken != originalEnvd ||
		got.TrafficAccessToken != originalTraffic ||
		got.ForwardAccessToken != originalForward {
		t.Fatalf("lifecycle upsert rebound service credentials: %+v", got)
	}
	if got.State != types.StatePaused {
		t.Fatalf("lifecycle state = %q, want %q", got.State, types.StatePaused)
	}
}

func TestSandboxServiceCredentialValidation(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		profile types.Profile
		mutate  func(*types.Sandbox)
	}{
		{name: "missing service secret", profile: types.ProfileBare, mutate: func(sb *types.Sandbox) { sb.ServiceSecret = "" }},
		{name: "non canonical service secret", profile: types.ProfileBare, mutate: func(sb *types.Sandbox) { sb.ServiceSecret = strings.Repeat("A", 64) }},
		{name: "missing forward access token", profile: types.ProfileBare, mutate: func(sb *types.Sandbox) { sb.ForwardAccessToken = "" }},
		{name: "e2b missing envd access token", profile: types.ProfileE2B, mutate: func(sb *types.Sandbox) { sb.EnvdAccessToken = "" }},
		{name: "e2b missing traffic access token", profile: types.ProfileE2B, mutate: func(sb *types.Sandbox) { sb.TrafficAccessToken = "" }},
		{name: "e2b oversized envd access token", profile: types.ProfileE2B, mutate: func(sb *types.Sandbox) { sb.EnvdAccessToken = strings.Repeat("e", 257) }},
		{name: "e2b oversized traffic access token", profile: types.ProfileE2B, mutate: func(sb *types.Sandbox) { sb.TrafficAccessToken = strings.Repeat("t", 257) }},
		{name: "e2b invalid UTF-8 access token", profile: types.ProfileE2B, mutate: func(sb *types.Sandbox) { sb.EnvdAccessToken = string([]byte{0xff}) }},
		{name: "bare has envd access token", profile: types.ProfileBare, mutate: func(sb *types.Sandbox) { sb.EnvdAccessToken = "unexpected" }},
		{name: "bare has traffic access token", profile: types.ProfileBare, mutate: func(sb *types.Sandbox) { sb.TrafficAccessToken = "unexpected" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := testStore(t)
			sb := testSandboxWithServiceCredentials("sandbox-invalid", tc.profile)
			tc.mutate(sb)
			if err := st.Put(ctx, sb); err == nil {
				t.Fatal("invalid sandbox service credentials were accepted")
			}
		})
	}
}

func TestSandboxStoreRejectsInvalidLocalID(t *testing.T) {
	st := testStore(t)
	sb := testSandboxWithServiceCredentials("../sandbox", types.ProfileBare)
	if err := st.Put(context.Background(), sb); err == nil || !strings.Contains(err.Error(), "invalid sandbox id") {
		t.Fatalf("invalid local sandbox ID error = %v", err)
	}
}

func TestSandboxMissingPersistedServiceCredentialIsCorrupt(t *testing.T) {
	ctx := context.Background()
	for _, column := range []string{"service_secret_enc", "forward_access_token_enc"} {
		t.Run(column, func(t *testing.T) {
			st := testStore(t)
			sb := testSandboxWithServiceCredentials("sandbox-corrupt", types.ProfileBare)
			if err := st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
			emptyEnc, err := st.box.EncryptString("")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx,
				`UPDATE sandboxes SET `+column+`=? WHERE id=?`, emptyEnc, sb.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Get(ctx, sb.ID); err == nil {
				t.Fatal("missing persisted sandbox service credential was accepted")
			}
		})
	}
}

func TestSandboxPersistedForwardTokenMustMatchServiceSecretAndSubject(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandboxWithServiceCredentials("sandbox-wrong-forward-subject", types.ProfileBare)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	wrongToken, err := keys.MintForwardAccessToken(sb.ServiceSecret, "different-subject")
	if err != nil {
		t.Fatal(err)
	}
	wrongEnc, err := st.box.EncryptString(wrongToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx,
		`UPDATE sandboxes SET forward_access_token_enc=? WHERE id=?`, wrongEnc, sb.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, sb.ID); err == nil {
		t.Fatal("forward token for a different AuthSandboxID was accepted")
	}
}
