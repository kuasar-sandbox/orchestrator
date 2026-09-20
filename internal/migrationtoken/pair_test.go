package migrationtoken

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestAuthenticatedSnapshotPairStrictness(t *testing.T) {
	p := validPayload()
	p.ResumeSourceRef = "file://shared.bundle@manifest:" + strings.Repeat("a", 64) + "@location:checkpoint"
	p.ResumeSandboxRef = "file://shared.bundle@manifest:" + strings.Repeat("b", 64) + "@location:checkpoint"
	token, err := Seal(testMaterial, p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(testMaterial, token)
	if err != nil {
		t.Fatal(err)
	}
	expected := types.ResumeSource{Kind: p.ResumeSourceKind, Ref: p.ResumeSourceRef, SandboxRef: p.ResumeSandboxRef}
	if err := ValidateExpectations(got, Expectations{ResumeSource: expected}); err != nil {
		t.Fatal(err)
	}
	expected.SandboxRef = "manifest://" + strings.Repeat("c", 64)
	if err := ValidateExpectations(got, Expectations{ResumeSource: expected}); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("same S different E accepted: %v", err)
	}
	raw, _ := json.Marshal(p)
	field := `"resumeSandboxRef":"` + p.ResumeSandboxRef + `"`
	for name, plain := range map[string]string{
		"missing E":          strings.Replace(string(raw), field+",", "", 1),
		"duplicate E":        strings.Replace(string(raw), field, field+","+field, 1),
		"null E":             strings.Replace(string(raw), field, `"resumeSandboxRef":null`, 1),
		"unknown pair field": strings.TrimSuffix(string(raw), "}") + `,"sandboxRef":"x"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(testMaterial, encryptPlaintext(t, []byte(plain))); !errors.Is(err, ErrMalformedToken) {
				t.Fatalf("strict pair JSON=%v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*MigrationTokenPayloadV1){
		"empty E":                 func(p *MigrationTokenPayloadV1) { p.ResumeSandboxRef = "" },
		"local E":                 func(p *MigrationTokenPayloadV1) { p.ResumeSandboxRef = "file:///private/root.sandbox" },
		"wrong E role":            func(p *MigrationTokenPayloadV1) { p.ResumeSandboxRef = "file://root.snapshot@location:checkpoint" },
		"E-only with association": func(p *MigrationTokenPayloadV1) { p.ResumeSourceKind = types.ResumeSourceSandbox },
	} {
		t.Run(name, func(t *testing.T) {
			bad := p
			mutate(&bad)
			raw, _ := json.Marshal(bad)
			if _, err := Seal(testMaterial, bad); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Seal=%v", err)
			}
			if _, err := Open(testMaterial, encryptPlaintext(t, raw)); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Open=%v", err)
			}
		})
	}
	p.ResumeSourceKind = types.ResumeSourceSandbox
	p.ResumeSourceRef = p.ResumeSandboxRef
	p.ResumeSandboxRef = ""
	token, err = Seal(testMaterial, p)
	if err != nil {
		t.Fatal(err)
	}
	got, err = Open(testMaterial, token)
	if err != nil || got.ResumeSourceRef != p.ResumeSourceRef || got.ResumeSandboxRef != "" {
		t.Fatalf("E-only=%+v %v", got, err)
	}
}
