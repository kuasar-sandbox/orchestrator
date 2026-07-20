package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateDragonboatSoftSettings(t *testing.T) {
	dir := t.TempDir()
	writeDragonboatSettingsTestFile(t, dir, requiredDragonboatSoftSettings, 0o644)
	if err := validateDragonboatSoftSettingsAt(filepath.Join(dir, "registry.yaml"), dir); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentDragonboatSoftSettings(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/" + dragonboatSoftSettingsFilename)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, dragonboatSoftSettingsFilename), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateDragonboatSoftSettingsAt("registry.yaml", dir); err != nil {
		t.Fatal(err)
	}
}

func TestValidateDragonboatSoftSettingsRejectsUnsafeLaunch(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(*testing.T, string)
		cwd       func(*testing.T, string) string
		wantError string
	}{
		{
			name:      "missing",
			prepare:   func(*testing.T, string) {},
			cwd:       func(_ *testing.T, dir string) string { return dir },
			wantError: dragonboatSoftSettingsFilename,
		},
		{
			name: "wrong directory",
			prepare: func(t *testing.T, dir string) {
				writeDragonboatSettingsTestFile(t, dir, requiredDragonboatSoftSettings, 0o644)
			},
			cwd: func(t *testing.T, dir string) string {
				other := filepath.Join(dir, "other")
				if err := os.Mkdir(other, 0o755); err != nil {
					t.Fatal(err)
				}
				return other
			},
			wantError: "must be the Registry config directory",
		},
		{
			name: "unexpected value",
			prepare: func(t *testing.T, dir string) {
				settings := requiredDragonboatSoftSettings
				settings.ReceiveQueueLength++
				writeDragonboatSettingsTestFile(t, dir, settings, 0o644)
			},
			cwd:       func(_ *testing.T, dir string) string { return dir },
			wantError: "does not match",
		},
		{
			name: "writable by group",
			prepare: func(t *testing.T, dir string) {
				writeDragonboatSettingsTestFile(t, dir, requiredDragonboatSoftSettings, 0o664)
			},
			cwd:       func(_ *testing.T, dir string) string { return dir },
			wantError: "must not be writable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			test.prepare(t, dir)
			err := validateDragonboatSoftSettingsAt(
				filepath.Join(dir, "registry.yaml"), test.cwd(t, dir),
			)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func writeDragonboatSettingsTestFile(
	t *testing.T,
	dir string,
	settings dragonboatSoftSettings,
	mode os.FileMode,
) {
	t.Helper()
	raw, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, dragonboatSoftSettingsFilename)
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}
