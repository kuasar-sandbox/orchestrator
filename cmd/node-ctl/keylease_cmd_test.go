package main

import (
	"strings"
	"testing"
)

func TestBuildRegistryAuthRejectsPasswordWithoutUsername(t *testing.T) {
	if _, err := buildRegistryAuth("", "", "password", ""); err == nil || !strings.Contains(err.Error(), "requires --registry-username") {
		t.Fatalf("password-only registry auth error = %v", err)
	}
}
