package orch

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

const (
	runKindSandbox = "sandbox"
	runKindBuild   = "build"

	runPrefixSandbox = "sr-"
	runPrefixBuild   = "br-"
)

var runIDRe = regexp.MustCompile(`^(sr|br)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func newRunID(kind string) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	switch kind {
	case runKindSandbox:
		return runPrefixSandbox + id.String(), nil
	case runKindBuild:
		return runPrefixBuild + id.String(), nil
	default:
		return "", fmt.Errorf("unknown run kind %q", kind)
	}
}

func validRunID(kind, runID string) bool {
	if !runIDRe.MatchString(runID) {
		return false
	}
	switch kind {
	case runKindSandbox:
		return len(runID) > len(runPrefixSandbox) && runID[:len(runPrefixSandbox)] == runPrefixSandbox
	case runKindBuild:
		return len(runID) > len(runPrefixBuild) && runID[:len(runPrefixBuild)] == runPrefixBuild
	default:
		return false
	}
}
