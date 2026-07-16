package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestRunRegistryRejectsPositionalArguments(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, arg := range []string{"export", "import", "unexpected"} {
		t.Run(arg, func(t *testing.T) {
			err := runRegistry([]string{arg}, log)
			if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
				t.Fatalf("runRegistry(%q) error=%v", arg, err)
			}
		})
	}
}
