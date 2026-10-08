package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

func recordRunnerE2ELaunch(work string, spec configsock.LaunchSpec, source types.ResumeSource, locations map[string]string) error {
	// Retain the former argument observation format for exact source assertions;
	// these are task preparation outputs, never credentials or guest environment.
	args := append([]string(nil), spec.Args...)
	switch source.Kind {
	case types.ResumeSourceSandbox:
		args = append(args, "--from", source.Ref)
	case types.ResumeSourceSnapshot:
		args = append(args, "--restore", source.Ref)
	}
	names := make([]string, 0, len(locations))
	for name := range locations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--ref-location", name+"="+locations[name])
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(work, "run-argv.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(body, '\n'))
	return errors.Join(err, f.Close())
}

func executeRunnerE2E(ctx context.Context, work string, input *sandboxsdk.Input, cg, wire *os.File, ready *sandboxsdk.Readiness) (int, error) {
	path := filepath.Join(work, "inject-sandbox-run")
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return sandboxsdk.Execute(ctx, input, cg, ready)
	}
	if err != nil {
		return 2, err
	}
	switch mode := strings.TrimSpace(string(body)); mode {
	case "hold", "park":
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				if mode == "park" {
					return sandboxsdk.Execute(ctx, input, cg, ready)
				}
				return 44, nil
			} else if err != nil {
				return 2, err
			}
			select {
			case <-ctx.Done():
				return 44, ctx.Err()
			case <-ticker.C:
			}
		}
	case "runtime-wire-failure":
		// Deliberately bypass the validating SDK bridge to exercise the actual
		// conductor wire parser's malformed-event fencing contract.
		_, err := wire.WriteString("control_ready\ninvalid_runtime_event\n")
		return 42, errors.Join(err, ready.Close())
	case "envd-init-failure":
		path := filepath.Join(input.RunRoot, input.PathID, "envd.sock")
		listener, err := net.Listen("unix", path)
		if err != nil {
			return 43, err
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}), ReadHeaderTimeout: 5 * time.Second}
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		ready.Notify(sandbox.ReadinessControlReady)
		ready.Notify(sandbox.ReadinessReady)
		select {
		case <-ctx.Done():
			_ = server.Close()
			err = <-done
		case err = <-done:
			_ = server.Close()
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		return 43, err
	default:
		return 2, fmt.Errorf("unknown sandbox run injection: %q", mode)
	}
}
