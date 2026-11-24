package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/sqlengine"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/storagetest"
)

type nativeExitConfig struct{ Backend, SQLitePath, Namespace string }
type nativeExitReceipt struct {
	Record metadata.SandboxRecord
	Writes int32
}

var nativeExitOpeners = map[string]func(*testing.T, nativeExitConfig) storagetest.Backend{
	"sqlite": func(t *testing.T, c nativeExitConfig) storagetest.Backend {
		e, err := sqlengine.OpenSQLite(t.Context(), c.SQLitePath, c.Namespace)
		if err != nil {
			t.Fatal(err)
		}
		return e
	},
	"postgres": func(t *testing.T, c nativeExitConfig) storagetest.Backend {
		e, err := sqlengine.OpenPostgres(t.Context(), os.Getenv("STORAGE_TEST_POSTGRES_DSN"), c.Namespace)
		if err != nil {
			t.Fatal(err)
		}
		return e
	},
}

// The child runs the actual ReserveSandbox path and native transaction. The
// receipt is written only after the physical commit, while its return to the
// Registry is blocked: no owner RPC or NodeLink write can have happened yet.
func TestRegistryNativeCommitExitHelper(t *testing.T) {
	path := os.Getenv("REGISTRY_NATIVE_EXIT_CONFIG")
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c nativeExitConfig
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	r, engine := newNativeFaultRegistry(t, func() storagetest.Backend { return nativeExitOpeners[c.Backend](t, c) })
	publishTestGroups(t, r, "n1", "/g")
	fault := &captureNativeIntent{RouteRepository: r.stores.routes, engine: engine, mode: 2}
	r.stores.routes = fault
	engine.afterCommit = func() {
		pipe := os.NewFile(3, "committed-intent")
		if err := json.NewEncoder(pipe).Encode(nativeExitReceipt{Record: fault.record, Writes: engine.writes.Load()}); err != nil {
			t.Fatal(err)
		}
		if err := pipe.Close(); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Minute)
		}
	}
	owner := &groupCommandOwner{}
	owner.call = func(context.Context, string, *routesync.Command) (*routesync.CmdAck, error) {
		t.Fatal("dispatch crossed the blocked physical commit return")
		return nil, errors.New("unreachable")
	}
	r.SetNodeOwner(owner)
	_, err = r.ReserveSandbox(t.Context(), testCreateReserve("/g", "commit-exit", nil))
	t.Fatalf("commit barrier returned: %v", err)
}

func TestRegistryNativeProcessExitAfterCommitBeforeDispatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("child descriptor fixture requires Unix")
	}
	for backend, open := range nativeExitOpeners {
		t.Run(backend, func(t *testing.T) {
			if backend == "postgres" && os.Getenv("STORAGE_TEST_POSTGRES_DSN") == "" {
				t.Skip("set STORAGE_TEST_POSTGRES_DSN for real PostgreSQL")
			}
			dir := t.TempDir()
			c := nativeExitConfig{Backend: backend, SQLitePath: filepath.Join(dir, "registry.sqlite"), Namespace: "registry-exit-" + newID()}
			config, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "config.json")
			if err := os.WriteFile(path, config, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			read, write, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			defer write.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			args := []string{"-test.run=^TestRegistryNativeCommitExitHelper$", "-test.timeout=30s"}
			// Native CI invokes tests through its packaged dynamic loader.
			// /proc/self/exe then identifies ld.so, not the Go test binary.
			if filepath.Base(exe) == "ld.so" {
				args = append([]string{"--inhibit-cache", "--library-path", filepath.Dir(exe), os.Args[0]}, args...)
			}
			cmd := exec.CommandContext(ctx, exe, args...)
			cmd.Env = append(os.Environ(), "REGISTRY_NATIVE_EXIT_CONFIG="+path)
			cmd.ExtraFiles = []*os.File{write}
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			_ = write.Close()
			var receipt nativeExitReceipt
			decoded := make(chan error, 1)
			go func() { decoded <- json.NewDecoder(read).Decode(&receipt) }()
			select {
			case err := <-decoded:
				if err != nil {
					t.Fatalf("child did not commit: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("child did not reach physical commit barrier")
			}
			if receipt.Writes == 0 || receipt.Record.NodeSandboxID == "" {
				t.Fatal("empty commit receipt")
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			waited = true
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.Success() {
				t.Fatalf("expected killed child, got %v", err)
			}
			r, _ := newNativeFaultRegistry(t, func() storagetest.Backend { return open(t, c) })
			r.parkTimeout = 5 * time.Second
			old := receipt.Record
			actual, _, found, err := r.stores.GetSandbox(ctx, old.Group, old.RouteKey)
			if err != nil || !found || actual.SandboxID != old.SandboxID || actual.NodeSandboxID != old.NodeSandboxID || actual.CreateTemplateID != old.CreateTemplateID || actual.GroupVersion != old.GroupVersion {
				t.Fatalf("committed decision changed after kill: found=%t err=%v", found, err)
			}
			owned, err := r.stores.ownership.ReadSandboxOwner(ctx, old.NodeID, old.NodeSandboxID)
			if err != nil || !owned.Exists {
				t.Fatalf("original ownership lost: %v", err)
			}
			owner := &notifyingNodeOwner{recordingNodeOwner: &recordingNodeOwner{}, command: make(chan *routesync.Command, 1)}
			r.SetNodeOwner(owner)
			done := make(chan error, 1)
			go func() { _, err := r.ReserveSandbox(ctx, testCreateReserve(old.Group, old.RouteKey, nil)); done <- err }()
			var command *routesync.Command
			select {
			case command = <-owner.command:
			case err := <-done:
				t.Fatalf("recovery returned without dispatch: %v", err)
			case <-ctx.Done():
				t.Fatal("recovery did not dispatch")
			}
			if command.Kind != routesync.CmdCreate || command.Cluster.StableID != old.SandboxID || command.SID != EncodeNodeSandboxID(old.SandboxID, old.NextSandboxGeneration) {
				t.Fatal("recovery reused the interrupted execution identity or changed logical identity")
			}
			event := testE2BRoute(old.SandboxID, routesync.StateRunning)
			event.SandboxID, event.TemplateID = command.SID, old.CreateTemplateID
			if err := r.applyRoute(testNodeSession(ctx, r.stores, old.NodeID), old.NodeID, &event); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatalf("recovery failed: %v", err)
			}
			profile, err := r.stores.nodes.ReadNodeProfile(ctx, old.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.stores.groups.ApplyGroupSupport(ctx, profile.Registration, 2, nil, old.Group); err != nil {
				t.Fatal(err)
			}
			auth, err := r.stores.groups.ReadGroupAuth(ctx, old.Group, testAPIFingerprint)
			if err != nil || !auth.Exists {
				t.Fatalf("retained object auth lost: %v", err)
			}
			owned, err = r.stores.ownership.ReadSandboxOwner(ctx, old.NodeID, old.NodeSandboxID)
			if err != nil || !owned.Exists {
				t.Fatalf("old unresolved responsibility lost: %v", err)
			}
			t.Logf("F02: SIGKILL after %d native writes committed and before any dispatch; logical identity retained, %s -> %s, old ownership and auth retained", receipt.Writes, old.NodeSandboxID, command.SID)
		})
	}
}
