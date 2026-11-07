package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Node-link handles commands serially. The registry waits five seconds for an
// ACK but does not cancel the node stream, so bound admission locally and leave
// time for insertion and the ACK. This is not a sandbox lifetime or user knob.
const snapshotRuntimeAdmissionBudget = 4 * time.Second

// checkImportedSnapshotRuntime keeps authenticated tenant artifact reads outside
// the shared conductor. It finishes before InsertSandbox and before Connect ACK.
func (o *Orchestrator) checkImportedSnapshotRuntime(ctx context.Context, sb *types.Sandbox) error {
	// Import already supplies this deadline through insertion. Keep direct
	// helper calls bounded too, without extending an earlier caller deadline.
	ctx, cancel := context.WithTimeout(ctx, snapshotRuntimeAdmissionBudget)
	defer cancel()
	runtimePath, err := filepath.Abs(o.runtimeFileFor(sb.Profile))
	if err != nil {
		return fmt.Errorf("import-sandbox: runtime path: %w", err)
	}
	request := configsock.SnapshotRuntimeCheckRequest{
		Source: sb.ResumeSource, Runtime: "file://" + runtimePath,
		ManifestConfig:    o.cfg.ManifestConfig,
		RefLocationParent: o.cfg.Checkpoint.Remote.RefLocationParent,
		RelativeDir:       filepath.Join(sb.BaseDir, "checkpoint"),
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, o.executables.OrchestratorCtl(), "check-snapshot-runtime")
	cmd.WaitDelay = 250 * time.Millisecond
	// Do not mutate process-global MANIFEST_KEY or forward any other tenant key.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "MANIFEST_KEY=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "MANIFEST_KEY="+sb.ManifestKey)
	out := &snapshotCheckOutput{limit: 16 << 10}
	stderr := &snapshotCheckOutput{limit: 4 << 10}
	if err := runSnapshotRuntimeHelper(ctx, cmd, raw, out, stderr); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("import-sandbox: runtime admission helper: %w: %s", err, stderr.String())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if out.truncated {
		return fmt.Errorf("import-sandbox: runtime admission result too large")
	}
	var result configsock.SnapshotRuntimeCheckResult
	if err := strictjson.Decode(out.Bytes(), &result); err != nil {
		return fmt.Errorf("import-sandbox: runtime admission response: %w", err)
	}
	switch result.Status {
	case configsock.SnapshotRuntimeCompatible:
		if result.Error != "" {
			return fmt.Errorf("import-sandbox: invalid successful runtime admission response")
		}
		return ctx.Err()
	case configsock.SnapshotRuntimeIncompatible:
		return fmt.Errorf("import-sandbox: %w: %s", migrationtoken.ErrIncompatible, result.Error)
	case configsock.SnapshotRuntimeSourceMismatch:
		return fmt.Errorf("import-sandbox: %w: %s", migrationtoken.ErrInvalidPayload, result.Error)
	case configsock.SnapshotRuntimeCheckError:
		return fmt.Errorf("import-sandbox: runtime admission: %s", result.Error)
	default:
		return fmt.Errorf("import-sandbox: invalid runtime admission status %q", result.Status)
	}
}

// A misbehaving helper must not grow the shared conductor's output buffer.
// Keep draining after the limit so the child cannot block on its output pipe.
type snapshotCheckOutput struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *snapshotCheckOutput) Write(p []byte) (int, error) {
	n := len(p)
	available := b.limit - b.buffer.Len()
	if n > available {
		p = p[:available]
		b.truncated = true
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

func (b *snapshotCheckOutput) Bytes() []byte  { return b.buffer.Bytes() }
func (b *snapshotCheckOutput) String() string { return b.buffer.String() }

// runSnapshotRuntimeHelper owns the pipes so normal output draining uses the
// admission context, not os/exec's short post-exit WaitDelay. Passing *os.File
// endpoints prevents exec from starting its own copying goroutines. The writers
// must not block: snapshotCheckOutput retains bounded data and drains the rest.
func runSnapshotRuntimeHelper(ctx context.Context, cmd *exec.Cmd, raw []byte, stdout, stderr io.Writer) error {
	var pipes [3][2]*os.File // stdin, stdout, stderr; read end then write end.
	defer func() {
		for _, pair := range pipes {
			for _, file := range pair {
				if file != nil {
					_ = file.Close()
				}
			}
		}
	}()
	for i := range pipes {
		reader, writer, err := os.Pipe()
		if err != nil {
			return err
		}
		pipes[i] = [2]*os.File{reader, writer}
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = pipes[0][0], pipes[1][1], pipes[2][1]
	if err := cmd.Start(); err != nil {
		return err
	}
	// Only the child may retain these ends; parent copies would prevent EOF.
	_ = pipes[0][0].Close()
	_ = pipes[1][1].Close()
	_ = pipes[2][1].Close()

	closed := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() {
		_ = pipes[0][1].Close()
		_ = pipes[1][0].Close()
		_ = pipes[2][0].Close()
		close(closed)
	})
	defer func() {
		if !stopClose() {
			<-closed
		}
	}()

	copies := make(chan error, 3)
	go func() {
		_, err := io.Copy(pipes[0][1], bytes.NewReader(raw))
		_ = pipes[0][1].Close()
		// Match exec's stdin rule when a successful child does not consume input.
		if errors.Is(err, syscall.EPIPE) {
			err = nil
		}
		copies <- err
	}()
	go func() {
		_, err := io.Copy(stdout, pipes[1][0])
		copies <- err
	}()
	go func() {
		_, err := io.Copy(stderr, pipes[2][0])
		copies <- err
	}()

	waitErr := cmd.Wait()
	var copyErr error
	for range 3 {
		copyErr = errors.Join(copyErr, <-copies)
	}
	// This also rejects a complete-looking prefix if cancellation closed pipes
	// still held by descendants after the main helper exited successfully.
	if err := ctx.Err(); err != nil {
		return err
	}
	if waitErr != nil {
		return waitErr
	}
	return copyErr
}
