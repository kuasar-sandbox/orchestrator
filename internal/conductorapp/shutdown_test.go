package conductorapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
)

type shutdownLauncher struct {
	launcher.Launcher // Unexpected operations fail the startup-only fixture.
	ctx               context.Context
	workCtx           context.Context
	closed            bool
	closeErr          error
}

func (l *shutdownLauncher) Stop(ctx context.Context, _ string) error {
	if l.closed || l.ctx.Err() != nil {
		return errors.New("connection closed before cleanup")
	}
	return ctx.Err()
}

func (l *shutdownLauncher) Close() error {
	if l.workCtx.Err() == nil {
		l.closeErr = errors.New("launcher closed before work cancellation")
	} else {
		l.closeErr = l.Stop(context.Background(), "sandbox-runner@test.service")
	}
	l.closed = true
	return l.closeErr
}

func TestRunKeepsLauncherUntilShutdown(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_parent=%t", cancelParent), func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cfg := &publicconfig.Conductor{}
			cfg.Paths.DBPath = filepath.Join(t.TempDir(), "conductor.db")
			lc := &shutdownLauncher{}
			startupErr := errors.New("stop before serving")
			runtime := &Runtime{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), SecretBox: box,
				Extension: startupExtensionFunc(func(ctx context.Context, _ conductorextension.Host) error {
					lc.workCtx = ctx
					if cancelParent {
						// SIGTERM cancels Run's parent, before its shutdown defer runs.
						cancel()
						if err := lc.Stop(context.Background(), "sandbox-runner@test.service"); err != nil {
							t.Errorf("cleanup after parent cancellation: %v", err)
						}
					}
					return startupErr
				}),
			}
			err = run(parent, cfg, executable, runtime, func(ctx context.Context) (launcher.Launcher, error) {
				lc.ctx = ctx
				return lc, nil
			})
			if !errors.Is(err, startupErr) {
				t.Fatalf("Run error = %v, want %v", err, startupErr)
			}
			if !lc.closed || lc.closeErr != nil {
				t.Fatalf("launcher closed=%t, shutdown error=%v", lc.closed, lc.closeErr)
			}
		})
	}
}
