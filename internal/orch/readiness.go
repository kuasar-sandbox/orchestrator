package orch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// runtimeReadinessListener owns the one-shot socket pathname as well as the
// listener. Close is safe from the launch path and context cancellation path.
type runtimeReadinessListener struct {
	listener *net.UnixListener
	path     string
	once     sync.Once
}

func listenRuntimeReadiness(runRoot, sandboxID string) (*runtimeReadinessListener, error) {
	path := configsock.ReadinessSocketPath(runRoot, sandboxID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("orch: remove stale readiness socket %s: %w", path, err)
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("orch: listen readiness socket %s: %w", path, err)
	}
	r := &runtimeReadinessListener{listener: l, path: path}
	if err := os.Chmod(path, 0o600); err != nil {
		r.Close()
		return nil, fmt.Errorf("orch: chmod readiness socket %s: %w", path, err)
	}
	return r, nil
}

func (l *runtimeReadinessListener) Close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		_ = l.listener.Close()
		_ = os.Remove(l.path)
	})
}

// waitRuntimeReadiness accepts one node-ctl bridge connection and consumes the
// complete sandbox-ctl event stream. Closing the listener/connection is what
// interrupts the otherwise blocking Unix reads when the launch context expires.
func waitRuntimeReadiness(ctx context.Context, l *runtimeReadinessListener) error {
	stopAcceptCancel := context.AfterFunc(ctx, l.Close)
	conn, err := l.listener.AcceptUnix()
	stopAcceptCancel()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("accept readiness connection: %w", err)
	}
	// The bridge is one-shot. No later connection may replace or augment the
	// event stream selected for this launch.
	l.Close()
	defer conn.Close()

	stopReadCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	err = configsock.ReadReadiness(conn)
	stopReadCancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("runtime readiness protocol: %w", err)
	}
	return nil
}
