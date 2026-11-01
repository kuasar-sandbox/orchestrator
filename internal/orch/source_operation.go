package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sys/unix"
)

var errSourceOperationBusy = errors.New("orch: checkpoint source operation is still active")

const sourceOperationLockName = ".checkpoint-operation.lock"

type sourceOperationKey struct{}

// A grant belongs to this admission and directory incarnation. It is never
// cached by SID: after contention the caller reloads the row and opens the
// current directory again. The inode lives until terminal BaseDir cleanup.
type sourceOperationGrant struct {
	sid, baseDir, instanceID string
	createdUnix              int64
	file                     *os.File
	closed                   atomic.Bool
}

func sourceGrant(ctx context.Context) *sourceOperationGrant {
	g, _ := ctx.Value(sourceOperationKey{}).(*sourceOperationGrant)
	if g != nil && !g.closed.Load() {
		return g
	}
	return nil
}

func withSourceGrant(ctx context.Context, g *sourceOperationGrant) (context.Context, func()) {
	var once sync.Once
	return context.WithValue(ctx, sourceOperationKey{}, g), func() {
		once.Do(func() {
			g.closed.Store(true)
			// Never LOCK_UN: an exec'd child shares the open file description
			// and must retain the flock even if conductor exits or is killed.
			if g.file != nil {
				_ = g.file.Close()
			}
		})
	}
}

// trySourceOperation must run under the SID lifecycle lock. It never waits
// while holding that lock; asynchronous finalizers return busy to their retry
// machinery and request admissions wait outside the lifecycle lock.
func (o *Orchestrator) trySourceOperation(ctx context.Context, sb *types.Sandbox) (context.Context, func(), error) {
	noop := func() {}
	if sb == nil {
		return ctx, noop, nil
	}
	if !types.ValidLocalSandboxID(sb.ID) {
		return ctx, nil, errors.New("orch: source operation requires a valid sandbox identity")
	}
	want := sb.BaseDir
	if want != "" && want != nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, sb.ID) {
		return ctx, nil, fmt.Errorf("orch: noncanonical source BaseDir %q", sb.BaseDir)
	}
	if g := sourceGrant(ctx); g != nil && g.sid == sb.ID && g.baseDir == want && g.createdUnix == sb.CreatedUnix && g.instanceID == sb.InstanceID {
		return ctx, noop, nil
	}
	if o.exports.ActiveDone(sb.ID) != nil {
		return ctx, nil, errSourceOperationBusy
	}
	// Even a portable source with no local directory needs a scoped logical
	// grant for its own export finalizer. Other admissions still see ActiveDone.
	if want == "" {
		next, release := withSourceGrant(ctx, &sourceOperationGrant{sid: sb.ID, instanceID: sb.InstanceID, createdUnix: sb.CreatedUnix})
		return next, release, nil
	}
	dirFD, err := unix.Open(want, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		// Already removed or not prepared yet. Do not recreate a directory
		// during cleanup; there is no source inode to protect here.
		next, release := withSourceGrant(ctx, &sourceOperationGrant{sid: sb.ID, instanceID: sb.InstanceID, baseDir: want, createdUnix: sb.CreatedUnix})
		return next, release, nil
	}
	if err != nil {
		return ctx, nil, fmt.Errorf("orch: open source directory: %w", err)
	}
	defer unix.Close(dirFD)
	fd, err := unix.Openat(dirFD, sourceOperationLockName, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return ctx, nil, fmt.Errorf("orch: open source operation lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), want+"/"+sourceOperationLockName)
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err == nil && (st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1) {
		err = errors.New("source operation lock must be a private regular file")
	}
	if err == nil {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	}
	if err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ctx, nil, errSourceOperationBusy
		}
		return ctx, nil, fmt.Errorf("orch: lock source operation: %w", err)
	}
	guardCtx, release := withSourceGrant(ctx, &sourceOperationGrant{sid: sb.ID, instanceID: sb.InstanceID, baseDir: want, createdUnix: sb.CreatedUnix, file: file})
	return guardCtx, release, nil
}

// retainSourceOperation transfers a separate reference to a long-running
// export before admission releases the lifecycle lock and its own reference.
func retainSourceOperation(ctx context.Context) (context.Context, func(), error) {
	g := sourceGrant(ctx)
	if g == nil {
		return ctx, func() {}, nil
	}
	if g.file == nil {
		next, release := withSourceGrant(ctx, &sourceOperationGrant{sid: g.sid, instanceID: g.instanceID, baseDir: g.baseDir, createdUnix: g.createdUnix})
		return next, release, nil
	}
	fd, err := unix.FcntlInt(g.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return ctx, nil, err
	}
	next, release := withSourceGrant(ctx, &sourceOperationGrant{sid: g.sid, instanceID: g.instanceID, baseDir: g.baseDir, createdUnix: g.createdUnix, file: os.NewFile(uintptr(fd), g.file.Name())})
	return next, release, nil
}

func inheritSourceOperation(ctx context.Context, cmd *exec.Cmd) {
	if g := sourceGrant(ctx); g != nil && g.file != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, g.file)
	}
}
