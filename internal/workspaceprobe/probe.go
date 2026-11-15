package workspaceprobe

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
)

// Drain joins an outstanding autofs lookup for each declared path. Linux's
// terminal AT_NO_AUTOMOUNT lookup waits for a pending token but leaves an idle
// mount trigger alone. Use the projected runtime tool to install our own probe
// in the guest's /run tmpfs: bare images need not supply any tools.
func Drain(ctx context.Context, sandboxCtl, runRoot, sandboxID string, targets []string) error {
	if len(image) == 0 || len(targets) == 0 {
		return errors.New("workspace paths or guest probe unavailable")
	}
	// ctl.sock limits each JSON exec request. Chunk by the encoded argv size,
	// allowing the transport envelope and temporary executable paths to fit.
	const argvBudget = ctl.MaxMessageBytes / 2
	var group []string
	used := 0
	for _, target := range targets {
		data, err := json.Marshal(target)
		if err != nil {
			return err
		}
		if len(group) > 0 && used+len(data)+1 > argvBudget {
			if err := drainGroup(ctx, sandboxCtl, runRoot, sandboxID, group); err != nil {
				return err
			}
			group = group[:0]
			used = 0
		}
		group = append(group, target)
		used += len(data) + 1
	}
	return drainGroup(ctx, sandboxCtl, runRoot, sandboxID, group)
}

func drainGroup(ctx context.Context, sandboxCtl, runRoot, sandboxID string, targets []string) error {
	name := ".kuasar-attach-" + uuid.NewString()
	var buf bytes.Buffer
	archive := tar.NewWriter(&buf)
	if err := archive.WriteHeader(&tar.Header{Name: name + "/", Typeflag: tar.TypeDir, Mode: 0700}); err != nil {
		return err
	}
	if err := archive.WriteHeader(&tar.Header{Name: name + "/probe", Typeflag: tar.TypeReg, Mode: 0500, Size: int64(len(image))}); err != nil {
		return err
	}
	if _, err := archive.Write(image); err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return err
	}
	run := func(execCtx context.Context, input io.Reader, args ...string) error {
		flags := []string{"exec", "--run-root", runRoot, "--path-id", sandboxID, "--tty=false", "--stdout=false", "--stderr=false"}
		if input != nil {
			flags = append(flags, "--stdin")
		}
		flags = append(flags, "--")
		cmd := exec.CommandContext(execCtx, sandboxCtl, append(flags, args...)...)
		cmd.Stdin = input
		return cmd.Run()
	}
	dir := path.Join("/run", name)
	program := path.Join(dir, "probe")
	cleanup := func() {
		// A transport error can occur after extraction or before the command
		// starts. Exercise the same unlink-only entry against this one random
		// path. Destruction of the guest also releases its staging tmpfs.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), proto.DeadlineExec)
		defer cancel()
		_ = run(cleanupCtx, nil, program, dir)
	}
	if err := run(ctx, &buf, "/opt/sandbox-runtime/bin/flatten-ctl", "tar", "extract", ":/run/"); err != nil {
		cleanup()
		return err
	}
	if err := run(ctx, nil, append([]string{program, dir}, targets...)...); err != nil {
		cleanup()
		return err
	}
	return nil
}
