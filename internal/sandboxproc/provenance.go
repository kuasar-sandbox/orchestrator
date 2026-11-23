package sandboxproc

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"golang.org/x/sys/unix"
)

// AttachConfigProvenance carries a task-local, digest-bound projection through
// the private CLI handoff. A sealed regular memfd has no pipe writer or EOF
// dependency, no persistent pathname, and cannot change after validation.
// The caller closes its returned copy immediately after Start (also on error).
func AttachConfigProvenance(cmd *exec.Cmd, envelope []byte) (*os.File, error) {
	env := cmd.Env[:0]
	for _, item := range cmd.Env {
		if !strings.HasPrefix(item, rtconfig.ConfigProvenanceFDEnv+"=") {
			env = append(env, item)
		}
	}
	cmd.Env = env
	if len(envelope) == 0 {
		return nil, nil
	}
	if len(envelope) > rtconfig.MaxConfigProvenanceBytes {
		return nil, fmt.Errorf("config provenance exceeds 64 KiB")
	}
	fd, err := unix.MemfdCreate("sandbox-config-provenance", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("create config provenance descriptor: %w", err)
	}
	file := os.NewFile(uintptr(fd), "sandbox-config-provenance")
	fail := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Write(envelope); err != nil {
		return fail(fmt.Errorf("write config provenance: %w", err))
	}
	if _, err := file.Seek(0, 0); err != nil {
		return fail(fmt.Errorf("rewind config provenance: %w", err))
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, seals); err != nil {
		return fail(fmt.Errorf("seal config provenance: %w", err))
	}
	childFD := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, file)
	cmd.Env = append(cmd.Env, rtconfig.ConfigProvenanceFDEnv+"="+strconv.Itoa(childFD))
	return file, nil
}
