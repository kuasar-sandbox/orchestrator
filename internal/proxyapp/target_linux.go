package proxyapp

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"golang.org/x/sys/unix"
)

type restartTarget struct {
	file     *os.File
	path     string
	identity componentexec.ExecutableIdentity
	digest   string
}

func openRestartTarget(ctx context.Context, path string) (_ *restartTarget, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%w: deployment executable must be absolute", ErrIncompatible)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			file.Close()
		}
	}()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Mode().Perm()&0111 == 0 {
		return nil, fmt.Errorf("%w: target is not an executable regular file", ErrIncompatible)
	}
	if stat.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("%w: target must not be group/world writable", ErrIncompatible)
	}
	header, err := elf.NewFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w: target ELF: %v", ErrIncompatible, err)
	}
	if header.Type != elf.ET_EXEC && header.Type != elf.ET_DYN {
		return nil, fmt.Errorf("%w: target ELF is not a program image", ErrIncompatible)
	}
	want, ok := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[runtime.GOARCH]
	if !ok || header.Machine != want || header.Class != elf.ELFCLASS64 {
		return nil, fmt.Errorf("%w: target architecture does not match %s", ErrIncompatible, runtime.GOARCH)
	}
	var identity unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &identity); err != nil {
		return nil, err
	}
	// Preserve the ordinary component dispatch ownership policy when selecting
	// a new image, including a native node-ctl image at the deployment path.
	if (os.Geteuid() == 0 && identity.Uid != 0) || (os.Geteuid() != 0 && identity.Uid != 0 && identity.Uid != uint32(os.Geteuid())) {
		return nil, fmt.Errorf("%w: target ownership does not match service dispatch policy", ErrIncompatible)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, &contextReader{ctx: ctx, reader: io.NewSectionReader(file, 0, stat.Size())}); err != nil {
		return nil, err
	}
	return &restartTarget{file: file, path: path, identity: componentexec.ExecutableIdentity{Device: uint64(identity.Dev), Inode: identity.Ino}, digest: hex.EncodeToString(hash.Sum(nil))}, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func (t *restartTarget) description() string {
	return fmt.Sprintf("sha256:%s device:%d inode:%d", t.digest, t.identity.Device, t.identity.Inode)
}
