//go:build linux

// Package activation receives explicitly named systemd socket-activation files.
// It does not select listeners or override Proxy configuration.
package activation

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"golang.org/x/sys/unix"
)

// Receive consumes LISTEN_FDS, LISTEN_FDNAMES and LISTEN_PID from the current
// process environment. Successful callers own the returned files and must close
// them or deliver them through a successful master BindRuntime. Files begin at
// descriptor 3; names are exact, unique, nonempty and ordered as systemd supplies
// them. Empty input returns an empty map. Files must be distinct TCP listeners.
//
// An absent LISTEN_PID is accepted. A present PID must be a positive decimal
// current PID; invalid or mismatching PID input never transfers or closes any
// descriptors. After PID and count validation, Receive owns the indicated range
// and closes it on subsequent failure. Environment cleanup occurs on every path.
// Owned descriptors are made close-on-exec; unrelated descriptors are untouched.
// Call Receive once in the final master, before starting concurrent children.
func Receive() (files map[string]*os.File, returnErr error) {
	countText, countSet := os.LookupEnv("LISTEN_FDS")
	namesText, namesSet := os.LookupEnv("LISTEN_FDNAMES")
	pidText, pidSet := os.LookupEnv("LISTEN_PID")
	defer func() {
		for _, key := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
			_ = os.Unsetenv(key)
		}
	}()
	files = make(map[string]*os.File)
	if !countSet && !namesSet && !pidSet {
		return files, nil
	}
	if pidSet {
		pid, err := decimal(pidText)
		if err != nil || pid == 0 || pid != uint64(os.Getpid()) {
			return nil, fmt.Errorf("activation: LISTEN_PID must equal current PID %d", os.Getpid())
		}
	}
	if !countSet {
		return nil, fmt.Errorf("activation: LISTEN_FDS is missing")
	}
	count, err := decimal(countText)
	if err != nil {
		return nil, fmt.Errorf("activation: invalid LISTEN_FDS: %w", err)
	}
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return nil, err
	}
	if count > uint64(^uint(0)>>1)-3 || count > limit.Cur || (count > 0 && count+3 > limit.Cur) {
		return nil, fmt.Errorf("activation: LISTEN_FDS exceeds descriptor range")
	}
	owned := make([]*os.File, 0, int(count))
	defer func() {
		if returnErr != nil {
			for _, file := range owned {
				_ = file.Close()
			}
			files = nil
		}
	}()
	for index := 0; index < int(count); index++ {
		file := os.NewFile(uintptr(3+index), "activation-"+strconv.Itoa(index))
		owned = append(owned, file)
	}
	for _, file := range owned {
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil {
			return nil, fmt.Errorf("activation: invalid descriptor: %w", err)
		}
		if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
			return nil, err
		}
	}
	var names []string
	if namesText != "" {
		names = strings.Split(namesText, ":")
	}
	if len(names) != int(count) {
		return nil, fmt.Errorf("activation: LISTEN_FDNAMES has %d names for %d files", len(names), count)
	}
	for index, name := range names {
		if err := listenresource.Name(name); err != nil {
			return nil, fmt.Errorf("activation: %w", err)
		}
		if _, duplicate := files[name]; duplicate {
			return nil, fmt.Errorf("activation: duplicate name %q", name)
		}
		files[name] = owned[index]
	}
	if err := listenresource.Validate(files); err != nil {
		return nil, fmt.Errorf("activation: %w", err)
	}
	return files, nil
}

func decimal(value string) (uint64, error) {
	if value == "" || strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, fmt.Errorf("expected unsigned decimal integer")
	}
	return strconv.ParseUint(value, 10, 64)
}
