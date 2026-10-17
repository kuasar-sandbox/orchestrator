package proxyapp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"golang.org/x/sys/unix"
)

// retainedResources contains service-owned files, separate from all App input
// files and instance listeners. Only the sealed private continuation can supply
// these resources; normal environment activation never overrides an address.
type retainedResources struct {
	Shm                                                          *os.File
	ShmPath                                                      string
	StatsIdentity, ShmIdentity                                   pathIdentity
	Data, MMDS, Stats, Metrics                                   *os.File
	DataExpression, MMDSExpression, StatsPath, MetricsExpression string
}

func (r *retainedResources) close() {
	if r != nil {
		closeFiles([]*os.File{r.Data, r.MMDS, r.Stats, r.Metrics, r.Shm})
	}
}

func retainResources(data, mmds, stats, metrics net.Listener, dataExpression, mmdsExpression, statsPath, metricsExpression, shmPath string) (_ *retainedResources, err error) {
	r := &retainedResources{DataExpression: dataExpression, MMDSExpression: mmdsExpression, StatsPath: statsPath, MetricsExpression: metricsExpression, ShmPath: shmPath}
	defer func() {
		if err != nil {
			r.close()
		}
	}()
	r.Shm, err = os.OpenFile(shmPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	r.ShmIdentity, err = identifyPath(shmPath)
	if err != nil {
		return nil, err
	}
	r.StatsIdentity, err = identifyPath(statsPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range []struct {
		listener net.Listener
		file     **os.File
	}{{data, &r.Data}, {mmds, &r.MMDS}, {stats, &r.Stats}, {metrics, &r.Metrics}} {
		*entry.file, err = listenerFile(entry.listener)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

type pathIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func identifyPath(path string) (pathIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return pathIdentity{}, err
	}
	if stat.Mode&unix.S_IFMT == unix.S_IFLNK {
		return pathIdentity{}, fmt.Errorf("proxy resource path must not be a symlink: %s", path)
	}
	return pathIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

func removeOwnedPath(path string, identity pathIdentity) error {
	if path == "" || identity.Inode == 0 {
		return nil
	}
	current, err := identifyPath(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != identity {
		return fmt.Errorf("proxy resource ownership changed: %s", path)
	}
	return os.Remove(path)
}

func (r *retainedResources) removePaths() error {
	if r == nil {
		return nil
	}
	return errors.Join(removeOwnedPath(r.StatsPath, r.StatsIdentity), removeOwnedPath(r.ShmPath, r.ShmIdentity))
}

func (s *listenerSet) inherit(expression, expected string, file *os.File) (net.Listener, error) {
	if expression != expected || file == nil {
		return nil, fmt.Errorf("proxy continuation listener does not match configured source %q", expression)
	}
	name, err := listenresource.Parse(expression)
	if err != nil {
		return nil, err
	}
	if name != "" {
		if s.used[name] {
			return nil, fmt.Errorf("listener %q is selected by multiple endpoints", name)
		}
		if provided := s.files[name]; provided != nil {
			a, err := listenresource.TCP(provided)
			if err != nil {
				return nil, err
			}
			b, err := listenresource.TCP(file)
			if err != nil {
				return nil, err
			}
			if a != b {
				return nil, fmt.Errorf("proxy BindRuntime replaced inherited listener %q", name)
			}
		}
		s.used[name] = true
	}
	return listenresource.Listener(file)
}

func candidateMetricsAddress(base string) (string, error) {
	if base == "" {
		return "", nil
	}
	host, port, err := net.SplitHostPort(base)
	if err != nil {
		return "", err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return "", fmt.Errorf("InplaceRestart metrics requires a numeric TCP port")
	}
	if number == 65535 {
		return "", fmt.Errorf("InplaceRestart cannot reserve metrics port after 65535")
	}
	if number != 0 {
		number++
	}
	return net.JoinHostPort(host, strconv.Itoa(number)), nil
}

// Candidate resources stay beside the explicitly configured originals. Names
// are private, bounded and collision checked; no existing path is removed.
func candidatePath(base, bootID string, socket bool) (string, error) {
	if !filepath.IsAbs(base) || len(bootID) != 32 || strings.IndexFunc(bootID, func(r rune) bool { return !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') }) >= 0 {
		return "", fmt.Errorf("proxy candidate: invalid base path or boot identity")
	}
	suffix := ".shm"
	if socket {
		suffix = ".sock"
	}
	path := filepath.Join(filepath.Dir(base), ".proxy-"+bootID+suffix)
	if len(filepath.Base(path)) > 255 || (socket && len(path) >= 108) {
		return "", fmt.Errorf("proxy candidate path is too long: %s", path)
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	if !parent.IsDir() {
		return "", fmt.Errorf("proxy candidate parent is not a directory")
	}
	if err := unix.Faccessat(unix.AT_FDCWD, filepath.Dir(path), unix.W_OK|unix.X_OK, unix.AT_EACCESS); err != nil {
		return "", fmt.Errorf("proxy candidate parent permissions: %w", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		if err == nil {
			err = fmt.Errorf("proxy candidate path already exists: %s", path)
		}
		return "", err
	}
	return path, nil
}
