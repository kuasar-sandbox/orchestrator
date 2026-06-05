// Package configsock implements the task config-socket: orchestrator-ctl run-task
// dials it at startup to fetch a generic LaunchSpec (exec/args/workdir/env) for
// its config-id, then exec-replaces into the target. The socket is the sole
// channel for the secret-bearing env (manifest key); bulky non-secret config is a
// plain file referenced by the spec's args. The caller is authenticated by
// SO_PEERCRED peer pid against the config-id's pidfile (/run/sandbox/<id>/<id>.pid).
package configsock

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const maxFrame = 1 << 20 // 1 MiB

// Request is what a task client (orchestrator-ctl run-task) sends.
type Request struct {
	ConfigID string `json:"config_id"`
	Version  int    `json:"version"`
}

// LaunchSpec is the generic launch config run-task applies and then exec-replaces
// into: the absolute target binary, its args (after argv0), the working dir, and
// env added to the inherited environment (secrets — e.g. MANIFEST_KEY — ride here,
// never on disk).
type LaunchSpec struct {
	Exec    string            `json:"exec"`
	Args    []string          `json:"args,omitempty"`
	Workdir string            `json:"workdir,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// Provider resolves a config-id to its LaunchSpec + the pidfile used to
// authenticate the caller (SO_PEERCRED). ok=false means the id is unknown.
type Provider interface {
	LaunchSpecFor(ctx context.Context, configID string) (resp *LaunchSpec, pidFile string, ok bool, err error)
}

type Server struct {
	path string
	prov Provider
	log  *slog.Logger
	ln   *net.UnixListener
}

func New(path string, prov Provider, log *slog.Logger) *Server {
	return &Server{path: path, prov: prov, log: log}
}

// Serve binds the UDS (0600) and accepts until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	_ = os.Remove(s.path)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.path, Net: "unix"})
	if err != nil {
		return fmt.Errorf("configsock: listen %s: %w", s.path, err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		ln.Close()
		return fmt.Errorf("configsock: chmod: %w", err)
	}
	s.ln = ln
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		c, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.log.Warn("configsock accept", "err", err)
			continue
		}
		go s.handle(ctx, c)
	}
}

func (s *Server) handle(ctx context.Context, c *net.UnixConn) {
	defer c.Close()
	pid, err := peerPID(c)
	if err != nil {
		s.log.Warn("configsock peercred", "err", err)
		return
	}
	req, err := readFrame[Request](c)
	if err != nil {
		s.log.Warn("configsock read", "err", err)
		return
	}
	if err := writeFrame(c, s.resolve(ctx, req, pid)); err != nil {
		s.log.Warn("configsock write", "err", err)
	}
}

func (s *Server) resolve(ctx context.Context, req *Request, peer int) *LaunchSpec {
	if req.ConfigID == "" {
		return &LaunchSpec{Error: "bad request"}
	}
	r, pidFile, ok, err := s.prov.LaunchSpecFor(ctx, req.ConfigID)
	if err != nil {
		s.log.Warn("configsock provider", "id", req.ConfigID, "err", err)
		return &LaunchSpec{Error: "internal error"}
	}
	if !ok {
		return &LaunchSpec{Error: "unknown task"}
	}
	if !s.authed(req.ConfigID, pidFile, peer) {
		return &LaunchSpec{Error: "not authorized"}
	}
	return r
}

// authed verifies the connecting pid matches the id's pidfile (SO_PEERCRED).
func (s *Server) authed(id, pidFile string, peer int) bool {
	want, err := readPID(pidFile)
	if err != nil {
		s.log.Warn("configsock pidfile", "id", id, "err", err)
		return false
	}
	if want != peer {
		s.log.Warn("configsock pid mismatch", "id", id, "want", want, "peer", peer)
		return false
	}
	return true
}

// peerPID returns the connecting process's pid via SO_PEERCRED.
func peerPID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var ucred *unix.Ucred
	var serr error
	if cerr := raw.Control(func(fd uintptr) {
		ucred, serr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); cerr != nil {
		return 0, cerr
	}
	if serr != nil {
		return 0, serr
	}
	return int(ucred.Pid), nil
}

func readPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

func readFrame[T any](r io.Reader) (*T, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrame {
		return nil, errors.New("configsock: bad frame length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var v T
	if err := json.Unmarshal(buf, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return errors.New("configsock: response too large")
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
