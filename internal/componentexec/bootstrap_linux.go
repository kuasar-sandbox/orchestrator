//go:build linux

// Package componentexec implements the private node-ctl to component process
// handoff. The public App packages consume this protocol; custom applications
// do not parse descriptors, inherited file descriptors, or environment state.
package componentexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const (
	bootstrapEnvironment = "KUASAR_INTERNAL_COMPONENT_BOOTSTRAP_FD"
	bootstrapMagic       = "kuasar-component-bootstrap"
	bootstrapVersion     = 1
	configSchemaVersion  = 1
	maxBootstrapBytes    = 4<<20 + 64<<10
)

// Component identifies the statically customized component being entered.
type Component string

const (
	ComponentConductor Component = "conductor"
	ComponentProxy     Component = "proxy"
)

// Role is the process role encoded by a top-level component bootstrap.
type Role string

const (
	RoleConductor Role = "conductor"
	RoleMaster    Role = "master"
)

// Bootstrap is the verified, one-use component handoff. Config is the exact
// JSON whose SHA-256 digest was verified while receiving the memfd.
type Bootstrap struct {
	Component           Component
	Role                Role
	NodeCtlExecutable   string
	ComponentExecutable string
	Config              json.RawMessage
}

type envelope struct {
	Magic               string          `json:"magic"`
	ProtocolVersion     int             `json:"protocolVersion"`
	Component           Component       `json:"component"`
	Role                Role            `json:"role"`
	ConfigSchemaVersion int             `json:"configSchemaVersion"`
	NodeCtlExecutable   string          `json:"nodeCtlExecutable"`
	ComponentExecutable string          `json:"componentExecutable"`
	Config              json.RawMessage `json:"config"`
	ConfigDigest        string          `json:"configDigest"`
}

// ErrNoBootstrap reports that a custom App was executed directly rather than
// entered through node-ctl. It is intentionally distinct from malformed
// bootstrap errors so public Apps can return their component-specific message.
var ErrNoBootstrap = errors.New("component bootstrap is missing")

// Exec replaces the current process with component and passes config through a
// sealed memfd. The environment contains only the inherited descriptor number,
// never configuration or secret material. A failed exec is returned to the
// caller and must not fall back to an in-process implementation.
func Exec(component Component, role Role, nodeCtlExecutable, componentExecutable string, config any) error {
	if !validComponentRole(component, role) {
		return fmt.Errorf("component bootstrap: invalid component or role")
	}
	if !filepath.IsAbs(nodeCtlExecutable) || !filepath.IsAbs(componentExecutable) {
		return fmt.Errorf("component bootstrap: executable paths must be absolute")
	}
	rawConfig, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("component bootstrap: encode config: %w", err)
	}
	if len(rawConfig) == 0 || len(rawConfig) > maxBootstrapBytes {
		return fmt.Errorf("component bootstrap: config exceeds size limit")
	}
	if rawConfig[0] != '{' {
		return fmt.Errorf("component bootstrap: config must be a JSON object")
	}
	digest := sha256.Sum256(rawConfig)
	rawEnvelope, err := json.Marshal(envelope{
		Magic:               bootstrapMagic,
		ProtocolVersion:     bootstrapVersion,
		Component:           component,
		Role:                role,
		ConfigSchemaVersion: configSchemaVersion,
		NodeCtlExecutable:   nodeCtlExecutable,
		ComponentExecutable: componentExecutable,
		Config:              rawConfig,
		ConfigDigest:        hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return fmt.Errorf("component bootstrap: encode envelope: %w", err)
	}
	if len(rawEnvelope) > maxBootstrapBytes {
		return fmt.Errorf("component bootstrap: envelope exceeds %d bytes", maxBootstrapBytes)
	}

	file, err := newSealedFile(rawEnvelope)
	if err != nil {
		return err
	}
	defer file.Close()
	fd := int(file.Fd())
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("component bootstrap: descriptor flags: %w", err)
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, flags&^unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("component bootstrap: inherit descriptor: %w", err)
	}

	env := withoutBootstrapEnvironment(os.Environ())
	env = append(env, bootstrapEnvironment+"="+strconv.Itoa(fd))
	argv := []string{componentExecutable}
	if err := unix.Exec(componentExecutable, argv, env); err != nil {
		return fmt.Errorf("exec custom %s %s: %w", component, componentExecutable, err)
	}
	return nil
}

func newSealedFile(payload []byte) (*os.File, error) {
	fd, err := unix.MemfdCreate("kuasar-component-bootstrap", unix.MFD_ALLOW_SEALING|unix.MFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("component bootstrap: memfd: %w", err)
	}
	file := os.NewFile(uintptr(fd), "kuasar-component-bootstrap")
	if err := writeAll(file, payload); err != nil {
		file.Close()
		return nil, fmt.Errorf("component bootstrap: write: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("component bootstrap: rewind: %w", err)
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, seals); err != nil {
		file.Close()
		return nil, fmt.Errorf("component bootstrap: seal: %w", err)
	}
	return file, nil
}

// Receive consumes and verifies a top-level component bootstrap. The internal
// environment variable is cleared and the descriptor is closed before return,
// including every malformed-input path.
func Receive(wantComponent Component, wantRole Role) (*Bootstrap, error) {
	rawFD, ok := os.LookupEnv(bootstrapEnvironment)
	_ = os.Unsetenv(bootstrapEnvironment)
	if !ok || rawFD == "" {
		return nil, ErrNoBootstrap
	}
	fd, err := strconv.Atoi(rawFD)
	if err != nil || fd < 3 || strconv.Itoa(fd) != rawFD {
		return nil, fmt.Errorf("component bootstrap: invalid descriptor")
	}
	file := os.NewFile(uintptr(fd), "kuasar-component-bootstrap")
	if file == nil {
		return nil, fmt.Errorf("component bootstrap: invalid descriptor")
	}
	defer file.Close()
	wantSeals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil {
		return nil, fmt.Errorf("component bootstrap: descriptor is not a sealed memfd: %w", err)
	}
	if seals&wantSeals != wantSeals {
		return nil, fmt.Errorf("component bootstrap: descriptor is not fully sealed")
	}

	raw, err := io.ReadAll(io.LimitReader(file, maxBootstrapBytes+1))
	if err != nil {
		return nil, fmt.Errorf("component bootstrap: read: %w", err)
	}
	if len(raw) > maxBootstrapBytes {
		return nil, fmt.Errorf("component bootstrap: exceeds %d bytes", maxBootstrapBytes)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("component bootstrap: empty")
	}
	if err := strictjson.RejectDuplicateKeys(raw); err != nil {
		return nil, fmt.Errorf("component bootstrap: %w", err)
	}
	var encoded envelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&encoded); err != nil {
		return nil, fmt.Errorf("component bootstrap: decode: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("component bootstrap: trailing JSON value")
		}
		return nil, fmt.Errorf("component bootstrap: trailing data: %w", err)
	}
	if encoded.Magic != bootstrapMagic {
		return nil, fmt.Errorf("component bootstrap: invalid magic")
	}
	if encoded.ProtocolVersion != bootstrapVersion {
		return nil, fmt.Errorf("component bootstrap: unsupported protocol version %d", encoded.ProtocolVersion)
	}
	if encoded.ConfigSchemaVersion != configSchemaVersion {
		return nil, fmt.Errorf("component bootstrap: unsupported config schema version %d", encoded.ConfigSchemaVersion)
	}
	if encoded.Component != wantComponent || encoded.Role != wantRole {
		return nil, fmt.Errorf("component bootstrap: unexpected component or role")
	}
	if !validComponentRole(encoded.Component, encoded.Role) {
		return nil, fmt.Errorf("component bootstrap: invalid component or role")
	}
	if !filepath.IsAbs(encoded.NodeCtlExecutable) || !filepath.IsAbs(encoded.ComponentExecutable) {
		return nil, fmt.Errorf("component bootstrap: executable paths must be absolute")
	}
	wantDigest, err := hex.DecodeString(encoded.ConfigDigest)
	if err != nil || len(wantDigest) != sha256.Size {
		return nil, fmt.Errorf("component bootstrap: invalid config digest")
	}
	gotDigest := sha256.Sum256(encoded.Config)
	if !bytes.Equal(wantDigest, gotDigest[:]) || encoded.ConfigDigest != hex.EncodeToString(gotDigest[:]) {
		return nil, fmt.Errorf("component bootstrap: config digest mismatch")
	}
	if len(encoded.Config) == 0 || encoded.Config[0] != '{' {
		return nil, fmt.Errorf("component bootstrap: config must be a JSON object")
	}
	configCopy := append(json.RawMessage(nil), encoded.Config...)
	return &Bootstrap{
		Component:           encoded.Component,
		Role:                encoded.Role,
		NodeCtlExecutable:   encoded.NodeCtlExecutable,
		ComponentExecutable: encoded.ComponentExecutable,
		Config:              configCopy,
	}, nil
}

// VerifyCurrentExecutable proves that the receiving process is the exact file
// selected and validated by node-ctl. This is misuse protection and process
// organization, not authentication against another process running as the same
// operating-system user.
func VerifyCurrentExecutable(expected string) error {
	current, err := os.Executable()
	if err != nil {
		return fmt.Errorf("component bootstrap: resolve current executable: %w", err)
	}
	currentInfo, err := os.Stat(current)
	if err != nil {
		return fmt.Errorf("component bootstrap: stat current executable: %w", err)
	}
	expectedInfo, err := os.Stat(expected)
	if err != nil {
		return fmt.Errorf("component bootstrap: stat selected executable: %w", err)
	}
	if !os.SameFile(currentInfo, expectedInfo) {
		return fmt.Errorf("component bootstrap: current executable does not match selected component")
	}
	return nil
}

// ClearEnvironment removes a stale top-level bootstrap descriptor from a
// process that is not entering a custom App, preventing propagation to helpers.
func ClearEnvironment() {
	rawFD, ok := os.LookupEnv(bootstrapEnvironment)
	_ = os.Unsetenv(bootstrapEnvironment)
	if !ok {
		return
	}
	fd, err := strconv.Atoi(rawFD)
	if err != nil || fd < 3 {
		return
	}
	_ = unix.Close(fd)
}

func validComponentRole(component Component, role Role) bool {
	return (component == ComponentConductor && role == RoleConductor) ||
		(component == ComponentProxy && role == RoleMaster)
}

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func withoutBootstrapEnvironment(env []string) []string {
	prefix := bootstrapEnvironment + "="
	out := make([]string, 0, len(env))
	for _, value := range env {
		if len(value) >= len(prefix) && value[:len(prefix)] == prefix {
			continue
		}
		out = append(out, value)
	}
	return out
}
