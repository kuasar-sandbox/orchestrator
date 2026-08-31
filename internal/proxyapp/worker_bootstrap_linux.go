//go:build linux

package proxyapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"golang.org/x/sys/unix"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const (
	workerBootstrapEnvironment = "KUASAR_INTERNAL_PROXY_WORKER_BOOTSTRAP_FD"
	workerBootstrapMagic       = "kuasar-proxy-worker-bootstrap"
	workerBootstrapVersion     = 1
	workerConfigSchemaVersion  = 1
	workerFDProtocolVersion    = 1
	maxWorkerBootstrapBytes    = maxEffectiveConfigBytes + 64<<10
)

const (
	RoleMaster = proxyextension.RoleMaster
	RoleWorker = proxyextension.RoleWorker
)

// Process aliases the public leaf value used for process-local runtime binding.
type Process = proxyextension.Process

type workerFDMapping struct {
	Data    int `json:"data"`
	Forward int `json:"forward"`
	MMDS    int `json:"mmds"`
	Wake    int `json:"wake"`
	Notify  int `json:"notify"`
	Stats   int `json:"stats"`
	MMDSRPC int `json:"mmdsRPC"`
}

type workerEnvelope struct {
	Magic               string                           `json:"magic"`
	ProtocolVersion     int                              `json:"protocolVersion"`
	Role                proxyextension.Role              `json:"role"`
	WorkerID            string                           `json:"workerID"`
	WorkerEpoch         uint64                           `json:"workerEpoch"`
	ConfigSchemaVersion int                              `json:"configSchemaVersion"`
	FDProtocolVersion   int                              `json:"fdProtocolVersion"`
	ExecutableIdentity  componentexec.ExecutableIdentity `json:"executableIdentity"`
	Config              json.RawMessage                  `json:"config"`
	ConfigDigest        string                           `json:"configDigest"`
	FDs                 workerFDMapping                  `json:"fds"`
}

// WorkerBootstrap is a verified, one-use master-to-worker handoff.
type WorkerBootstrap struct {
	process            Process
	effective          *EffectiveConfig
	executableIdentity componentexec.ExecutableIdentity
	fds                workerFDMapping
	fdsTaken           atomic.Bool
}

// Close closes inherited descriptors when worker preparation did not take
// ownership. It is safe to call after PrepareWorker and more than once.
func (b *WorkerBootstrap) Close() error {
	if b == nil || !b.fdsTaken.CompareAndSwap(false, true) {
		return nil
	}
	closeDescriptors(workerDescriptors(b.fds))
	return nil
}

func (b *WorkerBootstrap) takeFDs() (workerFDMapping, error) {
	if b == nil || !b.fdsTaken.CompareAndSwap(false, true) {
		return workerFDMapping{}, fmt.Errorf("proxy worker bootstrap descriptors were already consumed")
	}
	return b.fds, nil
}

// Process returns the verified worker identity.
func (b *WorkerBootstrap) Process() Process {
	if b == nil {
		return Process{}
	}
	return b.process
}

// EffectiveConfig returns the immutable declarative snapshot.
func (b *WorkerBootstrap) EffectiveConfig() *EffectiveConfig {
	if b == nil {
		return nil
	}
	return b.effective
}

// VerifyExecutable proves the worker re-executed the proxy master's current
// file identity rather than a configured or mutable pathname.
func (b *WorkerBootstrap) VerifyExecutable() error {
	if b == nil {
		return fmt.Errorf("proxy worker bootstrap is required")
	}
	return componentexec.VerifyCurrentExecutable(b.executableIdentity)
}

// WorkerBootstrapPresent reports whether this process was designated as an
// internal proxy worker. Any present value is consumed as worker state and
// fails closed if malformed; it never falls back to CLI dispatch.
func WorkerBootstrapPresent() bool {
	_, ok := os.LookupEnv(workerBootstrapEnvironment)
	return ok
}

// ClearWorkerEnvironment removes and closes stale worker bootstrap state before
// entering an unrelated command or spawning a helper.
func ClearWorkerEnvironment() {
	rawFD, ok := os.LookupEnv(workerBootstrapEnvironment)
	_ = os.Unsetenv(workerBootstrapEnvironment)
	if !ok {
		return
	}
	fd, err := strconv.Atoi(rawFD)
	if err == nil && fd >= 3 {
		_ = unix.Close(fd)
	}
}

func newWorkerBootstrapFile(workerID string, epoch uint64, effective *EffectiveConfig, fds workerFDMapping) (*os.File, error) {
	if effective == nil || effective.config == nil || len(effective.raw) == 0 {
		return nil, fmt.Errorf("proxy worker bootstrap: effective config is required")
	}
	if err := validateWorkerIdentity(workerID, epoch); err != nil {
		return nil, err
	}
	if err := validateWorkerFDMapping(fds, -1); err != nil {
		return nil, err
	}
	executableIdentity, err := componentexec.CurrentExecutableIdentity()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(workerEnvelope{
		Magic: workerBootstrapMagic, ProtocolVersion: workerBootstrapVersion,
		Role: RoleWorker, WorkerID: workerID, WorkerEpoch: epoch,
		ConfigSchemaVersion: workerConfigSchemaVersion, FDProtocolVersion: workerFDProtocolVersion,
		ExecutableIdentity: executableIdentity,
		Config:             append(json.RawMessage(nil), effective.raw...), ConfigDigest: hex.EncodeToString(effective.digest[:]),
		FDs: fds,
	})
	if err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: encode: %w", err)
	}
	if len(raw) == 0 || len(raw) > maxWorkerBootstrapBytes {
		return nil, fmt.Errorf("proxy worker bootstrap: exceeds %d bytes", maxWorkerBootstrapBytes)
	}
	return newSealedWorkerFile(raw)
}

// ReceiveWorkerBootstrap consumes and verifies the inherited sealed bootstrap.
// The environment variable is cleared and the bootstrap descriptor is closed
// on every return path.
func ReceiveWorkerBootstrap() (*WorkerBootstrap, error) {
	rawFD, ok := os.LookupEnv(workerBootstrapEnvironment)
	_ = os.Unsetenv(workerBootstrapEnvironment)
	if !ok || rawFD == "" {
		return nil, fmt.Errorf("proxy worker bootstrap is missing")
	}
	fd, err := strconv.Atoi(rawFD)
	if err != nil || fd < 3 || strconv.Itoa(fd) != rawFD {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid descriptor")
	}
	file := os.NewFile(uintptr(fd), "kuasar-proxy-worker-bootstrap")
	if file == nil {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid descriptor")
	}
	defer file.Close()
	wantSeals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: descriptor is not a sealed memfd: %w", err)
	}
	if seals&wantSeals != wantSeals {
		return nil, fmt.Errorf("proxy worker bootstrap: descriptor is not fully sealed")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxWorkerBootstrapBytes+1))
	if err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: read: %w", err)
	}
	if len(raw) == 0 || len(raw) > maxWorkerBootstrapBytes {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid size")
	}
	if err := strictjson.RejectDuplicateKeys(raw); err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: %w", err)
	}
	var encoded workerEnvelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("proxy worker bootstrap: trailing JSON value")
		}
		return nil, fmt.Errorf("proxy worker bootstrap: trailing data: %w", err)
	}
	if encoded.Magic != workerBootstrapMagic || encoded.ProtocolVersion != workerBootstrapVersion {
		return nil, fmt.Errorf("proxy worker bootstrap: unsupported protocol")
	}
	if encoded.Role != RoleWorker {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid role")
	}
	if encoded.ConfigSchemaVersion != workerConfigSchemaVersion || encoded.FDProtocolVersion != workerFDProtocolVersion {
		return nil, fmt.Errorf("proxy worker bootstrap: unsupported schema")
	}
	if err := validateWorkerIdentity(encoded.WorkerID, encoded.WorkerEpoch); err != nil {
		return nil, err
	}
	if encoded.ExecutableIdentity.Inode == 0 {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid executable identity")
	}
	if err := validateWorkerFDMapping(encoded.FDs, fd); err != nil {
		return nil, err
	}
	digestBytes, err := hex.DecodeString(encoded.ConfigDigest)
	if err != nil || len(digestBytes) != sha256.Size {
		return nil, fmt.Errorf("proxy worker bootstrap: invalid config digest")
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	if encoded.ConfigDigest != hex.EncodeToString(digest[:]) {
		return nil, fmt.Errorf("proxy worker bootstrap: non-canonical config digest")
	}
	effective, err := effectiveConfigFromRaw(encoded.Config, digest)
	if err != nil {
		return nil, err
	}
	return &WorkerBootstrap{
		process:   Process{Role: RoleWorker, WorkerID: encoded.WorkerID, WorkerEpoch: encoded.WorkerEpoch},
		effective: effective, executableIdentity: encoded.ExecutableIdentity, fds: encoded.FDs,
	}, nil
}

func validateWorkerIdentity(workerID string, epoch uint64) error {
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\x00") {
		return fmt.Errorf("proxy worker bootstrap: invalid worker id")
	}
	if epoch == 0 {
		return fmt.Errorf("proxy worker bootstrap: invalid worker epoch")
	}
	return nil
}

func validateWorkerFDMapping(fds workerFDMapping, bootstrapFD int) error {
	seen := make(map[int]string, 7)
	for _, field := range []struct {
		name     string
		fd       int
		optional bool
	}{
		{name: "data", fd: fds.Data, optional: true},
		{name: "forward", fd: fds.Forward},
		{name: "mmds", fd: fds.MMDS, optional: true},
		{name: "wake", fd: fds.Wake},
		{name: "notify", fd: fds.Notify},
		{name: "stats", fd: fds.Stats},
		{name: "mmdsRPC", fd: fds.MMDSRPC},
	} {
		if field.optional && field.fd == -1 {
			continue
		}
		if field.fd < 3 || field.fd > 1<<20 || field.fd == bootstrapFD {
			return fmt.Errorf("proxy worker bootstrap: invalid %s descriptor", field.name)
		}
		if previous, duplicate := seen[field.fd]; duplicate {
			return fmt.Errorf("proxy worker bootstrap: %s descriptor duplicates %s", field.name, previous)
		}
		seen[field.fd] = field.name
	}
	return nil
}

func newSealedWorkerFile(payload []byte) (*os.File, error) {
	fd, err := unix.MemfdCreate("kuasar-proxy-worker-bootstrap", unix.MFD_ALLOW_SEALING|unix.MFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("proxy worker bootstrap: memfd: %w", err)
	}
	file := os.NewFile(uintptr(fd), "kuasar-proxy-worker-bootstrap")
	if err := writeWorkerPayload(file, payload); err != nil {
		file.Close()
		return nil, fmt.Errorf("proxy worker bootstrap: write: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("proxy worker bootstrap: rewind: %w", err)
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, seals); err != nil {
		file.Close()
		return nil, fmt.Errorf("proxy worker bootstrap: seal: %w", err)
	}
	return file, nil
}

func writeWorkerPayload(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		count, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if count <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[count:]
	}
	return nil
}

func withoutWorkerEnvironment(environment []string) []string {
	internalNames := map[string]struct{}{
		workerBootstrapEnvironment: {},
		"KUASAR_PROXY_DATA_FD":     {}, "KUASAR_PROXY_FORWARD_FD": {}, "KUASAR_PROXY_MMDS_FD": {},
		"KUASAR_PROXY_WAKE_FD": {}, "KUASAR_PROXY_NOTIFY_FD": {}, "KUASAR_PROXY_STATS_FD": {},
		"KUASAR_PROXY_MMDSRPC_FD": {}, "KUASAR_PROXY_WORKER_ID": {}, "KUASAR_PROXY_WORKER_EPOCH": {},
	}
	out := make([]string, 0, len(environment))
	for _, value := range environment {
		name := value
		if index := strings.IndexByte(value, '='); index >= 0 {
			name = value[:index]
		}
		if _, internal := internalNames[name]; internal {
			continue
		}
		out = append(out, value)
	}
	return out
}
