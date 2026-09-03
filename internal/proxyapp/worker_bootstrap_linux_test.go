package proxyapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
)

func TestWorkerBootstrapSealedRoundTrip(t *testing.T) {
	effective, err := FreezeConfig(testProxyConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	fds := validWorkerFDMapping()
	file, err := newWorkerBootstrapFile(0, "proxy-3", 7, effective, fds)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	wantSeals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil {
		t.Fatal(err)
	}
	if seals&wantSeals != wantSeals {
		t.Fatalf("seals=%#x", seals)
	}
	fd := installWorkerBootstrapFile(t, file)
	bootstrap, err := ReceiveWorkerBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.process != (Process{Role: RoleWorker, WorkerID: "proxy-3", WorkerEpoch: 7}) {
		t.Fatalf("process=%+v", bootstrap.process)
	}
	if bootstrap.fds != fds || bootstrap.workerIndex != 0 || bootstrap.effective.Config().Paths.RunRoot != "/run/test-proxy" {
		t.Fatalf("bootstrap=%+v config=%+v", bootstrap.fds, bootstrap.effective.Config())
	}
	if err := bootstrap.VerifyExecutable(); err != nil {
		t.Fatal(err)
	}
	if _, found := os.LookupEnv(workerBootstrapEnvironment); found {
		t.Fatal("worker bootstrap environment was not cleared")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("bootstrap descriptor remains open: %v", err)
	}
}

func TestReceiveWorkerBootstrapRejectsMalformedInput(t *testing.T) {
	base := validWorkerEnvelope(t)
	tests := map[string]func() []byte{
		"bad magic":          func() []byte { value := base; value.Magic = "bad"; return marshalWorkerEnvelope(t, value) },
		"bad version":        func() []byte { value := base; value.ProtocolVersion++; return marshalWorkerEnvelope(t, value) },
		"bad role":           func() []byte { value := base; value.Role = RoleMaster; return marshalWorkerEnvelope(t, value) },
		"bad config schema":  func() []byte { value := base; value.ConfigSchemaVersion++; return marshalWorkerEnvelope(t, value) },
		"bad fd schema":      func() []byte { value := base; value.FDProtocolVersion++; return marshalWorkerEnvelope(t, value) },
		"bad worker":         func() []byte { value := base; value.WorkerID = ""; return marshalWorkerEnvelope(t, value) },
		"bad epoch":          func() []byte { value := base; value.WorkerEpoch = 0; return marshalWorkerEnvelope(t, value) },
		"bad worker index":   func() []byte { value := base; value.WorkerIndex = -1; return marshalWorkerEnvelope(t, value) },
		"large worker index": func() []byte { value := base; value.WorkerIndex = 2; return marshalWorkerEnvelope(t, value) },
		"bad identity": func() []byte {
			value := base
			value.ExecutableIdentity = componentexec.ExecutableIdentity{}
			return marshalWorkerEnvelope(t, value)
		},
		"bad digest": func() []byte {
			value := base
			value.ConfigDigest = strings.Repeat("0", sha256.Size*2)
			return marshalWorkerEnvelope(t, value)
		},
		"noncanonical digest": func() []byte {
			value := base
			value.ConfigDigest = strings.ToUpper(value.ConfigDigest)
			return marshalWorkerEnvelope(t, value)
		},
		"duplicate fd": func() []byte {
			value := base
			value.FDs.Stats = value.FDs.Wake
			return marshalWorkerEnvelope(t, value)
		},
		"missing required fd": func() []byte {
			value := base
			value.FDs.Data = -1
			return marshalWorkerEnvelope(t, value)
		},
		"out of range fd": func() []byte {
			value := base
			value.FDs.Data = 1<<20 + 1
			return marshalWorkerEnvelope(t, value)
		},
		"noncanonical config": func() []byte {
			value := base
			var indented bytes.Buffer
			if err := json.Indent(&indented, value.Config, "", "  "); err != nil {
				t.Fatal(err)
			}
			value.Config = indented.Bytes()
			digest := sha256.Sum256(value.Config)
			value.ConfigDigest = hex.EncodeToString(digest[:])
			return marshalWorkerEnvelope(t, value)
		},
		"unknown field": func() []byte {
			raw := marshalWorkerEnvelope(t, base)
			return append(raw[:len(raw)-1], []byte(`,"unknown":true}`)...)
		},
		"duplicate field": func() []byte {
			raw := marshalWorkerEnvelope(t, base)
			return append(raw[:len(raw)-1], []byte(`,"magic":"again"}`)...)
		},
		"truncated": func() []byte {
			raw := marshalWorkerEnvelope(t, base)
			return raw[:len(raw)/2]
		},
		"empty":     func() []byte { return nil },
		"oversized": func() []byte { return make([]byte, maxWorkerBootstrapBytes+1) },
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			fd := installWorkerBootstrapPayload(t, payload())
			if _, err := ReceiveWorkerBootstrap(); err == nil {
				t.Fatal("ReceiveWorkerBootstrap succeeded")
			}
			if _, found := os.LookupEnv(workerBootstrapEnvironment); found {
				t.Fatal("environment was not cleared")
			}
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
				t.Fatalf("rejected descriptor remains open: %v", err)
			}
		})
	}
}

func TestReceiveWorkerBootstrapRejectsMissingInvalidAndUnsealedFD(t *testing.T) {
	_ = os.Unsetenv(workerBootstrapEnvironment)
	if _, err := ReceiveWorkerBootstrap(); err == nil {
		t.Fatal("missing bootstrap accepted")
	}
	for _, value := range []string{"bad", "-1", "2", "+3", "03", " 3"} {
		_ = os.Setenv(workerBootstrapEnvironment, value)
		if _, err := ReceiveWorkerBootstrap(); err == nil {
			t.Fatalf("descriptor %q accepted", value)
		}
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	fd, err := unix.Dup(int(read.Fd()))
	read.Close()
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Setenv(workerBootstrapEnvironment, strconv.Itoa(fd))
	if _, err := ReceiveWorkerBootstrap(); err == nil || !strings.Contains(err.Error(), "sealed memfd") {
		t.Fatalf("unsealed error=%v", err)
	}
}

func TestClearAndSanitizeWorkerEnvironment(t *testing.T) {
	file, err := newSealedWorkerFile(marshalWorkerEnvelope(t, validWorkerEnvelope(t)))
	if err != nil {
		t.Fatal(err)
	}
	fd := installWorkerBootstrapFile(t, file)
	file.Close()
	ClearWorkerEnvironment()
	if WorkerBootstrapPresent() {
		t.Fatal("worker environment remains present")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("stale descriptor remains open: %v", err)
	}
	environment := withoutWorkerEnvironment([]string{
		"PATH=/bin", workerBootstrapEnvironment + "=4", "KUASAR_PROXY_DATA_FD=5", "KEEP=value",
	})
	if strings.Join(environment, ",") != "PATH=/bin,KEEP=value" {
		t.Fatalf("sanitized environment=%v", environment)
	}
}

func TestWorkerBootstrapCloseReleasesUnconsumedDescriptors(t *testing.T) {
	file, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	descriptors := make([]int, 7)
	for index := range descriptors {
		descriptors[index], err = unix.Dup(int(file.Fd()))
		if err != nil {
			t.Fatal(err)
		}
	}
	bootstrap := &WorkerBootstrap{fds: workerFDMapping{
		Data: descriptors[0], MMDS: descriptors[1], Wake: descriptors[2],
		Notify: descriptors[3], Stats: descriptors[4], MMDSRPC: descriptors[5], Admission: descriptors[6],
	}}
	if err := bootstrap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range descriptors {
		if _, err := unix.FcntlInt(uintptr(descriptor), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatalf("descriptor %d remains open: %v", descriptor, err)
		}
	}
}

func validWorkerEnvelope(t *testing.T) workerEnvelope {
	t.Helper()
	effective, err := FreezeConfig(testProxyConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := componentexec.CurrentExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return workerEnvelope{
		Magic: workerBootstrapMagic, ProtocolVersion: workerBootstrapVersion, Role: RoleWorker,
		WorkerID: "proxy-0", WorkerEpoch: 1, WorkerIndex: 0,
		ConfigSchemaVersion: workerConfigSchemaVersion, FDProtocolVersion: workerFDProtocolVersion,
		ExecutableIdentity: identity,
		Config:             append(json.RawMessage(nil), effective.raw...), ConfigDigest: hex.EncodeToString(effective.digest[:]),
		FDs: validWorkerFDMapping(),
	}
}

func validWorkerFDMapping() workerFDMapping {
	return workerFDMapping{Data: 100, MMDS: -1, Wake: 101, Notify: 102, Stats: 103, MMDSRPC: 104, Admission: 105}
}

func marshalWorkerEnvelope(t *testing.T, value workerEnvelope) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func installWorkerBootstrapPayload(t *testing.T, payload []byte) int {
	t.Helper()
	file, err := newSealedWorkerFile(payload)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	return installWorkerBootstrapFile(t, file)
}

func installWorkerBootstrapFile(t *testing.T, file *os.File) int {
	t.Helper()
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Dup(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Setenv(workerBootstrapEnvironment, strconv.Itoa(fd))
	t.Cleanup(func() {
		_ = os.Unsetenv(workerBootstrapEnvironment)
		_ = unix.Close(fd)
	})
	return fd
}
