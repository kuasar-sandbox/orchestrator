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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const (
	helperEnvironment          = "KUASAR_COMPONENTEXEC_TEST_HELPER"
	helperComponentEnvironment = "KUASAR_COMPONENTEXEC_TEST_COMPONENT"
)

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnvironment) {
	case "dispatch":
		_ = os.Setenv(helperEnvironment, "receive")
		executable, err := os.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(91)
		}
		component := os.Getenv(helperComponentEnvironment)
		if err := Exec(ComponentConductor, RoleConductor, executable, component, map[string]string{"value": "top-secret-bootstrap-value"}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(92)
		}
		os.Exit(93)
	case "receive":
		bootstrap, err := Receive(ComponentConductor, RoleConductor)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(94)
		}
		argvOrEnvironmentContainsConfig := strings.Contains(
			strings.Join(os.Environ(), "\x00")+"\x00"+strings.Join(os.Args, "\x00"),
			"top-secret-bootstrap-value",
		)
		fmt.Printf("%d\n%s\n%t\n", os.Getpid(), bootstrap.Config, argvOrEnvironmentContainsConfig)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestExecReplacesProcessAndKeepsConfigOutOfEnvironment(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	component := filepath.Join(t.TempDir(), "xconductor")
	copyComponentExecutable(t, executable, component)
	command := exec.Command(executable, "-test.run=^$")
	command.Env = append(os.Environ(), helperEnvironment+"=dispatch", helperComponentEnvironment+"="+component)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wantPID := command.Process.Pid
	if err := command.Wait(); err != nil {
		t.Fatalf("helper: %v\n%s", err, output.String())
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("helper output=%q", output.String())
	}
	gotPID, err := strconv.Atoi(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	if gotPID != wantPID {
		t.Fatalf("PID after exec=%d, want original PID %d", gotPID, wantPID)
	}
	if lines[1] != `{"value":"top-secret-bootstrap-value"}` {
		t.Fatalf("config=%q", lines[1])
	}
	if lines[2] != "false" {
		t.Fatalf("configuration leaked into environment: %q", output.String())
	}
}

func TestSealedMemfdAndReceive(t *testing.T) {
	rawConfig := json.RawMessage(`{"path":"/run/test"}`)
	raw := validEnvelope(t, rawConfig)
	file, err := newSealedFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantSeals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if seals&wantSeals != wantSeals {
		t.Fatalf("seals=%#x want %#x", seals, wantSeals)
	}
	if _, err := file.WriteAt([]byte("x"), 0); err == nil {
		t.Fatal("sealed memfd accepted a write")
	}

	fd := installBootstrap(t, raw)
	bootstrap, err := Receive(ComponentConductor, RoleConductor)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.NodeCtlExecutable != "/usr/bin/node-ctl" || bootstrap.ComponentExecutable != "/opt/bin/xconductor" || !bytes.Equal(bootstrap.Config, rawConfig) {
		t.Fatalf("bootstrap=%+v", bootstrap)
	}
	if _, ok := os.LookupEnv(bootstrapEnvironment); ok {
		t.Fatal("bootstrap environment was not cleared")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("consumed descriptor remains open: %v", err)
	}
}

func TestReceiveRejectsMalformedBootstrap(t *testing.T) {
	baseConfig := json.RawMessage(`{"ok":true}`)
	base := envelopeForConfig(baseConfig)
	tests := map[string]func() []byte{
		"bad magic":     func() []byte { value := base; value.Magic = "bad"; return marshalEnvelope(t, value) },
		"bad version":   func() []byte { value := base; value.ProtocolVersion++; return marshalEnvelope(t, value) },
		"bad schema":    func() []byte { value := base; value.ConfigSchemaVersion++; return marshalEnvelope(t, value) },
		"bad component": func() []byte { value := base; value.Component = ComponentProxy; return marshalEnvelope(t, value) },
		"bad role":      func() []byte { value := base; value.Role = RoleMaster; return marshalEnvelope(t, value) },
		"digest mismatch": func() []byte {
			value := base
			value.ConfigDigest = strings.Repeat("0", sha256.Size*2)
			return marshalEnvelope(t, value)
		},
		"noncanonical digest": func() []byte {
			value := base
			value.ConfigDigest = strings.ToUpper(value.ConfigDigest)
			return marshalEnvelope(t, value)
		},
		"non-object config": func() []byte {
			value := envelopeForConfig(json.RawMessage(`[]`))
			return marshalEnvelope(t, value)
		},
		"unknown field": func() []byte {
			raw := marshalEnvelope(t, base)
			return append(raw[:len(raw)-1], []byte(`,"unknown":1}`)...)
		},
		"duplicate field": func() []byte {
			raw := marshalEnvelope(t, base)
			return append(raw[:len(raw)-1], []byte(`,"magic":"again"}`)...)
		},
		"truncated": func() []byte { raw := marshalEnvelope(t, base); return raw[:len(raw)/2] },
		"empty":     func() []byte { return nil },
		"oversized": func() []byte { return make([]byte, maxBootstrapBytes+1) },
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			fd := installBootstrap(t, payload())
			if _, err := Receive(ComponentConductor, RoleConductor); err == nil {
				t.Fatal("Receive succeeded")
			}
			if _, ok := os.LookupEnv(bootstrapEnvironment); ok {
				t.Fatal("bootstrap environment was not cleared on failure")
			}
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
				t.Fatalf("rejected descriptor remains open: %v", err)
			}
		})
	}
}

func TestReceiveRejectsMissingInvalidAndUnsealedDescriptors(t *testing.T) {
	_ = os.Unsetenv(bootstrapEnvironment)
	if _, err := Receive(ComponentConductor, RoleConductor); !errors.Is(err, ErrNoBootstrap) {
		t.Fatalf("missing error=%v", err)
	}
	for _, value := range []string{"bad", "-1", "2", "+3", "03", " 3"} {
		_ = os.Setenv(bootstrapEnvironment, value)
		if _, err := Receive(ComponentConductor, RoleConductor); err == nil {
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
	_ = os.Setenv(bootstrapEnvironment, strconv.Itoa(fd))
	if _, err := Receive(ComponentConductor, RoleConductor); err == nil || !strings.Contains(err.Error(), "sealed memfd") {
		t.Fatalf("unsealed descriptor error=%v", err)
	}
}

func TestExecFailureDoesNotFallbackOrPersistEnvironment(t *testing.T) {
	_ = os.Unsetenv(bootstrapEnvironment)
	executable, executableErr := os.Executable()
	if executableErr != nil {
		t.Fatal(executableErr)
	}
	err := Exec(ComponentConductor, RoleConductor, executable, "/does/not/exist/xconductor", struct{}{})
	if err == nil || !strings.Contains(err.Error(), "exec custom conductor") {
		t.Fatalf("Exec error=%v", err)
	}
	if _, ok := os.LookupEnv(bootstrapEnvironment); ok {
		t.Fatal("failed exec mutated current process environment")
	}
}

func copyComponentExecutable(t *testing.T, source, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExecRejectsInvalidEnvelopeInputsBeforeCreatingHandoff(t *testing.T) {
	if err := Exec(ComponentProxy, RoleConductor, "/usr/bin/node-ctl", "/opt/xproxy", struct{}{}); err == nil {
		t.Fatal("mismatched component role accepted")
	}
	if err := Exec(ComponentConductor, RoleConductor, "relative-node-ctl", "/opt/xconductor", struct{}{}); err == nil {
		t.Fatal("relative node-ctl path accepted")
	}
	if err := Exec(ComponentConductor, RoleConductor, "/usr/bin/node-ctl", "/opt/xconductor", []string{"not", "object"}); err == nil {
		t.Fatal("non-object config accepted")
	}
}

func TestVerifyCurrentExecutableRejectsDifferentFile(t *testing.T) {
	different := filepath.Join(t.TempDir(), "different-component")
	if err := os.WriteFile(different, []byte("different"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCurrentExecutable(different); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("VerifyCurrentExecutable error=%v", err)
	}
}

func TestClearEnvironment(t *testing.T) {
	_ = os.Setenv(bootstrapEnvironment, "99")
	ClearEnvironment()
	if _, ok := os.LookupEnv(bootstrapEnvironment); ok {
		t.Fatal("environment remains set")
	}
	fd := installBootstrap(t, validEnvelope(t, json.RawMessage(`{}`)))
	ClearEnvironment()
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("stale sealed descriptor remains open: %v", err)
	}
}

func validEnvelope(t *testing.T, config json.RawMessage) []byte {
	t.Helper()
	return marshalEnvelope(t, envelopeForConfig(config))
}

func envelopeForConfig(config json.RawMessage) envelope {
	digest := sha256.Sum256(config)
	return envelope{
		Magic: bootstrapMagic, ProtocolVersion: bootstrapVersion,
		Component: ComponentConductor, Role: RoleConductor,
		ConfigSchemaVersion: configSchemaVersion,
		NodeCtlExecutable:   "/usr/bin/node-ctl", ComponentExecutable: "/opt/bin/xconductor",
		Config: config, ConfigDigest: hex.EncodeToString(digest[:]),
	}
}

func marshalEnvelope(t *testing.T, value envelope) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func installBootstrap(t *testing.T, payload []byte) int {
	t.Helper()
	file, err := newSealedFile(payload)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fd, err := unix.Dup(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Setenv(bootstrapEnvironment, strconv.Itoa(fd))
	t.Cleanup(func() {
		_ = os.Unsetenv(bootstrapEnvironment)
		_ = unix.Close(fd)
	})
	return fd
}
