//go:build linux

package activation

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReceiveProcess(t *testing.T) {
	if scenario := os.Getenv("ACTIVATION_TEST_CHILD"); scenario != "" {
		activationChild(t, scenario)
		return
	}
	for _, scenario := range []string{
		"absent", "zero", "zero-names", "pid-only", "count-missing", "count-empty", "count-invalid",
		"pid-missing", "pid-current", "pid-empty", "pid-invalid", "pid-mismatch",
		"ordered", "numeric", "duplicate", "missing-name", "empty-name", "blank-name", "extra-name",
		"alias", "regular", "connected", "unix", "closed",
	} {
		t.Run(scenario, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			first, err := listener.File()
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			other, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			second, err := other.File()
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			switch scenario {
			case "alias":
				second = first
			case "regular":
				first, err = os.CreateTemp(t.TempDir(), "regular")
				if err != nil {
					t.Fatal(err)
				}
				defer first.Close()
			case "connected":
				conn, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				first, err = conn.File()
				if err != nil {
					t.Fatal(err)
				}
				defer first.Close()
			case "unix":
				pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				first = os.NewFile(uintptr(pair[0]), "unix")
				defer first.Close()
				defer unix.Close(pair[1])
			}
			unrelated, err := os.CreateTemp(t.TempDir(), "unrelated")
			if err != nil {
				t.Fatal(err)
			}
			defer unrelated.Close()
			command := exec.Command(os.Args[0], "-test.run=^TestReceiveProcess$")
			for _, value := range os.Environ() {
				key, _, _ := strings.Cut(value, "=")
				if key != "LISTEN_PID" && key != "LISTEN_FDS" && key != "LISTEN_FDNAMES" && key != "ACTIVATION_TEST_CHILD" {
					command.Env = append(command.Env, value)
				}
			}
			command.Env = append(command.Env, "ACTIVATION_TEST_CHILD="+scenario)
			command.ExtraFiles = []*os.File{first, second, unrelated}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			// Receive in the child must never shutdown the shared socket itself.
			if scenario != "connected" {
				conn, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					t.Fatalf("external listener was damaged: %v", err)
				}
				conn.Close()
			}
		})
	}
}

func activationChild(t *testing.T, scenario string) {
	set := func(key, value string) {
		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}
	set("LISTEN_FDS", "2")
	set("LISTEN_FDNAMES", "mmds:data")
	wantError, owns := false, true
	switch scenario {
	case "absent":
		os.Unsetenv("LISTEN_FDS")
		os.Unsetenv("LISTEN_FDNAMES")
		owns = false
	case "zero":
		set("LISTEN_FDS", "0")
		os.Unsetenv("LISTEN_FDNAMES")
		owns = false
	case "zero-names":
		set("LISTEN_FDS", "0")
		set("LISTEN_FDNAMES", "data")
		owns = false
		wantError = true
	case "pid-only":
		os.Unsetenv("LISTEN_FDS")
		os.Unsetenv("LISTEN_FDNAMES")
		set("LISTEN_PID", strconv.Itoa(os.Getpid()))
		owns = false
		wantError = true
	case "count-missing":
		os.Unsetenv("LISTEN_FDS")
		owns = false
		wantError = true
	case "count-empty":
		set("LISTEN_FDS", "")
		owns = false
		wantError = true
	case "count-invalid":
		set("LISTEN_FDS", "-1")
		owns = false
		wantError = true
	case "pid-current":
		set("LISTEN_PID", strconv.Itoa(os.Getpid()))
	case "pid-empty":
		set("LISTEN_PID", "")
		owns = false
		wantError = true
	case "pid-invalid":
		set("LISTEN_PID", "abc")
		owns = false
		wantError = true
	case "pid-mismatch":
		set("LISTEN_PID", strconv.Itoa(os.Getpid()+1))
		owns = false
		wantError = true
	case "numeric":
		set("LISTEN_FDNAMES", "4:3")
	case "duplicate":
		set("LISTEN_FDNAMES", "data:data")
		wantError = true
	case "missing-name":
		set("LISTEN_FDNAMES", "data")
		wantError = true
	case "empty-name":
		set("LISTEN_FDNAMES", ":data")
		wantError = true
	case "blank-name":
		set("LISTEN_FDNAMES", "bad name:data")
		wantError = true
	case "extra-name":
		set("LISTEN_FDNAMES", "one:two:three")
		wantError = true
	case "alias", "regular", "connected", "unix":
		wantError = true
	case "closed":
		unix.Close(3)
		wantError = true
	}
	files, err := Receive()
	if (err != nil) != wantError {
		t.Fatalf("Receive error=%v, expected failure=%v", err, wantError)
	}
	for _, key := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		if _, exists := os.LookupEnv(key); exists {
			t.Fatalf("%s not consumed", key)
		}
	}
	for fd := 3; fd <= 5; fd++ {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if fd == 5 || !owns {
			if err != nil {
				t.Fatalf("unowned descriptor %d was closed: %v", fd, err)
			}
		} else if wantError {
			if err == nil {
				t.Fatalf("owned descriptor %d leaked", fd)
			}
		} else if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("owned descriptor %d not protected: %v", fd, err)
		}
	}
	if owns && !wantError {
		if len(files) != 2 {
			t.Fatalf("got %d files", len(files))
		}
		firstName := "mmds"
		if scenario == "numeric" {
			firstName = "4"
		}
		if files[firstName] == nil || files[firstName].Fd() != 3 {
			t.Fatal("names were guessed or reordered")
		}
		for _, file := range files {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
	} else if !wantError && len(files) != 0 {
		t.Fatalf("unexpected files: %v", files)
	}
}

func TestDecimalRejectsAmbiguousNumbers(t *testing.T) {
	for _, value := range []string{"", "-1", "+1", " 1", "1 ", "1.0", "18446744073709551616"} {
		if _, err := decimal(value); err == nil {
			t.Fatal(fmt.Sprintf("accepted %q", value))
		}
	}
}
