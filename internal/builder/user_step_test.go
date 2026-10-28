package builder

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// fakeStepEnvd answers the builder's envd process.Start surface over a UDS:
// it records each call's Basic-auth user, command, and PATH, scripting the
// `id -u` probes (account existence) and `useradd` outcomes the USER step
// performs.
type fakeStepEnvd struct {
	addr       string
	existing   map[string]bool
	failCmd    string // command prefix that exits non-zero
	breakProbe bool   // abort the connection mid-probe (inconclusive outcome)
	idMissExit int    // exit code the probe uses for a missing account
	mu         sync.Mutex
	calls      []fakeStepEnvdCall
}

type fakeStepEnvdCall struct {
	user      string
	cmd       string
	path      string
	noprofile bool
}

func (f *fakeStepEnvd) recorded() []fakeStepEnvdCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeStepEnvdCall(nil), f.calls...)
}

var (
	fakeProbe   = regexp.MustCompile(`^getent passwd '([^']+)'$`)
	fakeUseradd = regexp.MustCompile(`^useradd --create-home --shell /bin/bash(?: --gid '([^']+)')? -- '([^']+)'$`)
)

const (
	accountProbe = "getent passwd 'app'"
	accountAdd   = "useradd --create-home --shell /bin/bash -- 'app'"
)

func startFakeStepEnvd(t *testing.T, existing []string, failCmd string, breakProbe bool, idMissExit int) *fakeStepEnvd {
	t.Helper()
	if idMissExit == 0 {
		idMissExit = 2
	}
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "envd.sock"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeStepEnvd{
		addr: ln.Addr().String(), existing: map[string]bool{},
		failCmd: failCmd, breakProbe: breakProbe, idMissExit: idMissExit,
	}
	for _, u := range existing {
		f.existing[u] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /process.Process/Start", func(w http.ResponseWriter, r *http.Request) {
		var hdr [5]byte
		if _, err := io.ReadFull(r.Body, hdr[:]); err != nil {
			http.Error(w, "short frame", http.StatusBadRequest)
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[1:5]))
		if _, err := io.ReadFull(r.Body, body); err != nil {
			http.Error(w, "short frame", http.StatusBadRequest)
			return
		}
		var req struct {
			Process struct {
				Args []string          `json:"args"`
				Envs map[string]string `json:"envs"`
			} `json:"process"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		user, _, _ := r.BasicAuth()
		cmd, noprofile := "", false
		if len(req.Process.Args) > 0 {
			cmd = req.Process.Args[len(req.Process.Args)-1]
			noprofile = req.Process.Args[0] == "--noprofile"
		}
		f.mu.Lock()
		f.calls = append(f.calls, fakeStepEnvdCall{user: user, cmd: cmd, path: req.Process.Envs["PATH"], noprofile: noprofile})
		f.mu.Unlock()

		if f.breakProbe && fakeProbe.MatchString(cmd) {
			// Deliver a start event, then drop the connection: the client
			// sees a stream error, not a definite exit.
			writeStepEnvdFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
			panic(http.ErrAbortHandler)
		}
		exit := 0
		if m := fakeProbe.FindStringSubmatch(cmd); m != nil {
			if !f.existing[m[1]] {
				exit = f.idMissExit
			}
		} else if m := fakeUseradd.FindStringSubmatch(cmd); m != nil {
			if failCmd != "" && strings.HasPrefix(cmd, failCmd) {
				exit = 1
			} else {
				f.mu.Lock()
				f.existing[m[2]] = true
				f.mu.Unlock()
			}
		}
		writeStepEnvdFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
		if exit != 0 {
			writeStepEnvdFrame(w, 0, []byte(fmt.Sprintf(`{"event":{"end":{"exitCode":%d,"exited":true,"status":"exited"}}}`, exit)))
		} else {
			writeStepEnvdFrame(w, 0, []byte(`{"event":{"end":{"exited":true,"status":"exited"}}}`))
		}
		writeStepEnvdFrame(w, 0x02, []byte(`{}`))
	})
	go http.Serve(ln, mux)
	t.Cleanup(func() { ln.Close() })
	return f
}

func writeStepEnvdFrame(w io.Writer, flags byte, payload []byte) {
	hdr := [5]byte{flags}
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	_, _ = w.Write(hdr[:])
	_, _ = w.Write(payload)
}

func TestUserStepResolvesAndCreatesAccount(t *testing.T) {
	// The accumulated build context carries an image-supplied PATH; account
	// maintenance must not inherit it.
	poison := map[string]string{"PATH": "/opt/shadow", "FROM_IMAGE": "yes"}
	for _, test := range []struct {
		name        string
		user        string
		existing    []string
		failCmd     string
		breakProbe  bool
		idMissExit  int
		wantErr     string
		wantCalls   []string
		wantContext string
	}{
		{
			name: "existing account is only probed", user: "app", existing: []string{"app"},
			wantCalls: []string{accountProbe}, wantContext: "app",
		},
		{
			name:      "missing account is created",
			user:      "app",
			wantCalls: []string{accountProbe, accountAdd}, wantContext: "app",
		},
		{
			name: "creation failure fails the step", user: "app", failCmd: "useradd",
			wantErr:   "create missing user",
			wantCalls: []string{accountProbe, accountAdd},
		},
		{
			// A probe that ends without a definite exit (transport/stream
			// failure) must not be read as "account missing": creation only
			// follows a conclusive not-found.
			name: "inconclusive probe fails without creating", user: "app", breakProbe: true,
			wantErr:   "inconclusive",
			wantCalls: []string{accountProbe},
		},
		{
			// getent exits 2 exactly for a key miss; an NSS operational
			// error (exit 1) leaves existence unknown and must not trigger
			// creation.
			name: "nss error exit fails without creating", user: "app", idMissExit: 1,
			wantErr:   "account probe failed",
			wantCalls: []string{accountProbe},
		},
		{
			name: "missing tool exit fails without creating", user: "app", idMissExit: 127,
			wantErr:   "account probe failed",
			wantCalls: []string{accountProbe},
		},
		{
			// user:group keeps the full identity in the context while the
			// account part is what gets guaranteed in the sandbox.
			name: "user group form probes the account part", user: "app:staff", existing: []string{"app"},
			wantCalls: []string{accountProbe}, wantContext: "app:staff",
		},
		{
			// The group suffix stays config-only: the account part is what
			// gets guaranteed, with e2b's exact creation command.
			name:        "group suffix form creates the account part only",
			user:        "app:staff",
			wantCalls:   []string{accountProbe, accountAdd},
			wantContext: "app:staff",
		},
		{name: "uid:gid stays a pure identity switch", user: "1000:1000", wantContext: "1000:1000"},
		{name: "uid with group stays a pure identity switch", user: "1000:staff", wantContext: "1000:staff"},
		{name: "bare uid stays a pure identity switch", user: "0", wantContext: "0"},
		{
			// Shell metacharacters and option-looking operands never reach
			// a shell: non-portable names keep the pre-change switch-only
			// behavior.
			name: "shell metacharacters stay off the probe", user: "missing || true", wantContext: "missing || true",
		},
		{name: "option-looking operand stays off the probe", user: "--help", wantContext: "--help"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := startFakeStepEnvd(t, test.existing, test.failCmd, test.breakProbe, test.idMissExit)
			p := &buildPipeline{
				ctx:  context.Background(),
				spec: &configsock.BuildSpec{Timeouts: configsock.BuildTimeouts{StepSec: 5}},
			}
			c := &stepCtx{env: poison}
			err := p.applyStep(nil, &envdExec{uds: f.addr}, c, 0,
				configsock.BuildStep{Type: "USER", Args: []string{test.user}})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("USER step error = %v, want containing %q", err, test.wantErr)
				}
			} else if err != nil {
				t.Fatalf("USER step: %v", err)
			} else if c.user != test.wantContext {
				t.Fatalf("step context user = %q, want %q", c.user, test.wantContext)
			}
			var cmds []string
			for _, call := range f.recorded() {
				if call.user != "root" {
					t.Fatalf("USER step probe ran as %q, want root", call.user)
				}
				if strings.Contains(call.path, "/opt/shadow") {
					t.Fatalf("USER step probe PATH %q inherited the image-supplied PATH", call.path)
				}
				if !call.noprofile {
					t.Fatalf("USER step probe ran in a login shell (%q); account maintenance must skip profiles", call.cmd)
				}
				if !strings.Contains(call.path, "/usr/sbin") {
					t.Fatalf("USER step probe PATH %q lacks the trusted directories", call.path)
				}
				cmds = append(cmds, call.cmd)
			}
			if !reflect.DeepEqual(cmds, test.wantCalls) {
				t.Fatalf("guest commands = %#v, want %#v", cmds, test.wantCalls)
			}
		})
	}
}
