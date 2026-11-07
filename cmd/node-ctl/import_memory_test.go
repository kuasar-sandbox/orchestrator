package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
)

func TestImportCLIOptionalMemory(t *testing.T) {
	t.Setenv("E2B_API_KEY", "test-owner")
	for _, tc := range []struct {
		name, flag     string
		present, value bool
	}{
		{name: "absent"}, {name: "false", flag: "--memory=false", present: true}, {name: "true", flag: "--memory=true", present: true, value: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "ctl.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				raw, ok := body["memory"]
				if ok != tc.present || (ok && string(raw) != map[bool]string{true: "true", false: "false"}[tc.value]) {
					t.Errorf("memory=%s present=%t", raw, ok)
				}
				io.WriteString(w, `{"sandboxID":"target"}`)
			})}
			go server.Serve(listener)
			defer server.Close()
			args := []string{"kmt1.test", "--socket", socket}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			if err := importSandboxCmd(args, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotRuntimeHelperClassifiesOnlyDefinitiveMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status string
	}{
		{"compatible", nil, configsock.SnapshotRuntimeCompatible},
		{"incompatible", restore.ErrRuntimeIncompatible, configsock.SnapshotRuntimeIncompatible},
		{"source pair", taskartifact.ErrSnapshotPairMismatch, configsock.SnapshotRuntimeSourceMismatch},
		{"metadata failure", errors.New("metadata unavailable"), configsock.SnapshotRuntimeCheckError},
		{"canceled", context.Canceled, configsock.SnapshotRuntimeCheckError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out strings.Builder
			err := checkSnapshotRuntimeWith(context.Background(), strings.NewReader(`{"source":{},"runtime":"file:///runtime"}`), &out, func(context.Context, configsock.SnapshotRuntimeCheckRequest) error { return tc.err })
			if err != nil {
				t.Fatal(err)
			}
			var result configsock.SnapshotRuntimeCheckResult
			if err := json.Unmarshal([]byte(out.String()), &result); err != nil || result.Status != tc.status {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}
