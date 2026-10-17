package configsock

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type proxyCapabilitySink struct {
	*nativeStatsSink
	mu           sync.Mutex
	capabilities []string
}

func (s *proxyCapabilitySink) SetProxyCapabilities(capabilities []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capabilities = append([]string(nil), capabilities...)
}

func TestProxyPIDAdmissionOtherProcess(t *testing.T) {
	socket := os.Getenv("KUASAR_PROXY_PID_ADMISSION_TEST_SOCKET")
	if socket == "" {
		return
	}
	var body bytes.Buffer
	caps := proxyCaps()
	caps.Mmds = true
	if err := routesync.WriteMsg(&body, &routesync.Msg{Type: routesync.TypeRegister, Register: &caps}); err != nil {
		t.Fatal(err)
	}
	client := HTTPClient(socket)
	client.Timeout = 3 * time.Second
	request, err := http.NewRequest(http.MethodPut, "http://conductor/internal/plugin/proxy-candidate/register", &body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted child PID admitted: %d", response.StatusCode)
	}
}

func TestProxyCapabilitiesPreservePIDAdmission(t *testing.T) {
	for _, mode := range []string{"socket", "pidfile"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			dir, err := os.MkdirTemp("", "proxy-admission-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			stats := filepath.Join(dir, "stats.sock")
			registry := NewRegistry()
			pidfile := ""
			if mode != "socket" {
				pidfile = filepath.Join(dir, "plugins.pid")
				mustWrite(t, pidfile, fmt.Sprint(os.Getpid()))
			}
			socket, _ := startTestServer(t, Deps{Plugins: registry, RouteSource: nativeStatsSource{}, PluginPidfile: pidfile})
			streamCtx, cancel := context.WithCancel(ctx)
			sink := &proxyCapabilitySink{nativeStatsSink: &nativeStatsSink{ready: make(chan struct{})}}
			caps := proxyCaps()
			caps.Mmds = true
			caps.Proxy.StatsSocket = &routesync.Socket{Path: stats}
			subscriber := routesync.NewSubscriber(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", socket) }, "proxy", caps, sink, nil, discardLogger())
			done := make(chan struct{})
			go func() { subscriber.Run(streamCtx); close(done) }()
			defer func() { cancel(); <-done }()
			select {
			case <-sink.ready:
			case <-time.After(3 * time.Second):
				t.Fatal("allowlisted master did not apply initial Bookmark")
			}
			sink.mu.Lock()
			capabilities := append([]string(nil), sink.capabilities...)
			sink.mu.Unlock()
			if !slices.Contains(capabilities, routesync.InplaceRestartV1) {
				t.Fatalf("incorrect admission capabilities: %v", capabilities)
			}
			if pidfile != "" {
				command := exec.Command(os.Args[0], "-test.run=^TestProxyPIDAdmissionOtherProcess$")
				command.Env = append(os.Environ(), "KUASAR_PROXY_PID_ADMISSION_TEST_SOCKET="+socket)
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("PID gate regression: %v\n%s", err, output)
				}
			}
			cancel()
			<-done

		})
	}
}
