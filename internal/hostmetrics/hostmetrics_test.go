package hostmetrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestHostOTLPIdentityAndMultipleSourceMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "otlp.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan pmetric.Metrics, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("invalid request %s", r.URL.Path)
			w.WriteHeader(400)
			return
		}
		raw := pmetricotlp.NewExportRequest()
		data, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		if e := raw.UnmarshalProto(data); e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		done <- raw.Metrics()
	})}
	go server.Serve(listener)
	defer server.Shutdown(context.Background())
	t.Setenv("KS_HOST_OTLP_SOCKET", path)
	p := New("node-ctl", "runner", map[string]string{"sandbox.id": "sid", "sandbox.stable_id": "stable", "sandbox.run_id": "run"})
	now := time.Now().UTC()
	if err := p.Emit(context.Background(), []Sample{{Name: "sandbox.usage.cpu.seconds", Unit: "s", Timestamp: now, Value: 2.5, Attributes: map[string]string{"usage.name": "guest.cpu"}}, {Name: "sandbox.usage.cpu.seconds", Unit: "s", Timestamp: now, Value: 0.5, Attributes: map[string]string{"usage.name": "ch.cpu"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.DataPointCount() != 2 {
			t.Fatalf("points=%d", result.DataPointCount())
		}
		resource := result.ResourceMetrics().At(0).Resource().Attributes()
		for k, want := range map[string]string{"sandbox.id": "sid", "sandbox.stable_id": "stable", "sandbox.run_id": "run", "sandbox.telemetry.source": "runner"} {
			v, _ := resource.Get(k)
			if v.Str() != want {
				t.Fatalf("%s = %s want %s", k, v.Str(), want)
			}
		}
		if result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().Len() != 2 {
			t.Fatal("series not grouped")
		}
	case <-time.After(time.Second):
		t.Fatal("no metrics received")
	}
}

func TestUnreachableHostOTLPIsBounded(t *testing.T) {
	t.Setenv("KS_HOST_OTLP_SOCKET", filepath.Join(t.TempDir(), "missing.sock"))
	p := New("node-ctl", "runner", nil)
	before := time.Now()
	err := p.Emit(context.Background(), []Sample{{Name: "host.sample", Timestamp: time.Now(), Value: 1}})
	if err == nil {
		t.Fatal("missing socket accepted")
	}
	if time.Since(before) > time.Second {
		t.Fatal("send too slow")
	}
	if _, err := os.Stat(p.socket); !os.IsNotExist(err) {
		t.Fatal("created unexpected socket")
	}
}

func TestHostOTLPPartialAcceptance(t *testing.T) {
	for _, mediaType := range []string{"application/x-protobuf", "application/json"} {
		for _, rejected := range []int64{0, 1} {
			reply := pmetricotlp.NewExportResponse()
			reply.PartialSuccess().SetRejectedDataPoints(rejected)
			reply.PartialSuccess().SetErrorMessage("receiver message")
			body, err := reply.MarshalProto()
			if mediaType == "application/json" {
				body, err = reply.MarshalJSON()
			}
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", mediaType)
				_, _ = w.Write(body)
			}))
			p := New("node-ctl", "runner", nil)
			p.client = &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}}}
			err = p.Emit(context.Background(), []Sample{{Name: "host.sample", Timestamp: time.Now(), Value: 1}})
			server.Close()
			if rejected == 0 && err != nil || rejected > 0 && !errors.Is(err, ErrPartialAcceptance) {
				t.Fatalf("format=%s rejected=%d error=%v", mediaType, rejected, err)
			}
		}
	}
}
