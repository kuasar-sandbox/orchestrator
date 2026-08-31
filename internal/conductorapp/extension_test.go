package conductorapp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestStartExtensionExactlyOnceWithLiveHost(t *testing.T) {
	storage := extensionTestStore(t)
	extension := &recordingExtension{}
	runtime := &Runtime{Extension: extension}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := startExtension(ctx, runtime, storage, new(orch.Orchestrator)); err != nil {
		t.Fatal(err)
	}
	if extension.calls.Load() != 1 || extension.host == nil || extension.ctx != ctx {
		t.Fatalf("Start calls=%d host=%v ctx match=%t", extension.calls.Load(), extension.host, extension.ctx == ctx)
	}
	if _, found, err := extension.host.Sandboxes().Get(ctx, "missing"); err != nil || found {
		t.Fatalf("host sandbox source found=%t err=%v", found, err)
	}
}

func TestStartExtensionFailureAndNilFastPath(t *testing.T) {
	want := errors.New("extension unavailable")
	extension := &recordingExtension{err: want}
	_, err := startExtension(context.Background(), &Runtime{Extension: extension}, extensionTestStore(t), new(orch.Orchestrator))
	if !errors.Is(err, want) || extension.calls.Load() != 1 {
		t.Fatalf("start error=%v calls=%d", err, extension.calls.Load())
	}
	// A nil Extension must return before touching Store/Core or constructing hubs.
	if _, err := startExtension(context.Background(), &Runtime{}, nil, nil); err != nil {
		t.Fatalf("nil Extension start error=%v", err)
	}
}

func TestExtensionAPIWrapperAddRewriteOverrideAndPassThrough(t *testing.T) {
	var wrapCalls atomic.Int32
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Core-Path", r.URL.Path)
		_, _ = io.WriteString(w, "core:"+r.URL.Path)
	})
	wrapper := apiWrapperFunc(func(next http.Handler) http.Handler {
		wrapCalls.Add(1)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/private":
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, "private")
			case "/rewrite":
				r.URL.Path = "/core-rewritten"
				next.ServeHTTP(w, r)
			case "/core-overridden":
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, "override")
			default:
				next.ServeHTTP(w, r)
			}
		})
	})
	wrapped, err := wrapExtensionAPI(wrapper, core)
	if err != nil || wrapCalls.Load() != 1 {
		t.Fatalf("wrap = %v, calls=%d", err, wrapCalls.Load())
	}
	for _, test := range []struct {
		path     string
		status   int
		body     string
		corePath string
	}{
		{path: "/private", status: http.StatusCreated, body: "private"},
		{path: "/rewrite", status: http.StatusOK, body: "core:/core-rewritten", corePath: "/core-rewritten"},
		{path: "/core-overridden", status: http.StatusAccepted, body: "override"},
		{path: "/unchanged", status: http.StatusOK, body: "core:/unchanged", corePath: "/unchanged"},
	} {
		t.Run(test.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			wrapped.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
			if recorder.Code != test.status || recorder.Body.String() != test.body || recorder.Header().Get("X-Core-Path") != test.corePath {
				t.Fatalf("response = %d %q core-path=%q", recorder.Code, recorder.Body.String(), recorder.Header().Get("X-Core-Path"))
			}
		})
	}
}

func TestExtensionAPIWrapperNilResultFails(t *testing.T) {
	_, err := wrapExtensionAPI(apiWrapperFunc(func(http.Handler) http.Handler { return nil }), http.NotFoundHandler())
	if err == nil {
		t.Fatal("nil API wrapper result was accepted")
	}
}

type apiWrapperFunc func(http.Handler) http.Handler

func (f apiWrapperFunc) WrapAPI(next http.Handler) http.Handler { return f(next) }

type recordingExtension struct {
	calls atomic.Int32
	ctx   context.Context
	host  conductorextension.Host
	err   error
}

func (e *recordingExtension) Start(ctx context.Context, host conductorextension.Host) error {
	e.calls.Add(1)
	e.ctx = ctx
	e.host = host
	return e.err
}

func extensionTestStore(t *testing.T) *store.Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	storage, err := store.Open(filepath.Join(t.TempDir(), "extension.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return storage
}
