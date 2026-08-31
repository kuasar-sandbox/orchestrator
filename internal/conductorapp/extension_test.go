package conductorapp

import (
	"context"
	"errors"
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
	if err := startExtension(ctx, runtime, storage, new(orch.Orchestrator)); err != nil {
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
	err := startExtension(context.Background(), &Runtime{Extension: extension}, extensionTestStore(t), new(orch.Orchestrator))
	if !errors.Is(err, want) || extension.calls.Load() != 1 {
		t.Fatalf("start error=%v calls=%d", err, extension.calls.Load())
	}
	// A nil Extension must return before touching Store/Core or constructing hubs.
	if err := startExtension(context.Background(), &Runtime{}, nil, nil); err != nil {
		t.Fatalf("nil Extension start error=%v", err)
	}
}

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
