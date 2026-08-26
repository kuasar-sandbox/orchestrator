package prefetch

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestResolveUnsupported(t *testing.T) {
	r := resolver{cfg: config.CheckpointConfig{}}
	_, err := r.resolve(context.Background(), "garbage")
	if !errors.Is(err, ErrUnsupportedReference) {
		t.Fatalf("resolve(garbage) err = %v, want ErrUnsupportedReference", err)
	}

	// e2b reference of the wrong kind (img, not snp) is unsupported.
	img := "e2b-img-" + b64("file://x.image@location:sid")
	_, err = r.resolve(context.Background(), img)
	if !errors.Is(err, ErrUnsupportedReference) {
		t.Fatalf("resolve(img) err = %v, want ErrUnsupportedReference", err)
	}
}

func TestResolveTemplateID(t *testing.T) {
	parent := t.TempDir()
	sid := "mysid"
	sum := sha256Hex(sid)
	dir := filepath.Join(parent, sum[:2], sum[2:4], sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"abc123.snapshot", "abc123.overlay"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	r := resolver{cfg: config.CheckpointConfig{Remote: config.CheckpointRemoteConfig{RefLocationParent: "file://" + parent}}}
	ref := "file://abc123.snapshot@location:" + sid
	tid := "e2b-snp-" + b64(ref)
	got, err := r.resolve(context.Background(), tid)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.refKey != sid {
		t.Fatalf("refKey = %q, want %q", got.refKey, sid)
	}
	if len(got.paths) != 2 {
		t.Fatalf("paths = %v, want 2 (snapshot+overlay)", got.paths)
	}
	if filepath.Base(got.paths[0]) != "abc123.snapshot" {
		t.Fatalf("first path = %q, want snapshot", got.paths[0])
	}
	if filepath.Base(got.paths[1]) != "abc123.overlay" {
		t.Fatalf("second path = %q, want overlay", got.paths[1])
	}
}

func TestResolveMissingDir(t *testing.T) {
	parent := t.TempDir()
	r := resolver{cfg: config.CheckpointConfig{Remote: config.CheckpointRemoteConfig{RefLocationParent: "file://" + parent}}}
	ref := "file://abc123.snapshot@location:nosuch"
	tid := "e2b-snp-" + b64(ref)
	if _, err := r.resolve(context.Background(), tid); err == nil {
		t.Fatalf("resolve(missing dir) = nil, want error")
	}
}

func TestWarmDedupAndUnsupported(t *testing.T) {
	w := New(config.CheckpointConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil))).(*warmer)
	defer w.Close()

	// Unsupported reference: rejected, not accepted.
	_, err := w.Warm(context.Background(), PrefetchReq{Reference: "garbage"})
	if !errors.Is(err, ErrUnsupportedReference) {
		t.Fatalf("Warm(garbage) err = %v, want ErrUnsupportedReference", err)
	}

	// A reference with no artifacts resolves to an error (config fault), but the
	// missing ref_location_parent yields a config error, not unsupported.
	if _, err := w.Warm(context.Background(), PrefetchReq{Reference: "e2b-snp-" + b64("file://a.snapshot@location:x")}); err == nil {
		t.Fatalf("Warm(no ref_location_parent) = nil, want config error")
	}
}

func TestWarmSurvivesCallerCancellationAndTracksStatus(t *testing.T) {
	parent := t.TempDir()
	sid := "tracked-sid"
	sum := sha256Hex(sid)
	dir := filepath.Join(parent, sum[:2], sum[2:4], sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tracked.snapshot"), make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	w := New(config.CheckpointConfig{Remote: config.CheckpointRemoteConfig{RefLocationParent: "file://" + parent}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ref := "e2b-snp-" + b64("file://tracked.snapshot@location:"+sid)
	result, err := w.Warm(ctx, PrefetchReq{Reference: ref})
	if err != nil {
		t.Fatalf("Warm: %v", err)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, found := w.Status(result.RequestID)
		if !found {
			t.Fatal("request disappeared")
		}
		if status.State == "fetched" {
			return
		}
		if status.State == "failed" {
			t.Fatalf("prefetch failed after caller cancellation: %s", status.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("request did not finish: %+v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPrefetchFileOnTempFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "pf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	// Best-effort: should not return an error on a readable non-empty file.
	if err := prefetchFile(context.Background(), int(f.Fd()), 4096); err != nil {
		t.Fatalf("prefetchFile: %v", err)
	}
}

func TestPrefetchFileEmpty(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "pf-empty")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// An empty file must not attempt mmap(0)+MADV (EINVAL); it is a no-op.
	if err := prefetchFile(context.Background(), int(f.Fd()), 0); err != nil {
		t.Fatalf("prefetchFile(empty): %v", err)
	}
}

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
