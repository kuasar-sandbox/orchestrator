// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

type entry struct {
	message string
	fields  map[string]string
}

func capture(entries *[]entry) sendFunc {
	return func(message string, fields map[string]string) error {
		copyFields := make(map[string]string, len(fields))
		for key, value := range fields {
			copyFields[key] = value
		}
		*entries = append(*entries, entry{message, copyFields})
		return nil
	}
}
func mustWriter(t *testing.T, target Target, fallback io.Writer, prefix string, send sendFunc) *Writer {
	t.Helper()
	w, err := newWriter(target, fallback, prefix, send)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestWriterFramesAndFlushes(t *testing.T) {
	var entries []entry
	var fallback bytes.Buffer
	w := mustWriter(t, Target{Tag: "app", Fields: map[string]string{"STREAM": "out"}}, &fallback, "[app] ", capture(&entries))
	for _, p := range []string{"one\ntw", "o\r\n\n", "tail"} {
		if n, err := w.Write([]byte(p)); n != len(p) || err != nil {
			t.Fatalf("Write %d %v", n, err)
		}
	}
	if len(entries) != 2 {
		t.Fatalf("entries before Close=%d", len(entries))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].message != "one" || entries[1].message != "two" || entries[2].message != "tail" {
		t.Fatalf("entries=%#v", entries)
	}
	if fallback.Len() != 0 {
		t.Fatal("successful send duplicated to fallback")
	}
	for _, e := range entries {
		if e.fields["SYSLOG_IDENTIFIER"] != "app" || e.fields["STREAM"] != "out" {
			t.Fatal(e)
		}
	}
	if n, err := w.Write([]byte("late")); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after Close %d %v", n, err)
	}
}

func TestWriterFallbackPerFailedSend(t *testing.T) {
	for _, prefix := range []string{"", "[app] "} {
		var sink bytes.Buffer
		calls := 0
		w := mustWriter(t, Target{Tag: "app"}, &sink, prefix, func(string, map[string]string) error {
			calls++
			if calls == 2 {
				return nil
			}
			return errors.New("journal unavailable")
		})
		_, _ = w.Write([]byte("first\nsecond\nthird"))
		_ = w.Close()
		if got, want := sink.String(), prefix+"first\n"+prefix+"third\n"; got != want {
			t.Fatalf("fallback=%q want=%q", got, want)
		}
	}
	w := mustWriter(t, Target{Tag: "app"}, errorWriter{}, "", func(string, map[string]string) error { return errors.New("send") })
	if n, err := w.Write([]byte("line\n")); n != 5 || err != nil {
		t.Fatalf("best effort=%d %v", n, err)
	}
	_ = w.Close()
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("fallback unavailable") }

func TestWriterBoundsEveryEmissionAndBuffer(t *testing.T) {
	for _, newline := range []bool{false, true} {
		var total int
		w := mustWriter(t, Target{Tag: "app"}, nil, "", func(message string, _ map[string]string) error {
			if len(message) > MaxLine {
				t.Fatalf("message length=%d", len(message))
			}
			total += len(message)
			return nil
		})
		const size = 8 << 20
		p := bytes.Repeat([]byte{'x'}, size)
		if newline {
			p = append(p, '\n')
		}
		_, _ = w.Write(p)
		if len(w.buf) > MaxLine || cap(w.buf) > MaxLine {
			t.Fatalf("buffer len=%d cap=%d", len(w.buf), cap(w.buf))
		}
		_ = w.Close()
		if total != size {
			t.Fatalf("bytes=%d want=%d", total, size)
		}
	}
}

func TestWriterShortLinesDoNotReserveLargeBuffers(t *testing.T) {
	w := mustWriter(t, Target{Tag: "app"}, nil, "", func(string, map[string]string) error { return nil })
	_, _ = w.Write([]byte("small\n"))
	if cap(w.buf) != 0 {
		t.Fatalf("complete short line allocated persistent buffer=%d", cap(w.buf))
	}
	_, _ = w.Write([]byte("part"))
	if cap(w.buf) > 256 {
		t.Fatalf("short partial line reserved %d", cap(w.buf))
	}
	_ = w.Close()
}

func TestWriterIndependentTargetsAndCopiedFields(t *testing.T) {
	fields := map[string]string{"STREAM": "stdout"}
	var out, errEntries []entry
	w1 := mustWriter(t, Target{Tag: "app", Fields: fields}, nil, "", capture(&out))
	fields["STREAM"] = "stderr"
	w2 := mustWriter(t, Target{Tag: "app", Fields: fields}, nil, "", capture(&errEntries))
	fields["STREAM"] = "later"
	_, _ = w1.Write([]byte("out"))
	_, _ = w2.Write([]byte("err\n"))
	_ = w1.Close()
	_ = w2.Close()
	if len(out) != 1 || out[0].message != "out" || out[0].fields["STREAM"] != "stdout" {
		t.Fatal(out)
	}
	if len(errEntries) != 1 || errEntries[0].message != "err" || errEntries[0].fields["STREAM"] != "stderr" {
		t.Fatal(errEntries)
	}
}

func TestWriterConcurrentWholeRecordsAndClose(t *testing.T) {
	var entries []entry
	w := mustWriter(t, Target{Tag: "app"}, nil, "", capture(&entries))
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = fmt.Fprintf(w, "%d:%d\n", i, j)
			}
		}(i)
	}
	wg.Wait()
	_ = w.Close()
	if len(entries) != 1200 {
		t.Fatalf("entries=%d", len(entries))
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.message] {
			t.Fatalf("duplicate %q", e.message)
		}
		seen[e.message] = true
	}

	w = mustWriter(t, Target{Tag: "app"}, nil, "", func(string, map[string]string) error { return nil })
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				n, err := w.Write([]byte("line\n"))
				if err == nil && n != 5 {
					t.Errorf("short success %d", n)
				}
				if err != nil && !errors.Is(err, io.ErrClosedPipe) {
					t.Errorf("write: %v", err)
				}
			}
		}()
	}
	_ = w.Close()
	wg.Wait()
	_ = w.Close()
}

func TestWriterValidatesDirectTargets(t *testing.T) {
	for _, target := range []Target{{Tag: ""}, {Tag: "app", Fields: map[string]string{"MESSAGE": "x"}}, {Tag: "app", Fields: map[string]string{"A": strings.Repeat("x", MaxTargetBytes)}}} {
		if _, err := newWriter(target, nil, "", func(string, map[string]string) error { return nil }); err == nil {
			t.Fatal("invalid direct target accepted")
		}
	}
}
