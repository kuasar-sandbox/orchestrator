// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"strings"
	"testing"
)

func TestWriterDoesNotTrimCarriageReturnAtSizeBoundary(t *testing.T) {
	var got strings.Builder
	w := mustWriter(t, Target{Tag: "app"}, nil, "", func(message string, _ map[string]string) error {
		got.WriteString(message)
		return nil
	})
	want := strings.Repeat("x", MaxLine-1) + "\rmore"
	_, _ = w.Write([]byte(want + "\n"))
	_ = w.Close()
	if got.String() != want {
		t.Fatal("forced size boundary discarded a data byte")
	}
}

func TestWriterClosePreservesUnterminatedCarriageReturn(t *testing.T) {
	var entries []entry
	w := mustWriter(t, Target{Tag: "app"}, nil, "", capture(&entries))
	_, _ = w.Write([]byte("tail\r"))
	_ = w.Close()
	if len(entries) != 1 || entries[0].message != "tail\r" {
		t.Fatalf("Close changed unterminated data: %#v", entries)
	}
}
