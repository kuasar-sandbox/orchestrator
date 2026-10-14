// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestTerminalFallbackCoversAllFrameBoundaries(t *testing.T) {
	for _, size := range []int{MaxLine - 1, MaxLine, MaxLine + 1, 2 * MaxLine} {
		for _, ending := range []string{"\n", "\r\n", ""} {
			for _, prefix := range []string{"", "[console] "} {
				var out bytes.Buffer
				fallback := &terminalFallbackWriter{out: &out, raw: func() bool { return true }}
				w, err := newWriter(Target{Tag: "test"}, fallback, prefix, func(string, map[string]string) error {
					return errors.New("journal unavailable")
				})
				if err != nil {
					t.Fatal(err)
				}
				_, _ = w.Write([]byte(strings.Repeat("x", size)))
				for _, ch := range []byte(ending) {
					_, _ = w.Write([]byte{ch})
				}
				_ = w.Close()
				result := out.String()
				if strings.Count(result, "x") != size {
					t.Fatalf("lost payload at size=%d ending=%q", size, ending)
				}
				for i, ch := range result {
					if ch == '\n' && (i == 0 || result[i-1] != '\r') {
						t.Fatalf("bare LF at size=%d ending=%q prefix=%q", size, ending, prefix)
					}
				}
				if strings.Contains(result, "\r\r\n") {
					t.Fatalf("duplicate CR at size=%d ending=%q", size, ending)
				}
			}
		}
	}
}

func TestTerminalFallbackTracksModeAndLeavesSuccessAlone(t *testing.T) {
	var out bytes.Buffer
	raw, probes := false, 0
	fallback := &terminalFallbackWriter{out: &out, raw: func() bool { probes++; return raw }}
	for _, input := range []string{"cooked\n", "raw\n", "already\r\n", "restored\n"} {
		raw = input == "raw\n" || input == "already\r\n"
		if n, err := fallback.Write([]byte(input)); n != len(input) || err != nil {
			t.Fatalf("Write=%d,%v", n, err)
		}
	}
	if out.String() != "cooked\nraw\r\nalready\r\nrestored\n" {
		t.Fatalf("mode conversion=%q", out.String())
	}
	out.Reset()
	probes = 0
	w, err := newWriter(Target{Tag: "test"}, fallback, "", func(string, map[string]string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("native\n"))
	_ = w.Close()
	if probes != 0 || out.Len() != 0 {
		t.Fatal("successful native send inspected or wrote the fallback")
	}
}
