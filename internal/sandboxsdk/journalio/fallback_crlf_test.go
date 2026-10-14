// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestWriterFallbackPreservesTerminalCRLF(t *testing.T) {
	for _, prefix := range []string{"", "[app] "} {
		var fallback bytes.Buffer
		var messages []string
		w := mustWriter(t, Target{Tag: "app"}, &fallback, prefix, func(message string, _ map[string]string) error {
			messages = append(messages, message)
			return errors.New("journal unavailable")
		})
		// Bridge applies CRLF before the component writer while the terminal is
		// raw. The CR and LF may arrive in separate writes.
		_, _ = w.Write([]byte("first\r"))
		_, _ = w.Write([]byte("\nsecond\r\n"))
		_ = w.Close()
		if !reflect.DeepEqual(messages, []string{"first", "second"}) {
			t.Fatalf("native messages contain terminal framing: %#v", messages)
		}
		if want := prefix + "first\r\n" + prefix + "second\r\n"; fallback.String() != want {
			t.Fatalf("fallback=%q want=%q", fallback.String(), want)
		}
	}
}
