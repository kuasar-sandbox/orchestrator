// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import "io"

// terminalFallbackWriter formats only failed native sends. Querying the current
// terminal mode here handles startup, raw-mode bridging, and terminal restoration
// without making line framing depend on terminal state or on a trailing CR.
type terminalFallbackWriter struct {
	out io.Writer
	raw func() bool
}

func (w *terminalFallbackWriter) Write(p []byte) (int, error) {
	if !w.raw() {
		return w.out.Write(p)
	}
	var converted []byte
	start := 0
	for i, c := range p {
		if c != '\n' || (i > 0 && p[i-1] == '\r') {
			continue
		}
		converted = append(converted, p[start:i]...)
		converted = append(converted, '\r', '\n')
		start = i + 1
	}
	if converted == nil {
		return w.out.Write(p)
	}
	converted = append(converted, p[start:]...)
	n, err := w.out.Write(converted)
	if err == nil && n != len(converted) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
