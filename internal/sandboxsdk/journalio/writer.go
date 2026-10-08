// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// MaxLine bounds both a buffered partial line and each emitted message, even
// for one very large Write containing a newline. It is not a journal limit.
const MaxLine = 60 << 10

type sendFunc func(string, map[string]string) error

// Writer is one independently buffered output stream. Writes and Close are
// serialized; producers must finish before Close to preserve every tail byte.
// Send/fallback errors are best-effort and never fail a runtime data pump.
// Native journal I/O is synchronous; this is not a nonblocking logging queue.
type Writer struct {
	mu       sync.Mutex
	fields   map[string]string
	send     sendFunc
	fallback io.Writer
	prefix   string
	buf      []byte
	closed   bool
}

func newWriter(t Target, fallback io.Writer, prefix string, send sendFunc) (*Writer, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
	fields := make(map[string]string, len(t.Fields)+1)
	for key, value := range t.Fields {
		fields[key] = value
	}
	fields["SYSLOG_IDENTIFIER"] = t.Tag
	return &Writer{fields: fields, send: send, fallback: fallback, prefix: prefix}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	n := len(p)
	for len(p) != 0 {
		if len(w.buf) == MaxLine {
			// Retain a full frame until the next byte (possibly in another
			// Write) distinguishes an actual line ending from a forced split.
			// Close still flushes a full unterminated frame without trimming.
			lineEnd := p[0] == '\n'
			w.emit(w.buf, lineEnd)
			w.buf = w.buf[:0]
			if lineEnd {
				p = p[1:]
			}
			continue
		}
		limit := min(len(p), MaxLine-len(w.buf))
		chunk := p[:limit]
		if i := bytes.IndexByte(chunk, '\n'); i >= 0 {
			if len(w.buf) == 0 {
				w.emit(chunk[:i], true)
			} else {
				w.append(chunk[:i])
				w.emit(w.buf, true)
				w.buf = w.buf[:0]
			}
			p = p[i+1:]
			continue
		}
		w.append(chunk)
		p = p[limit:]
	}
	return n, nil
}

// Grow lazily, but cap capacity as well as length. Short complete lines do not
// allocate a persistent buffer; idle streams do not reserve MaxLine bytes.
func (w *Writer) append(p []byte) {
	need := len(w.buf) + len(p)
	if need > cap(w.buf) {
		size := min(MaxLine, max(need, max(256, 2*cap(w.buf))))
		buf := make([]byte, len(w.buf), size)
		copy(buf, w.buf)
		w.buf = buf
	}
	w.buf = append(w.buf, p...)
}

func (w *Writer) emit(line []byte, lineEnd bool) {
	original := line
	if lineEnd {
		line = bytes.TrimRight(line, "\r")
	}
	if len(line) == 0 {
		return
	}
	if err := w.send(string(line), w.fields); err == nil {
		return
	}
	// Normalize CRLF only for native MESSAGE. The fallback keeps the input's
	// CR so Bridge's conversion still works while the controlling TTY is raw.
	// Use the original sink, never log.Writer(), which might be this writer.
	if w.fallback != nil {
		_, _ = fmt.Fprintf(w.fallback, "%s%s\n", w.prefix, original)
	}
}

// Close flushes one remaining partial line and is idempotent. Later writes
// return io.ErrClosedPipe rather than silently accepting bytes after shutdown.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.emit(w.buf, false)
		w.buf = nil
		w.closed = true
	}
	return nil
}
