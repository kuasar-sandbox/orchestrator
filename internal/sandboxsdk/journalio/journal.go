// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"io"

	"github.com/coreos/go-systemd/v22/journal"
)

// New opens an explicit journal output. It never infers journal selection from
// stdio, reads identity variables, or inherits other outputs' fields. prefix is
// used only for best-effort fallback, never included in native MESSAGE values.
// A file fallback is terminal-aware only when a native send actually fails.
func New(t Target, fallback io.Writer, prefix string) (*Writer, error) {
	return newWriter(t, terminalFallback(fallback), prefix, func(message string, fields map[string]string) error {
		return journal.Send(message, journal.PriInfo, fields)
	})
}
