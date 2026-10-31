package sandboxsdk

import (
	"io"
	"log"

	"github.com/kuasar-sandbox/sandboxer/pkg/readiness"
)

// Readiness uses the SDK's event framing and stream ownership. The runner owns
// the diagnostic prefix and decides how its process logger is installed.
type Readiness = readiness.Writer

func NewReadiness(w io.WriteCloser) *Readiness {
	return readiness.NewWriter(w, func(format string, args ...any) {
		log.Printf("[sandbox-ctl] "+format, args...)
	})
}
