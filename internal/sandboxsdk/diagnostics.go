package sandboxsdk

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk/journalio"
)

// node-ctl owns one assigned sandbox for its entire process lifetime. Its own
// structured logger is separate from the standard logger used by sandboxer.
// The standard logger installation matches sandbox-ctl and is deliberately
// process-owned, not a library promise of concurrent diagnostic isolation.
var diagnosticsMu sync.Mutex

func diagnostics(target string) (io.Writer, func(), error) {
	if target == "" || target == "default" {
		return os.Stderr, func() {}, nil
	}
	parsed, err := journalio.Parse(target)
	if err != nil {
		return nil, nil, err
	}
	writer, err := journalio.New(parsed, os.Stderr, "")
	if err != nil {
		return nil, nil, err
	}
	diagnosticsMu.Lock()
	previous := log.Writer()
	log.SetOutput(writer)
	return writer, func() { log.SetOutput(previous); _ = writer.Close(); diagnosticsMu.Unlock() }, nil
}
