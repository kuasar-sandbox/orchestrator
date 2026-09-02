package configsock

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
)

const (
	ReadinessControlReady = "control_ready"
	ReadinessReady        = "ready"

	readinessSocketName = "ready.sock"
	readinessLineLimit  = 64
)

// ReadinessSocketPath is the one-shot runtime readiness socket shared by the
// orchestrator and node-ctl. It deliberately is not part of LaunchSpec: the
// assigned SandboxID and node RunRoot already define the SandboxRunDir for both
// processes.
func ReadinessSocketPath(runRoot, sandboxID string) string {
	return filepath.Join(nodepath.SandboxRunDir(runRoot, sandboxID), readinessSocketName)
}

// ReadReadiness consumes the complete sandbox-ctl readiness stream. The wire is
// deliberately tiny and closed: exactly control_ready, ready, then EOF. Keeping
// the parser here makes the UDS bridge and builder pipe enforce one protocol.
func ReadReadiness(r io.Reader) error {
	br := bufio.NewReaderSize(r, readinessLineLimit)
	for i, want := range []string{ReadinessControlReady, ReadinessReady} {
		got, err := readReadinessLine(br)
		if err != nil {
			return fmt.Errorf("read readiness event %d (%s): %w", i+1, want, err)
		}
		if got != want {
			return fmt.Errorf("readiness event %d = %q, want %q", i+1, got, want)
		}
	}

	b, err := br.ReadByte()
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("read readiness EOF: %w", err)
	default:
		return fmt.Errorf("unexpected readiness data after ready (first byte %#x)", b)
	}
}

func readReadinessLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", fmt.Errorf("readiness line exceeds %d bytes", readinessLineLimit)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", io.ErrUnexpectedEOF
		}
		return "", err
	}
	if len(line) > readinessLineLimit {
		return "", fmt.Errorf("readiness line exceeds %d bytes", readinessLineLimit)
	}
	return string(line[:len(line)-1]), nil
}
