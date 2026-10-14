// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func terminalFallback(out io.Writer) io.Writer {
	file, ok := out.(*os.File)
	if !ok {
		return out
	}
	if file == nil {
		return nil
	}
	return &terminalFallbackWriter{out: out, raw: func() bool {
		state, err := unix.IoctlGetTermios(int(file.Fd()), unix.TCGETS)
		return err == nil && state.Oflag&unix.OPOST == 0
	}}
}
