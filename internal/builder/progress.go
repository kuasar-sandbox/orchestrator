package builder

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/coreos/go-systemd/v22/journal"
)

// buildTag / consoleTag are defined in builder.go (aliasing the configsock
// contract): buildTag = the curated build progress the orchestrator surfaces to
// the SDK (journalctl … SYSLOG_IDENTIFIER=build); consoleTag = guest kernel
// dmesg, kept host-only.

// buildJournal is a concurrency-safe, line-buffering io.Writer that writes each
// line to journald tagged SYSLOG_IDENTIFIER=build (PRIORITY=info) — the one sink
// for run-builder's own milestones AND the RUN output relayed off the envd
// stream (envd is upstream, has no journald of its own; run-builder, the stream
// holder, writes it directly). Phase-sandbox app stdio + kernel go to journald
// straight from sandbox-ctl (--stdout-to/--console journald=…), not through here.
// Multiple goroutines write (the held startCmd stream runs concurrently with the
// main flow), hence the mutex. Falls back to a "[build] "-prefixed stderr when
// journald is unavailable (run-builder run outside systemd, e.g. a unit test).
type buildJournal struct {
	mu       sync.Mutex
	buf      []byte
	fallback io.Writer // non-nil ⇒ journald unavailable
}

func newBuildJournal() *buildJournal {
	b := &buildJournal{}
	if !journal.Enabled() {
		b.fallback = os.Stderr
	}
	return b
}

const buildJournalMaxLine = 60 << 10

func (b *buildJournal) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	for {
		i := bytes.IndexByte(b.buf, '\n')
		if i < 0 {
			break
		}
		b.emit(b.buf[:i])
		b.buf = b.buf[i+1:]
	}
	if len(b.buf) >= buildJournalMaxLine {
		b.emit(b.buf)
		b.buf = b.buf[:0]
	}
	return len(p), nil
}

// line writes one discrete milestone line (caller already a whole message).
func (b *buildJournal) line(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.emit([]byte(msg))
}

func (b *buildJournal) emit(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return
	}
	if b.fallback != nil {
		fmt.Fprintf(b.fallback, "[%s] %s\n", buildTag, line)
		return
	}
	_ = journal.Send(string(line), journal.PriInfo, map[string]string{"SYSLOG_IDENTIFIER": buildTag})
}

func (b *buildJournal) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buf) > 0 {
		b.emit(b.buf)
		b.buf = b.buf[:0]
	}
	return nil
}

// progress logs one build-facing milestone to the build journal (SDK-visible).
func (p *buildPipeline) progress(format string, args ...any) {
	if p.out != nil {
		p.out.line(fmt.Sprintf(format, args...))
	}
}
