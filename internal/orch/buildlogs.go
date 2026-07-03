package orch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// BuildLogs backs the e2b build-status logs: it returns the build's curated
// progress entries from offset onward. The build pipeline writes that progress
// to journald tagged SYSLOG_IDENTIFIER=build under the sandbox-builder@<bid>
// unit (run-builder's milestones + relayed RUN output + sandbox-ctl-streamed
// flatten progress); guest kernel dmesg (tag "console") and sandbox-ctl's own
// logs are deliberately NOT in this filter, so the SDK sees a clean build log.
// journald is the single sink — no temp files — and journalctl the reader
// (sdjournal needs CGO; this binary is CGO-free).
func (o *Orchestrator) BuildLogs(ctx context.Context, apiKey, tid, bid string, offset int) ([]api.BuildLogEntry, error) {
	b, err := o.st.GetBuild(ctx, bid)
	if err != nil {
		return nil, err
	}
	if !ownsBuild(b, apiKey) || b.TemplateID != tid {
		return nil, api.ErrNotFound
	}
	entries := o.readBuildJournal(ctx, bid)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(entries) {
		return nil, nil
	}
	return entries[offset:], nil
}

// readBuildJournal queries the build unit's journal for the tagged progress
// stream. A journalctl failure (not under systemd, unit never logged) yields no
// entries rather than an error — build logs are best-effort telemetry, not a
// gate on the build status itself.
func (o *Orchestrator) readBuildJournal(ctx context.Context, bid string) []api.BuildLogEntry {
	cmd := exec.CommandContext(ctx, "journalctl",
		"_SYSTEMD_UNIT="+o.builderUnit(bid),
		"SYSLOG_IDENTIFIER="+configsock.BuildLogTag,
		"--output=json", "--no-pager",
		"--output-fields=MESSAGE,PRIORITY,__REALTIME_TIMESTAMP")
	out, err := cmd.Output()
	if err != nil {
		o.log.Debug("build journal read failed; reporting no logs", "bid", bid, "err", err)
		return nil
	}
	var entries []api.BuildLogEntry
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<16), 8<<20) // some lines (RUN output) are large
	for sc.Scan() {
		var rec struct {
			Message    json.RawMessage `json:"MESSAGE"`
			Priority   string          `json:"PRIORITY"`
			RealtimeTS string          `json:"__REALTIME_TIMESTAMP"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) != nil {
			continue
		}
		msg := decodeJournalMessage(rec.Message)
		if msg == "" {
			continue
		}
		entries = append(entries, api.BuildLogEntry{
			Timestamp: microsToTime(rec.RealtimeTS),
			Level:     priorityToLevel(rec.Priority),
			Message:   msg,
		})
	}
	return entries
}

// decodeJournalMessage handles journald's two MESSAGE encodings: a plain JSON
// string (our text lines), or an array of byte values (when journald deems the
// payload non-UTF-8). Anything else → "".
func decodeJournalMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var b []byte
	if json.Unmarshal(raw, &b) == nil {
		return string(b)
	}
	return ""
}

// microsToTime parses __REALTIME_TIMESTAMP (microseconds since the epoch).
func microsToTime(s string) time.Time {
	us, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

// priorityToLevel maps a syslog PRIORITY to an e2b LogLevel string.
func priorityToLevel(p string) string {
	n, err := strconv.Atoi(p)
	if err != nil {
		return "info"
	}
	switch {
	case n <= 3: // emerg..err
		return "error"
	case n == 4: // warning
		return "warn"
	case n >= 7: // debug
		return "debug"
	default: // notice/info
		return "info"
	}
}
