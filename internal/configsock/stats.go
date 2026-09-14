package configsock

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const PathTelemetryStats = "/internal/plugin/telemetry/stats"

// Source work has its own deadline. Reserve bounded time to deliver its result,
// including a 503 after that deadline has expired.
const nativeStatsReplyTimeout = time.Second

type NativeStatsReader = conductorextension.StatsReader

func (r *Registry) telemetryStatsLease(peer int) (context.Context, bool) {
	if r == nil || peer <= 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.m[routesync.TelemetryPluginID]
	if p == nil || !p.ready || p.peerPID != peer || p.lease == nil || p.lease.Err() != nil {
		return nil, false
	}
	return p.lease, true
}

func (s *Server) handleTelemetryStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	peer, ok := peerFrom(r.Context())
	lease, live := s.deps.Plugins.telemetryStatsLease(peer)
	if !ok || !live || !s.pluginAuthed(peer) {
		http.Error(w, "not authorized (telemetry plugin)", http.StatusForbidden)
		return
	}
	if s.deps.Stats == nil {
		writeStatsError(w, api.ErrStatsUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), conductorextension.StatsTimeout)
	defer cancel()
	stop := context.AfterFunc(lease, cancel)
	defer stop()
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline.Add(nativeStatsReplyTimeout))
	bodyClosed := make(chan struct{})
	stopBody := context.AfterFunc(ctx, func() {
		_ = controller.SetReadDeadline(time.Now())
		_ = r.Body.Close()
		close(bodyClosed)
	})
	defer func() {
		if !stopBody() {
			<-bodyClosed
		}
		_ = controller.SetReadDeadline(time.Time{})
		_ = controller.SetWriteDeadline(time.Time{})
	}()
	const maxRequestBytes = 64 << 10
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		if ctx.Err() != nil || lease.Err() != nil || !time.Now().Before(deadline) {
			writeStatsError(w, api.ErrStatsUnavailable)
		} else {
			writeStatsError(w, api.ErrBadRequest)
		}
		return
	}
	var request conductorextension.StatsRequest
	if err := strictjson.Decode(body, &request); err != nil {
		writeStatsError(w, api.ErrBadRequest)
		return
	}
	rows, err := s.deps.Stats.ReadStats(ctx, request)
	if ctx.Err() != nil || lease.Err() != nil {
		err = api.ErrStatsUnavailable
	}
	if err != nil {
		writeStatsError(w, err)
		return
	}
	encoded, err := json.Marshal(rows)
	if err != nil || len(encoded) > conductorextension.MaxStatsResponseBytes {
		writeStatsError(w, fmt.Errorf("%w: native stats response encoding", api.ErrStatsUnavailable))
		return
	}
	writeStatsResponse(w, http.StatusOK, encoded)
}

func writeStatsError(w http.ResponseWriter, err error) {
	status, message := api.StatsErrorStatus(err)
	encoded, _ := json.Marshal(map[string]string{"message": message})
	writeStatsResponse(w, status, append(encoded, '\n'))
}

func writeStatsResponse(w http.ResponseWriter, status int, body []byte) {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(nativeStatsReplyTimeout))
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
	// Flush the complete, length-delimited response while the deadline applies,
	// before restoring the connection's deadline for another config-socket call.
	_ = controller.Flush()
}
