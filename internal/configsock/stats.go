package configsock

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

const PathTelemetryStats = "/internal/plugin/telemetry/stats"

// Source work has its own deadline. Reserve bounded time to deliver its result,
// including a 503 after that deadline has expired.
const nativeStatsReplyTimeout = time.Second

type NativeStatsReader = conductorextension.StatsReader

// Capture capability and authorization from the same live registration.
func (r *Registry) telemetryStatsLease(peer int) (lease context.Context, holdAware bool) {
	if r == nil || peer <= 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.m[routesync.TelemetryPluginID]
	if p == nil || !p.ready || p.peerPID != peer || p.lease == nil || p.lease.Err() != nil {
		return nil, false
	}
	return p.lease, p.Caps.HoldTraffic
}

func (s *Server) handleTelemetryStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	peer, ok := peerFrom(r.Context())
	lease, holdAware := s.deps.Plugins.telemetryStatsLease(peer)
	if !ok || lease == nil || !s.pluginAuthed(peer) {
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
	if !holdAware {
		ctx = proxystats.WithPhysicalTelemetry(ctx)
	}
	rows, err := s.deps.Stats.ReadStats(ctx, request)
	if ctx.Err() != nil || lease.Err() != nil {
		err = api.ErrStatsUnavailable
	}
	if err != nil {
		writeStatsError(w, err)
		return
	}
	if !holdAware {
		rows, err = legacyTelemetryStats(rows)
		if err != nil {
			writeStatsError(w, err)
			return
		}
	}
	encoded, err := json.Marshal(rows)
	if err != nil || len(encoded) > conductorextension.MaxStatsResponseBytes {
		writeStatsError(w, fmt.Errorf("%w: native stats response encoding", api.ErrStatsUnavailable))
		return
	}
	writeStatsResponse(w, http.StatusOK, encoded)
}

// Older Telemetry binaries export every service and the top-level connected
// count as physical sockets. Give only that internal consumer its original
// view without mutating data shared with the native API or an extension.
func legacyTelemetryStats(rows []conductorextension.SandboxStats) ([]conductorextension.SandboxStats, error) {
	var out []conductorextension.SandboxStats
	for i, row := range rows {
		if row.Traffic == nil {
			continue
		}
		r := *row.Traffic
		hold, present := r.Services["hold"]
		if !present {
			// The legacy Proxy selected its physical idle time by boot clock.
			// Service wall timestamps can be out of order after a clock step.
			continue
		}
		if hold.Parking != 0 || hold.Connected > r.Inflight.Connected {
			return nil, api.ErrStatsUnavailable
		}
		if out == nil {
			out = slices.Clone(rows)
		}
		r.Inflight.Connected -= hold.Connected
		r.Services = maps.Clone(r.Services)
		delete(r.Services, "hold")
		// A custom reader may return native totals without the Proxy's physical
		// view. Its wall timestamps cannot order events across a clock step.
		// When all physical services agree, their last idle time is unambiguous.
		r.IdleSince = nil
		if !r.InplaceRestart && r.State == routesync.StateRunning && r.Inflight.Connected == 0 && r.Inflight.Parking == 0 {
			for _, physical := range r.Services {
				if physical.IdleSince == nil || (r.IdleSince != nil && !r.IdleSince.Equal(*physical.IdleSince)) {
					r.IdleSince = nil
					break
				}
				r.IdleSince = physical.IdleSince
			}
		}
		out[i].Traffic = &r
	}
	if out == nil {
		return rows, nil
	}
	return out, nil
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
