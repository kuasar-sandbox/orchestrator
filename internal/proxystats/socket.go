package proxystats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	BatchPath             = "/v1/traffic:batchGet"
	maxBatchQueries       = 128
	maxBatchRequestBytes  = 64 << 10
	maxBatchResponseBytes = 1 << 20
)

type TrafficQuery struct {
	SandboxID string        `json:"sandboxID"`
	RunID     string        `json:"runID"`
	Profile   types.Profile `json:"profile"`
	State     types.State   `json:"state"`
}

type BatchRequest struct {
	Sandboxes []TrafficQuery `json:"sandboxes"`
}

type BatchResult struct {
	SandboxID string            `json:"sandboxID"`
	Stats     *api.TrafficStats `json:"stats"`
}

type BatchResponse struct {
	Sandboxes []BatchResult `json:"sandboxes"`
}

type RouteIdentity struct {
	RunID       string
	Profile     types.Profile
	State       types.State
	MaxInflight config.MaxInflight
}

type StatsServer struct {
	master *MasterStats
	synced func() bool
	lookup func(string) (RouteIdentity, bool)
	log    *slog.Logger
}

func NewStatsServer(master *MasterStats, synced func() bool, lookup func(string) (RouteIdentity, bool), log *slog.Logger) *StatsServer {
	return &StatsServer{master: master, synced: synced, lookup: lookup, log: log}
}

func (s *StatsServer) Serve(ctx context.Context, listener net.Listener) error {
	return s.ServeHandler(ctx, listener, s.Handler())
}

// ServeHandler serves a precomposed management handler on listener. It lets a
// trusted proxy master wrap the built-in stats routes without changing the
// stats-socket transport or constructing a loopback client.
func (s *StatsServer) ServeHandler(ctx context.Context, listener net.Listener, handler http.Handler) error {
	if handler == nil {
		return errors.New("proxy stats: management handler is required")
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	err := server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *StatsServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+BatchPath, s.batchGet)
	return mux
}

func (s *StatsServer) batchGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.master == nil || s.synced == nil || !s.synced() || s.lookup == nil || !s.master.Available() {
		writeSocketError(w, http.StatusServiceUnavailable, "traffic stats unavailable")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request BatchRequest
	if err := decoder.Decode(&request); err != nil || len(request.Sandboxes) == 0 || len(request.Sandboxes) > maxBatchQueries {
		writeSocketError(w, http.StatusBadRequest, "invalid batch request")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeSocketError(w, http.StatusBadRequest, "invalid batch request")
		return
	}
	response := BatchResponse{Sandboxes: make([]BatchResult, 0, len(request.Sandboxes))}
	for _, query := range request.Sandboxes {
		if !validSandboxID(query.SandboxID) ||
			(query.Profile != types.ProfileE2B && query.Profile != types.ProfileBare) ||
			(query.State != types.StateStarting && query.State != types.StateRunning && query.State != types.StatePaused) {
			writeSocketError(w, http.StatusBadRequest, "invalid traffic query")
			return
		}
		identity, found := s.lookup(query.SandboxID)
		if !found || identity.RunID != query.RunID || identity.Profile != query.Profile || identity.State != query.State {
			writeSocketError(w, http.StatusServiceUnavailable, "route identity unavailable")
			return
		}
		stats, err := s.master.SandboxTrafficStats(r.Context(), query.SandboxID, query.RunID, query.Profile, query.State)
		if err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, api.ErrStatsConflict) {
				status = http.StatusConflict
			}
			writeSocketError(w, status, "traffic stats unavailable")
			return
		}
		stats.MaxInflight = identity.MaxInflight
		response.Sandboxes = append(response.Sandboxes, BatchResult{SandboxID: query.SandboxID, Stats: stats})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func writeSocketError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}

func QuerySocket(ctx context.Context, socketPath string, queries []TrafficQuery) ([]BatchResult, error) {
	if socketPath == "" || len(queries) == 0 || len(queries) > maxBatchQueries {
		return nil, api.ErrStatsUnavailable
	}
	payload, err := json.Marshal(BatchRequest{Sandboxes: queries})
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://proxy-stats"+BatchPath, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: query proxy stats: %v", api.ErrStatsUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusConflict {
			return nil, api.ErrStatsConflict
		}
		return nil, api.ErrStatsUnavailable
	}
	limited := io.LimitReader(response.Body, maxBatchResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxBatchResponseBytes {
		return nil, api.ErrStatsUnavailable
	}
	var batch BatchResponse
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil || len(batch.Sandboxes) != len(queries) {
		return nil, api.ErrStatsUnavailable
	}
	for i, result := range batch.Sandboxes {
		if result.SandboxID != queries[i].SandboxID || result.Stats == nil {
			return nil, api.ErrStatsUnavailable
		}
	}
	return batch.Sandboxes, nil
}
