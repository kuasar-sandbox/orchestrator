package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Node view watches are low-frequency control-plane streams for scaler views.
// Router does not subscribe to a global route stream. node_list is projected from
// node_link quorum state; sandbox-group data is imported on scaler/provider side.
const (
	ScaleLinkNodeListWatchPath = "/scale-link/watch-node-list" // scaler node_list WATCH_LIST
	ScaleLinkRegisterPath      = "/scale-link/register"
	ScaleLinkSelectorPatchPath = "/scale-link/selector-patch"
	ScaleLinkImportSourcePath  = "/scale-link/import-source-lease"
	ScaleLinkSourceCursorPath  = "/scale-link/import-source-cursor"
	ScaleLinkPlacePath         = "/scale-link/place"
	ScaleLinkVerifyKeyPath     = "/scale-link/verify-key"
)

// ViewEvent is one frame on a view watch ([4B LE len][ViewEvent]); Value is the
// raw node_list record JSON.
type ViewEvent struct {
	Type  string          `json:"type"` // "reset" | "put" | "delete" | "bookmark"
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Rev   int64           `json:"rev,omitempty"`
	Token string          `json:"token,omitempty"`
}

type ScalerRegister struct {
	ID              string `json:"id"`
	Advertise       string `json:"advertise"`
	MemberlistLabel string `json:"memberlist_label"`
}

type ImportSourceLeaseRequest struct {
	SourceID  string `json:"source_id"`
	OwnerID   string `json:"owner_id"`
	RunID     string `json:"run_id"`
	TTLMillis int64  `json:"ttl_ms"`
}

type ImportSourceLeaseResponse struct {
	Acquired bool              `json:"acquired"`
	Lease    ImportSourceLease `json:"lease"`
}

type ImportSourceCursorRequest struct {
	SourceID string `json:"source_id"`
	OwnerID  string `json:"owner_id"`
	RunID    string `json:"run_id"`
	Term     uint64 `json:"term"`
	Cursor   string `json:"cursor,omitempty"`
	Complete bool   `json:"complete,omitempty"`
	Error    string `json:"error,omitempty"`
}

func (r *Registry) serveNodeListWatch(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "watch needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	ctx := req.Context()
	token := req.URL.Query().Get("from")
	ch, err := r.stores.WatchNodeListToken(ctx, token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	r.streamNodeListView(ctx, w, flusher, ch)
}

// ServeScaleLink mounts the scaler-facing scale_link API: node_list WATCH_LIST,
// scaler registration, import source leases, and selector patches.
func (r *Registry) ServeScaleLink(mux *http.ServeMux) {
	mux.HandleFunc(ScaleLinkNodeListWatchPath, r.serveNodeListWatch)
	mux.HandleFunc(ScaleLinkRegisterPath, r.serveScalerRegister)
	mux.HandleFunc(ScaleLinkSelectorPatchPath, r.serveSelectorPatch)
	mux.HandleFunc(ScaleLinkImportSourcePath, r.serveImportSourceLease)
	mux.HandleFunc(ScaleLinkSourceCursorPath, r.serveImportSourceCursor)
}

func (r *Registry) serveScalerRegister(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in ScalerRegister
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in.ID == "" || in.Advertise == "" {
		http.Error(w, "id and advertise are required", http.StatusBadRequest)
		return
	}
	label := in.MemberlistLabel
	if label == "" {
		label = r.scalerMemberlistLabel()
	}
	if want := r.scalerMemberlistLabel(); want != "" && label != want {
		http.Error(w, "memberlist_label does not match registry scale_link.scaler_label", http.StatusConflict)
		return
	}
	if err := r.joinScalerSeed(req.Context(), in.ID, label, in.Advertise); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Registry) serveSelectorPatch(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var patch routesync.SelectorPatch
	if err := json.NewDecoder(io.LimitReader(req.Body, 4<<20)).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if patch.Group == "" {
		http.Error(w, "group is required", http.StatusBadRequest)
		return
	}
	if err := r.applySelectorPatch(req.Context(), &patch); err != nil {
		if errors.Is(err, errMissingImportSourceLease) {
			http.Error(w, "import source lease fields are required", http.StatusBadRequest)
			return
		}
		if errors.Is(err, errStaleImportSourceLease) {
			http.Error(w, "stale import source lease", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Registry) serveImportSourceLease(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in ImportSourceLeaseRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in.SourceID == "" || in.OwnerID == "" || in.RunID == "" {
		http.Error(w, "source_id, owner_id and run_id are required", http.StatusBadRequest)
		return
	}
	resp, err := r.acquireImportSourceLease(req.Context(), in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (r *Registry) serveImportSourceCursor(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in ImportSourceCursorRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in.SourceID == "" || in.OwnerID == "" || in.RunID == "" || in.Term == 0 {
		http.Error(w, "source_id, owner_id, run_id and term are required", http.StatusBadRequest)
		return
	}
	rec, err := r.stores.CheckpointScaleLinkSource(req.Context(), in.SourceID, in.OwnerID, in.RunID, in.Term, in.Cursor, in.Complete, in.Error)
	if err != nil {
		if errors.Is(err, errScaleLinkStaleLease) {
			http.Error(w, "stale import source lease", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(importSourceLeaseFromState(rec))
}

func (r *Registry) acquireImportSourceLease(ctx context.Context, in ImportSourceLeaseRequest) (ImportSourceLeaseResponse, error) {
	ttl := time.Duration(in.TTLMillis) * time.Millisecond
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	rec, acquired, err := r.stores.AcquireScaleLinkSourceLease(ctx, in.SourceID, in.OwnerID, in.RunID, ttl)
	if err != nil {
		return ImportSourceLeaseResponse{}, err
	}
	return ImportSourceLeaseResponse{Acquired: acquired, Lease: importSourceLeaseFromState(rec)}, nil
}

func importSourceLeaseFromState(rec clusterstate.ScaleImportSourceState) ImportSourceLease {
	return ImportSourceLease{
		SourceID: rec.SourceID, OwnerID: rec.OwnerID, RunID: rec.RunID, Term: rec.Term,
		Cursor: rec.Cursor, Round: rec.Round,
		NextRunUnixMs: rec.NextRunUnixMs, LastError: rec.LastError, ExpiresUnixMs: rec.ExpiresUnixMs,
	}
}

func (r *Registry) streamNodeListView(ctx context.Context, w io.Writer, flusher http.Flusher, ch <-chan WatchEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			var ve *ViewEvent
			switch ev.Type {
			case WatchEventReset:
				ve = &ViewEvent{Type: "reset", Rev: ev.Rev, Token: ev.Token}
			case WatchEventPut:
				ve = &ViewEvent{Type: "put", Key: ev.Key, Value: ev.Value, Rev: ev.Rev, Token: ev.Token}
			case WatchEventDelete:
				ve = &ViewEvent{Type: "delete", Key: ev.Key, Rev: ev.Rev, Token: ev.Token}
			case WatchEventBookmark:
				ve = &ViewEvent{Type: "bookmark", Rev: ev.Rev, Token: ev.Token}
			default:
				continue
			}
			if err := writeFrame(w, ve); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
