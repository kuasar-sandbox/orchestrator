package registry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
)

// Node/group view watch (cluster.md §5.2): a standalone scaler subscribes these to
// mirror the node table + group placement config locally, exactly as the router
// subscribes /control/watch for routes. Same reset+snapshot+bookmark+deltas protocol
// (viewStream), resumable by from_rev. Group values are projected to strip the
// sealed manifest_key — the scaler has no business with keys.
const (
	ControlNodeWatchPath  = "/control/watch-nodes"
	ControlGroupWatchPath = "/control/watch-groups"
)

// ViewEvent is one frame on a view watch ([4B LE len][ViewEvent]); Value is the
// raw record JSON (a NodeRecord, or a key-stripped group projection).
type ViewEvent struct {
	Type  string          `json:"type"` // "reset" | "put" | "delete" | "bookmark"
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Rev   int64           `json:"rev,omitempty"`
}

// GroupView is the placement-only projection of a group sent to subscribers (no
// manifest_key / project_id — those never leave the registry/key-provider).
type GroupView struct {
	Group         string              `json:"group"`
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

func (r *Registry) serveNodeWatch(w http.ResponseWriter, req *http.Request) {
	r.viewStream(w, req, nodePrefix, func(_ string, raw []byte) ([]byte, bool) { return raw, true })
}

func (r *Registry) serveGroupWatch(w http.ResponseWriter, req *http.Request) {
	r.viewStream(w, req, groupPrefix, projectGroupView)
}

// projectGroupView strips the sealed manifest_key + project_id before a group goes
// to a subscriber (the scaler needs only placement labels/selectors).
func projectGroupView(_ string, raw []byte) ([]byte, bool) {
	var g GroupConfig
	if json.Unmarshal(raw, &g) != nil {
		return nil, false
	}
	out, err := json.Marshal(GroupView{Group: g.Group, NodeSelectors: g.NodeSelectors, ShuffleLabels: g.ShuffleLabels})
	if err != nil {
		return nil, false
	}
	return out, true
}

// viewStream serves the reset+snapshot+bookmark+deltas watch protocol over a store
// prefix (the same shape as serveWatch, generalized for raw records). project maps
// a stored value to the wire value (ok=false skips it).
func (r *Registry) viewStream(w http.ResponseWriter, req *http.Request, prefix string, project func(key string, raw []byte) ([]byte, bool)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "watch needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	ctx := req.Context()
	var fromRev int64
	if v := req.URL.Query().Get("from_rev"); v != "" {
		fromRev, _ = strconv.ParseInt(v, 10, 64)
	}
	// Resumable replay after a known rev; on compaction fall back to a snapshot.
	if fromRev > 0 {
		ch, err := r.stores.kv.Watch(ctx, prefix, fromRev)
		if err == nil {
			r.streamView(ctx, w, flusher, ch, prefix, project)
			return
		}
		if err != clusterstore.ErrCompacted {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	rev0, err := r.stores.kv.Rev(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch, err := r.stores.kv.Watch(ctx, prefix, rev0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := writeFrame(w, &ViewEvent{Type: "reset", Rev: rev0}); err != nil {
		return
	}
	if err := r.stores.kv.Range(ctx, prefix, func(kv clusterstore.KV) error {
		val, ok := project(kv.Key, kv.Value)
		if !ok {
			return nil
		}
		// Emit the logical id (prefix stripped) so the subscriber keys by node_id /
		// group, decoupled from the store-key scheme.
		return writeFrame(w, &ViewEvent{Type: "put", Key: strings.TrimPrefix(kv.Key, prefix), Value: val, Rev: rev0})
	}); err != nil {
		return
	}
	if err := writeFrame(w, &ViewEvent{Type: "bookmark", Rev: rev0}); err != nil {
		return
	}
	flusher.Flush()
	r.streamView(ctx, w, flusher, ch, prefix, project)
}

func (r *Registry) streamView(ctx context.Context, w io.Writer, flusher http.Flusher, ch <-chan clusterstore.Event, prefix string, project func(string, []byte) ([]byte, bool)) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			key := strings.TrimPrefix(ev.Key, prefix)
			var ve *ViewEvent
			switch ev.Type {
			case clusterstore.EventPut:
				val, pok := project(ev.Key, ev.Value)
				if !pok {
					continue
				}
				ve = &ViewEvent{Type: "put", Key: key, Value: val, Rev: ev.Rev}
			case clusterstore.EventDelete:
				ve = &ViewEvent{Type: "delete", Key: key, Rev: ev.Rev}
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
