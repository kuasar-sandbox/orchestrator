package registry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
)

// Node/group view watches are low-frequency control-plane streams for scaler
// views. Router does not subscribe to a global route stream. node_list is projected from
// node_link quorum state; group values are projected to strip sealed secrets.
const (
	ScaleLinkNodeListWatchPath = "/scale-link/watch-node-list" // scaler node_list WATCH_LIST
	ScaleLinkGroupWatchPath    = "/scale-link/watch-groups"
)

// ViewEvent is one frame on a view watch ([4B LE len][ViewEvent]); Value is the
// raw record JSON (a NodeRecord, or a key-stripped group projection).
type ViewEvent struct {
	Type  string          `json:"type"` // "reset" | "put" | "delete" | "bookmark"
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Rev   int64           `json:"rev,omitempty"`
}

// GroupView is the placement-only projection of a group sent to subscribers.
type GroupView struct {
	Group         string              `json:"group"`
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

func (r *Registry) serveNodeListWatch(w http.ResponseWriter, req *http.Request) {
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
	if fromRev > 0 {
		ch, err := r.stores.WatchNodeList(ctx, fromRev)
		if err == nil {
			r.streamNodeListView(ctx, w, flusher, ch)
			return
		}
		if err != clusterstore.ErrCompacted {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	rev0, err := r.stores.NodeListRev(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch, err := r.stores.WatchNodeList(ctx, rev0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := writeFrame(w, &ViewEvent{Type: "reset", Rev: rev0}); err != nil {
		return
	}
	if err := r.stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		val, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		return writeFrame(w, &ViewEvent{Type: "put", Key: entry.NodeID, Value: val, Rev: rev0})
	}); err != nil {
		return
	}
	if err := writeFrame(w, &ViewEvent{Type: "bookmark", Rev: rev0}); err != nil {
		return
	}
	flusher.Flush()
	r.streamNodeListView(ctx, w, flusher, ch)
}

func (r *Registry) serveGroupWatch(w http.ResponseWriter, req *http.Request) {
	r.viewStream(w, req, groupPrefix, projectGroupView)
}

// ServeScaleLink mounts the scaler-facing scale_link API: node_list WATCH_LIST,
// group placement view, and the full-duplex scale_link session (registered by the
// cluster-ctl registry command).
func (r *Registry) ServeScaleLink(mux *http.ServeMux) {
	mux.HandleFunc(ScaleLinkNodeListWatchPath, r.serveNodeListWatch)
	mux.HandleFunc(ScaleLinkGroupWatchPath, r.serveGroupWatch)
}

// projectGroupView strips sealed secrets before a group goes to a subscriber.
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

func (r *Registry) streamNodeListView(ctx context.Context, w io.Writer, flusher http.Flusher, ch <-chan clusterstore.Event) {
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
			case clusterstore.EventPut:
				ve = &ViewEvent{Type: "put", Key: ev.Key, Value: ev.Value, Rev: ev.Rev}
			case clusterstore.EventDelete:
				ve = &ViewEvent{Type: "delete", Key: ev.Key, Rev: ev.Rev}
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

// viewStream serves the reset+snapshot+bookmark+deltas watch protocol over a
// low-frequency store prefix. project maps a stored value to the wire value
// (ok=false skips it).
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
