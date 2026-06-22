package registry

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
)

// ControlWatchPath streams resolved sandbox route changes to the router's local cache
// (cluster.md §5.2/§5.3): GET ?from_rev=N. from_rev<=0 (or a compacted from_rev>0)
// → a reset + a snapshot (every current route as a put) + a bookmark + live deltas;
// a live from_rev>0 → replay the change-log strictly after N (no reset/snapshot),
// then live deltas. The reset tells the subscriber to rebuild vs keep its cache.
// Frames are length-prefixed JSON ([4B LE len][WatchEvent]).
const ControlWatchPath = "/control/watch"

// WatchEvent is one frame on the watch stream.
type WatchEvent struct {
	Type  string        `json:"type"` // "reset" | "put" | "delete" | "bookmark"
	Key   string        `json:"key,omitempty"`
	Route *RouteResolve `json:"route,omitempty"` // put: the resolved data-plane target (incl sid)
	Rev   int64         `json:"rev,omitempty"`
}

func (r *Registry) serveWatch(w http.ResponseWriter, req *http.Request) {
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
		ch, err := r.stores.kv.Watch(ctx, sandboxPrefix, fromRev)
		if err == nil {
			r.streamWatch(ctx, w, flusher, ch)
			return
		}
		if err != clusterstore.ErrCompacted {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Snapshot at rev0, watching deltas strictly after it (Watch started first so
	// no mutation between the snapshot and the live stream is lost).
	rev0, err := r.stores.kv.Rev(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch, err := r.stores.kv.Watch(ctx, sandboxPrefix, rev0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// A snapshot follows: tell the subscriber to discard its cache and rebuild from
	// the puts up to the bookmark. A resume (the from_rev>0 path above) sends no
	// reset, so the subscriber keeps its cache and applies the deltas live.
	if err := writeWatchFrame(w, &WatchEvent{Type: "reset", Rev: rev0}); err != nil {
		return
	}
	// Preload node endpoints once: a GetNode per snapshot row would be O(N) sqlite
	// reads at high density. Live deltas below resolve per-event (infrequent).
	nodeEP := map[string]string{}
	_ = r.stores.RangeNodes(ctx, func(n *NodeRecord) error { nodeEP[n.NodeID] = n.DataEndpoint; return nil })
	if err := r.stores.RangeAllSandboxes(ctx, func(rec *SandboxRecord) error {
		return writeWatchFrame(w, &WatchEvent{Type: "put", Key: sandboxKey(rec.Group, rec.RouteKey), Route: resolveRecordEP(rec, nodeEP[rec.NodeID]), Rev: rev0})
	}); err != nil {
		return
	}
	if err := writeWatchFrame(w, &WatchEvent{Type: "bookmark", Rev: rev0}); err != nil {
		return
	}
	flusher.Flush()
	r.streamWatch(ctx, w, flusher, ch)
}

func (r *Registry) streamWatch(ctx context.Context, w io.Writer, flusher http.Flusher, ch <-chan clusterstore.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return // watcher fell behind / ctx done — the client re-snapshots
			}
			var we *WatchEvent
			switch ev.Type {
			case clusterstore.EventPut:
				var rec SandboxRecord
				if json.Unmarshal(ev.Value, &rec) != nil {
					continue
				}
				we = &WatchEvent{Type: "put", Key: ev.Key, Route: r.resolveRecord(ctx, &rec), Rev: ev.Rev}
			case clusterstore.EventDelete:
				we = &WatchEvent{Type: "delete", Key: ev.Key, Rev: ev.Rev}
			default:
				continue
			}
			if err := writeWatchFrame(w, we); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (r *Registry) resolveRecord(ctx context.Context, rec *SandboxRecord) *RouteResolve {
	return resolveRecordEP(rec, r.nodeDataEndpoint(ctx, rec.NodeID))
}

func resolveRecordEP(rec *SandboxRecord, dataEndpoint string) *RouteResolve {
	return &RouteResolve{
		SID: rec.SID, Group: rec.Group, RouteKey: rec.RouteKey, NodeID: rec.NodeID,
		DataEndpoint: dataEndpoint, AccessToken: rec.AccessToken,
		State: string(rec.State),
	}
}

func writeWatchFrame(w io.Writer, ev *WatchEvent) error { return writeFrame(w, ev) }

// writeFrame writes a length-prefixed JSON frame ([4B LE len][JSON]).
func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
