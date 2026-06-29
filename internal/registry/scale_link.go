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
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Node view watches are low-frequency control-plane streams for scaler views.
// Router does not subscribe to a global route stream. node_list is projected from
// node_link quorum state; sandbox-group data is imported on scaler/provider side.
const (
	ScaleLinkNodeListWatchPath = "/scale-link/watch-node-list" // scaler node_list WATCH_LIST
	ScaleLinkRegisterPath      = "/scale-link/register"
	ScaleLinkSelectorPatchPath = "/scale-link/selector-patch"
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
	ID                  string `json:"id"`
	Advertise           string `json:"advertise"`
	MemberlistLabel     string `json:"memberlist_label"`
	MemberlistAdvertise string `json:"memberlist_advertise"`
}

func (r *Registry) serveNodeListWatch(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "watch needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	ctx := req.Context()
	label := r.nodeListWatchLabel()
	if token := req.URL.Query().Get("from"); token != "" {
		tokenLabel, fromRev, ok := parseNodeListWatchToken(token)
		if ok && tokenLabel == label && fromRev > 0 {
			ch, err := r.stores.WatchNodeList(ctx, fromRev)
			if err == nil {
				r.streamNodeListView(ctx, w, flusher, ch, label)
				return
			}
			if err != clusterstore.ErrCompacted {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
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
	token := makeNodeListWatchToken(label, rev0)
	if err := writeFrame(w, &ViewEvent{Type: "reset", Rev: rev0, Token: token}); err != nil {
		return
	}
	if err := r.stores.RangeNodeList(ctx, func(entry clusterstate.NodeListEntry) error {
		val, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		return writeFrame(w, &ViewEvent{Type: "put", Key: entry.NodeID, Value: val, Rev: rev0, Token: token})
	}); err != nil {
		return
	}
	if err := writeFrame(w, &ViewEvent{Type: "bookmark", Rev: rev0, Token: token}); err != nil {
		return
	}
	flusher.Flush()
	r.streamNodeListView(ctx, w, flusher, ch, label)
}

// ServeScaleLink mounts the scaler-facing scale_link API: node_list WATCH_LIST,
// scaler registration, and selector/key-allocation patches.
func (r *Registry) ServeScaleLink(mux *http.ServeMux) {
	mux.HandleFunc(ScaleLinkNodeListWatchPath, r.serveNodeListWatch)
	mux.HandleFunc(ScaleLinkRegisterPath, r.serveScalerRegister)
	mux.HandleFunc(ScaleLinkSelectorPatchPath, r.serveSelectorPatch)
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
	memberlistAdvertise := in.MemberlistAdvertise
	if memberlistAdvertise == "" {
		memberlistAdvertise = in.Advertise
	}
	if err := r.joinScalerSeed(req.Context(), in.ID, label, memberlistAdvertise); err != nil {
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
	r.applySelectorPatch(&patch)
	w.WriteHeader(http.StatusNoContent)
}

func (r *Registry) streamNodeListView(ctx context.Context, w io.Writer, flusher http.Flusher, ch <-chan clusterstore.Event, label string) {
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
				ve = &ViewEvent{Type: "put", Key: ev.Key, Value: ev.Value, Rev: ev.Rev, Token: makeNodeListWatchToken(label, ev.Rev)}
			case clusterstore.EventDelete:
				ve = &ViewEvent{Type: "delete", Key: ev.Key, Rev: ev.Rev, Token: makeNodeListWatchToken(label, ev.Rev)}
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

func (r *Registry) nodeListWatchLabel() string {
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	return r.scaleReadyLabel
}

func makeNodeListWatchToken(label string, rev int64) string {
	if label == "" || rev <= 0 {
		return ""
	}
	return label + ":" + strconv.FormatInt(rev, 10)
}

func parseNodeListWatchToken(token string) (string, int64, bool) {
	i := strings.LastIndexByte(token, ':')
	if i <= 0 || i == len(token)-1 {
		return "", 0, false
	}
	rev, err := strconv.ParseInt(token[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return token[:i], rev, true
}
