package registry

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Scaler-link (cluster.md §4.3/§5.2): the standalone scaler DIALS the registry
// (no scaler listen) and subscribes the node/group view (the control watches); on this
// link the registry REVERSE-REQUESTS placement — it writes place_req down and the
// scaler answers place_result up. channelPlacer is the registry's Placer over it;
// with no scaler attached, placement returns ErrNoNode (cold placement stalls
// until a scaler connects, §11) — the data plane (router cache) is unaffected.

// scalerConn writes place_req frames down the scaler-link response body (serialized).
type scalerConn struct {
	mu    sync.Mutex
	w     io.Writer
	flush func()
}

func (c *scalerConn) sendPlaceReq(req *routesync.PlaceReq) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := routesync.WriteMsg(c.w, &routesync.Msg{Type: routesync.TypePlaceReq, PlaceReq: req}); err != nil {
		return err
	}
	c.flush()
	return nil
}

// channelPlacer reverse-requests placement from the connected scaler.
type channelPlacer struct {
	r       *Registry
	timeout time.Duration
}

// NewChannelPlacer builds the production Placer: it forwards each placement to the
// scaler over the scaler-link. timeout bounds a placement (<=0 → 5s) so a slow/
// absent scaler can't consume the whole park budget.
func NewChannelPlacer(r *Registry, timeout time.Duration) Placer {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &channelPlacer{r: r, timeout: timeout}
}

func (p *channelPlacer) Place(ctx context.Context, req PlaceRequest) (string, error) {
	conn := p.r.scaler()
	if conn == nil {
		return "", ErrNoNode // no scaler attached → cold placement stalls (§11)
	}
	reqID := newID()
	ch := make(chan *routesync.PlaceResult, 1)
	p.r.registerPlace(reqID, ch)
	defer p.r.unregisterPlace(reqID)
	pr := &routesync.PlaceReq{ReqID: reqID, Group: req.Group, RouteKey: req.RouteKey, Build: req.Build, TargetRuntimeDigest: req.TargetRuntimeDigest}
	if err := conn.sendPlaceReq(pr); err != nil {
		return "", err
	}
	wctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	select {
	case res := <-ch:
		if res.NoNode || res.NodeID == "" {
			return "", ErrNoNode
		}
		return res.NodeID, nil
	case <-wctx.Done():
		return "", wctx.Err()
	}
}

// ServeScalerLink handles the scaler's reverse-call connection: the scaler PUTs,
// the registry writes place_req on the response body and reads place_result +
// selector_patch from the request body. Returns on disconnect.
func (r *Registry) ServeScalerLink(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "scaler-link needs a flushable writer", http.StatusInternalServerError)
		return
	}
	conn := &scalerConn{w: w, flush: flusher.Flush}
	// Hello first (so the scaler knows the link is up before it streams), while this
	// is still the only writer — mirrors node-link.
	if err := routesync.WriteMsg(w, &routesync.Msg{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}}); err != nil {
		return
	}
	flusher.Flush()
	r.setScaler(conn)
	defer r.clearScaler(conn)
	r.log.Info("scaler-link: scaler connected")

	body := req.Body
	for {
		m, err := routesync.ReadMsg(body)
		if err != nil {
			return
		}
		switch m.Type {
		case routesync.TypePlaceResult:
			if m.PlaceResult != nil {
				r.resolvePlace(m.PlaceResult)
			}
		case routesync.TypeSelectorPatch:
			if m.Patch != nil {
				r.applySelectorPatch(m.Patch) // shuffle-effective allocation set (§7.6)
			}
		}
	}
}

// --- active scaler + place-waiter registry ---

func (r *Registry) setScaler(c *scalerConn) {
	r.scalerMu.Lock()
	r.curScaler = c // a second scaler replaces the first (single active scaler)
	r.scalerMu.Unlock()
}

func (r *Registry) clearScaler(c *scalerConn) {
	r.scalerMu.Lock()
	if r.curScaler == c {
		r.curScaler = nil
	}
	r.scalerMu.Unlock()
}

func (r *Registry) scaler() *scalerConn {
	r.scalerMu.Lock()
	defer r.scalerMu.Unlock()
	return r.curScaler
}

func (r *Registry) registerPlace(reqID string, ch chan *routesync.PlaceResult) {
	r.scalerMu.Lock()
	r.placeWaiters[reqID] = ch
	r.scalerMu.Unlock()
}

func (r *Registry) unregisterPlace(reqID string) {
	r.scalerMu.Lock()
	delete(r.placeWaiters, reqID)
	r.scalerMu.Unlock()
}

func (r *Registry) resolvePlace(res *routesync.PlaceResult) {
	r.scalerMu.Lock()
	ch := r.placeWaiters[res.ReqID]
	r.scalerMu.Unlock()
	if ch != nil {
		select {
		case ch <- res:
		default:
		}
	}
}
