package routesync

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

// Source is the orchestrator-side route authority the stream distributes. The
// orchestrator (internal/orch) implements it.
type Source interface {
	// Range streams the full current route set (running + paused sandboxes) one
	// entry at a time through fn, in id order. Streaming (vs returning a slice)
	// keeps send-side memory bounded at high sandbox density. fn errors abort.
	Range(ctx context.Context, fn func(RouteEntry) error) error
	// Subscribe registers for route-change events. The returned channel is closed
	// by the source if it falls behind (the subscriber then reconnects + re-syncs);
	// cancel unregisters it.
	Subscribe() (ch <-chan Event, cancel func())
	// OnWake handles a subscriber's request to resume a sandbox (single-flight; the
	// resulting Upsert is delivered via Subscribe).
	OnWake(ctx context.Context, sid string)
	// Policy is the operational policy pushed to subscribers at handshake.
	Policy() Policy
}

// ReadRegister reads the subscriber's first up-frame (its Register caps). The
// config-socket plugin handler calls this before ServeStream so it can register the
// subscriber (and its proxy target) before streaming.
func ReadRegister(r io.Reader) (Register, error) {
	m, err := readMsg(r)
	if err != nil {
		return Register{}, err
	}
	if m.Type != TypeRegister || m.Register == nil {
		return Register{}, errors.New("routesync: expected register frame")
	}
	return *m.Register, nil
}

// ServeStream runs the orchestrator side of one subscriber's route stream on an
// already-accepted h2c request whose Register frame has already been read (reg). It
// writes Hello(Policy) and — when reg subscribes — the initial route set as Upserts,
// a Bookmark, then live deltas, to w; concurrently it reads Wake frames from body
// (acting on them only for route_wake). It returns when the connection drops (body
// EOF) or ctx is cancelled — which the caller treats as deregistration.
func ServeStream(ctx context.Context, w http.ResponseWriter, body io.Reader, src Source, reg Register, log *slog.Logger) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "plugin stream needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reader: drain up-frames (Wakes) until EOF/error. EOF == the subscriber
	// disconnected, so cancel to end the writer and return (-> deregister).
	go func() {
		defer cancel()
		for {
			m, err := readMsg(body)
			if err != nil {
				if sctx.Err() == nil {
					log.Debug("routesync: subscriber read end", "err", err)
				}
				return
			}
			if m.Type == TypeWake && m.SID != "" && reg.handlesWake() {
				// A wake's resume can be slow; handle it off the read loop
				// (single-flight in the source dedupes duplicate sids).
				go src.OnWake(sctx, m.SID)
			}
		}
	}()

	// Writer: handshake policy first (flush so the subscriber's RoundTrip returns).
	if err := writeMsg(w, &Msg{Type: TypeHello, Hello: &Hello{Version: Version, Policy: src.Policy()}}); err != nil {
		return
	}
	flusher.Flush()

	if !reg.subscribes() {
		// Lease only (no route stream): hold the connection open until disconnect.
		<-sctx.Done()
		return
	}

	// Subscribe before the range so events racing the initial stream are buffered
	// and replayed as idempotent upserts.
	ch, cancelSub := src.Subscribe()
	defer cancelSub()

	if err := src.Range(sctx, func(r RouteEntry) error {
		return writeMsg(w, &Msg{Type: TypeUpsert, Route: &r})
	}); err != nil {
		return
	}
	if err := writeMsg(w, &Msg{Type: TypeBookmark}); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-sctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return // lagged + dropped by the source; the subscriber reconnects + re-syncs
			}
			m := &Msg{Type: ev.Kind}
			switch ev.Kind {
			case TypeUpsert:
				r := ev.Route
				m.Route = &r
			case TypeDelete:
				m.SID = ev.SID
			default:
				continue
			}
			if err := writeMsg(w, m); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
