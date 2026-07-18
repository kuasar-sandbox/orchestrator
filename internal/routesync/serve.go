package routesync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

const maxPriorityOutboxBurst = 32

// Source is the route authority the stream distributes. internal/orch implements
// it for the proxy plane; the cluster node-link (node.md §10) implements it for the
// node's sandbox/build routes.
type Source interface {
	// Range streams the full current route set (running + paused sandboxes) one
	// entry at a time through fn, in id order. Streaming (vs returning a slice)
	// keeps send-side memory bounded at high sandbox density. fn errors abort.
	Range(ctx context.Context, fn func(RouteEntry) error) error
	// Subscribe registers for route-change events. The returned channel is closed
	// by the source if it falls behind (the subscriber then reconnects + re-syncs);
	// cancel unregisters it.
	Subscribe() (ch <-chan Event, cancel func())
	// OnWake handles a subscriber's request to resume one exact execution. The
	// source must reject a stale execution fence before any resume work.
	OnWake(ctx context.Context, wake RouteWake)
	// Policy is the operational policy pushed to subscribers at handshake.
	Policy() Policy
}

// ErrResumeUnavailable tells StreamAuthority to fall back to a full snapshot for
// this subscription. Other replay errors are treated as stream failures.
var ErrResumeUnavailable = errors.New("routesync: resume unavailable")

// ResumableSource is an optional Source extension. If ResumeFrom carries a token
// whose fingerprint matches SourceFingerprint, StreamAuthority replays changes
// strictly after the decoded sequence instead of sending a full Range snapshot.
// A fingerprint mismatch forces a full resync, which is the safety property node
// owner subscriptions need across source restarts.
type ResumableSource interface {
	SourceFingerprint() string
	Replay(ctx context.Context, afterSeq int64, fn func(Event) error) error
}

// RevisionSource is an optional Source extension used to stamp bookmarks with
// the latest opaque resume token for the next incremental subscription.
type RevisionSource interface {
	CurrentRevToken() string
}

// ReadRegister reads the subscriber's first up-frame (its Register caps). The
// config-socket plugin handler calls this before ServeAuthority so it can register
// the subscriber (and its proxy target) before streaming.
func ReadRegister(r io.Reader) (Register, error) {
	m, err := ReadMsg(r)
	if err != nil {
		return Register{}, err
	}
	if m.Type != TypeRegister || m.Register == nil {
		return Register{}, errors.New("routesync: expected register frame")
	}
	if m.Register.Version != Version {
		return Register{}, fmt.Errorf("routesync: protocol version %d is incompatible with required version %d", m.Register.Version, Version)
	}
	return *m.Register, nil
}

// ServeStream runs the proxy/observer route authority on an already-accepted h2c
// request whose Register frame has already been read (reg). It is a thin HTTP
// adapter over ServeAuthority — the proxy plane's only up-frame is a Wake.
func ServeStream(ctx context.Context, w http.ResponseWriter, body io.Reader, src Source, reg Register, log *slog.Logger) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "plugin stream needs a flushable (h2c) writer", http.StatusInternalServerError)
		return
	}
	onUp := func(uctx context.Context, m *Msg) {
		if m.Type == TypeWake && m.Wake != nil && m.Wake.SandboxID != "" && reg.handlesWake() {
			// A wake's resume can be slow; handle it off the read loop
			// (single-flight in the source dedupes duplicate sids).
			go src.OnWake(uctx, *m.Wake)
		}
	}
	ServeAuthority(ctx, w, flusher.Flush, body, src, reg, onUp, log)
}

// ServeAuthority runs the route-authority side of one bidirectional stream over a
// transport-neutral writer (w + flush) and reader (body), decoupled from HTTP. It
// writes Hello(Policy), then — when reg subscribes — the initial route set as
// Upserts, a Bookmark, and live deltas; concurrently it reads up-frames from body
// and hands each to onUp. It returns when body hits EOF/error or ctx is cancelled.
//
// Two callers drive it: the proxy plane (ServeStream, w = the h2c ResponseWriter,
// onUp = a Wake handler) and the cluster node-link (node.md §10, w = the request
// body of the dialed registry connection — the node is the authority that DIALS,
// onUp = a registry-command dispatcher). The frame codec and this loop are the
// single shared engine; only the transport adapter and onUp differ.
func ServeAuthority(ctx context.Context, w io.Writer, flush func(), body io.Reader, src Source, reg Register, onUp func(context.Context, *Msg), log *slog.Logger) {
	// Handshake: policy first (flush so the peer's RoundTrip returns), then the
	// shared route-stream loop. The cluster node-link sends a NodeRegister frame
	// instead of Hello and calls StreamAuthority directly.
	if err := WriteMsg(w, &Msg{Type: TypeHello, Hello: &Hello{Version: Version, Policy: src.Policy()}}); err != nil {
		return
	}
	flush()
	StreamAuthority(ctx, w, flush, body, src, reg, onUp, nil, log)
}

// StreamAuthority runs the route-stream half of an authority connection WITHOUT
// the handshake frame: it reads up-frames from body (each handed to onUp) and —
// when reg subscribes — streams the initial route set as Upserts, a Bookmark,
// then live deltas to w. It returns when body hits EOF/error or ctx is cancelled.
//
// The proxy plane reaches it via ServeAuthority (after a Hello); the cluster
// node-link reaches it directly after writing its NodeRegister, so the node — the
// route authority that DIALS the registry — reuses the same streaming loop.
//
// outbox (nil for the proxy plane) carries up-frames the up-handler produces —
// the node-link's command acks — so they serialize through this single writer
// alongside the route deltas rather than racing it.
func StreamAuthority(ctx context.Context, w io.Writer, flush func(), body io.Reader, src Source, reg Register, onUp func(context.Context, *Msg), outbox <-chan *Msg, log *slog.Logger) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reader: drain up-frames until EOF/error. EOF == the peer disconnected, so
	// cancel to end the writer and return (-> deregister).
	go func() {
		defer cancel()
		for {
			m, err := ReadMsg(body)
			if err != nil {
				if sctx.Err() == nil {
					log.Debug("routesync: authority read end", "err", err)
				}
				return
			}
			if onUp != nil {
				onUp(sctx, m)
			}
		}
	}()

	if !reg.subscribes() {
		// Lease only (no route stream): hold the connection open until disconnect.
		<-sctx.Done()
		return
	}

	// Subscribe before the range so events racing the initial stream are buffered
	// and replayed as idempotent upserts.
	ch, cancelSub := src.Subscribe()
	defer cancelSub()
	drainOutbox := func() error {
		wrote := false
		for i := 0; i < maxPriorityOutboxBurst; i++ {
			select {
			case m, ok := <-outbox:
				if !ok || m == nil {
					return io.EOF
				}
				if err := WriteMsg(w, m); err != nil {
					return err
				}
				wrote = true
			default:
				if wrote {
					flush()
				}
				return nil
			}
		}
		if wrote {
			flush()
		}
		return nil
	}
	writeRoute := func(ev Event) error {
		if err := drainOutbox(); err != nil {
			return err
		}
		return writeEvent(w, ev)
	}

	resumed := false
	if reg.ResumeFrom != "" {
		if rs, ok := src.(ResumableSource); ok {
			if after, ok := CheckRevToken(reg.ResumeFrom, rs.SourceFingerprint()); ok {
				err := rs.Replay(sctx, after, writeRoute)
				switch {
				case err == nil:
					resumed = true
				case errors.Is(err, ErrResumeUnavailable):
					resumed = false
				default:
					return
				}
			}
		}
	}
	if !resumed {
		if err := src.Range(sctx, func(r RouteEntry) error {
			if err := drainOutbox(); err != nil {
				return err
			}
			return WriteMsg(w, &Msg{Type: TypeUpsert, Route: &r})
		}); err != nil {
			return
		}
	}
	if err := drainOutbox(); err != nil {
		return
	}
	bookmark := &Msg{Type: TypeBookmark, FullSync: !resumed}
	if rev, ok := src.(RevisionSource); ok {
		bookmark.RevToken = rev.CurrentRevToken()
	}
	if err := WriteMsg(w, bookmark); err != nil {
		return
	}
	flush()

	for {
		// Command ACKs and durable execution events have bounded priority, but
		// each pass still writes one ready route delta. This gives both streams
		// deterministic progress under sustained load through the single writer.
		if err := drainOutbox(); err != nil {
			return
		}
		select {
		case ev, ok := <-ch:
			if !ok {
				return // lagged + dropped by the source; the subscriber reconnects + re-syncs
			}
			if err := writeEvent(w, ev); err != nil {
				return
			}
			flush()
			continue
		default:
		}
		select {
		case <-sctx.Done():
			return
		case m, ok := <-outbox:
			if !ok || m == nil {
				return
			}
			if err := WriteMsg(w, m); err != nil {
				return
			}
			flush()
		case ev, ok := <-ch:
			if !ok {
				return // lagged + dropped by the source; the subscriber reconnects + re-syncs
			}
			if err := writeEvent(w, ev); err != nil {
				return
			}
			flush()
		}
	}
}

func writeEvent(w io.Writer, ev Event) error {
	m := &Msg{Type: ev.Kind}
	switch ev.Kind {
	case TypeUpsert:
		r := ev.Route
		m.Route = &r
	case TypeDelete:
		m.SID = ev.SID
	default:
		return nil
	}
	return WriteMsg(w, m)
}
