package routesync

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

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
	// OnWake handles a subscriber's request to resume a sandbox (single-flight; the
	// resulting Upsert is delivered via Subscribe).
	OnWake(ctx context.Context, sid string)
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

// MmdsEvent is an MMDS endpoint change the source publishes to a live
// MMDS subscription — parallel to Event, for the independent endpoint sync
// family.
type MmdsEvent struct {
	Kind  string            // TypeMmdsUpsert | TypeMmdsDelete
	Entry MmdsEndpointEntry // upsert
	Key   MmdsEndpointKey   // delete
}

// MmdsSource is an optional Source extension providing MMDS endpoint sync,
// streamed parallel to (but independently generationed from) the route
// family above. internal/orch implements it when MMDS endpoints are
// enabled. StreamAuthority only engages it when the subscriber's
// Register.MmdsEndpoints is true AND src implements this interface —
// absence of either is not an error, it just means no MMDS sync frames are
// ever sent on this connection: there is no downgrade path, it simply keeps
// configurable endpoints unavailable.
type MmdsSource interface {
	// MmdsGeneration returns a fresh generation stamp identifying the full
	// scan RangeMmds is about to perform. Independent of the route family's
	// Rev/RevToken (do not conflate the two sync streams).
	MmdsGeneration() string
	// RangeMmds streams every currently-declared endpoint (with its current
	// secret plaintext, if any) through fn. Streaming keeps send-side memory
	// bounded, matching Source.Range's own rationale.
	RangeMmds(ctx context.Context, fn func(MmdsEndpointEntry) error) error
	// SubscribeMmds registers for live endpoint changes, parallel to
	// Source.Subscribe. The returned channel is closed if the source falls
	// behind; the subscriber then reconnects and does a fresh full sync.
	SubscribeMmds() (ch <-chan MmdsEvent, cancel func())
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
		if m.Type == TypeWake && m.SID != "" && reg.handlesWake() {
			// A wake's resume can be slow; handle it off the read loop
			// (single-flight in the source dedupes duplicate sids).
			go src.OnWake(uctx, m.SID)
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

	resumed := false
	if reg.ResumeFrom != "" {
		if rs, ok := src.(ResumableSource); ok {
			if after, ok := CheckRevToken(reg.ResumeFrom, rs.SourceFingerprint()); ok {
				err := rs.Replay(sctx, after, func(ev Event) error {
					return writeEvent(w, ev)
				})
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
			return WriteMsg(w, &Msg{Type: TypeUpsert, Route: &r})
		}); err != nil {
			return
		}
	}
	bookmark := &Msg{Type: TypeBookmark, FullSync: !resumed}
	if rev, ok := src.(RevisionSource); ok {
		bookmark.RevToken = rev.CurrentRevToken()
	}
	if err := WriteMsg(w, bookmark); err != nil {
		return
	}
	flush()

	// MMDS endpoint sync — independent full+incremental stream multiplexed
	// onto this same connection, engaged only when both
	// the subscriber asked for it and src supports it. mmdsCh stays nil
	// (never selected) otherwise, so the main loop below needs no separate
	// branch for "MMDS not active".
	var mmdsCh <-chan MmdsEvent
	if reg.MmdsEndpoints {
		if msrc, ok := src.(MmdsSource); ok {
			// Subscribe before the scan so changes racing the scan are
			// replayed after this generation's bookmark, not lost — the
			// authority subscribes to live changes before scanning one
			// consistent database snapshot.
			var cancelMmdsSub func()
			mmdsCh, cancelMmdsSub = msrc.SubscribeMmds()
			defer cancelMmdsSub()

			generation := msrc.MmdsGeneration()
			if err := WriteMsg(w, &Msg{Type: TypeMmdsSyncBegin, MmdsGeneration: generation}); err != nil {
				return
			}
			if err := msrc.RangeMmds(sctx, func(e MmdsEndpointEntry) error {
				entry := e
				return WriteMsg(w, &Msg{Type: TypeMmdsUpsert, MmdsEntry: &entry})
			}); err != nil {
				return
			}
			if err := WriteMsg(w, &Msg{Type: TypeMmdsBookmark, MmdsGeneration: generation}); err != nil {
				return
			}
			flush()
		}
	}

	for {
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
		case ev, ok := <-mmdsCh:
			if !ok {
				return // lagged + dropped by the source; the subscriber reconnects + re-syncs
			}
			if err := writeMmdsEvent(w, ev); err != nil {
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
		case ev, ok := <-mmdsCh:
			if !ok {
				return // lagged + dropped by the source; the subscriber reconnects + re-syncs
			}
			if err := writeMmdsEvent(w, ev); err != nil {
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

func writeMmdsEvent(w io.Writer, ev MmdsEvent) error {
	m := &Msg{Type: ev.Kind}
	switch ev.Kind {
	case TypeMmdsUpsert:
		e := ev.Entry
		m.MmdsEntry = &e
	case TypeMmdsDelete:
		k := ev.Key
		m.MmdsKey = &k
	default:
		return nil
	}
	return WriteMsg(w, m)
}
