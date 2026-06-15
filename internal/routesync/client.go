package routesync

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// Source is the orchestrator-side route authority the client distributes. The
// orchestrator (internal/orch) implements it.
type Source interface {
	// Range streams the full current route set (running + paused sandboxes) one
	// entry at a time through fn, in id order. Streaming (vs returning a slice)
	// keeps send-side memory bounded at high sandbox density. fn errors abort.
	Range(ctx context.Context, fn func(RouteEntry) error) error
	// Subscribe registers for route-change events. The returned channel is closed
	// by the source if it falls behind (the caller then reconnects + re-syncs);
	// cancel unregisters it.
	Subscribe() (ch <-chan Event, cancel func())
	// OnWake handles a proxy's request to resume a sandbox (single-flight; the
	// resulting Upsert is delivered via Subscribe).
	OnWake(ctx context.Context, sid string)
	// Policy is the operational policy pushed to proxies at handshake.
	Policy() Policy
}

// Client dials every proxy worker's UDS and keeps each one's route table in sync
// over a persistent, auto-reconnecting bidi stream.
type Client struct {
	sockets []string
	src     Source
	log     *slog.Logger
}

func NewClient(sockets []string, src Source, log *slog.Logger) *Client {
	return &Client{sockets: sockets, src: src, log: log}
}

// Run maintains one sync session per proxy socket until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, sock := range c.sockets {
		wg.Add(1)
		go func(sock string) {
			defer wg.Done()
			c.runSocket(ctx, sock)
		}(sock)
	}
	wg.Wait()
}

// runSocket reconnects to one proxy with capped backoff for the lifetime of ctx.
func (c *Client) runSocket(ctx context.Context, sock string) {
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}
	defer tr.CloseIdleConnections()
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := c.session(ctx, tr)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("routesync: proxy session ended; reconnecting", "sock", sock, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// session runs one connection: handshake + snapshot + delta push (request body)
// while reading wakes (response body), full-duplex.
func (c *Client) session(ctx context.Context, tr *http2.Transport) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(sctx, http.MethodPost, "http://proxy"+SyncPath, pr)
	if err != nil {
		return err
	}
	req.Header.Set(SyncHeader, "1")

	ch, cancelSub := c.src.Subscribe()
	defer cancelSub()

	// Writer goroutine: Hello -> Upserts -> Bookmark -> deltas. Closing pw ends the request.
	go func() { pw.CloseWithError(c.writeStream(sctx, pw, ch)) }()

	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Reader: wakes from the proxy. A wake's resume can be slow, so handle it off
	// the read loop (single-flight in the source dedupes duplicate sids).
	for {
		m, err := readMsg(resp.Body)
		if err != nil {
			return err
		}
		if m.Type == TypeWake && m.SID != "" {
			go c.src.OnWake(sctx, m.SID)
		}
	}
}

// writeStream sends the handshake, streams the current route set as individual
// Upserts followed by a Bookmark (no materialized all-routes frame), then forwards
// live route events until ctx ends or the subscription is dropped (returns an
// error -> reconnect + re-sync). Subscribing happens before the range (in session),
// so events racing the initial stream are buffered and replayed as idempotent upserts.
func (c *Client) writeStream(ctx context.Context, w io.Writer, ch <-chan Event) error {
	hello := &Msg{Type: TypeHello, Hello: &Hello{Version: Version, Role: "orchestrator", Policy: c.src.Policy()}}
	if err := writeMsg(w, hello); err != nil {
		return err
	}
	if err := c.src.Range(ctx, func(r RouteEntry) error {
		return writeMsg(w, &Msg{Type: TypeUpsert, Route: &r})
	}); err != nil {
		return err
	}
	if err := writeMsg(w, &Msg{Type: TypeBookmark}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return errors.New("routesync: subscription dropped (re-snapshot)")
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
				return err
			}
		}
	}
}
