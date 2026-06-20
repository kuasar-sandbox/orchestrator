// Package nodelink is the node side of the cluster node-link channel (node.md
// §10): a node-ctl serve dials the registry, registers its identity, then — as
// the route authority — streams its sandbox routes up while executing the
// registry's lifecycle/key commands. It reuses the routesync engine (the frame
// codec + StreamAuthority): the only node-link-specific bits are sending a
// NodeRegister frame instead of Hello and dispatching commands instead of wakes.
package nodelink

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Node is what the node-link client needs from the node's orchestrator: a route
// Source for the node's sandboxes (RouteEntry with Group/RouteKey set) and
// execution of registry commands. internal/orch implements it.
type Node interface {
	routesync.Source
	// HandleCommand executes a registry lifecycle / key command (create / connect
	// / delete / key_*) and returns a receipt ack (accepted, or rejected on a
	// precondition failure). Slow work (a boot/resume) runs asynchronously and the
	// terminal sandbox state is reported on the route stream; the ack only confirms
	// receipt + that synchronous preconditions (key installed, template valid) held.
	HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck
	// Heartbeat is the node's current water level (live sandbox count, drain),
	// sent periodically so the registry tracks liveness (the dead-node sweep) and
	// placement headroom (cluster.md §5.1 / §11).
	Heartbeat() *routesync.Heartbeat
}

// Client is a node's node-link client: it dials the registry, registers the
// node's identity, then streams its sandbox routes while executing registry
// commands, reconnecting with capped backoff.
type Client struct {
	dial      func(ctx context.Context) (net.Conn, error)
	identity  routesync.NodeRegister
	node      Node
	heartbeat time.Duration
	tlsConfig *tls.Config // non-nil = dial the registry over (m)TLS instead of h2c
	log       *slog.Logger
}

// New builds a Client. dial returns a fresh connection to the registry's
// node-link listener (a TCP or mTLS dial); heartbeat is the period between node
// heartbeats (<=0 → 10s).
func New(dial func(ctx context.Context) (net.Conn, error), identity routesync.NodeRegister, node Node, heartbeat time.Duration, tlsConfig *tls.Config, log *slog.Logger) *Client {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	return &Client{dial: dial, identity: identity, node: node, heartbeat: heartbeat, tlsConfig: tlsConfig, log: log}
}

// Run keeps a single node-link session alive, reconnecting with capped backoff
// until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	tr := &http2.Transport{}
	if c.tlsConfig != nil {
		// mTLS: dial plain, then complete a TLS handshake with the node's client
		// cert (the registry verifies it; SAN/fingerprint backs node_id, §5.4).
		tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			conn, err := c.dial(ctx)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(conn, c.tlsConfig)
			if err := tc.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tc, nil
		}
	} else {
		tr.AllowHTTP = true
		tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return c.dial(ctx)
		}
	}
	defer tr.CloseIdleConnections()
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := c.session(ctx, tr)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("node-link: session ended; reconnecting", "node", c.identity.NodeID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// session runs one node-link registration: it PUTs the node-link stream (request
// body = NodeRegister then the route stream) and reads the down stream (response
// body = Hello then commands), full-duplex over h2c. The node is the authority,
// so it WRITES routes and READS commands (the inverse of a proxy subscriber).
func (c *Client) session(ctx context.Context, tr *http2.Transport) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pr, pw := io.Pipe()
	defer pw.Close()

	scheme := "http"
	if c.tlsConfig != nil {
		scheme = "https"
	}
	req, err := http.NewRequestWithContext(sctx, http.MethodPut, scheme+"://registry"+routesync.NodeLinkPath, pr)
	if err != nil {
		return err
	}

	// Write NodeRegister concurrently so RoundTrip can start consuming the
	// request body; it returns once the registry has read it and replied.
	regErr := make(chan error, 1)
	go func() {
		regErr <- routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &c.identity})
	}()

	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := <-regErr; err != nil {
		return err
	}
	// Registry's Hello ack (the node ignores its policy).
	if _, err := routesync.ReadMsg(resp.Body); err != nil {
		return err
	}

	// Stream the node's routes (to pw) while executing commands (from resp.Body),
	// reusing the shared authority loop. Subscribe(kind=registry) makes the loop
	// stream routes; onUp dispatches commands.
	reg := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRegistry}}
	outbox := make(chan *routesync.Msg, 32)
	onUp := func(uctx context.Context, m *routesync.Msg) {
		if m.Type == routesync.TypeCommand && m.Cmd != nil {
			if ack := c.node.HandleCommand(uctx, m.Cmd); ack != nil {
				select {
				case outbox <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: ack}:
				case <-uctx.Done():
				}
			}
		}
	}
	// Periodic heartbeat (node -> registry): liveness for the dead-node sweep +
	// water level for placement (cluster.md §5.1 / §11), serialized via the outbox.
	go func() {
		t := time.NewTicker(c.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				hb := c.node.Heartbeat()
				if hb == nil {
					continue
				}
				select {
				case outbox <- &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: hb}:
				case <-sctx.Done():
					return
				}
			}
		}
	}()
	routesync.StreamAuthority(sctx, pw, func() {}, resp.Body, c.node, reg, onUp, outbox, c.log)
	return sctx.Err()
}
