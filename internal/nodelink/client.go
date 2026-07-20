// Package nodelink is the node side of the cluster node-link channel (node.md
// §10): a node-ctl conductor serve dials the registry, registers its identity, then — as
// the route authority — streams its sandbox routes up while executing the
// registry's lifecycle/key commands. It reuses the routesync engine (the frame
// codec + StreamAuthority): the only node-link-specific bits are sending a
// NodeRegister frame instead of Hello and dispatching commands instead of wakes.
package nodelink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Node is what the node-link client needs from the node's orchestrator: a route
// source for the node's sandboxes and execution of registry commands.
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
	// placement headroom (cluster.md / §11).
	Heartbeat() *routesync.Heartbeat
	// BuildEvents streams the node's build state transitions (registered/building/
	// ready/error) up to the registry, which converges the BuildStore (§5.1/§7.5).
	BuildEvents() <-chan *routesync.BuildEvent
}

type nodeLinkObserver interface {
	NodeLinkSession(endpoint string)
	NodeLinkRedirect(target routesync.NodeLinkTarget)
}

// SessionSequencer durably advances SessionSeq before every connection attempt.
// Implementations must fail closed rather than reuse a tuple after persistence
// ambiguity.
type SessionSequencer interface {
	NextSession(ctx context.Context) (routesync.SessionTuple, error)
}

// Client is a node's node-link client: it dials the registry, registers the
// node's identity, then streams its sandbox routes while executing registry
// commands, reconnecting with capped backoff.
type Client struct {
	dial          func(ctx context.Context) (net.Conn, error)
	dialEndpoint  func(ctx context.Context, endpoint string) (net.Conn, error)
	endpoint      string
	allowRedirect bool
	identity      routesync.NodeRegister
	sequencer     SessionSequencer
	node          Node
	heartbeat     time.Duration
	tlsConfig     *tls.Config // non-nil = dial the registry over (m)TLS instead of h2c
	log           *slog.Logger
}

// SetSessionSequencer installs the durable tuple source used by final cluster
// mode. It must be configured before Run starts.
func (c *Client) SetSessionSequencer(sequencer SessionSequencer) {
	c.sequencer = sequencer
}

// New builds a Client. dial returns a fresh connection to the registry's
// node-link listener (a TCP or mTLS dial); heartbeat is the period between node
// heartbeats (<=0 → 10s).
func New(dial func(ctx context.Context) (net.Conn, error), identity routesync.NodeRegister, node Node, heartbeat time.Duration, tlsConfig *tls.Config, log *slog.Logger) *Client {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{dial: dial, identity: identity, node: node, heartbeat: heartbeat, tlsConfig: tlsConfig, log: log}
}

// NewWithEndpoint builds a Client that knows the registry endpoint it is dialing.
// When allowRedirect is true the node advertises redirect support and will
// reconnect to owner endpoints returned by the registry.
func NewWithEndpoint(endpoint string, dial func(ctx context.Context, endpoint string) (net.Conn, error), identity routesync.NodeRegister, node Node, heartbeat time.Duration, tlsConfig *tls.Config, log *slog.Logger, allowRedirect bool) *Client {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	identity.AcceptRedirect = allowRedirect
	return &Client{
		dialEndpoint: dial, endpoint: endpoint, allowRedirect: allowRedirect,
		identity: identity, node: node, heartbeat: heartbeat, tlsConfig: tlsConfig, log: log,
	}
}

// Run keeps a single node-link session alive, reconnecting with capped backoff
// until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := 200 * time.Millisecond
	endpoint := c.endpoint
	var redirectTargets []routesync.NodeLinkTarget
	redirectIndex := 0
	for ctx.Err() == nil {
		err := c.session(ctx, endpoint)
		if ctx.Err() != nil {
			return
		}
		var redir nodeLinkRedirect
		if errors.As(err, &redir) && c.allowRedirect && c.dialEndpoint != nil {
			redirectTargets = redir.targets
			redirectIndex = 0
			endpoint = redirectTargets[0].Endpoint
			c.notifyRedirect(redirectTargets[0])
			c.log.Info("node-link: redirecting to node owner", "node", c.identity.NodeID, "member", redirectTargets[0].MemberID, "endpoint", endpoint)
			backoff = 200 * time.Millisecond
			continue
		}
		if c.allowRedirect && len(redirectTargets) > 0 && redirectIndex+1 < len(redirectTargets) {
			redirectIndex++
			endpoint = redirectTargets[redirectIndex].Endpoint
			c.notifyRedirect(redirectTargets[redirectIndex])
			c.log.Warn("node-link: trying next redirected owner", "node", c.identity.NodeID, "member", redirectTargets[redirectIndex].MemberID, "endpoint", endpoint, "err", err)
			continue
		}
		if c.allowRedirect && endpoint != c.endpoint {
			redirectTargets = nil
			redirectIndex = 0
			endpoint = c.endpoint
		}
		c.log.Warn("node-link: session ended; reconnecting", "node", c.identity.NodeID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(fullJitter(backoff)):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// session runs one node-link registration: it PUTs the node-link stream (request
// body = NodeRegister then the route stream) and reads the down stream (response
// body = Hello then commands), full-duplex over h2c. The node is the authority,
// so it WRITES routes and READS commands (the inverse of a proxy subscriber).
func (c *Client) session(ctx context.Context, endpoint string) error {
	identity := c.identity
	identity.Version = routesync.Version
	if c.sequencer != nil {
		tuple, err := c.sequencer.NextSession(ctx)
		if err != nil {
			return fmt.Errorf("node-link: persist session tuple: %w", err)
		}
		identity.NodeEpoch = tuple.NodeEpoch
		identity.SessionSeq = tuple.SessionSeq
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tr, scheme, host, err := c.transport(endpoint)
	if err != nil {
		return err
	}
	defer tr.CloseIdleConnections()
	pr, pw := io.Pipe()
	defer pw.Close()

	req, err := http.NewRequestWithContext(sctx, http.MethodPut, scheme+"://"+host+routesync.NodeLinkPath, pr)
	if err != nil {
		return err
	}

	// Write NodeRegister concurrently so RoundTrip can start consuming the
	// request body; it returns once the registry has read it and replied.
	regErr := make(chan error, 1)
	go func() {
		regErr <- routesync.WriteMsg(pw, &routesync.Msg{Type: routesync.TypeNodeRegister, NodeReg: &identity})
	}()

	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := <-regErr; err != nil {
		return err
	}
	// Registry's Hello ack may carry the node owner's resume token. Empty means
	// full resync; a fingerprint mismatch inside StreamAuthority also falls back
	// to full resync.
	hello, err := routesync.ReadMsg(resp.Body)
	if err != nil {
		return err
	}
	if hello.Type != routesync.TypeHello || hello.Hello == nil || hello.Hello.Version != routesync.Version {
		return fmt.Errorf("node-link: incompatible registry protocol version")
	}
	if redir := nodeLinkRedirectFromHello(hello); len(redir.targets) > 0 {
		return redir
	}
	c.notifySession(endpoint)

	// Stream the node's routes (to pw) while executing commands (from resp.Body),
	// reusing the shared authority loop. Subscribe(kind=registry) makes the loop
	// stream routes; onUp dispatches commands.
	reg := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRegistry}}
	reg.ResumeFrom = hello.Hello.ResumeFrom
	outbox := make(chan *routesync.Msg)
	highOut := make(chan *routesync.Msg, 32)
	hbUpdate := make(chan struct{}, 1)
	var hbMu sync.Mutex
	var latestHeartbeat *routesync.Msg
	go runNodeLinkOutbox(sctx, outbox, highOut, hbUpdate, &hbMu, &latestHeartbeat)
	onUp := func(uctx context.Context, m *routesync.Msg) {
		if m.Type == routesync.TypeCommand && m.Cmd != nil {
			if !commandMatchesSession(m.Cmd, identity) {
				select {
				case highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: &routesync.CmdAck{
					CmdID: m.Cmd.CmdID, Status: routesync.AckRejected,
					Outcome: routesync.DispatchSessionMoved, Reason: "command session tuple is stale",
				}}:
				case <-uctx.Done():
				}
				return
			}
			if ack := c.node.HandleCommand(uctx, m.Cmd); ack != nil {
				select {
				case highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: ack}:
				case <-uctx.Done():
				}
			}
		}
	}
	// Periodic heartbeat (node -> registry): liveness for the dead-node sweep +
	// water level for placement (cluster.md / §11), serialized via the outbox.
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
				hbMu.Lock()
				latestHeartbeat = &routesync.Msg{Type: routesync.TypeHeartbeat, Beat: hb}
				hbMu.Unlock()
				select {
				case hbUpdate <- struct{}{}:
				default:
				}
			}
		}
	}()
	// Build events (node -> registry): the BuildStore converges from these + releases
	// reserved resources on a terminal state (cluster.md / §7.5).
	go func() {
		evs := c.node.BuildEvents()
		if evs == nil {
			return
		}
		for {
			select {
			case <-sctx.Done():
				return
			case ev := <-evs:
				if ev == nil {
					continue
				}
				select {
				case highOut <- &routesync.Msg{Type: routesync.TypeBuildEvent, Build: ev}:
				case <-sctx.Done():
					return
				}
			}
		}
	}()
	routesync.StreamAuthority(sctx, pw, func() {}, resp.Body, c.node, reg, onUp, outbox, c.log)
	return sctx.Err()
}

func commandMatchesSession(cmd *routesync.Command, identity routesync.NodeRegister) bool {
	if cmd == nil {
		return false
	}
	if identity.NodeEpoch == 0 && identity.SessionSeq == 0 {
		return cmd.NodeEpoch == 0 && cmd.SessionSeq == 0
	}
	return cmd.NodeEpoch != 0 && cmd.SessionSeq != 0 &&
		cmd.NodeEpoch == identity.NodeEpoch && cmd.SessionSeq == identity.SessionSeq
}

func fullJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}

func runNodeLinkOutbox(
	ctx context.Context,
	outbox chan<- *routesync.Msg,
	highOut <-chan *routesync.Msg,
	hbUpdate <-chan struct{},
	hbMu *sync.Mutex,
	latestHeartbeat **routesync.Msg,
) {
	heartbeatPending := false
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-highOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
			continue
		default:
		}
		if heartbeatPending {
			msg := takeLatestHeartbeat(hbMu, latestHeartbeat)
			if msg == nil {
				heartbeatPending = false
				continue
			}
			if !sendNodeLinkOutbox(ctx, outbox, msg) {
				return
			}
			heartbeatPending = false
			continue
		}
		select {
		case <-ctx.Done():
			return
		case m := <-highOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
		case <-hbUpdate:
			heartbeatPending = true
		}
	}
}

func sendNodeLinkOutbox(ctx context.Context, outbox chan<- *routesync.Msg, msg *routesync.Msg) bool {
	if msg == nil {
		return true
	}
	select {
	case outbox <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func takeLatestHeartbeat(mu *sync.Mutex, latest **routesync.Msg) *routesync.Msg {
	mu.Lock()
	defer mu.Unlock()
	msg := *latest
	*latest = nil
	return msg
}

func (c *Client) notifySession(endpoint string) {
	if obs, ok := c.node.(nodeLinkObserver); ok {
		obs.NodeLinkSession(endpoint)
	}
}

func (c *Client) notifyRedirect(target routesync.NodeLinkTarget) {
	if obs, ok := c.node.(nodeLinkObserver); ok {
		obs.NodeLinkRedirect(target)
	}
}

func (c *Client) transport(endpoint string) (*http2.Transport, string, string, error) {
	scheme, host, dialEndpoint, err := normalizeEndpoint(endpoint, c.tlsConfig != nil)
	if err != nil {
		return nil, "", "", err
	}
	tr := &http2.Transport{}
	dial := func(ctx context.Context) (net.Conn, error) {
		if c.dialEndpoint != nil {
			return c.dialEndpoint(ctx, dialEndpoint)
		}
		if c.dial != nil {
			return c.dial(ctx)
		}
		return nil, fmt.Errorf("node-link: no dialer configured")
	}
	if scheme == "https" {
		tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			conn, err := dial(ctx)
			if err != nil {
				return nil, err
			}
			cfg := tlsConfigForHost(c.tlsConfig, host)
			tc := tls.Client(conn, cfg)
			if err := tc.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tc, nil
		}
		return tr, scheme, host, nil
	}
	tr.AllowHTTP = true
	tr.DialTLSContext = func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		return dial(ctx)
	}
	return tr, scheme, host, nil
}

func normalizeEndpoint(endpoint string, tlsEnabled bool) (scheme, host, dialEndpoint string, err error) {
	scheme = "http"
	if tlsEnabled {
		scheme = "https"
	}
	host = "registry"
	dialEndpoint = endpoint
	if endpoint == "" {
		return scheme, host, dialEndpoint, nil
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", "", "", err
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", "", "", fmt.Errorf("node-link: unsupported endpoint scheme %q", u.Scheme)
		}
		if u.Host == "" {
			return "", "", "", fmt.Errorf("node-link: endpoint host is required")
		}
		return u.Scheme, u.Host, u.Host, nil
	}
	if strings.HasPrefix(endpoint, "/") {
		return "http", "registry", endpoint, nil
	}
	return scheme, endpoint, endpoint, nil
}

func tlsConfigForHost(base *tls.Config, host string) *tls.Config {
	var cfg *tls.Config
	if base != nil {
		cfg = base.Clone()
	} else {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" {
		cfg.ServerName = serverName(host)
	}
	return cfg
}

func serverName(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err == nil {
		return host
	}
	return hostport
}

type nodeLinkRedirect struct {
	targets []routesync.NodeLinkTarget
}

func (e nodeLinkRedirect) Error() string {
	return fmt.Sprintf("node-link redirected to %d owner target(s)", len(e.targets))
}

func nodeLinkRedirectFromHello(m *routesync.Msg) nodeLinkRedirect {
	if m == nil || m.Type != routesync.TypeHello || m.Hello == nil || m.Hello.Redirect == nil {
		return nodeLinkRedirect{}
	}
	out := make([]routesync.NodeLinkTarget, 0, len(m.Hello.Redirect.Targets))
	for _, target := range m.Hello.Redirect.Targets {
		if target.Endpoint == "" {
			continue
		}
		out = append(out, target)
	}
	return nodeLinkRedirect{targets: out}
}
