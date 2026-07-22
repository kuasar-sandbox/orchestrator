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

// Node is the final node-link surface. Lifecycle facts are emitted exclusively
// through DurableEventOutbox; PlacementLoad is Holder-local soft state.
type Node interface {
	HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck
	PlacementLoad(context.Context) (*routesync.PlacementLoadSnapshot, error)
}

type placementWakeSource interface {
	PlacementWake() <-chan struct{}
}

type executionReadiness interface {
	WaitExecutionReady(context.Context) error
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

// DurableEventOutbox is the node-local latest-event journal. Implementations
// must retain unacknowledged events across process restart within one NodeEpoch.
type DurableEventOutbox interface {
	PendingExecutionEvents(context.Context, string, uint64, routesync.EventCursor, int, int) ([]routesync.ExecutionEvent, routesync.EventCursor, error)
	AckExecutionEvent(context.Context, string, uint64, routesync.EventAck) error
	EventWake() <-chan struct{}
}

// Client is a node's node-link client: it dials the registry, registers the
// node's identity, then streams its sandbox routes while executing registry
// commands, reconnecting with capped backoff.
type Client struct {
	dialEndpoint  func(ctx context.Context, endpoint string) (net.Conn, error)
	endpoint      string
	identity      routesync.NodeRegister
	sequencer     SessionSequencer
	eventOutbox   DurableEventOutbox
	eventBatch    int
	eventBytes    int
	eventInterval time.Duration
	node          Node
	placement     time.Duration
	tlsConfig     *tls.Config // non-nil = dial the registry over (m)TLS instead of h2c
	log           *slog.Logger
}

// SetEventReplayLimits configures one bounded replay batch per interval.
func (c *Client) SetEventReplayLimits(batch, bytes int, interval time.Duration) {
	if batch > 0 {
		c.eventBatch = batch
	}
	if bytes > 0 {
		c.eventBytes = bytes
	}
	if interval > 0 {
		c.eventInterval = interval
	}
}

// New builds the final node-link client. Session sequencing, durable event
// replay and Holder redirects are mandatory protocol behavior.
func New(
	endpoint string,
	dialEndpoint func(ctx context.Context, endpoint string) (net.Conn, error),
	identity routesync.NodeRegister,
	node Node,
	sequencer SessionSequencer,
	eventOutbox DurableEventOutbox,
	interval time.Duration,
	tlsConfig *tls.Config,
	log *slog.Logger,
) *Client {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		dialEndpoint: dialEndpoint, endpoint: endpoint, identity: identity, node: node,
		sequencer: sequencer, eventOutbox: eventOutbox,
		placement: interval, tlsConfig: tlsConfig, log: log,
		eventBatch: 64, eventBytes: 1 << 20, eventInterval: time.Second,
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
		if errors.As(err, &redir) {
			redirectTargets = redir.targets
			redirectIndex = 0
			endpoint = redirectTargets[0].Endpoint
			c.notifyRedirect(redirectTargets[0])
			c.log.Info("node-link: redirecting to node owner", "node", c.identity.NodeID, "member", redirectTargets[0].MemberID, "endpoint", endpoint)
			backoff = 200 * time.Millisecond
			continue
		}
		if len(redirectTargets) > 0 && redirectIndex+1 < len(redirectTargets) {
			redirectIndex++
			endpoint = redirectTargets[redirectIndex].Endpoint
			c.notifyRedirect(redirectTargets[redirectIndex])
			c.log.Warn("node-link: trying next redirected owner", "node", c.identity.NodeID, "member", redirectTargets[redirectIndex].MemberID, "endpoint", endpoint, "err", err)
			continue
		}
		if endpoint != c.endpoint {
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
	if c.sequencer == nil || c.eventOutbox == nil || c.dialEndpoint == nil || c.node == nil || endpoint == "" {
		return errors.New("node-link: incomplete final client dependencies")
	}
	tuple, err := c.sequencer.NextSession(ctx)
	if err != nil {
		return fmt.Errorf("node-link: persist session tuple: %w", err)
	}
	identity.NodeEpoch = tuple.NodeEpoch
	identity.SessionSeq = tuple.SessionSeq
	if readiness, ok := c.node.(executionReadiness); ok {
		if err := readiness.WaitExecutionReady(ctx); err != nil {
			return err
		}
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

	wireOut := make(chan *routesync.Msg)
	highOut := make(chan *routesync.Msg, 32)
	eventOut := make(chan *routesync.Msg, 64)
	loadUpdate := make(chan struct{}, 1)
	var loadMu sync.Mutex
	var latestLoad *routesync.Msg
	go runNodeLinkOutbox(sctx, wireOut, highOut, eventOut, loadUpdate, &loadMu, &latestLoad)
	writerDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-sctx.Done():
				writerDone <- sctx.Err()
				return
			case message := <-wireOut:
				if err := routesync.WriteMsg(pw, message); err != nil {
					writerDone <- err
					return
				}
			}
		}
	}()
	go runDurableEventReplay(sctx, c.eventOutbox, identity.NodeID, identity.NodeEpoch,
		c.eventBatch, c.eventBytes, c.eventInterval, eventOut, c.log)
	go c.runPlacementSnapshots(sctx, identity, loadUpdate, &loadMu, &latestLoad)

	readerDone := make(chan error, 1)
	go func() { readerDone <- c.readDownlink(sctx, resp.Body, identity, highOut) }()
	select {
	case err := <-readerDone:
		return err
	case err := <-writerDone:
		return err
	case <-sctx.Done():
		return sctx.Err()
	}
}

func (c *Client) readDownlink(
	ctx context.Context,
	reader io.Reader,
	identity routesync.NodeRegister,
	highOut chan<- *routesync.Msg,
) error {
	for {
		message, err := routesync.ReadMsg(reader)
		if err != nil {
			return err
		}
		switch message.Type {
		case routesync.TypeEventAck:
			if message.EventAck == nil {
				continue
			}
			if err := c.eventOutbox.AckExecutionEvent(ctx, identity.NodeID, identity.NodeEpoch, *message.EventAck); err != nil {
				c.log.Warn("node-link: reject execution event acknowledgement", "node", identity.NodeID,
					"kind", message.EventAck.ObjectKind, "object", message.EventAck.ObjectID,
					"event_seq", message.EventAck.EventSeq, "err", err)
			}
		case routesync.TypeCommand:
			if message.Cmd == nil {
				return errors.New("node-link: empty command frame")
			}
			var ack *routesync.CmdAck
			if !commandMatchesSession(message.Cmd, identity) {
				ack = &routesync.CmdAck{
					CmdID: message.Cmd.CmdID, Status: routesync.AckRejected,
					Outcome: routesync.DispatchSessionMoved, Reason: "command session tuple is stale",
				}
			} else {
				ack = c.node.HandleCommand(ctx, message.Cmd)
			}
			if ack == nil {
				return errors.New("node-link: command handler returned no acknowledgement")
			}
			select {
			case highOut <- &routesync.Msg{Type: routesync.TypeCmdAck, Ack: ack}:
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			return fmt.Errorf("node-link: forbidden downlink message type %q", message.Type)
		}
	}
}

func (c *Client) runPlacementSnapshots(
	ctx context.Context,
	identity routesync.NodeRegister,
	update chan<- struct{},
	mu *sync.Mutex,
	latest **routesync.Msg,
) {
	interval := c.placement
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var wake <-chan struct{}
	if source, ok := c.node.(placementWakeSource); ok {
		wake = source.PlacementWake()
	}
	var sampleSeq uint64
	publish := func() {
		snapshot, err := c.node.PlacementLoad(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.log.Warn("node-link: sample PlacementLoadSnapshot", "node", identity.NodeID, "err", err)
			}
			return
		}
		if snapshot == nil {
			return
		}
		sampleSeq++
		fillPlacementIdentity(snapshot, identity, sampleSeq)
		mu.Lock()
		*latest = &routesync.Msg{Type: routesync.TypePlacementLoad, Load: snapshot}
		mu.Unlock()
		select {
		case update <- struct{}{}:
		default:
		}
	}
	publish()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		case <-wake:
			publish()
		}
	}
}

func fillPlacementIdentity(snapshot *routesync.PlacementLoadSnapshot, identity routesync.NodeRegister, sampleSeq uint64) {
	snapshot.NodeID = identity.NodeID
	snapshot.NodeEpoch = identity.NodeEpoch
	snapshot.SessionSeq = identity.SessionSeq
	snapshot.DataEndpoint = identity.DataEndpoint
	snapshot.SampleSeq = sampleSeq
	snapshot.LoadModelVersion = identity.LoadModelVersion
	snapshot.RuntimeDigest = identity.RuntimeDigest
	snapshot.SandboxSlotCapacity = uint64(max(identity.Capacity, 0))
	if snapshot.SandboxSlotHardLimit == 0 {
		snapshot.SandboxSlotHardLimit = snapshot.SandboxSlotCapacity
	}
	if build := identity.BuildCapacity; build != nil {
		snapshot.BuildSlotCapacity = uint64(max(build.Slots, 0))
		snapshot.BuildCPUCapacity = uint64(max(build.CPU, 0))
		snapshot.BuildMemoryCapacity = uint64(max(build.Mem, 0))
		snapshot.BuildStorageCapacity = uint64(max(build.Storage, 0))
		if snapshot.BuildSlotHardLimit == 0 {
			snapshot.BuildSlotHardLimit = snapshot.BuildSlotCapacity
		}
	}
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
	eventOut <-chan *routesync.Msg,
	loadUpdate <-chan struct{},
	loadMu *sync.Mutex,
	latestLoad **routesync.Msg,
) {
	const (
		maxPriorityBurst = 32
		maxNonLoadBurst  = 32
	)
	loadPending := false
	priorityBurst := 0
	nonLoadBurst := 0
	for {
		select {
		case <-loadUpdate:
			loadPending = true
		default:
		}
		if priorityBurst >= maxPriorityBurst {
			select {
			case <-ctx.Done():
				return
			case m := <-eventOut:
				if !sendNodeLinkOutbox(ctx, outbox, m) {
					return
				}
				priorityBurst = 0
				nonLoadBurst++
				continue
			default:
			}
		}
		if loadPending && nonLoadBurst >= maxNonLoadBurst {
			msg := takeLatestLoad(loadMu, latestLoad)
			if msg == nil {
				loadPending = false
				nonLoadBurst = 0
				continue
			}
			if !sendNodeLinkOutbox(ctx, outbox, msg) {
				return
			}
			loadPending = false
			priorityBurst = 0
			nonLoadBurst = 0
			continue
		}
		select {
		case <-ctx.Done():
			return
		case m := <-highOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
			priorityBurst++
			nonLoadBurst++
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case m := <-eventOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
			priorityBurst = 0
			nonLoadBurst++
			continue
		default:
		}
		if loadPending {
			msg := takeLatestLoad(loadMu, latestLoad)
			if msg == nil {
				loadPending = false
				continue
			}
			if !sendNodeLinkOutbox(ctx, outbox, msg) {
				return
			}
			loadPending = false
			priorityBurst = 0
			nonLoadBurst = 0
			continue
		}
		select {
		case <-ctx.Done():
			return
		case m := <-highOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
			priorityBurst++
			nonLoadBurst++
		case m := <-eventOut:
			if !sendNodeLinkOutbox(ctx, outbox, m) {
				return
			}
			priorityBurst = 0
			nonLoadBurst++
		case <-loadUpdate:
			loadPending = true
		}
	}
}

func runDurableEventReplay(
	ctx context.Context,
	durable DurableEventOutbox,
	nodeID string,
	nodeEpoch uint64,
	batch, bytes int,
	interval time.Duration,
	eventOut chan<- *routesync.Msg,
	log *slog.Logger,
) {
	if batch <= 0 {
		batch = 64
	}
	if bytes <= 0 {
		bytes = 1 << 20
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var cursor routesync.EventCursor
	for {
		events, next, err := durable.PendingExecutionEvents(ctx, nodeID, nodeEpoch, cursor, batch, bytes)
		if err != nil && ctx.Err() == nil {
			log.Warn("node-link: load durable execution events", "node", nodeID, "err", err)
		}
		if err == nil {
			if len(events) == 0 && (cursor.ObjectKind != "" || cursor.ObjectID != "") {
				cursor = routesync.EventCursor{}
			} else {
				cursor = next
			}
		}
		for i := range events {
			event := events[i]
			select {
			case eventOut <- &routesync.Msg{Type: routesync.TypeExecutionEvent, ExecutionEvent: &event}:
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-durable.EventWake():
		case <-ticker.C:
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

func takeLatestLoad(mu *sync.Mutex, latest **routesync.Msg) *routesync.Msg {
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
		return c.dialEndpoint(ctx, dialEndpoint)
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
	lowerEndpoint := strings.ToLower(endpoint)
	if strings.HasPrefix(lowerEndpoint, "http://") || strings.HasPrefix(lowerEndpoint, "https://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", "", "", err
		}
		normalizedScheme := strings.ToLower(u.Scheme)
		if normalizedScheme != "http" && normalizedScheme != "https" {
			return "", "", "", fmt.Errorf("node-link: unsupported endpoint scheme %q", u.Scheme)
		}
		if u.Host == "" {
			return "", "", "", fmt.Errorf("node-link: endpoint host is required")
		}
		return normalizedScheme, u.Host, u.Host, nil
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
