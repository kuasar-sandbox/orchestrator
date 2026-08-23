package nodectl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/unixcred"
)

// Server is the controller daemon's RPC face. It accepts UDS
// connections from sandbox-ctl, dispatches messages to admission /
// allocator / state. State is memory-only and rebuilt from live inventory.
type Server struct {
	Path      string
	Identity  string
	State     *State
	Admission *AdmissionController
	Allocator *Allocator
	Inventory *Inventory
	Owner     *OwnerLock
	Auditor   *Auditor // optional
	Logf      func(string, ...any)

	listener net.Listener

	mu      sync.Mutex
	stopped bool

	// phaseHook is test-only fault injection. Production leaves it nil.
	phaseHook func(string)
}

// Listen binds the UDS. Must be called before Serve. Removes any
// stale socket file at the path.
func (s *Server) Listen() error {
	if s.Logf == nil {
		s.Logf = func(string, ...any) {}
	}
	if err := os.MkdirAll(filepathDir(s.Path), 0o755); err != nil {
		return fmt.Errorf("server: mkdir: %w", err)
	}
	identity, err := canonicalControllerSocket(s.Path)
	if err != nil {
		return fmt.Errorf("server: controller socket identity: %w", err)
	}
	if s.Identity != "" && s.Identity != identity {
		return fmt.Errorf("server: controller socket identity changed: configured %s, current %s", s.Identity, identity)
	}
	s.Identity = identity
	ownerPath := s.Identity + ".owner"
	if s.Owner == nil {
		s.Owner = &OwnerLock{Path: ownerPath}
	} else if s.Owner.Path != ownerPath {
		return fmt.Errorf("server: owner identity %s does not match %s", s.Owner.Path, ownerPath)
	}
	if err := s.Owner.Acquire(); err != nil {
		return err
	}
	fail := func(err error) error {
		_ = s.Owner.Close()
		return err
	}
	if s.Inventory != nil {
		inventoryIdentity := s.Identity
		if s.Inventory.ControllerSocket != "" {
			var err error
			inventoryIdentity, err = canonicalControllerSocket(s.Inventory.ControllerSocket)
			if err != nil {
				return fail(fmt.Errorf("server: inventory socket identity: %w", err))
			}
		}
		if inventoryIdentity != s.Identity {
			return fail(fmt.Errorf("server: inventory socket identity %s does not match %s", inventoryIdentity, s.Identity))
		}
		s.Inventory.ControllerSocket = s.Identity
		if err := s.Inventory.Recover(s.State); err != nil {
			return fail(fmt.Errorf("server: recover inventory: %w", err))
		}
	}
	// Only the owner may remove a stale socket. A second live controller fails
	// above and never reaches this unlink.
	_ = os.Remove(s.Path)
	addr, err := net.ResolveUnixAddr("unix", s.Path)
	if err != nil {
		return fail(fmt.Errorf("server: resolve: %w", err))
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return fail(fmt.Errorf("server: listen %s: %w", s.Path, err))
	}
	if err := os.Chmod(s.Path, 0o660); err != nil {
		l.Close()
		return fail(fmt.Errorf("server: chmod: %w", err))
	}
	s.listener = l
	s.Logf("controller listening on %s", s.Path)
	return nil
}

// Serve accepts connections until ctx is cancelled or Stop is called.
func (s *Server) Serve(ctx context.Context) error {
	if s.listener == nil {
		return errors.New("server: not listening")
	}
	defer func() {
		_ = s.Owner.Close()
	}()
	go func() {
		<-ctx.Done()
		_ = s.listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped || ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			s.Logf("accept: %v", err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			peerPID := 0
			if uc, ok := c.(*net.UnixConn); ok {
				peerPID, _ = unixcred.PeerPID(uc)
			}
			s.serveConn(ctx, c, peerPID)
		}(conn)
	}
}

// Stop closes the listener (in-flight handlers continue until they
// return).
func (s *Server) Stop() error {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// serveConn runs the per-connection message loop. The connection is
// associated with at most one Reservation (after Admit / StateSync
// succeeds), discovered by the Token field on incoming messages.
func (s *Server) serveConn(ctx context.Context, conn net.Conn, peerPID int) {
	defer conn.Close()

	var (
		token     string
		queuedSID string
	)

	defer func() {
		if token != "" {
			s.handleConnDrop(token, conn)
		} else if queuedSID != "" {
			s.handleQueuedConnDrop(queuedSID, conn)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Don't impose a read deadline on the controller side — sandbox-ctl
		// pings every 30s, and idle in between is normal.
		req, err := ReadMessage(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.Logf("read: %v", err)
			}
			return
		}
		// Token discovery: an admit returning Queued is completed by the
		// admission worker asynchronously, so the local token var may not
		// be set when the next message arrives. Sync the local token from
		// req.Token (which the client carries as its auth field on every
		// non-Admit message) so handleConnDrop later finds the reservation.
		if token == "" && req.Token != "" {
			token = req.Token
		}
		resp := s.dispatch(conn, peerPID, req, &token)
		if req.Type == TypeAdmit && resp == nil {
			queuedSID = req.SandboxID
		}
		if resp != nil {
			if s.phaseHook != nil {
				s.phaseHook("before_response:" + req.Type)
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := WriteMessage(conn, resp); err != nil {
				s.Logf("write: %v", err)
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
			if s.phaseHook != nil {
				s.phaseHook("after_response:" + req.Type)
			}
		}
	}
}

// dispatch routes one request to the appropriate handler and returns
// the response (or nil if the message has no reply).
func (s *Server) dispatch(conn net.Conn, peerPID int, req *Message, token *string) *Message {
	switch req.Type {
	case TypeAdmit:
		return s.handleAdmit(conn, peerPID, req, token)
	case TypeStateSync:
		return s.handleStateSync(conn, peerPID, req, token)
	case TypeSettled:
		return s.handleSettled(req, *token)
	case TypeRequestBudget:
		return s.handleRequestBudget(req, *token)
	case TypeOOMReport:
		return s.handleOOMReport(req, *token)
	case TypeHeartbeat:
		return s.handleHeartbeat(req, *token)
	case TypeRelease:
		resp := s.handleRelease(req, *token)
		if resp.Type == TypeAck {
			// Signal the connection loop to drop association only after the
			// checked aggregate removal committed.
			*token = ""
		}
		return resp
	case TypeAdminDrain:
		return s.handleAdminDrain(req)
	case TypeAdminStatus:
		return s.handleAdminStatus()
	case TypeAdminList:
		return s.handleAdminList(req)
	default:
		return &Message{Type: TypeError, Msg: "unknown type: " + req.Type}
	}
}

// handleAdmit either admits the request immediately, rejects with a
// long-term reason, or hands the conn over to the admission queue (in
// which case it returns nil — the worker writes the response itself).
//
// Returning nil signals the serveConn loop to skip writing here; the
// connection stays open and the admission worker is responsible for the
// reply. The conn-EOF monitor cancels the queue entry if the client
// disconnects while queued.
func (s *Server) handleAdmit(conn net.Conn, peerPID int, req *Message, token *string) *Message {
	// A recovered provisional or ACK-lost Admit is already charged. Let the
	// matching lease owner atomically replace that charge before applying gates
	// intended only for a new consumer.
	if s.State.CanReplayAdmit(admitSpecFromRequest(conn, peerPID, req)) {
		resp, err := s.buildAdmitOK(conn, peerPID, req, token)
		if err != nil {
			return &Message{Type: TypeAdmitResponse, Status: StatusRejected, Msg: err.Error()}
		}
		return resp
	}

	// Serialize the resource check through reservation insertion with queued
	// admissions. AnalyzeRequest is intentionally read-only; without this
	// existing queue/state lock order, two connections could both observe the
	// same headroom and then over-subscribe it in separate State.Admit calls.
	// Replays above are excluded because they replace an already charged upper
	// bound rather than introduce a new consumer.
	s.Admission.queueMu.Lock()
	oc := s.Admission.AnalyzeRequest(req)
	switch oc.Status {
	case OutcomeAdmitted:
		if !s.Admission.ConsumeToken() {
			// Race: another admit took the token. Re-evaluate (which may
			// now be short-term-block → queue).
			oc = s.Admission.AnalyzeRequest(req)
		}
	}
	if oc.Status == OutcomeAdmitted {
		if s.phaseHook != nil {
			s.phaseHook("admit_checked")
		}
		resp, err := s.buildAdmitOK(conn, peerPID, req, token)
		s.Admission.queueMu.Unlock()
		if err != nil {
			return &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Msg:    err.Error(),
			}
		}
		return resp
	}
	s.Admission.queueMu.Unlock()

	switch oc.Status {
	case OutcomePreCheckReject, OutcomeLongTermReject:
		return &Message{
			Type:   TypeAdmitResponse,
			Status: StatusRejected,
			Reason: oc.RejectCode,
			Msg:    oc.RejectMsg,
		}

	case OutcomeShortTermBlock:
		entry, ok := s.Admission.Enqueue(req, conn, peerPID)
		if !ok {
			return &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Reason: "queue_full",
				Msg:    "admission queue at capacity",
			}
		}
		if s.Auditor != nil {
			s.Auditor.Logf("admit_queued sid=%s pos=%d block=%d",
				req.SandboxID, entry.queuedPos, int(oc.Block))
		}
		// nil → serveConn loop skips this write; the admission worker
		// will write Admitted/Rejected when the head is processed. The
		// serveConn loop also resumes ReadMessage on this conn — so it
		// is the sole reader; the queue must not race it with its own
		// conn.Read (that bug caused 1-byte stream desync). Client EOF
		// while queued is detected lazily via TTL expiry or worker
		// WriteMessage failure.
		return nil
	}

	// Defensive (unreachable).
	return &Message{Type: TypeAdmitResponse, Status: StatusRejected, Msg: "unknown admission outcome"}
}

// BuildAdmitOKFromQueue is the adapter the admission worker calls when
// it pops a queued head that now passes all checks. It bridges the
// PendingAdmit-shaped argument to the per-request buildAdmitOK path so
// queued and synchronous admits build reservations identically.
func (s *Server) BuildAdmitOKFromQueue(p *PendingAdmit) (*Message, error) {
	return s.buildAdmitOK(p.conn, p.peerPID, p.req, nil)
}

// buildAdmitOK builds the Reservation, inserts it into State, returns
// the AdmitResponse message. Caller has already token-consumed.
func (s *Server) buildAdmitOK(conn net.Conn, peerPID int, req *Message, token *string) (*Message, error) {
	spec := admitSpecFromRequest(conn, peerPID, req)
	leasePath := ""
	if slices.Contains(req.ClientFeatures, FeatureStateSyncV1) {
		if s.Inventory == nil {
			return nil, fmt.Errorf("state-sync client requires lease inventory")
		}
		// Lease creation is the client's pre-Admit lifecycle barrier. Do not
		// reopen or decode it on the normal RPC path: recovery scans validate
		// immutable content, and StateSync performs the strict live-lock,
		// SO_PEERCRED and managed pidfile/YAML cross-check. Initial Admit stays
		// pure in-memory after the peer credential captured at accept time.
		if peerPID <= 0 {
			return nil, fmt.Errorf("admit peer credentials unavailable")
		}
		if !s.Inventory.cgroupAllowed(req.CgroupPath) {
			return nil, fmt.Errorf("admit cgroup is outside configured recovery roots")
		}
		leasePath = s.Inventory.LeasePath(req.SandboxID)
	}

	t := NewToken()
	spec.Token = t
	spec.LeasePath = leasePath
	_, oldConn, err := s.State.Admit(spec)
	if err != nil {
		return nil, err
	}
	// A replay may replace a Capacity-provisional recovery charge with the
	// exact initial reservation. Wake the existing FIFO after the atomic
	// replacement so resource-blocked admissions re-evaluate released
	// headroom. For a brand-new admission this is a harmless coalesced wake.
	if s.Admission != nil {
		s.Admission.PushWake()
	}
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}
	if token != nil {
		*token = t
	}

	s.Logf("admit %s sid=%s initial_budget=%d",
		t[:8], req.SandboxID, spec.InitialBudget)
	if s.Auditor != nil {
		s.Auditor.Logf("admit token=%s sid=%s initial_budget=%d capacity_memory=%d headroom_memory=%d",
			t[:8], req.SandboxID, spec.InitialBudget,
			req.CapacityMemoryBytes, req.FloorMemoryBytes)
	}
	return &Message{
		Type:                TypeAdmitResponse,
		Token:               t,
		Status:              StatusAdmitted,
		GrantedInitialAlloc: spec.InitialBudget,
	}, nil
}

func admitSpecFromRequest(conn net.Conn, peerPID int, req *Message) AdmitSpec {
	initialBudget := initialReservationBudget(req)
	return AdmitSpec{
		SandboxID: req.SandboxID, PeerPID: peerPID, CgroupPath: req.CgroupPath,
		Capacity:              Resources{MemoryBytes: req.CapacityMemoryBytes, CPUMilli: cpuMilliCeil(float64(req.CapacityCPU))},
		ConfiguredAllocatable: Resources{MemoryBytes: req.FloorMemoryBytes, CPUMilli: cpuMilliCeil(req.FloorCPU)},
		InitialBudget:         initialBudget,
		ClientFeatures:        req.ClientFeatures, Conn: conn,
	}
}

func (s *Server) handleStateSync(conn net.Conn, peerPID int, req *Message, token *string) *Message {
	if s.Inventory == nil {
		return &Message{Type: TypeError, Msg: "state sync inventory unavailable"}
	}
	live, err := s.Inventory.LookupLiveLease(req.SandboxID)
	if err != nil {
		return &Message{Type: TypeError, Msg: "state sync lease: " + err.Error()}
	}
	if !live.Lease.Supports(FeatureStateSyncV1) {
		return &Message{Type: TypeError, Msg: "lease does not advertise state_sync_v1"}
	}
	if err := s.Inventory.ValidateManaged(live, peerPID); err != nil {
		return &Message{Type: TypeError, Msg: "state sync identity: " + err.Error()}
	}
	l := live.Lease
	if req.AppliedAllocatableMemory == 0 || req.AppliedAllocatableMemory > l.CapacityMemory {
		return &Message{Type: TypeError, Msg: "state sync reservation outside (0, capacity]"}
	}
	newToken := NewToken()
	res, oldConns, err := s.State.Sync(SyncSpec{
		Token: newToken, SandboxID: l.SandboxID, PeerPID: live.OwnerPID,
		CgroupPath:            l.CgroupPath,
		Capacity:              Resources{MemoryBytes: l.CapacityMemory, CPUMilli: l.CapacityCPUMilli},
		ConfiguredAllocatable: Resources{MemoryBytes: l.FloorMemory, CPUMilli: l.FloorCPUMilli},
		ReservationMemory:     req.AppliedAllocatableMemory,
		Settled:               req.Settled, HostMemoryCurrent: req.CurrentRSS,
		LeasePath: live.Path, ClientFeatures: l.ClientFeatures, Conn: conn,
	})
	if err != nil {
		return &Message{Type: TypeError, Msg: "state sync: " + err.Error()}
	}
	for _, old := range oldConns {
		_ = old.Close()
	}
	// StateSync commonly replaces the restart-time Capacity provisional charge
	// (or an ambiguous response baseline) with an equal or smaller exact
	// reservation. Make that released pool capacity visible to the FIFO.
	if s.Admission != nil {
		s.Admission.PushWake()
	}
	*token = newToken
	s.Logf("state sync sid=%s pid=%d settled=%v reservation=%d", res.SandboxID, res.PeerPID, req.Settled, res.ReservationMemory)
	return &Message{Type: TypeAck, Token: newToken, NewAllocatable: res.ReservationMemory}
}

func (s *Server) handleSettled(req *Message, token string) *Message {
	res, found, err := s.State.SetSettled(token, req.CurrentRSS, time.Now())
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	if err != nil {
		return &Message{Type: TypeError, Msg: err.Error()}
	}

	s.Admission.PushWake()

	s.Logf("settled %s sid=%s host_memory_current=%d reservation=%d", token[:8], res.SandboxID, req.CurrentRSS, res.ReservationMemory)
	return &Message{Type: TypeAck}
}

func (s *Server) handleRequestBudget(req *Message, token string) *Message {
	urgency := req.Urgency
	if urgency == "" {
		urgency = UrgencyNormal
	}
	// Admit checks pool headroom before inserting its reservation. Serialize a
	// runtime grow with that existing check/insert critical section so the two
	// operations cannot consume the same headroom concurrently. This is purely
	// node-local reservation accounting; it does not expose admission state or
	// policy to the sandbox controller.
	if s.Admission != nil {
		s.Admission.queueMu.Lock()
		defer s.Admission.queueMu.Unlock()
	}
	result, found, err := s.State.ReconcileAndGrant(token, req.CurrentAlloc, req.RequestedDelta, urgency, s.Allocator)
	if !found {
		return &Message{Type: TypeError, Msg: "no precise reservation"}
	}
	if err != nil {
		return &Message{Type: TypeError, Msg: err.Error()}
	}
	res, dec, zone := result.Reservation, result.Decision, result.Zone
	// A zero-delta absolute-baseline request commits a completed local shrink;
	// a grow request may also reconcile an older conservative baseline down.
	// Wake is coalesced and lets the admission head re-check the updated pool.
	if s.Admission != nil {
		s.Admission.PushWake()
	}
	resp := &Message{
		Type:           TypeBudgetResponse,
		GrantedDelta:   dec.GrantedDelta,
		NewAllocatable: res.ReservationMemory,
		CooldownMs:     dec.CooldownMs,
	}
	if dec.GrantedDelta > 0 {
		s.Logf("grant %s sid=%s +%d → %d (zone=%s urgency=%s)",
			token[:8], res.SandboxID, dec.GrantedDelta, res.ReservationMemory, zone, urgency)
	}
	return resp
}

func (s *Server) handleOOMReport(req *Message, token string) *Message {
	res, found := s.State.RecordOOM(token, req.OOMCount)
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	s.Logf("oom_report %s sid=%s count=%d killed_pid=%d",
		token[:8], res.SandboxID, res.OOMCount, req.KilledPID)
	return &Message{Type: TypeAck}
}

func (s *Server) handleHeartbeat(req *Message, token string) *Message {
	res, found := s.State.Heartbeat(token, req.CurrentRSS, time.Now())
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	// Heartbeat is a consistency echo. Only sandbox-originated RequestBudget
	// and StateSync may change this reservation; the response is never a
	// balloon/cgroup command.
	return &Message{Type: TypeAck, NewAllocatable: res.ReservationMemory}
}

func (s *Server) handleRelease(req *Message, token string) *Message {
	res, found, err := s.State.Release(token)
	if !found {
		return &Message{Type: TypeAck}
	}
	if err != nil {
		return &Message{Type: TypeError, Msg: err.Error()}
	}
	// Both main-pool and startup-pool accounting are derived from
	// reservations + their Stage; removing the reservation here implicitly
	// releases both. Wake admission so any short-term-blocked queued
	// admit can re-evaluate against the freshly returned headroom.
	wasPreSettled := IsPreSettled(res.Stage)
	s.Allocator.CleanupHistory(token)

	s.Admission.PushWake()

	s.Logf("release %s sid=%s reason=%s pre_settled=%v",
		token[:8], res.SandboxID, req.Reason, wasPreSettled)
	if s.Auditor != nil {
		s.Auditor.Logf("release token=%s sid=%s reason=%s reservation_at_release=%d pre_settled=%v",
			token[:8], res.SandboxID, req.Reason, res.ReservationMemory, wasPreSettled)
	}
	return &Message{Type: TypeAck}
}

// handleAdminDrain toggles drain mode based on req.Drain. While drained,
// new Admit requests are rejected with "controller is draining".
// Existing reservations are unaffected.
func (s *Server) handleAdminDrain(req *Message) *Message {
	prev := s.Admission.SetDrained(req.Drain)
	s.Logf("admin: drain %v → %v", prev, req.Drain)
	return &Message{Type: TypeAck, Drained: req.Drain}
}

// handleAdminStatus returns one consistent snapshot of the live in-memory
// node state over the controller UDS.
func (s *Server) handleAdminStatus() *Message {
	snapshot := s.State.ResourceSnapshot()
	return &Message{
		Type: TypeAck, Zone: string(snapshot.Zone),
		NodeAllocated:    snapshot.Reserved.MemoryBytes,
		AllocatablePool:  snapshot.AllocatablePool.MemoryBytes,
		ReservationCount: snapshot.ReservationCount,
		ProvisionalCount: snapshot.ProvisionalCount, UnknownCount: snapshot.UnknownCount,
		Drained:    s.Admission.IsDrained(),
		NodeBudget: resourceView(snapshot.NodeBudget), HostReserved: resourceView(snapshot.HostReserved),
		OperationalMargin: resourceView(snapshot.OperationalMargin),
		Allocated:         resourceView(snapshot.Reserved), Pool: resourceView(snapshot.AllocatablePool),
		StartupInFlight: snapshot.StartupInFlight,
	}
}

func resourceView(r Resources) ResourcesView {
	return ResourcesView{MemoryBytes: r.MemoryBytes, CPUMilli: r.CPUMilli}
}

const maxAdminListPageEntries = 1024

func (s *Server) handleAdminList(req *Message) *Message {
	reservations := s.State.ReservationViews()
	views := make([]ReservationView, 0, len(reservations))
	for _, r := range reservations {
		lastReport := int64(0)
		if !r.LastReportAt.IsZero() {
			lastReport = r.LastReportAt.Unix()
		}
		views = append(views, ReservationView{
			SandboxID: r.SandboxID, PeerPID: r.PeerPID, CgroupPath: r.CgroupPath,
			Capacity: resourceView(r.Capacity), Floor: resourceView(r.ConfiguredAllocatable),
			AllocatableMemory:     r.ReservationMemory,
			EffectiveStartupBytes: r.InitialBudget, Stage: r.Stage,
			CurrentRSS: r.LastHostMemoryCurrent, LastReportUnix: lastReport,
			Connected: r.Conn != nil, Provisional: r.Provisional,
			RecoverySource: r.RecoverySource, StartupExpired: r.StartupExpired,
		})
	}

	if req.ListLimit < 0 {
		return &Message{Type: TypeError, Msg: "admin_list list_limit must not be negative"}
	}
	if req.ListLimit == 0 {
		// Pre-pagination clients send no limit. Preserve their one-frame response
		// while it fits, but fail explicitly instead of truncating or letting the
		// transport close on WriteMessage's frame-size check.
		resp := &Message{Type: TypeAck, Reservations: views}
		if err := WriteMessage(io.Discard, resp); err != nil {
			return &Message{Type: TypeError, Msg: "admin_list response exceeds protocol frame; upgrade client for pagination"}
		}
		return resp
	}

	start := 0
	for start < len(views) && views[start].SandboxID <= req.ListAfter {
		start++
	}
	limit := req.ListLimit
	if limit > maxAdminListPageEntries {
		limit = maxAdminListPageEntries
	}
	capacity := len(views) - start
	if capacity > limit {
		capacity = limit
	}
	page := make([]ReservationView, 0, capacity)
	for start+len(page) < len(views) && len(page) < limit {
		candidate := append(page, views[start+len(page)])
		next := ""
		if start+len(candidate) < len(views) {
			next = candidate[len(candidate)-1].SandboxID
		}
		probe := &Message{Type: TypeAck, Reservations: candidate, ListNext: next}
		if err := WriteMessage(io.Discard, probe); err != nil {
			if len(page) == 0 {
				return &Message{Type: TypeError, Msg: "admin_list reservation exceeds protocol frame"}
			}
			break
		}
		page = candidate
	}
	next := ""
	if start+len(page) < len(views) {
		next = page[len(page)-1].SandboxID
	}
	return &Message{Type: TypeAck, Reservations: page, ListNext: next}
}

// handleConnDrop is invoked by the connection goroutine after EOF /
// network error. Reservation is NOT immediately released — sandbox-ctl may
// reconnect and recover the same safe baseline through StateSync.
func (s *Server) handleConnDrop(token string, conn net.Conn) {
	res, changed := s.State.DropConnection(token, conn)
	if !changed {
		return
	}
	s.Logf("conn dropped for %s sid=%s (reservation pending StateSync)",
		token[:8], res.SandboxID)
}

func (s *Server) handleQueuedConnDrop(sid string, conn net.Conn) {
	res, changed := s.State.DropSandboxConnection(sid, conn)
	if !changed {
		return
	}
	s.Logf("conn dropped for queued sid=%s (reservation retained until checked cleanup)", res.SandboxID)
}

// filepathDir replicates filepath.Dir without importing path/filepath
// here (cuts an import line).
func filepathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

// IdleSweeper periodically scans reservations for stale heartbeats and
// expired creating-stage TTLs. It calls back into State / Admission to
// release the reservation.
type IdleSweeper struct {
	State      *State
	Admission  *AdmissionController
	Allocator  *Allocator
	Inventory  *Inventory
	StartupTTL time.Duration
	Heartbeat  time.Duration
	Interval   time.Duration
	Logf       func(string, ...any)
}

func (i *IdleSweeper) Run(ctx context.Context) {
	if i.Logf == nil {
		i.Logf = func(string, ...any) {}
	}
	if i.Interval == 0 {
		i.Interval = 10 * time.Second
	}
	t := time.NewTicker(i.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			i.sweep()
		}
	}
}

func (i *IdleSweeper) sweep() {
	now := time.Now()
	swept := false
	for _, res := range i.State.SweepSnapshot() {
		live := res.Conn != nil
		if !live && i.Inventory != nil {
			live = i.Inventory.ConsumerLive(res)
		}
		// Creating-stage TTL.
		if IsPreSettled(res.Stage) &&
			now.Sub(res.StageEnteredAt) > i.StartupTTL {
			i.State.MarkStartupExpired(res.SandboxID, res.identity())
			if live {
				i.Logf("sweep: sid=%s exceeded startup TTL but consumer is live; retaining charge", res.SandboxID)
				continue
			}
			i.Logf("sweep: sid=%s exceeded startup TTL and consumer is gone, releasing", res.SandboxID)
			if removed, ok, err := i.State.RemoveIfIdentity(res.SandboxID, res.identity()); err != nil {
				i.Logf("sweep: sid=%s checked release failed: %v", res.SandboxID, err)
				continue
			} else if ok {
				i.Allocator.CleanupHistory(removed.Token)
			}
			swept = true
			continue
		}
		// Heartbeat staleness — only when conn is gone (live conn keeps
		// timestamp fresh enough).
		if res.Conn == nil && i.Heartbeat > 0 &&
			now.Sub(res.LastHeartbeatAt) > 3*i.Heartbeat {
			if live {
				i.Logf("sweep: sid=%s heartbeat stale but consumer is live; retaining charge", res.SandboxID)
				continue
			}
			i.Logf("sweep: sid=%s heartbeat stale and consumer is gone, releasing", res.SandboxID)
			if removed, ok, err := i.State.RemoveIfIdentity(res.SandboxID, res.identity()); err != nil {
				i.Logf("sweep: sid=%s checked release failed: %v", res.SandboxID, err)
				continue
			} else if ok {
				i.Allocator.CleanupHistory(removed.Token)
			}
			swept = true
		}
	}
	if swept {
		// Headroom may have just opened up — wake admission worker.
		i.Admission.PushWake()
	}
}
