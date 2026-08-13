package nodectl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/unixcred"
)

// Server is the controller daemon's RPC face. It accepts UDS
// connections from sandbox-ctl, dispatches messages to admission /
// allocator / state, and persists state on every change.
type Server struct {
	Path      string
	State     *State
	Admission *AdmissionController
	Allocator *Allocator
	Persister *Persister
	Inventory *Inventory
	Owner     *OwnerLock
	// LegacyReservations is temporary rolling-upgrade compatibility. Inventory
	// is installed first and always wins over these state.json records.
	LegacyReservations map[string]*Reservation
	Auditor            *Auditor // optional
	Logf               func(string, ...any)

	listener net.Listener

	mu      sync.Mutex
	stopped bool
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
	if s.Owner == nil {
		s.Owner = &OwnerLock{Path: s.Path + ".owner"}
	}
	if err := s.Owner.Acquire(); err != nil {
		return err
	}
	fail := func(err error) error {
		_ = s.Owner.Close()
		return err
	}
	if s.Inventory != nil {
		if err := s.Inventory.Recover(s.State); err != nil {
			return fail(fmt.Errorf("server: recover inventory: %w", err))
		}
	}
	if err := s.State.MergePersistedReservations(s.LegacyReservations); err != nil {
		return fail(fmt.Errorf("server: merge legacy state: %w", err))
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
// associated with at most one Reservation (after Admit / Reattach
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
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := WriteMessage(conn, resp); err != nil {
				s.Logf("write: %v", err)
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
		}
	}
}

// dispatch routes one request to the appropriate handler and returns
// the response (or nil if the message has no reply).
func (s *Server) dispatch(conn net.Conn, peerPID int, req *Message, token *string) *Message {
	switch req.Type {
	case TypeAdmit:
		return s.handleAdmit(conn, peerPID, req, token)
	case TypeReattach:
		return s.handleReattach(conn, req, token)
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
		s.handleRelease(req, *token)
		// Signal the connection loop to drop association.
		*token = ""
		return &Message{Type: TypeAck}
	case TypeAdminDrain:
		return s.handleAdminDrain(req)
	case TypeAdminGrant:
		return s.handleAdminGrant(req)
	case TypeAdminReclaim:
		return s.handleAdminReclaim(req)
	case TypeAdminStatus:
		return s.handleAdminStatus()
	case TypeAdminList:
		return s.handleAdminList()
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
	oc := s.Admission.AnalyzeRequest(req)
	switch oc.Status {
	case OutcomeAdmitted:
		if !s.Admission.ConsumeToken() {
			// Race: another admit took the token. Re-evaluate (which may
			// now be short-term-block → queue).
			oc = s.Admission.AnalyzeRequest(req)
		}
	}

	switch oc.Status {
	case OutcomeAdmitted:
		// Token already consumed above (or admit didn't need a token recheck).
		resp, err := s.buildAdmitOK(conn, peerPID, req, token)
		if err != nil {
			return &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Msg:    err.Error(),
			}
		}
		return resp

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
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}
	if token != nil {
		*token = t
	}

	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
	}

	s.Logf("admit %s sid=%s initial_alloc=%d",
		t[:8], req.SandboxID, spec.InitialAllocatable)
	if s.Auditor != nil {
		s.Auditor.Logf("admit token=%s sid=%s initial_alloc=%d cap_mem=%d floor_mem=%d effective_startup=%d",
			t[:8], req.SandboxID, spec.InitialAllocatable,
			req.CapacityMemoryBytes, req.FloorMemoryBytes, spec.EffectiveStartupBudget)
	}
	return &Message{
		Type:                TypeAdmitResponse,
		Token:               t,
		Status:              StatusAdmitted,
		GrantedInitialAlloc: spec.InitialAllocatable,
	}, nil
}

func admitSpecFromRequest(conn net.Conn, peerPID int, req *Message) AdmitSpec {
	ebudget := computeEffectiveStartupBudget(req)
	return AdmitSpec{
		SandboxID: req.SandboxID, PeerPID: peerPID, CgroupPath: req.CgroupPath,
		Capacity:           Resources{MemoryBytes: req.CapacityMemoryBytes, CPUMilli: cpuMilliCeil(float64(req.CapacityCPU))},
		Floor:              Resources{MemoryBytes: req.FloorMemoryBytes, CPUMilli: cpuMilliCeil(req.FloorCPU)},
		InitialAllocatable: ebudget, EffectiveStartupBudget: ebudget,
		ClientFeatures: req.ClientFeatures, Conn: conn,
	}
}

// handleReattach re-binds a connection to an existing reservation
// (after sandbox-ctl reconnect or controller restart).
func (s *Server) handleReattach(conn net.Conn, req *Message, token *string) *Message {
	res, oldConn, found := s.State.Reattach(req.Token, conn)
	if !found {
		return &Message{Type: TypeError, Msg: "unknown token"}
	}
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}
	*token = req.Token
	s.Logf("reattach %s sid=%s stage=%s", req.Token[:8], res.SandboxID, res.Stage)
	return &Message{Type: TypeAck, Token: req.Token, NewAllocatable: res.AllocatableNowMem}
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
	if req.AppliedAllocatableMemory < l.FloorMemory || req.AppliedAllocatableMemory > l.CapacityMemory {
		return &Message{Type: TypeError, Msg: "state sync applied allocatable outside floor/capacity"}
	}
	newToken := NewToken()
	res, oldConns, err := s.State.Sync(SyncSpec{
		Token: newToken, SandboxID: l.SandboxID, PeerPID: live.OwnerPID,
		CgroupPath:    l.CgroupPath,
		Capacity:      Resources{MemoryBytes: l.CapacityMemory, CPUMilli: l.CapacityCPUMilli},
		Floor:         Resources{MemoryBytes: l.FloorMemory, CPUMilli: l.FloorCPUMilli},
		StartupMemory: l.StartupMemory, AppliedMemory: req.AppliedAllocatableMemory,
		Settled: req.Settled, CurrentRSS: req.CurrentRSS,
		LeasePath: live.Path, ClientFeatures: l.ClientFeatures, Conn: conn,
	})
	if err != nil {
		return &Message{Type: TypeError, Msg: "state sync: " + err.Error()}
	}
	for _, old := range oldConns {
		_ = old.Close()
	}
	*token = newToken
	s.Logf("state sync sid=%s pid=%d settled=%v alloc=%d", res.SandboxID, res.PeerPID, req.Settled, res.AllocatableNowMem)
	return &Message{Type: TypeAck, Token: newToken, NewAllocatable: res.AllocatableNowMem}
}

func (s *Server) handleSettled(req *Message, token string) *Message {
	res, found := s.State.SetSettled(token, req.CurrentRSS, time.Now())
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}

	s.Admission.PushWake()

	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
	}
	s.Logf("settled %s sid=%s rss=%d alloc=%d", token[:8], res.SandboxID, req.CurrentRSS, res.AllocatableNowMem)
	return &Message{Type: TypeAck}
}

func (s *Server) handleRequestBudget(req *Message, token string) *Message {
	urgency := req.Urgency
	if urgency == "" {
		urgency = UrgencyNormal
	}
	result, found := s.State.Grant(token, req.RequestedDelta, urgency, s.Allocator)
	if !found {
		return &Message{Type: TypeError, Msg: "no precise reservation"}
	}
	res, dec, zone := result.Reservation, result.Decision, result.Zone
	resp := &Message{
		Type:           TypeBudgetResponse,
		GrantedDelta:   dec.GrantedDelta,
		NewAllocatable: res.AllocatableNowMem,
		CooldownMs:     dec.CooldownMs,
	}
	if dec.GrantedDelta > 0 && s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
	}
	if dec.GrantedDelta > 0 {
		s.Logf("grant %s sid=%s +%d → %d (zone=%s urgency=%s)",
			token[:8], res.SandboxID, dec.GrantedDelta, res.AllocatableNowMem, zone, urgency)
	}
	return resp
}

func (s *Server) handleOOMReport(req *Message, token string) *Message {
	res, found := s.State.RecordOOM(token, req.OOMCount)
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
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
	// Sync allocatable_now to the client. If the active reclaimer or an
	// admin command shrank it, the client picks up the new value here
	// and applies it (cgroup memory.high + balloon resize).
	return &Message{Type: TypeAck, NewAllocatable: res.AllocatableNowMem}
}

func (s *Server) handleRelease(req *Message, token string) {
	res, found := s.State.Release(token)
	if !found {
		return
	}
	// Both main-pool and startup-pool accounting are derived from
	// reservations + their Stage; removing the reservation here implicitly
	// releases both. Wake admission so any short-term-blocked queued
	// admit can re-evaluate against the freshly returned headroom.
	wasPreSettled := IsPreSettled(res.Stage)
	s.Allocator.CleanupHistory(token)

	s.Admission.PushWake()

	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
	}
	s.Logf("release %s sid=%s reason=%s pre_settled=%v",
		token[:8], res.SandboxID, req.Reason, wasPreSettled)
	if s.Auditor != nil {
		s.Auditor.Logf("release token=%s sid=%s reason=%s alloc_at_release=%d pre_settled=%v",
			token[:8], res.SandboxID, req.Reason, res.AllocatableNowMem, wasPreSettled)
	}
}

// handleAdminDrain toggles drain mode based on req.Drain. While drained,
// new Admit requests are rejected with "controller is draining".
// Existing reservations are unaffected.
func (s *Server) handleAdminDrain(req *Message) *Message {
	prev := s.Admission.SetDrained(req.Drain)
	s.Logf("admin: drain %v → %v", prev, req.Drain)
	return &Message{Type: TypeAck, Drained: req.Drain}
}

// handleAdminGrant force-grants memory to a sandbox identified by
// SandboxID. Bypasses water-mark + rate-limit (admin override). The
// sandbox-ctl picks up the new allocatable on its next Heartbeat.
func (s *Server) handleAdminGrant(req *Message) *Message {
	res, delta, found := s.State.AdminGrant(req.SandboxID, req.RequestedDelta)
	if !found {
		return &Message{Type: TypeError, Msg: "no reservation for sandbox " + req.SandboxID}
	}
	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("admin grant persist: %v", err)
		}
	}
	s.Logf("admin grant sid=%s +%d → %d", req.SandboxID, delta, res.AllocatableNowMem)
	return &Message{Type: TypeAck, GrantedDelta: delta, NewAllocatable: res.AllocatableNowMem}
}

// handleAdminReclaim sets allocatable_now down to req.TargetAllocatable
// (clamped at floor) for a sandbox identified by SandboxID. The
// sandbox-ctl picks up the new value on its next Heartbeat and shrinks
// cgroup + balloon accordingly.
func (s *Server) handleAdminReclaim(req *Message) *Message {
	res, err := s.State.AdminReclaim(req.SandboxID, req.TargetAllocatable)
	if err != nil {
		return &Message{Type: TypeError, Msg: err.Error()}
	}
	if s.Persister != nil {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("admin reclaim persist: %v", err)
		}
	}
	s.Logf("admin reclaim sid=%s → %d", req.SandboxID, res.AllocatableNowMem)
	return &Message{Type: TypeAck, NewAllocatable: res.AllocatableNowMem}
}

// handleAdminStatus returns a summary of node state without requiring
// the caller to read state.json directly.
func (s *Server) handleAdminStatus() *Message {
	snapshot := s.State.ResourceSnapshot()
	return &Message{
		Type: TypeAck, Zone: string(snapshot.Zone),
		NodeAllocated:    snapshot.Allocated.MemoryBytes,
		AllocatablePool:  snapshot.AllocatablePool.MemoryBytes,
		ReservationCount: snapshot.ReservationCount,
		ProvisionalCount: snapshot.ProvisionalCount, UnknownCount: snapshot.UnknownCount,
		Drained:    s.Admission.IsDrained(),
		NodeBudget: resourceView(snapshot.NodeBudget), HostReserved: resourceView(snapshot.HostReserved),
		OperationalMargin: resourceView(snapshot.OperationalMargin),
		Allocated:         resourceView(snapshot.Allocated), Pool: resourceView(snapshot.AllocatablePool),
		StartupInFlight: snapshot.StartupInFlight,
	}
}

func resourceView(r Resources) ResourcesView {
	return ResourcesView{MemoryBytes: r.MemoryBytes, CPUMilli: r.CPUMilli}
}

func (s *Server) handleAdminList() *Message {
	reservations := s.State.ReservationViews()
	views := make([]ReservationView, 0, len(reservations))
	for _, r := range reservations {
		lastReport := int64(0)
		if !r.LastReportAt.IsZero() {
			lastReport = r.LastReportAt.Unix()
		}
		views = append(views, ReservationView{
			SandboxID: r.SandboxID, PeerPID: r.PeerPID, CgroupPath: r.CgroupPath,
			Capacity: resourceView(r.Capacity), Floor: resourceView(r.Floor),
			AllocatableMemory:     r.AllocatableNowMem,
			EffectiveStartupBytes: r.EffectiveStartupBudget, Stage: r.Stage,
			CurrentRSS: r.LastReportedRSS, LastReportUnix: lastReport,
			Connected: r.Conn != nil, Provisional: r.Provisional,
			RecoverySource: r.RecoverySource, StartupExpired: r.StartupExpired,
		})
	}
	return &Message{Type: TypeAck, Reservations: views}
}

// handleConnDrop is invoked by the connection goroutine after EOF /
// network error. Reservation is NOT immediately released — sandbox-ctl
// may reconnect within the reattach window.
func (s *Server) handleConnDrop(token string, conn net.Conn) {
	res, changed := s.State.DropConnection(token, conn)
	if !changed {
		return
	}
	s.Logf("conn dropped for %s sid=%s (reservation pending reattach)",
		token[:8], res.SandboxID)
}

func (s *Server) handleQueuedConnDrop(sid string, conn net.Conn) {
	res, changed := s.State.DropSandboxConnection(sid, conn)
	if !changed {
		return
	}
	s.Logf("conn dropped for queued sid=%s (reservation pending reattach)", res.SandboxID)
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
	Persister  *Persister
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
			if removed, ok := i.State.RemoveIfIdentity(res.SandboxID, res.identity()); ok {
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
			if removed, ok := i.State.RemoveIfIdentity(res.SandboxID, res.identity()); ok {
				i.Allocator.CleanupHistory(removed.Token)
			}
			swept = true
		}
	}
	if swept {
		// Headroom may have just opened up — wake admission worker.
		i.Admission.PushWake()
	}
	if i.Persister != nil {
		if err := i.Persister.Flush(i.State); err != nil {
			log.Printf("[node-ctl] sweep persist: %v", err)
		}
	}
}
