package nodectl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
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
	Auditor   *Auditor // optional
	Logf      func(string, ...any)

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
	_ = os.Remove(s.Path)
	if err := os.MkdirAll(filepathDir(s.Path), 0o755); err != nil {
		return fmt.Errorf("server: mkdir: %w", err)
	}
	addr, err := net.ResolveUnixAddr("unix", s.Path)
	if err != nil {
		return fmt.Errorf("server: resolve: %w", err)
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.Path, err)
	}
	if err := os.Chmod(s.Path, 0o660); err != nil {
		l.Close()
		return fmt.Errorf("server: chmod: %w", err)
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
			s.serveConn(ctx, c)
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
func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	var (
		token string
	)

	defer func() {
		if token != "" {
			s.handleConnDrop(token)
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
		resp := s.dispatch(conn, req, &token)
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
func (s *Server) dispatch(conn net.Conn, req *Message, token *string) *Message {
	switch req.Type {
	case TypeAdmit:
		return s.handleAdmit(conn, req, token)
	case TypeReattach:
		return s.handleReattach(conn, req, token)
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
func (s *Server) handleAdmit(conn net.Conn, req *Message, token *string) *Message {
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
		resp, err := s.buildAdmitOK(conn, req, token)
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
		entry, ok := s.Admission.Enqueue(req, conn)
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
	return s.buildAdmitOK(p.conn, p.req, nil)
}

// buildAdmitOK builds the Reservation, inserts it into State, returns
// the AdmitResponse message. Caller has already token-consumed.
func (s *Server) buildAdmitOK(conn net.Conn, req *Message, token *string) (*Message, error) {
	ebudget := computeEffectiveStartupBudget(req)

	s.State.Lock()

	t := NewToken()
	res := &Reservation{
		Token:                  t,
		SandboxID:              req.SandboxID,
		CgroupPath:             req.CgroupPath,
		Capacity:               Resources{MemoryBytes: req.CapacityMemoryBytes, CPUMilli: uint64(req.CapacityCPU) * 1000},
		Floor:                  Resources{MemoryBytes: req.FloorMemoryBytes, CPUMilli: uint64(req.FloorCPU * 1000)},
		AllocatableNowMem:      ebudget,
		EffectiveStartupBudget: ebudget,
		Stage:                  StageAdmitted,
		StageEnteredAt:         time.Now(),
		LastHeartbeatAt:        time.Now(),
		Conn:                   conn,
	}
	if err := s.State.Insert(res); err != nil {
		s.State.Unlock()
		return nil, err
	}
	if token != nil {
		*token = t
	}
	s.State.Unlock()

	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("persister flush: %v", err)
	}

	s.Logf("admit %s sid=%s initial_alloc=%d",
		t[:8], req.SandboxID, ebudget)
	if s.Auditor != nil {
		s.Auditor.Logf("admit token=%s sid=%s initial_alloc=%d cap_mem=%d floor_mem=%d effective_startup=%d",
			t[:8], req.SandboxID, ebudget,
			req.CapacityMemoryBytes, req.FloorMemoryBytes, ebudget)
	}
	return &Message{
		Type:                TypeAdmitResponse,
		Token:               t,
		Status:              StatusAdmitted,
		GrantedInitialAlloc: ebudget,
	}, nil
}

// handleReattach re-binds a connection to an existing reservation
// (after sandbox-ctl reconnect or controller restart).
func (s *Server) handleReattach(conn net.Conn, req *Message, token *string) *Message {
	s.State.Lock()
	defer s.State.Unlock()

	res := s.State.Lookup(req.Token)
	if res == nil {
		return &Message{Type: TypeError, Msg: "unknown token"}
	}
	res.Conn = conn
	*token = req.Token
	s.Logf("reattach %s sid=%s stage=%s", req.Token[:8], res.SandboxID, res.Stage)
	return &Message{Type: TypeAck, Token: req.Token, NewAllocatable: res.AllocatableNowMem}
}

func (s *Server) handleSettled(req *Message, token string) *Message {
	s.State.Lock()
	res := s.State.Lookup(token)
	if res == nil {
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "no reservation"}
	}

	// 1. main-pool release: collapse AllocatableNowMem from the elevated
	//    startup budget down to max(current_rss, floor). Steady-state grant
	//    paths will pump it back up if needed.
	floor := res.Floor.MemoryBytes
	newAlloc := req.CurrentRSS
	if newAlloc < floor {
		newAlloc = floor
	}
	res.AllocatableNowMem = newAlloc

	// 2. startup-pool release: the stage transition itself (admitted →
	//    settled) takes res out of the pre-settled set, so StartupInFlight
	//    accounting (derived) auto-decrements by res.EffectiveStartupBudget.
	res.Stage = StageSettled
	res.StageEnteredAt = time.Now()
	res.LastHeartbeatAt = time.Now()
	sandboxID := res.SandboxID
	s.State.Unlock()

	// Wake the admission worker — main + startup pool both just got
	// headroom back, queued admits may now fit.
	s.Admission.PushWake()

	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("persister flush: %v", err)
	}
	s.Logf("settled %s sid=%s rss=%d alloc=%d", token[:8], sandboxID, req.CurrentRSS, newAlloc)
	return &Message{Type: TypeAck}
}

func (s *Server) handleRequestBudget(req *Message, token string) *Message {
	s.State.Lock()
	res := s.State.Lookup(token)
	if res == nil {
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "no reservation"}
	}

	zone := s.State.MemoryZone()
	pool := s.State.AllocatablePool.MemoryBytes
	emerg := uint64(float64(pool) * s.State.Wm.EmergencyFactor)
	allocated := s.State.NodeAllocated().MemoryBytes

	urgency := req.Urgency
	if urgency == "" {
		urgency = UrgencyNormal
	}

	// Headroom: emergency_pool excluded for normal/low urgency.
	var headroom uint64
	if pool > allocated {
		headroom = pool - allocated
	}
	if urgency != UrgencyHigh {
		if pool > allocated+emerg {
			headroom = pool - allocated - emerg
		} else {
			headroom = 0
		}
	}
	// Cap by per-sandbox capacity.
	maxByCap := uint64(0)
	if res.Capacity.MemoryBytes > res.AllocatableNowMem {
		maxByCap = res.Capacity.MemoryBytes - res.AllocatableNowMem
	}
	if headroom > maxByCap {
		headroom = maxByCap
	}

	// In red/critical zone, only urgency=high gets through.
	if (zone == ZoneRed || zone == ZoneCritical) && urgency != UrgencyHigh {
		newAllocatable := res.AllocatableNowMem
		s.State.Unlock()
		return &Message{
			Type:           TypeBudgetResponse,
			GrantedDelta:   0,
			NewAllocatable: newAllocatable,
			CooldownMs:     500,
		}
	}

	dec := s.Allocator.Grant(token, req.RequestedDelta, headroom, urgency)
	res.AllocatableNowMem += dec.GrantedDelta
	if dec.GrantedDelta > 0 {
		if res.Stage != StageBurst {
			res.Stage = StageBurst
			res.StageEnteredAt = time.Now()
		}
	}
	resp := &Message{
		Type:           TypeBudgetResponse,
		GrantedDelta:   dec.GrantedDelta,
		NewAllocatable: res.AllocatableNowMem,
		CooldownMs:     dec.CooldownMs,
	}
	sandboxID := res.SandboxID
	newAllocatable := res.AllocatableNowMem
	s.State.Unlock()
	if dec.GrantedDelta > 0 {
		if err := s.Persister.Flush(s.State); err != nil {
			s.Logf("persister flush: %v", err)
		}
	}
	if dec.GrantedDelta > 0 {
		s.Logf("grant %s sid=%s +%d → %d (zone=%s urgency=%s)",
			token[:8], sandboxID, dec.GrantedDelta, newAllocatable, zone, urgency)
	}
	return resp
}

func (s *Server) handleOOMReport(req *Message, token string) *Message {
	s.State.Lock()
	res := s.State.Lookup(token)
	if res == nil {
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	res.OOMCount += req.OOMCount
	sandboxID := res.SandboxID
	oomCount := res.OOMCount
	s.State.Unlock()
	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("persister flush: %v", err)
	}
	s.Logf("oom_report %s sid=%s count=%d killed_pid=%d",
		token[:8], sandboxID, oomCount, req.KilledPID)
	return &Message{Type: TypeAck}
}

func (s *Server) handleHeartbeat(req *Message, token string) *Message {
	s.State.Lock()
	defer s.State.Unlock()
	res := s.State.Lookup(token)
	if res == nil {
		return &Message{Type: TypeError, Msg: "no reservation"}
	}
	res.LastHeartbeatAt = time.Now()
	if req.CurrentRSS > 0 {
		res.LastReportedRSS = req.CurrentRSS
	}
	// Sync allocatable_now to the client. If the active reclaimer or an
	// admin command shrank it, the client picks up the new value here
	// and applies it (cgroup memory.high + balloon resize).
	return &Message{Type: TypeAck, NewAllocatable: res.AllocatableNowMem}
}

func (s *Server) handleRelease(req *Message, token string) {
	s.State.Lock()
	res := s.State.Lookup(token)
	if res == nil {
		s.State.Unlock()
		return
	}
	// Both main-pool and startup-pool accounting are derived from
	// reservations + their Stage; removing the reservation here implicitly
	// releases both. Wake admission so any short-term-blocked queued
	// admit can re-evaluate against the freshly returned headroom.
	wasPreSettled := IsPreSettled(res.Stage)
	sandboxID := res.SandboxID
	allocatableAtRelease := res.AllocatableNowMem
	s.Allocator.CleanupHistory(token)
	s.State.Remove(token)
	s.State.Unlock()

	s.Admission.PushWake()

	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("persister flush: %v", err)
	}
	s.Logf("release %s sid=%s reason=%s pre_settled=%v",
		token[:8], sandboxID, req.Reason, wasPreSettled)
	if s.Auditor != nil {
		s.Auditor.Logf("release token=%s sid=%s reason=%s alloc_at_release=%d pre_settled=%v",
			token[:8], sandboxID, req.Reason, allocatableAtRelease, wasPreSettled)
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
	s.State.Lock()
	res := s.findBySandboxIDLocked(req.SandboxID)
	if res == nil {
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "no reservation for sandbox " + req.SandboxID}
	}
	newAlloc := res.AllocatableNowMem + req.RequestedDelta
	if newAlloc > res.Capacity.MemoryBytes {
		newAlloc = res.Capacity.MemoryBytes
	}
	delta := newAlloc - res.AllocatableNowMem
	res.AllocatableNowMem = newAlloc
	s.State.Unlock()
	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("admin grant persist: %v", err)
	}
	s.Logf("admin grant sid=%s +%d → %d", req.SandboxID, delta, newAlloc)
	return &Message{Type: TypeAck, GrantedDelta: delta, NewAllocatable: newAlloc}
}

// handleAdminReclaim sets allocatable_now down to req.TargetAllocatable
// (clamped at floor) for a sandbox identified by SandboxID. The
// sandbox-ctl picks up the new value on its next Heartbeat and shrinks
// cgroup + balloon accordingly.
func (s *Server) handleAdminReclaim(req *Message) *Message {
	s.State.Lock()
	res := s.findBySandboxIDLocked(req.SandboxID)
	if res == nil {
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "no reservation for sandbox " + req.SandboxID}
	}
	target := req.TargetAllocatable
	if target < res.Floor.MemoryBytes {
		target = res.Floor.MemoryBytes
	}
	if target > res.AllocatableNowMem {
		// Reclaim is shrink-only — for grow use admin_grant.
		s.State.Unlock()
		return &Message{Type: TypeError, Msg: "target above current allocatable; use admin_grant to grow"}
	}
	delta := res.AllocatableNowMem - target
	res.AllocatableNowMem = target
	s.State.Unlock()
	if err := s.Persister.Flush(s.State); err != nil {
		s.Logf("admin reclaim persist: %v", err)
	}
	s.Logf("admin reclaim sid=%s -%d → %d", req.SandboxID, delta, target)
	return &Message{Type: TypeAck, NewAllocatable: target}
}

// handleAdminStatus returns a summary of node state without requiring
// the caller to read state.json directly.
func (s *Server) handleAdminStatus() *Message {
	s.State.Lock()
	defer s.State.Unlock()
	allocated := s.State.NodeAllocated()
	return &Message{
		Type:             TypeAck,
		Zone:             string(s.State.MemoryZone()),
		NodeAllocated:    allocated.MemoryBytes,
		AllocatablePool:  s.State.AllocatablePool.MemoryBytes,
		ReservationCount: len(s.State.Reservations),
		Drained:          s.Admission.IsDrained(),
	}
}

// findBySandboxIDLocked searches the reservation map for a sandbox by
// id. Caller holds State.Lock. Returns nil if not found.
func (s *Server) findBySandboxIDLocked(sid string) *Reservation {
	for _, r := range s.State.Reservations {
		if r.SandboxID == sid {
			return r
		}
	}
	return nil
}

// handleConnDrop is invoked by the connection goroutine after EOF /
// network error. Reservation is NOT immediately released — sandbox-ctl
// may reconnect within the reattach window.
func (s *Server) handleConnDrop(token string) {
	s.State.Lock()
	defer s.State.Unlock()
	res := s.State.Lookup(token)
	if res == nil {
		return
	}
	res.Conn = nil
	s.Logf("conn dropped for %s sid=%s (reservation pending reattach)",
		token[:8], res.SandboxID)
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
	i.State.Lock()

	swept := false
	for token, res := range i.State.Reservations {
		// Creating-stage TTL.
		if IsPreSettled(res.Stage) &&
			now.Sub(res.StageEnteredAt) > i.StartupTTL {
			i.Logf("sweep: token %s sid=%s exceeded startup TTL, releasing",
				token[:8], res.SandboxID)
			i.Allocator.CleanupHistory(token)
			delete(i.State.Reservations, token)
			swept = true
			continue
		}
		// Heartbeat staleness — only when conn is gone (live conn keeps
		// timestamp fresh enough).
		if res.Conn == nil && i.Heartbeat > 0 &&
			now.Sub(res.LastHeartbeatAt) > 3*i.Heartbeat {
			i.Logf("sweep: token %s sid=%s no heartbeat for %v, releasing",
				token[:8], res.SandboxID, now.Sub(res.LastHeartbeatAt))
			i.Allocator.CleanupHistory(token)
			delete(i.State.Reservations, token)
			swept = true
		}
	}
	i.State.Unlock()
	if swept {
		// Headroom may have just opened up — wake admission worker.
		i.Admission.PushWake()
	}
	if err := i.Persister.Flush(i.State); err != nil {
		log.Printf("[node-ctl] sweep persist: %v", err)
	}
}
