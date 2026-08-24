package nodectl

import (
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"
)

// AdmissionPolicy bounds how new sandboxes are admitted.
//
// Rate / Burst form the admission token bucket (request-level rate limit).
// StartupTTL bounds how long an admit's reservation can persist without
// reaching Settled (after which the IdleSweeper releases it as a creation
// failure). QueueTTL bounds how long a short-term-blocked admit can wait
// in the server-side FIFO queue before being rejected. QueueMaxDepth
// caps the queue itself (admits arriving while at the cap are rejected
// with queue_full).
//
// Concurrency is NOT capped by a hardcoded count any more; it is bounded
// by startup_pool (watermarks.startup_factor × allocatable_pool), so the
// effective concurrent-creating count varies with per-sandbox startup
// budget size.
type AdmissionPolicy struct {
	Rate          int // token bucket fill rate (tokens/sec)
	Burst         int // token bucket capacity
	StartupTTL    time.Duration
	QueueTTL      time.Duration
	QueueMaxDepth int
}

// BlockReason classifies why a request cannot be admitted right now.
type BlockReason int

const (
	BlockNone              BlockReason = iota
	BlockedByMainBudget                // initial Budget > main headroom (waits for Settled/Released)
	BlockedByStartupBudget             // initial Budget > pre-settled headroom (waits for Settled/Released-before-settled)
	BlockedByTokenBucket               // token bucket empty (waits for refill — uses one-shot timer)
)

// Outcome captures a decision the admission worker can take.
type OutcomeStatus int

const (
	OutcomeAdmitted       OutcomeStatus = iota
	OutcomeLongTermReject               // drained / zone red / exceeds pool
	OutcomeShortTermBlock               // entry should wait in queue
	OutcomePreCheckReject               // request shape itself is invalid (e.g. burst > pool)
)

// Outcome is what TryAdmit/AnalyzeBlock returns.
type Outcome struct {
	Status     OutcomeStatus
	Block      BlockReason // populated when Status==OutcomeShortTermBlock
	RejectMsg  string
	RejectCode string // machine-readable reason: drained / zone_critical / exceeds_node_capacity / exceeds_startup_pool / invalid_burst
}

// PendingAdmit is a server-side queued admit. The connection is held
// open until the worker writes either an Admitted or Rejected response.
type PendingAdmit struct {
	req       *Message // the full Admit request
	conn      net.Conn
	peerPID   int
	queuedAt  time.Time
	queuedPos int // queue depth at insertion (informational, for metadata)

	// Closed by the per-entry TTL timer. Worker checks on each sweep.
	// Client-side disconnect is detected lazily: the worker's response
	// WriteMessage fails with EPIPE, clears the just-inserted reservation's
	// connection, and closes the conn. A disconnect after a successful response
	// is cleared by serveConn's queued-SID fallback until the first token-bearing
	// request arrives.
	// Proactive EOF read here is unsafe: it shares the conn with the
	// post-admit serveConn read loop and would race for bytes.
	cancelCh   chan struct{}
	ttlTimer   *time.Timer
	cancelOnce sync.Once // ensure cancelCh closed at most once
}

func (p *PendingAdmit) cancel() {
	p.cancelOnce.Do(func() { close(p.cancelCh) })
}

// AdmissionController owns the token bucket, the drain switch, and the
// server-side FIFO queue + worker. When both are involved, queueMu precedes a
// State method; State never calls back into AdmissionController.
type AdmissionController struct {
	policy AdmissionPolicy

	// Token bucket — only mutated under tokenMu.
	tokenMu    sync.Mutex
	tokens     float64
	lastRefill time.Time

	// Drain flag — separate mutex (independent state).
	drainMu sync.Mutex
	drained bool

	// Queue + worker coordination.
	queueMu    sync.Mutex
	queue      *list.List // *PendingAdmit
	wakeCh     chan struct{}
	tokenTimer *time.Timer // one-shot, set when head is BlockedByTokenBucket

	// Worker fan-outs.
	state     *State   // for budget checks
	auditor   *Auditor // optional
	logf      func(string, ...any)
	processFn func(*PendingAdmit) (*Message, error) // builds the AdmitResponse (reservation insert etc.)
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NewAdmissionController initializes with a full token bucket and empty
// queue. wire the queue worker via SetWiring before calling Run.
func NewAdmissionController(policy AdmissionPolicy) *AdmissionController {
	return &AdmissionController{
		policy:     policy,
		tokens:     float64(policy.Burst),
		lastRefill: time.Now(),
		queue:      list.New(),
		wakeCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
}

// SetWiring connects the controller to the State + audit + admit-builder
// callback. Must be called before Run.
//
// admitBuilder is invoked under queueMu (not under state.Lock — it takes
// state.Lock internally to insert the reservation, build the admit
// response message, then returns it for the worker to write to conn).
func (a *AdmissionController) SetWiring(
	state *State,
	auditor *Auditor,
	logf func(string, ...any),
	admitBuilder func(*PendingAdmit) (*Message, error),
) {
	a.state = state
	a.auditor = auditor
	a.logf = logf
	a.processFn = admitBuilder
}

// Run starts the worker goroutine. Returns immediately. Use Stop to halt.
func (a *AdmissionController) Run() {
	if a.processFn == nil {
		panic("AdmissionController.Run: SetWiring not called")
	}
	go a.workerLoop()
}

// Stop signals the worker to exit and waits for it.
func (a *AdmissionController) Stop() {
	close(a.stopCh)
	<-a.stoppedCh
}

func (a *AdmissionController) workerLoop() {
	defer close(a.stoppedCh)
	for {
		select {
		case <-a.wakeCh:
		case <-a.stopCh:
			a.drainQueueOnStop()
			return
		}
		a.processQueue()
	}
}

// pushWake signals the worker. Non-blocking; multiple coalesced wakes
// are fine since processQueue is idempotent.
func (a *AdmissionController) pushWake() {
	select {
	case a.wakeCh <- struct{}{}:
	default:
	}
}

// PushWake is the exported wake — server uses it after Settled/Released
// to ask the worker to re-evaluate the queue.
func (a *AdmissionController) PushWake() { a.pushWake() }

// processQueue is the worker's single pass: clean cancelled / TTL-expired
// entries, then FIFO-try admit head until head is blocked or queue empty.
// On token-bucket-only block, schedules a one-shot refill timer.
func (a *AdmissionController) processQueue() {
	a.queueMu.Lock()
	defer a.queueMu.Unlock()

	// 1. cancelled (conn EOF / TTL fired)
	for e := a.queue.Front(); e != nil; {
		next := e.Next()
		p := e.Value.(*PendingAdmit)
		select {
		case <-p.cancelCh:
			a.queue.Remove(e)
			// reply with Rejected only if conn still alive — write may fail
			// on EOF case; ignore.
			_ = WriteMessage(p.conn, &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Reason: "queue_canceled",
				Msg:    "queued admit canceled (TTL or client disconnect)",
			})
			_ = p.conn.Close()
			elapsedMs := int64(time.Since(p.queuedAt) / time.Millisecond)
			if a.auditor != nil {
				a.auditor.Logf("admit_queue_canceled sid=%s waited_ms=%d", p.req.SandboxID, elapsedMs)
			}
		default:
		}
		e = next
	}

	// 2. FIFO admit head
	for a.queue.Len() > 0 {
		head := a.queue.Front().Value.(*PendingAdmit)
		oc := a.analyzeRequest(head.req)
		switch oc.Status {
		case OutcomeAdmitted:
			// Consume token before committing the admit. If another admit
			// raced us to the last token, fall back to short-term block
			// behavior (head stays in queue, token-refill timer set).
			if !a.consumeToken() {
				a.resetTokenTimer()
				return
			}
			a.queue.Remove(a.queue.Front())
			head.ttlTimer.Stop()
			resp, err := a.processFn(head)
			if err != nil {
				_ = WriteMessage(head.conn, &Message{
					Type: TypeAdmitResponse, Status: StatusRejected, Msg: err.Error(),
				})
				_ = head.conn.Close()
				if a.auditor != nil {
					a.auditor.Logf("admit_queue_build_failed sid=%s err=%q", head.req.SandboxID, err.Error())
				}
				continue
			}
			resp.QueuedForMs = int64(time.Since(head.queuedAt) / time.Millisecond)
			resp.QueuePosAtIn = int64(head.queuedPos)
			if err := WriteMessage(head.conn, resp); err != nil {
				// Client gave up while queued; the reservation was just
				// inserted by processFn. Clear its connection before closing
				// so a stale pointer is not treated as liveness evidence.
				if a.state != nil {
					a.state.DropSandboxConnection(head.req.SandboxID, head.conn)
				}
				_ = head.conn.Close()
				if a.auditor != nil {
					a.auditor.Logf("admit_queue_write_failed sid=%s err=%q",
						head.req.SandboxID, err.Error())
				}
			}
			// Conn left open on success: caller will continue to use it
			// for RPC. No separate audit event here — the canonical
			// `admit token=...` line was already emitted by buildAdmitOK
			// (via processFn). The fact that this admit came from the
			// queue is reflected in the response's QueuedForMs metadata
			// that the client logs.
			continue

		case OutcomeLongTermReject:
			a.queue.Remove(a.queue.Front())
			head.ttlTimer.Stop()
			_ = WriteMessage(head.conn, &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Reason: oc.RejectCode,
				Msg:    oc.RejectMsg,
			})
			_ = head.conn.Close()
			if a.auditor != nil {
				a.auditor.Logf("admit_queue_dropped_long sid=%s reason=%s",
					head.req.SandboxID, oc.RejectCode)
			}
			continue

		case OutcomeShortTermBlock:
			// Head still blocked. Stop here — strict FIFO.
			if oc.Block == BlockedByTokenBucket {
				a.resetTokenTimer()
			}
			return

		case OutcomePreCheckReject:
			// Should never reach the queue (pre-check happens at admit
			// entry). Defensive: drop with the reject reason.
			a.queue.Remove(a.queue.Front())
			head.ttlTimer.Stop()
			_ = WriteMessage(head.conn, &Message{
				Type:   TypeAdmitResponse,
				Status: StatusRejected,
				Reason: oc.RejectCode,
				Msg:    oc.RejectMsg,
			})
			_ = head.conn.Close()
			continue
		}
	}
}

// resetTokenTimer arms a one-shot wake for when the next token would
// arrive. Caller must hold queueMu.
func (a *AdmissionController) resetTokenTimer() {
	if a.tokenTimer != nil {
		a.tokenTimer.Stop()
	}
	eta := a.nextTokenETA()
	if eta <= 0 {
		eta = 50 * time.Millisecond // floor: don't spin
	}
	a.tokenTimer = time.AfterFunc(eta, a.pushWake)
}

// nextTokenETA computes how long until the bucket has at least one token,
// based on the current refilled state.
func (a *AdmissionController) nextTokenETA() time.Duration {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	a.refillLocked()
	if a.tokens >= 1 {
		return 0
	}
	need := 1 - a.tokens
	rate := float64(a.policy.Rate)
	if rate <= 0 {
		return time.Hour // disabled / config error — sleep long, rely on other events
	}
	return time.Duration(need / rate * float64(time.Second))
}

// drainQueueOnStop replies Rejected to every pending entry as the worker
// shuts down (typically daemon stop).
func (a *AdmissionController) drainQueueOnStop() {
	a.queueMu.Lock()
	defer a.queueMu.Unlock()
	for e := a.queue.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PendingAdmit)
		p.ttlTimer.Stop()
		_ = WriteMessage(p.conn, &Message{
			Type:   TypeAdmitResponse,
			Status: StatusRejected,
			Reason: "daemon_shutting_down",
			Msg:    "node-ctl shutting down",
		})
		_ = p.conn.Close()
	}
	a.queue.Init()
	if a.tokenTimer != nil {
		a.tokenTimer.Stop()
	}
}

// refillLocked replenishes tokens based on elapsed time. Caller holds tokenMu.
func (a *AdmissionController) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(a.lastRefill).Seconds()
	a.tokens += elapsed * float64(a.policy.Rate)
	if a.tokens > float64(a.policy.Burst) {
		a.tokens = float64(a.policy.Burst)
	}
	a.lastRefill = now
}

// consumeToken decrements the bucket by one, refilling first. Returns
// true if a token was consumed.
func (a *AdmissionController) consumeToken() bool {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	a.refillLocked()
	if a.tokens < 1 {
		return false
	}
	a.tokens -= 1
	return true
}

// AnalyzeRequest is the exported decision function (used by server on
// initial admit, and internally by the worker for queued head).
//
// Returns OutcomePreCheckReject for shape errors, OutcomeLongTermReject
// for drain / zone-red / exceeds-pool, OutcomeShortTermBlock for
// resource-shortage that may clear with time, OutcomeAdmitted if the
// request can be granted right now.
//
// Note: this does NOT consume a token (it only checks). The caller (or
// the worker) calls ConsumeToken explicitly when ready to admit.
func (a *AdmissionController) AnalyzeRequest(req *Message) Outcome {
	return a.analyzeRequest(req)
}

func (a *AdmissionController) analyzeRequest(req *Message) Outcome {
	// 1. drain
	a.drainMu.Lock()
	drained := a.drained
	a.drainMu.Unlock()
	if drained {
		return Outcome{
			Status:     OutcomeLongTermReject,
			RejectCode: "drained",
			RejectMsg:  "controller is draining",
		}
	}
	if req == nil || req.CapacityMemoryBytes == 0 || req.FloorMemoryBytes == 0 ||
		req.FloorMemoryBytes > req.CapacityMemoryBytes || req.StartupBudgetMemory == 0 ||
		req.StartupBudgetMemory > req.CapacityMemoryBytes ||
		req.AllocatableAtSnapshot > req.CapacityMemoryBytes {
		return Outcome{
			Status:     OutcomePreCheckReject,
			RejectCode: "invalid_resource_contract",
			RejectMsg:  "memory Capacity, settled headroom, cold InitialBudget, or BudgetAtSnapshot is outside its valid range",
		}
	}

	// 2. Select the exact initial reservation. Cold start uses the already
	// aligned StartupBudgetMemory. Restore uses AllocatableAtSnapshot, whose
	// wire name is retained but whose value is BudgetAtSnapshot. Memory
	// headroom is an independent steady-policy input and is never folded into
	// this admission amount.
	initialBudget := initialReservationBudget(req)
	if initialBudget == 0 {
		return Outcome{
			Status:     OutcomePreCheckReject,
			RejectCode: "invalid_initial_budget",
			RejectMsg:  "selected initial reservation is zero",
		}
	}

	snapshot := a.state.AdmissionSnapshot()
	pool := snapshot.Pool.MemoryBytes
	startupPool := snapshot.StartupPool
	emerg := snapshot.EmergencyMemory
	mainReserved := snapshot.Reserved.MemoryBytes
	startupInFlight := snapshot.StartupInFlight
	zone := snapshot.Zone

	// 3. pre-check absolute capacity
	if initialBudget > pool {
		return Outcome{
			Status:     OutcomePreCheckReject,
			RejectCode: "exceeds_node_capacity",
			RejectMsg:  fmt.Sprintf("initial reservation %d > pool %d", initialBudget, pool),
		}
	}
	if initialBudget > startupPool {
		return Outcome{
			Status:     OutcomePreCheckReject,
			RejectCode: "exceeds_startup_pool",
			RejectMsg:  fmt.Sprintf("initial reservation %d > startup_pool %d", initialBudget, startupPool),
		}
	}

	// 4. zone red/critical — system protection (independent of pool math)
	if zone == ZoneRed || zone == ZoneCritical {
		return Outcome{
			Status:     OutcomeLongTermReject,
			RejectCode: "zone_critical",
			RejectMsg:  fmt.Sprintf("node in zone %s, refusing new admission", zone),
		}
	}

	// 5. budget headroom — both gates
	mainHeadroom := uint64(0)
	if mainReserved <= pool && emerg <= pool-mainReserved {
		mainHeadroom = pool - mainReserved - emerg
	}
	if initialBudget > mainHeadroom {
		return Outcome{
			Status: OutcomeShortTermBlock,
			Block:  BlockedByMainBudget,
		}
	}

	startupHeadroom := uint64(0)
	if startupPool > startupInFlight {
		startupHeadroom = startupPool - startupInFlight
	}
	if initialBudget > startupHeadroom {
		return Outcome{
			Status: OutcomeShortTermBlock,
			Block:  BlockedByStartupBudget,
		}
	}

	// 6. token bucket
	a.tokenMu.Lock()
	a.refillLocked()
	tokenAvail := a.tokens >= 1
	a.tokenMu.Unlock()
	if !tokenAvail {
		return Outcome{
			Status: OutcomeShortTermBlock,
			Block:  BlockedByTokenBucket,
		}
	}

	return Outcome{Status: OutcomeAdmitted}
}

// initialReservationBudget is the reservation-side adapter for the unchanged
// wire contract. A non-zero AllocatableAtSnapshot is BudgetAtSnapshot and
// selects restore admission; otherwise StartupBudgetMemory is the exact cold
// initial Budget. FloorMemoryBytes is settled guest headroom and does not
// participate.
func initialReservationBudget(req *Message) uint64 {
	if req.AllocatableAtSnapshot != 0 {
		return req.AllocatableAtSnapshot
	}
	return req.StartupBudgetMemory
}

// ConsumeToken decrements the token bucket by one. Caller must have
// just observed tokenAvail==true via AnalyzeRequest, then build the
// reservation and call this to charge the token. Returns false if the
// race lost (some other admit beat us to the token); caller should
// re-analyze.
func (a *AdmissionController) ConsumeToken() bool {
	return a.consumeToken()
}

// Enqueue inserts a pending admit at the tail. Caller is responsible
// for arranging conn-EOF monitoring (the goroutine that calls
// PendingAdmit.cancel on read EOF). Returns false if queue is at cap.
func (a *AdmissionController) Enqueue(req *Message, conn net.Conn, peerPID int) (*PendingAdmit, bool) {
	a.queueMu.Lock()
	defer a.queueMu.Unlock()
	if a.queue.Len() >= a.policy.QueueMaxDepth {
		return nil, false
	}
	p := &PendingAdmit{
		req:       req,
		conn:      conn,
		peerPID:   peerPID,
		queuedAt:  time.Now(),
		queuedPos: a.queue.Len(),
		cancelCh:  make(chan struct{}),
	}
	p.ttlTimer = time.AfterFunc(a.policy.QueueTTL, func() {
		p.cancel()
		a.pushWake()
	})
	a.queue.PushBack(p)
	a.pushWake()
	return p, true
}

// SetDrained toggles drain mode. Returns previous value.
func (a *AdmissionController) SetDrained(v bool) bool {
	a.drainMu.Lock()
	defer a.drainMu.Unlock()
	prev := a.drained
	a.drained = v
	return prev
}

// IsDrained reports whether drain mode is active.
func (a *AdmissionController) IsDrained() bool {
	a.drainMu.Lock()
	defer a.drainMu.Unlock()
	return a.drained
}

// QueueDepth returns the current queue length (for diagnostics).
func (a *AdmissionController) QueueDepth() int {
	a.queueMu.Lock()
	defer a.queueMu.Unlock()
	return a.queue.Len()
}

// NewToken returns a fresh random opaque token. 16 bytes hex = 32 chars.
func NewToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
