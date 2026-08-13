package nodectl

import (
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestAdmin_DrainBlocksAdmit(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	// Drain mode on.
	if err := c.AdminDrain(true); err != nil {
		t.Fatal(err)
	}
	if !srv.Admission.IsDrained() {
		t.Error("admission not drained after AdminDrain(true)")
	}

	// New Admit must be rejected.
	res, err := c.Admit(AdmitParams{
		SandboxID:           "blocked",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    32 << 20,
		StartupBudgetMemory: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRejected {
		t.Errorf("status=%s msg=%s, want rejected", res.Status, res.Msg)
	}

	// Disable drain.
	if err := c.AdminDrain(false); err != nil {
		t.Fatal(err)
	}
	if srv.Admission.IsDrained() {
		t.Error("admission still drained after AdminDrain(false)")
	}

	// Now admit succeeds.
	res, err = c.Admit(AdmitParams{
		SandboxID:           "ok",
		CapacityMemoryBytes: 256 << 20,
		FloorMemoryBytes:    32 << 20,
		StartupBudgetMemory: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusAdmitted {
		t.Errorf("status=%s, want admitted", res.Status)
	}
}

func TestAdmin_GrantOverridesPool(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	// Use a second client to admit a sandbox; first client stays as
	// admin-only (the API doesn't require an admit-token to call admin
	// verbs).
	c2 := &Client{SocketPath: srv.Path}
	if err := c2.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, err := c2.Admit(AdmitParams{
		SandboxID:           "sb-grant",
		CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 64 << 20,
	}); err != nil {
		t.Fatal(err)
	}

	newAlloc, err := c.AdminGrant("sb-grant", 256<<20)
	if err != nil {
		t.Fatal(err)
	}
	if newAlloc != 64<<20+256<<20 {
		t.Errorf("alloc=%d, want %d", newAlloc, 64<<20+256<<20)
	}

	// Server-side reservation should reflect new allocation.
	r := reservationForTest(t, srv.State, "sb-grant")
	if r.AllocatableNowMem != newAlloc {
		t.Errorf("reservation alloc=%v, want %d", r, newAlloc)
	}
}

func TestAdmin_ReclaimShrinksReservation(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	c2 := &Client{SocketPath: srv.Path}
	if err := c2.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, err := c2.Admit(AdmitParams{
		SandboxID:           "sb-rec",
		CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 512 << 20,
	}); err != nil {
		t.Fatal(err)
	}

	newAlloc, err := c.AdminReclaim("sb-rec", 128<<20)
	if err != nil {
		t.Fatal(err)
	}
	if newAlloc != 128<<20 {
		t.Errorf("alloc after reclaim = %d, want 128 MiB", newAlloc)
	}

	// Reclaiming below floor should clamp at floor.
	clamped, err := c.AdminReclaim("sb-rec", 1<<20) // 1 MiB < floor 64 MiB
	if err != nil {
		t.Fatal(err)
	}
	if clamped != 64<<20 {
		t.Errorf("clamped alloc = %d, want floor 64 MiB", clamped)
	}

	// Reclaim asking to grow should error.
	if _, err := c.AdminReclaim("sb-rec", 256<<20); err == nil {
		t.Error("expected error on reclaim-grow attempt")
	}
}

func TestAdmin_StatusReturnsZone(t *testing.T) {
	_, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	st, err := c.AdminStatus()
	if err != nil {
		t.Fatal(err)
	}
	if st.Zone != string(ZoneGreen) {
		t.Errorf("zone=%s, want green (empty)", st.Zone)
	}
	if st.AllocatablePool == 0 {
		t.Error("allocatable_pool not set")
	}
	if st.Drained {
		t.Error("drained=true on fresh server")
	}
}

func TestAdminListPaginatesThousandReservationsOverUDS(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 1<<50)
	defer cleanup()

	for i := 0; i < 1000; i++ {
		installReservationForTest(t, srv.State, Reservation{
			SandboxID: fmt.Sprintf("sandbox-%04d", i), PeerPID: 10000 + i,
			CgroupPath:        fmt.Sprintf("/sys/fs/cgroup/sandbox-%04d", i),
			Capacity:          Resources{MemoryBytes: 2 << 30, CPUMilli: 1000},
			Floor:             Resources{MemoryBytes: 64 << 20, CPUMilli: 100},
			AllocatableNowMem: 128 << 20, Stage: StageSettled,
			RecoverySource: RecoverySynced,
		})
	}

	reservations, err := c.AdminList()
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations) != 1000 {
		t.Fatalf("AdminList returned %d reservations, want 1000", len(reservations))
	}
	for i, reservation := range reservations {
		want := fmt.Sprintf("sandbox-%04d", i)
		if reservation.SandboxID != want {
			t.Fatalf("reservation %d SID = %q, want %q", i, reservation.SandboxID, want)
		}
	}

	// A large requested page is reduced to the largest complete frame that
	// fits, with the last returned SID as its continuation cursor.
	paged, err := net.Dial("unix", srv.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(paged, &Message{Type: TypeAdminList, ListLimit: 1024}); err != nil {
		_ = paged.Close()
		t.Fatal(err)
	}
	page, err := ReadMessage(paged)
	_ = paged.Close()
	if err != nil {
		t.Fatal(err)
	}
	if page.Type != TypeAck || len(page.Reservations) == 0 || len(page.Reservations) >= 1000 ||
		page.ListNext != page.Reservations[len(page.Reservations)-1].SandboxID {
		t.Fatalf("large AdminList page = type=%q count=%d next=%q", page.Type, len(page.Reservations), page.ListNext)
	}

	// A pre-pagination client must receive an explicit upgrade error when the
	// complete inventory cannot fit one frame, never a truncated success or an
	// unexplained transport close.
	legacy, err := net.Dial("unix", srv.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if err := WriteMessage(legacy, &Message{Type: TypeAdminList}); err != nil {
		t.Fatal(err)
	}
	resp, err := ReadMessage(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != TypeError || !strings.Contains(resp.Msg, "upgrade client for pagination") {
		t.Fatalf("legacy AdminList response = %+v", resp)
	}
}

func TestAdminListCursorIsExclusive(t *testing.T) {
	state := makeState(8<<30, 0)
	for i := 0; i < 5; i++ {
		installReservationForTest(t, state, Reservation{
			SandboxID: fmt.Sprintf("sandbox-%02d", i),
			Capacity:  Resources{MemoryBytes: 1 << 30}, Floor: Resources{MemoryBytes: 64 << 20},
			AllocatableNowMem: 64 << 20, Stage: StageSettled, RecoverySource: RecoveryAdmit,
		})
	}
	srv := &Server{State: state}
	legacy := srv.handleAdminList(&Message{Type: TypeAdminList})
	if legacy.Type != TypeAck || len(legacy.Reservations) != 5 || legacy.ListNext != "" {
		t.Fatalf("small legacy response = %+v", legacy)
	}
	first := srv.handleAdminList(&Message{Type: TypeAdminList, ListAfter: "sandbox-00", ListLimit: 2})
	if first.Type != TypeAck || len(first.Reservations) != 2 ||
		first.Reservations[0].SandboxID != "sandbox-01" ||
		first.Reservations[1].SandboxID != "sandbox-02" || first.ListNext != "sandbox-02" {
		t.Fatalf("first page = %+v", first)
	}
	last := srv.handleAdminList(&Message{Type: TypeAdminList, ListAfter: first.ListNext, ListLimit: 2})
	if last.Type != TypeAck || len(last.Reservations) != 2 ||
		last.Reservations[0].SandboxID != "sandbox-03" ||
		last.Reservations[1].SandboxID != "sandbox-04" || last.ListNext != "" {
		t.Fatalf("last page = %+v", last)
	}
}

func TestAdminListRejectsSingleReservationLargerThanFrame(t *testing.T) {
	state := makeState(8<<30, 0)
	installReservationForTest(t, state, Reservation{
		SandboxID: strings.Repeat("s", MaxMessageBytes),
		Capacity:  Resources{MemoryBytes: 1 << 30}, Floor: Resources{MemoryBytes: 64 << 20},
		AllocatableNowMem: 64 << 20, Stage: StageSettled, RecoverySource: RecoveryAdmit,
	})
	resp := (&Server{State: state}).handleAdminList(&Message{Type: TypeAdminList, ListLimit: 1})
	if resp.Type != TypeError || !strings.Contains(resp.Msg, "reservation exceeds protocol frame") {
		t.Fatalf("oversized reservation response = %+v", resp)
	}
}

func TestHeartbeat_ReturnsAllocatable(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	if _, err := c.Admit(AdmitParams{
		SandboxID:           "sb-hb",
		CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes:    64 << 20,
		StartupBudgetMemory: 256 << 20,
	}); err != nil {
		t.Fatal(err)
	}

	// First heartbeat: should reflect granted_initial_alloc.
	res, err := c.Heartbeat(120<<20, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.NewAllocatable != 256<<20 {
		t.Errorf("hb allocatable=%d, want 256 MiB", res.NewAllocatable)
	}

	// Server-side admin reclaim shrinks; next heartbeat surfaces the
	// new value to the client.
	if _, err := srv.State.AdminReclaim("sb-hb", 128<<20); err != nil {
		t.Fatal(err)
	}

	res, err = c.Heartbeat(120<<20, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.NewAllocatable != 128<<20 {
		t.Errorf("after shrink hb allocatable=%d, want 128 MiB", res.NewAllocatable)
	}
}
