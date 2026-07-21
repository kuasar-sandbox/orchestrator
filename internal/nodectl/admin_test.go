package nodectl

import (
	"context"
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
	res, err := c.Admit(context.Background(), AdmitParams{
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
	res, err = c.Admit(context.Background(), AdmitParams{
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
	if _, err := c2.Admit(context.Background(), AdmitParams{
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
	srv.State.Lock()
	r := srv.findBySandboxIDLocked("sb-grant")
	if r == nil || r.AllocatableNowMem != newAlloc {
		t.Errorf("reservation alloc=%v, want %d", r, newAlloc)
	}
	srv.State.Unlock()
}

func TestAdmin_ReclaimShrinksReservation(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	c2 := &Client{SocketPath: srv.Path}
	if err := c2.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, err := c2.Admit(context.Background(), AdmitParams{
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

func TestHeartbeat_ReturnsAllocatable(t *testing.T) {
	srv, c, cleanup := startTestServer(t, 8<<30)
	defer cleanup()

	if _, err := c.Admit(context.Background(), AdmitParams{
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
	srv.State.Lock()
	r := srv.findBySandboxIDLocked("sb-hb")
	r.AllocatableNowMem = 128 << 20
	srv.State.Unlock()

	res, err = c.Heartbeat(120<<20, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.NewAllocatable != 128<<20 {
		t.Errorf("after shrink hb allocatable=%d, want 128 MiB", res.NewAllocatable)
	}
}
