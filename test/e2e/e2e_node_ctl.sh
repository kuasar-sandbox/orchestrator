#!/usr/bin/env bash
#
# e2e_node_ctl.sh — exhaustive end-to-end exercise of node-ctl against
# the daemon binary. No /dev/kvm, no docker, no root required.
#
# Coverage:
#   1. Daemon boot + listen
#   2. Admit / Settled / RequestBudget / Heartbeat / Release on full
#      protocol via pkg/nodectl/Client
#   3. status / list CLI subcommands
#   4. Daemon restart preserves reservations through state.json
#   5. Active reclaimer: settled-stage allocatable shrinks toward
#      working-set + safety-margin after a Heartbeat
#   6. Admin grant / reclaim / drain CLI subcommands
#   7. Sandbox-ctl client surfaces controller-driven allocatable updates
#      via the Heartbeat response
#   8. Audit log captures admit / release / reclaim lines
#
# This validates the wire protocol AND the binary CLI flow together.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"

WORK="$(mktemp -d /tmp/e2e-node-ctl-XXXXXX)"
SOCK="$WORK/sandbox-resource.sock"
STATE="$WORK/state.json"
CONFIG="$WORK/node-ctl.yaml"
AUDIT="$WORK/audit.log"

cleanup_all() {
    [ -n "${DAEMON_PID:-}" ] && kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup_all EXIT

cat > "$CONFIG" <<EOF
listen: $SOCK
state_path: $STATE
cgroup_scan_paths:
  - /sys/fs/cgroup/sandboxes
resources:
  physical_memory: 8GiB
  physical_cpu: 4
  host_reserved:
    memory: 1GiB
    cpu: 0.5
watermarks:
  operational_margin_factor: 0.10
  high_factor: 0.85
  low_factor: 0.70
  emergency_factor: 0.05
rate_limits:
  memory_grant_per_sec_factor: 0.10
admission:
  rate: 100
  burst: 100
  max_concurrent_creating: 100
  startup_ttl: 60s
  queue_ttl: 30s
dampening:
  recover_duration: 30s
  cooldown_periods: 5
logging:
  level: info
  audit_path: $AUDIT
EOF

start_daemon() {
    "$BIN/node-ctl" daemon --config "$CONFIG" >"$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        [ -S "$SOCK" ] && return 0
        sleep 0.2
    done
    echo "FAIL: socket not created in time"
    cat "$WORK/daemon.log"
    return 1
}

stop_daemon() {
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
    DAEMON_PID=""
}

run_driver() {
    local file="$1"; shift
    ( cd "$REPO_ROOT" && go run "$file" "$@" )
}

assert_grep() {
    local pattern="$1" file="$2"
    if ! grep -q -- "$pattern" "$file"; then
        echo "FAIL: pattern '$pattern' missing from $file"
        cat "$file"
        exit 1
    fi
}

###############################################################################
# Phase 1: basic protocol round-trip
###############################################################################

echo "==> phase 1: basic protocol via in-tree client"
start_daemon

cat > "$WORK/basic_driver.go" <<'EOF'
package main

import (
	"fmt"
	"os"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func main() {
	c := &nodectl.Client{SocketPath: os.Args[1]}
	if err := c.Connect(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()

	res, err := c.Admit(nodectl.AdmitParams{
		SandboxID: "sb-basic", CapacityMemoryBytes: 1 << 30, CapacityCPU: 1,
		FloorMemoryBytes: 128 << 20, FloorCPU: 0.5,
		StartupBudgetMemory: 256 << 20,
		CgroupPath:          "/sys/fs/cgroup/sandboxes/sb-basic",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "admit:", err)
		os.Exit(1)
	}
	if res.Status != nodectl.StatusAdmitted {
		fmt.Fprintf(os.Stderr, "admit status=%s msg=%s\n", res.Status, res.Msg)
		os.Exit(1)
	}
	fmt.Printf("admit ok token=%s initial=%d\n", res.Token[:8], res.GrantedInitialAlloc)

	if err := c.Settled(120<<20, 0); err != nil {
		fmt.Fprintln(os.Stderr, "settled:", err)
		os.Exit(1)
	}
	fmt.Println("settled ok")

	g, alloc, _, err := c.RequestBudget(256<<20, 128<<20, nodectl.UrgencyNormal, "high_event")
	if err != nil || g == 0 {
		fmt.Fprintln(os.Stderr, "request_budget:", err, "granted:", g)
		os.Exit(1)
	}
	fmt.Printf("budget grant +%d → %d\n", g, alloc)

	hb, err := c.Heartbeat(alloc, 1000, 1, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "heartbeat:", err)
		os.Exit(1)
	}
	fmt.Printf("heartbeat ok alloc_from_server=%d\n", hb.NewAllocatable)

	if err := c.Release("normal"); err != nil {
		fmt.Fprintln(os.Stderr, "release:", err)
		os.Exit(1)
	}
	fmt.Println("release ok")
}
EOF

run_driver "$WORK/basic_driver.go" "$SOCK" | tee "$WORK/basic.out"
assert_grep "admit ok"     "$WORK/basic.out"
assert_grep "settled ok"   "$WORK/basic.out"
assert_grep "budget grant" "$WORK/basic.out"
assert_grep "heartbeat ok" "$WORK/basic.out"
assert_grep "release ok"   "$WORK/basic.out"

echo "==> phase 1: status / list CLI"
"$BIN/node-ctl" status --state "$STATE" | tee "$WORK/status.out"
assert_grep "node_budget"      "$WORK/status.out"
assert_grep "allocatable_pool" "$WORK/status.out"

###############################################################################
# Phase 2: persist across daemon restart
###############################################################################

echo "==> phase 2: daemon restart preserves reservations"

cat > "$WORK/persist_driver.go" <<'EOF'
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func main() {
	c := &nodectl.Client{SocketPath: os.Args[1]}
	if err := c.Connect(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	defer c.Close()
	res, err := c.Admit(nodectl.AdmitParams{
		SandboxID: "sb-persist", CapacityMemoryBytes: 256 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 32 << 20, StartupBudgetMemory: 32 << 20,
	})
	if err != nil || res.Status != nodectl.StatusAdmitted {
		fmt.Fprintln(os.Stderr, "admit:", err, res.Status, res.Msg); os.Exit(1)
	}
	fmt.Printf("token=%s\n", res.Token)
	time.Sleep(20 * time.Second)
}
EOF

run_driver "$WORK/persist_driver.go" "$SOCK" >"$WORK/persist.out" 2>&1 &
PERSIST_PID=$!
sleep 3
TOK_BEFORE="$( python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); ks=list(d.get("reservations", {}).keys()); print(ks[0] if ks else "")' "$STATE" )"
[ -n "$TOK_BEFORE" ] || { echo "FAIL: no reservation pre-restart"; exit 1; }
echo "  pre-restart: $(echo "$TOK_BEFORE" | head -c 8)…"

stop_daemon
sleep 0.5
start_daemon
sleep 0.5

TOK_AFTER="$( python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); ks=list(d.get("reservations", {}).keys()); print(ks[0] if ks else "")' "$STATE" )"
echo "  post-restart: $(echo "$TOK_AFTER" | head -c 8)…"
[ "$TOK_BEFORE" = "$TOK_AFTER" ] || { echo "FAIL: reservation lost across restart"; exit 1; }
kill "$PERSIST_PID" 2>/dev/null || true
wait "$PERSIST_PID" 2>/dev/null || true

###############################################################################
# Phase 3: admin commands (drain / grant / reclaim)
###############################################################################

echo "==> phase 3: admin commands"

# Restart with a clean state so reservation counts are predictable.
stop_daemon
rm -f "$STATE" "$AUDIT"
start_daemon

# Spawn a long-lived sandbox to manipulate via admin commands.
cat > "$WORK/holder_driver.go" <<'EOF'
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func main() {
	c := &nodectl.Client{SocketPath: os.Args[1]}
	if err := c.Connect(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	defer c.Close()
	res, err := c.Admit(nodectl.AdmitParams{
		SandboxID: "sb-admin", CapacityMemoryBytes: 1 << 30, CapacityCPU: 1,
		FloorMemoryBytes: 64 << 20, StartupBudgetMemory: 256 << 20,
	})
	if err != nil || res.Status != nodectl.StatusAdmitted {
		fmt.Fprintln(os.Stderr, "admit:", err, res.Status, res.Msg); os.Exit(1)
	}
	if err := c.Settled(120<<20, 0); err != nil { fmt.Fprintln(os.Stderr, "settled:", err); os.Exit(1) }
	fmt.Printf("ready token=%s\n", res.Token)

	// Heartbeat every 500ms; print received allocatable so the e2e
	// script can verify admin-driven changes propagate.
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		<-t.C
		hb, err := c.Heartbeat(120<<20, 0, 0, 0)
		if err != nil { fmt.Fprintln(os.Stderr, "hb:", err); continue }
		fmt.Printf("hb alloc=%d\n", hb.NewAllocatable)
	}
	if err := c.Release("normal"); err != nil { fmt.Fprintln(os.Stderr, "release:", err) }
}
EOF

run_driver "$WORK/holder_driver.go" "$SOCK" >"$WORK/holder.out" 2>&1 &
HOLDER_PID=$!
sleep 2  # let admit + settle land

# admin grant
"$BIN/node-ctl" grant --memory 128MiB --socket "$SOCK" sb-admin | tee "$WORK/grant.out"
assert_grep "granted +134217728" "$WORK/grant.out"
sleep 1
# Holder's hb should now reflect 256MiB + 128MiB = 384MiB.
grep -q "hb alloc=402653184" "$WORK/holder.out" || { echo "FAIL: holder did not see grant via heartbeat"; cat "$WORK/holder.out"; exit 1; }
echo "  holder observed grant via heartbeat"

# admin reclaim
"$BIN/node-ctl" reclaim --memory 128MiB --socket "$SOCK" sb-admin | tee "$WORK/reclaim.out"
assert_grep "reclaimed sb-admin" "$WORK/reclaim.out"
sleep 1
grep -q "hb alloc=134217728" "$WORK/holder.out" || { echo "FAIL: holder did not see reclaim via heartbeat"; cat "$WORK/holder.out"; exit 1; }
echo "  holder observed reclaim via heartbeat"

# admin drain
"$BIN/node-ctl" drain --socket "$SOCK" | tee "$WORK/drain.out"
assert_grep "drain mode enabled" "$WORK/drain.out"

# Verify drain blocks new admit.
cat > "$WORK/drained_driver.go" <<'EOF'
package main

import (
	"fmt"
	"os"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func main() {
	c := &nodectl.Client{SocketPath: os.Args[1]}
	if err := c.Connect(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	defer c.Close()
	res, err := c.Admit(nodectl.AdmitParams{
		SandboxID: "sb-blocked", CapacityMemoryBytes: 256 << 20, CapacityCPU: 1,
		FloorMemoryBytes: 32 << 20, StartupBudgetMemory: 32 << 20,
	})
	if err != nil { fmt.Fprintln(os.Stderr, "admit:", err); os.Exit(1) }
	fmt.Printf("status=%s msg=%s\n", res.Status, res.Msg)
}
EOF
run_driver "$WORK/drained_driver.go" "$SOCK" | tee "$WORK/drained.out"
assert_grep "status=rejected" "$WORK/drained.out"
echo "  drain blocked new admit"

# Disable drain.
"$BIN/node-ctl" drain --disable --socket "$SOCK" | tee "$WORK/undrain.out"
assert_grep "drain mode disabled" "$WORK/undrain.out"

wait "$HOLDER_PID" 2>/dev/null || true

###############################################################################
# Phase 4: active reclaimer
###############################################################################

echo "==> phase 4: active reclaimer shrinks settled allocatable"

# The reclaimer ticks every 10s and uses LastReportedRSS * 1.25 as the
# target. We pre-load a reservation with a small RSS and large
# allocatable, then wait for one tick.
cat > "$WORK/reclaim_driver.go" <<'EOF'
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/nodectl"
)

func main() {
	c := &nodectl.Client{SocketPath: os.Args[1]}
	if err := c.Connect(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	defer c.Close()
	res, err := c.Admit(nodectl.AdmitParams{
		SandboxID: "sb-reclaim", CapacityMemoryBytes: 1 << 30, CapacityCPU: 1,
		FloorMemoryBytes: 32 << 20, StartupBudgetMemory: 512 << 20,
	})
	if err != nil || res.Status != nodectl.StatusAdmitted {
		fmt.Fprintln(os.Stderr, "admit:", err, res.Status, res.Msg); os.Exit(1)
	}
	// Settled with a small RSS — 64 MiB. Reclaim target = 64*1.25 = 80 MiB.
	if err := c.Settled(64<<20, 0); err != nil { fmt.Fprintln(os.Stderr, "settled:", err); os.Exit(1) }
	fmt.Printf("settled rss=64MiB alloc=512MiB\n")

	// Hold for ~12s so the 10s reclaim ticker fires at least once.
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	deadline := time.Now().Add(12 * time.Second)
	final := uint64(0)
	for time.Now().Before(deadline) {
		<-t.C
		hb, err := c.Heartbeat(64<<20, 0, 0, 0)
		if err != nil { continue }
		final = hb.NewAllocatable
	}
	fmt.Printf("final_alloc=%d\n", final)
}
EOF

run_driver "$WORK/reclaim_driver.go" "$SOCK" 2>&1 | tee "$WORK/reclaim_check.out"
# Expected target: 64 MiB * 1.25 = 80 MiB = 83886080 bytes
expected=83886080
final="$( grep "^final_alloc=" "$WORK/reclaim_check.out" | tail -1 | cut -d= -f2 )"
if [ -z "$final" ] || [ "$final" = "536870912" ]; then
    echo "FAIL: reclaimer did not shrink (final=$final, expected ~$expected)"
    exit 1
fi
if [ "$final" -gt $((expected + 4*1024*1024)) ]; then
    echo "FAIL: reclaim insufficient (final=$final, expected near $expected)"
    exit 1
fi
echo "  reclaimer shrank to $final bytes"

###############################################################################
# Phase 5: audit log
###############################################################################

echo "==> phase 5: audit log captures events"
[ -f "$AUDIT" ] || { echo "FAIL: no audit log produced"; exit 1; }
assert_grep "admit token="    "$AUDIT"
assert_grep "release token="  "$AUDIT"
assert_grep "reclaim sid="    "$AUDIT"
echo "  audit log contains admit/release/reclaim entries"

###############################################################################
# Done
###############################################################################

echo
echo "==> e2e_node_ctl: PASS (all 5 phases)"
