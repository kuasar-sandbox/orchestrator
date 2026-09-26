#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

sid=resource-static
resource_setup_sandbox "$sid"
# Static mode deliberately omits the node controller: this proves the
# sandbox-local memory control path can deliver a real CH grow by itself.
resource_write_sandbox "$sid" 320 1024 512 false
python3 - "$WORK/$sid.yaml" <<'PY'
import sys
p=sys.argv[1]
s=open(p).read()
s=s.replace("    controller: "+__import__("os").environ["WORK"]+"/sandbox-resource.sock\n","")
s=s.replace('launch:\n  exec: /bin/sleep\n  args: ["300"]\n',
'''launch:
  exec: /usr/local/bin/python3
  env: { PYTHONUNBUFFERED: "1" }
  args:
    - "-c"
    - |
      import time
      blocks=[]
      print("control-workload-ready", flush=True)
      time.sleep(3)
      for _ in range(6):
          blocks.append(bytearray(48*1024*1024))
          for b in blocks[-1:]: b[::4096]=b"\\1"*(len(b)//4096)
          print("pressure", len(blocks), flush=True)
          time.sleep(1)
      print("workload done", flush=True)
      time.sleep(30)
''')
open(p,"w").write(s)
PY

resource_run_sandbox "$sid"
pid=$RESOURCE_LAST_PID
deadline=$((SECONDS+30))
while [ "$SECONDS" -lt "$deadline" ]; do
    if resource_memory_control_observed "$sid" && grep -q control-workload-ready "$WORK/$sid.log" 2>/dev/null; then break; fi
    kill -0 "$pid" || resource_fail "$sid exited before control readiness"
    sleep 0.2
done
resource_memory_control_observed "$sid" || resource_fail "memory control did not initialize"
read -r target_before actual_before <<<"$(resource_read_balloon "$sid")"
[[ "$target_before" =~ ^[0-9]+$ && "$actual_before" =~ ^[0-9]+$ ]] || resource_fail "invalid initial CH balloon state"
grows=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grows=0
if [ "$grows" -eq 0 ]; then resource_wait_local_grow "$sid" "$pid" "$grows" 30; fi

deadline=$((SECONDS+30))
target_after=$target_before
while [ "$SECONDS" -lt "$deadline" ]; do
    read -r target_after actual_after <<<"$(resource_read_balloon "$sid")" || true
    [ "$target_after" -lt "$target_before" ] && break
    kill -0 "$pid" || resource_fail "$sid exited before CH accepted grow"
    sleep 0.25
done
[ "$target_after" -lt "$target_before" ] || resource_fail "static grow did not reduce CH balloon target"
deadline=$((SECONDS+45))
while [ "$SECONDS" -lt "$deadline" ]; do
    grep -q 'workload done' "$WORK/$sid.log" && break
    kill -0 "$pid" || resource_fail "$sid exited before workload completion"
    sleep 0.25
done
grep -q 'workload done' "$WORK/$sid.log" || resource_fail "workload did not complete"
resource_assert_no_oom "$sid"
resource_shutdown_pid "$pid"
echo "PASS orchestrator.resource-control.sh"
