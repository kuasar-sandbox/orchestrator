#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy
# This prerequisite is built through the real API from the prepared image.
register source-image
SOURCE_TID="$TID"; SOURCE_BID="$BID"
code=$(req POST "/v2/templates/$SOURCE_TID/builds/$SOURCE_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "source image trigger=$code"; }
wait_ready "$SOURCE_TID" "$SOURCE_BID" source-image null img
SOURCE_IMAGE="$PERSIST"
# ---- BM: Build Register MMDS in a real builder guest -----------------------
echo "==> BM: Build Register MMDS routes/initial secret, Trigger immutability, real guest GET"
MMDS_BUILD_SECRET=MMDS_BUILD_SECRET_GUEST_E2E
REQ_MMDS_HEADER='{"secrets":{"build_secret":"MMDS_BUILD_SECRET_GUEST_E2E"},"routes":[{"path":"/e2e/build-static","data":"MMDS_BUILD_STATIC_GUEST_E2E"},{"path":"/e2e/build-secret","type":"secret","secret":"build_secret"},{"path":"/e2e/build-unresolved","type":"secret","secret":"build_unresolved"}]}'
register e2e-mmds e2b '{"kind":"sandbox","memory":true}' 1
BM_TID="$TID"; BM_BID="$BID"

# Both Trigger entry points are forbidden from replacing Register's MMDS.
REQ_MMDS_HEADER='{"routes":[]}'
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = 400 ] || { cat "$WORK/resp.body"; fail "BM Trigger MMDS header override=$code (want 400)"; }
unset REQ_MMDS_HEADER
BM_OVERRIDE_BODY=$(python3 - "$PULL_REF" <<'PY'
import json, sys
print(json.dumps({
    "fromImage": sys.argv[1],
    "metadata": {"kuasar-sandbox.mmds": json.dumps({"routes": []})},
}))
PY
)
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "$BM_OVERRIDE_BODY")
[ "$code" = 400 ] || { cat "$WORK/resp.body"; fail "BM Trigger MMDS metadata override=$code (want 400)"; }

BM_SECRET_HASH="$(printf '%s' "$MMDS_BUILD_SECRET" | sha256sum | cut -d' ' -f1)"
BM_RUN=$(python3 - "$BM_SECRET_HASH" <<'PY'
import base64, sys
expected_hash = sys.argv[1]
program = f'''
import hashlib, http.client

def request(method, path, token=None):
    conn = http.client.HTTPConnection("169.254.169.254", 80, timeout=10)
    headers = {{}}
    if method == "PUT":
        headers["X-metadata-token-ttl-seconds"] = "60"
    if token:
        headers["X-metadata-token"] = token
    conn.request(method, path, headers=headers)
    response = conn.getresponse()
    result = response.status, {{k.lower(): v for k, v in response.getheaders()}}, response.read()
    conn.close()
    return result

status, headers, body = request("PUT", "/latest/api/token")
assert status == 200, status
token = body.decode()
status, headers, body = request("GET", "/e2e/build-static", token)
assert status == 200 and body == b"MMDS_BUILD_STATIC_GUEST_E2E", (status, body)
assert headers.get("content-type") == "text/plain", headers
status, headers, body = request("GET", "/e2e/build-secret", token)
assert status == 200 and hashlib.sha256(body).hexdigest() == {expected_hash!r}, "build secret response mismatch"
assert headers.get("content-type") == "text/plain", headers
status, headers, body = request("GET", "/e2e/build-unresolved", token)
assert status == 404, (status, body)
print("MMDS_BUILD_GUEST_OK")
'''
encoded = base64.b64encode(program.encode()).decode()
print("python3 -c 'import base64; exec(base64.b64decode(\"%s\"))'" % encoded)
PY
)
BM_BODY=$(python3 - "$PULL_REF" "$BM_RUN" <<'PY'
import json, sys
print(json.dumps({"fromImage": sys.argv[1], "steps": [{"type": "RUN", "args": [sys.argv[2]]}]}))
PY
)
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "$BM_BODY")
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "BM trigger=$code (want 202)"; }
wait_ready "$BM_TID" "$BM_BID" BM '{"kind":"sandbox","memory":true}' snp
BM_PERSIST="$PERSIST"
case "$BM_PERSIST" in e2b-snp-*) : ;; *) fail "BM persist=$BM_PERSIST (want e2b-snp-…)";; esac

journalctl KUASAR_BUILD_ID="$BM_BID" --no-pager --output=cat >"$WORK/bm-mmds.journal" 2>/dev/null || true
grep -q 'MMDS_BUILD_GUEST_OK' "$WORK/bm-mmds.journal" \
    || { diag "$BM_BID"; fail "BM real builder guest did not report MMDS_BUILD_GUEST_OK"; }
python3 - "$WORK/lib/node-ctl.db" "$BM_BID" "$MMDS_BUILD_SECRET" <<'PY'
import json, pathlib, sqlite3, sys
db_path, build_id, secret = sys.argv[1:]
with sqlite3.connect(db_path, timeout=5) as db:
    row = db.execute("select metadata_json from builds where build_id=?", (build_id,)).fetchone()
    secret_rows = db.execute(
        "select count(*) from build_mmds_route_secret_values where build_id=?", (build_id,)
    ).fetchone()[0]
assert row is not None, "BM build row missing"
metadata = json.loads(row[0])
assert "kuasar-sandbox.mmds" not in metadata, "BM terminal build retained builder-only MMDS routes"
assert secret not in row[0], "BM secret leaked into build metadata"
assert secret_rows == 0, "BM terminal cleanup left a build secret row"
for path in pathlib.Path(db_path).parent.glob(pathlib.Path(db_path).name + "*"):
    assert secret.encode() not in path.read_bytes(), f"BM plaintext found in {path}"
PY
BM_REF=$(persist_ref "$BM_PERSIST") || fail "BM persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BM_REF" >"$WORK/bm-snapshot.json" 2>"$WORK/bm-snapshot.err" \
    || { cat "$WORK/bm-snapshot.err"; fail "sandbox-ctl info BM snapshot"; }
for artifact in "$WORK/orch.log" "$WORK/bm-mmds.journal" "$WORK/bm-snapshot.json" "$WORK/bm-snapshot.err"; do
    grep -a -F -q -- "$MMDS_BUILD_SECRET" "$artifact" \
        && fail "Build Register MMDS secret plaintext appeared in $artifact"
done
grep -Fq 'kuasar-sandbox.mmds' "$WORK/bm-snapshot.json" \
    && fail "Build Register MMDS routes leaked into final snapshot config"
echo "==> PASS: BM real guest MMDS, Trigger immutability, terminal cleanup, and artifact/log secrecy"

# ---- B2: fromTemplate(img) + steps + startCmd/readyCmd → e2b-snp ------------
# One registration deliberately never executes. Together with B2 it proves
# registration admission and execution admission retain distinct charges,
# including across the conductor crash below.
register registration-only-peer
REGISTRATION_ONLY_TID="$TID"; REGISTRATION_ONLY_BID="$BID"
echo "==> B2: fromTemplate=$SOURCE_IMAGE + steps + startCmd/readyCmd"
register e2e-tpl e2b '' 1
B2_TID="$TID"; B2_BID="$BID"
[ "$B2_BID" != "$SOURCE_BID" ] || fail "new registration reused B1 build id"
B2_BODY=$(cat <<EOF
{"fromTemplate":"$SOURCE_IMAGE",
 "steps":[
   {"type":"RUN","args":["touch /tmp/issue374-recovery-started; while [ ! -e /tmp/issue374-recovery-ready ]; do sleep 0.1; done; rm /tmp/issue374-recovery-started /tmp/issue374-recovery-ready; useradd -m -d /home/user user || adduser -D user"]},
   {"type":"RUN","args":["grep -Eq '^0::/user(/|$)' /proc/self/cgroup && test ! -s /sys/fs/cgroup/cgroup.procs && for group in user ptys socats; do test -d /sys/fs/cgroup/\$group && test -e /sys/fs/cgroup/\$group/cpu.weight && test -e /sys/fs/cgroup/\$group/memory.max && test -e /sys/fs/cgroup/\$group/io.weight || exit 1; done"]},
   {"type":"RUN","args":["echo b2 > /etc/b2-marker"]},
   {"type":"ENV","args":["BUILT","yes"]},
   {"type":"WORKDIR","args":["/home/user"]}],
 "startCmd":"touch /home/user/started; exec sleep 86400",
 "readyCmd":"test -f /home/user/started"}
EOF
)
code=$(req POST "/v2/templates/$B2_TID/builds/$B2_BID" "$AK" "$B2_BODY")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B2 trigger = $code (want 202)"; }
assert_active_build_accounting "$B2_TID" "$B2_BID" b
timeout -k 5s 20 "$BIN/sandbox-ctl" exec --path-id b --run-root "$WORK/run/builds/$B2_BID" \
    -- /bin/sh -c 'while [ ! -e /tmp/issue374-recovery-started ]; do sleep 0.1; done' \
    || fail "B2 guest RUN did not start before conductor crash"
live_build_signature "$B2_BID" >"$WORK/b2-before-recovery.json" \
    || fail "B2 live ownership signature before conductor crash"
crash_conductor
start_conductor
wait_phase_reservation b "$B2_BID" || fail "B2 reservation did not reconnect after conductor crash"
assert_active_build_accounting "$B2_TID" "$B2_BID" b
live_build_signature "$B2_BID" >"$WORK/b2-after-recovery.json" \
    || fail "B2 live ownership signature after conductor crash"
cmp "$WORK/b2-before-recovery.json" "$WORK/b2-after-recovery.json" \
    || fail "B2 recovery changed durable ownership or replaced a live worker/VMM"
grep -F 'reconcile: adopted live build' "$WORK/orch.log" | grep -Fq "$B2_BID" \
    || fail "B2 was not adopted as the same live Build"
timeout -k 5s 20 "$BIN/sandbox-ctl" exec --path-id b --run-root "$WORK/run/builds/$B2_BID" \
    -- /bin/touch /tmp/issue374-recovery-ready || fail "B2 recovered guest exec could not release RUN"
echo "==> PASS: B2 live recovery retained the exact run-id, claim, durable row and worker/ctl/VMM processes"
# A has already self-cleaned. Plant a marker in its sibling path while B is
# active; observing it after C starts proves B cleanup was scoped to PathID=b.
mkdir -p "$WORK/run/builds/$B2_BID/a"
touch "$WORK/run/builds/$B2_BID/a/sibling-proof"
wait_phase_reservation c "$B2_BID" \
    || fail "B2 phase C never exposed a connected settled nodectl reservation"
[ -f "$WORK/run/builds/$B2_BID/a/sibling-proof" ] \
    || fail "phase B cleanup removed sibling phase A"
wait_ready "$B2_TID" "$B2_BID" B2 null snp
B2_PERSIST="$PERSIST"
case "$B2_PERSIST" in e2b-snp-*) : ;; *) fail "B2 persist=$B2_PERSIST (want e2b-snp-…)";; esac
assert_terminal_build_unowned "$B2_BID" ready || fail "B2 terminal row retained execution ownership"
assert_build_processes_gone "$WORK/b2-before-recovery.json" \
    || fail "B2 recovered execution released its claim before all processes exited"
[ ! -e "$WORK/run/builds/$B2_BID" ] || fail "terminal Build retained BuildRunDir"
[ ! -e "$WORK/lib/builds/$B2_BID" ] || fail "terminal Build retained BuildBaseDir"
echo "==> PASS: B2 ready → $B2_PERSIST"

# Deep asserts through the artifact chain: the uploaded snapshot.cfg names its
# captured overlay as manifest:// and carries the e2b start/ready metadata.
# base_ref remains outside issue #79's forced snapshot-layer conversion; this
# Store-backed Builder explicitly publishes its new platform image before phase
# C, selecting the existing manifest:// strategy. B3 below validates the merged
# ENV/WORKDIR in the restored guest as well.
B2_REF=$(persist_ref "$B2_PERSIST") \
    || fail "B2 persist id does not contain a valid portable ref: $B2_PERSIST"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B2_REF" >"$WORK/b2.cfg.json" 2>"$WORK/b2.cfg.err" \
    || { cat "$WORK/b2.cfg.err"; fail "sandbox-ctl info $B2_REF"; }
grep -q '"e2b.start_cmd": *"touch /home/user/started' "$WORK/b2.cfg.json" \
    || fail "B2 snapshot.cfg missing e2b.start_cmd metadata: $(cat "$WORK/b2.cfg.json")"
grep -q '"e2b.ready_cmd": *"test -f /home/user/started"' "$WORK/b2.cfg.json" \
    || fail "B2 snapshot.cfg missing e2b.ready_cmd metadata"
B2_IMG_HEX=$(python3 - "$WORK/b2.cfg.json" <<'PY'
import json, re, sys
with open(sys.argv[1], encoding="utf-8") as source:
    cfg = json.load(source)
assert cfg["Launch"]["CgroupControl"] is True, cfg["Launch"]
resources = cfg["Resources"]
# Snapshot portability fixes capacity only. The restore node re-resolves
# allocatable, startup, overhead, and controller from its own resource policy.
assert resources == {"Capacity": {"CPU": 2, "Memory": "3GiB"}}, resources
root = cfg["Boot"]["Root"]
# The Builder chooses the existing manifest strategy for its newly published
# platform base. Issue #79 independently requires the captured overlay layer to
# be manifest-only; neither field may contain a physical Bundle selector.
assert re.fullmatch(r"manifest://[0-9a-f]{64}", root["BaseRef"]), root
assert re.fullmatch(r"manifest://[0-9a-f]{64}", root["Overlay"]["Base"]), root
print(root["BaseRef"].removeprefix("manifest://"))
PY
) || fail "B2 snapshot.cfg lost fixed capacity, published platform base, manifest overlay, or launch.cgroup_control=true"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "manifest://$B2_IMG_HEX" >"$WORK/b2.img.json" 2>"$WORK/b2.img.err" \
    || { cat "$WORK/b2.img.err"; fail "flatten-ctl info manifest://$B2_IMG_HEX"; }
grep -q '"BUILT=yes"' "$WORK/b2.img.json" || fail "B2 image config missing merged ENV BUILT=yes: $(cat "$WORK/b2.img.json")"
grep -q '"WorkingDir": *"/home/user"' "$WORK/b2.img.json" || fail "B2 image config missing merged WORKDIR"
echo "==> PASS: B2 artifacts — cgroup_control + snapshot metadata + published manifest base + manifest:// overlay + merged ENV/WORKDIR"
wait_resource_reservations_empty || fail "phase sandbox reservation remained after B2 completion"
echo "==> PASS: phases A/B/C used precise nodectl reservations and left no reservation"
code=$(req GET "/templates/$REGISTRATION_ONLY_TID/builds/$REGISTRATION_ONLY_BID/status" "$AK")
[ "$code" = 200 ] || fail "registration-only peer disappeared across recovery"
python3 - "$WORK/resp.body" <<'PY_REGISTRATION'
import json, sys
status = json.load(open(sys.argv[1]))
assert status["status"] == "building" and not status["executionClaimed"], status
assert not status.get("runID") and not status.get("phase"), status
PY_REGISTRATION
code=$(req DELETE "/templates/$REGISTRATION_ONLY_TID" "$AK")
[ "$code" = 204 ] || fail "registration-only peer delete=$code"
wait_build_row_deleted "$REGISTRATION_ONLY_BID" || fail "registration-only peer retained admission after delete"

# ---- B3: fromTemplate(snp) + steps only (start/ready inherited) -------------
echo "==> B3: fromTemplate=$B2_PERSIST + steps (inherits startCmd/readyCmd)"
register e2e-child e2b '' 1
B3_TID="$TID"; B3_BID="$BID"
code=$(req POST "/v2/templates/$B3_TID/builds/$B3_BID" "$AK" \
    "{\"fromTemplate\":\"$B2_PERSIST\",\"steps\":[{\"type\":\"RUN\",\"args\":[\"test -f /etc/b2-marker && test x\$BUILT = xyes && test x\$PWD = x/home/user\"]}]}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B3 trigger = $code (want 202)"; }
wait_ready "$B3_TID" "$B3_BID" B3 null snp
B3_PERSIST="$PERSIST"
case "$B3_PERSIST" in e2b-snp-*) : ;; *) fail "B3 persist=$B3_PERSIST (want e2b-snp-…)";; esac
assert_terminal_build_unowned "$B3_BID" ready || fail "B3 terminal row retained execution ownership"
B3_UNIT=$(build_journal_unit "$B3_BID") || fail "B3 journal lost its builder unit identity"
journalctl --no-pager -o cat -u "$B3_UNIT" >"$WORK/b3-builder.journal" 2>&1 || true
B3_ROOT_READS=$(grep -F -c 'build task artifact prepared' "$WORK/b3-builder.journal" || true)
[ "$B3_ROOT_READS" = "1" ] \
    || { cat "$WORK/b3-builder.journal"; fail "B3 task root snapshot.cfg read count=$B3_ROOT_READS (want 1)"; }
grep -F -q 'task_artifact_ref_count' "$WORK/b3-builder.journal" \
    || { cat "$WORK/b3-builder.journal"; fail "B3 task artifact ref-count instrumentation missing"; }
# ready is only reachable if the RUN saw B2's marker and merged ENV/WORKDIR,
# and the inherited startCmd/readyCmd ran on the new template VM.
echo "==> PASS: B3 ready → $B3_PERSIST (one task-local source preparation; materialized RUN/ENV/WORKDIR + start/ready inheritance)"

# ---- B6: SNP source + explicit Image, no steps → forced B, no C -----------
echo "==> B6: fromTemplate=$B2_PERSIST (SNP), explicit image, no steps"
register e2e-reimage e2b '{"kind":"image"}'
B6_TID="$TID"; B6_BID="$BID"
code=$(req POST "/v2/templates/$B6_TID/builds/$B6_BID" "$AK" \
    "{\"fromTemplate\":\"$B2_PERSIST\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B6 trigger = $code (want 202)"; }
wait_ready "$B6_TID" "$B6_BID" B6 '{"kind":"image"}' img
B6_PERSIST="$PERSIST"
case "$B6_PERSIST" in e2b-img-*) : ;; *) fail "B6 persist=$B6_PERSIST (want e2b-img-…)";; esac
assert_phase_history "$B6_BID" b c B6
B6_REF=$(persist_ref "$B6_PERSIST") || fail "B6 persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B6_REF" >"$WORK/b6-image.json" 2>"$WORK/b6-image.err" \
    || { cat "$WORK/b6-image.err"; fail "flatten-ctl info B6 image"; }
python3 - "$WORK/b6-image.json" "$B2_REF" <<'PY' \
    || fail "B6 image retained source Snapshot identity or commands"
import json, sys
config = json.load(open(sys.argv[1]))
encoded = json.dumps(config, sort_keys=True)
assert sys.argv[2] not in encoded, encoded
assert "e2b.start_cmd" not in encoded and "e2b.ready_cmd" not in encoded, encoded
PY
echo "==> PASS: B6 SNP→E cold selection forced Phase B, ignored inherited commands, emitted only Image, and skipped C"

echo "PASS builder.steps.sh"
