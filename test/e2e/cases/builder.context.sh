#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy
register missing-storage
# Unconfigured: COPY and the files endpoint are unsupported → 501 (loud).
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\"a\",\"b\"],\"filesHash\":\"x\"}]}")
[ "$code" = "501" ] || fail "COPY trigger (no files_storage) = $code (want 501)"
code=$(req GET "/templates/$TID/files/deadbeef" "$AK")
[ "$code" = "501" ] || fail "files endpoint (no files_storage) = $code (want 501)"
echo "==> PASS: without files_storage, COPY + files endpoint report 501"
stop_conductor
start_builder_files_storage
write_builder_config
start_conductor
register malformed-copy
# Configured: a COPY with no filesHash is a malformed request → 400.
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\"a\",\"b\"]}]}")
[ "$code" = "400" ] || fail "COPY trigger (no filesHash) = $code (want 400)"
echo "==> PASS: COPY without a filesHash rejected (400); files-storage configured (B4 exercises it)"
# ---- B4: COPY build context via files endpoint + presigned direct upload ----
# Acts as the e2b client: GET the files endpoint (present=false) → PUT the
# gzipped context straight to the bucket → GET again (present=true), then build
# fromImage with COPY steps and assert (via a RUN step) that the files landed
# with the right ownership.
    echo "==> B4: COPY build context (files endpoint → presigned PUT → in-build extract)"
    REQ_NETWORK_HEADER="$BUILD_NETWORK_HEADER"
    register e2e-copy e2b '{"kind":"image"}'
    B4_TID="$TID"; B4_BID="$BID"

    # Build the COPY context: ./hello.txt + ./sub/nested.txt, gzipped tar with
    # arcnames relative to the context (the e2b SDK's layout).
    CTX="$WORK/ctx"; mkdir -p "$CTX/sub"
    echo "COPY-MARKER-$RANDOM" > "$CTX/hello.txt"; MARKER="$(cat "$CTX/hello.txt")"
    echo nested > "$CTX/sub/nested.txt"
    ( cd "$CTX" && tar czf "$WORK/ctx.tgz" . )
    HASH="$(sha256sum "$WORK/ctx.tgz" | cut -d' ' -f1)"

    # 1. files endpoint: not present yet, returns a presigned PUT url.
    code=$(req GET "/templates/$B4_TID/files/$HASH" "$AK")
    [ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "files GET = $code (want 201)"; }
    # Parse with a real JSON reader (not grep): the presigned url carries '&',
    # which stdlib json escapes to & — every real client (the e2b SDK)
    # decodes it; a sed extraction would not.
    present=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["present"])' "$WORK/resp.body")
    [ "$present" = "False" ] || fail "files: expected present=false on first GET: $(cat "$WORK/resp.body")"
    PUT_URL="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["url"])' "$WORK/resp.body")"
    [ -n "$PUT_URL" ] || fail "files: no presigned url"

    # 2. client uploads the context straight to the bucket (bytes skip the orchestrator).
    pcode=$(curl -sS --noproxy '*' -o /dev/null -w '%{http_code}' -X PUT --data-binary @"$WORK/ctx.tgz" "$PUT_URL")
    [ "$pcode" = "200" ] || fail "presigned PUT = $pcode (want 200)"

    # 3. now present (idempotency: client skips re-upload).
    code=$(req GET "/templates/$B4_TID/files/$HASH" "$AK")
    [ "$code" = "201" ] || fail "files GET#2 = $code"
    present=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["present"])' "$WORK/resp.body")
    [ "$present" = "True" ] || fail "files: expected present=true after upload: $(cat "$WORK/resp.body")"
    echo "==> PASS: files endpoint round-trip (present false→PUT→true; direct-to-bucket upload)"

    # Negative: a COPY referencing an un-uploaded context → 400 at trigger.
    code=$(req POST "/v2/templates/$B4_TID/builds/$B4_BID" "$AK" \
        "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\".\",\"/opt/x\"],\"filesHash\":\"deadbeefdeadbeef\"}]}")
    [ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "COPY w/ unuploaded context = $code (want 400)"; }
    echo "==> PASS: COPY referencing an un-uploaded context rejected (400)"

    # Real build: COPY whole context to /opt/ct (default owner 0:0), the single
    # file to /opt/ct2/ with --chown 1000:1000, then RUN asserts presence+owner.
    # Reaching ready proves the extract + ownership are correct.
    B4_BODY=$(cat <<EOF
{"fromImage":"$NETWORK_PULL_REF",
 "steps":[
   {"type":"COPY","args":[".","/opt/ct"],"filesHash":"$HASH"},
   {"type":"COPY","args":["hello.txt","/opt/ct2/","1000:1000"],"filesHash":"$HASH"},
   {"type":"RUN","args":["test \"\$(cat /opt/ct/hello.txt)\" = \"$MARKER\" && test -f /opt/ct/sub/nested.txt && test \"\$(stat -c %u:%g /opt/ct/hello.txt)\" = 0:0 && test \"\$(stat -c %u:%g /opt/ct2/hello.txt)\" = 1000:1000"]},
   $BUILD_NETWORK_STEP]}
EOF
)
    code=$(req POST "/v2/templates/$B4_TID/builds/$B4_BID" "$AK" "$B4_BODY")
    [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B4 trigger = $code (want 202)"; }
    wait_ready "$B4_TID" "$B4_BID" B4 '{"kind":"image"}' img
    B4_PERSIST="$PERSIST"
    case "$B4_PERSIST" in e2b-img-*) : ;; *) fail "B4 persist=$B4_PERSIST (want e2b-img-…)";; esac
    echo "==> PASS: B4 ready → $B4_PERSIST (explicit image, A/B request DNS, COPY ownership verified in-build)"
echo "PASS builder.context.sh"
