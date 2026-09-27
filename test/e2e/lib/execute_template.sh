#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
build_ready_template() {
REQ_RESOURCE_HEADER='{"capacity":{"cpu":2,"memory":"2560MiB"}}'
code=$(req POST /v3/templates "$AK" "{\"name\":\"exec-tmpl\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":8192}")
unset REQ_RESOURCE_HEADER
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\",\"startCmd\":\"exec sleep 86400\",\"readyCmd\":\"true\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
case "$TEMPLATE" in e2b-snp-*) : ;; *) fail "build produced $TEMPLATE (want e2b-snp-...)";; esac
echo "==> built template: $TEMPLATE"

}

build_image_template() {
code=$(req POST /v3/templates "$AK" '{"name":"proxy-tmpl","cpuCount":2,"memoryMB":6144}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
echo "==> built template: $TEMPLATE"

}
