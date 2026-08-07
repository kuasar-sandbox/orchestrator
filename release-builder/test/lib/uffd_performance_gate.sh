#!/usr/bin/env bash

# Assert a real-KVM UFFD scenario and append its metrics to the BMS artifact.
# Call only after sandbox-ctl exits and atomically writes --stats-json.
uffd_performance_gate() {
    local scenario="$1"
    local latency_ms="$2"
    local max_latency_ms="$3"
    local stats_json="$4"
    local expected_tail="$5"

    [[ "$latency_ms" =~ ^[0-9]+$ ]] || {
        echo "FAIL: $scenario latency is not an integer: $latency_ms" >&2
        return 1
    }
    [[ "$max_latency_ms" =~ ^[1-9][0-9]*$ ]] || {
        echo "FAIL: $scenario maximum latency is invalid: $max_latency_ms" >&2
        return 1
    }
    [ -s "$stats_json" ] || {
        echo "FAIL: $scenario missing UFFD stats: $stats_json" >&2
        return 1
    }
    command -v jq >/dev/null 2>&1 || {
        echo "FAIL: jq is required for the UFFD performance gate" >&2
        return 1
    }

    local metrics
    metrics=$(jq -er '[
        .uffd.errors,
        .uffd.faults_absent,
        .uffd.source_read_calls,
        .uffd.urgent_copy_calls,
        .uffd.urgent_zero_calls,
        .uffd.tail_buffered_data,
        .uffd.tail_deferred_data,
        .uffd.tail_zero,
        .uffd.tail_pages_completed,
        .uffd.tail_dropped_busy,
        .uffd.tail_conflicts
    ] | if all(.[]; type == "number") then @tsv else error("missing numeric UFFD metric") end' "$stats_json") || {
        echo "FAIL: $scenario has incomplete UFFD stats: $stats_json" >&2
        return 1
    }

    local errors faults source_reads urgent_copy urgent_zero
    local tail_buffered tail_deferred tail_zero tail_completed tail_dropped tail_conflicts
    IFS=$'\t' read -r errors faults source_reads urgent_copy urgent_zero \
        tail_buffered tail_deferred tail_zero tail_completed tail_dropped tail_conflicts \
        <<<"$metrics"

    local failures=()
    (( latency_ms <= max_latency_ms )) \
        || failures+=("latency ${latency_ms}ms exceeds ${max_latency_ms}ms")
    (( errors == 0 )) || failures+=("UFFD errors=$errors")
    (( faults > 0 )) || failures+=("faults_absent=$faults")
    (( tail_completed > 0 )) || failures+=("tail_pages_completed=$tail_completed")

    case "$expected_tail" in
        zero)
            (( urgent_zero > 0 )) || failures+=("urgent_zero_calls=$urgent_zero")
            (( tail_zero > 0 )) || failures+=("tail_zero=$tail_zero")
            ;;
        deferred)
            (( source_reads > 0 )) || failures+=("source_read_calls=$source_reads")
            (( urgent_copy > 0 )) || failures+=("urgent_copy_calls=$urgent_copy")
            (( tail_deferred > 0 )) || failures+=("tail_deferred_data=$tail_deferred")
            ;;
        buffered)
            (( source_reads > 0 )) || failures+=("source_read_calls=$source_reads")
            (( urgent_copy > 0 )) || failures+=("urgent_copy_calls=$urgent_copy")
            (( tail_buffered > 0 )) || failures+=("tail_buffered_data=$tail_buffered")
            ;;
        *)
            failures+=("unknown expected tail mode $expected_tail")
            ;;
    esac

    local output_dir
    output_dir="${KUASAR_CI_DIR:-$(dirname "$stats_json")}"
    local output="$output_dir/uffd-e2e-gate.tsv"
    mkdir -p "$output_dir"
    if [ ! -s "$output" ]; then
        printf 'scenario\tlatency_ms\tmax_latency_ms\terrors\tfaults_absent\tsource_read_calls\turgent_copy_calls\turgent_zero_calls\ttail_buffered_data\ttail_deferred_data\ttail_zero\ttail_pages_completed\ttail_dropped_busy\ttail_conflicts\tresult\n' >"$output"
    fi
    local result=PASS
    [ "${#failures[@]}" -eq 0 ] || result=FAIL
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$scenario" "$latency_ms" "$max_latency_ms" "$errors" "$faults" \
        "$source_reads" "$urgent_copy" "$urgent_zero" "$tail_buffered" \
        "$tail_deferred" "$tail_zero" "$tail_completed" "$tail_dropped" \
        "$tail_conflicts" "$result" >>"$output"

    echo "==> UFFD gate $scenario: latency=${latency_ms}ms/${max_latency_ms}ms tail=$expected_tail completed=$tail_completed result=$result"
    if [ "${#failures[@]}" -ne 0 ]; then
        printf 'FAIL: %s: %s\n' "$scenario" "${failures[*]}" >&2
        return 1
    fi
}
