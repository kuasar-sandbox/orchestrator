#!/usr/bin/env bash

set -uo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: ci-timed.sh <stage> <command> [args ...]" >&2
    exit 2
fi

stage=$1
shift
metrics_file=${KUASAR_CI_TIMINGS:-}

if [ -z "$metrics_file" ]; then
    exec "$@"
fi

mkdir -p "$(dirname "$metrics_file")"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
time_output="$(mktemp)"
trap 'rm -f "$time_output"' EXIT

if [ -x /usr/bin/time ]; then
    /usr/bin/time -q -f '%e\t%U\t%S\t%P\t%M\t%I\t%O' -o "$time_output" -- "$@"
    rc=$?
    IFS=$'\t' read -r elapsed user_seconds system_seconds cpu_percent max_rss_kb fs_input fs_output <"$time_output"
else
    start_ns="$(date +%s%N)"
    "$@"
    rc=$?
    end_ns="$(date +%s%N)"
    elapsed="$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end - start) / 1000000000 }')"
    user_seconds=unavailable
    system_seconds=unavailable
    cpu_percent=unavailable
    max_rss_kb=unavailable
    fs_input=unavailable
    fs_output=unavailable
fi

exec {metrics_fd}>>"$metrics_file"
flock "$metrics_fd"
if [ ! -s "$metrics_file" ]; then
    printf 'stage\tstarted_at\telapsed_seconds\tuser_seconds\tsystem_seconds\tcpu_percent\tmax_rss_kb\tfs_input\tfs_output\texit_code\n' >&"$metrics_fd"
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$stage" "$started_at" "$elapsed" "$user_seconds" "$system_seconds" \
    "$cpu_percent" "$max_rss_kb" "$fs_input" "$fs_output" "$rc" >&"$metrics_fd"
flock -u "$metrics_fd"
exec {metrics_fd}>&-

exit "$rc"
