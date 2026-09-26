#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../e2e/lib/vmm_cgroup.sh"

WORK="$(mktemp -d /tmp/vmm-cgroup-test-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
PROC_ROOT="$WORK/proc"
CGROUP_PROCS="$WORK/cgroup.procs"
FAKE_BIN="$WORK/bin"
PASSED=0

reset_fixture() {
    rm -rf "$PROC_ROOT"
    mkdir -p "$PROC_ROOT/101/task/101" "$FAKE_BIN"
    : > "$FAKE_BIN/cloud-hypervisor"
    : > "$FAKE_BIN/rogue"
    ln -s "$FAKE_BIN/cloud-hypervisor" "$PROC_ROOT/101/exe"
    ln -s "$FAKE_BIN/cloud-hypervisor" "$PROC_ROOT/101/task/101/exe"
}

add_owner_thread() { # $1=tid $2=executable basename
    local tid="$1" executable="$2"
    mkdir -p "$PROC_ROOT/101/task/$tid"
    ln -s "$FAKE_BIN/$executable" "$PROC_ROOT/101/task/$tid/exe"
}

add_kernel_process() { # $1=pid $2=comm
    mkdir -p "$PROC_ROOT/$1"
    printf '%s\n' "$2" > "$PROC_ROOT/$1/comm"
}

add_userspace_process() { # $1=pid $2=executable basename
    mkdir -p "$PROC_ROOT/$1"
    ln -s "$FAKE_BIN/$2" "$PROC_ROOT/$1/exe"
}

expect_pass() { # $1=name
    local name="$1" output
    if ! output=$(e2e_assert_vmm_cgroup_members "$PROC_ROOT" "$CGROUP_PROCS" 2>&1); then
        echo "FAIL: $name: $output" >&2
        exit 1
    fi
    PASSED=$((PASSED + 1))
}

expect_fail() { # $1=name $2=error substring
    local name="$1" want="$2" output
    if output=$(e2e_assert_vmm_cgroup_members "$PROC_ROOT" "$CGROUP_PROCS" 2>&1); then
        echo "FAIL: $name unexpectedly passed" >&2
        exit 1
    fi
    case "$output" in
        *"$want"*) ;;
        *) echo "FAIL: $name: got '$output', want substring '$want'" >&2; exit 1 ;;
    esac
    PASSED=$((PASSED + 1))
}

reset_fixture
printf '101\n' > "$CGROUP_PROCS"
expect_pass "Cloud Hypervisor only"

reset_fixture
add_owner_thread 102 cloud-hypervisor
add_kernel_process 202 kvm-nx-lpage-recovery-102
printf '0\n101\n202\n' > "$CGROUP_PROCS"
expect_pass "associated NX recovery worker"

reset_fixture
add_userspace_process 303 rogue
printf '101\n303\n' > "$CGROUP_PROCS"
expect_fail "unrelated userspace process" "unexpected userspace process pid=303"

reset_fixture
add_kernel_process 202 kvm-pit/101
printf '101\n202\n' > "$CGROUP_PROCS"
expect_fail "unrecognized kernel process" "unrecognized kernel process pid=202"

reset_fixture
add_kernel_process 202 kvm-nx-lpage-recovery-999
printf '101\n202\n' > "$CGROUP_PROCS"
expect_fail "NX worker from another VM" "not associated with a Cloud Hypervisor creator tid=999"

reset_fixture
add_owner_thread 102 rogue
add_kernel_process 202 kvm-nx-lpage-recovery-102
printf '101\n202\n' > "$CGROUP_PROCS"
expect_fail "NX worker with non-VMM creator" "not associated with a Cloud Hypervisor creator tid=102"

reset_fixture
add_kernel_process 202 kvm-nx-lpage-recovery-101
printf '202\n' > "$CGROUP_PROCS"
expect_fail "worker without hypervisor" "no visible Cloud Hypervisor process"

reset_fixture
printf '101\nnot-a-pid\n' > "$CGROUP_PROCS"
expect_fail "malformed cgroup entry" "invalid pid not-a-pid"

echo "vmm_cgroup_test: PASS ($PASSED cases)"
