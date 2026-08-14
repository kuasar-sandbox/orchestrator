#!/usr/bin/env bash

# Validate the process ownership of a per-sandbox vmm cgroup. KVM creates the
# NX huge-page recovery worker as "kvm-nx-lpage-recovery-<creator-tid>" and
# copies all cgroups from that creator. Accept the worker only when the suffix
# still names a thread of one of the Cloud Hypervisor processes in this cgroup.
#
# Arguments: PROC_ROOT CGROUP_PROCS
e2e_assert_vmm_cgroup_members() {
    local proc_root="$1" cgroup_procs="$2"
    local pid exe comm worker creator_tid hypervisor_pid owner_exe associated
    local -a members=() hypervisors=() workers=()

    if ! mapfile -t members < "$cgroup_procs"; then
        echo "cannot read vmm cgroup members from $cgroup_procs" >&2
        return 1
    fi
    [ "${#members[@]}" -gt 0 ] || {
        echo "vmm cgroup has no process" >&2
        return 1
    }

    for pid in "${members[@]}"; do
        case "$pid" in
            0)
                # A kernel task outside a nested PID namespace is rendered as
                # zero in cgroup.procs and cannot be inspected through /proc.
                continue
                ;;
            ''|*[!0-9]*)
                echo "vmm cgroup contains invalid pid $pid" >&2
                return 1
                ;;
        esac
        [ -d "$proc_root/$pid" ] || {
            echo "vmm cgroup process pid=$pid disappeared before inspection" >&2
            return 1
        }

        if exe=$(readlink "$proc_root/$pid/exe" 2>/dev/null); then
            if [ "${exe##*/}" != "cloud-hypervisor" ]; then
                echo "vmm cgroup contains unexpected userspace process pid=$pid exe=$exe" >&2
                return 1
            fi
            hypervisors+=("$pid")
            continue
        fi

        if ! IFS= read -r comm < "$proc_root/$pid/comm"; then
            echo "vmm cgroup process pid=$pid has neither an executable nor a readable comm" >&2
            return 1
        fi
        if [[ "$comm" =~ ^kvm-nx-lpage-recovery-([1-9][0-9]*)$ ]]; then
            workers+=("$pid:${BASH_REMATCH[1]}")
            continue
        fi
        echo "vmm cgroup contains unrecognized kernel process pid=$pid comm=$comm" >&2
        return 1
    done

    [ "${#hypervisors[@]}" -gt 0 ] || {
        echo "vmm cgroup has no visible Cloud Hypervisor process" >&2
        return 1
    }

    for worker in "${workers[@]}"; do
        pid=${worker%%:*}
        creator_tid=${worker#*:}
        associated=0
        for hypervisor_pid in "${hypervisors[@]}"; do
            [ -d "$proc_root/$hypervisor_pid/task/$creator_tid" ] || continue
            owner_exe=$(readlink "$proc_root/$hypervisor_pid/task/$creator_tid/exe" 2>/dev/null) || continue
            if [ "${owner_exe##*/}" = "cloud-hypervisor" ]; then
                associated=1
                break
            fi
        done
        [ "$associated" = 1 ] || {
            echo "vmm cgroup NX recovery worker pid=$pid is not associated with a Cloud Hypervisor creator tid=$creator_tid" >&2
            return 1
        }
    done
}
