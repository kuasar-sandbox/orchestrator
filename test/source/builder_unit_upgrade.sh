#!/usr/bin/env bash
# Isolated systemd regression for the one-time Builder parent-policy removal.
# Every file/unit below belongs to this fixture; no deployment Build is touched.
set -euo pipefail
skip() {
    echo "SKIP: $*" >&2
    [ "${REQUIRE_BUILDER:-0}" != 1 ] || exit 1
    exit 0
}
[ -d /run/systemd/system ] || skip "systemd is required"
command -v systemctl >/dev/null || skip "systemctl is required"
if [ "$(id -u)" -ne 0 ]; then exec sudo -nE bash "$0" "$@"; fi
name="kuasarbuilderupgrade$$"
unit="$name.service"
slice="$name.slice"
unit_file="/run/systemd/system/$unit"
slice_file="/run/systemd/system/$slice"
property_dir="/run/systemd/system.control/$unit.d"
for path in "$unit_file" "$slice_file" "$property_dir"; do
    [ ! -e "$path" ] || { echo "fixture path already exists: $path" >&2; exit 1; }
done
properties=()
cleanup() {
    local status=$?
    trap - EXIT
    systemctl stop "$unit" "$slice" || status=1
    for path in "${properties[@]}"; do rm -f -- "$path"; done
    if [ -d "$property_dir" ]; then rmdir -- "$property_dir" || status=1; fi
    rm -f -- "$unit_file" "$slice_file"
    systemctl daemon-reload || status=1
    exit "$status"
}
trap cleanup EXIT
cat >"$slice_file" <<EOF_SLICE
[Unit]
Description=Isolated Builder upgrade fixture
Before=slices.target

[Slice]
CPUQuota=150%
MemoryMax=268435456
EOF_SLICE
cat >"$unit_file" <<EOF_UNIT
[Unit]
Description=Isolated Builder upgrade worker
[Service]
Type=exec
ExecStart=/bin/sleep infinity
Slice=$slice
Delegate=yes
KillMode=control-group
EOF_UNIT
systemctl daemon-reload
systemctl start "$unit"
# Only the two files created by this exact set-property call are ours.
# Track them before the call so partial failure still reclaims fixture files.
properties=("$property_dir/50-CPUQuota.conf" "$property_dir/50-MemoryMax.conf")
systemctl set-property --runtime "$unit" CPUQuota=125% MemoryMax=134217728
for path in "${properties[@]}"; do
    [ -f "$path" ] || { echo "missing exact runtime property file: $path" >&2; exit 1; }
done
unit_cgroup="/sys/fs/cgroup$(systemctl show --value -p ControlGroup "$unit")"
slice_cgroup="/sys/fs/cgroup$(systemctl show --value -p ControlGroup "$slice")"
assert_limits() {
    python3 - "$1" "$2" "$3" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
quota, period = (path / "cpu.max").read_text().split()
memory = (path / "memory.max").read_text().strip()
expected_cpu, expected_memory = sys.argv[2:]
assert memory == expected_memory, (path, memory, expected_memory)
if expected_cpu == "max":
    assert quota == "max", (path, quota, period)
else:
    assert quota != "max" and int(quota) * 100 == int(period) * int(expected_cpu), (path, quota, period, expected_cpu)
print(f"{path.name}: cpu.max={quota} {period}; memory.max={memory}")
PY
}
echo "==> Old generated slice and live runtime properties"
assert_limits "$slice_cgroup" 150 268435456
assert_limits "$unit_cgroup" 125 134217728
original_pid=$(systemctl show --value -p MainPID "$unit")
# This is the generated slice shape after InstallUnits updates its owned file.
cat >"$slice_file" <<EOF_SLICE
[Unit]
Description=Isolated Builder upgrade fixture
Before=slices.target

[Slice]
EOF_SLICE
systemctl daemon-reload
echo "==> Updated slice after reload; existing live service runtime properties remain"
assert_limits "$slice_cgroup" max max
assert_limits "$unit_cgroup" 125 134217728
[ "$(systemctl show --value -p MainPID "$unit")" = "$original_pid" ]
# The fixture process is not a Build. In a deployment, wait for the exact Build
# to finish and release all ownership normally before touching its old settings.
systemctl stop "$unit"
for path in "${properties[@]}"; do rm -- "$path"; done
rmdir -- "$property_dir"
systemctl daemon-reload
systemctl start "$unit"
echo "==> Fresh execution after exact, source-confirmed runtime file removal"
assert_limits "$slice_cgroup" max max
assert_limits "$unit_cgroup" max max
echo "==> PASS: generated slice update and existing runtime properties are distinct"
