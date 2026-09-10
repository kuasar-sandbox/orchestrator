#!/usr/bin/env bash
# A license search must never accept partial output from a failed traversal.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'chmod -R u+rwx "$test_root"; rm -rf "$test_root"' EXIT
fail() { printf 'test-license-traversal: %s\n' "$*" >&2; exit 1; }
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"
fixture="$test_root/source"
mkdir -p "$fixture/LICENSES/nested"
printf 'top-level fixture license\n' > "$fixture/LICENSE"
printf 'nested fixture notice\n' > "$fixture/LICENSES/nested/NOTICE"

for failure in top-link top-file nested-link nested-file; do
  if (
    release_materials_init "$test_root/stage-$failure" "$test_root/work-$failure" fixture
    find() {
      local kind=f target=top argument previous=''
      [ "$1" != "$fixture/LICENSES" ] || target=nested
      for argument in "$@"; do
        [ "$previous" != -type ] || kind="$argument"
        previous="$argument"
      done
      [ "$kind" != l ] || kind='link'
      [ "$kind" != f ] || kind='file'
      command find "$@" || return
      [ "$failure" != "$target-$kind" ] # Deliberate failure after real partial output.
    }
    release_materials_copy_licenses "$fixture" project
  ) > "$test_root/$failure.log" 2>&1; then
    fail "accepted $failure traversal failure"
  fi
  grep -Fq 'cannot enumerate' "$test_root/$failure.log" \
    || fail "$failure was rejected for an unrelated reason"
  [ -z "$(find "$test_root/stage-$failure" -type f -print)" ] \
    || fail "$failure copied a partial license set"
done

# Exercise real unreadable-directory behavior as well when not running as root.
if [ "$(id -u)" -ne 0 ]; then
  chmod 000 "$fixture/LICENSES/nested"
  if (
    release_materials_init "$test_root/unreadable-stage" "$test_root/unreadable-work" fixture
    release_materials_copy_licenses "$fixture" project
  ) > "$test_root/unreadable.log" 2>&1; then
    fail "accepted an unreadable license subtree"
  fi
  chmod 0755 "$fixture/LICENSES/nested"
  grep -Fq 'cannot enumerate' "$test_root/unreadable.log" \
    || fail "unreadable subtree failed for an unrelated reason"
  printf 'test-license-traversal: real unreadable subtree rejected\n'
fi

release_materials_init "$test_root/complete-stage" "$test_root/complete-work" fixture
release_materials_copy_licenses "$fixture" project
cmp "$fixture/LICENSE" "$test_root/complete-stage/share/licenses/fixture/project/LICENSE"
cmp "$fixture/LICENSES/nested/NOTICE" "$test_root/complete-stage/share/licenses/fixture/project/LICENSES/nested/NOTICE"
printf 'test-license-traversal: four partial-output failures and complete collection PASS\n'
