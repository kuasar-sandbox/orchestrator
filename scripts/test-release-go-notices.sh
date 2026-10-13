#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/release-materials.sh"
fail() { echo "test-release-go-notices: $*" >&2; exit 1; }
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
module=github.com/rootless-containers/proto/go-proto
version=v0.0.0-20230421021042-4cd87ebadd67
mkdir "$work/module"
release_materials_init "$work/stage" "$work/materials" orchestrator
notice="$(release_materials_go_license_source "$module" "$version" "$work/module")"
release_materials_copy_licenses "$notice" "go/$module@$version"
cmp "$notice/COPYING" "$work/stage/share/licenses/orchestrator/go/$module@$version/COPYING"
# Unknown versions retain ordinary module-license checks and fail on missing
# notices; a supplemental license must never cover an arbitrary newer module.
for identity in "$module@v0.0.0-20250101000000-000000000000" example.invalid/other@v1.0.0; do
  directory="$(release_materials_go_license_source "${identity%@*}" "${identity##*@}" "$work/module")"
  [ "$directory" = "$work/module" ] || fail "unknown module used supplemental notice"
  if (release_materials_copy_licenses "$directory" unknown >/dev/null 2>&1); then
    fail "missing module license was accepted"
  fi
done
mkdir -p "$work/source/scripts/release-notices/rootless-containers-proto"
for mode in absent modified symlink; do
  candidate="$work/source/scripts/release-notices/rootless-containers-proto/COPYING"
  case "$mode" in
    modified) printf 'changed license\n' > "$candidate" ;;
    symlink) rm "$candidate"; ln -s "$notice/COPYING" "$candidate" ;;
  esac
  if (ROOT="$work/source" release_materials_go_license_source "$module" "$version" "$work/module" >/dev/null 2>&1); then
    fail "$mode supplemental notice was accepted"
  fi
done
echo "pinned Go supplemental notices: PASS"
