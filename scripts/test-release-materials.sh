#!/usr/bin/env bash
# Exercise source checks with a private, file-backed Go proxy and module cache.
set -euo pipefail
# Mutation fixtures use the release tree's explicit public modes. Private test
# work roots still come from mktemp; package-level umask regressions are separate.
umask 022
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'chmod -R u+w "$TMP"; rm -rf "$TMP"' EXIT
fail() { echo "test-release-materials: $*" >&2; exit 1; }
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

# Neither source selection nor copied notices may trust hidden index changes.
mkdir -p "$TMP/git-index"
git -C "$TMP/git-index" init -q
git -C "$TMP/git-index" config --local user.name 'Chen Xiaohui'
git -C "$TMP/git-index" config --local user.email 'graych@gmail.com'
printf 'committed license\n' > "$TMP/git-index/LICENSE"
git -C "$TMP/git-index" add -- LICENSE
git -C "$TMP/git-index" commit -qm 'test: create indexed license fixture'
for flag in assume-unchanged skip-worktree; do
  git -C "$TMP/git-index" update-index "--$flag" -- LICENSE
  printf 'hidden modified license\n' > "$TMP/git-index/LICENSE"
  [ -z "$(git -C "$TMP/git-index" status --porcelain)" ] \
    || fail "fixture did not hide its tracked change"
  if (release_materials_resolve_git_source "$TMP/git-index" "" fixture > "$TMP/index-$flag.log" 2>&1); then
    fail "source selection accepted $flag"
  fi
  grep -Fq 'uses assume-unchanged or skip-worktree' "$TMP/index-$flag.log" \
    || fail "hidden source index failed for an unrelated reason"
  if (
    release_materials_init "$TMP/index-stage" "$TMP/index-work" fixture
    release_materials_copy_licenses "$TMP/git-index" project > "$TMP/license-$flag.log" 2>&1
  ); then
    fail "license collection accepted a hidden tracked change"
  fi
  grep -Fq 'license material differs from the selected source commit' "$TMP/license-$flag.log" \
    || fail "hidden license change failed for an unrelated reason"
  git -C "$TMP/git-index" update-index "--no-$flag" -- LICENSE
  git -C "$TMP/git-index" cat-file blob HEAD:LICENSE > "$TMP/git-index/LICENSE"
done
release_materials_resolve_git_source "$TMP/git-index" "" fixture >/dev/null

# Only this local module fixture has no public checksum entry. Production
# routing/environment is exercised independently by test-release-go-environment.
release_materials_go_payload_allowed() {
  [ "$1:$2" = fixture:bin/tool ] || fail "unexpected fixture payload"
}
export GOWORK=off GOMODCACHE="$TMP/mod-cache" GOPROXY="file://$TMP/proxy"
# The fixture module is deliberately local and has no public checksum entry.
export GOSUMDB=off GOFLAGS=
# Keep organization-named fixture modules on this file proxy even when the
# caller configures private module routes; no fixture may fall back to VCS.
export GOENV=off GOPRIVATE='' GONOPROXY=none GONOSUMDB=none GOINSECURE=''
export GOAUTH=off GOTOOLCHAIN=local
# An additional organization-owned module is not one of the explicitly
# source-bound internal components and must still ship notices.
module=github.com/kuasar-sandbox/license-fixture
version=v1.0.0
mkdir -p "$TMP/proxy/$module/@v" "$TMP/consumer" "$TMP/material-work"
(
  # Kernel-only material collection has no module JSON and must work without jq.
  command() {
    if [ "$1" = -v ] && [ "${2:-}" = jq ]; then return 1; fi
    builtin command "$@"
  }
  release_materials_init "$TMP/no-go-stage" "$TMP/no-go-work" fixture
  release_materials_finish
)
[ ! -e "$TMP/no-go-stage/share/sources" ] || fail "no-Go package contains source records"
RELEASE_MATERIALS_WORK="$TMP/material-work"
printf 'module %s\n\ngo 1.24\n' "$module" > "$TMP/proxy/$module/@v/$version.mod"
printf '{"Version":"%s","Time":"2020-01-01T00:00:00Z"}\n' "$version" \
  > "$TMP/proxy/$module/@v/$version.info"
cat > "$TMP/make-zip.go" <<'EOF'
package main
import (
	"archive/zip"
	"os"
)
func main() {
	f, err := os.Create(os.Args[1])
	if err != nil { panic(err) }
	w := zip.NewWriter(f)
	files := []struct{ name, body string }{
		{"go.mod", "module github.com/kuasar-sandbox/license-fixture\n\ngo 1.24\n"},
		{"fixture.go", "package fixture\nfunc Value() int { return 1 }\n"},
		{"LICENSE", "fixture copyright and license\n"},
	}
	for _, file := range files {
		out, err := w.Create("github.com/kuasar-sandbox/license-fixture@v1.0.0/" + file.name)
		if err != nil { panic(err) }
		if _, err = out.Write([]byte(file.body)); err != nil { panic(err) }
	}
	if err = w.Close(); err != nil { panic(err) }
	if err = f.Close(); err != nil { panic(err) }
}
EOF
GO111MODULE=off go run "$TMP/make-zip.go" "$TMP/proxy/$module/@v/$version.zip"
printf 'module release-consumer.invalid\n\ngo 1.24\nrequire %s %s\n' "$module" "$version" \
  > "$TMP/consumer/go.mod"
printf 'package main\nimport f "%s"\nfunc main() { println(f.Value()) }\n' "$module" \
  > "$TMP/consumer/main.go"
(cd "$TMP/consumer" && go mod download "$module@$version" && go build -o "$TMP/tool" .)
checksum="$(go version -m "$TMP/tool" | awk -F '\t' -v module="$module" \
  '$2 == "dep" && $3 == module { print $5 }')"
[[ "$checksum" == h1:* ]] || fail "fixture binary has no module checksum"
directory="$(release_materials_go_source "$module" "$version" "$checksum")"
cmp "$directory/LICENSE" <(printf 'fixture copyright and license\n') \
  || fail "verified source did not return the downloaded license"
# A real local third-party replacement carries no module h1. Refuse it
# explicitly, instead of downloading and attributing a different remote source.
mkdir -p "$TMP/local-consumer"
printf 'module local-consumer.invalid\n\ngo 1.24\nrequire %s %s\nreplace %s => %s\n' \
  "$module" "$version" "$module" "$directory" > "$TMP/local-consumer/go.mod"
printf 'package main\nimport f "%s"\nfunc main() { println(f.Value()) }\n' "$module" \
  > "$TMP/local-consumer/main.go"
(cd "$TMP/local-consumer" && go build -o "$TMP/local-tool" .)
if (
  release_materials_init "$TMP/local-stage" "$TMP/local-work" fixture
  release_materials_add_go_binary "$TMP/local-tool" bin/tool
) > "$TMP/local-replacement.log" 2>&1; then
  fail "accepted an unsupported third-party local Go replacement"
fi
grep -Fq 'third-party local Go replacements are not supported' "$TMP/local-replacement.log" \
  || fail "local replacement was rejected for an unrelated reason"
if (
  go() { fail 'unsigned source unexpectedly reached Go module resolution'; }
  release_materials_go_source "$module" "$version" -
) > "$TMP/unsigned-source.log" 2>&1; then
  fail "accepted a third-party source without an authenticated checksum"
fi
grep -Fq 'requires a module checksum' "$TMP/unsigned-source.log" \
  || fail "unsigned source reached the network or failed for an unrelated reason"
if (release_materials_go_source "$module" "$version" \
  h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= >/dev/null 2>&1); then
  fail "source verification accepted a checksum different from the binary"
fi
mkdir -p "$TMP/replacement-consumer"
printf 'module replacement-consumer.invalid\n\ngo 1.24\nrequire example.invalid/original-fixture v0.0.0\nreplace example.invalid/original-fixture => %s %s\n' \
  "$module" "$version" > "$TMP/replacement-consumer/go.mod"
printf 'package main\nimport f "example.invalid/original-fixture"\nfunc main() { println(f.Value()) }\n' \
  > "$TMP/replacement-consumer/main.go"
(cd "$TMP/replacement-consumer" && go mod download "$module@$version" && go build -o "$TMP/replaced-tool" .)
release_materials_init "$TMP/replacement-stage" "$TMP/replacement-work" fixture
release_materials_add_go_binary "$TMP/replaced-tool" bin/tool
printf '%s\t%s\t%s\n' "$module" "$version" "$checksum" > "$TMP/effective-module"
cmp "$TMP/effective-module" "$RELEASE_MATERIALS_WORK/go-modules" \
  || fail "replacement inventory does not select the actual module and checksum"
grep -Fqx "bin/tool"$'\t'"replacement"$'\t'"example.invalid/original-fixture"$'\t'"$module@$version"$'\t'"$checksum" \
  "$RELEASE_MATERIALS_WORK/go-build-info" || fail "replacement metadata lost its source checksum"
release_materials_finish
cmp "$directory/LICENSE" "$TMP/replacement-stage/share/licenses/fixture/go/$module@$version/LICENSE"
mkdir -p "$TMP/replacement-stage/bin" "$TMP/validation"
install -m 0755 "$TMP/replaced-tool" "$TMP/replacement-stage/bin/tool"
mkdir -p "$TMP/replacement-stage/share/licenses/fixture/project"
install -m 0644 "$directory/LICENSE" "$TMP/replacement-stage/share/licenses/fixture/project/LICENSE"
WORK="$TMP/validation" release_materials_validate "$TMP/replacement-stage" fixture
WORK="$TMP/validation" release_materials_validate_go "$TMP/replacement-stage" fixture bin/tool
[ ! -e "$TMP/replacement-stage/share/sources" ] || fail "source records were published"
# Derive effective dependencies from the actual binary, without module downloads
# or a historical inventory. Organization membership does not exempt notices.
for missing in "go/$module@$version" "go-toolchain/$(go env GOVERSION)"; do
  altered="$TMP/missing-$(basename "$missing")"
  cp -a "$TMP/replacement-stage" "$altered"
  rm -rf "$altered/share/licenses/fixture/$missing"
  if (WORK="$TMP/validation" release_materials_validate_go "$altered" fixture bin/tool > "$TMP/missing-license.log" 2>&1); then
    fail "validator accepted missing notices: $missing"
  fi
  grep -Fq 'license material is missing' "$TMP/missing-license.log" || fail "missing notices failed for an unrelated reason"
done
(
  release_materials_go_source() { fail 'standalone validation attempted a source download'; }
  WORK="$TMP/validation" release_materials_validate_go "$TMP/replacement-stage" fixture bin/tool
)
altered="$TMP/original-module-notices"
cp -a "$TMP/replacement-stage" "$altered"
mkdir -p "$altered/share/licenses/fixture/go/example.invalid/original-fixture@v0.0.0"
mv "$altered/share/licenses/fixture/go/$module@$version/LICENSE" \
  "$altered/share/licenses/fixture/go/example.invalid/original-fixture@v0.0.0/LICENSE"
if (WORK="$TMP/validation" release_materials_validate_go "$altered" fixture bin/tool >/dev/null 2>&1); then
  fail "validator accepted original-module notices instead of the effective replacement"
fi

(cd "$TMP/consumer" && GOEXPERIMENT=arenas go build -o "$TMP/experimental-tool" .)
release_materials_init "$TMP/experiment-stage" "$TMP/experiment-work" fixture
release_materials_add_go_binary "$TMP/experimental-tool" bin/tool
grep -Eq '[- ]X:arenas|GOEXPERIMENT[[:space:]]+arenas' "$RELEASE_MATERIALS_WORK/go-build-info" \
  || fail "Go experiment information was not retained"
if grep -q ' ' "$RELEASE_MATERIALS_WORK/go-toolchains"; then
  fail "Go experiment suffix was included in the toolchain path"
fi
release_materials_finish

# Older Go versions append experiment names to the first build-info line.
# Exercise that representation independently of the installed Go release.
release_materials_init "$TMP/suffix-stage" "$TMP/suffix-work" fixture
(
  go() { command go "$@" | awk 'NR == 1 { $0 = $0 " X:arenas" } { print }'; }
  release_materials_add_go_binary "$TMP/tool" bin/tool
)
grep -Fq ' X:arenas' "$RELEASE_MATERIALS_WORK/go-build-info"
if grep -q ' ' "$RELEASE_MATERIALS_WORK/go-toolchains"; then
  fail "historical Go experiment suffix was included in the toolchain path"
fi
release_materials_finish

echo "test-release-materials: PASS (module checksum, effective replacement, Go experiment, notices and offline validation)"
