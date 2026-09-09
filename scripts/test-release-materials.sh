#!/usr/bin/env bash
# Exercise source checks with a private, file-backed Go proxy and module cache.
set -euo pipefail
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

export GOWORK=off GOMODCACHE="$TMP/mod-cache" GOPROXY="file://$TMP/proxy"
# The fixture module is deliberately local and has no public checksum entry.
export GOSUMDB=off GOFLAGS=
module=example.invalid/license-fixture
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
[ "$(wc -l < "$TMP/no-go-stage/share/sources/fixture/GO-MODULES.tsv")" -eq 1 ] \
  || fail "no-Go materials unexpectedly contain module records"
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
		{"go.mod", "module example.invalid/license-fixture\n\ngo 1.24\n"},
		{"fixture.go", "package fixture\nfunc Value() int { return 1 }\n"},
		{"LICENSE", "fixture copyright and license\n"},
	}
	for _, file := range files {
		out, err := w.Create("example.invalid/license-fixture@v1.0.0/" + file.name)
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
directory="$(release_materials_verified_go_source "$module" "$version" "$checksum")"
cmp "$directory/LICENSE" <(printf 'fixture copyright and license\n') \
  || fail "verified source did not return the downloaded license"
if (release_materials_verified_go_source "$module" "$version" \
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
WORK="$TMP/validation" release_materials_validate "$TMP/replacement-stage" fixture
# A valid row cannot conceal a second contradictory row for that same source.
for mismatch in duplicate version source integrity license; do
  altered="$TMP/source-record-$mismatch"
  cp -a "$TMP/replacement-stage" "$altered"
  awk -F '\t' -v mismatch="$mismatch" 'BEGIN { OFS=FS }
    { print }
    $2 == "Go toolchain" {
      if (mismatch == "version") $3="go0.0.0"
      if (mismatch == "source") $4="https://example.invalid/other-source"
      if (mismatch == "integrity") $5="other-integrity"
      if (mismatch == "license") $6="share/licenses/fixture/project"
      print
    }' "$TMP/replacement-stage/share/sources/fixture/SOURCES.tsv" \
    > "$altered/share/sources/fixture/SOURCES.tsv"
  mkdir -p "$altered/share/licenses/fixture/project"
  install -m 0644 "$directory/LICENSE" "$altered/share/licenses/fixture/project/LICENSE"
  release_materials_hash_tree "$altered" fixture "$altered/share/sources/fixture/MATERIALS.sha256"
  if (WORK="$TMP/validation" release_materials_validate "$altered" fixture > "$TMP/source-$mismatch.log" 2>&1); then
    fail "validator accepted an ambiguous source record: $mismatch"
  fi
  grep -Fq 'missing or inconsistent source record for Go toolchain' "$TMP/source-$mismatch.log" \
    || fail "ambiguous source failed for an unrelated reason"
done

altered="$TMP/changed-published-license"
cp -a "$TMP/replacement-stage" "$altered"
printf 'unrelated license bytes\n' > "$altered/share/licenses/fixture/go/$module@$version/LICENSE"
release_materials_hash_tree "$altered" fixture "$altered/share/sources/fixture/MATERIALS.sha256"
if (WORK="$TMP/validation" release_materials_validate "$altered" fixture > "$TMP/published-license.log" 2>&1); then
  fail "validator accepted altered module licenses with regenerated checksums"
fi
grep -Fq 'Go module license bytes differ from the verified source' "$TMP/published-license.log" \
  || fail "changed published license failed for an unrelated reason"
# Recomputing the material hashes must not allow an inventory to revert to
# the original (unbuilt) module or advertise another replacement checksum.
for mismatch in original checksum; do
  altered="$TMP/replacement-$mismatch"
  cp -a "$TMP/replacement-stage" "$altered"
  {
    printf 'module\tversion\tchecksum\n'
    if [ "$mismatch" = original ]; then
      printf 'example.invalid/original-fixture\tv0.0.0\t-\n'
    else
      printf '%s\t%s\th1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n' "$module" "$version"
    fi
  } > "$altered/share/sources/fixture/GO-MODULES.tsv"
  release_materials_hash_tree "$altered" fixture "$altered/share/sources/fixture/MATERIALS.sha256"
  if (WORK="$TMP/validation" release_materials_validate "$altered" fixture > "$TMP/$mismatch.log" 2>&1); then
    fail "validator accepted a mismatched effective module inventory: $mismatch"
  fi
  grep -Fq 'Go module inventory differs from the build records' "$TMP/$mismatch.log" \
    || fail "effective module inventory mismatch failed for an unrelated reason"
done

altered="$TMP/replacement-license-redirect"
cp -a "$TMP/replacement-stage" "$altered"
mkdir -p "$altered/share/licenses/fixture/project"
install -m 0644 "$directory/LICENSE" "$altered/share/licenses/fixture/project/LICENSE"
awk -F '\t' 'BEGIN { OFS=FS } $2 == "Go toolchain" { $6="share/licenses/fixture/project" } { print }' \
  "$altered/share/sources/fixture/SOURCES.tsv" > "$TMP/redirected-sources"
mv "$TMP/redirected-sources" "$altered/share/sources/fixture/SOURCES.tsv"
release_materials_hash_tree "$altered" fixture "$altered/share/sources/fixture/MATERIALS.sha256"
if (WORK="$TMP/validation" release_materials_validate "$altered" fixture > "$TMP/redirect.log" 2>&1); then
  fail "validator accepted a Go toolchain license redirected to the project"
fi
grep -Fq 'missing or inconsistent source record for Go toolchain' "$TMP/redirect.log" \
  || fail "Go toolchain license redirect failed for an unrelated reason"

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

chmod u+w "$directory/LICENSE"
printf 'locally changed license\n' > "$directory/LICENSE"
if (release_materials_verified_go_source "$module" "$version" "$checksum" \
  >"$TMP/tamper-output" 2>"$TMP/tamper-error"); then
  fail "source verification accepted a modified extracted module license"
fi
grep -Fq 'dir has been modified' "$TMP/tamper-error" \
  || fail "tampered source failed for an unrelated reason"
echo "test-release-materials: PASS (checksum, effective replacement, Go experiment and cache tampering)"
