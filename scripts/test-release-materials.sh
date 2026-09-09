#!/usr/bin/env bash
# Exercise source checks with a private, file-backed Go proxy and module cache.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'chmod -R u+w "$TMP"; rm -rf "$TMP"' EXIT
fail() { echo "test-release-materials: $*" >&2; exit 1; }
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

export GOWORK=off GOMODCACHE="$TMP/mod-cache" GOPROXY="file://$TMP/proxy"
# The fixture module is deliberately local and has no public checksum entry.
export GOSUMDB=off GOFLAGS=
module=example.invalid/license-fixture
version=v1.0.0
mkdir -p "$TMP/proxy/$module/@v" "$TMP/consumer" "$TMP/material-work"
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
