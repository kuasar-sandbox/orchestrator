#!/usr/bin/env bash

# License collection for release archives; source observations stay temporary.
# This file is sourced by scripts/release.sh; the caller provides fail().

release_materials_safe_relative() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z0-9._+@:/~=-]+$ ]] \
    && [[ "$value" != . && "$value" != .. ]] \
    && [[ "$value" != /* && "$value" != ../* && "$value" != */../* && "$value" != */.. ]]
}

release_materials_init() {
  [ "$#" -eq 3 ] || fail "release_materials_init requires stage, work and unit"
  RELEASE_MATERIALS_STAGE="$1"
  RELEASE_MATERIALS_WORK="$2"
  RELEASE_MATERIALS_UNIT="$3"
  release_materials_safe_relative "$RELEASE_MATERIALS_UNIT" \
    || fail "unsafe release material unit: $RELEASE_MATERIALS_UNIT"
  mkdir -p "$RELEASE_MATERIALS_WORK" \
    "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT"
  : > "$RELEASE_MATERIALS_WORK/sources"
  : > "$RELEASE_MATERIALS_WORK/go-build-info"
  : > "$RELEASE_MATERIALS_WORK/go-modules"
  : > "$RELEASE_MATERIALS_WORK/go-toolchains"
}

release_materials_resolve_git_source() {
  [ "$#" -eq 3 ] \
    || fail "release_materials_resolve_git_source requires source directory, expected SHA and name"
  local source="$1" expected="$2" name="$3" actual source_root status index_flags
  [ -d "$source" ] || fail "$name source directory is missing: $source"
  source_root="$(git -C "$source" rev-parse --show-toplevel 2>/dev/null || true)"
  [ -n "$source_root" ] || fail "$name source directory is not a Git worktree: $source"
  [ "$(cd "$source" && pwd -P)" = "$(cd "$source_root" && pwd -P)" ] \
    || fail "$name source directory is not the Git worktree root: $source"
  actual="$(git -C "$source" rev-parse HEAD 2>/dev/null || true)"
  [[ "$actual" =~ ^[0-9a-f]{40}$ ]] || fail "cannot resolve the $name source commit"
  if [ -n "$expected" ] && [ "$expected" != "$actual" ]; then
    fail "$name source commit $actual does not match the selected commit $expected"
  fi
  # Status/diff may trust these index flags and hide changed tracked inputs.
  # Never clear a developer's flags: require a normal, inspectable checkout.
  index_flags="$(git -C "$source" -c core.quotePath=true ls-files -v)" \
    || fail "cannot inspect the $name source index"
  if LC_ALL=C grep -Eq '^[a-zS] ' <<< "$index_flags"; then
    fail "$name source uses assume-unchanged or skip-worktree; select a source tree with normal index flags"
  fi
  status="$(git -C "$source" status --porcelain=v1 --untracked-files=all --ignore-submodules=none)"
  [ -z "$status" ] || fail "$name source worktree is dirty"
  printf '%s\n' "$actual"
}

release_materials_git_version() {
  local source="$1" version="$2" sha="$3" tagged
  tagged="$(git -C "$source" rev-parse --verify --quiet "refs/tags/$version^{commit}" || true)"
  if [ -z "$tagged" ]; then
    # Local source builds may target a release whose tag does not exist yet.
    # Record the exact source identity instead of claiming that tag's contents.
    printf 'git:%s\n' "$sha"
  elif [ "$tagged" = "$sha" ]; then
    printf '%s\n' "$version"
  else
    fail "$version does not identify the selected source commit $sha"
  fi
}

release_materials_copy_licenses() {
  [ "$#" -eq 2 ] || fail "release_materials_copy_licenses requires source directory and destination label"
  local source="$1" label="$2" destination file relative source_root count=0
  local links listing nested=''
  [ -d "$source" ] || fail "license source directory is missing: $source"
  release_materials_safe_relative "$label" \
    || fail "unsafe release license label: $label"
  destination="$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label"
  mkdir -p "$destination"
  source_root="$(git -C "$source" rev-parse --show-toplevel 2>/dev/null || true)"
  links="$(find "$source" -mindepth 1 -maxdepth 1 -type l \
    \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \
       -o -iname 'AUTHORS*' -o -iname 'CREDITS*' -o -iname 'COPYRIGHT*' \) \
    -print)" || fail "cannot enumerate top-level license material: $source"
  if [ -n "$links" ]; then
    fail "top-level license material must not be a symbolic link: $source"
  fi
  if [ -d "$source/LICENSES" ]; then
    links="$(find "$source/LICENSES" -type l -print)" \
      || fail "cannot enumerate license directory: $source/LICENSES"
    [ -z "$links" ] || fail "license directory contains a symbolic link: $source/LICENSES"
    nested="$(find "$source/LICENSES" -type f -print)" \
      || fail "cannot enumerate license directory: $source/LICENSES"
  fi
  # Capture each traversal status before copying anything. A failed find can
  # emit valid-looking partial output, which process substitution would hide.
  listing="$(find "$source" -mindepth 1 -maxdepth 1 -type f \
    \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \
       -o -iname 'AUTHORS*' -o -iname 'CREDITS*' -o -iname 'COPYRIGHT*' \) -print)" \
    || fail "cannot enumerate top-level license material: $source"
  listing="$(printf '%s\n' "$listing" "$nested" | LC_ALL=C sort)" \
    || fail "cannot sort license material"

  while IFS= read -r file; do
    [ -n "$file" ] || continue
    [ ! -L "$file" ] || fail "license material must not be a symbolic link: $file"
    relative="${file#"$source"/}"
    if [ "$source_root" = "$(cd "$source" && pwd -P)" ]; then
      git -C "$source" ls-files --error-unmatch -- "$relative" >/dev/null 2>&1 \
        || fail "license material is absent from the selected source commit: $relative"
      git -C "$source" cat-file blob "HEAD:$relative" | cmp -s "$file" - \
        || fail "license material differs from the selected source commit: $relative"
    fi
    mkdir -p "$destination/$(dirname "$relative")"
    install -m 0644 "$file" "$destination/$relative"
    count=$((count + 1))
  done <<< "$listing"
  [ "$count" -gt 0 ] || fail "no license or notice material found in $source"
}

release_materials_record_source() {
  [ "$#" -eq 6 ] || fail "release_materials_record_source requires payload, name, version, source, integrity and license directory"
  local value
  for value in "$@"; do
    [[ "$value" != *$'\t'* && "$value" != *$'\n'* && -n "$value" ]] \
      || fail "source material fields must be non-empty single-line values"
  done
  release_materials_safe_relative "$6" \
    || fail "unsafe source material license directory: $6"
  printf '%s\t%s\t%s\t%s\t%s\tshare/licenses/%s/%s\n' \
    "$1" "$2" "$3" "$4" "$5" "$RELEASE_MATERIALS_UNIT" "$6" \
    >> "$RELEASE_MATERIALS_WORK/sources"
}

release_materials_add_go_binary() {
  [ "$#" -eq 2 ] || fail "release_materials_add_go_binary requires binary and archive path"
  local binary="$1" archive_path="$2" raw toolchain toolchain_full
  [ -f "$binary" ] || fail "Go release payload is missing: $binary"
  release_materials_safe_relative "$archive_path" \
    || fail "unsafe Go payload archive path: $archive_path"
  raw="$RELEASE_MATERIALS_WORK/go-version-$(( $(find "$RELEASE_MATERIALS_WORK" -maxdepth 1 -name 'go-version-*' | wc -l) + 1 ))"
  go version -m "$binary" > "$raw" 2>/dev/null \
    || fail "Go build info is missing from $binary"
  # Official component packages only support the existing Kuasar sibling
  # replacements. A third-party local directory has no authenticated module h1.
  awk -F '\t' '
    $2 == "dep" { module=$3 }
    $2 == "=>" && ($3 ~ /^\// || $3 ~ /^\.\.?\// || $4 == "(devel)" || $4 == "") {
      if (module != "github.com/kuasar-sandbox/accelerator" && module != "github.com/kuasar-sandbox/connector" && module != "github.com/kuasar-sandbox/sandboxer") unsupported=1
    }
    END { exit unsupported }
  ' "$raw" || fail "third-party local Go replacements are not supported in release materials; select a versioned module replacement"
  toolchain_full="$(awk 'NR == 1 { sub(/^.*: /, ""); print; exit }' "$raw")"
  toolchain="${toolchain_full%% *}"
  toolchain="${toolchain%%-X:*}"
  [[ "$toolchain" =~ ^go[0-9] ]] || fail "cannot read the Go toolchain from $binary"
  printf '%s\ttoolchain\tgo\t%s\t-\n' "$archive_path" "$toolchain_full" \
    >> "$RELEASE_MATERIALS_WORK/go-build-info"
  printf '%s\n' "$toolchain" >> "$RELEASE_MATERIALS_WORK/go-toolchains"

  awk -F '\t' -v payload="$archive_path" \
    -v info="$RELEASE_MATERIALS_WORK/go-build-info" \
    -v modules="$RELEASE_MATERIALS_WORK/go-modules" '
      function flush_module() {
        if (previous != "") print previous "\t" previous_version "\t" previous_checksum >> modules
        previous = ""
      }
      $2 != "=>" { flush_module() }
      $2 == "path" {
        print payload "\tmain-package\t" $3 "\t-\t-" >> info
      }
      $2 == "mod" {
        version = ($4 == "" ? "(devel)" : $4)
        checksum = ($5 == "" ? "-" : $5)
        print payload "\tmain-module\t" $3 "\t" version "\t" checksum >> info
      }
      $2 == "dep" {
        version = ($4 == "" ? "-" : $4)
        checksum = ($5 == "" ? "-" : $5)
        print payload "\tmodule\t" $3 "\t" version "\t" checksum >> info
        previous = $3
        previous_version = version
        previous_checksum = checksum
      }
      $2 == "=>" && previous != "" {
        previous_module = previous
        if ($3 ~ /^\// || $3 ~ /^\.\.?\// || $4 == "(devel)" || $4 == "") {
          target = "local-source"
          checksum = "-"
          flush_module()
        } else {
          target = $3 "@" $4
          checksum = ($5 == "" ? "-" : $5)
          print $3 "\t" $4 "\t" checksum >> modules
        }
        print payload "\treplacement\t" previous_module "\t" target "\t" checksum >> info
        previous = ""
      }
      $2 == "build" {
        split($3, setting, "=")
        if (setting[1] == "GOOS" || setting[1] == "GOARCH" ||
            setting[1] == "CGO_ENABLED" || setting[1] == "GOEXPERIMENT" || setting[1] == "-tags" ||
            setting[1] == "vcs" || setting[1] == "vcs.revision" ||
            setting[1] == "vcs.time" || setting[1] == "vcs.modified") {
          print payload "\tbuild-setting\t" setting[1] "\t" substr($3, length(setting[1]) + 2) "\t-" >> info
        }
      }
      END { flush_module() }
    ' "$raw"
}

release_materials_require_go_revision() {
  local binary="$1" sha="$2" revision modified info
  info="$(go version -m "$binary" 2>/dev/null)" \
    || fail "Go build info is missing from $binary"
  revision="$(awk -F '\t' '$2 == "build" && $3 ~ /^vcs.revision=/ { print substr($3, 14) }' <<< "$info")"
  modified="$(awk -F '\t' '$2 == "build" && $3 ~ /^vcs.modified=/ { print substr($3, 14) }' <<< "$info")"
  if [ "$revision" != "$sha" ] || [ "$modified" != false ]; then
    fail "Go payload must be built from the clean selected commit $sha: $binary"
  fi
}



release_materials_go_source() {
  local module="$1" version="$2" checksum="$3" download_root json directory
  [[ "$checksum" =~ ^h1:[A-Za-z0-9+/]{43}=$ ]] \
    || fail "third-party Go source requires a module checksum: $module@$version"
  download_root="$(mktemp -d "$RELEASE_MATERIALS_WORK/go-source.XXXXXX")"
  # Resolve the version recorded in the binary using the normal Go module
  # cache and routing. A temporary working directory protects the caller's
  # go.mod/go.sum; standalone archive validation does not fetch module sources.
  json="$(cd "$download_root" && GOWORK=off GOFLAGS='' GO111MODULE=on go mod download -json "$module@$version")" \
    || fail "cannot obtain Go module source for $module@$version"
  [ "$(jq -er '.Sum' <<< "$json")" = "$checksum" ] \
    || fail "Go module source checksum differs from the binary: $module@$version"
  directory="$(jq -er '.Dir' <<< "$json")" \
    || fail "Go module source directory is missing for $module@$version"
  printf '%s\n' "$directory"
}

release_materials_copy_go_licenses() {
  # Collect notices from the compiler actually selected for this build. This
  # records distribution material; it does not authenticate the compiler.
  local source="${1%/}" toolchain="$2" destination file relative base notices
  destination="$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/go-toolchain/$toolchain"
  [ -f "$source/LICENSE" ] || fail "selected Go distribution has no LICENSE"
  notices="$(mktemp "$RELEASE_MATERIALS_WORK/go-notices.XXXXXX")"
  find "$source/" \( -type f -o -type l \) -print0 | LC_ALL=C sort -z > "$notices" \
    || fail "cannot enumerate Go notices"
  mkdir -p "$destination"
  while IFS= read -r -d '' file; do
    relative="${file#"$source"/}"
    case "$relative" in api/*|doc/*|misc/*|test/*) continue ;; esac
    base="${relative##*/}"
    case "${base,,}" in *.go|*.c|*.h|*.s|*.rs|*.py) continue ;; esac
    case "${base^^}" in
      LICENSE|LICENSE.*|LICENSE-*|LICENCE|LICENCE.*|LICENCE-*|COPYING|COPYING.*|COPYING-*|NOTICE|NOTICE.*|NOTICE-*|PATENTS|PATENTS.*|PATENTS-*|AUTHORS|AUTHORS.*|AUTHORS-*|CREDITS|CREDITS.*|CREDITS-*|COPYRIGHT|COPYRIGHT.*|COPYRIGHT-*) ;;
      *) [[ "${relative^^}" == */LICENSES/* ]] || continue ;;
    esac
    release_materials_safe_relative "$relative" || fail "unsafe Go notice path"
    if [ ! -f "$file" ] || [ -L "$file" ]; then
      fail "Go notice is not a regular file: $relative"
    fi
    mkdir -p "$destination/$(dirname "$relative")"
    if [ -f "$destination/$relative" ]; then
      cmp -s "$file" "$destination/$relative" || fail "conflicting Go notices for $toolchain: $relative"
    else
      install -m 0644 "$file" "$destination/$relative"
    fi
  done < "$notices"
}


# Some upstream submodules omit their repository-root license from the module
# zip. Supplement only this exact, verified module version with the unmodified
# upstream notice tracked in the selected component source. Do not exempt the
# module from inventory or accept a missing notice for an unknown version.
release_materials_go_license_source() {
  local module="$1" version="$2" directory="$3" notice digest
  case "$module@$version" in
    github.com/rootless-containers/proto/go-proto@v0.0.0-20230421021042-4cd87ebadd67)
      notice="${TOOL_ROOT:-$ROOT}/scripts/release-notices/rootless-containers-proto"
      [ -f "$notice/COPYING" ] && [ ! -L "$notice/COPYING" ] \
        || fail "pinned rootless-containers/proto COPYING is missing"
      digest="$(sha256sum "$notice/COPYING" | awk '{print $1}')"
      [ "$digest" = cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30 ] \
        || fail "pinned rootless-containers/proto COPYING differs from upstream"
      directory="$notice"
      ;;
  esac
  printf '%s\n' "$directory"
}

release_materials_finish() {
  local module version checksum directory toolchain toolchain_root installed_toolchain source payload
  while IFS= read -r toolchain; do
    [ -n "$toolchain" ] || continue
    if [ -n "${RELEASE_MATERIALS_GO_ENV:-}" ]; then
      toolchain_root="$(jq -er '.GOROOT' "$RELEASE_MATERIALS_GO_ENV")"
      installed_toolchain="$(jq -er '.GOVERSION' "$RELEASE_MATERIALS_GO_ENV")"
    else
      toolchain_root="$(command go -C "${ROOT:-$PWD}" env GOROOT)"
      installed_toolchain="$(command go -C "${ROOT:-$PWD}" env GOVERSION)"
    fi
    [ "$installed_toolchain" = "$toolchain" ] \
      || fail "resolved Go toolchain $installed_toolchain does not match $toolchain"
    release_materials_copy_go_licenses "$toolchain_root" "$toolchain"
    source="https://go.dev/dl/#$toolchain"
    checksum=-
    while IFS= read -r payload; do
      release_materials_record_source "$payload" "Go toolchain" "$toolchain" \
        "$source" "$checksum" "go-toolchain/$toolchain"
    done < <(awk -F '\t' -v version="$toolchain" \
      '$2 == "toolchain" { selected=$4; sub(/[ -]X:.*/, "", selected); if (selected == version) print $1 }' \
      "$RELEASE_MATERIALS_WORK/go-build-info" | LC_ALL=C sort -u)
  done < <(LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/go-toolchains")
  LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/go-modules" \
    > "$RELEASE_MATERIALS_WORK/go-modules-sorted"
  if [ -s "$RELEASE_MATERIALS_WORK/go-modules-sorted" ]; then
    command -v jq >/dev/null || fail "jq is required to collect Go module license material"
  fi
  while IFS=$'\t' read -r module version checksum; do
    [ -n "$module" ] || continue
    # Only exact components named in this package's source contract
    # have their notices collected separately. Organization membership is not
    # a license-material exemption.
    case "$module" in
      github.com/kuasar-sandbox/accelerator|github.com/kuasar-sandbox/connector|github.com/kuasar-sandbox/sandboxer) continue ;;
    esac
    directory="$(release_materials_go_source "$module" "$version" "$checksum")"
    directory="$(release_materials_go_license_source "$module" "$version" "$directory")"
    release_materials_copy_licenses "$directory" "go/$module@$version"
  done < "$RELEASE_MATERIALS_WORK/go-modules-sorted"

  find "$RELEASE_MATERIALS_STAGE/share" -type d -exec chmod 0755 {} +
  find "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT" -type f -exec chmod 0644 {} +
}

# Check the actual binaries and their accompanying notices. These temporary
# observations are never included in the archive or used as release eligibility.
release_materials_require_license() {
  local root="$1" unit="$2" label="$3" directory
  release_materials_safe_relative "$label" || fail "unsafe license path: $label"
  directory="$root/share/licenses/$unit/$label"
  [ -d "$directory" ] && [ ! -L "$directory" ] \
    && [ -n "$(find "$directory" -type f -size +0c -print -quit)" ] \
    || fail "license material is missing or empty: $label"
}

release_materials_validate_go() (
  local root="$1" unit="$2" payload="$3" binary="${4:-$1/$3}" validation
  local toolchain module version checksum label
  validation="$(mktemp -d "$WORK/go-notices.XXXXXX")"
  release_materials_init "$validation/stage" "$validation/materials" "$unit"
  release_materials_add_go_binary "$binary" "$payload"
  awk -F '\t' '$2 == "main-package" { rows++ } END { exit rows != 1 }' \
    "$RELEASE_MATERIALS_WORK/go-build-info" || fail "Go main package is missing: $payload"
  while IFS= read -r toolchain; do
    release_materials_require_license "$root" "$unit" "go-toolchain/$toolchain"
    [ -s "$root/share/licenses/$unit/go-toolchain/$toolchain/LICENSE" ] \
      || fail "Go toolchain LICENSE is missing: $toolchain"
  done < "$RELEASE_MATERIALS_WORK/go-toolchains"
  while IFS=$'\t' read -r module version checksum; do
    [ -n "$module" ] || continue
    label="go/$module@$version"
    # Only explicitly supported sibling modules use separately collected
    # notices. Other modules, including go-erofs, stay versioned dependencies.
    case "$unit:$module" in
      sandboxer:github.com/kuasar-sandbox/accelerator|sandboxer:github.com/kuasar-sandbox/connector|\
      orchestrator:github.com/kuasar-sandbox/accelerator|orchestrator:github.com/kuasar-sandbox/connector|orchestrator:github.com/kuasar-sandbox/sandboxer|\
      runtime:github.com/kuasar-sandbox/accelerator|runtime:github.com/kuasar-sandbox/sandboxer)
        label="${module##*/}" ;;
    esac
    release_materials_require_license "$root" "$unit" "$label"
  done < "$RELEASE_MATERIALS_WORK/go-modules"
)

release_materials_validate() {
  [ "$#" -eq 2 ] || fail "release_materials_validate requires extracted root and unit"
  local root="$1" unit="$2" directory file listing
  release_materials_safe_relative "$unit" || fail "unsafe release material unit: $unit"
  [ ! -e "$root/share/sources" ] && [ ! -L "$root/share/sources" ] \
    || fail "new packages must not include historical source records"
  for directory in "$root/share" "$root/share/licenses" "$root/share/licenses/$unit"; do
    [ -d "$directory" ] && [ ! -L "$directory" ] \
      || fail "license directory is missing or unsafe: $directory"
    [ "$(stat -c '%a' "$directory")" = 755 ] || fail "license directory has unsafe mode: $directory"
  done
  listing="$(mktemp "$WORK/license-paths.XXXXXX")"
  find "$root/share/licenses/$unit" -print0 > "$listing" || fail "cannot enumerate license material"
  while IFS= read -r -d '' file; do
    release_materials_safe_relative "${file#"$root"/}" || fail "unsafe license path: $file"
    [ ! -L "$file" ] || fail "license material contains a symbolic link: $file"
    if [ -d "$file" ]; then
      [ "$(stat -c '%a' "$file")" = 755 ] || fail "license directory has unsafe mode: $file"
    elif [ -f "$file" ]; then
      [ -s "$file" ] && [ "$(stat -c '%a' "$file")" = 644 ] \
        || fail "license material is empty or has unsafe mode: $file"
      case "${file##*/}" in
        SOURCES.tsv|GO-BUILD-INFO.tsv|GO-MODULES.tsv|MATERIALS.sha256|CLOUD-HYPERVISOR-Cargo.lock)
          fail "new packages must not include historical source records" ;;
      esac
    else
      fail "license material has an unsafe type: $file"
    fi
  done < "$listing"
  release_materials_require_license "$root" "$unit" project
}
