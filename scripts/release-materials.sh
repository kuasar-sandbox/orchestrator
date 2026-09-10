#!/usr/bin/env bash

# Deterministic license and source material support for release archives.
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
    "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT" \
    "$RELEASE_MATERIALS_STAGE/share/sources/$RELEASE_MATERIALS_UNIT"
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
    fail "$name source uses assume-unchanged or skip-worktree; use a fresh checkout"
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

release_materials_require_git_licenses() {
  local root="$1" unit="$2" source="$3" sha="$4" label="$5"
  local verification entry relative mode type blob count=0
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "license source must identify an exact Git commit"
  release_materials_safe_relative "$label" || fail "unsafe Git license material label"
  git -C "$source" cat-file -e "$sha^{commit}" 2>/dev/null \
    || fail "selected license source commit is unavailable; fetch that exact commit before validation"
  verification="$(mktemp -d "$WORK/verify-git-license.XXXXXX")"
  mkdir -p "$verification/source"
  git -C "$source" ls-tree -r -z "$sha" > "$verification/tree" \
    || fail "cannot enumerate selected license source tree"
  while IFS= read -r -d '' entry; do
    relative="${entry#*$'\t'}"
    case "$relative" in
      LICENSES/*) ;;
      */*) continue ;;
      *)
        case "${relative,,}" in license*|copying*|notice*|patents*|authors*|credits*|copyright*) ;; *) continue ;; esac
        ;;
    esac
    release_materials_safe_relative "$relative" || fail "unsafe selected license source path"
    read -r mode type blob <<< "${entry%%$'\t'*}"
    [[ "$type" = blob && "$mode" =~ ^100(644|755)$ && "$blob" =~ ^[0-9a-f]{40}$ ]] \
      || fail "selected Git license material must be a regular file"
    mkdir -p "$verification/source/$(dirname "$relative")"
    git -C "$source" cat-file blob "$blob" > "$verification/source/$relative" \
      || fail "cannot read selected license source blob"
    count=$((count + 1))
  done < "$verification/tree"
  [ "$count" -gt 0 ] || fail "selected Git commit has no license material"
  (
    release_materials_init "$verification/stage" "$verification/materials" "$unit"
    release_materials_copy_licenses "$verification/source" "$label"
    diff -r "$verification/stage/share/licenses/$unit/$label" "$root/share/licenses/$unit/$label" >/dev/null \
      || fail "license bytes differ from selected Git source: $label"
  )
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
      if (module !~ /^github\.com\/kuasar-sandbox\//) unsupported=1
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

release_materials_require_project_source() {
  local root="$1" unit="$2" payload="$3" version="$4"
  shift 4
  [ "$#" -gt 0 ] || fail "project source validation requires its Go payloads"
  local sha="${SOURCE_SHA:-}" info file recorded_version
  if [ -z "$sha" ]; then
    info="$(go version -m "$root/$1" 2>/dev/null)" || fail "project Go build info is missing"
    sha="$(awk -F '\t' '$2 == "build" && $3 ~ /^vcs.revision=/ {print substr($3, 14)}' <<< "$info")"
  fi
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "project source must bind a full lowercase commit"
  for file in "$@"; do
    release_materials_require_go_revision "$root/$file" "$sha"
  done
  recorded_version="$(awk -F '\t' -v payload="$payload" -v unit="$unit" \
    'NR > 1 && $1 == payload && $2 == unit {print $3}' "$root/share/sources/$unit/SOURCES.tsv")"
  if [ "$recorded_version" != "$version" ] && [ "$recorded_version" != "git:$sha" ]; then
    fail "project source version differs from its target release or exact pre-tag commit"
  fi
  release_materials_require_source "$root" "$unit" "$payload" "$unit" "$recorded_version" \
    "https://github.com/kuasar-sandbox/$unit/commit/$sha" "git:$sha"
}

release_materials_validate_go_routing() {
  local proxy="$1" sumdb="$2" route identity endpoint extra
  local -a routes
  IFS=',|' read -r -a routes <<< "$proxy"
  for route in "${routes[@]}"; do
    case "$route" in direct|off) continue ;; esac
    [[ "$route" == https://?* && "$route" != *[@?#[:space:]]* ]] \
      || fail "Go source verification requires credential-free HTTPS module routing"
  done
  [ "$sumdb" != off ] || fail "Go source verification requires an enabled checksum database"
  [[ "$sumdb" != *$'\n'* && "$sumdb" != *$'\r'* ]] \
    || fail "Go source checksum routing must be a single line"
  read -r identity endpoint extra <<< "$sumdb"
  [[ "$identity" =~ ^[A-Za-z0-9._+/:=-]+$ && -z "$extra" ]] \
    || fail "invalid Go source checksum database identity"
  if [ -n "$endpoint" ]; then
    [[ "$endpoint" == https://?* && "$endpoint" != *[@?#[:space:]]* ]] \
      || fail "Go source checksum routing must use credential-free HTTPS"
  fi
}

release_materials_go_command() {
  local verification="$1" proxy="${GOPROXY:-https://proxy.golang.org,direct}"
  local sumdb="${GOSUMDB:-sum.golang.org}" name value
  shift
  release_materials_validate_go_routing "$proxy" "$sumdb" || return 1
  mkdir -p "$verification/home" "$verification/module-cache"
  chmod 0700 "$verification/home" "$verification/module-cache"
  # Fresh module/VCS state cannot inherit a cached Git credential helper.
  # Go private-module bypasses and all caller authentication are excluded.
  local -a clean=(env -i "PATH=$PATH" "HOME=$verification/home"
    "GOMODCACHE=$verification/module-cache" "GOCACHE=$verification/go-cache"
    "GOENV=off" "GOAUTH=off" "GOFLAGS=" "GO111MODULE=on" "GOWORK=off" "GOTOOLCHAIN=local"
    "GOPROXY=$proxy" "GOSUMDB=$sumdb" "GOPRIVATE=" "GONOPROXY="
    "GONOSUMDB=" "GOINSECURE=" "GIT_CONFIG_NOSYSTEM=1"
    "GIT_CONFIG_GLOBAL=/dev/null" "GIT_CONFIG_SYSTEM=/dev/null"
    "GIT_TERMINAL_PROMPT=0" "GIT_ASKPASS=/bin/false"
    "GIT_SSH_COMMAND=/bin/false" "SSH_ASKPASS=/bin/false")
  for name in LANG LC_ALL TZ SSL_CERT_FILE SSL_CERT_DIR \
      HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy; do
    value="${!name:-}"
    [ -n "$value" ] || continue
    case "$name" in
      HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|http_proxy|https_proxy|all_proxy)
        [[ "$value" != *@* && "$value" != *$'\n'* && "$value" != *$'\r'* ]] \
          || fail "Go source verification does not pass authenticated proxy URLs" ;;
    esac
    clean+=("$name=$value")
  done
  "${clean[@]}" go "$@"
}

release_materials_go_payload_allowed() {
  case "$1:$2" in
    orchestrator:bin/node-ctl|orchestrator:bin/cluster-ctl|orchestrator:bin/node-stub-ctl|orchestrator:bin/e2b-key-ctl ) return 0 ;;
    *) fail "unexpected Go payload identity" ;;
  esac
}

release_materials_verified_go_source() {
  local module="$1" version="$2" checksum="$3" verify_root json directory
  [[ "$checksum" =~ ^h1:[A-Za-z0-9+/]{43}=$ ]] \
    || fail "third-party Go source requires an authenticated module checksum: $module@$version"
  verify_root="$(mktemp -d "$RELEASE_MATERIALS_WORK/verify-go.XXXXXX")"
  # Use a temporary module so packaging cannot change the caller's go.mod/sum.
  # Verify the extracted cache as well as the download sum; download alone does
  # not detect edits to an already-extracted LICENSE.
  (
    cd "$verify_root" || exit
    release_materials_go_command "$verify_root" mod init release-material-verification.invalid >/dev/null 2>&1 || exit 1
    release_materials_go_command "$verify_root" mod edit "-require=$module@$version" || exit 1
    release_materials_go_command "$verify_root" mod download -json "$module@$version" > source.json || exit 1
    release_materials_go_command "$verify_root" mod verify > verify.log 2>&1 \
      || { cat verify.log >&2; exit 1; }
  ) || fail "Go module cache verification failed for $module@$version"
  json="$(cat "$verify_root/source.json")"
  if [ "$checksum" = - ] || [ "$(jq -er '.Sum' <<< "$json")" != "$checksum" ]; then
    fail "Go module source checksum differs from the binary: $module@$version"
  fi
  directory="$(jq -er '.Dir' <<< "$json")" \
    || fail "Go module source directory is missing for $module@$version"
  printf '%s\n' "$directory"
}

_release_materials_download_go_toolchain() {
  local toolchain="$1" proxy="${GOPROXY:-https://proxy.golang.org,direct}"
  local sumdb="${GOSUMDB:-sum.golang.org}" route verify_root sumdb_identity sumdb_url sumdb_extra
  local -a routes
  [[ "$toolchain" =~ ^go[0-9]+\.[0-9]+(\.[0-9]+|beta[0-9]+|rc[0-9]+)$ ]] \
    || fail "unsupported official Go toolchain version"
  IFS=',|' read -r -a routes <<< "$proxy"
  for route in "${routes[@]}"; do
    case "$route" in direct|off) continue ;; esac
    [[ "$route" == https://?* && "$route" != *[@?#[:space:]]* ]] \
      || fail "Go distribution verification requires credential-free HTTPS module routing"
  done
  [ "$sumdb" != off ] || fail "Go distribution verification requires an enabled checksum database"
  [[ "$sumdb" != *$'\n'* && "$sumdb" != *$'\r'* ]] \
    || fail "Go distribution checksum routing must be a single line"
  read -r sumdb_identity sumdb_url sumdb_extra <<< "$sumdb"
  [[ "$sumdb_identity" =~ ^[A-Za-z0-9._+/:=-]+$ && -z "$sumdb_extra" ]] \
    || fail "invalid Go distribution checksum database identity"
  if [ -n "$sumdb_url" ]; then
    [[ "$sumdb_url" == https://?* && "$sumdb_url" != *[@?#[:space:]]* ]] \
      || fail "Go distribution checksum routing must use credential-free HTTPS"
  fi
  mkdir -p "$RELEASE_MATERIALS_WORK"
  verify_root="$(mktemp -d "$RELEASE_MATERIALS_WORK/go-distribution.XXXXXX")"
  (
    cd "$verify_root" || exit
    # Toolchain modules require sumdb authentication. Do not inherit Go's
    # private distpack/proxy bootstrap exceptions, or alter the selected compiler.
    GOPROXY="$proxy" GOSUMDB="$sumdb" \
      release_materials_go_command "${WORK:-$RELEASE_MATERIALS_WORK}/toolchain-download" \
      mod download -json "golang.org/toolchain@v0.0.1-$toolchain.linux-amd64" \
      > source.json 2> download.log
  ) || fail "could not authenticate the official Go distribution; check module/checksum routing or the verified cache"
  jq -e --arg version "v0.0.1-$toolchain.linux-amd64" \
    '.Path == "golang.org/toolchain" and .Version == $version and
     (.Sum | type == "string") and (.Zip | type == "string") and (.Error == null)' \
    "$verify_root/source.json" >/dev/null \
    || fail "Go distribution download returned an invalid identity"
  printf '%s\n' "$verify_root/source.json"
}

release_materials_download_go_toolchain() {
  _release_materials_download_go_toolchain "$@"
}

release_materials_go_toolchain_materials() {
  local toolchain="$1" installed_root="$2" destination="$3" json archive checksum
  local helper="${BASH_SOURCE[0]%/*}/release-go-toolchain.go"
  json="$(release_materials_download_go_toolchain "$toolchain")" || return 1
  archive="$(jq -er '.Zip' "$json")" || fail "Go distribution ZIP is missing"
  checksum="$(jq -er '.Sum' "$json")" || fail "Go distribution h1 is missing"
  if [ ! -x "$RELEASE_MATERIALS_WORK/verify-go-distribution" ]; then
    GOWORK=off GOENV=off GOTOOLCHAIN=local GOFLAGS='' GO111MODULE=off \
      command go build -o "$RELEASE_MATERIALS_WORK/verify-go-distribution" "$helper" \
      || fail "could not build the trusted Go distribution verifier"
  fi
  "$RELEASE_MATERIALS_WORK/verify-go-distribution" "$archive" \
    "v0.0.1-$toolchain.linux-amd64" "$checksum" "$installed_root" "$destination" \
    > "${destination}.verification.log" \
    || fail "Go distribution, installed build inputs or license material failed verification"
  printf 'https://proxy.golang.org/golang.org/toolchain/@v/v0.0.1-%s.linux-amd64.zip\t%s\n' \
    "$toolchain" "$checksum"
}

release_materials_verify_build_go() {
  local info="$1" toolchain installed_root identity
  jq -e '.GOHOSTOS == "linux" and .GOHOSTARCH == "amd64"' "$info" >/dev/null \
    || fail "release Go compiler must run on Linux/amd64"
  toolchain="$(jq -er '.GOVERSION' "$info")" || fail "selected Go compiler version is missing"
  installed_root="$(jq -er '.GOROOT' "$info")" || fail "selected Go compiler root is missing"
  identity="$(release_materials_go_toolchain_materials "$toolchain" "$installed_root" \
    "$RELEASE_MATERIALS_WORK/build-licenses")" || return 1
  printf '%s\n' "$identity" > "$RELEASE_MATERIALS_WORK/build-source.tsv"
}

release_materials_hash_tree() {
  local root="$1" unit="$2" output="$3"
  (
    cd "$root" || exit
    find "share/licenses/$unit" "share/sources/$unit" -type f \
      ! -path "share/sources/$unit/MATERIALS.sha256" -print \
      | LC_ALL=C sort \
      | while IFS= read -r file; do sha256sum "$file"; done
  ) > "$output"
}

release_materials_finish() {
  local source_root="$RELEASE_MATERIALS_STAGE/share/sources/$RELEASE_MATERIALS_UNIT"
  local module version checksum directory toolchain toolchain_root installed_toolchain identity source payload license_destination
  while IFS= read -r toolchain; do
    [ -n "$toolchain" ] || continue
    if [ -n "${RELEASE_MATERIALS_GO_ENV:-}" ]; then
      toolchain_root="$(jq -er '.GOROOT' "$RELEASE_MATERIALS_GO_ENV")"
      installed_toolchain="$(jq -er '.GOVERSION' "$RELEASE_MATERIALS_GO_ENV")"
    else
      toolchain_root="$(GOWORK=off GOENV=off GOTOOLCHAIN=local command go env GOROOT)"
      installed_toolchain="$(GOWORK=off GOENV=off GOTOOLCHAIN=local command go env GOVERSION)"
    fi
    [ "$installed_toolchain" = "$toolchain" ] \
      || fail "resolved Go toolchain $installed_toolchain does not match $toolchain"
    directory="$RELEASE_MATERIALS_WORK/licenses-$toolchain"
    identity="$(release_materials_go_toolchain_materials "$toolchain" "$toolchain_root" "$directory")" \
      || fail "Go toolchain material authentication failed"
    IFS=$'\t' read -r source checksum <<< "$identity"
    # The verifier emits only authenticated, regular license/notice files.
    # Preserve nested vendor paths instead of filtering this tree a second time.
    license_destination="$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/go-toolchain/$toolchain"
    mkdir -p "$license_destination"
    cp -R "$directory/." "$license_destination/"
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
    case "$module" in
      github.com/kuasar-sandbox/*) continue ;;
    esac
    directory="$(release_materials_verified_go_source "$module" "$version" "$checksum")"
    release_materials_copy_licenses "$directory" "go/$module@$version"
  done < "$RELEASE_MATERIALS_WORK/go-modules-sorted"

  {
    printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
    LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/sources"
  } > "$source_root/SOURCES.tsv"
  {
    printf 'payload\trecord\tname\tversion_or_value\tchecksum\n'
    LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/go-build-info"
  } > "$source_root/GO-BUILD-INFO.tsv"
  {
    printf 'module\tversion\tchecksum\n'
    cat "$RELEASE_MATERIALS_WORK/go-modules-sorted"
  } > "$source_root/GO-MODULES.tsv"
  release_materials_hash_tree "$RELEASE_MATERIALS_STAGE" "$RELEASE_MATERIALS_UNIT" \
    "$source_root/MATERIALS.sha256"
  find "$RELEASE_MATERIALS_STAGE/share" -type d -exec chmod 0755 {} +
  find "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT" "$source_root" -type f -exec chmod 0644 {} +
}

release_materials_validate_payload() {
  local root="$1" payload="$2" path carrier embedded
  local -a paths
  IFS=, read -r -a paths <<< "$payload"
  for path in "${paths[@]}"; do
    carrier="${path%%:*}"
    if [[ "$path" == *:* ]]; then
      embedded="${path#*:}"
      if [[ "$embedded" != /* ]] || ! release_materials_safe_relative "${embedded#/}"; then
        fail "unsafe embedded payload path: $path"
      fi
    fi
    case "$carrier" in
      */\*)
        carrier="${carrier%/*}"
        release_materials_safe_relative "$carrier" \
          || fail "unsafe payload directory: $path"
        if [ ! -d "$root/$carrier" ] \
          || [ -z "$(find "$root/$carrier" -type f -print -quit)" ]; then
          fail "declared payload directory has no files: $path"
        fi
        ;;
      *)
        release_materials_safe_relative "$carrier" \
          || fail "unsafe payload path: $path"
        [ -f "$root/$carrier" ] || fail "declared payload is not shipped: $path"
        ;;
    esac
  done
}

release_materials_require_source() {
  local root="$1" unit="$2" payload="$3" name="$4" version="$5" source="${6:-}" integrity="${7:-}"
  local label=""
  case "$name" in
    "Go toolchain") label="go-toolchain/$version" ;;
    "$unit") label=project ;;
    accelerator|connector|sandboxer) label="$name" ;;
  esac
  [ -z "$label" ] || label="share/licenses/$unit/$label"
  awk -F '\t' -v payload="$payload" -v name="$name" -v version="$version" \
    -v source="$source" -v integrity="$integrity" -v label="$label" '
    NR > 1 && $1 == payload && $2 == name {
      rows++
      if ((version != "" && $3 != version) || (source != "" && $4 != source) ||
          (integrity != "" && $5 != integrity) || (label != "" && $6 != label)) invalid=1
    }
    END { exit rows != 1 || invalid }
  ' "$root/share/sources/$unit/SOURCES.tsv" \
    || fail "missing or inconsistent source record for $name ($payload)"
}

release_materials_require_go_key() {
  # After release_materials_validate authenticates each observed payload once,
  # require every official payload without repeating its expensive verification.
  local root="$1" unit="$2" payload="$3"
  awk -F '\t' -v payload="$payload" '
    NR > 1 && $1 == payload { found = 1 }
    END { exit !found }
  ' "$root/share/sources/$unit/GO-BUILD-INFO.tsv" \
    || fail "missing required Go payload record: $payload"
}

release_materials_require_go() {
  local root="$1" unit="$2" payload="$3" toolchain
  local info="$root/share/sources/$unit/GO-BUILD-INFO.tsv"
  toolchain="$(awk -F '\t' -v payload="$payload" '
    NR > 1 && $1 == payload && $2 == "toolchain" && $3 == "go" { print $4 }
  ' "$info")"
  [[ "$toolchain" =~ ^go[0-9]+\.[0-9]+(\.[0-9]+)?([a-z]+[0-9]+)?(-X:[A-Za-z0-9_,]+|\ X:[A-Za-z0-9_,]+)?$ ]] \
    || fail "missing or inconsistent Go toolchain for $payload"
  awk -F '\t' -v payload="$payload" '
    NR > 1 && $1 == payload && $2 == "main-package" { rows++ }
    END { exit rows != 1 }
  ' "$info" || fail "missing or inconsistent Go package record for $payload"
  toolchain="${toolchain%% *}"
  toolchain="${toolchain%%-X:*}"
  release_materials_require_source "$root" "$unit" "$payload" "Go toolchain" "${toolchain%%-X:*}"
  (
    local verify_work identity source checksum
    verify_work="$(mktemp -d "$WORK/verify-toolchain-license.XXXXXX")"
    release_materials_init "$verify_work/stage" "$verify_work/materials" "$unit"
    identity="$(release_materials_go_toolchain_materials "$toolchain" - "$verify_work/licenses")" \
      || fail "Go toolchain source authentication failed"
    IFS=$'\t' read -r source checksum <<< "$identity"
    release_materials_require_source "$root" "$unit" "$payload" "Go toolchain" "$toolchain" "$source" "$checksum"
    diff -r "$verify_work/licenses" "$root/share/licenses/$unit/go-toolchain/$toolchain" >/dev/null \
      || fail "Go toolchain license bytes differ from the authenticated distribution"
  ) || return 1
  if [[ "$payload" != *:* ]]; then
    (
      local validation_work
      validation_work="$(mktemp -d "$WORK/go-records.XXXXXX")"
      release_materials_init "$validation_work/stage" "$validation_work/materials" "$unit"
      release_materials_add_go_binary "$root/$payload" "$payload"
      LC_ALL=C sort -u "$validation_work/materials/go-build-info" > "$validation_work/expected"
      awk -F '\t' -v payload="$payload" 'NR > 1 && $1 == payload' "$info" \
        | LC_ALL=C sort -u > "$validation_work/actual"
      cmp -s "$validation_work/expected" "$validation_work/actual" \
        || fail "Go build records differ from the shipped binary: $payload"
    )
  fi
}

release_materials_validate() {
  [ "$#" -eq 2 ] || fail "release_materials_validate requires extracted root and unit"
  local root="$1" unit="$2" source_root="$1/share/sources/$2"
  local file actual header directory payload module version checksum
  release_materials_safe_relative "$unit" \
    || fail "unsafe release material unit: $unit"
  for directory in "$root/share" "$root/share/licenses" "$root/share/sources" \
    "$root/share/licenses/$unit" "$source_root"; do
    [ -d "$directory" ] || fail "release material directory is missing: $directory"
    [ ! -L "$directory" ] || fail "release material directory is a symbolic link: $directory"
    [ "$(stat -c '%a' "$directory")" = 755 ] \
      || fail "release material directory has unsafe mode: $directory"
  done
  for file in SOURCES.tsv GO-BUILD-INFO.tsv GO-MODULES.tsv MATERIALS.sha256; do
    [ -s "$source_root/$file" ] || fail "release source material is missing: share/sources/$unit/$file"
    [ "$(stat -c '%a' "$source_root/$file")" = 644 ] \
      || fail "release source material has unsafe mode: share/sources/$unit/$file"
    [ "$(stat -c '%s' "$source_root/$file")" -le 16777216 ] \
      || fail "release source material is too large: share/sources/$unit/$file"
  done
  awk -F '\t' '
    NR == 1 {
      if ($0 != "payload\tname\tversion\tsource\tintegrity\tlicense_directory") {
        exit 1
      }
      next
    }
    NF != 6 || NR > 16385 || seen[$0]++ { exit 1 }
    {
      for (field = 1; field <= 6; field++) {
        if ($field == "") {
          exit 1
        }
      }
      rows++
    }
    END {
      if (rows < 1) {
        exit 1
      }
    }
  ' "$source_root/SOURCES.tsv" || fail "invalid SOURCES.tsv records"
  IFS= read -r header < "$source_root/GO-BUILD-INFO.tsv" \
    || fail "invalid GO-BUILD-INFO.tsv header"
  [ "$header" = $'payload\trecord\tname\tversion_or_value\tchecksum' ] \
    || fail "invalid GO-BUILD-INFO.tsv header"
  IFS= read -r header < "$source_root/GO-MODULES.tsv" \
    || fail "invalid GO-MODULES.tsv header"
  [ "$header" = $'module\tversion\tchecksum' ] \
    || fail "invalid GO-MODULES.tsv header"
  awk -F '\t' '
    NR == 1 { next }
    NF != 5 || NR > 16385 { exit 1 }
    {
      for (field = 1; field <= 5; field++) if ($field == "") exit 1
      if ($2 !~ /^(toolchain|main-package|main-module|module|replacement|build-setting)$/) exit 1
    }
  ' "$source_root/GO-BUILD-INFO.tsv" || fail "invalid Go build records"
  # Validate every key before any toolchain or dependency fetch. Path aliases
  # must not multiply verification work for one actual official executable.
  while IFS= read -r payload; do
    release_materials_go_payload_allowed "$unit" "$payload" || return 1
  done < <(awk -F '\t' 'NR > 1 { print $1 }' "$source_root/GO-BUILD-INFO.tsv" | LC_ALL=C sort -u)
  awk -F '\t' '
    NR == 1 { next }
    NF != 3 || $1 == "" || $2 == "" || $3 == "" { exit 1 }
  ' "$source_root/GO-MODULES.tsv" || fail "invalid Go module records"
  # Records are sorted by payload/type, not by dependency order. Apply each
  # replacement to the module in that SAME binary before taking the union.
  awk -F '\t' '
    NR > 1 && $2 == "module" { modules[$1 SUBSEP $3] = $3 FS $4 FS $5 }
    NR > 1 && $2 == "replacement" && $4 != "local-source" {
      separator = index($4, "@")
      if (separator <= 1 || separator == length($4)) { invalid = 1; next }
      replacements[$1 SUBSEP $3] = substr($4, 1, separator - 1) FS substr($4, separator + 1) FS $5
    }
    END {
      if (invalid) exit 1
      for (key in replacements) if (!(key in modules)) exit 1
      for (key in modules) print (key in replacements ? replacements[key] : modules[key])
    }
  ' "$source_root/GO-BUILD-INFO.tsv" | LC_ALL=C sort -u > "$WORK/expected-go-modules-$unit" \
    || fail "invalid effective Go module records"
  sed -n '2,$p' "$source_root/GO-MODULES.tsv" > "$WORK/actual-go-modules-$unit"
  cmp -s "$WORK/expected-go-modules-$unit" "$WORK/actual-go-modules-$unit" \
    || fail "Go module inventory differs from the build records"
  while IFS=$'\t' read -r payload _ _ _ _ file || [ -n "$payload" ]; do
    release_materials_validate_payload "$root" "$payload"
    release_materials_safe_relative "$file" \
      || fail "unsafe declared license directory: $file"
    case "$file" in
      "share/licenses/$unit/"?*) ;;
      *) fail "declared license directory is outside share/licenses/$unit: $file" ;;
    esac
    [ -d "$root/$file" ] || fail "declared license directory is missing: $file"
    [ -n "$(find "$root/$file" -type f -print -quit)" ] \
      || fail "declared license directory is empty: $file"
  done < <(sed -n '2,$p' "$source_root/SOURCES.tsv")
  while IFS= read -r payload; do
    release_materials_validate_payload "$root" "$payload"
    release_materials_require_go "$root" "$unit" "$payload" || return 1
  done < <(awk -F '\t' 'NR > 1 { print $1 }' "$source_root/GO-BUILD-INFO.tsv" | LC_ALL=C sort -u)
  while IFS=$'\t' read -r module version checksum || [ -n "$module" ]; do
    case "$module" in github.com/kuasar-sandbox/*) continue ;; esac
    [[ "$checksum" =~ ^h1:[A-Za-z0-9+/]{43}=$ ]] \
      || fail "third-party Go source requires an authenticated module checksum: $module@$version"
    directory="share/licenses/$unit/go/$module@$version"
    release_materials_safe_relative "$directory" || fail "unsafe Go module license path"
    if [ ! -d "$root/$directory" ] || [ -z "$(find "$root/$directory" -type f -print -quit)" ]; then
      fail "Go module license material is missing: $module@$version"
    fi
    if [ "$checksum" != - ]; then
      (
        local verify_work verified_source
        verify_work="$(mktemp -d "$WORK/verify-module-license.XXXXXX")"
        release_materials_init "$verify_work/stage" "$verify_work/materials" "$unit"
        verified_source="$(release_materials_verified_go_source "$module" "$version" "$checksum")"
        release_materials_copy_licenses "$verified_source" "go/$module@$version"
        diff -r "$verify_work/stage/$directory" "$root/$directory" >/dev/null \
          || fail "Go module license bytes differ from the verified source: $module@$version"
      ) || return 1
    fi
  done < <(sed -n '2,$p' "$source_root/GO-MODULES.tsv")
  if find "$root/share/licenses/$unit" "$source_root" -type l -print -quit | grep -q .; then
    fail "release materials contain a symbolic link"
  fi
  while IFS= read -r file; do
    [ "$(stat -c '%a' "$file")" = 644 ] \
      || fail "release material has unsafe mode: ${file#"$root"/}"
  done < <(find "$root/share/licenses/$unit" "$source_root" -type f -print)
  while IFS= read -r file; do
    [ "$(stat -c '%a' "$file")" = 755 ] \
      || fail "release material directory has unsafe mode: ${file#"$root"/}"
  done < <(find "$root/share/licenses/$unit" "$source_root" -type d -print)
  actual="$WORK/licenses-actual-$unit"
  release_materials_hash_tree "$root" "$unit" "$actual"
  cmp -s "$source_root/MATERIALS.sha256" "$actual" \
    || fail "release material inventory does not match the archive"
  (cd "$root" && sha256sum --quiet -c "share/sources/$unit/MATERIALS.sha256") \
    || fail "release material checksum validation failed"
  if grep -E $'(^|\t)(/home/|/mnt/|/tmp/|[A-Za-z]:\\\\)' \
    "$source_root/SOURCES.tsv" "$source_root/GO-BUILD-INFO.tsv" >/dev/null; then
    fail "release source material contains a machine-local path"
  fi
}
