#!/usr/bin/env bash

# Deterministic license and source material support for release archives.
# This file is sourced by scripts/release.sh; the caller provides fail().

release_materials_safe_relative() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z0-9._+@:/~=-]+$ ]] \
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
  local source="$1" expected="$2" name="$3" actual source_root status
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
  status="$(git -C "$source" status --porcelain=v1 --untracked-files=all --ignore-submodules=none)"
  [ -z "$status" ] || fail "$name source worktree is dirty"
  printf '%s\n' "$actual"
}

release_materials_copy_licenses() {
  [ "$#" -eq 2 ] || fail "release_materials_copy_licenses requires source directory and destination label"
  local source="$1" label="$2" destination file relative count=0
  [ -d "$source" ] || fail "license source directory is missing: $source"
  release_materials_safe_relative "$label" \
    || fail "unsafe release license label: $label"
  destination="$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label"
  mkdir -p "$destination"
  if find "$source" -mindepth 1 -maxdepth 1 -type l \
    \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \
       -o -iname 'AUTHORS*' -o -iname 'CREDITS*' -o -iname 'COPYRIGHT*' \) \
    -print -quit | grep -q .; then
    fail "top-level license material must not be a symbolic link: $source"
  fi
  if [ -d "$source/LICENSES" ] \
    && find "$source/LICENSES" -type l -print -quit | grep -q .; then
    fail "license directory contains a symbolic link: $source/LICENSES"
  fi

  while IFS= read -r file; do
    [ ! -L "$file" ] || fail "license material must not be a symbolic link: $file"
    relative="${file#"$source"/}"
    mkdir -p "$destination/$(dirname "$relative")"
    install -m 0644 "$file" "$destination/$relative"
    count=$((count + 1))
  done < <(
    {
      find "$source" -mindepth 1 -maxdepth 1 -type f \
        \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'PATENTS*' \
           -o -iname 'AUTHORS*' -o -iname 'CREDITS*' -o -iname 'COPYRIGHT*' \) -print
      [ ! -d "$source/LICENSES" ] || find "$source/LICENSES" -type f -print
    } | LC_ALL=C sort
  )
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
  local binary="$1" archive_path="$2" raw toolchain
  [ -f "$binary" ] || fail "Go release payload is missing: $binary"
  release_materials_safe_relative "$archive_path" \
    || fail "unsafe Go payload archive path: $archive_path"
  raw="$RELEASE_MATERIALS_WORK/go-version-$(( $(find "$RELEASE_MATERIALS_WORK" -maxdepth 1 -name 'go-version-*' | wc -l) + 1 ))"
  go version -m "$binary" > "$raw" 2>/dev/null \
    || fail "Go build info is missing from $binary"
  toolchain="$(awk 'NR == 1 { sub(/^.*: /, ""); print; exit }' "$raw")"
  [[ "$toolchain" =~ ^go[0-9] ]] || fail "cannot read the Go toolchain from $binary"
  printf '%s\ttoolchain\tgo\t%s\t-\n' "$archive_path" "$toolchain" \
    >> "$RELEASE_MATERIALS_WORK/go-build-info"
  printf '%s\n' "$toolchain" >> "$RELEASE_MATERIALS_WORK/go-toolchains"
  release_materials_record_source "$archive_path" "Go toolchain" "$toolchain" \
    "https://go.dev/dl/$toolchain.src.tar.gz" "go-version:$toolchain" \
    "go-toolchain/$toolchain"
  awk -F '\t' -v payload="$archive_path" \
    -v info="$RELEASE_MATERIALS_WORK/go-build-info" \
    -v modules="$RELEASE_MATERIALS_WORK/go-modules" '
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
        print $3 "\t" version "\t" checksum >> modules
        previous = $3
      }
      $2 == "=>" && previous != "" {
        if ($3 ~ /^\// || $3 ~ /^\.\.?\// || $4 == "(devel)" || $4 == "") {
          target = "local-source"
        } else {
          target = $3 "@" $4
        }
        print payload "\treplacement\t" previous "\t" target "\t-" >> info
      }
      $2 == "build" {
        split($3, setting, "=")
        if (setting[1] == "GOOS" || setting[1] == "GOARCH" ||
            setting[1] == "CGO_ENABLED" || setting[1] == "-tags" ||
            setting[1] == "vcs" || setting[1] == "vcs.revision" ||
            setting[1] == "vcs.time" || setting[1] == "vcs.modified") {
          print payload "\tbuild-setting\t" setting[1] "\t" substr($3, length(setting[1]) + 2) "\t-" >> info
        }
      }
    ' "$raw"
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
  local module version json directory toolchain toolchain_root installed_toolchain
  command -v jq >/dev/null || fail "jq is required to collect Go module license material"
  while IFS= read -r toolchain; do
    [ -n "$toolchain" ] || continue
    toolchain_root="$(GOTOOLCHAIN="$toolchain" go env GOROOT 2>/dev/null)" \
      || fail "cannot locate license material for Go toolchain $toolchain"
    installed_toolchain="$(GOTOOLCHAIN="$toolchain" go version 2>/dev/null | awk '{print $3}')"
    [ "$installed_toolchain" = "$toolchain" ] \
      || fail "resolved Go toolchain $installed_toolchain does not match $toolchain"
    release_materials_copy_licenses "$toolchain_root" "go-toolchain/$toolchain"
  done < <(LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/go-toolchains")
  LC_ALL=C sort -u "$RELEASE_MATERIALS_WORK/go-modules" \
    > "$RELEASE_MATERIALS_WORK/go-modules-sorted"
  while IFS=$'\t' read -r module version _; do
    [ -n "$module" ] || continue
    case "$module" in
      github.com/kuasar-sandbox/*) continue ;;
    esac
    json="$(GOWORK=off go mod download -json "$module@$version")" \
      || fail "cannot resolve Go module source for $module@$version"
    directory="$(jq -er '.Dir' <<< "$json")" \
      || fail "Go module source directory is missing for $module@$version"
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

release_materials_validate() {
  [ "$#" -eq 2 ] || fail "release_materials_validate requires extracted root and unit"
  local root="$1" unit="$2" source_root="$1/share/sources/$2"
  local file actual header directory
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
  done
  awk -F '\t' '
    NR == 1 {
      if ($0 != "payload\tname\tversion\tsource\tintegrity\tlicense_directory") {
        exit 1
      }
      next
    }
    NF != 6 { exit 1 }
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
  while IFS=$'\t' read -r _ _ _ _ _ file; do
    release_materials_safe_relative "$file" \
      || fail "unsafe declared license directory: $file"
    case "$file" in
      "share/licenses/$unit/"?*) ;;
      *) fail "declared license directory is outside share/licenses/$unit: $file" ;;
    esac
    [ -d "$root/$file" ] || fail "declared license directory is missing: $file"
  done < <(sed -n '2,$p' "$source_root/SOURCES.tsv")
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
