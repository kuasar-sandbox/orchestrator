#!/usr/bin/env bash

set -euo pipefail

ORGANIZATION=kuasar-sandbox
COMPONENTS=(accelerator connector sandboxer orchestrator runtime vmlinux)
API_URL="${GITHUB_API_URL:-https://api.github.com}"

fail() {
  echo "aggregate-release: $*" >&2
  exit 1
}

component_repository() {
  case "$1" in
    accelerator|connector|orchestrator|sandboxer) printf '%s/%s\n' "$ORGANIZATION" "$1" ;;
    runtime|vmlinux) printf '%s/guest-runtime\n' "$ORGANIZATION" ;;
    *) fail "unknown component: $1" ;;
  esac
}

component_archive() {
  local name="$1" version="$2" architecture="$3"
  case "$name" in
    accelerator|connector|orchestrator|sandboxer)
      printf '%s-%s-linux-%s.tar.gz\n' "$name" "$version" "$architecture"
      ;;
    runtime) printf 'sandbox-runtime-%s-%s.tar.gz\n' "$architecture" "$version" ;;
    vmlinux) printf 'vmlinux-%s-%s.tar.gz\n' "$architecture" "$version" ;;
  esac
}

validate_mapping() {
  local mapping="$1"
  [ -f "$mapping" ] || fail "mapping file not found: $mapping"
  jq -e '
    .schemaVersion == 1
    and (.version | test("^release-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.architecture == "x86_64" or .architecture == "aarch64")
    and (.components | keys == ["accelerator", "connector", "orchestrator", "runtime", "sandboxer", "vmlinux"])
    and (.components.accelerator | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.components.connector | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.components.orchestrator | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.components.sandboxer | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.components.runtime | test("^runtime-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.components.vmlinux | test("^vmlinux-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (keys == ["architecture", "components", "schemaVersion", "version"])
  ' "$mapping" >/dev/null || fail "release mapping failed schema validation"
}

api_get() {
  local endpoint="$1"
  curl --fail --show-error --silent \
    --retry 4 --retry-all-errors --connect-timeout 10 --max-time 60 \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GH_TOKEN" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$API_URL/$endpoint"
}

download_asset() {
  local repository="$1" asset_id="$2" output="$3"
  curl --fail --show-error --silent --location \
    --retry 4 --retry-all-errors --connect-timeout 10 --max-time 600 \
    -H "Accept: application/octet-stream" \
    -H "Authorization: Bearer $GH_TOKEN" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$API_URL/repos/$repository/releases/assets/$asset_id" > "$output"
}

verify_download() {
  local file="$1" asset="$2"
  local expected_size expected_digest actual_size actual_digest
  expected_size="$(jq -er '.size' <<< "$asset")"
  expected_digest="$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' <<< "$asset")"
  actual_size="$(stat -c '%s' "$file")"
  actual_digest="sha256:$(sha256sum "$file" | awk '{print $1}')"
  [ "$actual_size" = "$expected_size" ] || fail "downloaded size mismatch: $(basename "$file")"
  [ "$actual_digest" = "$expected_digest" ] || fail "downloaded digest mismatch: $(basename "$file")"
}

validate_component_manifest() {
  local name="$1" version="$2" architecture="$3" repository="$4" archive="$5" manifest="$6"
  jq -e \
    --arg name "$name" \
    --arg version "$version" \
    --arg architecture "$architecture" \
    --arg repository "$repository" \
    --arg archive "$archive" '
      .schemaVersion == 1
      and .kind == "component"
      and .name == $name
      and .version == $version
      and .tag == $version
      and .architecture == $architecture
      and .repository == $repository
      and .archive == $archive
      and (.commit | test("^[0-9a-f]{40}$"))
      and (.sources | type == "array" and length >= 1)
      and (.commit as $commit | [.sources[] | select(.repository == $repository and .commit == $commit)] | length == 1)
      and (
        $name != "runtime"
        or ([.sources[] | select(
          .repository == "kuasar-sandbox/sandboxer"
          and (.requestedRef | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
        )] | length == 1)
      )
      and (.validation.workflowRepository == $repository)
      and (.validation.workflowRunId | type == "number" and . > 0)
      and (.validation.workflowRunAttempt | type == "number" and . > 0)
      and (.validation.workflowRunUrl | type == "string" and length > 0)
      and (.artifacts | length == 2)
      and ([.artifacts[].name] | sort == (["SHA256SUMS", $archive] | sort))
      and ([.artifacts[].sha256] | all(test("^[0-9a-f]{64}$")))
      and ([.artifacts[].size] | all(type == "number" and . >= 0))
    ' "$manifest" >/dev/null || fail "$name release.json failed validation"
}

validate_resolved() {
  local resolved="$1"
  [ -f "$resolved" ] || fail "resolved release file not found: $resolved"
  jq -e '
    .schemaVersion == 1
    and .kind == "resolved-aggregate"
    and (.version | test("^release-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and (.architecture == "x86_64" or .architecture == "aarch64")
    and (.mapping.repository == "kuasar-sandbox/orchestrator")
    and .mapping.branch == "release"
    and (.mapping.commit | test("^[0-9a-f]{40}$"))
    and (.mapping.path == ("releases/" + .version + ".json"))
    and (.mapping.sha256 | test("^[0-9a-f]{64}$"))
    and (.components | keys == ["accelerator", "connector", "orchestrator", "runtime", "sandboxer", "vmlinux"])
  ' "$resolved" >/dev/null || fail "resolved aggregate failed schema validation"

  local mapping="$WORK/resolved-mapping.json"
  jq '.mapping.spec' "$resolved" > "$mapping"
  validate_mapping "$mapping"
  [ "$(jq -er '.version' "$resolved")" = "$(jq -er '.version' "$mapping")" ] \
    || fail "resolved version does not match mapping"
  [ "$(jq -er '.architecture' "$resolved")" = "$(jq -er '.architecture' "$mapping")" ] \
    || fail "resolved architecture does not match mapping"
  [ "$(sha256sum "$mapping" | awk '{print $1}')" = "$(jq -er '.mapping.canonicalSha256' "$resolved")" ] \
    || fail "canonical mapping digest mismatch"

  local name version repository archive manifest
  for name in "${COMPONENTS[@]}"; do
    version="$(jq -er --arg name "$name" '.mapping.spec.components[$name]' "$resolved")"
    repository="$(component_repository "$name")"
    archive="$(component_archive "$name" "$version" "$(jq -er '.architecture' "$resolved")")"
    jq -e \
      --arg name "$name" \
      --arg version "$version" \
      --arg repository "$repository" \
      --arg archive "$archive" '
        .components[$name].name == $name
        and .components[$name].version == $version
        and .components[$name].repository == $repository
        and .components[$name].archive == $archive
        and (.components[$name].commit | test("^[0-9a-f]{40}$"))
        and (.components[$name].assets | length == 3)
        and ([.components[$name].assets[].name] | sort == (["SHA256SUMS", "release.json", $archive] | sort))
        and ([.components[$name].assets[].digest] | all(test("^sha256:[0-9a-f]{64}$")))
      ' "$resolved" >/dev/null || fail "resolved component is invalid: $name"
    manifest="$WORK/$name-release.json"
    jq --arg name "$name" '.components[$name].manifest' "$resolved" > "$manifest"
    validate_component_manifest "$name" "$version" "$(jq -er '.architecture' "$resolved")" \
      "$repository" "$archive" "$manifest"
  done

}

resolve_release() {
  [ "$#" -eq 3 ] || fail "usage: aggregate-release.sh resolve <mapping.json> <mapping-commit> <output-dir>"
  local mapping="$1" mapping_commit="$2" output="$3"
  validate_mapping "$mapping"
  [[ "$mapping_commit" =~ ^[0-9a-f]{40}$ ]] || fail "mapping commit must be a full commit SHA"
  [ ! -e "$output" ] || fail "output already exists: $output"
  : "${GH_TOKEN:?GH_TOKEN is required}"

  mkdir -p "$output"
  jq -S . "$mapping" > "$output/mapping.json"
  local version architecture
  version="$(jq -er '.version' "$output/mapping.json")"
  architecture="$(jq -er '.architecture' "$output/mapping.json")"
  local components="$WORK/components.ndjson"
  : > "$components"

  local name tag repository archive release_state expected_names actual_names
  local release_manifest sums_file release_asset sums_asset archive_asset tag_state tag_commit
  for name in "${COMPONENTS[@]}"; do
    tag="$(jq -er --arg name "$name" '.components[$name]' "$output/mapping.json")"
    repository="$(component_repository "$name")"
    archive="$(component_archive "$name" "$tag" "$architecture")"
    release_state="$WORK/$name-release-state.json"
    api_get "repos/$repository/releases/tags/$tag" > "$release_state"
    local prerelease=false
    [[ "$tag" != *-preview.* ]] || prerelease=true
    jq -e --arg tag "$tag" --argjson prerelease "$prerelease" '
      .tag_name == $tag and .draft == false and .prerelease == $prerelease
    ' "$release_state" >/dev/null || fail "$repository $tag is not a published release"

    expected_names="$WORK/$name-expected-assets"
    actual_names="$WORK/$name-actual-assets"
    printf '%s\n' "$archive" SHA256SUMS release.json | LC_ALL=C sort > "$expected_names"
    jq -r '.assets[].name' "$release_state" | LC_ALL=C sort > "$actual_names"
    cmp -s "$expected_names" "$actual_names" || fail "$repository $tag has an unexpected asset set"

    release_asset="$(jq -ec '.assets[] | select(.name == "release.json")' "$release_state")"
    sums_asset="$(jq -ec '.assets[] | select(.name == "SHA256SUMS")' "$release_state")"
    archive_asset="$(jq -ec --arg archive "$archive" '.assets[] | select(.name == $archive)' "$release_state")"
    release_manifest="$WORK/$name-release.json"
    sums_file="$WORK/$name-SHA256SUMS"
    download_asset "$repository" "$(jq -er '.id' <<< "$release_asset")" "$release_manifest"
    download_asset "$repository" "$(jq -er '.id' <<< "$sums_asset")" "$sums_file"
    verify_download "$release_manifest" "$release_asset"
    verify_download "$sums_file" "$sums_asset"
    validate_component_manifest "$name" "$tag" "$architecture" "$repository" "$archive" "$release_manifest"

    local archive_sha archive_size sums_sha sums_size
    archive_sha="$(jq -er --arg archive "$archive" '.artifacts[] | select(.name == $archive) | .sha256' "$release_manifest")"
    archive_size="$(jq -er --arg archive "$archive" '.artifacts[] | select(.name == $archive) | .size' "$release_manifest")"
    sums_sha="$(jq -er '.artifacts[] | select(.name == "SHA256SUMS") | .sha256' "$release_manifest")"
    sums_size="$(jq -er '.artifacts[] | select(.name == "SHA256SUMS") | .size' "$release_manifest")"
    if [ "sha256:$archive_sha" != "$(jq -er '.digest' <<< "$archive_asset")" ] \
      || [ "$archive_size" != "$(jq -er '.size' <<< "$archive_asset")" ]; then
      fail "$name archive metadata differs between release.json and GitHub"
    fi
    if [ "sha256:$sums_sha" != "$(jq -er '.digest' <<< "$sums_asset")" ] \
      || [ "$sums_size" != "$(jq -er '.size' <<< "$sums_asset")" ]; then
      fail "$name checksum metadata differs between release.json and GitHub"
    fi
    [ "$(cat "$sums_file")" = "$archive_sha  $archive" ] \
      || fail "$name SHA256SUMS does not describe exactly its archive"

    tag_state="$WORK/$name-tag.json"
    api_get "repos/$repository/git/ref/tags/$tag" > "$tag_state"
    tag_commit="$(jq -er '.object.sha' "$tag_state")"
    [ "$tag_commit" = "$(jq -er '.commit' "$release_manifest")" ] \
      || fail "$repository $tag does not point to the release manifest commit"

    jq -cn \
      --arg name "$name" \
      --arg repository "$repository" \
      --arg version "$tag" \
      --arg commit "$tag_commit" \
      --arg archive "$archive" \
      --argjson manifest "$(cat "$release_manifest")" \
      --argjson assets "$(jq '[.assets[] | {id, name, digest, size, url}]' "$release_state")" '
        {
          name: $name,
          repository: $repository,
          version: $version,
          commit: $commit,
          archive: $archive,
          manifest: $manifest,
          assets: $assets
        }
      ' >> "$components"
  done

  local component_object="$WORK/components.json"
  jq -s 'map({key: .name, value: .}) | from_entries' "$components" > "$component_object"
  local mapping_sha canonical_mapping_sha mapping_path
  mapping_sha="$(sha256sum "$mapping" | awk '{print $1}')"
  canonical_mapping_sha="$(sha256sum "$output/mapping.json" | awk '{print $1}')"
  mapping_path="releases/$version.json"
  jq -n \
    --arg version "$version" \
    --arg architecture "$architecture" \
    --arg mapping_commit "$mapping_commit" \
    --arg mapping_path "$mapping_path" \
    --arg mapping_sha "$mapping_sha" \
    --arg canonical_mapping_sha "$canonical_mapping_sha" \
    --slurpfile mapping "$output/mapping.json" \
    --slurpfile components "$component_object" '
      {
        schemaVersion: 1,
        kind: "resolved-aggregate",
        version: $version,
        architecture: $architecture,
        mapping: {
          repository: "kuasar-sandbox/orchestrator",
          branch: "release",
          commit: $mapping_commit,
          path: $mapping_path,
          sha256: $mapping_sha,
          canonicalSha256: $canonical_mapping_sha,
          spec: $mapping[0]
        },
        components: $components[0]
      }
    ' > "$output/resolved.json"
  validate_resolved "$output/resolved.json"
  echo "==> resolved $version from release mapping $mapping_commit"
}

fetch_assets() {
  [ "$#" -eq 2 ] || fail "usage: aggregate-release.sh fetch-assets <resolved.json> <output-dir>"
  local resolved="$1" output="$2"
  validate_resolved "$resolved"
  [ ! -e "$output" ] || fail "output already exists: $output"
  : "${GH_TOKEN:?GH_TOKEN is required}"
  mkdir -p "$output/assets"
  install -m 0644 "$resolved" "$output/resolved.json"

  local name repository archive asset archive_path
  for name in "${COMPONENTS[@]}"; do
    repository="$(jq -er --arg name "$name" '.components[$name].repository' "$resolved")"
    archive="$(jq -er --arg name "$name" '.components[$name].archive' "$resolved")"
    asset="$(jq -ec --arg name "$name" --arg archive "$archive" \
      '.components[$name].assets[] | select(.name == $archive)' "$resolved")"
    archive_path="$output/assets/$archive"
    download_asset "$repository" "$(jq -er '.id' <<< "$asset")" "$archive_path"
    verify_download "$archive_path" "$asset"
  done
  echo "==> fetched six independently published component archives"
}

materialize_assets() {
  [ "$#" -eq 3 ] \
    || fail "usage: aggregate-release.sh materialize-assets <resolved.json> <staged-dir> <output-dir>"
  local resolved="$1" staged="$2" output="$3"
  validate_resolved "$resolved"
  [ -f "$staged/resolved.json" ] || fail "staged resolved release file is missing"
  [ -d "$staged/assets" ] || fail "staged component assets are missing"
  cmp -s "$resolved" "$staged/resolved.json" \
    || fail "staged component assets use a different resolved release"
  [ ! -e "$output" ] || fail "output already exists: $output"

  local expected="$WORK/staged-expected" actual="$WORK/staged-actual"
  {
    printf 'assets\n'
    printf 'resolved.json\n'
  } | LC_ALL=C sort > "$expected"
  find "$staged" -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" || fail "staged release contains unexpected top-level entries"

  : > "$expected"
  local name archive asset archive_path metadata expected_metadata bin_stage file
  for name in "${COMPONENTS[@]}"; do
    jq -er --arg name "$name" '.components[$name].archive' "$resolved" >> "$expected"
  done
  LC_ALL=C sort -o "$expected" "$expected"
  find "$staged/assets" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' \
    | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" || fail "staged release contains an unexpected asset set"
  if find "$staged/assets" -mindepth 1 -maxdepth 1 ! -type f -print -quit | grep -q .; then
    fail "staged release contains a non-file asset"
  fi

  mkdir -p "$output/assets" "$output/install/bin"
  install -m 0644 "$resolved" "$output/resolved.json"
  for name in "${COMPONENTS[@]}"; do
    archive="$(jq -er --arg name "$name" '.components[$name].archive' "$resolved")"
    asset="$(jq -ec --arg name "$name" --arg archive "$archive" \
      '.components[$name].assets[] | select(.name == $archive)' "$resolved")"
    archive_path="$staged/assets/$archive"
    verify_download "$archive_path" "$asset"

    while IFS= read -r file; do
      if [[ "$file" = /* || "$file" = ../* || "$file" = */../* || "$file" = */.. \
        || "$file" = *$'\n'* ]]; then
        fail "$archive contains an unsafe path"
      fi
      case "$file" in
        .|./|./bin|./bin/) ;;
        ./bin/*) ;;
        *) continue ;;
      esac
    done < <(tar -tzf "$archive_path")
    bin_stage="$WORK/$name-bin"
    mkdir -p "$bin_stage"
    tar -xzf "$archive_path" -C "$bin_stage" ./bin
    if find "$bin_stage" -type l -print -quit | grep -q .; then
      fail "$archive contains a symbolic link under bin/"
    fi
    while IFS= read -r -d '' file; do
      local basename
      basename="$(basename "$file")"
      [ ! -e "$output/install/bin/$basename" ] || fail "duplicate aggregate binary: $basename"
      cp -p "$file" "$output/install/bin/$basename"
    done < <(find "$bin_stage/bin" -mindepth 1 -maxdepth 1 -type f -print0)

    metadata="$(tar -xOzf "$archive_path" "./release/$name.json" 2>/dev/null)" \
      || fail "$archive does not contain release/$name.json"
    expected_metadata="$WORK/$name-expected-metadata.json"
    jq --arg name "$name" '.components[$name].manifest | del(.kind, .tag, .validation, .artifacts)' \
      "$resolved" > "$expected_metadata"
    jq -S . <<< "$metadata" > "$WORK/$name-metadata.json"
    jq -S . "$expected_metadata" > "$WORK/$name-expected-sorted.json"
    cmp -s "$WORK/$name-metadata.json" "$WORK/$name-expected-sorted.json" \
      || fail "$archive embedded metadata differs from the resolved release"
    install -m 0644 "$archive_path" "$output/assets/$archive"
  done
  echo "==> verified and materialized six staged component archives"
}

validate_bundle() {
  local bundle="$1"
  local manifest="$bundle/release.json"
  [ -f "$manifest" ] || fail "aggregate release.json is missing"
  [ -s "$bundle/release-notes.md" ] || fail "aggregate release-notes.md is missing"
  [ -d "$bundle/assets" ] || fail "aggregate assets directory is missing"
  jq -e '
    .schemaVersion == 1
    and .kind == "aggregate"
    and (.version | test("^release-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-preview\\.[0-9]{8})?$"))
    and .tag == .version
    and (.architecture == "x86_64" or .architecture == "aarch64")
    and (.mapping.commit | test("^[0-9a-f]{40}$"))
    and (.components | keys == ["accelerator", "connector", "orchestrator", "runtime", "sandboxer", "vmlinux"])
    and (.validation.suite == "BMS E2E")
    and (.validation.workflowRunId | type == "number" and . > 0)
    and (.validation.workflowRunAttempt | type == "number" and . > 0)
    and (.artifacts | length == 7)
    and ([.artifacts[].name] | unique | length == 7)
    and ([.artifacts[].sha256] | all(test("^[0-9a-f]{64}$")))
    and ([.artifacts[].size] | all(type == "number" and . >= 0))
  ' "$manifest" >/dev/null || fail "aggregate release.json failed schema validation"

  local resolved="$WORK/bundle-resolved.json"
  jq '{
    schemaVersion,
    kind: "resolved-aggregate",
    version,
    architecture,
    mapping,
    components
  }' "$manifest" > "$resolved"
  validate_resolved "$resolved"

  local expected="$WORK/bundle-expected" actual="$WORK/bundle-actual"
  {
    jq -r '.components.accelerator.archive' "$manifest"
    jq -r '.components.connector.archive' "$manifest"
    jq -r '.components.sandboxer.archive' "$manifest"
    jq -r '.components.orchestrator.archive' "$manifest"
    jq -r '.components.runtime.archive' "$manifest"
    jq -r '.components.vmlinux.archive' "$manifest"
    printf 'SHA256SUMS\n'
  } | LC_ALL=C sort > "$expected"
  jq -r '.artifacts[].name' "$manifest" | LC_ALL=C sort > "$WORK/manifest-artifacts"
  cmp -s "$expected" "$WORK/manifest-artifacts" \
    || fail "aggregate release.json contains an unexpected artifact set"
  find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" || fail "aggregate bundle contains an unexpected asset set"
  local name sha size count=0
  while IFS=$'\t' read -r name sha size; do
    [[ "$name" =~ ^[A-Za-z0-9._-]+$ ]] || fail "unsafe aggregate artifact name: $name"
    [ "$(sha256sum "$bundle/assets/$name" | awk '{print $1}')" = "$sha" ] \
      || fail "aggregate checksum mismatch: $name"
    [ "$(stat -c '%s' "$bundle/assets/$name")" = "$size" ] \
      || fail "aggregate size mismatch: $name"
    count=$((count + 1))
  done < <(jq -r '.artifacts[] | [.name, .sha256, (.size | tostring)] | @tsv' "$manifest")
  [ "$count" -eq 7 ] || fail "aggregate must describe seven assets"
  (cd "$bundle/assets" && sha256sum --quiet -c SHA256SUMS) \
    || fail "aggregate SHA256SUMS validation failed"
}

prepare_release() {
  [ "$#" -eq 3 ] || fail "usage: aggregate-release.sh prepare <resolved.json> <fetched-dir> <output-dir>"
  local resolved="$1" fetched="$2" output="$3"
  validate_resolved "$resolved"
  [ -d "$fetched/assets" ] || fail "fetched component assets are missing"
  [ ! -e "$output" ] || fail "output already exists: $output"
  : "${RELEASE_WORKFLOW_REPOSITORY:?RELEASE_WORKFLOW_REPOSITORY is required}"
  : "${RELEASE_WORKFLOW_RUN_ID:?RELEASE_WORKFLOW_RUN_ID is required}"
  : "${RELEASE_WORKFLOW_RUN_ATTEMPT:?RELEASE_WORKFLOW_RUN_ATTEMPT is required}"
  : "${RELEASE_WORKFLOW_RUN_URL:?RELEASE_WORKFLOW_RUN_URL is required}"

  mkdir -p "$output/assets"
  local name archive artifacts="$WORK/aggregate-artifacts.ndjson"
  : > "$artifacts"
  for name in "${COMPONENTS[@]}"; do
    archive="$(jq -er --arg name "$name" '.components[$name].archive' "$resolved")"
    [ -f "$fetched/assets/$archive" ] || fail "fetched archive is missing: $archive"
    install -m 0644 "$fetched/assets/$archive" "$output/assets/$archive"
  done
  (cd "$output/assets" && sha256sum \
    "$(jq -er '.components.accelerator.archive' "$resolved")" \
    "$(jq -er '.components.connector.archive' "$resolved")" \
    "$(jq -er '.components.sandboxer.archive' "$resolved")" \
    "$(jq -er '.components.orchestrator.archive' "$resolved")" \
    "$(jq -er '.components.runtime.archive' "$resolved")" \
    "$(jq -er '.components.vmlinux.archive' "$resolved")" > SHA256SUMS)

  while IFS= read -r filename; do
    jq -cn \
      --arg name "$filename" \
      --arg sha256 "$(sha256sum "$output/assets/$filename" | awk '{print $1}')" \
      --argjson size "$(stat -c '%s' "$output/assets/$filename")" \
      '{name: $name, sha256: $sha256, size: $size}' >> "$artifacts"
  done < <(find "$output/assets" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)

  jq -n \
    --slurpfile resolved "$resolved" \
    --slurpfile artifacts "$artifacts" \
    --arg workflow_repository "$RELEASE_WORKFLOW_REPOSITORY" \
    --argjson workflow_run_id "$RELEASE_WORKFLOW_RUN_ID" \
    --argjson workflow_run_attempt "$RELEASE_WORKFLOW_RUN_ATTEMPT" \
    --arg workflow_run_url "$RELEASE_WORKFLOW_RUN_URL" '
      {
        schemaVersion: 1,
        kind: "aggregate",
        version: $resolved[0].version,
        tag: $resolved[0].version,
        architecture: $resolved[0].architecture,
        mapping: $resolved[0].mapping,
        components: $resolved[0].components,
        validation: {
          suite: "BMS E2E",
          workflowRepository: $workflow_repository,
          workflowRunId: $workflow_run_id,
          workflowRunAttempt: $workflow_run_attempt,
          workflowRunUrl: $workflow_run_url
        },
        artifacts: $artifacts
      }
    ' > "$output/release.json"
  {
    printf 'Kuasar Sandbox %s aggregates six independently published component versions.\n\n' \
      "$(jq -er '.version' "$resolved")"
    printf '%s\n' "The exact version mapping is stored on the \`release\` branch and embedded in \`release.json\`. The original component archives passed the full BMS E2E suite without rebuilding or repackaging. Verify all archives with \`SHA256SUMS\`."
  } > "$output/release-notes.md"
  validate_bundle "$output"
  echo "==> prepared aggregate $(jq -er '.version' "$resolved")"
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

for command in curl jq sha256sum tar; do
  command -v "$command" >/dev/null || fail "$command is required"
done

case "${1:-}" in
  validate-map)
    [ "$#" -eq 2 ] || fail "usage: aggregate-release.sh validate-map <mapping.json>"
    validate_mapping "$2"
    ;;
  validate-resolved)
    [ "$#" -eq 2 ] || fail "usage: aggregate-release.sh validate-resolved <resolved.json>"
    validate_resolved "$2"
    ;;
  resolve)
    shift
    resolve_release "$@"
    ;;
  fetch-assets)
    shift
    fetch_assets "$@"
    ;;
  materialize-assets)
    shift
    materialize_assets "$@"
    ;;
  prepare)
    shift
    prepare_release "$@"
    ;;
  validate-bundle)
    [ "$#" -eq 2 ] || fail "usage: aggregate-release.sh validate-bundle <bundle-dir>"
    validate_bundle "$2"
    ;;
  *)
    fail "usage: aggregate-release.sh <validate-map|validate-resolved|resolve|fetch-assets|materialize-assets|prepare|validate-bundle> ..."
    ;;
esac
