#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "test-release-tools: $*" >&2
  exit 1
}

mkdir -p "$TMP/api" "$TMP/assets" "$TMP/fake-bin"
cat > "$TMP/mapping.json" <<'EOF'
{
  "architecture": "x86_64",
  "components": {
    "accelerator": "v1.0.0",
    "connector": "v1.1.0",
    "orchestrator": "v1.3.0",
    "runtime": "runtime-v2.0.0",
    "sandboxer": "v1.2.0",
    "vmlinux": "vmlinux-v3.0.0"
  },
  "schemaVersion": 1,
  "version": "release-v4.0.0-preview.20260804"
}
EOF
"$SCRIPT_DIR/aggregate-release.sh" validate-map "$TMP/mapping.json"

cp "$TMP/mapping.json" "$TMP/invalid-mapping.json"
jq '.components.runtime = "v2.0.0"' "$TMP/invalid-mapping.json" > "$TMP/invalid-mapping.tmp"
mv "$TMP/invalid-mapping.tmp" "$TMP/invalid-mapping.json"
if "$SCRIPT_DIR/aggregate-release.sh" validate-map "$TMP/invalid-mapping.json" >/dev/null 2>&1; then
  fail "mapping validator accepted a runtime version without its prefix"
fi

component_repository() {
  case "$1" in
    accelerator|connector|orchestrator|sandboxer) printf 'kuasar-sandbox/%s\n' "$1" ;;
    runtime|vmlinux) printf 'kuasar-sandbox/guest-runtime\n' ;;
  esac
}

component_version() {
  jq -er --arg name "$1" '.components[$name]' "$TMP/mapping.json"
}

component_archive() {
  local name="$1" version
  version="$(component_version "$name")"
  case "$name" in
    accelerator|connector|orchestrator|sandboxer)
      printf '%s-%s-linux-x86_64.tar.gz\n' "$name" "$version"
      ;;
    runtime) printf 'sandbox-runtime-x86_64-%s.tar.gz\n' "$version" ;;
    vmlinux) printf 'vmlinux-x86_64-%s.tar.gz\n' "$version" ;;
  esac
}

components="$TMP/components.ndjson"
: > "$components"
index=0
for name in accelerator connector sandboxer orchestrator runtime vmlinux; do
  index=$((index + 1))
  version="$(component_version "$name")"
  repository="$(component_repository "$name")"
  archive="$(component_archive "$name")"
  commit="$(printf '%040d' "$index")"
  stage="$TMP/stage-$name"
  mkdir -p "$stage/bin" "$stage/release"
  case "$name" in
    accelerator) files=(manifest-ctl store-ctl cache-ctl) ;;
    connector) files=(connector-ctl) ;;
    sandboxer) files=(sandbox-ctl sandbox-init cloud-hypervisor) ;;
    orchestrator) files=(node-ctl cluster-ctl node-stub-ctl e2b-key-ctl) ;;
    runtime)
      files=(sandbox-runtime.bundle sandbox-runtime-x86_64-runtime-v2.0.0.bundle flatten-ctl mkfs.erofs)
      ;;
    vmlinux) files=(vmlinux vmlinux-x86_64-vmlinux-v3.0.0) ;;
  esac
  for file in "${files[@]}"; do
    printf '#!/bin/sh\nexit 0\n' > "$stage/bin/$file"
    case "$file" in
      sandbox-runtime*.bundle|vmlinux*) ;;
      *) chmod +x "$stage/bin/$file" ;;
    esac
  done

  sources="$TMP/$name-sources.json"
  jq -n \
    --arg repository "$repository" \
    --arg version "$version" \
    --arg commit "$commit" '
      [{repository: $repository, requestedRef: $version, commit: $commit, role: "primary"}]
    ' > "$sources"
  if [ "$name" = runtime ]; then
    jq --arg commit 0000000000000000000000000000000000000003 '
      . + [{
        repository: "kuasar-sandbox/sandboxer",
        requestedRef: "v1.2.0",
        commit: $commit,
        role: "dependency"
      }]
    ' "$sources" > "$TMP/runtime-sources.tmp"
    mv "$TMP/runtime-sources.tmp" "$sources"
  fi

  jq -n \
    --arg name "$name" \
    --arg version "$version" \
    --arg archive "$archive" \
    --arg repository "$repository" \
    --arg commit "$commit" \
    --slurpfile sources "$sources" '
      {
        schemaVersion: 1,
        name: $name,
        version: $version,
        architecture: "x86_64",
        archive: $archive,
        repository: $repository,
        commit: $commit,
        createdAt: "2026-08-04T00:00:00Z",
        sources: $sources[0]
      }
    ' > "$stage/release/$name.json"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$TMP/assets/$archive" -C "$stage" .
  archive_sha="$(sha256sum "$TMP/assets/$archive" | awk '{print $1}')"
  archive_size="$(stat -c '%s' "$TMP/assets/$archive")"
  printf '%s  %s\n' "$archive_sha" "$archive" > "$TMP/$name-SHA256SUMS"
  sums_sha="$(sha256sum "$TMP/$name-SHA256SUMS" | awk '{print $1}')"
  sums_size="$(stat -c '%s' "$TMP/$name-SHA256SUMS")"

  manifest="$TMP/$name-release.json"
  jq -n \
    --slurpfile package "$stage/release/$name.json" \
    --arg archive "$archive" \
    --arg archive_sha "$archive_sha" \
    --argjson archive_size "$archive_size" \
    --arg sums_sha "$sums_sha" \
    --argjson sums_size "$sums_size" \
    --arg repository "$repository" '
      $package[0] + {
        kind: "component",
        tag: $package[0].version,
        validation: {
          workflowRepository: $repository,
          workflowRunId: 100,
          workflowRunAttempt: 1,
          workflowRunUrl: "https://example.invalid/actions/runs/100"
        },
        artifacts: [
          {name: $archive, sha256: $archive_sha, size: $archive_size},
          {name: "SHA256SUMS", sha256: $sums_sha, size: $sums_size}
        ]
      }
    ' > "$manifest"
  manifest_sha="$(sha256sum "$manifest" | awk '{print $1}')"
  manifest_size="$(stat -c '%s' "$manifest")"
  cp "$TMP/assets/$archive" "$TMP/assets/$index"
  cp "$TMP/$name-SHA256SUMS" "$TMP/assets/$((index + 100))"
  cp "$manifest" "$TMP/assets/$((index + 200))"

  jq -n \
    --arg tag "$version" \
    --arg archive "$archive" \
    --arg archive_sha "$archive_sha" \
    --argjson archive_size "$archive_size" \
    --arg sums_sha "$sums_sha" \
    --argjson sums_size "$sums_size" \
    --arg manifest_sha "$manifest_sha" \
    --argjson manifest_size "$manifest_size" \
    --argjson id "$index" '
      {
        tag_name: $tag,
        draft: false,
        prerelease: false,
        assets: [
          {id: $id, name: $archive, digest: ("sha256:" + $archive_sha), size: $archive_size, url: "asset"},
          {id: ($id + 100), name: "SHA256SUMS", digest: ("sha256:" + $sums_sha), size: $sums_size, url: "asset"},
          {id: ($id + 200), name: "release.json", digest: ("sha256:" + $manifest_sha), size: $manifest_size, url: "asset"}
        ]
      }
    ' > "$TMP/api/release-$version.json"
  jq -n --arg commit "$commit" '{object: {sha: $commit}}' \
    > "$TMP/api/tag-$version.json"

  jq -cn \
    --arg name "$name" \
    --arg repository "$repository" \
    --arg version "$version" \
    --arg commit "$commit" \
    --arg archive "$archive" \
    --argjson manifest "$(cat "$manifest")" \
    --arg archive_sha "$archive_sha" \
    --argjson archive_size "$archive_size" \
    --arg sums_sha "$sums_sha" \
    --argjson sums_size "$sums_size" \
    --arg manifest_sha "$manifest_sha" \
    --argjson manifest_size "$manifest_size" \
    --argjson id "$index" '
      {
        name: $name,
        repository: $repository,
        version: $version,
        commit: $commit,
        archive: $archive,
        manifest: $manifest,
        assets: [
          {id: $id, name: $archive, digest: ("sha256:" + $archive_sha), size: $archive_size, url: "asset"},
          {id: ($id + 100), name: "SHA256SUMS", digest: ("sha256:" + $sums_sha), size: $sums_size, url: "asset"},
          {id: ($id + 200), name: "release.json", digest: ("sha256:" + $manifest_sha), size: $manifest_size, url: "asset"}
        ]
      }
    ' >> "$components"
done

cat > "$TMP/fake-bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
url="${*: -1}"
case "$url" in
  */releases/assets/*)
    id="${url##*/}"
    cat "${FAKE_ASSET_DIR:?}/$id"
    ;;
  */releases/tags/*)
    tag="${url##*/}"
    cat "${FAKE_API_DIR:?}/release-$tag.json"
    ;;
  */git/ref/tags/*)
    tag="${url##*/}"
    cat "${FAKE_API_DIR:?}/tag-$tag.json"
    ;;
  *)
    echo "fake curl: unsupported URL: $url" >&2
    exit 2
    ;;
esac
EOF
chmod +x "$TMP/fake-bin/curl"
env PATH="$TMP/fake-bin:$PATH" GH_TOKEN=fake \
  FAKE_API_DIR="$TMP/api" FAKE_ASSET_DIR="$TMP/assets" \
  "$SCRIPT_DIR/aggregate-release.sh" resolve "$TMP/mapping.json" \
  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "$TMP/resolution"
cp "$TMP/resolution/resolved.json" "$TMP/resolved.json"
"$SCRIPT_DIR/aggregate-release.sh" validate-resolved "$TMP/resolved.json"

env PATH="$TMP/fake-bin:$PATH" GH_TOKEN=fake \
  FAKE_API_DIR="$TMP/api" FAKE_ASSET_DIR="$TMP/assets" \
  "$SCRIPT_DIR/aggregate-release.sh" fetch "$TMP/resolved.json" "$TMP/fetched"
[ "$(find "$TMP/fetched/install/bin" -mindepth 1 -maxdepth 1 -type f | wc -l)" -eq 17 ] \
  || fail "fetcher did not install the complete component file set"
while read -r repository file; do
  case "$repository" in ''|\#*) continue ;; esac
  [ -f "$TMP/fetched/install/bin/$file" ] \
    || fail "fetcher did not install required prebuilt file: $file"
done < "$SCRIPT_DIR/bin-inputs.manifest"

env \
  RELEASE_WORKFLOW_REPOSITORY=kuasar-sandbox/orchestrator \
  RELEASE_WORKFLOW_RUN_ID=456 \
  RELEASE_WORKFLOW_RUN_ATTEMPT=2 \
  RELEASE_WORKFLOW_RUN_URL=https://example.invalid/actions/runs/456 \
  "$SCRIPT_DIR/aggregate-release.sh" prepare \
    "$TMP/resolved.json" "$TMP/fetched" "$TMP/bundle"
"$SCRIPT_DIR/aggregate-release.sh" validate-bundle "$TMP/bundle"

[ ! -e "$TMP/bundle/assets/guest-runtime-v4.0.0-linux-x86_64.tar.gz" ] \
  || fail "aggregate unexpectedly contains a generic guest-runtime archive"
[ -f "$TMP/bundle/assets/sandbox-runtime-x86_64-runtime-v2.0.0.tar.gz" ] \
  || fail "aggregate is missing the independently versioned runtime archive"
[ -f "$TMP/bundle/assets/vmlinux-x86_64-vmlinux-v3.0.0.tar.gz" ] \
  || fail "aggregate is missing the independently versioned vmlinux archive"

cp -a "$TMP/bundle" "$TMP/tampered"
printf 'tampered\n' >> "$TMP/tampered/assets/accelerator-v1.0.0-linux-x86_64.tar.gz"
if "$SCRIPT_DIR/aggregate-release.sh" validate-bundle "$TMP/tampered" >/dev/null 2>&1; then
  fail "aggregate validator accepted a tampered component archive"
fi

jq 'del(.components.runtime.manifest.sources[] | select(.repository == "kuasar-sandbox/sandboxer"))' \
  "$TMP/resolved.json" > "$TMP/no-sandboxer.json"
if "$SCRIPT_DIR/aggregate-release.sh" validate-resolved "$TMP/no-sandboxer.json" >/dev/null 2>&1; then
  fail "resolved validator accepted runtime without sandboxer provenance"
fi

cat > "$TMP/fake-bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

state="${FAKE_GH_STATE:?}"
mkdir -p "$state"

not_found() {
  echo 'gh: Not Found (HTTP 404)' >&2
  exit 1
}

emit() {
  local json="$1" filter="$2"
  if [ -n "$filter" ]; then
    jq -r "$filter" <<< "$json"
  else
    printf '%s\n' "$json"
  fi
}

[ "${1:-}" = api ] || { echo "unsupported fake gh command" >&2; exit 2; }
shift
method=GET
input=
filter=
raw=false
endpoint=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --method) method="$2"; shift 2 ;;
    --input) input="$2"; shift 2 ;;
    --jq) filter="$2"; shift 2 ;;
    -H)
      [[ "$2" != *github.raw* ]] || raw=true
      shift 2
      ;;
    *) endpoint="$1"; shift ;;
  esac
done
request="$state/request.json"
if [ "$input" = - ]; then
  cat > "$request"
elif [ -n "$input" ]; then
  cp "$input" "$request"
fi

case "$method $endpoint" in
  "GET repos/kuasar-sandbox/orchestrator/git/ref/heads/release")
    [ -f "$state/branch" ] || not_found
    emit "$(jq -cn --arg sha "$(cat "$state/branch")" '{object: {sha: $sha}}')" "$filter"
    ;;
  GET\ repos/kuasar-sandbox/orchestrator/git/commits/*)
    emit '{"tree":{"sha":"1111111111111111111111111111111111111111"}}' "$filter"
    ;;
  GET\ repos/kuasar-sandbox/orchestrator/contents/releases/release-v4.0.0-preview.20260804.json\?ref=*)
    [ -f "$state/mapping" ] || not_found
    if [ "$raw" = true ]; then
      cat "$state/mapping"
    else
      emit '{"sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}' "$filter"
    fi
    ;;
  "GET repos/kuasar-sandbox/orchestrator/commits?sha=release&path=releases/release-v4.0.0-preview.20260804.json&per_page=1")
    emit "$(jq -cn --arg sha "$(cat "$state/branch")" '[{sha: $sha}]')" "$filter"
    ;;
  "POST repos/kuasar-sandbox/orchestrator/git/trees")
    jq -j '.tree[0].content' "$request" > "$state/mapping"
    emit '{"sha":"1111111111111111111111111111111111111111"}' "$filter"
    ;;
  "POST repos/kuasar-sandbox/orchestrator/git/commits")
    emit '{"sha":"2222222222222222222222222222222222222222"}' "$filter"
    ;;
  "POST repos/kuasar-sandbox/orchestrator/git/refs")
    jq -er '.sha' "$request" > "$state/branch"
    emit '{"ref":"refs/heads/release"}' "$filter"
    ;;
  *)
    echo "fake gh: unsupported API call: $method $endpoint" >&2
    exit 2
    ;;
esac
EOF
chmod +x "$TMP/fake-bin/gh"

map_state="$TMP/map-state"
first_commit="$(env PATH="$TMP/fake-bin:$PATH" FAKE_GH_STATE="$map_state" \
  "$SCRIPT_DIR/release-map.sh" put "$TMP/mapping.json")"
[ "$first_commit" = 2222222222222222222222222222222222222222 ] \
  || fail "first mapping did not create the expected orphan commit"
second_commit="$(env PATH="$TMP/fake-bin:$PATH" FAKE_GH_STATE="$map_state" \
  "$SCRIPT_DIR/release-map.sh" put "$TMP/mapping.json")"
[ "$second_commit" = "$first_commit" ] || fail "identical mapping was not idempotent"

jq '.components.accelerator = "v9.9.9"' "$TMP/mapping.json" > "$TMP/conflicting-mapping.json"
if env PATH="$TMP/fake-bin:$PATH" FAKE_GH_STATE="$map_state" \
  "$SCRIPT_DIR/release-map.sh" put "$TMP/conflicting-mapping.json" >/dev/null 2>&1; then
  fail "release mapping writer accepted a conflicting existing version"
fi

cat > "$TMP/fake-bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

state="${FAKE_GH_STATE:?}"
mkdir -p "$state"

not_found() {
  echo 'gh: Not Found (HTTP 404)' >&2
  exit 1
}

render_release() {
  local assets='[]'
  if [ -s "$state/assets.ndjson" ]; then
    assets="$(jq -s '.' "$state/assets.ndjson")"
  fi
  local prerelease
  prerelease="$(cat "$state/release-prerelease" 2>/dev/null || printf false)"
  jq -cn \
    --argjson draft "$(cat "$state/release-draft")" \
    --argjson prerelease "$prerelease" \
    --argjson assets "$assets" \
    '{id: 77, tag_name: "release-v4.0.0-preview.20260804", draft: $draft,
      prerelease: $prerelease, assets: $assets}'
}

emit() {
  local json="$1" filter="$2"
  if [ -n "$filter" ]; then jq -r "$filter" <<< "$json"; else printf '%s\n' "$json"; fi
}

if [ "${1:-}" = api ]; then
  shift
  method=GET
  input=
  filter=
  raw=false
  slurp=false
  endpoint=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --method) method="$2"; shift 2 ;;
      --input) input="$2"; shift 2 ;;
      --jq) filter="$2"; shift 2 ;;
      --paginate) shift ;;
      --slurp) slurp=true; shift ;;
      -H)
        [[ "$2" != *github.raw* ]] || raw=true
        shift 2
        ;;
      *) endpoint="$1"; shift ;;
    esac
  done
  request="$state/request.json"
  if [ "$input" = - ]; then cat > "$request"; elif [ -n "$input" ]; then cp "$input" "$request"; fi
  case "$method $endpoint" in
    "GET repos/kuasar-sandbox/orchestrator/git/ref/heads/release")
      emit '{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}' "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/compare/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      emit '{"status":"identical"}' "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/contents/releases/release-v4.0.0-preview.20260804.json?ref=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      [ "$raw" = true ] || exit 2
      cat "$state/mapping"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/git/ref/tags/release-v4.0.0-preview.20260804")
      [ -f "$state/tag" ] || not_found
      emit "$(jq -cn --arg sha "$(cat "$state/tag")" '{object: {sha: $sha}}')" "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/releases/tags/release-v4.0.0-preview.20260804")
      [ -f "$state/release-draft" ] || not_found
      [ "$(cat "$state/release-draft")" = false ] || not_found
      emit "$(render_release)" "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/releases?per_page=100")
      if [ -f "$state/release-draft" ] && [ "$(cat "$state/release-draft")" = true ]; then
        delay="$(cat "$state/visibility-delay" 2>/dev/null || printf 0)"
        if [ "$delay" -gt 0 ]; then
          printf '%s\n' "$((delay - 1))" > "$state/visibility-delay"
          json='[]'
        else
          json="[$(render_release)]"
        fi
      else
        json='[]'
      fi
      [ "$slurp" = false ] || json="[$json]"
      emit "$json" "$filter"
      ;;
    "POST repos/kuasar-sandbox/orchestrator/git/refs")
      [ "$(jq -er '.ref' "$request")" = refs/tags/release-v4.0.0-preview.20260804 ] || exit 2
      jq -er '.sha' "$request" > "$state/tag"
      emit '{"ref":"refs/tags/release-v4.0.0-preview.20260804"}' "$filter"
      ;;
    "DELETE repos/kuasar-sandbox/orchestrator/releases/77")
      rm -f "$state/release-draft" "$state/release-prerelease" \
        "$state/assets.ndjson" "$state/visibility-delay"
      printf 'true\n' > "$state/deleted-draft"
      ;;
    "PATCH repos/kuasar-sandbox/orchestrator/releases/77")
      [ "$(jq -er '.draft' "$request")" = false ] || exit 2
      [ "$(jq -er '.prerelease' "$request")" = true ] || exit 2
      [ "$(jq -er '.make_latest' "$request")" = false ] || exit 2
      printf 'true\n' > "$state/release-prerelease"
      printf 'false\n' > "$state/release-draft"
      emit "$(render_release)" "$filter"
      ;;
    *)
      echo "fake gh: unsupported API call: $method $endpoint" >&2
      exit 2
      ;;
  esac
  exit 0
fi

if [ "${1:-}" = release ]; then
  action="${2:-}"
  tag="${3:-}"
  [ "$tag" = release-v4.0.0-preview.20260804 ] || exit 2
  case "$action" in
    create)
      [ -f "$state/tag" ] || exit 2
      printf 'true\n' > "$state/release-draft"
      printf 'false\n' > "$state/release-prerelease"
      : > "$state/assets.ndjson"
      shift 3
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --repo|--target|--title|--notes-file) shift 2 ;;
          --draft|--verify-tag) shift ;;
          *)
            file="$1"
            jq -cn \
              --arg name "$(basename "$file")" \
              --arg digest "sha256:$(sha256sum "$file" | awk '{print $1}')" \
              --argjson size "$(stat -c '%s' "$file")" \
              '{name: $name, digest: $digest, size: $size, state: "uploaded"}' \
              >> "$state/assets.ndjson"
            shift
            ;;
        esac
      done
      if [ "${FAKE_GH_FAIL_CREATE_ONCE:-0}" = 1 ] && [ ! -f "$state/failed-once" ]; then
        touch "$state/failed-once"
        exit 42
      fi
      printf '1\n' > "$state/visibility-delay"
      ;;
    *) exit 2 ;;
  esac
  exit 0
fi

echo "fake gh: unsupported command" >&2
exit 2
EOF
chmod +x "$TMP/fake-bin/gh"

publish_state="$TMP/publish-state"
mkdir -p "$publish_state"
cp "$TMP/mapping.json" "$publish_state/mapping"
if env PATH="$TMP/fake-bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  FAKE_GH_STATE="$publish_state" FAKE_GH_FAIL_CREATE_ONCE=1 \
  "$SCRIPT_DIR/publish-release.sh" publish "$TMP/bundle" >/dev/null 2>&1; then
  fail "publish fixture did not stop after the interrupted draft upload"
fi
[ "$(cat "$publish_state/release-draft")" = true ] \
  || fail "interrupted aggregate release was not left as a draft"
env PATH="$TMP/fake-bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  FAKE_GH_STATE="$publish_state" \
  "$SCRIPT_DIR/publish-release.sh" publish "$TMP/bundle"
[ "$(cat "$publish_state/release-draft")" = false ] \
  || fail "aggregate draft was not published after retry"
[ -f "$publish_state/deleted-draft" ] \
  || fail "aggregate retry did not delete the stale draft by release ID"
[ "$(cat "$publish_state/release-prerelease")" = true ] \
  || fail "aggregate preview was not published as a prerelease"
[ "$(cat "$publish_state/tag")" = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ] \
  || fail "aggregate tag does not point to its release mapping commit"

echo "test-release-tools: PASS"
