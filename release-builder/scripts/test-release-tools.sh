#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

fail() {
  echo "test-release-tools: $*" >&2
  exit 1
}

VERSION=v1.2.3
ARCH=x86_64
REVISIONS="$TMP/revisions.tsv"
DIST="$TMP/dist"
BUNDLE="$TMP/bundle"
mkdir -p "$DIST"

declare -A revisions=(
  [accelerator]=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  [connector]=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  [guest-runtime]=cccccccccccccccccccccccccccccccccccccccc
  [orchestrator]=dddddddddddddddddddddddddddddddddddddddd
  [sandboxer]=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
)

printf 'repository\trequested_ref\tresolved_sha\trole\n' > "$REVISIONS"
for repo in accelerator connector guest-runtime orchestrator sandboxer; do
  printf 'kuasar-sandbox/%s\tmain\t%s\trelease\n' "$repo" "${revisions[$repo]}" \
    >> "$REVISIONS"
done

archives=(
  "orchestrator-$VERSION-linux-$ARCH.tar.gz"
  "accelerator-$VERSION-linux-$ARCH.tar.gz"
  "guest-runtime-$VERSION-linux-$ARCH.tar.gz"
  "sandboxer-$VERSION-linux-$ARCH.tar.gz"
  "connector-$VERSION-linux-$ARCH.tar.gz"
  "sandbox-runtime-$ARCH-$VERSION.tar.gz"
  "vmlinux-$ARCH-$VERSION.tar.gz"
)
metadata_names=(orchestrator accelerator guest-runtime sandboxer connector sandbox-runtime vmlinux)
metadata_repos=(orchestrator accelerator guest-runtime sandboxer connector guest-runtime guest-runtime)

for index in "${!archives[@]}"; do
  archive="${archives[$index]}"
  name="${metadata_names[$index]}"
  repo="${metadata_repos[$index]}"
  stage="$TMP/stage-$name"
  mkdir -p "$stage/release"
  jq -n \
    --arg name "$name" \
    --arg version "$VERSION" \
    --arg arch "$ARCH" \
    --arg archive "$archive" \
    --arg commit "${revisions[$repo]}" '
      {
        name: $name,
        version: $version,
        arch: $arch,
        archive: $archive,
        commit: $commit,
        built: "2026-08-04T00:00:00Z"
      }
    ' > "$stage/release/$name.json"
  tar -czf "$DIST/$archive" -C "$stage" .
done
(cd "$DIST" && sha256sum "${archives[@]}" > SHA256SUMS)

env \
  RELEASE_WORKFLOW_REPOSITORY=kuasar-sandbox/orchestrator \
  RELEASE_WORKFLOW_RUN_ID=123456 \
  RELEASE_WORKFLOW_RUN_ATTEMPT=2 \
  RELEASE_WORKFLOW_RUN_URL=https://github.com/kuasar-sandbox/orchestrator/actions/runs/123456 \
  "$SCRIPT_DIR/prepare-release.sh" "$VERSION" "$ARCH" "$REVISIONS" "$DIST" "$BUNDLE"

jq -e \
  --arg version "$VERSION" \
  --arg orchestrator "${revisions[orchestrator]}" '
    .schemaVersion == 1
    and .version == $version
    and .tag == ("release-" + $version)
    and .repositories.orchestrator.commit == $orchestrator
    and .validation.workflowRunId == 123456
    and .validation.workflowRunAttempt == 2
    and (.artifacts | length == 8)
  ' "$BUNDLE/release.json" >/dev/null \
  || fail "prepared release.json is malformed"
[ "$(find "$BUNDLE/assets" -mindepth 1 -maxdepth 1 -type f | wc -l)" -eq 8 ] \
  || fail "prepared bundle must contain eight release assets"
grep -q 'full BMS E2E suite passed' "$BUNDLE/release-notes.md" \
  || fail "release notes do not describe validation"

mkdir -p "$TMP/bin"
cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" != api ]; then
  echo "fake gh only supports api" >&2
  exit 2
fi
endpoint="${*: -1}"
if [ "${FAKE_GH_EXISTING_TAG:-0}" = 1 ] \
  && [ "$endpoint" = repos/kuasar-sandbox/orchestrator/git/ref/tags/release-v1.2.3 ]; then
  printf '{"object":{"sha":"ffffffffffffffffffffffffffffffffffffffff"}}\n'
  exit 0
fi
echo 'gh: Not Found (HTTP 404)' >&2
exit 1
EOF
chmod +x "$TMP/bin/gh"

env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  "$SCRIPT_DIR/publish-release.sh" check "$VERSION"
if env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator FAKE_GH_EXISTING_TAG=1 \
  "$SCRIPT_DIR/publish-release.sh" check "$VERSION" >/dev/null 2>&1; then
  fail "preflight accepted an existing release tag"
fi

env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  "$SCRIPT_DIR/publish-release.sh" validate "$BUNDLE"
cp -a "$BUNDLE" "$TMP/tampered-bundle"
printf 'tampered\n' >> "$TMP/tampered-bundle/assets/${archives[0]}"
if env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  "$SCRIPT_DIR/publish-release.sh" validate "$TMP/tampered-bundle" >/dev/null 2>&1; then
  fail "bundle validation accepted a tampered asset"
fi

cp -a "$DIST" "$TMP/extra-dist"
printf 'unexpected\n' > "$TMP/extra-dist/unexpected.txt"
if env \
  RELEASE_WORKFLOW_REPOSITORY=kuasar-sandbox/orchestrator \
  RELEASE_WORKFLOW_RUN_ID=123456 \
  RELEASE_WORKFLOW_RUN_ATTEMPT=2 \
  RELEASE_WORKFLOW_RUN_URL=https://github.com/kuasar-sandbox/orchestrator/actions/runs/123456 \
  "$SCRIPT_DIR/prepare-release.sh" "$VERSION" "$ARCH" "$REVISIONS" \
    "$TMP/extra-dist" "$TMP/extra-bundle" >/dev/null 2>&1; then
  fail "release preparation accepted an unexpected dist file"
fi

cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

STATE="${FAKE_GH_STATE:?}"
mkdir -p "$STATE"

not_found() {
  echo 'gh: Not Found (HTTP 404)' >&2
  exit 1
}

emit_json() {
  local json="$1"
  local filter="${2:-}"
  if [ -n "$filter" ]; then
    jq -r "$filter" <<< "$json"
  else
    printf '%s\n' "$json"
  fi
}

render_release() {
  local draft assets
  draft="$(cat "$STATE/release-draft")"
  assets='[]'
  if [ -s "$STATE/assets.ndjson" ]; then
    assets="$(jq -s '.' "$STATE/assets.ndjson")"
  fi
  jq -cn \
    --argjson draft "$draft" \
    --argjson assets "$assets" '
      {
        id: 77,
        tag_name: "release-v1.2.3",
        draft: $draft,
        prerelease: false,
        assets: $assets
      }
    '
}

if [ "${1:-}" = api ]; then
  shift
  method=GET
  input=
  filter=
  endpoint=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --method) method="$2"; shift 2 ;;
      --input) input="$2"; shift 2 ;;
      --jq) filter="$2"; shift 2 ;;
      -H) shift 2 ;;
      *) endpoint="$1"; shift ;;
    esac
  done
  request="$STATE/request.json"
  if [ "$input" = - ]; then
    cat > "$request"
  elif [ -n "$input" ]; then
    cp "$input" "$request"
  fi

  case "$method $endpoint" in
    "GET repos/kuasar-sandbox/orchestrator/git/ref/heads/release")
      [ -f "$STATE/branch" ] || not_found
      emit_json "$(jq -cn --arg sha "$(cat "$STATE/branch")" '{object: {sha: $sha}}')" "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/git/ref/tags/release-v1.2.3")
      [ -f "$STATE/tag" ] || not_found
      emit_json "$(jq -cn --arg sha "$(cat "$STATE/tag")" '{object: {sha: $sha}}')" "$filter"
      ;;
    "GET repos/kuasar-sandbox/orchestrator/releases/tags/release-v1.2.3")
      [ -f "$STATE/release-draft" ] || not_found
      emit_json "$(render_release)" "$filter"
      ;;
    GET\ repos/kuasar-sandbox/orchestrator/contents/release.json\?ref=*)
      [ -f "$STATE/pending-manifest" ] || not_found
      cat "$STATE/pending-manifest"
      ;;
    GET\ repos/kuasar-sandbox/orchestrator/git/commits/*)
      [ -f "$STATE/commit-request.json" ] || not_found
      jq -c --arg sha "${endpoint##*/}" '{sha: $sha, parents: .parents}' \
        "$STATE/commit-request.json"
      ;;
    "POST repos/kuasar-sandbox/orchestrator/git/trees")
      jq -j '.tree[0].content' "$request" > "$STATE/pending-manifest"
      emit_json '{"sha":"1111111111111111111111111111111111111111"}' "$filter"
      ;;
    "POST repos/kuasar-sandbox/orchestrator/git/commits")
      cp "$request" "$STATE/commit-request.json"
      emit_json '{"sha":"2222222222222222222222222222222222222222"}' "$filter"
      ;;
    "POST repos/kuasar-sandbox/orchestrator/git/refs")
      ref="$(jq -er '.ref' "$request")"
      sha="$(jq -er '.sha' "$request")"
      case "$ref" in
        refs/heads/release)
          printf '%s\n' "$sha" > "$STATE/branch"
          printf 'branch\n' >> "$STATE/events"
          ;;
        refs/tags/release-v1.2.3)
          printf '%s\n' "$sha" > "$STATE/tag"
          printf 'tag\n' >> "$STATE/events"
          ;;
        *) exit 2 ;;
      esac
      emit_json "$(jq -cn --arg ref "$ref" --arg sha "$sha" '{ref: $ref, object: {sha: $sha}}')" "$filter"
      ;;
    "PATCH repos/kuasar-sandbox/orchestrator/git/refs/heads/release")
      sha="$(jq -er '.sha' "$request")"
      [ "$(jq -er '.force' "$request")" = false ]
      printf '%s\n' "$sha" > "$STATE/branch"
      printf 'branch\n' >> "$STATE/events"
      emit_json "$(jq -cn --arg sha "$sha" '{object: {sha: $sha}}')" "$filter"
      ;;
    "PATCH repos/kuasar-sandbox/orchestrator/releases/77")
      [ "$(jq -er '.draft' "$request")" = false ]
      printf 'false\n' > "$STATE/release-draft"
      printf 'publish\n' >> "$STATE/events"
      emit_json "$(render_release)" "$filter"
      ;;
    *)
      echo "fake gh: unsupported api call: $method $endpoint" >&2
      exit 2
      ;;
  esac
  exit 0
fi

if [ "${1:-}" = release ]; then
  subcommand="${2:-}"
  tag="${3:-}"
  [ "$tag" = release-v1.2.3 ] || exit 2
  case "$subcommand" in
    create)
      [ -f "$STATE/tag" ] || exit 2
      printf 'true\n' > "$STATE/release-draft"
      : > "$STATE/assets.ndjson"
      printf 'draft\n' >> "$STATE/events"
      ;;
    edit)
      [ -f "$STATE/release-draft" ] || exit 2
      printf 'edit\n' >> "$STATE/events"
      ;;
    upload)
      shift 3
      : > "$STATE/assets.ndjson"
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --repo) shift 2 ;;
          --clobber) shift ;;
          *)
            file="$1"
            jq -cn \
              --arg name "$(basename "$file")" \
              --arg digest "sha256:$(sha256sum "$file" | awk '{print $1}')" \
              --argjson size "$(stat -c '%s' "$file")" '
                {name: $name, digest: $digest, size: $size, state: "uploaded"}
              ' >> "$STATE/assets.ndjson"
            shift
            ;;
        esac
      done
      printf 'upload\n' >> "$STATE/events"
      [ "${FAKE_GH_FAIL_AFTER_UPLOAD:-0}" != 1 ] || exit 42
      ;;
    *) exit 2 ;;
  esac
  exit 0
fi

echo "fake gh: unsupported command: $*" >&2
exit 2
EOF
chmod +x "$TMP/bin/gh"

publish_state="$TMP/publish-state"
if env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  FAKE_GH_STATE="$publish_state" FAKE_GH_FAIL_AFTER_UPLOAD=1 \
  "$SCRIPT_DIR/publish-release.sh" publish "$BUNDLE" >/dev/null 2>&1; then
  fail "state-machine fixture did not stop after the first draft upload"
fi
[ ! -e "$publish_state/branch" ] \
  || fail "release branch advanced before draft assets were verified"

env PATH="$TMP/bin:$PATH" GH_REPO=kuasar-sandbox/orchestrator \
  FAKE_GH_STATE="$publish_state" \
  "$SCRIPT_DIR/publish-release.sh" publish "$BUNDLE"
[ "$(cat "$publish_state/branch")" = 2222222222222222222222222222222222222222 ] \
  || fail "release branch does not point to the prepared release commit"
[ "$(cat "$publish_state/tag")" = "$(cat "$publish_state/branch")" ] \
  || fail "release tag and branch diverged"
[ "$(cat "$publish_state/release-draft")" = false ] \
  || fail "draft release was not published"
[ "$(cat "$publish_state/events")" = $'tag\ndraft\nupload\nedit\nupload\nbranch\npublish' ] \
  || fail "publish state-machine order is incorrect"

echo "test-release-tools: PASS"
