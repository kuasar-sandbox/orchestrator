#!/usr/bin/env bash

set -euo pipefail

[ "$#" -eq 4 ] || {
  echo "usage: test-publisher.sh <publisher> <bundle> <repository> <tag>" >&2
  exit 2
}
PUBLISHER="$1"
BUNDLE="$2"
REPOSITORY="$3"
TAG="$4"
COMMIT="$(jq -er '.commit // .mapping.commit' "$BUNDLE/release.json")"
EXPECTED_PRERELEASE=false
EXPECTED_LATEST=true
if [[ "$TAG" = *-preview.* ]]; then
  EXPECTED_PRERELEASE=true
  EXPECTED_LATEST=false
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/state"

cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

state="${FAKE_GH_STATE:?}"
repository="${FAKE_GH_REPOSITORY:?}"
tag="${FAKE_GH_TAG:?}"
commit="${FAKE_GH_COMMIT:?}"

not_found() {
  echo 'gh: Not Found (HTTP 404)' >&2
  exit 1
}

release_json() {
  local assets='[]'
  [ ! -s "$state/assets.ndjson" ] || assets="$(jq -s '.' "$state/assets.ndjson")"
  local draft prerelease
  draft="$(cat "$state/release-draft")"
  prerelease="$(cat "$state/release-prerelease" 2>/dev/null || printf false)"
  jq -cn --arg tag "$tag" --arg commit "$commit" --argjson draft "$draft"     --argjson prerelease "$prerelease" --argjson assets "$assets"     '{id: 77, tag_name: $tag, target_commitish: $commit, draft: $draft,
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
  slurp=false
  endpoint=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --method) method="$2"; shift 2 ;;
      --input) input="$2"; shift 2 ;;
      --jq) filter="$2"; shift 2 ;;
      --paginate) shift ;;
      --slurp) slurp=true; shift ;;
      -H) shift 2 ;;
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
    "GET repos/$repository/git/ref/tags/$tag")
      [ -f "$state/tag" ] || not_found
      emit "$(jq -cn --arg sha "$(cat "$state/tag")" '{object: {sha: $sha}}')" "$filter"
      ;;
    "POST repos/$repository/git/refs")
      [ "$(jq -er '.ref' "$request")" = "refs/tags/$tag" ] || exit 2
      jq -er '.sha' "$request" > "$state/tag"
      emit '{"ref":"created"}' "$filter"
      ;;
    "GET repos/$repository/releases/tags/$tag")
      [ -f "$state/release-draft" ] || not_found
      [ "$(cat "$state/release-draft")" = false ] || not_found
      emit "$(release_json)" "$filter"
      ;;
    "GET repos/$repository/releases?per_page=100")
      if [ -f "$state/release-draft" ] && [ "$(cat "$state/release-draft")" = true ]; then
        delay="$(cat "$state/visibility-delay" 2>/dev/null || printf 0)"
        if [ "$delay" -gt 0 ]; then
          printf '%s\n' "$((delay - 1))" > "$state/visibility-delay"
          json='[]'
        else
          json="[$(release_json)]"
        fi
      else
        json='[]'
      fi
      [ "$slurp" = false ] || json="[$json]"
      emit "$json" "$filter"
      ;;
    "DELETE repos/$repository/releases/77")
      rm -f "$state/release-draft" "$state/release-prerelease" \
        "$state/assets.ndjson" "$state/visibility-delay"
      count="$(cat "$state/delete-count" 2>/dev/null || printf 0)"
      printf '%s\n' "$((count + 1))" > "$state/delete-count"
      ;;
    "PATCH repos/$repository/releases/77")
      [ "$(jq -er '.draft' "$request")" = false ] || exit 2
      jq -r '.prerelease' "$request" > "$state/release-prerelease"
      jq -er '.make_latest' "$request" > "$state/make-latest"
      printf 'false\n' > "$state/release-draft"
      emit "$(release_json)" "$filter"
      ;;
    *)
      echo "fake gh: unsupported API call: $method $endpoint" >&2
      exit 2
      ;;
  esac
  exit 0
fi

if [ "${1:-}" = release ] && [ "${2:-}" = create ]; then
  [ "${3:-}" = "$tag" ] || exit 2
  [ -f "$state/tag" ] || exit 2
  shift 3
  : > "$state/assets.ndjson"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --repo|--target|--title|--notes-file) shift 2 ;;
      --draft|--verify-tag) shift ;;
      *)
        file="$1"
        jq -cn --arg name "$(basename "$file")"           --arg digest "sha256:$(sha256sum "$file" | awk '{print $1}')"           --argjson size "$(stat -c '%s' "$file")"           '{name: $name, digest: $digest, size: $size, state: "uploaded"}'           >> "$state/assets.ndjson"
        shift
        ;;
    esac
  done
  printf 'true\n' > "$state/release-draft"
  printf 'false\n' > "$state/release-prerelease"
  if [ "${FAKE_GH_FAIL_CREATE_ONCE:-0}" = 1 ] && [ ! -f "$state/failed-once" ]; then
    touch "$state/failed-once"
    exit 42
  fi
  printf '1\n' > "$state/visibility-delay"
  exit 0
fi

echo "fake gh: unsupported command" >&2
exit 2
EOF
chmod +x "$TMP/bin/gh"

common_env=(
  PATH="$TMP/bin:$PATH"
  GH_REPO="$REPOSITORY"
  FAKE_GH_STATE="$TMP/state"
  FAKE_GH_REPOSITORY="$REPOSITORY"
  FAKE_GH_TAG="$TAG"
  FAKE_GH_COMMIT="$COMMIT"
)

env "${common_env[@]}" "$PUBLISHER" check "$TAG"
if env "${common_env[@]}" FAKE_GH_FAIL_CREATE_ONCE=1   "$PUBLISHER" publish "$BUNDLE" >/dev/null 2>&1; then
  echo "test-publisher: interrupted draft creation unexpectedly succeeded" >&2
  exit 1
fi
[ "$(cat "$TMP/state/release-draft")" = true ] || {
  echo "test-publisher: interrupted publish did not leave a draft" >&2
  exit 1
}
env "${common_env[@]}" "$PUBLISHER" publish "$BUNDLE"
[ "$(cat "$TMP/state/delete-count")" = 1 ] || {
  echo "test-publisher: retry did not replace the stale draft by release ID" >&2
  exit 1
}
[ "$(cat "$TMP/state/tag")" = "$COMMIT" ] || {
  echo "test-publisher: tag does not point to the bundle commit" >&2
  exit 1
}
[ "$(cat "$TMP/state/release-draft")" = false ] || {
  echo "test-publisher: release remains a draft" >&2
  exit 1
}
[ "$(cat "$TMP/state/release-prerelease")" = "$EXPECTED_PRERELEASE" ] || {
  echo "test-publisher: release has the wrong prerelease state" >&2
  exit 1
}
[ "$(cat "$TMP/state/make-latest")" = "$EXPECTED_LATEST" ] || {
  echo "test-publisher: release has the wrong latest policy" >&2
  exit 1
}
if env "${common_env[@]}" "$PUBLISHER" check "$TAG" >/dev/null 2>&1; then
  echo "test-publisher: preflight accepted an already published release" >&2
  exit 1
fi

echo "test-publisher: PASS"
