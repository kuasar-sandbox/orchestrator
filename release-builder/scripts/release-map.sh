#!/usr/bin/env bash

set -euo pipefail

REPOSITORY="${GH_REPO:-kuasar-sandbox/orchestrator}"
RELEASE_BRANCH=release
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() {
  echo "release-map: $*" >&2
  exit 1
}

api_optional() {
  local endpoint="$1" output="$2"
  if gh api "$endpoint" > "$output" 2> "$TMP/api-error"; then
    return 0
  fi
  if grep -q '(HTTP 404)' "$TMP/api-error"; then
    : > "$output"
    return 4
  fi
  cat "$TMP/api-error" >&2
  return 1
}

read_remote_mapping() {
  local ref="$1" path="$2" output="$3"
  gh api -H 'Accept: application/vnd.github.raw+json' \
    "repos/$REPOSITORY/contents/$path?ref=$ref" > "$output"
}

wait_for_release_ref() {
  local expected="$1" observed="" attempt error="$TMP/ref-error"
  for attempt in $(seq 1 10); do
    : > "$error"
    if observed="$(gh api "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" \
      --jq '.object.sha' 2> "$error")" && [ "$observed" = "$expected" ]; then
      return 0
    fi
    [ "$attempt" -eq 10 ] || sleep 1
  done
  [ ! -s "$error" ] || cat "$error" >&2
  fail "$RELEASE_BRANCH did not expose the new mapping commit: expected $expected, observed ${observed:-unavailable}"
}

put_mapping() {
  [ "$#" -eq 1 ] || fail "usage: release-map.sh put <mapping.json>"
  local mapping="$1"
  "$SCRIPT_DIR/aggregate-release.sh" validate-map "$mapping"
  local canonical="$TMP/mapping.json"
  jq -S . "$mapping" > "$canonical"
  local version path branch_state parent_commit="" parent_tree="" remote existing_commit
  version="$(jq -er '.version' "$canonical")"
  path="releases/$version.json"
  branch_state="$TMP/branch.json"

  if api_optional "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" "$branch_state"; then
    parent_commit="$(jq -er '.object.sha' "$branch_state")"
    parent_tree="$(gh api "repos/$REPOSITORY/git/commits/$parent_commit" --jq '.tree.sha')"
    remote="$TMP/remote-mapping.json"
    if api_optional "repos/$REPOSITORY/contents/$path?ref=$parent_commit" "$TMP/content-state.json"; then
      read_remote_mapping "$parent_commit" "$path" "$remote"
      jq -S . "$remote" > "$TMP/remote-canonical.json"
      cmp -s "$canonical" "$TMP/remote-canonical.json" \
        || fail "$path already exists with different component versions"
      existing_commit="$(gh api "repos/$REPOSITORY/commits?sha=$RELEASE_BRANCH&path=$path&per_page=1" --jq '.[0].sha')"
      [[ "$existing_commit" =~ ^[0-9a-f]{40}$ ]] || fail "cannot resolve existing mapping commit"
      printf '%s\n' "$existing_commit"
      return 0
    else
      local rc=$?
      [ "$rc" -eq 4 ] || exit "$rc"
    fi
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi

  local tree_payload="$TMP/tree.json" tree_sha commit_payload="$TMP/commit.json" commit_sha
  if [ -n "$parent_tree" ]; then
    jq -n --arg base_tree "$parent_tree" --arg path "$path" --rawfile content "$canonical" '
      {
        base_tree: $base_tree,
        tree: [{path: $path, mode: "100644", type: "blob", content: $content}]
      }
    ' > "$tree_payload"
  else
    jq -n --arg path "$path" --rawfile content "$canonical" '
      {tree: [{path: $path, mode: "100644", type: "blob", content: $content}]}
    ' > "$tree_payload"
  fi
  tree_sha="$(gh api --method POST "repos/$REPOSITORY/git/trees" --input "$tree_payload" --jq '.sha')"
  [[ "$tree_sha" =~ ^[0-9a-f]{40}$ ]] || fail "GitHub returned an invalid tree SHA"

  if [ -n "$parent_commit" ]; then
    jq -n --arg message "Define $version" --arg tree "$tree_sha" --arg parent "$parent_commit" \
      '{message: $message, tree: $tree, parents: [$parent]}' > "$commit_payload"
  else
    jq -n --arg message "Define $version" --arg tree "$tree_sha" \
      '{message: $message, tree: $tree, parents: []}' > "$commit_payload"
  fi
  commit_sha="$(gh api --method POST "repos/$REPOSITORY/git/commits" \
    --input "$commit_payload" --jq '.sha')"
  [[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || fail "GitHub returned an invalid commit SHA"

  if [ -n "$parent_commit" ]; then
    jq -n --arg sha "$commit_sha" '{sha: $sha, force: false}' \
      | gh api --method PATCH "repos/$REPOSITORY/git/refs/heads/$RELEASE_BRANCH" --input - >/dev/null
  else
    jq -n --arg ref "refs/heads/$RELEASE_BRANCH" --arg sha "$commit_sha" \
      '{ref: $ref, sha: $sha}' \
      | gh api --method POST "repos/$REPOSITORY/git/refs" --input - >/dev/null
  fi

  wait_for_release_ref "$commit_sha"
  read_remote_mapping "$commit_sha" "$path" "$TMP/published-mapping.json"
  jq -S . "$TMP/published-mapping.json" > "$TMP/published-canonical.json"
  cmp -s "$canonical" "$TMP/published-canonical.json" \
    || fail "published mapping bytes failed verification"
  printf '%s\n' "$commit_sha"
}

[[ "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "invalid repository"
for command in gh jq; do
  command -v "$command" >/dev/null || fail "$command is required"
done
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "${1:-}" in
  put) shift; put_mapping "$@" ;;
  *) fail "usage: release-map.sh put <mapping.json>" ;;
esac
