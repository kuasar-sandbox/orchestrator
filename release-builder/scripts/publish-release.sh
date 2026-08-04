#!/usr/bin/env bash

set -euo pipefail

RELEASE_BRANCH=release
REPOSITORY="${GH_REPO:-${GITHUB_REPOSITORY:-}}"

fail() {
  echo "publish-release: $*" >&2
  exit 1
}

validate_version() {
  local version="$1"
  [[ "$version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] \
    || fail "version must match vX.Y.Z without leading zeroes"
}

api_optional() {
  local endpoint="$1"
  local response="$TMP/api-response"
  local error="$TMP/api-error"
  if gh api "$endpoint" > "$response" 2> "$error"; then
    cat "$response"
    return 0
  fi
  if grep -q '(HTTP 404)' "$error"; then
    return 4
  fi
  cat "$error" >&2
  return 1
}

read_release_manifest() {
  local commit="$1"
  local output="$2"
  gh api \
    -H 'Accept: application/vnd.github.raw+json' \
    "repos/$REPOSITORY/contents/release.json?ref=$commit" > "$output"
  jq -e '.schemaVersion == 1 and (.version | type == "string") and (.tag | type == "string")' \
    "$output" >/dev/null \
    || fail "$RELEASE_BRANCH commit $commit does not contain a valid release.json"
}

optional_state() {
  local endpoint="$1"
  local output="$2"
  if api_optional "$endpoint" > "$output"; then
    return 0
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    : > "$output"
    return 4
  fi
}

preflight() {
  local version="$1"
  local tag="release-$version"
  local state="$TMP/state"

  validate_version "$version"
  if optional_state "repos/$REPOSITORY/releases/tags/$tag" "$state"; then
    fail "GitHub release already exists: $tag"
  fi
  if optional_state "repos/$REPOSITORY/git/ref/tags/$tag" "$state"; then
    fail "Git tag already exists: $tag"
  fi
  if optional_state "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" "$state"; then
    local branch_sha branch_manifest branch_version
    branch_sha="$(jq -er '.object.sha' "$state")"
    branch_manifest="$TMP/branch-release.json"
    read_release_manifest "$branch_sha" "$branch_manifest"
    branch_version="$(jq -er '.version' "$branch_manifest")"
    [ "$branch_version" != "$version" ] \
      || fail "$RELEASE_BRANCH already records $version; resume the original publish job"
  fi
}

validate_bundle() {
  local bundle="$1"
  local manifest="$bundle/release.json"
  local notes="$bundle/release-notes.md"
  local assets="$bundle/assets"
  local version architecture expected_names actual_names

  [ -f "$manifest" ] || fail "release.json is missing from $bundle"
  [ -s "$notes" ] || fail "release-notes.md is missing or empty in $bundle"
  [ -d "$assets" ] || fail "assets directory is missing from $bundle"
  jq -e \
    --arg repository "$REPOSITORY" '
      .schemaVersion == 1
      and (.version | test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$"))
      and .tag == ("release-" + .version)
      and (.architecture == "x86_64" or .architecture == "aarch64")
      and (.repositories | keys == ["accelerator", "connector", "guest-runtime", "orchestrator", "sandboxer"])
      and .repositories.accelerator.repository == "kuasar-sandbox/accelerator"
      and .repositories.connector.repository == "kuasar-sandbox/connector"
      and .repositories["guest-runtime"].repository == "kuasar-sandbox/guest-runtime"
      and .repositories.orchestrator.repository == "kuasar-sandbox/orchestrator"
      and .repositories.sandboxer.repository == "kuasar-sandbox/sandboxer"
      and ([.repositories[].commit] | all(test("^[0-9a-f]{40}$")))
      and .validation.workflowRepository == $repository
      and (.validation.workflowRunId | type == "number" and . > 0)
      and (.validation.workflowRunAttempt | type == "number" and . > 0)
      and (.validation.workflowRunUrl | type == "string" and length > 0)
      and (.artifacts | length == 8)
      and ([.artifacts[].name] | unique | length == 8)
      and ([.artifacts[].sha256] | all(test("^[0-9a-f]{64}$")))
      and ([.artifacts[].size] | all(type == "number" and . >= 0))
    ' "$manifest" >/dev/null \
    || fail "release.json failed schema validation"

  version="$(jq -er '.version' "$manifest")"
  architecture="$(jq -er '.architecture' "$manifest")"
  expected_names="$TMP/expected-bundle-names"
  actual_names="$TMP/actual-bundle-names"
  printf '%s\n' \
    "accelerator-$version-linux-$architecture.tar.gz" \
    "connector-$version-linux-$architecture.tar.gz" \
    "guest-runtime-$version-linux-$architecture.tar.gz" \
    "orchestrator-$version-linux-$architecture.tar.gz" \
    "sandbox-runtime-$architecture-$version.tar.gz" \
    "sandboxer-$version-linux-$architecture.tar.gz" \
    SHA256SUMS \
    "vmlinux-$architecture-$version.tar.gz" \
    | LC_ALL=C sort > "$expected_names"
  jq -r '.artifacts[].name' "$manifest" | LC_ALL=C sort > "$actual_names"
  cmp -s "$expected_names" "$actual_names" \
    || fail "release.json contains an unexpected artifact set"

  local count=0 name expected_sha expected_size actual_sha actual_size
  while IFS=$'\t' read -r name expected_sha expected_size; do
    [[ "$name" =~ ^[A-Za-z0-9._-]+$ ]] || fail "unsafe artifact name: $name"
    [ -f "$assets/$name" ] || fail "artifact is missing: $name"
    actual_sha="$(sha256sum "$assets/$name" | awk '{print $1}')"
    actual_size="$(stat -c '%s' "$assets/$name")"
    [ "$actual_sha" = "$expected_sha" ] || fail "artifact checksum mismatch: $name"
    [ "$actual_size" = "$expected_size" ] || fail "artifact size mismatch: $name"
    count=$((count + 1))
  done < <(jq -r '.artifacts[] | [.name, .sha256, (.size | tostring)] | @tsv' "$manifest")
  [ "$count" -eq 8 ] || fail "release.json must describe exactly eight artifacts"
}

verify_previous_release() {
  local branch_sha="$1"
  local branch_manifest="$2"
  local previous_tag previous_tag_state previous_release_state previous_tag_sha

  previous_tag="$(jq -er '.tag' "$branch_manifest")"
  previous_tag_state="$TMP/previous-tag.json"
  optional_state "repos/$REPOSITORY/git/ref/tags/$previous_tag" "$previous_tag_state" \
    || fail "$RELEASE_BRANCH head is not referenced by $previous_tag"
  previous_tag_sha="$(jq -er '.object.sha' "$previous_tag_state")"
  [ "$previous_tag_sha" = "$branch_sha" ] \
    || fail "$previous_tag does not point to the current $RELEASE_BRANCH head"

  previous_release_state="$TMP/previous-release.json"
  optional_state "repos/$REPOSITORY/releases/tags/$previous_tag" "$previous_release_state" \
    || fail "published GitHub release is missing for $previous_tag"
  jq -e '.draft == false and .prerelease == false' "$previous_release_state" >/dev/null \
    || fail "$previous_tag is not a completed release"
}

verify_pending_commit() {
  local commit_sha="$1"
  local manifest="$2"
  local parent_sha="${3:-}"
  local pending_manifest="$TMP/pending-release.json"
  local commit_state="$TMP/pending-commit.json"

  read_release_manifest "$commit_sha" "$pending_manifest"
  cmp -s "$pending_manifest" "$manifest" \
    || fail "existing release tag records a different build"
  gh api "repos/$REPOSITORY/git/commits/$commit_sha" > "$commit_state"
  if [ -n "$parent_sha" ]; then
    jq -e --arg parent "$parent_sha" '
      (.parents | length) == 1 and .parents[0].sha == $parent
    ' "$commit_state" >/dev/null \
      || fail "pending release commit is not based on the current $RELEASE_BRANCH head"
  else
    jq -e '(.parents | length) == 0' "$commit_state" >/dev/null \
      || fail "first pending release commit is not an orphan root"
  fi
}

create_release_commit() {
  local version="$1"
  local manifest="$2"
  local parent_sha="${3:-}"
  local tree_payload commit_payload tree_sha

  tree_payload="$TMP/tree.json"
  jq -n --rawfile content "$manifest" '
    {
      tree: [
        {path: "release.json", mode: "100644", type: "blob", content: $content}
      ]
    }
  ' > "$tree_payload"
  tree_sha="$(gh api --method POST "repos/$REPOSITORY/git/trees" \
    --input "$tree_payload" --jq '.sha')"
  [[ "$tree_sha" =~ ^[0-9a-f]{40}$ ]] || fail "GitHub returned an invalid release tree SHA"

  commit_payload="$TMP/commit.json"
  if [ -n "$parent_sha" ]; then
    jq -n \
      --arg message "Release $version" \
      --arg tree "$tree_sha" \
      --arg parent "$parent_sha" \
      '{message: $message, tree: $tree, parents: [$parent]}' > "$commit_payload"
  else
    jq -n \
      --arg message "Release $version" \
      --arg tree "$tree_sha" \
      '{message: $message, tree: $tree, parents: []}' > "$commit_payload"
  fi
  gh api --method POST "repos/$REPOSITORY/git/commits" \
    --input "$commit_payload" --jq '.sha'
}

advance_release_branch() {
  local commit_sha="$1"
  local previous_sha="${2:-}"
  local payload="$TMP/ref.json"

  if [ -n "$previous_sha" ]; then
    jq -n --arg sha "$commit_sha" '{sha: $sha, force: false}' > "$payload"
    gh api --method PATCH "repos/$REPOSITORY/git/refs/heads/$RELEASE_BRANCH" \
      --input "$payload" >/dev/null
  else
    jq -n \
      --arg ref "refs/heads/$RELEASE_BRANCH" \
      --arg sha "$commit_sha" \
      '{ref: $ref, sha: $sha}' > "$payload"
    gh api --method POST "repos/$REPOSITORY/git/refs" --input "$payload" >/dev/null
  fi
}

create_tag_ref() {
  local tag="$1"
  local commit_sha="$2"
  local payload="$TMP/tag-ref.json"
  jq -n \
    --arg ref "refs/tags/$tag" \
    --arg sha "$commit_sha" \
    '{ref: $ref, sha: $sha}' > "$payload"
  gh api --method POST "repos/$REPOSITORY/git/refs" --input "$payload" >/dev/null
}

verify_uploaded_assets() {
  local release_state="$1"
  local bundle="$2"
  local manifest="$bundle/release.json"
  local expected="$TMP/expected-assets.tsv"
  local actual="$TMP/actual-assets.tsv"
  local manifest_sha manifest_size

  jq -r '.artifacts[] | [.name, ("sha256:" + .sha256), (.size | tostring), "uploaded"] | @tsv' \
    "$manifest" > "$expected"
  manifest_sha="$(sha256sum "$manifest" | awk '{print $1}')"
  manifest_size="$(stat -c '%s' "$manifest")"
  printf 'release.json\tsha256:%s\t%s\tuploaded\n' "$manifest_sha" "$manifest_size" \
    >> "$expected"
  LC_ALL=C sort -o "$expected" "$expected"

  jq -r '.assets[] | [.name, .digest, (.size | tostring), .state] | @tsv' \
    "$release_state" | LC_ALL=C sort > "$actual"
  if ! cmp -s "$expected" "$actual"; then
    echo "publish-release: uploaded asset set does not match the prepared bundle" >&2
    diff -u "$expected" "$actual" >&2 || true
    exit 1
  fi
}

publish_bundle() {
  local bundle="$1"
  local manifest="$bundle/release.json"
  local version tag branch_state branch_sha branch_manifest branch_version
  local tag_state tag_sha release_state release_commit release_id
  local branch_is_current=false

  validate_bundle "$bundle"
  version="$(jq -er '.version' "$manifest")"
  tag="$(jq -er '.tag' "$manifest")"
  validate_version "$version"

  branch_state="$TMP/branch.json"
  if optional_state "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" "$branch_state"; then
    branch_sha="$(jq -er '.object.sha' "$branch_state")"
    branch_manifest="$TMP/branch-release.json"
    read_release_manifest "$branch_sha" "$branch_manifest"
    branch_version="$(jq -er '.version' "$branch_manifest")"
    if [ "$branch_version" = "$version" ]; then
      cmp -s "$branch_manifest" "$manifest" \
        || fail "$RELEASE_BRANCH already records a different build for $version"
      branch_is_current=true
      release_commit="$branch_sha"
    else
      verify_previous_release "$branch_sha" "$branch_manifest"
    fi
  fi

  tag_state="$TMP/tag.json"
  if optional_state "repos/$REPOSITORY/git/ref/tags/$tag" "$tag_state"; then
    tag_sha="$(jq -er '.object.sha' "$tag_state")"
    if [ "$branch_is_current" = true ]; then
      [ "$tag_sha" = "$release_commit" ] \
        || fail "$tag does not point to the current $RELEASE_BRANCH head"
    else
      verify_pending_commit "$tag_sha" "$manifest" "${branch_sha:-}"
      release_commit="$tag_sha"
    fi
  elif [ "$branch_is_current" = true ]; then
    create_tag_ref "$tag" "$release_commit"
  else
    release_state="$TMP/release-before-commit.json"
    if optional_state "repos/$REPOSITORY/releases/tags/$tag" "$release_state"; then
      fail "GitHub release already exists without the expected release tag: $tag"
    fi
    release_commit="$(create_release_commit "$version" "$manifest" "${branch_sha:-}")"
    [[ "$release_commit" =~ ^[0-9a-f]{40}$ ]] \
      || fail "GitHub returned an invalid release commit SHA"
    create_tag_ref "$tag" "$release_commit"
  fi

  release_state="$TMP/release.json"
  if optional_state "repos/$REPOSITORY/releases/tags/$tag" "$release_state"; then
    jq -e '.draft == true' "$release_state" >/dev/null \
      || fail "$tag is already published; refusing to replace it"
    gh release edit "$tag" \
      --repo "$REPOSITORY" \
      --draft \
      --target "$release_commit" \
      --title "Kuasar Sandbox $version" \
      --notes-file "$bundle/release-notes.md" >/dev/null
  else
    gh release create "$tag" \
      --repo "$REPOSITORY" \
      --draft \
      --verify-tag \
      --target "$release_commit" \
      --title "Kuasar Sandbox $version" \
      --notes-file "$bundle/release-notes.md" >/dev/null
  fi

  artifact_files=()
  while IFS= read -r name; do
    artifact_files+=("$bundle/assets/$name")
  done < <(jq -r '.artifacts[].name' "$manifest")
  artifact_files+=("$manifest")
  gh release upload "$tag" --repo "$REPOSITORY" --clobber "${artifact_files[@]}"

  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$release_state"
  verify_uploaded_assets "$release_state" "$bundle"
  if [ "$branch_is_current" != true ]; then
    advance_release_branch "$release_commit" "${branch_sha:-}"
  fi
  release_id="$(jq -er '.id' "$release_state")"
  jq -n '{draft: false, prerelease: false, make_latest: "true"}' \
    | gh api --method PATCH "repos/$REPOSITORY/releases/$release_id" --input - >/dev/null

  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$release_state"
  jq -e '.draft == false and .prerelease == false' "$release_state" >/dev/null \
    || fail "$tag was not published"
  branch_sha="$(gh api "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" --jq '.object.sha')"
  tag_sha="$(gh api "repos/$REPOSITORY/git/ref/tags/$tag" --jq '.object.sha')"
  if [ "$branch_sha" != "$release_commit" ] || [ "$tag_sha" != "$release_commit" ]; then
    fail "published tag, release commit and $RELEASE_BRANCH head diverged"
  fi

  echo "==> published $tag from $release_commit"
}

if [ -z "$REPOSITORY" ]; then
  fail "GH_REPO or GITHUB_REPOSITORY is required"
fi
[[ "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] \
  || fail "invalid repository: $REPOSITORY"
command -v gh >/dev/null || fail "gh is required"
command -v jq >/dev/null || fail "jq is required"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "${1:-}" in
  check)
    [ "$#" -eq 2 ] || fail "usage: publish-release.sh check <version>"
    preflight "$2"
    ;;
  validate)
    [ "$#" -eq 2 ] || fail "usage: publish-release.sh validate <bundle-dir>"
    validate_bundle "$2"
    ;;
  publish)
    [ "$#" -eq 2 ] || fail "usage: publish-release.sh publish <bundle-dir>"
    publish_bundle "$2"
    ;;
  *)
    fail "usage: publish-release.sh <check <version>|validate <bundle-dir>|publish <bundle-dir>>"
    ;;
esac
