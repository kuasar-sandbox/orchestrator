#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "prepare-release: $*" >&2
  exit 1
}

if [ "$#" -ne 5 ]; then
  echo "usage: prepare-release.sh <version> <arch> <revisions.tsv> <dist-dir> <output-dir>" >&2
  exit 2
fi

VERSION="$1"
ARCH="$2"
REVISION_MANIFEST="$3"
DIST="$4"
OUTPUT="$5"

[[ "$VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] \
  || fail "version must match vX.Y.Z without leading zeroes"
case "$ARCH" in
  amd64) ARCH=x86_64 ;;
  arm64) ARCH=aarch64 ;;
esac
case "$ARCH" in
  x86_64|aarch64) ;;
  *) fail "unsupported architecture: $ARCH" ;;
esac
[ -f "$REVISION_MANIFEST" ] || fail "revision manifest not found: $REVISION_MANIFEST"
[ -d "$DIST" ] || fail "release dist directory not found: $DIST"
if [ -z "$OUTPUT" ] || [ "$OUTPUT" = "/" ] || [ "$OUTPUT" = "." ]; then
  fail "unsafe output directory: $OUTPUT"
fi
[ ! -e "$OUTPUT" ] || fail "output directory already exists: $OUTPUT"

: "${RELEASE_WORKFLOW_REPOSITORY:?RELEASE_WORKFLOW_REPOSITORY is required}"
: "${RELEASE_WORKFLOW_RUN_ID:?RELEASE_WORKFLOW_RUN_ID is required}"
: "${RELEASE_WORKFLOW_RUN_ATTEMPT:?RELEASE_WORKFLOW_RUN_ATTEMPT is required}"
: "${RELEASE_WORKFLOW_RUN_URL:?RELEASE_WORKFLOW_RUN_URL is required}"
[[ "$RELEASE_WORKFLOW_REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] \
  || fail "invalid workflow repository: $RELEASE_WORKFLOW_REPOSITORY"
[[ "$RELEASE_WORKFLOW_RUN_ID" =~ ^[1-9][0-9]*$ ]] \
  || fail "workflow run ID must be a positive integer"
[[ "$RELEASE_WORKFLOW_RUN_ATTEMPT" =~ ^[1-9][0-9]*$ ]] \
  || fail "workflow run attempt must be a positive integer"

repos=(accelerator connector guest-runtime orchestrator sandboxer)
declare -A expected_repo=()
declare -A revisions=()
declare -A seen_repo=()
for repo in "${repos[@]}"; do
  expected_repo["$repo"]=1
done

IFS= read -r header < "$REVISION_MANIFEST" || fail "revision manifest is empty"
[ "$header" = $'repository\trequested_ref\tresolved_sha\trole' ] \
  || fail "unexpected revision manifest header"

line_number=1
while IFS=$'\t' read -r repository requested_ref resolved_sha role extra; do
  line_number=$((line_number + 1))
  if [ -z "$repository" ] || [ -z "$requested_ref" ] || [ -z "$resolved_sha" ] \
    || [ -z "$role" ] || [ -n "$extra" ]; then
    fail "malformed revision manifest row $line_number"
  fi
  [[ "$repository" =~ ^kuasar-sandbox/([A-Za-z0-9_.-]+)$ ]] \
    || fail "unexpected repository at row $line_number: $repository"
  repo="${BASH_REMATCH[1]}"
  [ "${expected_repo[$repo]:-}" = 1 ] \
    || fail "repository is outside the five-repository release set: $repository"
  [ -z "${seen_repo[$repo]:-}" ] || fail "duplicate revision for $repository"
  [[ "$resolved_sha" =~ ^[0-9a-f]{40}$ ]] \
    || fail "invalid commit SHA for $repository"
  seen_repo["$repo"]=1
  revisions["$repo"]="$resolved_sha"
done < <(sed -n '2,$p' "$REVISION_MANIFEST")

for repo in "${repos[@]}"; do
  [ -n "${revisions[$repo]:-}" ] || fail "revision for kuasar-sandbox/$repo is missing"
done
[ "${#seen_repo[@]}" -eq "${#repos[@]}" ] \
  || fail "revision manifest must contain exactly five repositories"

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
expected_files=("${archives[@]}" SHA256SUMS)
declare -A expected_archive=()
declare -A checksums=()
for archive in "${archives[@]}"; do
  expected_archive["$archive"]=1
done

mapfile -t actual_files < <(find "$DIST" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
mapfile -t sorted_expected_files < <(printf '%s\n' "${expected_files[@]}" | LC_ALL=C sort)
[ "${actual_files[*]}" = "${sorted_expected_files[*]}" ] \
  || fail "dist must contain exactly the seven release archives and SHA256SUMS"

for index in "${!archives[@]}"; do
  archive="${archives[$index]}"
  metadata_name="${metadata_names[$index]}"
  metadata_repo="${metadata_repos[$index]}"
  metadata="$(tar -xOzf "$DIST/$archive" "./release/$metadata_name.json" 2>/dev/null)" \
    || fail "$archive does not contain release/$metadata_name.json"
  jq -e \
    --arg name "$metadata_name" \
    --arg version "$VERSION" \
    --arg arch "$ARCH" \
    --arg archive "$archive" \
    --arg commit "${revisions[$metadata_repo]}" '
      .name == $name
      and .version == $version
      and .arch == $arch
      and .archive == $archive
      and .commit == $commit
      and (.built | type == "string")
    ' <<< "$metadata" >/dev/null \
    || fail "$archive contains release metadata that does not match the revision set"
done

checksum_rows=0
while read -r checksum filename extra; do
  if [ -z "$checksum" ] || [ -z "$filename" ] || [ -n "${extra:-}" ]; then
    fail "malformed SHA256SUMS row"
  fi
  filename="${filename#\*}"
  [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || fail "invalid checksum for $filename"
  [ "${expected_archive[$filename]:-}" = 1 ] \
    || fail "unexpected SHA256SUMS entry: $filename"
  [ -z "${checksums[$filename]:-}" ] || fail "duplicate SHA256SUMS entry: $filename"
  actual_checksum="$(sha256sum "$DIST/$filename" | awk '{print $1}')"
  [ "$actual_checksum" = "$checksum" ] || fail "checksum mismatch for $filename"
  checksums["$filename"]="$checksum"
  checksum_rows=$((checksum_rows + 1))
done < "$DIST/SHA256SUMS"
[ "$checksum_rows" -eq "${#archives[@]}" ] \
  || fail "SHA256SUMS must contain exactly seven entries"

mkdir -p "$OUTPUT/assets"
for filename in "${expected_files[@]}"; do
  install -m 0644 "$DIST/$filename" "$OUTPUT/assets/$filename"
done

artifacts_json="$OUTPUT/.artifacts.ndjson"
for filename in "${archives[@]}"; do
  jq -cn \
    --arg name "$filename" \
    --arg sha256 "${checksums[$filename]}" \
    --argjson size "$(stat -c '%s' "$DIST/$filename")" \
    '{name: $name, sha256: $sha256, size: $size}' >> "$artifacts_json"
done
jq -cn \
  --arg name SHA256SUMS \
  --arg sha256 "$(sha256sum "$DIST/SHA256SUMS" | awk '{print $1}')" \
  --argjson size "$(stat -c '%s' "$DIST/SHA256SUMS")" \
  '{name: $name, sha256: $sha256, size: $size}' >> "$artifacts_json"

created_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg version "$VERSION" \
  --arg tag "release-$VERSION" \
  --arg architecture "$ARCH" \
  --arg created_at "$created_at" \
  --arg accelerator "${revisions[accelerator]}" \
  --arg connector "${revisions[connector]}" \
  --arg guest_runtime "${revisions[guest-runtime]}" \
  --arg orchestrator "${revisions[orchestrator]}" \
  --arg sandboxer "${revisions[sandboxer]}" \
  --arg workflow_repository "$RELEASE_WORKFLOW_REPOSITORY" \
  --argjson workflow_run_id "$RELEASE_WORKFLOW_RUN_ID" \
  --argjson workflow_run_attempt "$RELEASE_WORKFLOW_RUN_ATTEMPT" \
  --arg workflow_run_url "$RELEASE_WORKFLOW_RUN_URL" \
  --slurpfile artifacts "$artifacts_json" '
    {
      schemaVersion: 1,
      version: $version,
      tag: $tag,
      architecture: $architecture,
      createdAt: $created_at,
      repositories: {
        accelerator: {
          repository: "kuasar-sandbox/accelerator",
          commit: $accelerator
        },
        connector: {
          repository: "kuasar-sandbox/connector",
          commit: $connector
        },
        "guest-runtime": {
          repository: "kuasar-sandbox/guest-runtime",
          commit: $guest_runtime
        },
        orchestrator: {
          repository: "kuasar-sandbox/orchestrator",
          commit: $orchestrator
        },
        sandboxer: {
          repository: "kuasar-sandbox/sandboxer",
          commit: $sandboxer
        }
      },
      validation: {
        suite: "BMS E2E",
        workflowRepository: $workflow_repository,
        workflowRunId: $workflow_run_id,
        workflowRunAttempt: $workflow_run_attempt,
        workflowRunUrl: $workflow_run_url
      },
      artifacts: $artifacts
    }
  ' > "$OUTPUT/release.json"
rm -f "$artifacts_json"

cat > "$OUTPUT/release-notes.md" <<EOF
This aggregate release was built and validated from one exact five-repository revision set.

### Source revisions

| Repository | Commit |
|---|---|
| accelerator | \`${revisions[accelerator]}\` |
| connector | \`${revisions[connector]}\` |
| guest-runtime | \`${revisions[guest-runtime]}\` |
| orchestrator | \`${revisions[orchestrator]}\` |
| sandboxer | \`${revisions[sandboxer]}\` |

### Validation

The full BMS E2E suite passed before packaging. [Workflow run]($RELEASE_WORKFLOW_RUN_URL)

### Assets

The seven merge-safe component archives can be extracted into one deployment directory. Verify them with \`SHA256SUMS\`. \`release.json\` records the revision set, workflow run and artifact digests used by the \`release\` branch commit.
EOF

echo "==> prepared $OUTPUT for release-$VERSION"
