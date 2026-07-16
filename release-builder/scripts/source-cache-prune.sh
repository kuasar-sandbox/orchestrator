#!/usr/bin/env bash

set -euo pipefail

SOURCE_DIR=${1:?usage: source-cache-prune.sh SOURCE_DIR [PROTECTED_ARCHIVE]}
PROTECTED_ARCHIVE=${2:-}
MAX_ENTRIES=${KUASAR_SOURCE_CACHE_MAX_ENTRIES:-32}

if ! [[ "$MAX_ENTRIES" =~ ^[1-9][0-9]*$ ]]; then
    echo "source-cache-prune: KUASAR_SOURCE_CACHE_MAX_ENTRIES must be a positive integer" >&2
    exit 2
fi

install -d -m 0755 "$SOURCE_DIR"
exec {cache_lock_fd}>"$SOURCE_DIR/.cache.lock"
flock "$cache_lock_fd"

records=()
mapfile -d '' records < <(
    find "$SOURCE_DIR" -maxdepth 1 -type f -name '*.tar.gz' -printf '%T@\t%p\0' \
        | sort -z -nr
)

kept=0
removed=0
if [ -n "$PROTECTED_ARCHIVE" ] && [ -f "$PROTECTED_ARCHIVE" ]; then
    kept=1
fi

for record in "${records[@]}"; do
    archive=${record#*$'\t'}
    if [ -n "$PROTECTED_ARCHIVE" ] && [ "$archive" = "$PROTECTED_ARCHIVE" ]; then
        continue
    fi
    if [ "$kept" -lt "$MAX_ENTRIES" ]; then
        kept=$((kept + 1))
        continue
    fi
    rm -f "$archive" "$archive.sha256"
    removed=$((removed + 1))
done

# Remove metadata left by interrupted downloads and the former per-SHA lock
# layout. The repository-wide lock above serializes all current users.
while IFS= read -r -d '' checksum; do
    [ -f "${checksum%.sha256}" ] || rm -f "$checksum"
done < <(find "$SOURCE_DIR" -maxdepth 1 -type f -name '*.tar.gz.sha256' -print0)
find "$SOURCE_DIR" -maxdepth 1 -type f -name '*.lock' ! -name '.cache.lock' -delete

printf 'source-cache-prune: retained=%d removed=%d directory=%s\n' "$kept" "$removed" "$SOURCE_DIR"
