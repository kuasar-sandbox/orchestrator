#!/usr/bin/env bash

set -euo pipefail

VERSION=2.92.0
ARCHIVE="gh_${VERSION}_linux_amd64.tar.gz"
SHA256=b57848131bdf0c229cd35e1f2a51aa718199858b2e728410b37e89a428943ec4

[ -n "${RUNNER_TEMP:-}" ] || { echo "install-gh-cli: RUNNER_TEMP is required" >&2; exit 1; }
[ -n "${GITHUB_PATH:-}" ] || { echo "install-gh-cli: GITHUB_PATH is required" >&2; exit 1; }
[ "$(uname -m)" = x86_64 ] || { echo "install-gh-cli: x86_64 is required" >&2; exit 1; }

cache_root=${KUASAR_GH_CLI_CACHE:-/var/cache/kuasar/gh-cli}
if ! install -d -m 0755 "$cache_root" 2>/dev/null \
  || ! { exec 9>"$cache_root/.download.lock"; } 2>/dev/null; then
  cache_root="$RUNNER_TEMP/gh-cli-cache"
  install -d -m 0755 "$cache_root"
  exec 9>"$cache_root/.download.lock"
fi
cache_archive="$cache_root/$ARCHIVE"
flock 9
if ! printf '%s  %s\n' "$SHA256" "$cache_archive" | sha256sum --check --status; then
  download_path="$(mktemp "$cache_root/$ARCHIVE.XXXXXX")"
  trap 'rm -f "$download_path"' EXIT
  curl --fail --location --retry 3 --retry-max-time 240 \
    --connect-timeout 20 --max-time 300 \
    --silent --show-error --output "$download_path" \
    "https://github.com/cli/cli/releases/download/v${VERSION}/${ARCHIVE}"
  printf '%s  %s\n' "$SHA256" "$download_path" | sha256sum --check
  mv "$download_path" "$cache_archive"
  trap - EXIT
fi
flock -u 9

install_root="$(mktemp -d "$RUNNER_TEMP/kuasar-gh-cli.XXXXXX")"
archive_path="$install_root/$ARCHIVE"
cp "$cache_archive" "$archive_path"
printf '%s  %s\n' "$SHA256" "$archive_path" | sha256sum --check
tar -xzf "$archive_path" -C "$install_root"

bin_dir="$install_root/gh_${VERSION}_linux_amd64/bin"
[ -x "$bin_dir/gh" ] || { echo "install-gh-cli: extracted gh is missing" >&2; exit 1; }
printf '%s\n' "$bin_dir" >> "$GITHUB_PATH"
"$bin_dir/gh" --version
