#!/usr/bin/env bash
# Prepare the existing immutable e2b base fixture before artifact E2E.
set -euo pipefail
[ "$#" = 3 ] || { echo "usage: prepare_base_image.sh <base-image> <output-tag> base|execute" >&2; exit 2; }
E2E_IMAGE=$1
REF=$2
mode=$3
case "$mode" in base|execute) ;; *) exit 2 ;; esac
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
docker image inspect "$E2E_IMAGE" >/dev/null
cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -c|-n) shift 2 ;;
    -c*|-n*) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
exec "$@"
SH
cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice \
 && if ! id -u user >/dev/null 2>&1; then \
      if command -v useradd >/dev/null 2>&1; then useradd -m -d /home/user -s /bin/sh user; \
      elif command -v adduser >/dev/null 2>&1; then adduser -D -h /home/user -s /bin/sh user; \
      else echo "missing useradd/adduser" >&2; exit 1; fi; \
    fi \
 && mkdir -p /home/user \
 && chown user:user /home/user \
 && id user >/dev/null
EOF
if [ "$mode" = execute ]; then printf '%s\n' 'CMD ["sleep", "86400"]' >> "$WORK/Dockerfile.e2e"; fi
docker build --pull=false --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK"
