#!/usr/bin/env bash

# plaintext_tarstream_ref prints a scheme-qualified local ref for a plaintext
# tarstream fixture. Plaintext fixtures carry exactly one canonical digest
# marker; callers should fail before starting a sandbox if that contract is not
# satisfied.
plaintext_tarstream_ref() {
    local path="$1"
    local listing marker_count marker digest

    if [ ! -f "$path" ]; then
        echo "plaintext tarstream does not exist: $path" >&2
        return 1
    fi
    if ! listing="$(tar -tf "$path")"; then
        echo "failed to inspect plaintext tarstream: $path" >&2
        return 1
    fi

    marker_count="$(grep -Ec '^\.kuasar\.digest\.[0-9a-f]{64}$' <<<"$listing" || true)"
    if [ "$marker_count" -ne 1 ]; then
        echo "plaintext tarstream must contain exactly one digest marker: $path" >&2
        return 1
    fi
    marker="$(grep -E '^\.kuasar\.digest\.[0-9a-f]{64}$' <<<"$listing")"
    digest="${marker#.kuasar.digest.}"
    printf 'file://%s@digest:%s\n' "$path" "$digest"
}
