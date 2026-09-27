#!/usr/bin/env bash
persist_ref() {
    python3 - "$1" <<'PY'
import base64
import re
import sys

try:
    profile, kind, payload = sys.argv[1].split("-", 2)
    if profile not in {"e2b", "bare"} or kind not in {"img", "sbx", "snp"}:
        raise ValueError
    raw = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    if base64.urlsafe_b64encode(raw).decode().rstrip("=") != payload:
        raise ValueError
    ref = raw.decode()
    manifest_ref = re.fullmatch(r"manifest://[0-9a-f]{64}", ref)
    located_ref = re.fullmatch(
        r"file://[0-9a-f]{64}\.(?:bundle|sandbox|snapshot|overlay)"
        r"@(?:manifest|digest|hmac):[0-9a-f]{64}"
        r"@location:[A-Za-z0-9][A-Za-z0-9._-]{0,127}",
        ref,
    )
    if not manifest_ref and not located_ref:
        raise ValueError
except (ValueError, UnicodeDecodeError):
    raise SystemExit(1)
print(ref)
PY
}
valid_persist_id() { persist_ref "$1" >/dev/null; }
manifest_count() {
    if [ ! -d "$WORK/store/manifest/G1" ]; then
        echo 0
        return
    fi
    find "$WORK/store/manifest/G1" -type f | wc -l
}
snapshot_manifest_keys() { # output file
    local output="$1"
    if [ ! -d "$WORK/store/manifest/G1" ]; then
        : >"$output"
        return
    fi
    find "$WORK/store/manifest/G1" -type f -printf '%f\n' | sort -u >"$output"
}
assert_only_manifest_ref_added() { # before-list manifest-ref label
    local before="$1" ref="$2" label="$3" key after additions final
    [[ "$ref" == manifest://* ]] || fail "$label expected a Manifest ref, got $ref"
    key=${ref#manifest://}
    after="$WORK/$label-manifests.after"
    additions="$WORK/$label-manifests.added"
    snapshot_manifest_keys "$after"
    comm -13 "$before" "$after" >"$additions"
    if [ -s "$additions" ] && [ "$(wc -l <"$additions")" != "1" ]; then
        cat "$additions" >&2
        fail "$label created more than its one final Manifest root"
    fi
    if [ -s "$additions" ] && [ "$(cat "$additions")" != "$key" ]; then
        cat "$additions" >&2
        fail "$label created an intermediate Manifest instead of only $key"
    fi
    final="$WORK/store/manifest/G1/${key:0:2}/${key:2:2}/$key"
    if [ ! -f "$final" ] || [ -L "$final" ]; then
        fail "$label final Manifest root is missing or unsafe: $final"
    fi
}
located_ref_details() { # ref -> "name<TAB>directory<TAB>file"
    python3 - "$1" "$WORK/ref-locations" <<'PY'
import hashlib, pathlib, re, sys

ref, parent = sys.argv[1:]
match = re.search(r"@location:([A-Za-z0-9][A-Za-z0-9._-]{0,127})$", ref)
assert match, ref
name = match.group(1)
payload = ref.removeprefix("file://").split("@", 1)[0]
assert payload and "/" not in payload, payload
digest = hashlib.sha256(name.encode()).hexdigest()
directory = pathlib.Path(parent, digest[:2], digest[2:4], name)
print(f"{name}\t{directory}\t{directory / payload}")
PY
}
located_file_path() { # ref
    local directory file
    IFS=$'\t' read -r _ directory file < <(located_ref_details "$1")
    printf '%s\n' "$file"
}
artifact_info() { # ref output stderr
    local ref="$1" output="$2" error_output="$3" binding="" location_dir=""
    local args=(info --json --manifest-config "$WORK/manifest.yaml")
    if [[ "$ref" == *"@location:"* ]]; then
        # Include every currently materialized publication mapping. With the
        # undated layout one Build's image and checkpoint publications share
        # one BuildID-keyed location, but older publications still resolve
        # through their own names.
        while IFS= read -r -d '' location_dir; do
            binding="$(basename "$location_dir")=file://$location_dir"
            args+=(--ref-location "$binding")
        done < <(find "$WORK/ref-locations" -mindepth 3 -maxdepth 3 -type d -print0 | sort -z)
    fi
    MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" "${args[@]}" "$ref" >"$output" 2>"$error_output"
}
assert_located_final() { # ref extension
    local ref="$1" extension="$2" final
    [[ "$ref" == file://*"@location:"* ]] || fail "ref is not located: $ref"
    final=$(located_file_path "$ref") || fail "cannot resolve located ref $ref"
    if [ ! -f "$final" ] || [ -L "$final" ]; then
        fail "located final is missing, non-regular, or a symlink: $final"
    fi
    [[ "$final" == *"$extension" ]] \
        || fail "located final $final does not end in $extension"
}
assert_bundle_directory_only() { # ref expected-bundle-count
    local ref="$1" expected="$2" final directory bundles others
    assert_located_final "$ref" .bundle
    final=$(located_file_path "$ref")
    directory=$(dirname "$final")
    bundles=$(find "$directory" -maxdepth 1 -type f -name '*.bundle' | wc -l)
    others=$(find "$directory" -maxdepth 1 -type f ! -name '*.bundle' -print)
    [ "$bundles" = "$expected" ] \
        || { find "$directory" -maxdepth 1 -ls >&2; fail "Bundle count=$bundles in $directory (want $expected)"; }
    [ -z "$others" ] \
        || { printf '%s\n' "$others" >&2; fail "named Bundle location contains a tarstream/partial final"; }
}
