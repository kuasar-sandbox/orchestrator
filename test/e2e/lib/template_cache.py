"""Run-owned immutable execute-image cache; caller holds the entry flock."""
import argparse
import base64
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import tempfile
import time

RECIPE = "execute-image-fromImage-no-steps-v1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(env):
    provenance = env["E2E_TEMPLATE_CACHE_PROVENANCE"]
    image = env["ORCHESTRATOR_EXECUTE_IMAGE"]
    arch = env["E2E_ARCH"]
    require(re.fullmatch(r"[0-9a-f]{64}", provenance), "invalid prepared provenance digest")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "execute image must be a full config ID")
    require(arch == platform.machine() and arch in {"x86_64", "aarch64"}, "native architecture required")
    return {"schema": 1, "provenance": provenance, "image_config_id": image,
            "arch": arch, "role": "orchestrator-execute", "recipe": RECIPE,
            "tools": {name: digest(Path(env["BIN"]) / name)
                      for name in ("manifest-ctl", "flatten-ctl")}}


def entry_path(env, value):
    root = Path(env["E2E_TEMPLATE_CACHE_DIR"])
    require(root.is_absolute() and root.is_dir() and not root.is_symlink(), "cache root must be a real absolute directory")
    require(stat.S_IMODE(root.stat().st_mode) == 0o700, "cache root must remain private")
    key = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return root / key


def image_key(template, profile):
    require(profile in {"e2b", "bare"}, "invalid image profile")
    prefix = profile + "-img-"
    require(template.startswith(prefix), "expected image template in the requested profile")
    payload = template[len(prefix):]
    raw = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    require(base64.urlsafe_b64encode(raw).decode().rstrip("=") == payload, "noncanonical template reference")
    ref = raw.decode()
    require(re.fullmatch(r"manifest://[0-9a-f]{64}", ref), "expected a Manifest image reference")
    return ref.removeprefix("manifest://")


def image_template(key, profile):
    require(profile in {"e2b", "bare"} and re.fullmatch(r"[0-9a-f]{64}", key), "invalid image identity")
    return profile + "-img-" + base64.urlsafe_b64encode(("manifest://" + key).encode()).decode().rstrip("=")


def validate(entry, value):
    require(entry.is_dir() and not entry.is_symlink(), "damaged cache entry directory")
    require({p.name for p in entry.iterdir()} == {"image.tar", "receipt.json"}, "incomplete cache entry")
    for name in ("image.tar", "receipt.json"):
        path = entry / name
        require(stat.S_ISREG(path.lstat().st_mode) and path.stat().st_nlink == 1,
                "cache files must be independent regular files")
    receipt = json.loads((entry / "receipt.json").read_text())
    image = entry / "image.tar"
    require(receipt["identity"] == value, "cache identity mismatch")
    require(receipt["bytes"] == image.stat().st_size > 0 and receipt["sha256"] == digest(image),
            "cache content integrity failure")
    return image


@contextlib.contextmanager
def neutral_config(work):
    # YAML is already a prerequisite of the execute harness. Keep the case's
    # Store/chunker/encryption settings, changing only local stream encryption.
    import yaml
    config = yaml.safe_load((work / "manifest.yaml").read_text())
    config.setdefault("crypto", {})["local"] = "off"
    with tempfile.NamedTemporaryFile(mode="w", prefix="template-neutral-", suffix=".json", dir=work) as stream:
        json.dump(config, stream)
        stream.flush()
        yield stream.name


def command(binary, *args):
    return subprocess.check_output([str(binary), *map(str, args)], text=True, timeout=120).strip()


def check_image(binary, config, reference):
    info = json.loads(command(binary, "info", "--json", "--manifest-config", config, reference))
    require(isinstance(info.get("erofs_size"), int) and info["erofs_size"] > 0
            and isinstance(info.get("config"), dict), "cached artifact is not a complete execute Image")


def publish(entry, value, work, binary, template, profile, metrics=None):
    require(not entry.exists() and not entry.is_symlink(), "cache entry already exists")
    key = image_key(template, profile)
    # An interrupted staging directory is retained for diagnostics and never
    # selected as an entry. Rename publishes content and receipt together.
    stage = Path(tempfile.mkdtemp(prefix="." + entry.name + ".pending-", dir=entry.parent))
    started = time.monotonic()
    with neutral_config(work) as config:
        check_image(binary / "flatten-ctl", config, "manifest://" + key)
        export_started = time.monotonic()
        command(binary / "manifest-ctl", "load", "--manifest-config", config,
                "--no-progress", "--output", stage / "image.tar", key)
        export_seconds = time.monotonic() - export_started
        check_image(binary / "flatten-ctl", config, stage / "image.tar")
    receipt = {"identity": value, "bytes": (stage / "image.tar").stat().st_size,
               "sha256": digest(stage / "image.tar"), "export_seconds": time.monotonic() - started}
    (stage / "receipt.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
    for path in stage.iterdir():
        path.chmod(0o400)
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
    validate(stage, value)
    stage.chmod(0o500)
    descriptor = os.open(stage, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    stage.rename(entry)
    descriptor = os.open(entry.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if metrics is not None:
        metrics.update(phase="publish", export_seconds=export_seconds,
                       validation_and_publication_seconds=time.monotonic() - started - export_seconds)


def restore(entry, value, work, binary, profile, metrics=None):
    started = time.monotonic()
    image = validate(entry, value)
    with neutral_config(work) as config:
        check_image(binary / "flatten-ctl", config, image)
        import_started = time.monotonic()
        key = command(binary / "manifest-ctl", "store", "--manifest-config", config, "--no-progress", image)
        import_seconds = time.monotonic() - import_started
        require(re.fullmatch(r"[0-9a-f]{64}", key), "import returned an invalid Manifest key")
        check_image(binary / "flatten-ctl", config, "manifest://" + key)
    if metrics is not None:
        metrics.update(phase="restore", import_seconds=import_seconds,
                       verification_seconds=time.monotonic() - started - import_seconds)
    return image_template(key, profile)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("entry", "publish", "restore", "record"))
    parser.add_argument("--work", type=Path)
    parser.add_argument("--profile", choices=("e2b", "bare"), default="e2b")
    parser.add_argument("--template")
    parser.add_argument("--mode", choices=("miss", "hit", "disabled"))
    parser.add_argument("--started", type=float)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--phase", default="preparation", choices=("preparation", "production", "registration"))
    args = parser.parse_args()
    if args.action == "record":
        record = dict(mode=args.mode, profile=args.profile, phase=args.phase, wall_seconds=time.monotonic() - args.started)
        with args.output.open("a") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        print("==> template preparation " + json.dumps(record, sort_keys=True))
        return
    value = identity(os.environ)
    entry = entry_path(os.environ, value)
    metrics = {"profile": args.profile}
    if args.action == "entry":
        print(entry)
    elif args.action == "publish":
        publish(entry, value, args.work, Path(os.environ["BIN"]), args.template, args.profile, metrics)
    else:
        print(restore(entry, value, args.work, Path(os.environ["BIN"]), args.profile, metrics))
    if args.output is not None:
        with args.output.open("a") as stream:
            stream.write(json.dumps(metrics, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
