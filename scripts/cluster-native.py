#!/usr/bin/env python3
"""Build and validate the fixed FoundationDB runtime without a system install.

The lock is a reviewed build input, not an environment-selected dependency.
Only cluster-ctl uses this native path; the other commands remain CGO=0.
"""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request

from deb_extract import extract_deb, sha256


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
LOCK = json.loads((SCRIPT_DIR / "cluster-native-lock.json").read_text())
NATIVE_SHARE = "share/licenses/orchestrator/native"
MANIFEST = NATIVE_SHARE + "/runtime.json"


def digest(path):
    return sha256(path)


def check(condition, message):
    if not condition:
        raise ValueError(message)


def architecture(value):
    value = {"amd64": "x86_64", "arm64": "aarch64"}.get(value, value)
    check(value in LOCK["architectures"], "unsupported native architecture: " + value)
    return value


def manifest(arch):
    return json.dumps({
        "architecture": arch,
        "foundationdb_version": LOCK["foundationdb_version"],
        "foundationdb_commit": LOCK["foundationdb_commit"],
        "go_binding": LOCK["go_binding"],
        "headers": LOCK["headers"],
        "foundationdb_license": LOCK["foundationdb_license"],
        "runtime": LOCK["architectures"][arch],
        "x86_64_cpu_requirement": LOCK["x86_64_cpu_requirement"],
    }, sort_keys=True, indent=2) + "\n"


def download(asset, cache):
    name = asset["url"].rsplit("/", 1)[-1]
    destination = cache / (asset["sha256"] + "-" + name)
    if destination.exists():
        check(not destination.is_symlink() and digest(destination) == asset["sha256"],
              "cached native input failed SHA256: " + str(destination))
        return destination
    cache.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=cache, delete=False) as stream:
            temporary = Path(stream.name)
            with urllib.request.urlopen(asset["url"], timeout=60) as response:
                shutil.copyfileobj(response, stream)
        check(digest(temporary) == asset["sha256"], "native input failed SHA256: " + asset["url"])
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def copy(source, destination, mode=0o644):
    check(source.is_file(), "missing native input: " + str(source))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    destination.chmod(mode)


def readelf(path, option):
    return subprocess.check_output(["readelf", option, str(path)], text=True,
                                   env={**os.environ, "LC_ALL": "C"})


def inspect_elf(path, arch):
    with path.open("rb") as stream:
        header = stream.read(20)
    machine = 62 if arch == "x86_64" else 183
    check(len(header) == 20 and header[:6] == b"\x7fELF\x02\x01" and
          int.from_bytes(header[18:20], "little") == machine,
          "wrong ELF architecture: " + str(path))
    dynamic = readelf(path, "-d")
    needed = re.findall(r"\(NEEDED\).*\[([^]]+)\]", dynamic)
    soname = re.findall(r"\(SONAME\).*\[([^]]+)\]", dynamic)
    versions = readelf(path, "-V")
    definitions = set()
    requirements = {}
    section = ""
    provider = None
    for line in versions.splitlines():
        if line.startswith("Version definition section"):
            section = "definitions"
        elif line.startswith("Version needs section"):
            section = "requirements"
        elif line.startswith("Version symbols section"):
            section = "symbols"
        if section == "definitions":
            definitions.update(re.findall(r"Name: (\S+)", line))
        elif section == "requirements":
            match = re.search(r"File: (\S+)", line)
            if match:
                provider = match.group(1)
            match = re.search(r"Name: (\S+)", line)
            if match and provider:
                requirements.setdefault(provider, set()).add(match.group(1))
    return {"path": path, "needed": needed, "soname": soname,
            "definitions": definitions, "requirements": requirements}


def validate(root, arch, binary=True):
    config = LOCK["architectures"][arch]
    expected = set(config["files"])
    expected.add(MANIFEST)
    expected.add("bin/run-cluster-ctl")
    actual = set()
    for prefix in ("lib/cluster-ctl", NATIVE_SHARE):
        directory = root / prefix
        check(directory.is_dir() and not directory.is_symlink(), "missing native directory: " + prefix)
        for path in directory.rglob("*"):
            check(not path.is_symlink(), "native runtime must not contain symlinks: " + str(path))
            if not path.is_dir():
                actual.add(path.relative_to(root).as_posix())
    actual.add("bin/run-cluster-ctl")
    check(actual == expected, "native runtime file set differs from the fixed allowlist")
    for name, checksum in config["files"].items():
        path = root / name
        check(path.is_file() and not path.is_symlink() and digest(path) == checksum,
              "native runtime SHA256 mismatch: " + name)
        expected_mode = 0o755 if name == "lib/cluster-ctl/ld.so" else 0o644
        check(path.stat().st_mode & 0o777 == expected_mode, "wrong native runtime mode: " + name)
    check((root / MANIFEST).read_text() == manifest(arch), "native runtime provenance differs from the lock")
    launcher = root / "bin/run-cluster-ctl"
    check(launcher.is_file() and not launcher.is_symlink() and
          launcher.read_bytes() == (SCRIPT_DIR / "run-cluster-ctl").read_bytes() and
          launcher.stat().st_mode & 0o777 == 0o755, "native launcher differs from the reviewed launcher")
    programs = [root / "bin/cluster-ctl"] if binary else []
    if binary:
        program = inspect_elf(programs[0], arch)
        check("libfdb_c.so" in program["needed"], "cluster-ctl does not link the FoundationDB client")
    validate_programs(root, arch, programs)


def validate_programs(root, arch, programs):
    """Verify a native program's entire dependency and version closure as data."""
    config = LOCK["architectures"][arch]
    libraries = [inspect_elf(root / name, arch) for name in config["files"] if name.startswith("lib/")]
    providers = {}
    for library in libraries:
        for name in [library["path"].name, *library["soname"]]:
            check(name not in providers or providers[name] is library, "duplicate native SONAME: " + name)
            providers[name] = library
    objects = libraries + [inspect_elf(path, arch) for path in programs]
    for obj in objects:
        for needed in obj["needed"]:
            check(needed in providers, "unbundled native dependency: " + needed + " required by " + str(obj["path"]))
        for needed, versions in obj["requirements"].items():
            check(needed in providers and versions <= providers[needed]["definitions"],
                  "native ABI not supplied by the bundle: " + needed + " required by " + str(obj["path"]))


def prepare(arch, work):
    config = LOCK["architectures"][arch]
    cache = work.parent / "downloads"
    work.mkdir(parents=True, exist_ok=True)
    runtime = work / "runtime"
    if runtime.exists():
        validate(runtime, arch, binary=False)
        for name, checksum in LOCK["headers"]["files"].items():
            header = work / "include/foundationdb" / name
            check(header.is_file() and not header.is_symlink() and digest(header) == checksum,
                  "cached FoundationDB header failed SHA256: " + name)
        return runtime, work / "include"
    with tempfile.TemporaryDirectory(prefix="prepare-", dir=work) as temporary:
        temporary = Path(temporary)
        extracted = temporary / "packages"
        extracted.mkdir()
        for asset in [config["client"], *config["packages"]]:
            source = download(asset, cache)
            if asset["url"].endswith(".so"):
                copy(source, extracted / "usr/lib/libfdb_c.so")
            else:
                extract_deb(source, extracted, asset["sha256"])
        # The release's standalone headers tar omits fdb_c_types.h. The
        # architecture-independent SDK in the official clients deb is complete.
        header_root = temporary / "headers"
        extract_deb(download(LOCK["headers"], cache), header_root, LOCK["headers"]["sha256"])
        include = temporary / "include/foundationdb"
        include.mkdir(parents=True)
        source_headers = header_root / "usr/include/foundationdb"
        check({path.name for path in source_headers.iterdir()} == set(LOCK["headers"]["files"]),
              "unexpected FoundationDB SDK contents")
        for name, checksum in LOCK["headers"]["files"].items():
            source = source_headers / name
            check(source.is_file() and not source.is_symlink() and digest(source) == checksum,
                  "FoundationDB header failed SHA256: " + name)
            copy(source, include / name)
        stage = temporary / "runtime"
        triplet = arch + "-linux-gnu"
        for name in config["files"]:
            if name == "lib/cluster-ctl/libfdb_c.so":
                source = extracted / "usr/lib/libfdb_c.so"
            elif name.startswith("lib/"):
                filename = config["loader"] if name.endswith("/ld.so") else name.rsplit("/", 1)[-1]
                source = extracted / "lib" / triplet / filename
            elif "/foundationdb/" in name:
                source = download(LOCK["foundationdb_license"], cache)
            elif name.endswith("/copyright"):
                package = name.split("/")[-2]
                source = extracted / "usr/share/doc" / package / "copyright"
            else:
                source = SCRIPT_DIR / "native-licenses" / name.rsplit("/", 1)[-1]
            if source.is_symlink():
                check(source.resolve().is_relative_to(extracted), "native package symlink escapes extraction root")
            copy(source, stage / name, 0o755 if name.endswith("/ld.so") else 0o644)
        copy(SCRIPT_DIR / "run-cluster-ctl", stage / "bin/run-cluster-ctl", 0o755)
        (stage / MANIFEST).write_text(manifest(arch))
        for directory in [stage, *[path for path in stage.rglob("*") if path.is_dir()]]:
            directory.chmod(0o755)
        validate(stage, arch, binary=False)
        stage.replace(runtime)
        (temporary / "include").replace(work / "include")
    return runtime, work / "include"


def native_environment(arch, runtime, include):
    check(architecture(platform.machine()) == arch, "native cluster builds require a matching architecture runner")
    lib = runtime / "lib/cluster-ctl"
    return {**os.environ, "GOOS": "linux", "GOARCH": LOCK["architectures"][arch]["goarch"],
            "CGO_ENABLED": "1", "CGO_CFLAGS": "-I" + str(include),
            "CGO_LDFLAGS": "-L" + str(lib) + " -Wl,-rpath-link," + str(lib)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "build", "test", "install", "validate"))
    parser.add_argument("--arch", required=True, type=architecture)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--go", default="go")
    args, flags = parser.parse_known_args()
    arch = args.arch
    if args.command == "validate":
        check(args.root is not None and not flags, "validate requires --root")
        validate(args.root, arch)
        return
    if args.command == "install":
        check(args.root is not None and args.output is not None and not flags, "install requires --root and --output")
        validate(args.root, arch)
        for name in [*LOCK["architectures"][arch]["files"], MANIFEST, "bin/run-cluster-ctl"]:
            copy(args.root / name, args.output / name,
                 0o755 if name in ("bin/run-cluster-ctl", "lib/cluster-ctl/ld.so") else 0o644)
        return
    runtime, include = prepare(arch, ROOT / "build/cluster-native" / arch)
    if args.command == "prepare":
        check(not flags, "unexpected prepare arguments")
        return
    env = native_environment(arch, runtime, include)
    if flags[:1] == ["--"]:
        flags = flags[1:]
    if args.command == "test":
        cluster_file = os.environ.get("FDB_CLUSTER_FILE")
        check(cluster_file and Path(cluster_file).is_file(), "test-cluster-native requires an explicit FDB_CLUSTER_FILE for a test database")
        loader = runtime / "lib/cluster-ctl/ld.so"
        library_path = runtime / "lib/cluster-ctl"
        executor = str(loader) + " --inhibit-cache --library-path " + str(library_path)
        env["CLUSTER_NATIVE_TEST_LOADER"] = str(loader)
        env["CLUSTER_NATIVE_TEST_LIBRARY_PATH"] = str(library_path)
        subprocess.run([args.go, "test", "-tags=fdb", "-exec=" + executor, *flags, "./internal/fdbdriver",
                        "./internal/cluster/metastore", "./internal/storage/sqlengine",
                        "./cmd/cluster-ctl", "./test/e2e/cluster_stub"], cwd=ROOT, env=env, check=True)
        return
    check(args.output is not None, "build requires --output")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bundle = output / "cluster-runtime"
    if bundle.exists():
        shutil.rmtree(bundle)
    shutil.copytree(runtime, bundle)
    subprocess.run([args.go, "build", "-tags=fdb", *flags, "-o", str(bundle / "bin/cluster-ctl"),
                    "./cmd/cluster-ctl"], cwd=ROOT, env=env, check=True)
    validate(bundle, arch)
    # Exercise the bundled loader and full dependency closure on the native
    # runner; version does not connect to a database or start a cluster role.
    smoke_env = {key: value for key, value in os.environ.items() if not key.startswith("LD_")}
    subprocess.run([str(bundle / "bin/run-cluster-ctl"), "version"], env=smoke_env,
                   stdout=subprocess.DEVNULL, check=True, timeout=30)
    for name in ("cluster-ctl", "run-cluster-ctl"):
        link = output / name
        link.unlink(missing_ok=True)
        link.symlink_to("cluster-runtime/bin/" + name)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        sys.exit("cluster-native: " + str(error))
