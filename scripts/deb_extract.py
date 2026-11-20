"""Read pinned Debian assets without dpkg, installation, or privileged writes.

Only a caller-supplied SHA256 authenticates an input. Archive links are resolved
inside the archive and copied as regular files; no host symlink is followed.
Compatible with Python 3.9, including RPM-based native build runners.
"""

import hashlib
import io
from pathlib import Path, PurePosixPath
import posixpath
import tarfile


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def _name(name):
    while name.startswith("./"):
        name = name[2:]
    name = name.rstrip("/")
    if name in ("", "."):
        return ""
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or str(path) != name:
        raise ValueError("unsafe Debian archive path: " + name)
    return name


def extract_deb(archive, destination, expected_sha256):
    archive, destination = Path(archive), Path(destination)
    if archive.is_symlink() or sha256(archive) != expected_sha256:
        raise ValueError("Debian input failed SHA256: " + str(archive))
    data = None
    with archive.open("rb") as stream:
        if stream.read(8) != b"!<arch>\n":
            raise ValueError("invalid Debian ar archive")
        seen = set()
        while True:
            header = stream.read(60)
            if not header:
                break
            if len(header) != 60 or header[58:] != b"`\n":
                raise ValueError("invalid Debian ar member")
            name = header[:16].decode("ascii").strip().rstrip("/")
            size = int(header[48:58].decode("ascii").strip())
            if size < 0 or name in seen or "/" in name or not name:
                raise ValueError("invalid or duplicate Debian ar member")
            seen.add(name)
            payload = stream.read(size)
            if len(payload) != size:
                raise ValueError("truncated Debian ar member")
            if size % 2 and stream.read(1) != b"\n":
                raise ValueError("invalid Debian ar padding")
            if name == "debian-binary" and payload != b"2.0\n":
                raise ValueError("unsupported Debian package version")
            if name.startswith("data.tar"):
                if data is not None or name not in ("data.tar", "data.tar.gz", "data.tar.xz", "data.tar.bz2"):
                    raise ValueError("unsupported Debian data archive")
                data = payload
        if "debian-binary" not in seen or data is None:
            raise ValueError("Debian package has no version or data member")
    if destination.is_symlink():
        raise ValueError("Debian extraction root must not be a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tar:
        members = {}
        for member in tar.getmembers():
            name = _name(member.name)
            if not name:
                if not member.isdir():
                    raise ValueError("Debian archive root is not a directory")
                continue
            if name in members or not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise ValueError("unsupported or duplicate Debian payload: " + name)
            members[name] = member

        def regular(name, chain):
            if name in chain or name not in members:
                raise ValueError("unresolved Debian archive link: " + name)
            member = members[name]
            if member.isfile():
                return member
            if not (member.issym() or member.islnk()):
                raise ValueError("Debian archive link does not name a regular file: " + name)
            target = member.linkname
            if member.issym() and not target.startswith("/"):
                target = posixpath.join(posixpath.dirname(name), target)
            target = _name(posixpath.normpath(target.lstrip("/")))
            return regular(target, chain | {name})

        for name, member in members.items():
            path = root / name
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ValueError("Debian extraction path escapes its root: " + name)
            if member.isdir():
                path.mkdir(parents=True, exist_ok=True)
                path.chmod(0o755)
                continue
            source = regular(name, set())
            path.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(source) as incoming, path.open("wb") as outgoing:
                while True:
                    chunk = incoming.read(1 << 20)
                    if not chunk:
                        break
                    outgoing.write(chunk)
            path.chmod(0o755 if source.mode & 0o111 else 0o644)
