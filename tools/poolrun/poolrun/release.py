"""Immutable whitelisted source exports, no working-tree execution."""
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
import zipfile
from .store import atomic, digest, read


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", value):
        raise ValueError("invalid identifier")
    return value


def relative(value):
    p = PurePosixPath(value)
    if p.is_absolute() or not p.parts or any(x in ("..", ".") for x in p.parts) or "\\" in value:
        raise ValueError("unsafe relative path")
    return str(p)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def create(project, root, include, release_id, contract, destination):
    identifier(project); identifier(release_id)
    root = Path(root).resolve()
    names = [relative(x.strip()) for x in Path(include).read_text().splitlines() if x.strip() and not x.startswith("#")]
    banned = {".git", ".venv", ".private_artifacts", "__pycache__", "logs", ".env", "credentials"}
    contents, metadata = {}, {}
    for name in names:
        path = root / name
        if banned.intersection(Path(name).parts) or path.suffix in (".pem", ".key"):
            raise ValueError("private/runtime file not allowed in code export")
        if path.is_symlink() or root not in path.resolve().parents or not path.is_file():
            raise ValueError("export must contain regular in-tree files")
        data = path.read_bytes()
        contents[name] = data
        metadata[name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    if sum(len(x) for x in contents.values()) > 16 * 2**20:
        raise ValueError("code package exceeds16MiB; use approved object transport")
    for name, data in contents.items():
        if (root / name).read_bytes() != data:
            raise ValueError("source changed during export")
    descriptor = dict(contract, project=project, release_id=release_id, files=metadata)
    descriptor["content_id"] = digest(descriptor)
    target = Path(destination) / release_id
    if target.exists():
        if read(target / "release.json") != descriptor:
            raise ValueError("immutable release ID conflict")
        return target / "release.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=".release-", dir=target.parent))
    try:
        with zipfile.ZipFile(tmp / "code.zip", "w", zipfile.ZIP_DEFLATED) as archive:
            for name, data in contents.items():
                archive.writestr(name, data)
        atomic(tmp / "release.json", descriptor)
        os.rename(tmp, target)
    finally:
        if tmp.exists():
            shutil.rmtree(tmp)
    return target / "release.json"


def verify_package(descriptor, data):
    expected = dict(descriptor)
    content_id = expected.pop("content_id")
    if digest(expected) != content_id:
        raise ValueError("release descriptor digest mismatch")
    if len(data) > 16 * 2**20:
        raise ValueError("oversized package")
    archive = zipfile.ZipFile(io.BytesIO(data))
    if len(set(archive.namelist())) != len(archive.namelist()) or set(archive.namelist()) != set(descriptor["files"]):
        raise ValueError("package file inventory mismatch")
    total = 0
    for info in archive.infolist():
        relative(info.filename)
        if stat.S_ISLNK(info.external_attr >> 16):
            raise ValueError("symlinks not allowed")
        expected_file = descriptor["files"][info.filename]
        total += info.file_size
        if total > 16 * 2**20 or info.file_size != expected_file["size"]:
            raise ValueError("expanded package size mismatch")
        if hashlib.sha256(archive.read(info)).hexdigest() != expected_file["sha256"]:
            raise ValueError("package hash mismatch")
    return archive


def prepare(host_root, descriptor, package):
    archive = verify_package(descriptor, package)
    target = Path(host_root) / "releases" / identifier(descriptor["project"]) / identifier(descriptor["release_id"])
    if target.exists():
        if read(target / ".poolrun.json") != descriptor:
            raise ValueError("existing release differs")
        return str(target.resolve())
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=".incoming-", dir=target.parent))
    try:
        for name in archive.namelist():
            f = tmp / name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(archive.read(name))
            f.chmod(0o444)
        atomic(tmp / ".poolrun.json", descriptor)
        (tmp / ".poolrun.json").chmod(0o444)
        for directory in sorted(tmp.rglob("*"), reverse=True):
            if directory.is_dir():
                directory.chmod(0o555)
        tmp.chmod(0o555)
        os.rename(tmp, target)
    finally:
        if tmp.exists():
            tmp.chmod(0o700)
            for d in tmp.rglob("*"):
                if d.is_dir(): d.chmod(0o700)
            shutil.rmtree(tmp)
    return str(target.resolve())


def expand(argv, paths):
    result = []
    for value in argv:
        def substitute(match):
            key = match.group(1)
            if key not in paths:
                raise ValueError("unknown placeholder: " + key)
            return str(paths[key])
        result.append(re.sub(r"\$\{([^}]+)\}", substitute, value))
    return result
