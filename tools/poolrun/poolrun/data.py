"""Content addressed files, verified atomic publication and conservative GC."""
import os
from pathlib import Path
import uuid
from .store import FileLock, atomic, read
from .release import sha


def publish_bytes(path, data):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
    try:
        with part.open("xb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(part, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally: part.unlink(missing_ok=True)


class Cache:
    def __init__(self, root):
        self.root = Path(root).resolve() / "cache"
        (self.root / "blobs").mkdir(parents=True, exist_ok=True)

    def path(self, oid):
        if len(oid) != 64 or any(c not in "0123456789abcdef" for c in oid): raise ValueError("invalid content hash")
        p = self.root / "blobs" / oid
        if p.is_symlink(): raise ValueError("cache symlink refused")
        return p

    def ready(self):
        result=[]
        for p in (self.root/"blobs").iterdir():
            if not p.is_file() or p.is_symlink() or len(p.name)!=64:continue
            try:
                marker=read(p.with_suffix(".ready"));stat=p.stat()
                if marker.get("size")==stat.st_size and marker.get("mtime_ns")==stat.st_mtime_ns:result.append(p.name)
            except (OSError,ValueError):pass
        return result

    def publish(self, oid, part, size):
        with FileLock(self.root / "cache.lock"):
            p = self.path(oid)
            part = Path(part)
            if part.is_symlink() or not part.is_file() or part.stat().st_size != size or sha(part) != oid: raise ValueError("input checksum/size mismatch")
            # fsync downloaded bytes before durable READY marker.
            with part.open("rb") as f: os.fsync(f.fileno())
            if p.exists() and (p.stat().st_size!=size or sha(p)!=oid):
                os.rename(p,p.with_name(oid+".corrupt."+uuid.uuid4().hex))
            if not p.exists(): os.replace(part, p)
            else: part.unlink()
            p.chmod(0o444)
            atomic(p.with_suffix(".ready"), {"sha256": oid, "size": size,"mtime_ns":p.stat().st_mtime_ns})

    def gc(self, protected, reconstructible):
        deleted = []
        with FileLock(self.root / "cache.lock"):
            for oid in set(reconstructible) - set(protected):
                p = self.path(oid)
                if p.exists(): p.unlink(); p.with_suffix(".ready").unlink(missing_ok=True); deleted.append(oid)
        return deleted
