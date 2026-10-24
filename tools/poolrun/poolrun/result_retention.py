"""Retire only acknowledged publication copies, never business recovery data."""
import os
from pathlib import Path
import re
import stat
import subprocess

import psutil

from .release import sha
from .store import FileLock, atomic, digest, read


def physical(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute physical result path required')
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('linked result path')
    return path


def manifest(files):
    if not isinstance(files, list) or not files:
        raise ValueError('nonempty result manifest required')
    names = set()
    for item in files:
        name = item['file']
        path = Path(name)
        if (not name or path.is_absolute() or '..' in path.parts
                or path.as_posix() != name or name == '.' or '\\' in name
                or name in names or type(item['size']) is not int or item['size'] < 0
                or not re.fullmatch('[0-9a-f]{64}', item['sha256'])):
            raise ValueError('invalid result manifest')
        names.add(name)
    return files


def fingerprint(path):
    s = path.stat()
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def verify(path, item):
    physical(path)
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError('result must be a regular file')
    before = fingerprint(path)
    if before[2] != item['size'] or sha(path) != item['sha256']:
        raise ValueError('result payload differs: ' + item['file'])
    if fingerprint(path) != before:
        raise ValueError('result changed during verification')
    return before


def no_open_files(source):
    check = subprocess.run(['lsof', '-nP', '-Fpn', '+D', str(source)],
                           capture_output=True, text=True, timeout=30)
    if check.returncode != 1 or check.stdout or check.stderr:
        raise ValueError('publication files open or open-file check unavailable')


def retire(agent_root, aid, entry, policy):
    """An explicit policy declares outputs/ a private transport staging area.

    Downstream/pinned/recovery readers must use the durable URI, not outputs/.
    Only new positively accepted COMPLETE events enter this path. Missing old
    ACK evidence is not inferred from an exit file or a historical acked ID.
    """
    root = physical(entry['directory'])
    if root.name != aid or root.parent.parent != physical(agent_root)/'attempts':
        raise ValueError('foreign Attempt directory')
    with FileLock(root/'result-retirement.lock'):
        attempt = read(root/'attempt.json')
        receipt = read(root/'exit.json')
        saved = read(root/'durable.json')
        event = dict(receipt, kind='COMPLETE', attempt_id=aid, durable_receipt=saved)
        if (attempt['id'] != aid or receipt.get('kind') != 'RESULT_PENDING'
                or receipt.get('validated') is not True or receipt.get('tree_exited') is not True
                or digest(event) != entry['result_retirement']['event_sha256']):
            raise ValueError('accepted completion binding differs')
        files = manifest(receipt['result']['files'])
        if saved.get('durable') is not True or saved.get('files') != files:
            raise ValueError('durable manifest differs')
        source, target = physical(root/'outputs'), physical(saved['uri'])
        allowed = [physical(p) for p in policy['roots']]
        if not any(p.is_dir() and target.is_relative_to(p) for p in allowed):
            raise ValueError('durable location outside approved roots')
        if target.is_relative_to(root) or root.is_relative_to(target):
            raise ValueError('durable location overlaps Attempt')
        # A still-existing or permission-inaccessible PID fails closed, including
        # a reused PID. No process is killed or restarted by retention.
        for name in ('process.json', 'runner.json'):
            if psutil.pid_exists(read(root/name)['pid']):
                raise ValueError('worker/supervisor PID still present')
        retained = {f['file']: verify(target/f['file'], f) for f in files}
        copies = {}
        for f in files:
            path = source/f['file']
            physical(path)
            if path.exists():
                copies[f['file']] = verify(path, f)
        no_open_files(source)
        removed = 0
        for f in files:
            name = f['file']
            physical(target/name)
            if fingerprint(target/name) != retained[name]:
                raise ValueError('retained result changed before retirement')
            if name in copies:
                path = physical(source/name)
                if fingerprint(path) != copies[name]:
                    raise ValueError('publication changed before retirement')
                path.unlink()
                fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                removed += f['size']
        result = {'status': 'retired', 'uri': str(target), 'manifest_sha256': digest(files),
                  'logical_bytes_removed_this_pass': removed,
                  'publication_bytes': sum(f['size'] for f in files)}
        atomic(root/'result-retirement.json', result)
        return result
