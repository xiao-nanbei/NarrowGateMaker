"""Read-only discovery of externally managed prepared caches.

An inventory is a locality hint, not admission: the business cache reader still
validates its original contract. No copying, cache deletion or build occurs here.
"""
import json
from pathlib import Path
import time
from .store import digest


def scan(roots):
    result = {}
    for namespace, directory in roots.items():
        row = {"complete": False, "ready": [], "errors": []}
        result[namespace] = row
        try:
            root = Path(directory).expanduser()
            if root.is_symlink() or not root.is_dir():
                raise ValueError("cache root unavailable")
            for child in sorted(root.iterdir()):
                if not child.is_dir() or child.is_symlink() or '.part-' in child.name:
                    continue
                manifest = child / 'manifest.json'
                if not manifest.is_file():
                    continue
                try:
                    if manifest.is_symlink() or manifest.stat().st_size > 8*1024*1024:
                        raise ValueError('invalid manifest')
                    data = json.loads(manifest.read_text())
                    identity, files = data['identity'], data['files']
                    if not isinstance(identity, dict) or not identity or not isinstance(files, dict) or not files:
                        raise ValueError('missing identity or file inventory')
                    for name, size in files.items():
                        path = child / name
                        if (Path(name).is_absolute() or '..' in Path(name).parts
                                or path.is_symlink() or not path.resolve().is_relative_to(child.resolve())
                                or type(size) is not int or size < 0 or path.stat().st_size != size):
                            raise ValueError('incomplete cache file')
                    row['ready'].append(digest(identity))
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    row['errors'].append(child.name + ': ' + str(exc))
            row['ready'] = sorted(set(row['ready']))
            row['complete'] = True
        except (OSError, ValueError) as exc:
            row['errors'].append(str(exc))
    return result


def locality_reason(state, task, host, reports, seen, now, started_at, wait_seconds=30):
    hint = task['spec'].get('cache_affinity')
    if not hint:
        return None
    namespace, identity = hint['namespace'], digest(hint['identity'])
    spec = task['spec']
    allowed_hosts = task.get('placement_hosts', spec.get('hosts'))
    release = state['releases'].get(spec['project']+'/'+task['code_policy']['release'], {})
    candidates = []
    for name, policy in state['hosts'].items():
        if (policy.get('draining') or now >= policy.get('stop_accepting_at', float('inf'))
                or name in task['avoid_hosts'] or (allowed_hosts and name not in allowed_hosts)
                or (task.get('resume_host') and name != task['resume_host'])
                or not set(spec.get('capabilities', [])).issubset(policy.get('capabilities', []))
                or policy['platform'] not in release.get('platforms', [])):
            continue
        need = dict(spec['resources'], **task.get('resource_override', {}))
        if any(need[k] > policy[k] for k in ('cpus', 'disk_bytes', 'memory_bytes')
               if k != 'memory_bytes' or policy.get('memory_admission', True)):
            continue
        candidates.append(name)
    ready, unknown = [], []
    for name in candidates:
        report = reports.get(name, {})
        inventory = report.get('cache_inventory', {}).get(namespace, {})
        fresh = now-seen.get(name, 0) < 30 and report.get('cache_inventory_age_seconds', float('inf')) < 60
        if not fresh or not inventory.get('complete'):
            unknown.append(name)
        elif identity in inventory.get('ready', []):
            ready.append(name)
    if host in unknown:
        return 'WAIT_CACHE_INVENTORY: local scan unavailable'
    if unknown and now-max(started_at, task['submitted']) < wait_seconds:
        return 'WAIT_CACHE_INVENTORY: '+','.join(sorted(unknown))
    # A busy warm host keeps its task affinity; other hosts can take other work.
    if ready:
        return None if host in ready else 'WAIT_CACHE_HOST: '+','.join(sorted(ready))
    return None


class Inventory:
    def __init__(self, roots):
        self.roots = roots
        self.checked = None
        self.value = {}

    def report(self):
        now = time.monotonic()
        if self.checked is None or now-self.checked >= 30:
            self.value = scan(self.roots)
            self.checked = time.monotonic()
        return self.value, time.monotonic()-self.checked
