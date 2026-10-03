"""SSH-only configuration; old control endpoints fail with migration guidance."""
from pathlib import Path
from .store import read


LEGACY = {'master', 'token', 'token_file', 'admin_token', 'admin_token_file',
          'dev_http', 'bind', 'port', 'cert', 'key', 'ca_file', 'fingerprint'}


def reject_legacy(data):
    if LEGACY & data.keys() or any(LEGACY & v.keys() for v in data.get('hosts', {}).values()):
        raise ValueError('HTTP/TLS/token configuration retired: migrate to ssh target, identity_file, known_hosts_file and server-owned forced gateway commands')


def load(path):
    path = Path(path).resolve()
    data = read(path)
    reject_legacy(data)
    for key in ('identity_file', 'known_hosts_file', 'config_file'):
        if key in data.get('ssh', {}):
            p = Path(data['ssh'][key]).expanduser()
            data['ssh'][key] = str(p if p.is_absolute() else path.parent / p)
    return data
