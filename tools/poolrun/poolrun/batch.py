"""One finite submission plan over existing control operations, not a scheduler."""
import base64
from collections import Counter
from pathlib import Path
import zipfile

from .release import verify_package
from .ssh_transport import MAX_PACKAGE, encode
from .store import digest, read


def load_plan(path):
    """Load all local submission bytes before the first remote mutation."""
    path = Path(path).expanduser().resolve(strict=True)
    plan = read(path)
    if (not isinstance(plan, dict)
            or set(plan) != {'schema', 'releases', 'objects', 'tasks'}
            or plan['schema'] != 'poolrun.batch.v1'):
        raise ValueError('batch requires schema=poolrun.batch.v1, releases, objects, tasks')

    def locate(value):
        if not isinstance(value, str) or not value:
            raise ValueError('batch file references must be nonempty paths')
        value = Path(value).expanduser()
        return value if value.is_absolute() else path.parent / value

    operations = []
    for field in ('releases', 'objects'):
        if not isinstance(plan[field], list):
            raise ValueError(f'batch.{field} must be a list of manifest paths')
        seen = set()
        for name in plan[field]:
            source = locate(name)
            value = read(source)
            if not isinstance(value, dict):
                raise ValueError(f'{field} manifest must be a JSON object')
            if field == 'releases':
                with (source.parent / 'code.zip').open('rb') as stream:
                    package = stream.read(MAX_PACKAGE + 1)
                if len(package) > MAX_PACKAGE:
                    raise ValueError('package exceeds limit')
                try:
                    verify_package(value, package)
                except zipfile.BadZipFile as exc:
                    raise ValueError(f'invalid release package: {source.parent / "code.zip"}') from exc
                key = (value['project'], value['release_id'])
                body = dict(op='release', release=value,
                            package=base64.b64encode(package).decode())
            else:
                key = value['id']
                body = dict(op='object', object=value)
            if key in seen:
                raise ValueError(f'duplicate {field} identity in batch: {key}')
            seen.add(key)
            operations.append(body)
    import json
    tasks = [json.loads(line) for line in locate(plan['tasks']).read_text().splitlines()
             if line.strip()]
    if not tasks or not all(isinstance(task, dict) for task in tasks):
        raise ValueError('batch tasks must contain at least one task object')
    ids = [task.get('id', task.get('task_id')) for task in tasks]
    if any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids):
        raise ValueError('batch task identities must be nonempty and unique')
    operations.append(dict(op='submit', tasks=tasks))
    for body in operations:
        body['request_id'] = 'batch-' + digest(body)
        # Apply the existing protocol size limit before registering anything.
        encode(dict(op='admin', body=body, request_id=body['request_id']))
    return operations


async def apply_plan(config, operations, send):
    completed = []
    for body in operations:
        result = await send(config, 'admin', body)
        completed.append(dict(operation=body['op'], request_id=body['request_id'], result=result))
    return dict(status='ACKNOWLEDGED', operations=completed,
                note='Registered and queued only; Agents perform preparation and execution. '
                     'Use status --summary to inspect actual state.')


def summarize(state, project=None):
    """Compact authoritative statuses, without claiming RUNNING means progress."""
    tasks = {k: t for k, t in state.get('tasks', {}).items()
             if project is None or t['spec']['project'] == project}
    attempts = {k: a for k, a in state.get('attempts', {}).items()
                if a['task_id'] in tasks and tasks[a['task_id']].get('attempt_id') == k}
    hosts = {}
    for name, policy in state.get('hosts', {}).items():
        report = state.get('live', {}).get(name, {})
        statuses = Counter(a['status'] for a in attempts.values() if a['host'] == name)
        preparations = [tid for tid, p in state.get('preparations', {}).items()
                        if tid in tasks and p.get('host') == name]
        hosts[name] = dict(slots=policy['slots'], draining=policy.get('draining', False),
                           attempts=dict(sorted(statuses.items())), preparing=preparations,
                           errors=report.get('errors', {}),
                           disk_free=report.get('disk_free'),
                           memory_available=report.get('memory_available'))
    attention = {tid: dict(status=t['status'], reason=t.get('reason'),
                           attempt_id=t.get('attempt_id'))
                 for tid, t in tasks.items() if t['status'] not in ('COMPLETE', 'RUNNING')}
    return dict(project=project, tasks=dict(sorted(Counter(t['status'] for t in tasks.values()).items())),
                hosts=hosts, attention=attention,
                note='RUNNING is controller state, not proof of replay progress. '
                     'Preparation and result upload are not economic replay; '
                     'host errors/resources are last reported observations.')
