"""Bounded JSON lines over system OpenSSH; no TCP listener or RPC framework."""
import asyncio
import contextlib
import ctypes
import json
import os
from pathlib import Path
import re
import shlex
import socket
import stat
import struct
import sys
import uuid

MAX_MESSAGE = 24 * 2**20
MAX_PACKAGE = 16 * 2**20


class ProtocolError(ValueError):
    pass


class RemoteError(ValueError):
    def __init__(self, error):
        self.code = error['code']
        super().__init__(error['message'])


class ConnectionLost(ConnectionError):
    """Execution may have committed: retry exactly the same request identity."""


def encode(value):
    data = json.dumps(value, allow_nan=False, separators=(',', ':')).encode() + b'\n'
    if len(data) > MAX_MESSAGE:
        raise ProtocolError('message exceeds limit')
    return data


def decode(data):
    if not data or not data.endswith(b'\n') or len(data) > MAX_MESSAGE:
        raise ProtocolError('incomplete or oversized JSON line')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ProtocolError('duplicate JSON field')
            result[key] = value
        return result
    try:
        value = json.loads(data, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ProtocolError('nonfinite JSON')))
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ProtocolError('invalid JSON message') from exc
    if not isinstance(value, dict):
        raise ProtocolError('JSON object required')
    return value


def request_message(op, body=None, request_id=None):
    return dict(request_id=request_id or uuid.uuid4().hex, op=op, body=body or {})


def validate_request(message):
    if set(message) != {'request_id', 'op', 'body'}:
        raise ProtocolError('request requires only request_id, op, body')
    if not isinstance(message['request_id'], str) or not 8 <= len(message['request_id']) <= 200:
        raise ProtocolError('stable request_id required')
    if not isinstance(message['op'], str) or not isinstance(message['body'], dict):
        raise ProtocolError('invalid operation or body')


def ssh_argv(config):
    from .config import reject_legacy
    reject_legacy(config)
    cfg = config['ssh']
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*', cfg['target']):
        raise ValueError('ssh.target must be an approved SSH alias')
    args = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=3',
            '-o', 'ConnectTimeout=10', '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no',
            '-o', 'GlobalKnownHostsFile=/dev/null', '-o', 'UpdateHostKeys=no',
            '-o', 'ClearAllForwardings=yes', '-o', 'ControlMaster=no', '-o', 'ControlPath=none']
    for field, flag in [('identity_file', '-i'), ('known_hosts_file', None)]:
        path = str(Path(cfg[field]).expanduser().resolve())
        args += [flag, path] if flag else ['-o', 'UserKnownHostsFile=' + path]
    if cfg.get('config_file'):
        args += ['-F', str(Path(cfg['config_file']).expanduser().resolve())]
    command = cfg.get('remote_argv', ['python3', '-m', 'poolrun', 'ssh-gateway'])
    if not isinstance(command, list) or not command or not all(isinstance(x, str) and '\0' not in x for x in command):
        raise ValueError('administrator remote_argv must be a nonempty string list')
    return args + [cfg['target'], shlex.join(command)]


class Client:
    """One serialized persistent session, bounded admission and finite reconnects."""
    def __init__(self, config):
        from .config import reject_legacy
        reject_legacy(config)
        self.config = config
        self.local_principal = 'admin'
        if 'socket' in config and 'host' in config:
            host = config['host']
            if not isinstance(host, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', host):
                raise ValueError('local agent requires a valid host identity')
            self.local_principal = 'agent:' + host
        self.argv = None if 'socket' in config else ssh_argv(config)
        self.lock = asyncio.Lock()
        self.waiting = 0
        self.process = self.reader = self.writer = self.stderr_task = None
        self.stderr_tail = b''
        self.connections = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def _stderr(self):
        while block := await self.process.stderr.read(8192):
            self.stderr_tail = (self.stderr_tail + block)[-16384:]

    async def connect(self):
        if self.writer is not None:
            return
        if self.argv is None:
            self.reader, self.writer = await asyncio.open_unix_connection(self.config['socket'], limit=MAX_MESSAGE)
        else:
            self.process = await asyncio.create_subprocess_exec(*self.argv, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=MAX_MESSAGE, close_fds=True)
            self.reader, self.writer = self.process.stdout, self.process.stdin
            self.stderr_task = asyncio.create_task(self._stderr())
        self.connections += 1

    async def close(self):
        if self.writer:
            self.writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.writer.wait_closed(), 1)
        if self.process:
            if self.process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), 2)
                except asyncio.TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        self.process.kill()
                    await self.process.wait()
            else:
                await self.process.wait()
        if self.stderr_task:
            self.stderr_task.cancel()
            await asyncio.gather(self.stderr_task, return_exceptions=True)
        self.reader = self.writer = self.process = self.stderr_task = None

    async def request(self, op, body=None, request_id=None):
        message = request_message(op, body, request_id or (body or {}).get('request_id'))
        validate_request(message)
        wire = encode(message if self.argv else {'principal': self.local_principal, 'request': message})
        if self.waiting >= min(32, max(1, self.config.get('max_pending_requests', 32))):
            raise ConnectionLost('bounded control queue full; retain request for retry')
        self.waiting += 1
        try:
            async with self.lock:
                for attempt in range(4):
                    try:
                        async with asyncio.timeout(self.config.get('request_timeout', 30)):
                            await self.connect()
                            self.writer.write(wire)
                            await self.writer.drain()
                            response = decode(await self.reader.readline())
                            if response.get('request_id') != message['request_id'] or type(response.get('ok')) is not bool:
                                raise ProtocolError('mismatched response')
                            if response['ok']:
                                if 'result' not in response:
                                    raise ProtocolError('missing result')
                                return response['result']
                            error = response.get('error')
                            if not isinstance(error, dict) or not {'code', 'message'} <= error.keys():
                                raise ProtocolError('invalid error response')
                            raise RemoteError(error)
                    except RemoteError:
                        raise
                    except (OSError, ValueError, asyncio.TimeoutError) as exc:
                        await self.close()
                        if attempt == 3:
                            detail = self.stderr_tail[-2000:].decode('utf-8', errors='replace')
                            raise ConnectionLost('SSH/socket unavailable or invalid protocol; outcome unknown: ' + str(exc) + '; stderr: ' + detail) from exc
                        await asyncio.sleep(min(4, .25 * 2**attempt))
                    except asyncio.CancelledError:
                        await self.close()
                        raise
        finally:
            self.waiting -= 1


def peer_uid(sock):
    if sys.platform.startswith('linux'):
        return struct.unpack('3i', sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]
    uid, gid = ctypes.c_uint(), ctypes.c_uint()
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.getpeereid(sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
        raise PermissionError('cannot authenticate Unix socket peer')
    return uid.value


async def serve_socket(master, path):
    """Store already owns the single-writer lock before touching the socket."""
    path = Path(path).absolute()
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = parent.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o007:
        raise PermissionError('socket directory must be owner-controlled, not world accessible')
    if path.exists() or path.is_symlink():
        if not stat.S_ISSOCK(path.lstat().st_mode):
            raise PermissionError('refusing to remove non-socket')
        # Also reject a live socket belonging to a differently configured root.
        with socket.socket(socket.AF_UNIX) as probe:
            try:
                probe.connect(str(path))
            except ConnectionRefusedError:
                pass
            else:
                raise PermissionError('another resident master owns this socket')
        path.unlink()
    clients = set()
    async def session(reader, writer):
        task = asyncio.current_task()
        clients.add(task)
        try:
            uid = peer_uid(writer.get_extra_info('socket'))
            allowed = master.config.get('gateway_uids', {}).get(str(uid), [])
            if uid != os.getuid() and not allowed:
                raise PermissionError('unapproved local account')
            if len(clients) > 64:
                raise ConnectionError('control session limit')
            while True:
                line = await reader.readline()
                if not line:
                    break
                envelope = decode(line)
                principal, message = envelope['principal'], envelope['request']
                if uid != os.getuid() and principal not in allowed:
                    raise PermissionError('principal not approved for local UID')
                response = dispatch(master, principal, message)
                writer.write(encode(response))
                await writer.drain()
        except (OSError, ValueError, KeyError, ConnectionError) as exc:
            print('control session closed:', type(exc).__name__, file=sys.stderr)
        finally:
            clients.discard(task)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
    server = await asyncio.start_unix_server(session, path=str(path), limit=MAX_MESSAGE)
    os.chmod(path, 0o660 if master.config.get('gateway_uids') else 0o600)
    if master.config.get('socket_gid') is not None:
        os.chown(path, -1, master.config['socket_gid'])
    try:
        # start_unix_server already accepts. serve_forever() waits for clients
        # on cancellation in Python 3.12, before our cleanup can close them.
        await asyncio.Future()
    finally:
        server.close()
        for task in list(clients):
            task.cancel()
        await asyncio.gather(*clients, return_exceptions=True)
        await server.wait_closed()
        path.unlink(missing_ok=True)


def dispatch(master, principal, message):
    rid = message.get('request_id') if isinstance(message, dict) else None
    try:
        validate_request(message)
        result = master.handle(principal, message['op'], message['body'], rid)
        response = dict(request_id=rid, ok=True, result=result)
        try:
            encode(response)  # Never emit half of an oversized response.
        except ProtocolError:
            return dict(request_id=rid, ok=False, error=dict(code='RESPONSE_TOO_LARGE',
                        message='response exceeds limit; mutation may have committed, retain request ID'))
        return response
    except Exception as exc:
        code = 'FORBIDDEN' if isinstance(exc, PermissionError) else 'INVALID_REQUEST' if isinstance(exc, (ValueError, KeyError)) else 'STORAGE_OR_INTERNAL'
        print('request failed:', type(exc).__name__, str(exc), file=sys.stderr)
        return dict(request_id=rid, ok=False, error=dict(code=code, message=str(exc)))


def gateway(path, principal):
    """Forced-command parameters are server-owned; SSH_ORIGINAL_COMMAND is ignored."""
    if principal != 'admin' and not re.fullmatch(r'agent:[A-Za-z0-9_.-]+', principal):
        raise ValueError('invalid server principal')
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(35)
        sock.connect(path)
        with sock.makefile('rb') as replies:
            while True:
                line = sys.stdin.buffer.readline(MAX_MESSAGE + 1)
                if not line:
                    return
                message = decode(line)
                validate_request(message)
                sock.sendall(encode({'principal': principal, 'request': message}))
                response = decode(replies.readline(MAX_MESSAGE + 1))
                sys.stdout.buffer.write(encode(response))
                sys.stdout.buffer.flush()
