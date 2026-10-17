"""Real isolated loopback OpenSSH. Never touches system config or owner keys."""
import getpass
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import time


class Unavailable(RuntimeError):
    pass


class SSHLab:
    def __init__(self, root, master_socket):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.master_socket = str(master_socket)
        self.process = None
        self.log = None
        sshd = shutil.which('sshd') or '/usr/sbin/sshd'
        if not Path(sshd).exists() or not shutil.which('ssh-keygen'):
            raise Unavailable('OpenSSH server/keygen unavailable')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.keys = {}
        def key(name):
            path = self.root / name
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(path)], check=True)
            return path
        host = key('hostkey')
        public = host.with_suffix('.pub').read_text().split()
        self.known = self.root / 'known_hosts'
        self.known.write_text(f'[127.0.0.1]:{self.port} {public[0]} {public[1]}\n')
        lines = []
        for principal in ('admin', 'agent:a', 'agent:b', 'agent:c', 'unauthorized'):
            path = key(principal.replace(':', '-'))
            self.keys[principal] = path
            if principal == 'unauthorized':
                continue
            command = shlex.join(['/usr/bin/env', 'PYTHONPATH=' + os.environ.get('PYTHONPATH', ''),
                sys.executable, '-m', 'poolrun', 'ssh-gateway', '--socket', self.master_socket, '--principal', principal])
            quoted = command.replace('\\', '\\\\').replace('"', '\\"')
            lines.append(f'restrict,command="{quoted}" ' + path.with_suffix('.pub').read_text().strip())
        auth = self.root / 'authorized_keys'
        auth.write_text('\n'.join(lines) + '\n')
        auth.chmod(0o600)
        self.aliases = self.root / 'ssh_config'
        self.aliases.write_text(f'Host poolrun-test\n HostName 127.0.0.1\n Port {self.port}\n User {getpass.getuser()}\n IdentityAgent none\n')
        config = self.root / 'sshd_config'
        config.write_text(f'''Port {self.port}
ListenAddress 127.0.0.1
HostKey {host}
PidFile {self.root / 'sshd.pid'}
AuthorizedKeysFile {auth}
StrictModes yes
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
PermitRootLogin prohibit-password
AllowUsers {getpass.getuser()}
AllowTcpForwarding no
AllowAgentForwarding no
PermitTunnel no
X11Forwarding no
PermitTTY no
LogLevel VERBOSE
''')
        self.log = (self.root / 'sshd.log').open('ab')
        self.process = subprocess.Popen([sshd, '-D', '-e', '-f', str(config)], stdout=self.log, stderr=self.log)
        for _ in range(40):
            if self.process.poll() is not None:
                reason = (self.root / 'sshd.log').read_text()[-1500:]
                self.close()
                raise Unavailable('isolated sshd refused startup: ' + reason)
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.1):
                    break
            except OSError:
                time.sleep(.05)
        else:
            self.close()
            raise Unavailable('isolated sshd did not listen')

    def config(self, principal):
        return {'ssh': {'target': 'poolrun-test', 'identity_file': str(self.keys[principal]),
                        'known_hosts_file': str(self.known), 'config_file': str(self.aliases)}, 'request_timeout': 4}

    def close(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
