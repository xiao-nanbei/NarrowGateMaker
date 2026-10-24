"""Protocol/socket units and explicitly separated real OpenSSH acceptance."""
import asyncio
from pathlib import Path
import sys
import tempfile
from contextlib import asynccontextmanager

import pytest

from poolrun.master import Master
from poolrun.ssh_transport import (Client, ConnectionLost, ProtocolError, RemoteError,
    decode, dispatch, encode, request_message, serve_socket, ssh_argv, MAX_MESSAGE)
from poolrun.store import read
from test_core import state, task, report
from ssh_lab import SSHLab, Unavailable


def make_master(root):
    master = Master(root, {'hosts': {h:dict(state()['hosts']['h']) for h in ('a','b','c')}})
    master.store.transition(None, lambda s: s['releases'].update(state()['releases']))
    return master


@asynccontextmanager
async def resident(root, real_ssh=False):
    with tempfile.TemporaryDirectory(prefix='prs-', dir='/tmp') as short:
        path = Path(short)/'master.sock'
        master = make_master(root/'control')
        server = asyncio.create_task(serve_socket(master, path))
        lab = None
        try:
            for _ in range(100):
                if path.exists(): break
                await asyncio.sleep(.01)
            assert path.exists()
            if real_ssh:
                try: lab = SSHLab(root/'ssh', path)
                except Unavailable as exc: pytest.skip(str(exc))
            yield master, path, lab
        finally:
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)
            master.store.close()
            if lab: lab.close()


@pytest.mark.parametrize('line', [b'', b'{}', b'hello\n', b'[]\n', b'{"a":1,"a":2}\n', b'{"n":NaN}\n', b'\xff\n'])
def test_invalid_lines(line):
    with pytest.raises(ProtocolError): decode(line)


def test_local_agent_keeps_agent_permissions(tmp_path):
    async def run():
        async with resident(tmp_path) as (master, path, _):
            async with Client({'socket': str(path), 'host': 'a'}) as client:
                reply = await client.request('poll', {'host': 'a', 'report': report()})
                assert isinstance(reply, dict)
                with pytest.raises(RemoteError) as exc:
                    await client.request('admin', {'op': 'hold', 'project': 'p'})
                assert exc.value.code == 'FORBIDDEN'
                assert master.store.state['holds'] == []
    asyncio.run(run())


def test_size_limit_and_ssh_argv():
    with pytest.raises(ProtocolError): encode({'data':'x'*MAX_MESSAGE})
    cfg={'ssh':{'target':'approved-alias','identity_file':'/unused/key','known_hosts_file':'/unused/known',
                'remote_argv':['/path with spaces/python','-m','poolrun','ssh-gateway']}}
    argv=ssh_argv(cfg)
    assert '-T' in argv and 'StrictHostKeyChecking=yes' in argv
    assert not {'-N','-n','-L','-R','-D','-t'} & set(argv)
    assert argv[-1].startswith("'/path with spaces/python'")
    cfg['ssh']['target']='-oProxyCommand=bad'
    with pytest.raises(ValueError): ssh_argv(cfg)


def test_permissions_persistence_fences_and_response_replay(tmp_path, monkeypatch):
    m=make_master(tmp_path)
    def call(principal, op, body, rid='request-0001'):
        return dispatch(m,principal,request_message(op,body,rid))
    try:
        for op in ('admin','status','backup','snapshot'):
            assert call('agent:a',op,{'op':'submit','tasks':[task()]})['error']['code']=='FORBIDDEN'
        for body in ({'host':'b','report':report()},{'role':'admin','report':report()}):
            assert call('agent:a','poll',body)['error']['code']=='FORBIDDEN'
        assert call('admin','admin',{'op':'submit','tasks':[task()]})['ok']
        call('agent:a','poll',{'report':report()})
        first=call('agent:a','start',{'task_id':'t'})
        assert first['ok'];aid=first['result']['id']
        m.seen.clear()  # Old successful START must replay even without a fresh heartbeat.
        assert call('agent:a','start',{'task_id':'t'})==first
        assert len(m.store.state['attempts'])==1
        assert not call('agent:a','start',{'task_id':'different'})['ok']
        event={'attempt_id':aid,'kind':'COMPLETE','tree_exited':True,'validated':True,
               'durable_receipt':{'durable':True},'result':{'files':[]}}
        answer=call('agent:a','event',{'event':event},'result-0001')
        assert answer['ok']
        assert call('agent:a','event',{'event':event},'result-0001')==answer
        event['result']={'files':['conflicting']}
        assert not call('agent:a','event',{'event':event},'result-0002')['ok']
        monkeypatch.setattr('poolrun.store.atomic',lambda *a:(_ for _ in ()).throw(OSError('fsync failed')))
        assert call('admin','admin',{'op':'drain','host':'a'},'drain-0001')['error']['code']=='STORAGE_OR_INTERNAL'
        assert not m.store.state['hosts']['a']['draining']
    finally: m.store.close()


def test_socket_partial_multiline_limit_and_single_writer(tmp_path):
    async def run():
        async with resident(tmp_path) as (master,path,_):
            assert path.stat().st_mode & 0o777 == 0o600
            with pytest.raises(BlockingIOError): make_master(tmp_path/'control')
            assert path.exists()
            other=make_master(tmp_path/'other')
            try:
                with pytest.raises(PermissionError,match='another resident'):
                    await serve_socket(other,path)
            finally:other.store.close()
            r,w=await asyncio.open_unix_connection(str(path),limit=MAX_MESSAGE)
            message=encode({'principal':'admin','request':request_message('status')})
            w.write(message[:7]);await w.drain();await asyncio.sleep(.02)
            assert not r._buffer
            w.write(message[7:]+message);await w.drain()
            assert decode(await r.readline())['ok']
            assert decode(await r.readline())['ok']
            w.write(b'invalid text\n');await w.drain()
            assert await r.read()==b''
            w.close();await w.wait_closed()
            r,w=await asyncio.open_unix_connection(str(path))
            w.write(b'x'*(MAX_MESSAGE+1));await w.drain()
            assert await r.read()==b''
            w.close();await w.wait_closed()
            # Only a Unix listening socket is created by the Master.
            import psutil
            assert not [c for c in psutil.Process().net_connections(kind='inet') if c.status=='LISTEN']
    asyncio.run(run())


def test_real_ssh_identity_persistent_session_and_lost_replies(tmp_path):
    async def run():
        async with resident(tmp_path,True) as (m,path,lab):
            async with Client(lab.config('admin')) as admin, Client(lab.config('agent:a')) as agent:
                await admin.request('admin',{'op':'submit','tasks':[task()]})
                await agent.request('poll',{'report':report()})
                pid=agent.process.pid
                # Send without consuming the response: the commit is real and durable.
                req=request_message('start',{'task_id':'t'},'lost-start-0001')
                agent.writer.write(encode(req));await agent.writer.drain()
                for _ in range(200):
                    if m.store.state['attempts']:break
                    await asyncio.sleep(.01)
                assert len(m.store.state['attempts'])==1
                aid=next(iter(m.store.state['attempts']))
                await agent.close()
                answer=await agent.request('start',req['body'],req['request_id'])
                assert answer['id']==aid and agent.process.pid!=pid
                event={'attempt_id':aid,'kind':'COMPLETE','tree_exited':True,'validated':True,
                       'durable_receipt':{'saved':True},'result':{'files':[]}}
                req=request_message('event',{'event':event},'lost-result-0001')
                agent.writer.write(encode(req));await agent.writer.drain()
                for _ in range(200):
                    if m.store.state['tasks']['t']['status']=='COMPLETE':break
                    await asyncio.sleep(.01)
                assert read(m.store.path)['tasks']['t']['status']=='COMPLETE'
                await agent.close()
                assert (await agent.request('event',req['body'],req['request_id']))['accepted']
                pid=agent.process.pid;connections=agent.connections
                for _ in range(5): await agent.request('poll',{'report':dict(report(),attempts=[aid])})
                await admin.request('admin',{'op':'submit','tasks':[task('t2')]})
                await agent.request('poll',{'report':dict(report(),attempts=[aid])})
                second=await agent.request('start',{'task_id':'t2'})
                await agent.request('event',{'event':dict(event,attempt_id=second['id'])})
                assert agent.process.pid==pid and agent.connections==connections
                for op,body in [('admin',{'op':'drain','host':'b'}),('poll',{'host':'b','report':report()}),('snapshot',{})]:
                    with pytest.raises(RemoteError) as exc:await agent.request(op,body)
                    assert exc.value.code=='FORBIDDEN'
            # The key's forced command wins over the supplied SSH remote command.
            config=lab.config('admin');marker=tmp_path/'must-not-exist'
            config['ssh']['remote_argv']=['touch',str(marker)]
            async with Client(config) as client:assert 'tasks' in await client.request('status')
            assert not marker.exists()
            async with Client(lab.config('unauthorized')) as client:
                with pytest.raises(ConnectionLost):await client.request('status')
            bad=tmp_path/'wrong_known_hosts'
            bad.write_text(lab.known.read_text().replace(lab.known.read_text().split()[2],lab.keys['unauthorized'].with_suffix('.pub').read_text().split()[1]))
            config=lab.config('admin');config['ssh']['known_hosts_file']=str(bad)
            async with Client(config) as client:
                with pytest.raises(ConnectionLost):await client.request('status')
    asyncio.run(run())


@pytest.mark.parametrize('output', ['noise', 'partial', 'oversize', 'stderr'])
def test_pipe_fault_units_not_ssh_acceptance(output):
    async def run():
        config={'ssh':{'target':'unused','identity_file':'/unused','known_hosts_file':'/unused'},'request_timeout':1}
        client=Client(config)
        code="import sys,json; m=json.loads(sys.stdin.readline()); "
        if output=='noise':code+="print('shell banner')"
        elif output=='partial':code+="sys.stdout.write('{');sys.stdout.flush()"
        elif output=='oversize':code+=f"print('x'*{MAX_MESSAGE+1})"
        else:code+="sys.stderr.write('e'*2000000);sys.stderr.flush(); print(json.dumps({'request_id':m['request_id'],'ok':True,'result':{}}))"
        client.argv=[sys.executable,'-c',code]  # Explicit subprocess fault-injection unit only.
        async with client:
            if output=='stderr':
                assert await client.request('status')=={}
                assert len(client.stderr_tail)<=16384
            else:
                with pytest.raises(ConnectionLost):await client.request('status')
            assert client.connections<=4
        assert client.process is None
    asyncio.run(run())


def test_pending_start_survives_agent_restart(tmp_path):
    from poolrun.agent import Agent
    config={'root':str(tmp_path),'host':'a','poll_seconds':.001,
            'ssh':{'target':'unused','identity_file':'/unused','known_hosts_file':'/unused'}}
    sent=[]
    async def first():
        agent=Agent(config)
        polls=0
        async def request(op,body):
            nonlocal polls
            if op=='poll':
                polls+=1
                if polls>1:raise asyncio.CancelledError()
                return {'offer':'t'}
            assert op=='start';sent.append(dict(body));raise ConnectionLost('lost response')
        agent.request=request
        with pytest.raises(asyncio.CancelledError):await agent.run()
    asyncio.run(first())
    assert read(tmp_path/'agent-state.json')['pending_start']==sent[0]
    async def second():
        agent=Agent(config)
        polls=0
        async def request(op,body):
            nonlocal polls
            if op=='poll':
                polls+=1
                if polls>1:raise asyncio.CancelledError()
                return {}  # Reconciled, but no new offer needed to replay old START.
            assert op=='start';sent.append(dict(body));return {'id':'same-attempt'}
        agent.request=request
        launched=[];agent.launch=lambda a:launched.append(a['id'])
        with pytest.raises(asyncio.CancelledError):await agent.run()
        assert launched==['same-attempt']
    asyncio.run(second())
    assert sent[0]==sent[1]
    assert 'pending_start' not in read(tmp_path/'agent-state.json')


def test_local_peer_uid_rejected(tmp_path,monkeypatch):
    async def run():
        async with resident(tmp_path) as (m,path,_):
            monkeypatch.setattr('poolrun.ssh_transport.peer_uid',lambda s:999999)
            async with Client({'socket':str(path),'request_timeout':.2}) as client:
                with pytest.raises(ConnectionLost):await client.request('status')
            assert not m.store.state['tasks']
    asyncio.run(run())


def test_real_ssh_cli_and_package_boundaries(tmp_path):
    import base64
    from poolrun.__main__ import remote
    from poolrun.release import create
    from poolrun.store import atomic
    async def run():
        async with resident(tmp_path,True) as (m,path,lab):
            cfg={**lab.config('admin'),'request_dir':str(tmp_path/'requests')}
            body={'op':'submit','tasks':[task()],'request_id':'cli-submit-0001'}
            assert await remote(cfg,'admin',body)==await remote(cfg,'admin',body)
            assert len(m.store.state['tasks'])==1
            with pytest.raises(ValueError,match='different payload'):
                await remote(cfg,'admin',dict(body,tasks=[task('other')]))
            request_files=list((tmp_path/'requests').glob('*.json'))
            assert read(request_files[0])['status']=='ACKNOWLEDGED'
            client_config=tmp_path/'client.json';atomic(client_config,cfg)
            p=await asyncio.create_subprocess_exec(sys.executable,'-m','poolrun','--config',str(client_config),'status',stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            out,err=await p.communicate();assert p.returncode==0,err
            assert 't' in decode(out.rstrip()+b'\n')['tasks']
            work=tmp_path/'code';work.mkdir();(work/'job.py').write_text('print(1)\n')
            include=tmp_path/'include';include.write_text('job.py\n')
            contract={'env_id':'test','platforms':['test'],'argv':['python','job.py'],
                      'validator_argv':['python','job.py'],'checkpoint_contract':'none','output_contract':'test'}
            rel=create('pack',work,include,'v1',contract,tmp_path/'packages')
            content=(rel.parent/'code.zip').read_bytes()
            await remote(cfg,'admin',{'op':'release','release':read(rel),'package':base64.b64encode(content).decode(),'request_id':'cli-release-0001'})
            async with Client(lab.config('agent:a')) as client:
                fetched=await client.request('packages',{'project':'pack','release':'v1'})
                assert base64.b64decode(fetched['package'])==content
                with pytest.raises(RemoteError):await client.request('packages',{'project':'../escape','release':'v1'})
            backup=await remote(cfg,'backup',{'request_id':'backup-0001'})
            assert backup==await remote(cfg,'backup',{'request_id':'backup-0001'})
            assert Path(backup['path']).is_file()
            assert len(list((m.root/'backups').glob('state-*.json')))==1
    asyncio.run(run())


def test_control_source_has_no_http_stack():
    import ast
    root=Path(__file__).resolve().parents[1]
    for file in (root/'poolrun').glob('*.py'):
        for node in ast.walk(ast.parse(file.read_text())):
            if isinstance(node,ast.Import):assert all(not x.name.startswith(('aiohttp','http.','urllib.request')) for x in node.names)
            if isinstance(node,ast.ImportFrom):assert not (node.module or '').startswith(('aiohttp','http.','urllib.request'))
    assert 'aiohttp' not in (root/'requirements.lock').read_text()
