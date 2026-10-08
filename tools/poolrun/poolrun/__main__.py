"""CLI. Preserve --request-id and payload after uncertain SSH outcomes."""
import argparse
import asyncio
import base64
import json
from pathlib import Path
import uuid
from .ssh_transport import Client, gateway
from .master import serve
from .release import create
from .store import read
from .config import load


async def remote(config, op, body=None):
    from .store import atomic, digest
    path = None
    if op in ('admin', 'backup'):
        body = dict(body or {})
        body.setdefault('request_id', uuid.uuid4().hex)
        directory = Path(config.get('request_dir', '~/.local/state/poolrun/requests')).expanduser()
        path = directory / (digest(body['request_id'])+'.json')
        record = {'op': op, 'body': body, 'endpoint': config.get('ssh', config.get('socket'))}
        if path.exists() and read(path)['request'] != record:
            raise ValueError('saved request ID belongs to different payload/endpoint')
        atomic(path, {'request': record, 'status': 'UNKNOWN_UNTIL_ACK'})
    async with Client(config) as client:
        result = await client.request(op, body)
    if path:
        atomic(path, {'request': record, 'status': 'ACKNOWLEDGED', 'result': result})
    return result


def main():
    p = argparse.ArgumentParser(description="PoolRun Lite — finite task pools, explicit network policy")
    p.add_argument("--config", default="client.json", help="admin client JSON; commands may specify their own config")
    p.add_argument("--request-id", help="reuse for idempotent retry of a mutating request")
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("master"); m.add_argument("--root", required=True); m.add_argument("--config", required=True)
    a = sub.add_parser("agent"); a.add_argument("--config", required=True)
    g = sub.add_parser('ssh-gateway', help='server-owned forced command; never a task command')
    g.add_argument('--socket', required=True); g.add_argument('--principal', required=True)
    s = sub.add_parser("submit"); s.add_argument("--file", required=True)
    s = sub.add_parser("status"); s.add_argument("--versions", action="store_true")
    s = sub.add_parser("why"); s.add_argument("task")
    s = sub.add_parser("drain"); s.add_argument("--host", required=True); s.add_argument("--resume", action="store_true")
    s = sub.add_parser("stop"); s.add_argument("--task", required=True); s.add_argument("--mode", choices=["checkpoint", "terminate"], required=True);s.add_argument("--deadline",type=float)
    s = sub.add_parser("retry"); s.add_argument("--task", required=True); s.add_argument("--uncertain", action="store_true"); s.add_argument("--memory-bytes", type=int)
    s.add_argument("--override-soft-memory-failure", action="store_true", help="owner-approved one-time retry after correcting a soft watchdog policy")
    s = sub.add_parser("resume"); s.add_argument("--task", required=True); s.add_argument("--release", required=True)
    s = sub.add_parser("hold"); s.add_argument("--project", required=True)
    s = sub.add_parser("rollout"); s.add_argument("--project", required=True); s.add_argument("--release", required=True)
    s.add_argument("--scope", choices=["pending"], default="pending"); s.add_argument("--mode", choices=["future-only"], default="future-only"); s.add_argument("--missing", choices=["wait"], default="wait")
    s = sub.add_parser("backup");s.add_argument("--destination")
    s = sub.add_parser("upgrade");s.add_argument("--task",required=True);s.add_argument("--release",required=True);s.add_argument("--mode",choices=["checkpoint","restart"],required=True)
    s = sub.add_parser("host-update");s.add_argument("--host",required=True);s.add_argument("--resources",required=True,help="JSON file with new slots/CPU/RAM/disk budgets")
    s = sub.add_parser("pending-placement", help="change hosts for never-started tasks only")
    s.add_argument("--project", required=True); s.add_argument("--hosts", required=True)
    s = sub.add_parser("gc");s.add_argument("--host",required=True);s.add_argument("--apply",action="store_true",help="default is dry-run")
    s = sub.add_parser("env-seal");s.add_argument("--root",required=True);s.add_argument("--python",required=True);s.add_argument("--include",required=True);s.add_argument("--output",required=True)
    s = sub.add_parser("agent-reset-transfer");s.add_argument("--root",required=True);s.add_argument("--target",required=True,help="task ID for input preparation or attempt ID for result upload; Agent must be stopped")
    s = sub.add_parser("object-register"); s.add_argument("--file", required=True)
    r = sub.add_parser("release").add_subparsers(dest="release_cmd", required=True)
    s = r.add_parser("create")
    for name in ("project", "root", "include", "id", "contract", "destination"): s.add_argument("--"+name, required=True)
    s = r.add_parser("register"); s.add_argument("--file", required=True)
    s = r.add_parser("prepare");s.add_argument("--project",required=True);s.add_argument("--release",required=True);s.add_argument("--hosts",required=True)
    args = p.parse_args()
    if args.cmd == 'ssh-gateway': gateway(args.socket, args.principal); return
    if args.cmd == "master": serve(args.root, load(args.config)); return
    if args.cmd == "agent":
        from .agent import Agent
        asyncio.run(Agent(load(args.config)).run()); return
    if args.cmd == "agent-reset-transfer":
        from .store import Store
        store=Store(args.root,name="agent-state")
        def reset(s):
            for k in ("staging_retry","upload_retry","errors"):s.setdefault(k,{}).pop(args.target,None)
            return {"reset":args.target,"business_attempt_unchanged":True}
        try:print(store.transition(str(uuid.uuid4()),reset))
        finally:store.close()
        return
    if args.cmd == "env-seal":
        from .environment import seal
        from .store import atomic
        atomic(args.output,seal(args.root,args.python,[x for x in Path(args.include).read_text().splitlines() if x.strip() and not x.startswith("#")]))
        print(args.output);return
    if args.cmd == "release" and args.release_cmd == "create":
        print(create(args.project,args.root,args.include,args.id,read(args.contract),args.destination)); return
    config = load(args.config)
    body = {"request_id": args.request_id or str(uuid.uuid4()), "op": args.cmd}
    if args.cmd not in ('status', 'why'):
        import sys
        print('Control request_id=' + body['request_id'] + '; retain this ID and payload if outcome is unknown', file=sys.stderr)
    if args.cmd in ("status", "why"):
        result = asyncio.run(remote(config, "status"))
        if args.cmd == "why":
            from .model import eligible
            import time
            t = result["tasks"][args.task]
            result = {"task": t, "hosts": {h: result.get('cache_locality',{}).get(args.task,{}).get(h) or eligible(result,t,h,result["live"].get(h,{}),time.time()) for h in result["hosts"]}}
    elif args.cmd == "backup":
        result = asyncio.run(remote(config, "backup", body))
        if args.destination:
            # Download an exact authoritative snapshot, not the redacted status view.
            snapshot=asyncio.run(remote(config,"snapshot",{}))
            from .store import atomic
            target=Path(args.destination)/("state-"+str(__import__("time").time_ns())+".json")
            atomic(target,snapshot);result["local_copy"]=str(target)
    else:
        if args.cmd == "submit": body["tasks"] = [json.loads(x) for x in Path(args.file).read_text().splitlines() if x.strip()]
        elif args.cmd == "object-register": body.update(op="object", object=read(args.file))
        elif args.cmd == "release" and args.release_cmd == "prepare":
            body.update(op="prepare",project=args.project,release=args.release,hosts=args.hosts.split(","))
        elif args.cmd == "release":
            f = Path(args.file)
            body.update(op="release", release=read(f), package=base64.b64encode((f.parent/"code.zip").read_bytes()).decode())
        else:
            body.update({k:v for k,v in vars(args).items() if k not in ("config", "cmd", "request_id") and v is not None})
            if args.cmd == "drain": body["enabled"] = not args.resume
            if args.cmd == "host-update":body["resources"]=read(args.resources)
            if args.cmd == "pending-placement":body["hosts"]=args.hosts.split(",")
        result = asyncio.run(remote(config, "admin", body))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
