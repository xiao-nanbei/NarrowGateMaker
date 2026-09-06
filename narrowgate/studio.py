"""Durable, loopback-only replay control and independent HTTP worker.

The first adapter runs the existing public synthetic demo, never live trading.
SQLite belongs to the control host; workers exchange artifacts through HTTP.
"""

import argparse
import asyncio
import contextlib
import fcntl
import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener

LEASE_SECONDS = 45
ARTIFACT_LIMIT = 2_000_000
TERMINAL = {"completed", "failed", "canceled"}
FILES = (
    "summary.json",
    "trace.jsonl",
    "receipt.json",
    "stdout.log",
    "stderr.log",
    "environment.json",
)
RUNNER = "replay-demo"
DATASET = "synthetic-demo"


def dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)


def identifier(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 100
        or not all(c.isalnum() or c in "-_" for c in value)
    ):
        raise ValueError("identifier must use 1–100 letters, digits, '-' or '_'")
    return value


class Conflict(ValueError):
    """A request would duplicate work or replace a different attempt."""


def atomic_text(path: Path, content: str):
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Store:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db_path = self.root / "studio.sqlite3"
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL,
                    specification TEXT NOT NULL, created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL,
                    name TEXT NOT NULL, arm TEXT NOT NULL, status TEXT NOT NULL,
                    worker_id TEXT, session TEXT, created_at REAL NOT NULL,
                    updated_at REAL NOT NULL, error TEXT,
                    cancel_requested INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY, session TEXT NOT NULL, last_seen REAL NOT NULL,
                    capabilities TEXT NOT NULL, datasets TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
                    data TEXT NOT NULL);
            """)

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def event(db, job_id: str):
        row = dict(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        row.pop("session", None)
        db.execute("INSERT INTO events(kind,data) VALUES ('job_changed',?)", (dumps(row),))

    def create(self, specification: dict, key: str) -> dict:
        identifier(key)
        if not isinstance(specification, dict) or set(specification) != {
            "name",
            "runner",
            "dataset",
            "arms",
        }:
            raise ValueError("expected name, runner, dataset and arms only")
        if specification["runner"] != RUNNER or specification["dataset"] != DATASET:
            raise ValueError("only the public synthetic demo adapter is available")
        name, arms = specification["name"], specification["arms"]
        if not isinstance(name, str) or not name.strip() or len(name) > 100:
            raise ValueError("name must contain 1–100 characters")
        if not isinstance(arms, list) or not 1 <= len(arms) <= 4:
            raise ValueError("provide 1–4 independent demo arms")
        if any(not isinstance(a, str) for a in arms) or len(set(arms)) != len(arms):
            raise ValueError("arms must be unique names")
        for arm in arms:
            identifier(arm)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                "SELECT * FROM experiments WHERE request_key=?", (key,)
            ).fetchone()
            if existing:
                if existing["specification"] != dumps(specification):
                    raise Conflict("idempotency key already belongs to a different request")
                experiment_id = existing["id"]
            else:
                experiment_id = uuid.uuid4().hex
                now = time.time()
                db.execute(
                    "INSERT INTO experiments VALUES (?,?,?,?)",
                    (experiment_id, key, dumps(specification), now),
                )
                for arm in arms:
                    job_id = uuid.uuid4().hex
                    db.execute(
                        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                        (job_id, experiment_id, name, arm, "queued", None, None, now, now, None),
                    )
                    self.event(db, job_id)
            rows = db.execute(
                "SELECT id FROM jobs WHERE experiment_id=?", (experiment_id,)
            ).fetchall()
        return {"id": experiment_id, "jobs": [self.job(r["id"]) for r in rows]}

    def expire(self):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT id FROM jobs WHERE status IN ('running','archiving','cancel_requested') "
                "AND updated_at < ?",
                (time.time() - LEASE_SECONDS,),
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE jobs SET status='lost', error=? WHERE id=?",
                    ("worker heartbeat expired; not automatically requeued", row["id"]),
                )
                self.event(db, row["id"])

    def job(self, job_id: str) -> dict:
        identifier(job_id)
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        result = dict(row)
        result.pop("session", None)
        return result

    def jobs(self) -> list[dict]:
        self.expire()
        with self.connect() as db:
            ids = db.execute("SELECT id FROM jobs ORDER BY created_at DESC LIMIT 1000").fetchall()
        return [self.job(row["id"]) for row in ids]

    def register(self, worker_id: str, session: str) -> dict:
        identifier(worker_id)
        identifier(session)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT * FROM nodes WHERE id=?", (worker_id,)).fetchone()
            if previous and previous["session"] != session:
                active = db.execute(
                    "SELECT 1 FROM jobs WHERE worker_id=? AND status NOT IN "
                    "('completed','failed','canceled')",
                    (worker_id,),
                ).fetchone()
                if active or previous["last_seen"] > time.time() - LEASE_SECONDS:
                    raise Conflict("worker id is still owned; use its original worker or a new id")
            db.execute(
                "INSERT OR REPLACE INTO nodes VALUES (?,?,?,?,?)",
                (worker_id, session, time.time(), dumps([RUNNER]), dumps([DATASET])),
            )
        return {"registered": True}

    @staticmethod
    def authenticate_worker(db, worker_id, session):
        node = db.execute("SELECT session FROM nodes WHERE id=?", (worker_id,)).fetchone()
        if not node or node["session"] != session:
            raise Conflict("worker session is not registered")
        db.execute("UPDATE nodes SET last_seen=? WHERE id=?", (time.time(), worker_id))

    def claim(self, worker_id: str, session: str) -> dict | None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.authenticate_worker(db, worker_id, session)
            old = db.execute(
                "SELECT id FROM jobs WHERE worker_id=? AND status NOT IN "
                "('completed','failed','canceled')",
                (worker_id,),
            ).fetchone()
            if old:
                # A lost claim response must resolve to the same job, never another one.
                return self.job(old["id"])
            row = db.execute(
                "SELECT id FROM jobs WHERE status='queued' ORDER BY created_at,id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            job_id = row["id"]
            db.execute(
                "UPDATE jobs SET status='running',worker_id=?,session=?,updated_at=? WHERE id=?",
                (worker_id, session, time.time(), job_id),
            )
            self.event(db, job_id)
        return self.job(job_id)

    def heartbeat(self, job_id, worker_id, session) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.authenticate_worker(db, worker_id, session)
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row or row["worker_id"] != worker_id or row["session"] != session:
                raise Conflict("job is owned by a different worker/attempt")
            if row["status"] == "lost":
                # The original attempt may reconnect; no other worker can claim it.
                status = "cancel_requested" if row["cancel_requested"] else "running"
                db.execute("UPDATE jobs SET status=?,error=NULL WHERE id=?", (status, job_id))
                self.event(db, job_id)
            if row["status"] not in TERMINAL:
                db.execute("UPDATE jobs SET updated_at=? WHERE id=?", (time.time(), job_id))
        return {"status": self.job(job_id)["status"], "cancel": bool(row["cancel_requested"])}

    def cancel(self, job_id):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                raise KeyError(job_id)
            if row["status"] not in TERMINAL:
                status = "canceled" if row["status"] == "queued" else "cancel_requested"
                # Keep heartbeat age: cancellation is not proof that a lost worker is alive.
                db.execute(
                    "UPDATE jobs SET status=?,cancel_requested=1 WHERE id=?", (status, job_id)
                )
                self.event(db, job_id)
        return self.job(job_id)

    def publish(self, job_id: str, worker_id: str, session: str, payload: dict) -> dict:
        status = payload.get("status")
        files = payload.get("files", {})
        if not isinstance(status, str) or status not in TERMINAL or not isinstance(files, dict):
            raise ValueError("invalid completion payload")
        if payload.get("error") is not None and not isinstance(payload["error"], str):
            raise ValueError("error must be text or null")
        if set(files) - set(FILES) or any(not isinstance(v, str) for v in files.values()):
            raise ValueError("only known text artifacts may be published")
        if sum(len(v.encode()) for v in files.values()) > ARTIFACT_LIMIT:
            raise ValueError("demo artifacts exceed bounded upload size")
        if not {"stdout.log", "stderr.log", "environment.json"} <= files.keys():
            raise ValueError("logs and environment must be durable before completion")
        if status == "completed":
            validate_demo_artifacts(files)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.authenticate_worker(db, worker_id, session)
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row or row["worker_id"] != worker_id or row["session"] != session:
                raise Conflict("late result belongs to a different worker/attempt")
            if row["status"] in TERMINAL:
                stored = self.read_artifacts(job_id)
                if stored == files and row["status"] == status:
                    return self.job(job_id)
                raise Conflict("terminal result cannot be replaced")
            if row["cancel_requested"] and status == "completed":
                raise Conflict("canceled attempt cannot publish success")
            db.execute("UPDATE jobs SET status='archiving' WHERE id=?", (job_id,))
            output = self.root / "outputs" / identifier(job_id)
            output.mkdir(parents=True, exist_ok=True, mode=0o700)
            for name, content in files.items():
                target = output / name
                temporary = output / (name + ".partial")
                with temporary.open("w", encoding="utf-8") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(target)
            for directory in (output, output.parent, self.root):
                fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            db.execute(
                "UPDATE jobs SET status=?,updated_at=?,error=? WHERE id=?",
                (status, time.time(), payload.get("error"), job_id),
            )
            self.event(db, job_id)
        return self.job(job_id)

    def read_artifacts(self, job_id: str) -> dict:
        output = self.root / "outputs" / identifier(job_id)
        return {
            name: (output / name).read_text(encoding="utf-8")
            for name in FILES
            if (output / name).is_file()
        }


def validate_demo_artifacts(files: dict):
    """Use the shipped reference bytes; do not invent another research receipt."""
    from narrowgate.replay_demo import DEFAULT_REFERENCE_DIR

    for name in ("summary.json", "trace.jsonl", "receipt.json"):
        if name not in files:
            raise ValueError(f"missing required demo artifact: {name}")
        if files[name].encode() != (DEFAULT_REFERENCE_DIR / name).read_bytes():
            raise ValueError(f"demo reference mismatch: {name}")


def create_app(root: Path, token: str = ""):
    from fastapi import FastAPI
    from fastapi import Request as WebRequest
    from fastapi.responses import JSONResponse, StreamingResponse
    from fastapi.staticfiles import StaticFiles

    store = Store(root)
    app = FastAPI(title="NarrowGate Replay Studio", docs_url=None, redoc_url=None)
    app.state.store = store

    async def body(request):
        content = bytearray()
        async for chunk in request.stream():
            content.extend(chunk)
            if len(content) > ARTIFACT_LIMIT * 2:
                raise ValueError("request body exceeds the demo upload limit")
        result = json.loads(content)
        if not isinstance(result, dict):
            raise ValueError("JSON body must be an object")
        return result

    @app.middleware("http")
    async def boundary(request: WebRequest, call_next):
        host = request.url.hostname
        if host not in {"localhost", "127.0.0.1", "::1", "testserver"}:
            return JSONResponse(
                {"detail": "use the loopback endpoint through SSH"}, status_code=403
            )
        origin = request.headers.get("origin")
        if origin and urlparse(origin).netloc != request.headers.get("host"):
            return JSONResponse({"detail": "cross-origin access is disabled"}, status_code=403)
        if request.url.path.startswith("/api/") and token:
            import hmac

            if not hmac.compare_digest(request.headers.get("authorization", ""), f"Bearer {token}"):
                return JSONResponse({"detail": "authentication required"}, status_code=401)
        length = request.headers.get("content-length", "0")
        if not length.isdigit() or int(length) > ARTIFACT_LIMIT * 2:
            return JSONResponse({"detail": "request too large"}, status_code=413)
        return await call_next(request)

    @app.exception_handler(ValueError)
    async def bad_request(_request, exc):
        return JSONResponse(
            {"detail": str(exc)}, status_code=409 if isinstance(exc, Conflict) else 400
        )

    @app.exception_handler(KeyError)
    async def missing(_request, _exc):
        return JSONResponse({"detail": "job not found"}, status_code=404)

    @app.get("/api/runners")
    def runners():
        return {
            "items": [
                {
                    "id": RUNNER,
                    "label": "Synthetic replay demo",
                    "available": True,
                    "classification": "synthetic_non_economic",
                }
            ]
        }

    @app.get("/api/datasets")
    def datasets():
        return {"items": [{"id": DATASET, "role": "public_synthetic", "available": True}]}

    @app.get("/api/nodes")
    def nodes():
        with store.connect() as db:
            rows = db.execute("SELECT * FROM nodes ORDER BY id").fetchall()
        return {
            "items": [
                {
                    "id": r["id"],
                    "last_seen": r["last_seen"],
                    "online": r["last_seen"] >= time.time() - LEASE_SECONDS,
                    "capabilities": json.loads(r["capabilities"]),
                    "datasets": json.loads(r["datasets"]),
                }
                for r in rows
            ]
        }

    @app.get("/api/jobs")
    def jobs():
        return {"items": store.jobs()}

    @app.get("/api/jobs/{job_id}")
    def job(job_id: str):
        store.expire()
        return store.job(job_id)

    @app.post("/api/experiments")
    async def create(request: WebRequest):
        key = request.headers.get("idempotency-key", "")
        return store.create(await body(request), key)

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        return store.cancel(job_id)

    @app.get("/api/jobs/{job_id}/report")
    def report(job_id: str):
        if store.job(job_id)["status"] != "completed":
            raise Conflict("report is unavailable until artifacts are verified and durable")
        artifacts = store.read_artifacts(job_id)
        return {
            "schema_version": "backtest_report.v1",
            "classification": "synthetic_non_economic",
            "summary": json.loads(artifacts["summary.json"]),
            "trace": [json.loads(line) for line in artifacts["trace.jsonl"].splitlines()],
            "limitations": [
                "Hand-authored synthetic mechanics, not strategy economic evidence.",
                "Queue position is simulated, not observed at the exchange.",
                "No real-market runner or E/C policy is enabled in this adapter.",
            ],
        }

    @app.get("/api/jobs/{job_id}/logs")
    def logs(job_id: str):
        store.job(job_id)
        artifacts = store.read_artifacts(job_id)
        return {
            "stdout": artifacts.get("stdout.log", ""),
            "stderr": artifacts.get("stderr.log", ""),
            "scope": "published terminal logs; running logs remain on the worker",
        }

    @app.get("/api/events")
    async def events(request: WebRequest, after: int = 0):
        cursor = max(after, int(request.headers.get("last-event-id", "0")))

        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                store.expire()
                with store.connect() as db:
                    rows = db.execute(
                        "SELECT * FROM events WHERE id>? ORDER BY id LIMIT 100", (cursor,)
                    ).fetchall()
                for row in rows:
                    cursor = row["id"]
                    yield f"id: {cursor}\nevent: {row['kind']}\ndata: {row['data']}\n\n"
                if not rows:
                    yield ": keepalive\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/workers/{worker_id}/register")
    async def register(worker_id: str, request: WebRequest):
        payload = await body(request)
        return store.register(worker_id, payload["session"])

    @app.post("/api/workers/{worker_id}/claim")
    async def claim(worker_id: str, request: WebRequest):
        payload = await body(request)
        return {"job": store.claim(worker_id, payload["session"])}

    @app.post("/api/workers/{worker_id}/jobs/{job_id}/heartbeat")
    async def heartbeat(worker_id: str, job_id: str, request: WebRequest):
        payload = await body(request)
        return store.heartbeat(job_id, worker_id, payload["session"])

    @app.post("/api/workers/{worker_id}/jobs/{job_id}/publish")
    async def publish(worker_id: str, job_id: str, request: WebRequest):
        payload = await body(request)
        return store.publish(job_id, worker_id, payload["session"], payload)

    assets = Path(__file__).with_name("studio_static")
    if assets.is_dir():
        app.mount("/", StaticFiles(directory=assets, html=True), name="studio")
    return app


class Client:
    def __init__(self, url: str, token: str = ""):
        parsed = urlparse(url)
        if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("connect via an HTTP loopback SSH tunnel, not a public API")
        self.url = url.rstrip("/")
        self.token = token
        self.opener = build_opener(ProxyHandler({}))

    def post(self, route: str, body: dict):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(
            self.url + route, data=dumps(body).encode(), headers=headers, method="POST"
        )
        with self.opener.open(request, timeout=10) as response:
            return json.load(response)


def stop_child(process):
    if process.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def execute_demo(client, worker_id, session, job, root: Path, stopping) -> None:
    job_id = identifier(job["id"])
    directory = root / job_id
    directory.mkdir(mode=0o700)  # Never overwrite or resume an ambiguous old process.
    output = directory / "result"
    route = f"/api/workers/{worker_id}/jobs/{job_id}"
    payload = {"session": session}
    environment = {
        key: os.environ[key]
        for key in ("PATH", "LANG", "SYSTEMROOT", "TMPDIR")
        if key in os.environ
    }
    environment.update(
        {
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    command = [
        sys.executable,
        "-m",
        "narrowgate",
        "replay-demo",
        "--verify-reference",
        "--output-dir",
        str(output),
    ]
    started = time.time()
    status, error = "failed", None
    with (
        (directory / "stdout.log").open("w") as stdout,
        (directory / "stderr.log").open("w") as stderr,
    ):
        process = subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                if stopping() or time.time() - started > 600:
                    status = "canceled" if stopping() else "failed"
                    error = "worker stopped" if stopping() else "demo wall-clock limit exceeded"
                    break
                try:
                    state = client.post(route + "/heartbeat", payload)
                    if state["cancel"]:
                        status, error = "canceled", "owner cancellation or expired lease"
                        break
                except (URLError, TimeoutError):
                    # Keep the child alive during a bounded control interruption, not forever.
                    if time.time() - started > 600:
                        raise
                time.sleep(0.2)
            else:
                status = "completed" if process.returncode == 0 else "failed"
                error = None if status == "completed" else f"runner exit {process.returncode}"
        finally:
            stop_child(process)
    files = {name: (directory / name).read_text() for name in ("stdout.log", "stderr.log")}
    for name in ("summary.json", "trace.jsonl", "receipt.json"):
        if (output / name).exists():
            files[name] = (output / name).read_text()
    files["environment.json"] = dumps(
        {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "runner": RUNNER,
            "elapsed_seconds": time.time() - started,
            "returncode": process.returncode,
        }
    )
    payload.update({"status": status, "error": error, "files": files})
    # Local outbox survives upload failure. Same worker/session can retry this exact payload.
    outbox = directory / "publication.json"
    atomic_text(outbox, dumps(payload))
    publish_outbox(client, route, payload, directory)


def publish_outbox(client, route, payload, directory):
    deadline = time.monotonic() + 60
    while True:
        try:
            client.post(route + "/publish", payload)
            break
        except HTTPError as exc:
            if exc.code == 409 and payload["status"] == "completed":
                current = retry_control(
                    client, route + "/heartbeat", {"session": payload["session"]}, deadline=deadline
                )
                if current["cancel"]:
                    payload = {
                        **payload,
                        "status": "canceled",
                        "error": "canceled after runner exit, before publication",
                    }
                    atomic_text(directory / "publication.json", dumps(payload))
                    continue
            if exc.code < 500:
                raise RuntimeError(
                    f"publication rejected ({exc.code}); retained in {directory}"
                ) from exc
            if time.monotonic() >= deadline:
                raise
        except (URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"publication failed; retained in {directory}") from None
        time.sleep(2)


def retry_control(client, route, body, *, deadline=None):
    deadline = time.monotonic() + 60 if deadline is None else deadline
    while True:
        try:
            return client.post(route, body)
        except HTTPError as exc:
            if exc.code < 500 or time.monotonic() >= deadline:
                raise
        except (URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise
        time.sleep(2)


def worker(args) -> int:
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    client = Client(args.url, os.environ.get("NARROWGATE_STUDIO_TOKEN", ""))
    identifier(args.worker_id)
    stopped = False

    def request_stop(_sig, _frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    with (root / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = root / "worker.json"
        if not state_path.exists():
            atomic_text(
                state_path, dumps({"worker_id": args.worker_id, "session": uuid.uuid4().hex})
            )
        state = json.loads(state_path.read_text())
        if state["worker_id"] != args.worker_id:
            raise Conflict("work directory belongs to a different worker id")
        session = identifier(state["session"])
        base = f"/api/workers/{args.worker_id}"
        retry_control(client, base + "/register", {"session": session})
        while not stopped:
            result = retry_control(client, base + "/claim", {"session": session})
            if result["job"]:
                job = result["job"]
                directory = root / identifier(job["id"])
                outbox = directory / "publication.json"
                if outbox.is_file():
                    publish_outbox(
                        client,
                        base + f"/jobs/{job['id']}",
                        json.loads(outbox.read_text()),
                        directory,
                    )
                elif directory.exists():
                    raise Conflict(
                        "previous worker execution is uncertain; "
                        "inspect its process/logs, do not rerun"
                    )
                elif job["cancel_requested"]:
                    payload = {
                        "session": session,
                        "status": "canceled",
                        "files": {
                            "stdout.log": "",
                            "stderr.log": "Canceled before runner start\n",
                            "environment.json": dumps({"runner_started": False}),
                        },
                    }
                    publish_outbox(client, base + f"/jobs/{job['id']}", payload, root)
                else:
                    execute_demo(client, args.worker_id, session, job, root, lambda: stopped)
            if args.once:
                return 0
            if not result["job"]:
                time.sleep(2)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="start the loopback control API and packaged frontend")
    serve.add_argument("--state-dir", type=Path, required=True)
    serve.add_argument("--port", type=int, default=8080)
    run = sub.add_parser("worker", help="run one independent worker; no shared SQLite access")
    run.add_argument("--url", default="http://127.0.0.1:8080")
    run.add_argument("--worker-id", required=True)
    run.add_argument("--work-dir", type=Path, required=True)
    run.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "worker":
        return worker(args)
    import uvicorn

    app = create_app(args.state_dir, os.environ.get("NARROWGATE_STUDIO_TOKEN", ""))
    uvicorn.run(
        app, host="127.0.0.1", port=args.port, access_log=False, timeout_graceful_shutdown=5
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
