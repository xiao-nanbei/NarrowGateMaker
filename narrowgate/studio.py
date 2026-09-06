"""Durable, loopback-only replay control and independent HTTP worker.

The first adapter runs the existing public synthetic demo, never live trading.
SQLite belongs to the control host; workers exchange artifacts through HTTP.
"""

import argparse
import asyncio
import contextlib
import csv
import fcntl
import hashlib
import json
import math
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
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
B0_CLASSIFICATION = "real_market_baseline_read_only"


def b0_projection(summary_path: Path) -> dict:
    """Import completed owner-selected outputs, never discover or execute replay work.

    Source locators and raw artifacts remain private and are never sent to the browser.
    This checks import consistency, not a new economic or cross-host qualification.
    """
    summary_path = summary_path.resolve()
    root = summary_path.parent

    def selected(relative: str) -> Path:
        path = (root / relative).resolve()
        if (
            Path(relative).is_absolute()
            or ".." in Path(relative).parts
            or "partial" in relative.lower()
            or not path.is_relative_to(root)
            or not path.is_file()
        ):
            raise ValueError("B0 source must be a complete selected file inside the summary root")
        return path

    def number(value) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ValueError("B0 numeric field is invalid")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("B0 numeric fields must be finite")
        return result

    def equal(left, right):
        if not math.isclose(number(left), number(right), rel_tol=1e-12, abs_tol=1e-8):
            raise ValueError("B0 selected outputs do not reconcile with the summary")

    content = selected(summary_path.name).read_bytes()
    source = json.loads(content)
    plan = json.loads(selected("input_plan.json").read_bytes())
    if (
        source["visibility"] != "local_only_do_not_publish"
        or source["arm"] != "baseline"
        or source["source_commit"] != plan["source_commit"]
    ):
        raise ValueError("a private baseline summary with matching source identity is required")
    verified = source["verification"]
    for key in (
        "all_segments_complete",
        "full_fill_trace_reconciled",
        "funding_cashflows_reconciled",
        "campaign_values_reconciled_with_csv_rounding",
    ):
        if verified[key] is not True:
            raise ValueError("B0 source summary has incomplete reconciliation")
    dates = source["dates"]
    if (
        not dates
        or dates != sorted(set(dates))
        or dates != plan["days"]
        or len(dates) != source["unique_utc_days"]
    ):
        raise ValueError("B0 coverage must match the frozen unique chronological day list")
    segments = source["segments"]
    if not segments or len(segments) != source["continuous_segments"]:
        raise ValueError("B0 segment count is incomplete")
    planned = {item["id"]: item["days"] for item in plan["segments"]}
    if len(planned) != len(segments) or len(planned) != len(plan["segments"]):
        raise ValueError("B0 segments must match the input plan exactly")
    fields = {
        "trading_pnl": ("trading_pnl_after_fees_usdc", "replay_pnl"),
        "funding_pnl": ("funding_cashflow_usdc", "funding_cashflow_usdc"),
        "net_pnl": ("net_pnl_usdc", "replay_net_pnl"),
        "filled_orders": ("fills", "fills_total"),
        "campaign_count": ("campaigns", "campaigns"),
        "buy_fills": ("buy_fills", "fills_bid_buy"),
        "sell_fills": ("sell_fills", "fills_ask_sell"),
        "closed_campaigns": ("closed_campaigns", "closed_campaigns"),
        "open_campaigns": ("open_campaigns", "open_campaigns"),
    }
    queue_totals = {
        key: number(verified[key])
        for key in (
            "queue_lookup_count",
            "queue_exact_count",
            "queue_known_zero_count",
            "queue_missing_count",
            "native_events_consumed",
            "native_events_rejected",
            "native_gap_invalid_sequence_time_reversal_counts",
        )
    }
    rows, covered, stems = [], [], set()
    for item in segments:
        segment_days = planned[item["segment"]]
        start, end = (date.fromisoformat(value) for value in item["segment"].split("_"))
        expected = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
        if not expected or segment_days != expected or len(expected) != item["days"]:
            raise ValueError("B0 segment dates are not contiguous or do not match the plan")
        covered.extend(expected)
        stem = item["selected_output"]
        origin = stem.split("/", 1)[0]
        if stem in stems or not origin.startswith(("local_", "cloud_")):
            raise ValueError("B0 selected output is duplicated or has an unknown source host")
        stems.add(stem)
        artifacts = {
            suffix: selected(stem + suffix)
            for suffix in (
                ".json",
                ".daily.csv",
                ".campaign_labels.csv",
                ".fill_trace.csv",
                ".funding.csv",
            )
        }
        metadata = json.loads(artifacts[".json"].read_bytes())
        with artifacts[".daily.csv"].open() as handle:
            daily = list(csv.DictReader(handle))
        if len(daily) != 1:
            raise ValueError("B0 continuous segment must have exactly one aggregate CSV row")
        row = daily[0]
        if (
            metadata["days"] != expected
            or metadata["arms"] != ["baseline"]
            or metadata["config_sha256"] != source["strategy"]["config_sha256"]
            or metadata["accounting_window"] != "continuous_segment"
            or row["arm"] != "baseline"
            or row["day"] != expected[0]
            or row["window_end_day"] != expected[-1]
            or int(row["window_day_count"]) != len(expected)
            or row["accounting_window"] != "continuous_segment"
            or row["economic_pnl_complete"].lower() != "true"
        ):
            raise ValueError("B0 segment is incomplete or has mismatched accounting metadata")
        projected = {
            "index": len(rows) + 1,
            "start_day": expected[0],
            "end_day": expected[-1],
            "day_count": len(expected),
            "source": "local" if origin.startswith("local_") else "azure",
            "queue_mode": "strict"
            if row.get("exchange_book_queue_mode") == "strict"
            else "non_strict",
        }
        for output, (summary_key, csv_key) in fields.items():
            equal(item[summary_key], row[csv_key])
            projected[output] = number(item[summary_key])
        equal(projected["net_pnl"], projected["trading_pnl"] + projected["funding_pnl"])
        rows.append(projected)
    if covered != dates:
        raise ValueError("B0 selected segments overlap or do not exactly cover the frozen dates")
    totals = {}
    for output, (source_key, _) in fields.items():
        equal(source["totals"][source_key], sum(row[output] for row in rows))
        totals[output] = number(source["totals"][source_key])
    equal(
        source["totals"]["fill_fee_cost_usdc"],
        sum(number(item["fill_fee_cost_usdc"]) for item in segments),
    )
    overlap = verified["host_comparison_days"]
    if not overlap or overlap != sorted(set(overlap)) or not set(overlap).issubset(dates):
        raise ValueError("B0 host comparison days are invalid")
    report_id = "b0-" + hashlib.sha256(content).hexdigest()[:24]
    report = {
        "id": report_id,
        "name": f"B0 · {len(dates)} UTC 日 · 只读结果",
        "classification": B0_CLASSIFICATION,
        "summary": {
            **totals,
            "coverage_days": len(dates),
            "segment_count": len(rows),
            "fees_already_included": True,
            "fee_cost": number(source["totals"]["fill_fee_cost_usdc"]),
        },
        "segments": rows,
        "verification": {
            **queue_totals,
            "overlap_days": overlap,
            "passed": True,
            "description": "既有摘要记录的本地 / Azure 跨主机核验；本次只读导入，没有重跑或重新核验远端。",
        },
        "limitations": [
            "这是 modeled diagnostic B0，不是精确实盘经济复现、策略晋级或 E/C 训练结果。",
            "每行代表连续 segment；段内状态延续，数据缺口后重新开始。不能视为每日收益或跨缺口连续账户曲线，也不计算 Sharpe、日胜率或日置信区间。",
            "交易 PnL 已含成交手续费和终点 MTM；资金费仅加一次。Campaign 是金额分解，不能再累加到净 PnL。",
            f"源摘要记录 {queue_totals['queue_missing_count']:g} 次激活查询缺少 exact / known-zero 覆盖。队列位置和成交是模型估计；strict 模式不等于全部精确队列。",
            "native rejected 是 accepted=false，包含正常重复、已覆盖或 snapshot 前更新，并含 D−1 warmup；不是坏行情数。native 计数按源摘要展示，本次导入不重新做原生数据资格核验。",
            "Python REST 异步 GLOBAL FIFO、24h native warmup；短时延迟 pilot 与 bulk-cancel n=1 prior 不能证明长期尾部或实盘路径等价。",
            "部分回调、bulk terminal、IOC 查询和网关失败 / UNKNOWN 时序未建模；资金费不反馈到当前 trading-PnL 风控，没有保证金 / 强平模型。",
            "来源 local / Azure 描述已选产物的执行来源，不表示当前云节点在线；没有启动云同步、worker 或任何回测。",
        ],
    }
    return report


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
                CREATE TABLE IF NOT EXISTS results (
                    id TEXT PRIMARY KEY, report TEXT NOT NULL, imported_at REAL NOT NULL);
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

    def import_b0(self, summary_path: Path) -> dict:
        if self.root.stat().st_mode & 0o077:
            raise ValueError("private B0 imports require an owner-only state directory (mode 0700)")
        report = b0_projection(summary_path)
        result_id = report["id"]
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT * FROM results WHERE id=?", (result_id,)).fetchone()
            if existing:
                if existing["report"] != dumps(report):
                    raise Conflict(
                        "imported B0 display fields changed; preserve the original result"
                    )
            else:
                db.execute(
                    "INSERT INTO results VALUES (?,?,?)",
                    (result_id, dumps(report), time.time()),
                )
        return self.result(result_id)

    def result(self, result_id: str) -> dict:
        identifier(result_id)
        with self.connect() as db:
            row = db.execute("SELECT * FROM results WHERE id=?", (result_id,)).fetchone()
        if row is None:
            raise KeyError(result_id)
        return {**json.loads(row["report"]), "imported_at": row["imported_at"]}

    def results(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT id FROM results ORDER BY imported_at DESC LIMIT 1000"
            ).fetchall()
        return [
            {key: report[key] for key in ("id", "name", "classification", "imported_at")}
            | {key: report["summary"][key] for key in ("coverage_days", "segment_count")}
            for report in (self.result(row["id"]) for row in rows)
        ]

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

    @app.get("/api/results")
    def results():
        return {"items": store.results()}

    @app.get("/api/results/{result_id}")
    def result(result_id: str):
        return store.result(result_id)

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
    imported = sub.add_parser(
        "import-b0", help="import existing private B0 results; never run replay"
    )
    imported.add_argument("--state-dir", type=Path, required=True)
    imported.add_argument("--summary", type=Path, required=True)
    run = sub.add_parser("worker", help="run one independent worker; no shared SQLite access")
    run.add_argument("--url", default="http://127.0.0.1:8080")
    run.add_argument("--worker-id", required=True)
    run.add_argument("--work-dir", type=Path, required=True)
    run.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "import-b0":
        report = Store(args.state_dir).import_b0(args.summary)
        print(
            dumps(
                {
                    "id": report["id"],
                    "classification": report["classification"],
                    "coverage_days": report["summary"]["coverage_days"],
                    "segment_count": report["summary"]["segment_count"],
                }
            )
        )
        return 0
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
