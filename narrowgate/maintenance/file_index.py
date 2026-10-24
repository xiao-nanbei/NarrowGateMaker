"""Full metadata-only refresh of the existing private inventory and offline pages.

No payload hashing, transfer, research mutation, or source-file deletion. Generated
review outputs alone are replaced; concurrent disappearance is reported explicitly.
"""

import collections
import datetime as dt
import html
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from .locks import physical

TZ = dt.timezone(dt.timedelta(hours=8))
PAGE_SIZE = 1000
STYLE = (
    "body{font:15px/1.6 system-ui;margin:24px;color:#203044}"
    "table{border-collapse:collapse;width:100%}"
    "td,th{border:1px solid #ddd;padding:7px;text-align:left;vertical-align:top}"
    "td{overflow-wrap:anywhere}th{background:#eef4fa}a{color:#165da8}"
    ".note{background:#eef4fa;padding:12px}"
)


def stamp(value=None):
    return dt.datetime.fromtimestamp(time.time() if value is None else value, TZ).isoformat(
        timespec="seconds"
    )


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def category(path, lock_records=None):
    parts = set(path.parts)
    name = path.name.lower()
    if name == ".ds_store":
        return "低风险Finder缓存"
    if name in {
        "requirements.lock",
        "uv.lock",
        "poetry.lock",
        "pipfile.lock",
        "yarn.lock",
        "cargo.lock",
    }:
        return "保留源码或依赖环境"
    if name.endswith(".lock"):
        reason = (lock_records or {}).get(str(path), {}).get("reason", "用途未核验")
        return "保留：锁文件 — " + reason
    if "raw" in parts or "tardis_facts_btcusdc" in parts or "tardis_facts_btcusdt" in parts:
        return "保留：原始数据或规范化事实"
    if "experiments" in parts or "evidence" in parts or "models" in parts:
        return "保留：实验证据或模型；依赖未作本轮复核"
    if "__pycache__" in parts or ".pytest_cache" in parts or name.endswith(".pyc"):
        return "可重建缓存候选；活动release须排除"
    if ".git" in parts or ".venv" in parts or path.suffix in {".py", ".cpp", ".h", ".hpp", ".md"}:
        return "保留源码或依赖环境"
    if "checkpoint" in name:
        return "检查点候选；须完成与恢复依赖验收"
    if name.endswith(".log") or ".log." in name:
        return "日志候选；须排除活动或唯一证据"
    if "cache" in parts or "working" in parts:
        return "可重建缓存候选；活动release须排除"
    return "保留待用途核验"


def candidate(cat):
    return any(x in cat for x in ("候选", "待用途核验", "待核验锁", "Finder缓存"))


def document(title, body, note):
    return (
        '<!doctype html><html lang="zh-CN"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{html.escape(title)}</title><style>{STYLE}</style>"
        f'<h1>{html.escape(title)}</h1><p class="note">{html.escape(note)}</p>{body}</html>'
    )


def page_rows(rows):
    text = [
        "<table><tr><th>修改时间</th><th>文件或目录</th><th>逻辑字节</th><th>分类／说明</th></tr>"
    ]
    for r in rows:
        path = Path(r["path"])
        label = html.escape(str(path))
        modified = html.escape(r.get("modified", r.get("oldest", "")))
        description = html.escape(r.get("category", str(r.get("files", 0)) + " 个直接子文件"))
        text.append(
            f'<tr><td>{modified}</td><td><a href="{html.escape(path.as_uri(), quote=True)}">'
            f"{label}</a></td><td>{r['bytes']:,}</td><td>{description}</td></tr>"
        )
    return "".join(text) + "</table>"


def paginate(pages, title, records, note):
    links = []
    chunk = []
    count = 0

    def flush():
        nonlocal chunk
        name = f"{title}-{len(links) + 1:04d}.html"
        body = (
            f'<a href="{html.escape(title)}.html">返回本组</a> · <a href="index.html">首页</a>'
            + page_rows(chunk)
        )
        (pages / name).write_text(document(title, body, note))
        links.append((name, len(chunk)))
        chunk = []

    for row in records:
        chunk.append(row)
        count += 1
        if len(chunk) == PAGE_SIZE:
            flush()
    if chunk:
        flush()
    body = (
        '<a href="index.html">返回首页</a>'
        + f"<p>共 {count:,} 条，每页最多 {PAGE_SIZE:,} 条。</p><ul>"
    )
    body += (
        "".join(
            f'<li><a href="{html.escape(n)}">第 {i + 1} 页 · {c:,} 条</a></li>'
            for i, (n, c) in enumerate(links)
        )
        + "</ul>"
    )
    (pages / (title + ".html")).write_text(document(title, body, note))
    return count


@contextmanager
def refresh_lock(base):
    import fcntl

    with (base / ".refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def refresh(base, roots, *, labels=None, lock_records=None):
    base = physical(Path(base))
    roots = [physical(Path(p)) for p in roots]
    base.mkdir(parents=True, exist_ok=True)
    with refresh_lock(base):
        with tempfile.TemporaryDirectory(prefix=".full-refresh-", dir=base) as stage:
            return _refresh(base, roots, Path(stage), labels=labels, lock_records=lock_records)


def _refresh(base, roots, stage, *, labels=None, lock_records=None):
    labels = labels or {str(p): p.name for p in roots}
    if not all(p.is_dir() for p in roots):
        raise ValueError("A selected scan root is unavailable; retain old report")
    devices = {p: p.stat().st_dev for p in roots}
    started = stamp()
    old = json.loads((base / "汇总.json").read_text()) if (base / "汇总.json").exists() else {}
    pages = stage / "轻量分页"
    pages.mkdir()
    assets = pages / "search"
    assets.mkdir()
    for name in ["search.js", "search.css", "panel.html"]:
        shutil.copy2(Path(__file__).with_name("assets") / name, assets / name)
    db = sqlite3.connect(stage / "scan.sqlite")
    db.execute("pragma temp_store=FILE")
    db.execute("pragma cache_size=-16000")
    db.execute(
        "create table files(path text primary key,root text,mtime real,month text,"
        "location text,category text,payload text)"
    )
    errors = []
    vanished = 0
    symlinks = 0
    empty = 0
    count = 0
    logical = 0
    batch = []
    excluded_git_entries = 0
    for root in roots:
        stack = [root]
        while stack:
            directory = stack.pop()
            if directory == base:
                continue  # Explicit self-generated output exclusion.
            try:
                with os.scandir(directory) as iterator:
                    entries = list(iterator)
            except OSError as exc:
                errors.append({"path": str(directory), "error": str(exc)})
                continue
            if not entries:
                empty += 1
            for entry in entries:
                # Exclude repository internals before traversal/stat/counting,
                # including nested repositories and worktree .git files.
                if entry.name == ".git":
                    excluded_git_entries += 1
                    continue
                p = Path(entry.path)
                try:
                    st = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(st.st_mode):
                        stack.append(p)
                        continue
                    if stat.S_ISLNK(st.st_mode):
                        symlinks += 1
                        continue
                    if not stat.S_ISREG(st.st_mode):
                        continue
                except FileNotFoundError:
                    vanished += 1
                    continue
                except OSError as exc:
                    errors.append({"path": str(p), "error": str(exc)})
                    continue
                cat = category(p, lock_records)
                modified = stamp(st.st_mtime)
                row = dict(
                    path=str(p),
                    root=str(root),
                    modified=modified,
                    mtime=st.st_mtime,
                    bytes=st.st_size,
                    allocated_bytes=st.st_blocks * 512,
                    hardlinks=st.st_nlink,
                    category=cat,
                )
                location = labels[str(root)]
                batch.append(
                    (
                        str(p),
                        str(root),
                        st.st_mtime,
                        modified[:7],
                        location,
                        cat,
                        json.dumps(row, ensure_ascii=False),
                    )
                )
                count += 1
                logical += st.st_size
                if len(batch) >= 5000:
                    db.executemany("insert into files values(?,?,?,?,?,?,?)", batch)
                    db.commit()
                    batch = []
                    if count % 50000 == 0:
                        print(
                            json.dumps({"phase": "SCAN", "files": count, "root": str(root)}),
                            flush=True,
                        )
        print(json.dumps({"phase": "ROOT_DONE", "files": count, "root": str(root)}), flush=True)
    if batch:
        db.executemany("insert into files values(?,?,?,?,?,?,?)", batch)
        db.commit()
    if errors:
        db.close()
        raise ValueError(
            f"Index scan incomplete ({len(errors)} errors); retain old report: {errors[:3]}"
        )
    db.execute("create index ordering on files(mtime,path)")
    db.execute("create index grouping on files(location,month,mtime)")
    finished = stamp()
    note = (
        f"全量元信息扫描：{started} 至 {finished}。"
        "所有 .git 目录及同名文件均不展示、不计入统计。活跃文件可能随后变化；"
        "修改时间不等于数据日期，分类不构成删除许可。"
        "本次不删除源文件、不复核所有依赖或归档。"
    )
    directories = {}
    months = collections.defaultdict(lambda: [0, 0])
    categories = collections.defaultdict(lambda: [0, 0])
    with (
        (stage / "全部文件_修改时间升序.jsonl").open("w") as full,
        (stage / "清理候选与待核验.jsonl").open("w") as candidates,
    ):
        for (payload,) in db.execute("select payload from files order by mtime,path"):
            row = json.loads(payload)
            full.write(payload + "\n")
            if candidate(row["category"]):
                candidates.write(payload + "\n")
            parent = str(Path(row["path"]).parent)
            d = directories.setdefault(
                parent,
                dict(path=parent, files=0, bytes=0, oldest=row["modified"], newest=row["modified"]),
            )
            d["files"] += 1
            d["bytes"] += row["bytes"]
            d["newest"] = row["modified"]
            for totals in [months[row["modified"][:7]], categories[(row["root"], row["category"])]]:
                totals[0] += 1
                totals[1] += row["bytes"]
    dirs = sorted(directories.values(), key=lambda r: (r["oldest"], r["path"]))
    with (stage / "目录时间顺序.jsonl").open("w") as f:
        for row in dirs:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    groups = []
    for location, month in db.execute(
        "select distinct location,month from files order by location,month"
    ).fetchall():
        for kind in ["全量", "候选"]:
            title = f"{kind}-{location}-{month}"
            rows = (
                json.loads(x[0])
                for x in db.execute(
                    "select payload from files where location=? and month=? order by mtime,path",
                    (location, month),
                )
            )
            if kind == "候选":
                rows = (r for r in rows if candidate(r["category"]))
            n = paginate(pages, title, rows, note)
            groups.append((title, n))
    groups.append(("目录时间顺序", paginate(pages, "目录时间顺序", iter(dirs), note)))
    roots_html = "<ul>" + "".join(f"<li>{html.escape(str(r))}</li>" for r in roots) + "</ul>"
    body = (
        f"<p>实际扫描 {count:,} 个文件；逻辑总量 {logical / 1024**3:,.2f} GiB。"
        "此为逻辑量，不是可释放磁盘空间。</p>" + roots_html
    )
    body += (
        "<p>排除本审查输出目录以避免自引用膨胀；不跟随符号链接。"
        "所有 .git 目录及同名文件在扫描入口排除，不进入原始清单、分页、"
        "目录汇总、搜索或容量统计；不会删除实际Git文件。"
        "旧清理回执仍在汇总.json中，仅表示历史操作。</p>"
    )
    body += (
        f"<p>扫描错误 {len(errors)}；扫描期间消失 {vanished}；未跟随符号链接 {symlinks}。</p><ul>"
    )
    body += (
        "".join(
            f'<li><a href="{html.escape(name)}.html">{html.escape(name)}</a> · {n:,} 条</li>'
            for name, n in groups
        )
        + "</ul>"
    )
    (pages / "index.html").write_text(document("文件时间审查 · 全量刷新", body, note))
    summary = {
        **old,
        "scanned_at": started,
        "updated_at": finished,
        "files": count,
        "logical_bytes": logical,
        "categories": [
            dict(root=r, category=c, files=v[0], bytes=v[1])
            for (r, c), v in sorted(categories.items())
        ],
        "months": dict(months),
        "empty_directories": empty,
        "errors": errors,
        "update_mode": "FULL_METADATA_RESCAN_CURRENT_ROOTS",
        "note": note,
        "full_refresh": dict(
            scan_started=started,
            scan_finished=finished,
            roots=list(map(str, roots)),
            exclusions=[str(base), "Any path component exactly .git (directory or file)"],
            excluded_git_entries=excluded_git_entries,
            symlinks_not_followed=symlinks,
            concurrently_disappeared=vanished,
            directory_aggregation="Direct child regular files, not recursive totals",
            source_payload_bytes_read=0,
            source_files_deleted=0,
            old_cleanup_fields="Historical receipts, not current classifications or new deletions",
        ),
    }
    dump(stage / "汇总.json", summary)
    from .search_index import build

    build(stage, labels=labels)
    # Check every generated page target and inventory/search count.
    bad = []
    for page in pages.glob("*.html"):
        for target in re.findall(r'href="([^"]+\.html)"', page.read_text()):
            if "://" not in target and not (pages / html.unescape(target)).is_file():
                bad.append((page.name, target))
    if bad or db.execute("select count(*) from files").fetchone()[0] != count:
        raise ValueError(f"Generated index validation failed: {bad[:10]}")
    if (
        db.execute(
            "select count(*) from files where path like '%/.git/%' or path like '%/.git'"
        ).fetchone()[0]
        != 0
    ):
        raise ValueError("Git internals entered the generated index")
    db.close()
    (stage / "scan.sqlite").unlink()
    old_pages = base / "轻量分页"
    old_pages.mkdir(exist_ok=True)
    if any(not p.is_dir() or p.stat().st_dev != devices[p] for p in roots):
        raise ValueError("A selected volume changed during indexing; retain old report")
    if old_pages.is_symlink() or not all(
        not p.is_symlink()
        and (
            p.name == "search"
            or (
                p.is_file()
                and p.suffix == ".html"
                and (
                    p.name == "index.html"
                    or p.name.startswith(("全量-", "候选-", "目录", "空目录回执-"))
                )
            )
        )
        for p in old_pages.iterdir()
    ):
        raise ValueError("Unknown user asset; retain old pages")
    old_search = old_pages / "search"
    if old_search.exists() and not all(
        p.is_file()
        and not p.is_symlink()
        and (
            p.name in {"search.js", "search.css", "panel.html", "manifest.js"}
            or re.fullmatch(r"index-\d+-[a-f0-9]+\.js", p.name)
        )
        for p in old_search.iterdir()
    ):
        raise ValueError("Unknown search asset; retain old pages")
    backup = stage / "previous-generated-pages"
    old_pages.rename(backup)
    try:
        pages.rename(old_pages)
    except OSError:
        backup.rename(old_pages)
        raise
    for name in [
        "全部文件_修改时间升序.jsonl",
        "清理候选与待核验.jsonl",
        "目录时间顺序.jsonl",
        "汇总.json",
    ]:
        os.replace(stage / name, base / name)
    shutil.rmtree(backup)
    (base / "时间清理审阅.html").write_text(
        document(
            "文件审查 · 当前全量快照",
            f"<p>本轮实际扫描 {count:,} 个文件。"
            '<a href="轻量分页/index.html">打开全量分页和全局搜索</a></p>' + roots_html,
            note,
        )
    )
    print(
        json.dumps(
            {
                "status": "FULL_REFRESH_COMPLETE",
                "files": count,
                "errors": len(errors),
                "logical_bytes": logical,
                "finished": finished,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    return summary
