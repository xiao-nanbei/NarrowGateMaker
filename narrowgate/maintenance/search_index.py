"""Rebuild this private file:// search after updating the existing JSONL inventory.

No disk rescan, deletion, network access or runtime dependency inspection occurs.
Run with the project Python. Generated chunks are display indexes, not data backups.
"""

import base64
import datetime
import gzip
import hashlib
import json
import re
from pathlib import Path

CHUNK_ROWS = 8000


def js_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def build(base, *, labels):
    base = Path(base)
    pages = base / "轻量分页"
    assets = pages / "search"
    source_path = base / "全部文件_修改时间升序.jsonl"
    summary_path = base / "汇总.json"
    summary = json.loads(summary_path.read_text())
    source_stat = source_path.stat()
    stamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(
        timespec="seconds"
    )
    roots, categories, groups, parts, rows = [], [], [], [], []
    total = hidden = source_count = 0
    digest = hashlib.sha256()
    available_pages = {path.name for path in pages.glob("*.html")}

    def intern(items, value):
        if value not in items:
            items.append(value)
        return items.index(value)

    def flush():
        if not rows:
            return
        payload = gzip.compress(js_json(rows).encode(), compresslevel=6, mtime=0)
        identity = hashlib.sha256(payload).hexdigest()[:16]
        name = f"index-{len(parts):04d}-{identity}.js"
        content = (
            "window.NG_FILE_CHUNK=" + js_json([name, base64.b64encode(payload).decode()]) + ";\n"
        )
        target = assets / name
        if not target.exists():
            target.write_text(content)
        parts.append({"file": name, "rows": len(rows), "bytes": len(content.encode())})
        rows.clear()

    with source_path.open("rb") as source:
        for line in source:
            digest.update(line)
            record = json.loads(line)
            source_count += 1
            path = record["path"]
            if ".git" in Path(path).parts:
                hidden += 1
                continue
            root = record["root"]
            assert path.startswith(root + "/"), path
            location = labels[root]
            group = f"全量-{location}-{record['modified'][:7]}.html"
            group = group if group in available_pages else ""
            rows.append(
                [
                    intern(roots, root),
                    path[len(root) + 1 :],
                    record["modified"],
                    record["bytes"],
                    intern(categories, record["category"]),
                    intern(groups, group),
                ]
            )
            total += 1
            if len(rows) >= CHUNK_ROWS:
                flush()
    flush()
    assert source_count == summary["files"], (source_count, summary["files"])
    assert (
        source_path.stat().st_size == source_stat.st_size
        and source_path.stat().st_mtime_ns == source_stat.st_mtime_ns
    ), "Inventory changed during build"
    manifest = {
        "schema": 1,
        "generatedAt": stamp,
        "scannedAt": summary["scanned_at"],
        "updatedAt": summary["updated_at"],
        "sourceCount": source_count,
        "count": total,
        "hiddenGitCount": hidden,
        "sourceSha256": digest.hexdigest(),
        "roots": roots,
        "locations": [labels[r] for r in roots],
        "categories": categories,
        "groups": groups,
        "parts": parts,
    }
    manifest_path = assets / "manifest.js"
    temp = assets / "manifest.tmp"
    temp.write_text("window.NG_FILE_SEARCH=" + js_json(manifest) + ";\n")
    temp.replace(manifest_path)
    # The current manifest is published first; obsolete generated indexes are disposable.
    keep = {part["file"] for part in parts}
    for old in assets.glob("index-*.js"):
        if old.name not in keep:
            old.unlink()
    index_path = pages / "index.html"
    index = index_path.read_text()
    index = re.sub(
        r"<!-- GLOBAL FILE SEARCH START -->.*?<!-- GLOBAL FILE SEARCH END -->",
        "",
        index,
        flags=re.S,
    )
    fragment = (assets / "panel.html").read_text()
    insertion = "<!-- GLOBAL FILE SEARCH START -->" + fragment + "<!-- GLOBAL FILE SEARCH END -->"
    index, count = re.subn(r"</h1>", lambda m: m[0] + insertion, index, count=1)
    assert count == 1
    index_path.write_text(index)
    latest_summary = json.loads(summary_path.read_text())
    assert (
        latest_summary["files"] == summary["files"]
        and latest_summary["updated_at"] == summary["updated_at"]
    ), "Summary changed during build"
    summary = latest_summary
    summary["keyword_search"] = {
        "visibility": "local_only_do_not_publish",
        "generated_at": stamp,
        "indexed_files": total,
        "hidden_git_files": hidden,
        "source_sha256": digest.hexdigest(),
        "compressed_asset_bytes": sum(x["bytes"] for x in parts),
        "builder": "narrowgate.maintenance.search_index",
        "entry": "轻量分页/index.html",
        "rule": (
            "Rebuild after every inventory update. Search is a historical snapshot, "
            "never deletion authorization or live dependency verification."
        ),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(
        js_json(
            {
                k: summary["keyword_search"][k]
                for k in [
                    "indexed_files",
                    "hidden_git_files",
                    "compressed_asset_bytes",
                    "generated_at",
                ]
            }
        )
    )
