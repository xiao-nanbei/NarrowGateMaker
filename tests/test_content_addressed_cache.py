from __future__ import annotations

import pandas as pd

from models.audit.content_addressed_cache import (
    DirectoryContentAddressedCache,
    ParquetContentAddressedCache,
)


def test_content_cache_hits_only_identical_mechanics(tmp_path) -> None:
    cache = ParquetContentAddressedCache(tmp_path, namespace="request_state")
    identity = {"day": "2026-07-25", "source": {"sha256": "a"}}
    frame = pd.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})

    first = cache.store(identity, frame, metadata={"engine": "cpp"})
    second = cache.load(identity)

    assert not first.hit
    assert second is not None and second.hit
    pd.testing.assert_frame_equal(second.frame, frame)
    assert second.manifest["metadata"]["engine"] == "cpp"
    assert cache.load({"day": "2026-07-25", "source": {"sha256": "b"}}) is None


def test_directory_cache_admits_complete_multi_file_artifact(tmp_path) -> None:
    cache = DirectoryContentAddressedCache(tmp_path, namespace="sparse_tape")
    identity = {"day": "2026-04-13", "watch_sha256": "abc"}
    build_count = 0

    def build(payload_dir):
        nonlocal build_count
        build_count += 1
        (payload_dir / "summary.json").write_text("{}\n", encoding="ascii")
        nested = payload_dir / "book"
        nested.mkdir()
        (nested / "events.bin").write_bytes(b"events")
        return {"rows": 2}

    first = cache.get_or_build(identity, build)
    second = cache.get_or_build(identity, build)

    assert not first.hit
    assert second.hit
    assert build_count == 1
    assert second.manifest["metadata"]["rows"] == 2
    assert (second.payload_dir / "book" / "events.bin").read_bytes() == b"events"


def test_directory_cache_rebuilds_incomplete_entry_under_lock(tmp_path) -> None:
    cache = DirectoryContentAddressedCache(tmp_path, namespace="sparse_tape")
    identity = {"day": "2026-04-13", "watch_sha256": "incomplete"}
    entry = cache._entry(cache.key(identity))
    entry.mkdir(parents=True)
    (entry / "orphan.tmp").write_text("partial", encoding="ascii")

    def build(payload_dir):
        (payload_dir / "summary.json").write_text("{}\n", encoding="ascii")
        return {"rows": 1}

    record = cache.get_or_build(identity, build)

    assert not record.hit
    assert not (record.entry_dir / "orphan.tmp").exists()
    assert (record.payload_dir / "summary.json").is_file()
