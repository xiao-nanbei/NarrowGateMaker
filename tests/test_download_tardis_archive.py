from datetime import date
import json
from pathlib import Path

import pytest
import zstandard

from data.downloaders.tardis_archive import (
    Contract,
    RemoteTarget,
    _days,
    _parse_contract,
    _space_preflight,
    _target,
    _validate_zstd,
    resolve_tardis_artifact_path,
)


def test_archive_output_requires_selected_retained_batch(monkeypatch, tmp_path) -> None:
    from data.downloaders import tardis_archive as archive

    monkeypatch.setattr(archive, "tardis_raw_root", lambda: None)
    with pytest.raises(ValueError, match="retained purchased archive root"):
        archive._archive_output_root(None)
    configured = tmp_path / "raw" / "retained-purchase"
    explicit = tmp_path / "raw" / "other-purchase"
    monkeypatch.setattr(archive, "tardis_raw_root", lambda: configured)
    assert archive._archive_output_root(None) == configured
    assert archive._archive_output_root(explicit) == explicit


def test_contract_and_target_identity() -> None:
    contract = _parse_contract("binance-futures,incremental_book_L2,BTCUSDC")
    assert contract == Contract("binance-futures", "incremental_book_L2", "BTCUSDC")
    url, relative = _target("https://example.test/tardis/", contract, date(2026, 1, 2))
    assert relative == (
        "binance-futures/incremental_book_L2/2026/01/02/BTCUSDC.csv.zst"
    )
    assert url == f"https://example.test/tardis/{relative}"


def test_missing_archive_root_fails_before_network(monkeypatch) -> None:
    from data.downloaders import tardis_archive as archive

    monkeypatch.setattr(archive, "tardis_raw_root", lambda: None)

    def unexpected_network(**kwargs):
        raise AssertionError("must not inspect remote files without an output root")

    monkeypatch.setattr(archive, "build_plan", unexpected_network)
    with pytest.raises(ValueError, match="retained purchased archive root"):
        archive.main(["--start", "2026-01-02", "--end", "2026-01-02",
                      "--contract", "binance-futures,trades,BTCUSDC", "--plan-only"])


def test_day_range_is_closed_and_chronological() -> None:
    assert _days(date(2026, 1, 30), date(2026, 2, 1)) == [
        date(2026, 1, 30),
        date(2026, 1, 31),
        date(2026, 2, 1),
    ]


def test_delivery_explicit_assignment_partitions_without_changing_keys():
    from data.downloaders.tardis_archive import _delivery_keys
    config = dict(start="2025-08-01", end="2025-08-03",
                  contracts=["binance-futures,incremental_book_L2,BTCUSDC"])
    keys = _delivery_keys(config)
    remote = _delivery_keys({**config, "include_keys": keys[1:]})
    local = _delivery_keys({**config, "exclude_keys": keys[1:]})
    assert remote == keys[1:] and local == keys[:1]
    assert set(remote).isdisjoint(local)
    assert set(remote + local) == set(keys)


@pytest.mark.parametrize("selection", [["../escape"], ["unknown"], "not-a-list", [1]])
def test_delivery_rejects_invalid_assignment(selection):
    from data.downloaders.tardis_archive import _DeliveryFailure, _delivery_keys
    with pytest.raises(_DeliveryFailure, match="invalid_delivery_assignment"):
        _delivery_keys(dict(start="2025-08-01", end="2025-08-02",
                            contracts=["binance-futures,trades,BTCUSDC"], include_keys=selection))


def test_space_preflight_fails_closed(monkeypatch, tmp_path: Path) -> None:
    target = RemoteTarget(
        "binance-futures",
        "book_ticker",
        "BTCUSDC",
        "2026-01-01",
        "https://example.test/file",
        "binance-futures/book_ticker/2026/01/01/BTCUSDC.csv.zst",
        True,
        100,
        "etag",
        "",
        "bytes",
        "",
    )
    usage = type("Usage", (), {"free": 50})()
    monkeypatch.setattr(
        "data.downloaders.tardis_archive.shutil.disk_usage", lambda _path: usage
    )
    with pytest.raises(RuntimeError, match="insufficient space"):
        _space_preflight(
            [target],
            tmp_path,
            reserve_gib=0.0,
            factor=2.5,
            min_free_gib=0.0,
        )


def test_zstd_validation_records_csv_boundaries(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv.zst"
    payload = b"a,b\n1,2\n3,4\n"
    path.write_bytes(zstandard.ZstdCompressor().compress(payload))
    result = _validate_zstd(path)
    assert result == {
        "decompressed_bytes": len(payload),
        "csv_rows": 2,
        "header": "a,b",
        "first_data_row": "1,2",
        "last_data_row": "3,4",
    }


def test_relocated_tardis_path_resolves_only_when_target_exists(
    monkeypatch, tmp_path: Path
) -> None:
    direct = tmp_path / "tardis"
    legacy = direct / "0730-beinan"
    relocated = direct / "binance-futures/book_ticker/file.csv.zst"
    relocated.parent.mkdir(parents=True)
    relocated.write_bytes(b"payload")
    monkeypatch.setattr("data.downloaders.tardis_archive.HISTORICAL_ARTIFACT_ROOT", direct)
    monkeypatch.setattr("data.downloaders.tardis_archive.LEGACY_OUTPUT_ROOT", legacy)

    assert resolve_tardis_artifact_path(
        legacy / "binance-futures/book_ticker/file.csv.zst"
    ) == relocated
    missing = legacy / "binance-futures/book_ticker/missing.csv.zst"
    assert resolve_tardis_artifact_path(missing) == missing


@pytest.mark.parametrize("failure", [None, "fusion", "inclusion", "changed_source"])
def test_book_archive_fusion_preserves_existing_source_and_retires_only_verified_input(tmp_path, monkeypatch, failure):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data.daily_raw import sha256_file
    from data.downloaders.tardis_archive import _publish_downloaded_book
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    source = tmp_path / "BTCUSDC.csv.zst"
    source.write_bytes(zstandard.ZstdCompressor().compress(
        b"exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        b"binance-futures,BTCUSDC,1788566400000000,1788566400001000,true,bid,100.00,2.000\n"))
    row = dict(venue="binance-futures", dataset="incremental_book_L2", symbol="BTCUSDC",
               day="2026-09-05", path=str(source), sha256=sha256_file(source))
    canonical = daily_market_path(row["day"], row["symbol"], row["dataset"])
    canonical.parent.mkdir(parents=True)
    core = ["symbol", "timestamp", "is_snapshot", "side", "price", "amount"]
    old = pa.table({"symbol": ["BTCUSDC"], "timestamp": [1788566400000000],
                    "is_snapshot": [True], "side": ["ask"], "price": ["101.00"], "amount": ["3.000"]})
    pq.write_table(old, canonical)

    def fuse(sources, output, day, *, symbol, **_kwargs):
        assert set(sources) == {"tardis"} and output == canonical
        if failure == "fusion":
            raise ValueError("injected fusion failure")
        stage = sources["tardis"]
        table = pq.read_table(stage)
        entry = dict(source_id="tardis", sha256=sha256_file(stage), rows=len(table))
        if failure == "inclusion":
            entry["sha256"] = "f" * 64
        combined = pa.concat_tables([old, table.select(core).replace_schema_metadata(None)])
        pq.write_table(combined.replace_schema_metadata({
            b"narrowgate.included_sources": json.dumps([entry]).encode()}), output)
        if failure == "changed_source":
            source.write_bytes(b"new unique source")
        return dict(sha256=sha256_file(output), included_sources=[entry], rows=len(combined))

    monkeypatch.setattr("data.daily_raw.fuse_orderbook_day", fuse)
    saved = []
    if failure:
        with pytest.raises(ValueError):
            _publish_downloaded_book(row, keep_archive=False, persist=saved.append)
        assert source.exists() and source.with_suffix(".fusion-input.parquet").exists()
        assert not saved
    else:
        result = _publish_downloaded_book(row, keep_archive=False, persist=saved.append)
        assert result["archive_retired"] and result["derived_rebuilt"] is False
        assert saved[0]["retirement_intent"] and not saved[0]["archive_retired"]
        assert not source.exists() and not source.with_suffix(".fusion-input.parquet").exists()
        assert pq.read_table(canonical)["price"].to_pylist() == ["101.00", "100.00"]


def test_book_retirement_intent_resumes_after_unlink_but_rejects_changed_remaining_input(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data.daily_raw import sha256_file
    from data.downloaders.tardis_archive import _finish_book_retirement
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    source = tmp_path / "already-unlinked.csv.zst"
    stage = source.with_suffix(".fusion-input.parquet")
    stage.write_bytes(b"verified converted source")
    row = dict(day="2026-09-05", symbol="BTCUSDC", path=str(source),
               sha256="a" * 64, converted_source_path=str(stage), converted_source_sha256=sha256_file(stage),
               retirement_intent=True)
    canonical = daily_market_path(row["day"], row["symbol"], "incremental_book_L2")
    canonical.parent.mkdir(parents=True)
    pq.write_table(pa.table({"timestamp": [1788566400000000]}).replace_schema_metadata({
        b"narrowgate.included_sources": json.dumps([dict(source_id="tardis", sha256=row["converted_source_sha256"])]).encode()}), canonical)
    stage.write_bytes(b"changed unique input")
    with pytest.raises(ValueError, match="changed before source retirement"):
        _finish_book_retirement(row)
    assert stage.exists()
    stage.write_bytes(b"verified converted source")
    assert _finish_book_retirement(row)["archive_retired"] and not stage.exists()
    assert _finish_book_retirement(row)["archive_retired"]


@pytest.mark.parametrize("dataset,symbol", [("book_ticker", "BTCUSDC"), ("incremental_book_L2", "BTCUSDT")])
def test_nonexecution_book_contracts_remain_archive_only(dataset, symbol):
    from data.downloaders.tardis_archive import _publish_downloaded_book

    row = dict(venue="binance-futures", dataset=dataset, symbol=symbol, path="not-opened")
    assert _publish_downloaded_book(row, keep_archive=False) == row


def test_cli_resumes_retired_book_without_redownload_or_losing_intent(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    import data.downloaders.tardis_archive as downloader
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    relative = "binance-futures/incremental_book_L2/2026/09/05/BTCUSDC.csv.zst"
    target = RemoteTarget("binance-futures", "incremental_book_L2", "BTCUSDC", "2026-09-05",
                          "https://example.test/" + relative, relative, True, 123, "fixed-etag", "", "bytes", "")
    row = {**target.__dict__, "path": str(tmp_path / relative), "sha256": "a" * 64,
           "converted_source_path": str((tmp_path / relative).with_suffix(".fusion-input.parquet")), "converted_source_sha256": "b" * 64,
           "retirement_intent": True, "archive_retired": False}
    manifest = tmp_path / "download.json"
    manifest.write_text(json.dumps({"downloads": [row]}))
    canonical = daily_market_path(row["day"], row["symbol"], "incremental_book_L2")
    canonical.parent.mkdir(parents=True)
    pq.write_table(pa.table({"timestamp": [1788566400000000]}).replace_schema_metadata({
        b"narrowgate.included_sources": json.dumps([dict(source_id="tardis", sha256="b" * 64)]).encode()}), canonical)
    monkeypatch.setattr(downloader, "build_plan", lambda **kwargs: [target])
    monkeypatch.setattr(downloader, "_space_preflight", lambda *args, **kwargs: {})
    monkeypatch.setattr(downloader, "_download_one", lambda *args, **kwargs: pytest.fail("already published and retired"))
    assert downloader.main(["--output-root", str(tmp_path), "--start", row["day"], "--end", row["day"],
                            "--contract", "binance-futures,incremental_book_L2,BTCUSDC", "--manifest", str(manifest)]) == 0
    result = json.loads(manifest.read_text())
    assert result["complete"] and result["downloads"][0]["archive_retired"]


@pytest.mark.parametrize("interrupt_before_link", [False, True])
def test_pending_tardis_merge_keeps_native_context_and_retires_after_next_day(tmp_path, monkeypatch, interrupt_before_link):
    import os

    import pyarrow as pa
    import pyarrow.parquet as pq

    from data.daily_raw import BOOK_SCHEMA, sha256_file
    from data.downloaders.tardis_archive import _publish_downloaded_book
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    source = tmp_path / "BTCUSDC.csv.zst"
    source.write_bytes(zstandard.ZstdCompressor().compress(
        b"exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        b"binance-futures,BTCUSDC,1788566400000000,1788566400001000,true,bid,100.00,2.000\n"))
    row = dict(venue="binance-futures", dataset="incremental_book_L2", symbol="BTCUSDC",
               day="2026-09-05", path=str(source), sha256=sha256_file(source))
    canonical = daily_market_path(row["day"], row["symbol"], row["dataset"])
    canonical.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([dict(exchange="binance-futures", symbol="BTCUSDC",
        timestamp=1788566400000000, event_time=1788566400000, received_time=1788566400000000000,
        source_hour=0, is_snapshot=True, side="ask", price="101.00", amount="3.00")], schema=BOOK_SCHEMA), canonical)
    native_sha = sha256_file(canonical)
    calls = []

    def fuse(sources, output, day, *, next_sources, **_kwargs):
        assert set(sources) == {"tardis", "cryptohft"}
        assert sha256_file(sources["cryptohft"]) == native_sha
        calls.append(bool(next_sources))
        included = [dict(source_id=key, sha256=sha256_file(path), rows=pq.ParquetFile(path).metadata.num_rows)
                    for key, path in sources.items()]
        temporary = output.with_suffix(".test-publish.parquet")
        pq.write_table(pa.table({"timestamp": [1788566400000000]}).replace_schema_metadata({
            b"narrowgate.book_fusion": b"reconstructed_fusion.v1",
            b"narrowgate.included_sources": json.dumps(included).encode()}), temporary)
        temporary.replace(output)
        return dict(sha256=sha256_file(output), included_sources=included, rows=1)

    monkeypatch.setattr("data.daily_raw.fuse_orderbook_day", fuse)
    saved = []
    if interrupt_before_link:
        original = os.link
        monkeypatch.setattr(os, "link", lambda *_args: (_ for _ in ()).throw(InterruptedError("injected")))
        # Both supported filesystem-copy paths are interrupted after the durable
        # intent, before any native context is available.
        monkeypatch.setattr("data.downloaders.tardis_archive.shutil.copyfile",
                            lambda *_args: (_ for _ in ()).throw(RuntimeError("copy interrupted")))
        with pytest.raises(RuntimeError, match="copy interrupted"):
            _publish_downloaded_book(row, keep_archive=False, persist=saved.append)
        assert not source.with_suffix(".cryptohft-context.parquet").exists()
        row = saved[-1]
        monkeypatch.setattr(os, "link", original)
    first = _publish_downloaded_book(row, keep_archive=False, persist=saved.append)
    assert first["status"] == "PUBLISHED_BOUNDARY_PENDING" and not first["archive_retired"]
    assert source.exists() and source.with_suffix(".fusion-input.parquet").exists()
    context = source.with_suffix(".cryptohft-context.parquet")
    assert context.is_file() and sha256_file(context) == native_sha
    assert calls == [False]
    next_source = daily_market_path("2026-09-06", "BTCUSDC", "incremental_book_L2")
    next_source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"event_time": [1788652800000] * 24,
                             "received_time": [1788652800000000000] * 24,
                             "source_hour": list(range(24))}), next_source)
    final = _publish_downloaded_book(first, keep_archive=False, persist=saved.append)
    assert calls == [False, True] and final["archive_retired"]
    assert not source.exists() and not context.exists()
    assert not source.with_suffix(".fusion-input.parquet").exists()
    assert next_source.exists()  # An adjacent input is never part of this retirement.


def test_cli_refreshes_prior_pending_ingestion_without_redownloading_it(tmp_path, monkeypatch):
    import data.downloaders.tardis_archive as downloader

    old_relative = "binance-futures/incremental_book_L2/2026/09/05/BTCUSDC.csv.zst"
    old = dict(relative_path=old_relative, path=str((tmp_path / old_relative).resolve()), day="2026-09-05",
               venue="binance-futures", dataset="incremental_book_L2", symbol="BTCUSDC",
               boundary_status="BOUNDARY_PENDING")
    new_relative = "binance-futures/book_ticker/2026/09/06/BTCUSDC.csv.zst"
    target = RemoteTarget("binance-futures", "book_ticker", "BTCUSDC", "2026-09-06",
                          "https://example.test/" + new_relative, new_relative, True, 123, "etag", "", "bytes", "")
    manifest = tmp_path / "download.json"
    manifest.write_text(json.dumps({"downloads": [old]}))
    monkeypatch.setattr(downloader, "build_plan", lambda **_kwargs: [target])
    monkeypatch.setattr(downloader, "_space_preflight", lambda *_args, **_kwargs: {})
    downloaded, published = [], []

    def download(item, root, **_kwargs):
        downloaded.append(item.day)
        return {**item.__dict__, "path": str(root / item.relative_path), "size_bytes": 123, "sha256": "a" * 64}

    def publish(row, **_kwargs):
        published.append(row["day"])
        return {**row, "boundary_status": "EXISTING_CANONICAL_BOUNDARY_VERIFIED", "archive_retired": True}

    monkeypatch.setattr(downloader, "_download_one", download)
    monkeypatch.setattr(downloader, "_publish_downloaded_book", publish)
    assert downloader.main(["--output-root", str(tmp_path), "--start", "2026-09-06", "--end", "2026-09-06",
                            "--contract", "binance-futures,book_ticker,BTCUSDC", "--manifest", str(manifest)]) == 0
    assert downloaded == ["2026-09-06"] and published == ["2026-09-06", "2026-09-05"]
    result = json.loads(manifest.read_text())
    assert result["complete"] and result["boundary_pending_days"] == [] and len(result["downloads"]) == 2


class DeliveryResponse:
    def __init__(self, payload=b"", *, status=200, headers=None, fail_after=None):
        self.status_code = status
        self.headers = headers or {}
        self.url = "https://objects.example.test/private/BTCUSDC.csv.xz?signature=DO_NOT_LOG"
        self.history = [type("Redirect", (), {"status_code": 307})()]
        self.payload = payload
        self.fail_after = fail_after

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, _size):
        import requests

        if self.fail_after is not None:
            yield self.payload[:self.fail_after]
            raise requests.ConnectionError("signed https://example.test/?token=DO_NOT_LOG")
        yield self.payload


def delivery_setup(tmp_path, monkeypatch, responses):
    import data.downloaders.tardis_archive as module

    root = tmp_path / "incoming"
    root.mkdir()
    key = "binance-futures/trades/2025/08/01/BTCUSDC"
    config = dict(attempts=3, max_polls=3, poll_interval=300, timeout_s=1)
    budget = module._DeliveryBudget(root, 0, 1000000)
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        assert kwargs["stream"] and kwargs["allow_redirects"]
        return responses.pop(0)

    monkeypatch.setattr(module.requests, "get", get)
    monkeypatch.setattr(module.requests, "head", lambda *a, **k: pytest.fail("GET-only delivery"))
    return module, root, key, config, budget, calls


def delivery_payload(codec="zst", dataset="trades"):
    import lzma

    payload = b"exchange,symbol,timestamp,local_timestamp,id,side,price,amount\nbinance-futures,BTCUSDC,1,2,3,buy,100,1\n"
    if dataset == "incremental_book_L2":
        payload = payload.replace(b",id,", b",is_snapshot,")
    return lzma.compress(payload) if codec == "xz" else zstandard.ZstdCompressor(write_checksum=True).compress(payload)


@pytest.mark.parametrize('bad_second', [False, True])
def test_delivery_full_csv_identity_checks_after_first_row(tmp_path, bad_second):
    from data.downloaders.tardis_archive import _validate_delivery_content, _DeliveryFailure
    p = tmp_path / 'download.part'
    symbol = 'BTCUSDC' if bad_second else 'BTCUSDT'
    content = ('exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n'
               'binance-futures,BTCUSDT,1754006400000000,0,true,bid,100,1\n'
               f'binance-futures,{symbol},1754006400100000,0,true,ask,101,1\n')
    p.write_bytes(zstandard.ZstdCompressor().compress(content.encode()))
    key = 'binance-futures/incremental_book_L2/2025/08/01/BTCUSDT'
    if bad_second:
        with pytest.raises(_DeliveryFailure, match='content_market_mismatch'):
            _validate_delivery_content(p, key)
    else:
        result = _validate_delivery_content(p, key)
        assert result['content_rows'] == 2 and result['content_identity_verified']
        assert result['maximum_recorded_exchange_gap_us'] == 100000
        assert result['adjacent_exchange_day_rows'] == 0
        assert not result['full_exchange_coverage_proven']


def test_delivery_named_assignment_locks_and_separate_manifests(tmp_path):
    from data.downloaders.tardis_archive import _delivery_assignment, _DeliveryFailure
    with _delivery_assignment(tmp_path, 'main') as main:
        with _delivery_assignment(tmp_path, 'retry') as retry:
            assert main != retry
        with pytest.raises(_DeliveryFailure, match='assignment_already_running'):
            with _delivery_assignment(tmp_path, 'main'):
                pass
        with pytest.raises(_DeliveryFailure, match='delivery_already_running'):
            with _delivery_assignment(tmp_path, None):
                pass
    with _delivery_assignment(tmp_path, None):
        with pytest.raises(_DeliveryFailure, match='delivery_already_running'):
            with _delivery_assignment(tmp_path, 'retry'):
                pass
    with pytest.raises(_DeliveryFailure, match='invalid_delivery_assignment_name'):
        with _delivery_assignment(tmp_path, '../escape'):
            pass


def test_delivery_concurrent_same_key_downloads_only_once(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    response = DeliveryResponse(delivery_payload(), headers={'Content-Disposition': 'attachment; filename="BTCUSDC.csv.zst"'})
    module, root, key, config, budget, calls = delivery_setup(tmp_path, monkeypatch, [response])
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: module._delivery_step(key, root, 'https://example.test', config, budget), range(2)))
    assert all(r['status'] == 'completed' for r in results)
    assert len(calls) == 1


def test_delivery_named_assignment_preserves_existing_unregistered_file(tmp_path, monkeypatch):
    module, root, key, config, budget, calls = delivery_setup(tmp_path, monkeypatch, [])
    archive = root / (key + '.csv.zst')
    archive.parent.mkdir(parents=True)
    archive.write_bytes(delivery_payload())
    result = module._delivery_step(key, root, 'https://example.test', {**config, 'assignment_name': 'retry'}, budget)
    assert result['error_code'] == 'existing_archive_requires_validation'
    assert result['status'] == 'failed' and not calls
    assert archive.read_bytes() == delivery_payload()


@pytest.mark.parametrize("codec", ["xz", "zst", "zstd"])
def test_private_delivery_redirect_format_magic_and_repeat_zero_network(tmp_path, monkeypatch, codec):
    payload = delivery_payload(codec)
    response = DeliveryResponse(payload, headers={"Content-Disposition": f'attachment; filename="BTCUSDC.csv.{codec}"'})
    module, root, key, config, budget, calls = delivery_setup(tmp_path, monkeypatch, [response])
    state = module._delivery_step(key, root, "https://delivery.example.test/PRIVATE_TOKEN", config, budget)
    assert state["status"] == "completed" and state["compression_verified"]
    assert state["compression"] == codec and state["total_bytes"] is None
    assert (root / (key + ".csv." + codec)).read_bytes() == payload
    assert calls[0][0].endswith("BTCUSDC.csv.xz")
    assert calls[0][1]["headers"] == {"Accept-Encoding": "identity"}
    assert module._delivery_step(key, root, "https://delivery.example.test/PRIVATE_TOKEN", config, budget) == state
    assert len(calls) == 1
    saved = (root / (".delivery-state/" + key + ".json")).read_text()
    assert "PRIVATE_TOKEN" not in saved and "DO_NOT_LOG" not in saved and "https://" not in saved


@pytest.mark.parametrize("codec", ["xz", "zst", "zstd"])
@pytest.mark.parametrize("damage", ["truncated", "trailing", "partial_second"])
def test_private_delivery_rejects_incomplete_unknown_length_stream(tmp_path, monkeypatch, codec, damage):
    payload = delivery_payload(codec)
    damaged = payload[:-1] if damage == "truncated" else payload + (b"junk" if damage == "trailing" else payload[:3])
    response = DeliveryResponse(damaged, headers={"Content-Disposition": f'attachment; filename="BTCUSDC.csv.{codec}"'})
    module, root, key, config, budget, _ = delivery_setup(tmp_path, monkeypatch, [response])
    state = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert state["status"] == "pending" and state["restart_required"]
    assert not (root / (key + ".csv." + codec)).exists()
    assert (root / (key + ".part")).exists()


@pytest.mark.parametrize("codec", ["xz", "zst", "zstd"])
def test_archive_verifier_checks_all_concatenated_frames(tmp_path, codec):
    from data.downloaders.tardis_archive import _validate_archive

    path = tmp_path / "frames"
    payload = delivery_payload(codec)
    path.write_bytes(payload + (b"\x00" * 4 if codec == "xz" else b"") + payload)
    assert _validate_archive(path, codec)["compression_verified"]


@pytest.mark.parametrize("code", [202, 429, 503])
def test_delivery_pending_and_backoff_are_durable_and_respect_retry_after(tmp_path, monkeypatch, code):
    import time

    module, root, key, config, budget, _ = delivery_setup(
        tmp_path, monkeypatch, [DeliveryResponse(status=code, headers={"Retry-After": "900"})])
    before = time.time()
    state = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert state["status"] == "pending" and state["next_attempt_at"] >= before + 900
    assert module._delivery_saved(root, key)[1] == state
    if code == 429:
        assert budget.cooldown_until >= before + 900


def test_delivery_resume_binds_etag_range_and_total_then_rejects_wrong_range(tmp_path, monkeypatch):
    payload = delivery_payload()
    headers = {"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"',
               "Content-Length": str(len(payload)), "ETag": '"same"'}
    responses = [DeliveryResponse(payload, headers=headers, fail_after=20),
                 DeliveryResponse(payload[20:], status=206, headers={**headers,
                     "Content-Length": str(len(payload) - 20), "Content-Range": f"bytes 21-{len(payload)-1}/{len(payload)}"}),
                 DeliveryResponse(payload, headers=headers)]
    module, root, key, config, budget, calls = delivery_setup(tmp_path, monkeypatch, responses)
    first = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert first["status"] == "pending" and first["part_bytes"] == 20
    assert "DO_NOT_LOG" not in json.dumps(first)
    second = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert second["error_code"] == "range_identity_mismatch" and second["restart_required"]
    assert calls[1][1]["headers"]["Range"] == "bytes=20-"
    assert calls[1][1]["headers"]["If-Range"] == '"same"'
    third = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert third["status"] == "completed" and "Range" not in calls[2][1]["headers"]
    assert (root / (key + ".csv.zst")).read_bytes() == payload


def test_delivery_valid_range_resume_and_unknown_validator_restart(tmp_path, monkeypatch):
    payload = delivery_payload()
    headers = {"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"',
               "Content-Length": str(len(payload)), "ETag": '"same"'}
    responses = [DeliveryResponse(payload, headers=headers, fail_after=20),
                 DeliveryResponse(payload[20:], status=206, headers={**headers,
                     "Content-Length": str(len(payload) - 20), "Content-Range": f"bytes 20-{len(payload)-1}/{len(payload)}"})]
    module, root, key, config, budget, _ = delivery_setup(tmp_path, monkeypatch, responses)
    module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert module._delivery_step(key, root, "https://example.test/private", config, budget)["status"] == "completed"


def test_delivery_unknown_length_interruption_restarts_not_appends(tmp_path, monkeypatch):
    payload = delivery_payload()
    headers = {"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"'}
    responses = [DeliveryResponse(payload, headers=headers, fail_after=20), DeliveryResponse(payload, headers=headers)]
    module, root, key, config, budget, calls = delivery_setup(tmp_path, monkeypatch, responses)
    module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert module._delivery_step(key, root, "https://example.test/private", config, budget)["status"] == "completed"
    assert "Range" not in calls[1][1]["headers"]
    assert (root / (key + ".csv.zst")).read_bytes() == payload


def delivery_config(tmp_path, root):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"base_url": "https://example.test/PRIVATE_TOKEN", "output_root": str(root),
                               "start": "2025-08-01", "end": "2025-08-01", "workers": 1,
                               "reserve_gib": 0, "requests_per_second": 1000000,
                               "contracts": ["binance-futures,incremental_book_L2,BTCUSDC"]}))
    path.chmod(0o600)
    return path


def test_private_delivery_cli_archive_only_never_publishes_or_retires(tmp_path, monkeypatch, capsys):
    module, root, _, _, _, calls = delivery_setup(tmp_path, monkeypatch, [DeliveryResponse(
        delivery_payload(dataset="incremental_book_L2"), headers={"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"'})])
    for name in ("_publish_downloaded_book", "_finish_book_retirement", "build_plan"):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("archive-only isolation"))
    config = delivery_config(tmp_path, root)
    assert module.main(["--delivery-config", str(config), "--archive-only"]) == 0
    assert module.main(["--delivery-config", str(config), "--archive-only"]) == 0
    assert len(calls) == 1
    assert "PRIVATE_TOKEN" not in capsys.readouterr().out
    assert json.loads((root / "manifest.json").read_text())["complete"]


def test_private_delivery_duplicate_start_lock_and_required_isolation(tmp_path, monkeypatch):
    import fcntl

    module, root, _, _, _, calls = delivery_setup(tmp_path, monkeypatch, [])
    config = delivery_config(tmp_path, root)
    assert module.main(["--delivery-config", str(config)]) == 2
    with (root / ".delivery.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert module.main(["--delivery-config", str(config), "--archive-only"]) == 2
    assert not calls


@pytest.mark.parametrize("code", [404, 410, 401, 403, 409])
def test_private_delivery_permanent_responses_do_not_retry_on_restart(tmp_path, monkeypatch, code):
    module, root, _, _, _, calls = delivery_setup(tmp_path, monkeypatch, [DeliveryResponse(status=code)])
    config = delivery_config(tmp_path, root)
    assert module.main(["--delivery-config", str(config), "--archive-only"]) == 2
    assert module.main(["--delivery-config", str(config), "--archive-only"]) == 2
    assert len(calls) == 1


@pytest.mark.parametrize("replace_from,replace_to", [(b"BTCUSDC", b"BTCUSDT"),
                                                   (b"binance-futures", b"other-exchange"),
                                                   (b",id,", b",missing,")])
def test_delivery_rejects_wrong_csv_identity(tmp_path, monkeypatch, replace_from, replace_to):
    payload = zstandard.ZstdDecompressor().decompress(delivery_payload()).replace(replace_from, replace_to)
    compressed = zstandard.ZstdCompressor().compress(payload)
    module, root, key, config, budget, _ = delivery_setup(tmp_path, monkeypatch, [DeliveryResponse(
        compressed, headers={"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"'})])
    state = module._delivery_step(key, root, "https://example.test/private", config, budget)
    assert state["error_code"] == "delivered_csv_identity_mismatch"
    assert state["status"] != "completed" and not (root / (key + ".csv.zst")).exists()


def test_delivery_manifest_precedes_first_request(tmp_path, monkeypatch):
    module, root, _, _, _, _ = delivery_setup(tmp_path, monkeypatch, [])
    config = delivery_config(tmp_path, root)

    def get(*args, **kwargs):
        manifest = json.loads((root / "manifest.json").read_text())
        assert manifest["targets"] == 1 and manifest["start"] == "2025-08-01"
        assert manifest["counts"] == {"queued": 1} and manifest["complete"] is False
        return DeliveryResponse(status=404)

    monkeypatch.setattr(module.requests, "get", get)
    assert module.main(["--delivery-config", str(config), "--archive-only"]) == 2


def test_delivery_priority_preserves_all_targets_and_default_order():
    from data.downloaders.tardis_archive import _delivery_keys

    contracts = ["binance-futures,incremental_book_L2,BTCUSDT", "binance-futures,trades,BTCUSDT",
                 "binance-futures,incremental_book_L2,BTCUSDC", "binance-futures,trades,BTCUSDC"]
    config = dict(start="2025-08-01", end="2026-09-11", contracts=contracts)
    original = _delivery_keys(config)
    assert len(original) == len(set(original)) == 1628
    assert original[:2] == ["binance-futures/incremental_book_L2/2025/08/01/BTCUSDT",
                            "binance-futures/trades/2025/08/01/BTCUSDT"]
    preferred = {**config, "priority_ranges": [{"start": "2026-08-01", "end": "2026-08-31"}],
                 "priority_contracts": ["binance-futures,incremental_book_L2,BTCUSDC"]}
    ordered = _delivery_keys(preferred)
    assert set(ordered) == set(original)
    assert ordered[:31] == [f"binance-futures/incremental_book_L2/2026/08/{day:02d}/BTCUSDC"
                            for day in range(1, 32)]
    assert all("/2026/08/" in key for key in ordered[:124])
    assert all("/2026/08/" not in key for key in ordered[124:])


def test_delivery_priority_channel_precedes_other_channels_across_gap_dates():
    from data.downloaders.tardis_archive import _delivery_keys

    config = dict(start="2025-08-01", end="2025-08-05", contracts=[
        "binance-futures,trades,BTCUSDT",
        "binance-futures,incremental_book_L2,BTCUSDC",
    ])
    original = _delivery_keys(config)
    ordered = _delivery_keys({**config, "priority_ranges": [
        {"start": "2025-08-04", "end": "2025-08-04"},
        {"start": "2025-08-02", "end": "2025-08-02"},
        {"start": "2025-08-02", "end": "2025-08-04"},
    ], "priority_contracts": ["binance-futures,incremental_book_L2,BTCUSDC"]})
    assert len(ordered) == len(set(ordered)) == len(original)
    assert set(ordered) == set(original)
    assert ordered[:3] == [f"binance-futures/incremental_book_L2/2025/08/{day:02d}/BTCUSDC"
                           for day in (4, 2, 3)]
    assert ordered[3:6] == [f"binance-futures/trades/2025/08/{day:02d}/BTCUSDT"
                            for day in (4, 2, 3)]
    assert ordered[6:] == [key for key in original if "/01/" in key or "/05/" in key]


def test_delivery_priority_whole_symbol_retains_gap_and_month_priority():
    from data.downloaders.tardis_archive import _delivery_keys

    contracts = [f"binance-futures,{channel},{symbol}"
                 for symbol in ("BTCUSDT", "BTCUSDC")
                 for channel in ("incremental_book_L2", "trades")]
    config = dict(start="2025-08-01", end="2026-09-11", contracts=contracts)
    original = _delivery_keys(config)
    ordered = _delivery_keys({**config, "priority_contracts": [
        "binance-futures,incremental_book_L2,BTCUSDC",
        "binance-futures,trades,BTCUSDC"], "priority_ranges": [
            {"start": "2025-08-29", "end": "2025-08-29"},
            {"start": "2026-08-22", "end": "2026-08-22"},
            {"start": "2026-08-01", "end": "2026-08-31"},
            {"start": config["start"], "end": config["end"]},
        ]})
    assert len(ordered) == len(set(ordered)) == 1628
    assert set(ordered) == set(original)
    assert all(key.endswith("/BTCUSDC") for key in ordered[:814])
    assert all(key.endswith("/BTCUSDT") for key in ordered[814:])
    for offset, channel in ((0, "incremental_book_L2"), (407, "trades")):
        block = ordered[offset:offset + 407]
        assert all(f"/{channel}/" in key for key in block)
        assert block[:2] == [f"binance-futures/{channel}/{day}/BTCUSDC"
                             for day in ("2025/08/29", "2026/08/22")]
        assert all("/2026/08/" in key for key in block[2:32])


@pytest.mark.parametrize("priority", [
    {"priority_ranges": [{"start": "2025-07-31", "end": "2025-08-01"}]},
    {"priority_ranges": [{"start": "2025-08-02", "end": "2025-08-01"}]},
    {"priority_contracts": ["binance-futures,trades,BTCUSDT"]},
    {"priority_contracts": ["binance-futures,trades,BTCUSDC"] * 2},
])
def test_delivery_priority_rejects_unrequested_or_invalid_scope(priority):
    from data.downloaders.tardis_archive import _DeliveryFailure, _delivery_keys

    with pytest.raises(_DeliveryFailure, match="priority_"):
        _delivery_keys(dict(start="2025-08-01", end="2025-08-02",
                            contracts=["binance-futures,trades,BTCUSDC"], **priority))


def test_delivery_scheduler_keeps_only_worker_count_outstanding(tmp_path, monkeypatch):
    module, root, _, _, _, _ = delivery_setup(tmp_path, monkeypatch, [])
    config_path = delivery_config(tmp_path, root)
    config = json.loads(config_path.read_text())
    config.update(end="2025-08-10", workers=2)
    config_path.write_text(json.dumps(config))
    original_executor = module.concurrent.futures.ThreadPoolExecutor
    original_wait = module.concurrent.futures.wait
    outstanding = submitted = waits = 0

    class BoundedExecutor(original_executor):
        def submit(self, *args, **kwargs):
            nonlocal outstanding, submitted
            outstanding += 1
            submitted += 1
            assert outstanding <= 2  # Includes finished but unconsumed work.
            return super().submit(*args, **kwargs)

    def wait(futures, **kwargs):
        nonlocal outstanding, waits
        waits += 1
        done, pending = original_wait(futures, **kwargs)
        outstanding -= len(done)
        return done, pending

    monkeypatch.setattr(module.concurrent.futures, "ThreadPoolExecutor", BoundedExecutor)
    monkeypatch.setattr(module.concurrent.futures, "wait", wait)
    monkeypatch.setattr(module.requests, "get", lambda *_args, **_kwargs: DeliveryResponse(status=404))
    assert module.main(["--delivery-config", str(config_path), "--archive-only"]) == 2
    assert submitted == 10 and outstanding == 0 and waits > 0
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["targets"] == 10 and manifest["counts"] == {"unavailable": 10}
    assert manifest["active_transfers"] == 0


@pytest.mark.parametrize("code", [202, 503])
def test_delivery_due_retries_do_not_starve_unchecked_files(tmp_path, monkeypatch, code):
    import time

    module, root, _, _, _, _ = delivery_setup(tmp_path, monkeypatch, [])
    config_path = delivery_config(tmp_path, root)
    config = json.loads(config_path.read_text())
    config["end"] = "2025-08-03"
    config_path.write_text(json.dumps(config))
    keys = module._delivery_keys(config)
    for index, key in enumerate(keys[:2]):
        path, _ = module._delivery_saved(root, key)
        module._atomic_json(dict(key=key, status="pending", next_attempt_at=index + 1,
                                 http_status=code, error_code=f"HTTP_{code}"), path)
    calls = []

    def step(key, *_args):
        calls.append(key)
        if key == keys[0] and calls.count(key) == 1:
            # Even if the next request takes longer than the retry delay, the
            # oldest waiting file must progress before this repeat poll.
            return dict(key=key, status="pending", next_attempt_at=time.time() - 1,
                        http_status=code, error_code=f"HTTP_{code}")
        return dict(key=key, status="unavailable")

    monkeypatch.setattr(module, "_delivery_step", step)
    assert module.main(["--delivery-config", str(config_path), "--archive-only"]) == 2
    assert calls == [keys[2], keys[0], keys[1], keys[0]]
    assert json.loads((root / "manifest.json").read_text())["targets"] == 3


def test_delivery_sigterm_stops_dispatch_saves_partial_and_resumes_without_failures(tmp_path, monkeypatch):
    import signal

    payload = delivery_payload(dataset="incremental_book_L2")
    headers = {"Content-Disposition": 'attachment; filename="BTCUSDC.csv.zst"',
               "Content-Length": str(len(payload)), "ETag": '"same"'}
    module, root, _, _, _, calls = delivery_setup(tmp_path, monkeypatch, [])
    config_path = delivery_config(tmp_path, root)
    config = json.loads(config_path.read_text())
    config["end"] = "2025-08-03"
    config_path.write_text(json.dumps(config))
    handlers = {}
    previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    monkeypatch.setattr(module.signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    class InterruptedResponse(DeliveryResponse):
        def iter_content(self, _size):
            yield self.payload[:20]
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            yield self.payload[20:]

    def interrupted_get(url, **kwargs):
        calls.append((url, kwargs))
        return InterruptedResponse(payload, headers=headers)

    monkeypatch.setattr(module.requests, "get", interrupted_get)
    assert module.main(["--delivery-config", str(config_path), "--archive-only"]) == 143
    key = "binance-futures/incremental_book_L2/2025/08/01/BTCUSDC"
    state = module._delivery_saved(root, key)[1]
    assert state["status"] == "pending" and state["error_code"] == "delivery_stop_requested"
    assert state.get("failures", 0) == 0 and not state["restart_required"]
    assert state["part_bytes"] == 20 and (root / (key + ".part")).read_bytes() == payload[:20]
    assert len(calls) == 1 and handlers == previous_handlers
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["targets"] == 3 and manifest["counts"] == {"pending": 1, "queued": 2}
    assert manifest["stop_signal"] == signal.SIGTERM and manifest["active_transfers"] == 0

    def resumed_get(url, **kwargs):
        calls.append((url, kwargs))
        if "/08/01/" in url:
            assert kwargs["headers"]["Range"] == "bytes=20-"
            assert kwargs["headers"]["If-Range"] == '"same"'
            return DeliveryResponse(payload[20:], status=206, headers={**headers,
                "Content-Length": str(len(payload) - 20), "Content-Range": f"bytes 20-{len(payload)-1}/{len(payload)}"})
        return DeliveryResponse(payload, headers=headers)

    monkeypatch.setattr(module.requests, "get", resumed_get)
    assert module.main(["--delivery-config", str(config_path), "--archive-only"]) == 0
    assert len(calls) == 4 and handlers == previous_handlers
    state = module._delivery_saved(root, key)[1]
    assert state["status"] == "completed" and state.get("failures", 0) == 0
    assert json.loads((root / "manifest.json").read_text())["counts"] == {"completed": 3}


def test_delivery_priorities_do_not_reset_existing_retry_after(tmp_path, monkeypatch):
    import signal
    import time

    module, root, _, _, _, calls = delivery_setup(tmp_path, monkeypatch, [])
    config_path = delivery_config(tmp_path, root)
    config = json.loads(config_path.read_text())
    config.update(end="2025-08-02", priority_ranges=[{"start": "2025-08-02", "end": "2025-08-02"}])
    config_path.write_text(json.dumps(config))
    key = "binance-futures/incremental_book_L2/2025/08/02/BTCUSDC"
    state_path, _ = module._delivery_saved(root, key)
    state = {"key": key, "status": "pending", "polls": 3, "next_attempt_at": time.time() + 900,
             "error_code": "HTTP_202", "http_status": 202}
    module._atomic_json(state, state_path)
    handlers = {}
    monkeypatch.setattr(module.signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    def get(url, **kwargs):
        calls.append((url, kwargs))
        assert "/08/01/" in url  # The priority target is not due yet.
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return DeliveryResponse(status=404)

    monkeypatch.setattr(module.requests, "get", get)
    assert module.main(["--delivery-config", str(config_path), "--archive-only"]) == 143
    assert len(calls) == 1 and module._delivery_saved(root, key)[1] == state
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["targets"] == 2 and manifest["counts"] == {"pending": 1, "unavailable": 1}


def test_delivery_real_sigterm_exits_gracefully_without_dispatching_next_target(tmp_path):
    import selectors
    import signal
    import subprocess
    import sys

    root = tmp_path / "incoming"
    config_path = delivery_config(tmp_path, root)
    config = json.loads(config_path.read_text())
    config["end"] = "2025-08-03"
    config_path.write_text(json.dumps(config))
    code = """
import sys
import data.downloaders.tardis_archive as module
def step(key, root, base_url, config, budget):
    print('ACTIVE_TARGET', flush=True)
    assert budget.stopping.wait(10), 'SIGTERM was not received'
    return dict(key=key, status='pending')
module._delivery_step = step
raise SystemExit(module.main(sys.argv[1:]))
"""
    process = subprocess.Popen([sys.executable, "-c", code, "--delivery-config", str(config_path), "--archive-only"],
                               cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "downloader did not reach active target"
            assert process.stdout.readline().strip() == "ACTIVE_TARGET"
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 143, stderr
        assert "ACTIVE_TARGET" not in stdout and stderr == ""
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["stop_signal"] == signal.SIGTERM and manifest["active_transfers"] == 0
    assert manifest["targets"] == 3 and manifest["counts"] == {"pending": 1, "queued": 2}
