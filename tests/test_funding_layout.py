import hashlib
import json

import pandas as pd
import pytest

from data.migrate_funding_layout import migrate
from data_paths import daily_market_path


def prepare(root):
    old = root / "raw/binance_futures/BTCUSDC/2026-09-11/funding.parquet"
    old.parent.mkdir(parents=True)
    pd.DataFrame([{"symbol": "BTCUSDC", "fundingTime": 100,
                   "fundingRate": "0.001", "markPrice": "100"}]).to_parquet(old)
    (root / "raw/daily-index.json").write_text(json.dumps({"records": [
        {"channel": "trades", "path": "untouched"}]}))
    return old


def test_migration_preserves_bytes_updates_index_and_resumes(tmp_path):
    old = prepare(tmp_path)
    digest = hashlib.sha256(old.read_bytes()).hexdigest()
    target = daily_market_path("2026-09-11", "BTCUSDC", "funding", tmp_path)
    for _ in range(2):
        receipt = migrate(tmp_path)
        assert receipt["files"] == receipt["rows"] == 1
        assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
        rows = json.loads((tmp_path / "raw/daily-index.json").read_text())["records"]
        assert rows[0] == {"channel": "trades", "path": "untouched"}
        assert rows[1]["path"] == str(target)
        assert len(rows) == 2
    assert not old.exists()
    assert not (tmp_path / "raw/binance_futures/BTCUSDC").exists()


def test_conflicting_destination_preserves_original(tmp_path):
    old = prepare(tmp_path)
    target = daily_market_path("2026-09-11", "BTCUSDC", "funding", tmp_path)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"conflict")
    with pytest.raises(ValueError, match="Conflicting"):
        migrate(tmp_path)
    assert old.is_file()
