import json
import stat

import pytest
import requests

from data import download_infoway as iw


class Response:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def json(self):
        return self.payload


class Session:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.adapters = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def mount(self, prefix, adapter):
        self.adapters.append(adapter)

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def candles():
    return iw.request_plan(
        "candles", "BTCUSDT.P", interval="1m", count=2, end="2026-09-08T00:00:00Z"
    )


def test_preview_needs_no_key_or_network(monkeypatch, capsys):
    monkeypatch.delenv("INFOWAY_API_KEY", raising=False)
    monkeypatch.setattr(iw.requests, "Session", lambda: pytest.fail("network called"))
    assert (
        iw.main(
            [
                "candles",
                "--code",
                "BTCUSDT.P",
                "--interval",
                "1m",
                "--count",
                "120",
                "--end",
                "2026-09-08T00:00:00Z",
            ]
        )
        == 0
    )
    plan = json.loads(capsys.readouterr().out)
    assert plan["status"] == "PREVIEW_NO_NETWORK"
    assert plan["request_count_ceiling"] == 1
    assert plan["venue"] == "UNKNOWN"
    assert plan["replay_eligible"] is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"count": 501},
        {"count": 0},
        {"end": "2026-09-08"},
        {"end": "2026-09-08T00:00:00.123Z"},
        {"interval": "1s"},
    ],
)
def test_invalid_candle_plan_stops_offline(kwargs):
    opts = dict(interval="1m", count=120, end="2026-09-08T00:00:00Z")
    opts.update(kwargs)
    with pytest.raises(ValueError):
        iw.request_plan("candles", "BTCUSDT.P", **opts)


@pytest.mark.parametrize("code", ["BTCUSDT,BTCUSDC", "../secret", "https://elsewhere", ""])
def test_single_code_only(code):
    with pytest.raises(ValueError):
        iw.request_plan("info", code)


def test_realtime_endpoints_cannot_claim_historical_read():
    with pytest.raises(ValueError, match="Only candles"):
        iw.request_plan("depth", "BTCUSDT", end="2025-08-01T00:00:00Z")
    with pytest.raises(ValueError, match="not documented"):
        iw.request_plan("historical_depth", "BTCUSDT")
    assert iw.request_plan("info", "BTCUSDT.P")["params"] == {
        "type": "CRYPTO",
        "symbols": "BTCUSDT.P",
    }


def run_sample(tmp_path, monkeypatch, response, *, channel="candles", output="sample"):
    session = Session(response)
    monkeypatch.setattr(iw.requests, "Session", lambda: session)
    plan = candles() if channel == "candles" else iw.request_plan(channel, "BTCUSDT.P")
    result = iw.download_sample(
        plan,
        api_key="TEST-SECRET-KEY",
        output_dir=tmp_path / output,
        budget_file=tmp_path / "budget.jsonl",
    )
    return result, session


def test_one_request_preserves_payload_and_budget_prevents_repeat(tmp_path, monkeypatch):
    payload = {
        "ret": 200,
        "data": [
            {
                "s": "BTCUSDT.P",
                "respList": [{"t": "1788825480", "v": None}, {"t": "1788825480", "v": "2"}],
            }
        ],
    }
    result, session = run_sample(tmp_path, monkeypatch, Response(payload))
    assert len(session.calls) == 1
    assert session.adapters[0].max_retries.total == 0
    args, kwargs = session.calls[0]
    assert args == ("POST", "https://data.infoway.io/crypto/v2/batch_kline")
    assert kwargs["headers"] == {"apiKey": "TEST-SECRET-KEY"}
    assert kwargs["allow_redirects"] is False
    assert kwargs["json"]["timestamp"] == 1788825600
    assert result["status"] == "SAVED_NOT_ADMITTED"
    assert result["duplicate_timestamp_rows"] == 1
    assert result["source_clock_verified"] is False
    assert json.loads((tmp_path / "sample/response.json").read_text()) == payload
    for p in (tmp_path / "sample").iterdir():
        assert stat.S_IMODE(p.stat().st_mode) == 0o600
        assert "TEST-SECRET-KEY" not in p.read_text()
    with pytest.raises(ValueError, match="budget exhausted"):
        iw.download_sample(
            candles(),
            api_key="TEST-SECRET-KEY",
            output_dir=tmp_path / "next",
            budget_file=tmp_path / "budget.jsonl",
        )
    assert len(session.calls) == 1
    with pytest.raises(FileExistsError):
        iw.download_sample(
            candles(),
            api_key="TEST-SECRET-KEY",
            output_dir=tmp_path / "sample",
            budget_file=tmp_path / "other-budget.jsonl",
        )
    assert not (tmp_path / "other-budget.jsonl").exists()


@pytest.mark.parametrize(
    "response",
    [
        Response({}, 429),
        Response({}, 302),
        Response({"ret": 401, "msg": "TEST-SECRET-KEY"}),
        Response({"ret": 200, "data": []}),
        Response({"ret": 200, "data": [{"s": "BTCUSDC", "respList": []}]}),
        Response({"ret": 200, "data": [{"s": "BTCUSDT.P", "respList": [{"t": "9999999999999"}]}]}),
        requests.Timeout("connection failed TEST-SECRET-KEY"),
    ],
)
def test_failure_is_counted_and_never_retried_or_leaks_key(tmp_path, monkeypatch, response):
    with pytest.raises(ValueError, match="failed/unknown") as error:
        run_sample(tmp_path, monkeypatch, response)
    assert "TEST-SECRET-KEY" not in str(error.value)
    assert len((tmp_path / "budget.jsonl").read_text().splitlines()) == 1
    assert not (tmp_path / "sample/receipt.json").exists()
    for p in (tmp_path / "sample").iterdir():
        assert "TEST-SECRET-KEY" not in p.read_text()


def test_missing_key_no_budget_no_network(tmp_path, monkeypatch):
    monkeypatch.setattr(iw.requests, "Session", lambda: pytest.fail("network called"))
    with pytest.raises(ValueError, match="INFOWAY_API_KEY"):
        iw.download_sample(
            candles(), api_key="", output_dir=tmp_path / "out", budget_file=tmp_path / "budget"
        )
    assert list(tmp_path.iterdir()) == []


def test_corrupt_budget_never_resets(tmp_path):
    p = tmp_path / "budget"
    p.write_text('{"attempt":1')
    with pytest.raises(ValueError):
        iw._reserve_request(p, 2)
    assert p.read_text() == '{"attempt":1'


def test_budget_shared_across_calls_and_spaced(tmp_path, monkeypatch):
    p = tmp_path / "budget"
    monkeypatch.setattr(iw.time, "time", lambda: 1000.0)
    waits = []
    monkeypatch.setattr(iw.time, "sleep", waits.append)
    assert iw._reserve_request(p, 2) == 1
    assert iw._reserve_request(p, 2) == 2
    assert waits == [1.1]
    with pytest.raises(ValueError, match="budget exhausted"):
        iw._reserve_request(p, 2)


def test_http_failure_keeps_status_without_body(tmp_path, monkeypatch):
    with pytest.raises(ValueError):
        run_sample(tmp_path, monkeypatch, Response({"secret": "TEST-SECRET-KEY"}, 429))
    failure = json.loads((tmp_path / "sample/failure.json").read_text())
    assert failure["http_status"] == 429
    assert failure["provider_ret"] is None


def test_epoch_cutoff_not_shifted_by_display_timezone():
    utc = candles()
    local = iw.request_plan(
        "candles", "BTCUSDT.P", interval="1m", count=2, end="2026-09-08T08:00:00+08:00"
    )
    assert utc == local


@pytest.mark.parametrize("channel,key", [("info", "symbol"), ("depth", "s"), ("trade", "s")])
def test_each_documented_sample_channel(tmp_path, monkeypatch, channel, key):
    payload = {"ret": 200, "data": [{key: "BTCUSDT.P"}]}
    result, session = run_sample(tmp_path, monkeypatch, Response(payload), channel=channel)
    assert result["items"] == 1
    assert result["coverage"] == "UNKNOWN"
    assert len(session.calls) == 1
