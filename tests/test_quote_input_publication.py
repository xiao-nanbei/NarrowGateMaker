from strategy.signal import SignalEngine


def test_complete_depth_publication_and_snapshot_share_watermark():
    signal = SignalEngine(enable_ml=False)
    publications = []
    signal.add_quote_input_observer(lambda domain, version: publications.append(
        (domain, version, len(signal._last_depth.bids), len(signal._last_depth.asks))))
    event = {"T": 1000, "b": [["99", "1"], ["98", "2"]],
             "a": [["101", "1"], ["102", "2"]]}
    signal.on_depth(event, receive_ts_ns=1_001_000_000)
    snapshot = signal.quote_decision_snapshot(now_ns=1_002_000_000)
    assert publications == [("BOOK_VIEW", 1, 2, 2)]
    assert dict(snapshot.quote_input_versions)["BOOK_VIEW"] == 1
    signal.quote_decision_snapshot(now_ns=1_003_000_000)
    assert len(publications) == 1
    signal.on_depth(event, receive_ts_ns=1_004_000_000)
    assert publications[-1] == ("BOOK_VIEW", 2, 2, 2)
    assert dict(snapshot.quote_input_versions)["BOOK_VIEW"] == 1


def test_trade_accumulation_is_not_completed_bar_publication():
    signal = SignalEngine(enable_ml=False)
    publications = []
    signal.add_quote_input_observer(lambda *args: publications.append(args))
    for ts in (1000, 1100, 1900):
        signal.on_agg_trade({"T": ts, "s": "BTCUSDC", "p": "100",
                             "q": "0.001", "m": False},
                            receive_ts_ns=ts * 1_000_000)
    assert publications == []
    signal.on_agg_trade({"T": 2000, "s": "BTCUSDC", "p": "101",
                         "q": "0.001", "m": False}, receive_ts_ns=2_000_000_000)
    assert publications == [("BAR_OR_FEATURE_VIEW", 1)]
