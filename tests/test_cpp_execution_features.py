"""Exact execution-v1 parity, including delivery clocks and reset semantics."""
from dataclasses import replace
from decimal import Decimal, localcontext

import pytest

from live.feature_protocol import LiveExecutionFeatures

NS = 1_000_000_000


@pytest.fixture
def kernel():
    native = pytest.importorskip("narrowgate_cpp")
    assert native.EXECUTION_FEATURE_FRAME_ABI == 1
    return native.execution_feature_frame_v1


def pair(kernel):
    values = [LiveExecutionFeatures(symbol="BTCUSDC", start_ns=0,
        allowed_lateness_ns=0, max_book_age_ns=NS, frame_kernel=k)
        for k in (None, kernel)]
    for value in values:
        value.connection(connected=True, now_ns=0)
    return values


def deliver(values, i, *, aggregate=False, depth=20):
    event = dict(e="trade", s="BTCUSDC", t=i, T=i*1000+100, E=i*1000+101,
        p=str(Decimal("100.1234567890123456789") + Decimal(i % 11) / 37),
        q=str(Decimal("0.001234567890123456789") * (i % 7+1)), m=bool(i % 2))
    if aggregate:
        event.update(e="aggTrade", a=event.pop("t"), f=i*3, l=i*3+2)
    book = dict(e="depthUpdate", s="BTCUSDC", T=i*1000+102, E=i*1000+103, u=i+1,
        b=[[str(100-Decimal(j)/100), str(Decimal(j+1)/37)] for j in range(depth)],
        a=[[str(101+Decimal(j)/100), str(Decimal(j+2)/41)] for j in range(depth)])
    for value in values:
        method = value.aggregate_trade if aggregate else value.individual_trade
        method(event, receive_ns=i*NS+102_000_000, ready_ns=i*NS+103_000_000)
        value.partial_depth(book, receive_ns=i*NS+104_000_000, ready_ns=i*NS+105_000_000)


@pytest.mark.parametrize("aggregate", [False, True])
@pytest.mark.parametrize("precision", [28, 38])
def test_exact_frames_every_cut_and_reset(kernel, aggregate, precision):
    with localcontext() as context:
        context.prec = precision
        values = pair(kernel)
        for i in range(125):
            deliver(values, i, aggregate=aggregate, depth=5 if i % 7 == 0 else 20)
            for cut in (i*NS+105_000_000, (i+1)*NS):
                assert values[0].frame(cut) == values[1].frame(cut)
        for value in values:
            value.connection(connected=False, now_ns=126*NS)
            value.connection(connected=True, now_ns=127*NS)
        assert values[0].frame(127*NS) == values[1].frame(127*NS)
        for i in range(128, 192):
            deliver(values, i, aggregate=aggregate)
            assert values[0].frame((i+1)*NS) == values[1].frame((i+1)*NS)
        for cut in (194*NS, 200*NS):
            assert values[0].frame(cut) == values[1].frame(cut)


def test_sparse_unknown_and_partial_fields(kernel):
    values = pair(kernel)
    for i in range(61):
        deliver(values, i)
    for value in values:
        value.frame(61*NS)
    reference, native = (v.features for v in values)
    original = list(reference.bars[reference.market_id])
    for mutation in ("gap", "unknown", "missing_close", "missing_native_count", "invalid_volume"):
        rows = original[:]
        if mutation == "gap":
            rows.pop(40)
        else:
            changes = {"unknown": {"coverage": "unknown"}, "missing_close": {"close": None},
                "missing_native_count": {"native_packet_count": None}, "invalid_volume": {"volume": None}}[mutation]
            rows[40] = replace(rows[40], **changes)
        for engine in (reference, native):
            engine.bars[engine.market_id] = rows
        if mutation == "invalid_volume":
            for engine in (reference, native):
                with pytest.raises(TypeError):
                    engine.frame(61*NS)
        else:
            assert reference.frame(61*NS) == native.frame(61*NS)


def test_native_qualification_selects_current_protocol(kernel):
    from live.deployment_runtime import native_live_abi_contract_payload, native_live_parity_tests
    contract = native_live_abi_contract_payload(["execution_feature_frame_v1", "EXECUTION_FEATURE_FRAME_ABI"])
    assert "SignalFeatureEngine" not in contract["required_apis"]
    assert "tests/test_cpp_execution_features.py" in native_live_parity_tests(contract)


@pytest.mark.parametrize("changed_side", ["b", "a"])
def test_native_frame_consumes_levels_eleven_through_twenty(kernel, changed_side):
    """The current depth20 model inputs must not inherit a legacy top10 cut."""
    values = pair(kernel)
    for i in range(61):
        deliver(values, i)
    before = [value.frame(61 * NS) for value in values]
    book = dict(e="depthUpdate", s="BTCUSDC", T=61102, E=61103, u=62,
        b=[[str(100-Decimal(j)/100), str(Decimal(j+1)/37)] for j in range(20)],
        a=[[str(101+Decimal(j)/100), str(Decimal(j+2)/41)] for j in range(20)])
    for level in book[changed_side][10:]:
        level[1] = str(Decimal(level[1]) * 7)
    for value in values:
        value.partial_depth(book, receive_ns=61*NS+104_000_000,
                            ready_ns=61*NS+105_000_000)
    after = [value.frame(62 * NS) for value in values]
    assert before[0] == before[1]
    assert after[0] == after[1]
    old, new = dict(before[1].values), dict(after[1].values)
    side = "bid" if changed_side == "b" else "ask"
    for name in ("depth_bid_5", "depth_ask_5", "depth_imbalance_5",
                 "mid", "spread", "weighted_mid_proxy"):
        assert new[name] == old[name]
    assert new[f"depth_{side}_20"] > old[f"depth_{side}_20"]
    assert new["depth_imbalance_20"] != old["depth_imbalance_20"]
