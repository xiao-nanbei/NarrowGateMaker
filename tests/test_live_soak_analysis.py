from scripts.analyze_live_soak import _global_flow_health


def test_global_flow_health_reports_latest_and_counter_deltas():
    lines = [
        "x INFO HEALTH pos=+0.0000 globalFlowNative=1 globalFlowMarkets=11 "
        "globalFlowTradeBatches=100 globalFlowTradeEvents=150 "
        "globalFlowTradeAccepted=145 globalFlowBookEvents=1000 globalFlowOOO=2 "
        "globalFlowStaleTrades=5 globalFlowTradeOverflow=0 globalFlowBookOverflow=0",
        "x INFO HEALTH pos=+0.0000 globalFlowNative=1 globalFlowMarkets=11 "
        "globalFlowTradeBatches=160 globalFlowTradeEvents=250 "
        "globalFlowTradeAccepted=240 globalFlowBookEvents=1800 globalFlowOOO=3 "
        "globalFlowStaleTrades=7 globalFlowTradeOverflow=0 globalFlowBookOverflow=0",
    ]

    result = _global_flow_health(lines)

    assert result["samples"] == 2
    assert result["native_rate"] == 1.0
    assert result["latest"]["globalFlowMarkets"] == 11
    assert result["delta"]["globalFlowTradeEvents"] == 100
    assert result["delta"]["globalFlowTradeAccepted"] == 95
    assert result["delta"]["globalFlowBookEvents"] == 800
    assert result["delta"]["globalFlowOOO"] == 1
    assert result["delta"]["globalFlowStaleTrades"] == 2
