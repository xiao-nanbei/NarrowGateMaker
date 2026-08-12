from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    compute_quote_core,
)


def _cfg() -> QuoteCoreConfig:
    return QuoteCoreConfig(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.01,
        adverse_guard_enabled=True,
        adverse_pause=True,
        adverse_markout_threshold=3.0,
        adverse_markout_pause_threshold=6.0,
        adverse_markout_pause_hybrid=True,
    )


def _pred() -> QuotePrediction:
    return QuotePrediction(
        dir_10s=0.5,
        vol_10s=1.0,
        ret_10s=0.0,
        tox_bid=0.0,
        tox_ask=0.0,
    )


def test_hybrid_markout_pause_requires_latch() -> None:
    no_latch = compute_quote_core(
        QuoteState(
            mid=100.0,
            inventory=0.0,
            sigma_sq=1.0,
            mo_ema_bid=-7.0,
            mo_ema_ask=-7.0,
        ),
        _cfg(),
        _pred(),
    ).quote_context
    assert no_latch["BUY"]["side_adverse"]
    assert no_latch["SELL"]["side_adverse"]
    assert not no_latch["BUY"]["side_adverse_pause"]
    assert not no_latch["SELL"]["side_adverse_pause"]

    with_latch = compute_quote_core(
        QuoteState(
            mid=100.0,
            inventory=0.0,
            sigma_sq=1.0,
            mo_ema_bid=-7.0,
            mo_ema_ask=-7.0,
            bid_adverse_markout_pause_latch=True,
            ask_adverse_markout_pause_latch=True,
        ),
        _cfg(),
        _pred(),
    ).quote_context
    assert with_latch["BUY"]["side_adverse_pause"]
    assert with_latch["SELL"]["side_adverse_pause"]
