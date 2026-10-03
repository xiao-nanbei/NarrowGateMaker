from examples.data_contract_demo import run_demo


def test_public_example_uses_current_consumers_without_purchased_data(tmp_path):
    result = run_demo(tmp_path)
    assert result["fixture"] == "synthetic_not_market_evidence"
    assert result["unique_trade_ids"] == [1, 2]
    assert result["observed_quantity"] == "3"
    assert result["consumer_stats"]["future_fill_violations"] == 0
    assert result["training"] == result["economic_replay"] == "not_run"
