"""Installed replay entry: one explicit ConsumerBundle and replay configuration."""

import argparse
import json
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-bundle", type=Path, required=True)
    parser.add_argument("--data-check-only", action="store_true")
    parser.add_argument("--data-replay-config", type=Path)
    parser.add_argument("--summary-json", type=Path)
    args = parser.parse_args(argv)
    from models.backtest_tick import TICK, load_public_inputs, simulate_public_inputs

    if args.data_check_only:
        if args.data_replay_config is not None or args.summary_json is not None:
            parser.error("input checking does not execute a replay or write its summary")
        inputs = load_public_inputs(args.data_bundle, tick_size=TICK)
        print(json.dumps({"input_status": "loaded", "trades": len(inputs["trades"]),
                          "bars": len(inputs["bars"]), "frames": len(inputs["frames"]),
                          "depth_observations": len(inputs["bbo"].ts_ms),
                          "economic_replay": "not_run",
                          "native_observation_parity": inputs["native_observation_parity"]}))
        return 0
    if args.data_replay_config is None or args.summary_json is None:
        parser.error("replay requires --data-replay-config and --summary-json")
    from data.facts import save_private_json

    config = json.loads(args.data_replay_config.read_text())
    if not isinstance(config, dict) or set(config) - {"params", "model_dir"}:
        parser.error("replay configuration accepts only params and model_dir")
    if not isinstance(config.get("params"), dict):
        parser.error("replay configuration requires explicit params")
    engine = None
    if config.get("model_dir") is not None:
        from strategy.signal import SignalEngine

        engine = SignalEngine.from_public_models(Path(config["model_dir"]), symbol="BTCUSDC")
    result = simulate_public_inputs(args.data_bundle, config["params"], signal_engine=engine)
    scalar = {key: value for key, value in result.items()
              if value is None or isinstance(value, (str, int, float, bool))}
    scalar["public_input_contract"] = result["public_input_contract"]
    save_private_json(args.summary_json, scalar)
    print(json.dumps({"status": "finished", "summary": str(args.summary_json)}))
    return 0
