"""New-source execution-only frames to existing 13-head label semantics."""

import hashlib
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

import numpy as np
import pandas as pd

from data.observation import EXECUTION_FEATURE_NAMES
from features.feature_engineer import add_labels


# Native packet count is unavailable in individual Tardis trades, not zero.
FEATURE_COLUMNS = tuple(n for n in EXECUTION_FEATURE_NAMES if n != "native_packet_count_10s")


def delivery_identity(profile):
    from dataclasses import asdict
    from data.runtime import ObservationProfile
    value = asdict(ObservationProfile(**profile))
    # Feature timers emit frames, not market deliveries. P3 uses a 1s timer
    # with 10s origins; the training panel emits only those 10s frames.
    value.pop("feature_period_ns")
    path = value.pop("measured_latency_path")
    if path and not value.get("measured_latency_sha256"):
        raise ValueError("measured delivery requires content identity")
    return value


def validate_calibration_consumer(p3, consumer, day, *, tick_size=0.1):
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    meta = p3.get("metadata", {})
    if (p3.get("schema_version") != "narrowgate_p3_touch_calibration.v3"
            or p3.get("model_type") != "empirical_survival"
            or meta.get("fit_days") != list(TRAIN_DAYS)
            or meta.get("distance_unit") != "USDC_per_BTC"
            or meta.get("quote_tick_size") != tick_size or tick_size != 0.1
            or meta.get("event_type") != "touch" or meta.get("horizon_s") != 10.0
            or meta.get("queue_included") is not False):
        raise ValueError("P3 source/unit/training support mismatch")
    plan = meta.get("plan", {})
    current = consumer["plan"]
    if (plan.get("market_id") != current.get("market_id")
            or plan.get("fit_days") != list(TRAIN_DAYS)
            or delivery_identity(plan["observation_profile"]) != delivery_identity(current["observation_profile"])):
        raise ValueError("P3 delivery/market binding mismatch")
    rows = meta.get("daily_inputs", [])
    if [r.get("day") for r in rows] != list(TRAIN_DAYS):
        raise ValueError("P3 daily source support mismatch")
    sources = [{"day": Path(s["path"]).name, "manifest_sha256": s["sha256"]}
               for s in consumer["source_bundles"]]
    if next((r.get("source_bundles") for r in rows if r["day"] == day), None) != sources:
        raise ValueError("P3 consumer source binding mismatch")


def training_identity(path):
    """New public-input identity, never the retired feature-DAG authority."""
    from data.observation import CONTRACT as OBSERVATION, FEATURE_CONTRACT
    from data.tardis_input import CONTRACT as INPUT
    from strategy.model_contract import absolute_price_variance_unit_contract
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    path = Path(path)
    payload = json.loads(path.read_text())
    expected = {"schema": "f03.public_feature_panel.v1", "symbol": "BTCUSDC",
        "input_contract_id": INPUT, "observation_contract_id": OBSERVATION,
        "feature_contract_id": FEATURE_CONTRACT, "decision_time_semantics": "feature_ready_index",
        "source_profile": "tardis_only", "reference_market": None}
    if any(payload.get(k) != v for k, v in expected.items()):
        raise ValueError("invalid new public training contract")
    if (payload.get("feature_timestamp_semantics", "feature_ready_index") != "feature_ready_index"
            or payload.get("feature_cutoff_semantics", "feature_ready_index") != "feature_ready_index"
            or "feature_bucket_ms" in payload):
        raise ValueError("conflicting public training clock declarations")
    if payload.get("feature_cols") != list(FEATURE_COLUMNS) or payload.get("split", {}).get("train") != list(TRAIN_DAYS):
        raise ValueError("new public feature/split identity differs from declared experiment")
    for name in ("label_contract_id", "split_manifest_id"):
        if not payload.get(name):
            raise ValueError(f"missing {name}")
    calibration = payload.get("label_quote_calibration", {})
    model_path = Path(calibration.get("path", ""))
    if not model_path.is_file() or hashlib.sha256(model_path.read_bytes()).hexdigest() != calibration.get("sha256"):
        raise ValueError("new P3 calibration identity mismatch")
    p3 = json.loads(model_path.read_text())
    if (p3.get("schema_version") != "narrowgate_p3_touch_calibration.v3"
            or p3.get("model_type") != "empirical_survival"
            or p3.get("metadata", {}).get("fit_days") != list(TRAIN_DAYS)):
        raise ValueError("new P3 training support mismatch")
    training_source_identity(path, TRAIN_DAYS)
    for row in payload["daily_inputs"]:
        validate_calibration_consumer(p3, json.loads(Path(row["consumer_manifest_path"]).read_text()), row["day"])
    descriptor = {k: payload[k] for k in (*expected.keys(), "feature_cols", "label_contract_id")}
    contract_hash = hashlib.sha256(json.dumps(descriptor, sort_keys=True).encode()).hexdigest()
    return {"feature_manifest_path": str(path), "feature_manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "public_input_contract": expected, "feature_semantics_version": 1,
        # Existing trainer ABI keys identify this new feature contract, not the legacy DAG.
        "feature_dag_id": FEATURE_CONTRACT, "feature_dag_sha256": contract_hash,
        "feature_panel_split": payload["split"], "feature_cutoff_semantics": "feature_ready_index",
        "feature_timestamp_semantics": "feature_ready_index", "feature_sampling_interval_ms": 10_000,
        "execution_trade_count_unit": "individual_execution", "reference_trade_count_unit": "unavailable",
        "reference_trade_symbol": None, "volatility_unit_contract": absolute_price_variance_unit_contract("BTCUSDC"),
        "feature_label_quote_calibration": calibration,
        **{k: payload[k] for k in ("input_contract_id", "observation_contract_id", "feature_contract_id",
                                   "label_contract_id", "split_manifest_id")}}


def training_source_identity(path, days):
    payload = json.loads(Path(path).read_text())
    rows = payload.get("daily_inputs", [])
    if [r.get("day") for r in rows] != list(days):
        raise ValueError("public training inputs must bind every declared training day exactly once")
    for row in rows:
        source = Path(row["consumer_manifest_path"])
        if hashlib.sha256(source.read_bytes()).hexdigest() != row.get("consumer_manifest_sha256"):
            raise ValueError("public training consumer identity changed")
    encoded = json.dumps(rows, sort_keys=True).encode()
    return {"source_manifest_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
            "train_source_identity_sha256": hashlib.sha256(encoded).hexdigest()}


def publish_model_contract(model_dir, identity, targets, *, selection_contract):
    from strategy.model_contract import REQUIRED_MODEL_HEADS
    from data.facts import save_private_json
    if set(targets) != set(REQUIRED_MODEL_HEADS) or len(targets) != len(REQUIRED_MODEL_HEADS):
        raise ValueError("new model contract requires all 13 heads")
    root = Path(model_dir)
    result = {k: identity[k] for k in ("input_contract_id", "observation_contract_id", "feature_contract_id",
                                      "label_contract_id", "split_manifest_id")}
    result.update(schema="narrowgate.semantic_model_bundle.v1", symbol="BTCUSDC", heads={}, reference_market=None, promotion_authority="research_only")
    if identity.get("feature_manifest_sha256"):
        result["feature_manifest_sha256"] = identity["feature_manifest_sha256"]
    expected_selection = dict(selection_contract)
    expected_selection.pop("spec_path", None)
    shared = None
    for head in targets:
        metadata = json.loads((root/(head+"_meta.json")).read_text())
        if metadata.get("feature_cols") != list(FEATURE_COLUMNS):
            raise ValueError("trained head schema does not match new public features")
        if (metadata.get("feature_timestamp_semantics") != "feature_ready_index"
                or metadata.get("feature_cutoff_semantics") != "feature_ready_index"
                or "feature_bucket_ms" in metadata):
            raise ValueError("new public publication requires unambiguous feature_ready_index clock")
        binding = head_training_identity(metadata, identity, head)
        if binding["selection"] != expected_selection:
            raise ValueError("head differs from requested training identity")
        if shared is not None and binding != shared:
            raise ValueError("mixed 13-head training identity")
        shared = binding
        result["heads"][head] = {"sha256": hashlib.sha256((root/(head+".txt")).read_bytes()).hexdigest(),
            "metadata_sha256": hashlib.sha256((root/(head+"_meta.json")).read_bytes()).hexdigest(),
            "feature_cols": list(FEATURE_COLUMNS), "missing_policy": "native_nan"}
    result["training_identity"] = shared
    save_private_json(root/"public_input_model.json", result)
    return result


def head_training_identity(metadata, identity, head):
    """Common training identity only; per-head outcomes/row counts may differ."""
    keys = ("input_contract_id", "observation_contract_id", "feature_contract_id",
            "label_contract_id", "split_manifest_id")
    if metadata.get("name") != head or any(not identity.get(k) or metadata.get(k) != identity[k] for k in keys):
        raise ValueError("head input/label/split identity mismatch")
    selection = dict(metadata.get("train_only_selection") or {})
    selection.pop("spec_path", None)  # Relocation is not a new experiment.
    required = ("spec_sha256", "feature_manifest_sha256", "feature_dag_sha256",
                "source_manifest_sha256", "train_source_identity_sha256", "fit_days",
                "selection_days", "refit_days", "sample_weight_policy")
    if any(not selection.get(k) for k in required) or selection.get("external_panel_read_during_fit") is not False:
        raise ValueError("head training identity is incomplete")
    if identity.get("feature_manifest_sha256") and selection["feature_manifest_sha256"] != identity["feature_manifest_sha256"]:
        raise ValueError("head feature parent mismatch")
    if selection["sample_weight_policy"].get("half_life_days") not in ("inf", 240, 120, 60):
        raise ValueError("head half-life identity missing or invalid")
    return {**{k: identity[k] for k in keys}, "selection": selection}


def label_frames(frames, outcome_bars, *, config_path, visible_bars):
    """Keep offline outcome Bars separate from delivered feature history.

    All arguments are new-source inputs bound by the caller's run manifest.
    outcome_bars has a UTC bucket-start index; visible_bars has ready_ns and
    end_ns. The existing 60-second/minimum-20-differences variance rule uses
    delivered history only; dimensionless log volatility is not substituted.
    """
    required = {"cutoff_ns", "max_dependency_ready_ns", *FEATURE_COLUMNS}
    if not required <= set(frames):
        raise ValueError("new public frame schema required")
    decisions = pd.to_datetime(frames["cutoff_ns"], unit="ns", utc=True)
    if decisions.hasnans or not decisions.is_monotonic_increasing or decisions.duplicated().any():
        raise ValueError("ordered unique decision times required")
    dependencies = frames["max_dependency_ready_ns"]
    if (dependencies.notna() & (dependencies > frames["cutoff_ns"])).any():
        raise ValueError("future feature dependency")
    if not isinstance(outcome_bars.index, pd.DatetimeIndex) or outcome_bars.index.tz is None:
        raise ValueError("outcome Bars require explicit UTC bucket-start times")
    outcome_ns = outcome_bars.index.as_unit("ns").asi8
    if (not outcome_bars.index.is_monotonic_increasing or outcome_bars.index.has_duplicates
            or np.any(outcome_ns % 1_000_000_000)
            or np.any(np.diff(outcome_ns) != 1_000_000_000)):
        raise ValueError("dense second-aligned outcome Bars required; missing values stay unknown")
    bars = visible_bars.sort_values("ready_ns", kind="stable")
    if (not bars["end_ns"].is_monotonic_increasing or bars["end_ns"].duplicated().any()
            or (bars["ready_ns"] < bars["end_ns"]).any()):
        raise ValueError("invalid visible Bar time contract")
    closes = pd.to_numeric(bars["close"], errors="coerce")
    diffs = closes.diff()
    contiguous = bars["end_ns"].diff().eq(1_000_000_000)
    diffs = diffs.where(contiguous & bars["coverage"].eq("observed"))
    variance = diffs.rolling(60, min_periods=20).var().to_numpy()
    ready = bars["ready_ns"].to_numpy(dtype=np.int64)
    position = np.searchsorted(ready, frames["cutoff_ns"].to_numpy(), side="right") - 1
    visible_variance = np.full(len(frames), np.nan)
    valid = position >= 0
    visible_variance[valid] = variance[position[valid]]
    frame = frames.loc[:, FEATURE_COLUMNS].copy()
    if "validity_mask" in frames:
        masks = np.asarray(frames["validity_mask"].tolist(), dtype=bool)
        if masks.shape != (len(frames), len(EXECUTION_FEATURE_NAMES)):
            raise ValueError("public feature validity mask schema mismatch")
        selected = [EXECUTION_FEATURE_NAMES.index(name) for name in FEATURE_COLUMNS]
        frame = frame.where(masks[:, selected])
    frame.index = pd.DatetimeIndex(decisions)
    labeled = add_labels(frame, outcome_bars, symbol="BTCUSDC", config_path=config_path,
        include_outcome_times=True, decision_times=frame.index, decision_variance=visible_variance)
    # The trainer replaces declared time-only weights per head and fit phase.
    labeled["sample_weight"] = 1.0
    labeled.attrs["feature_cols"] = list(FEATURE_COLUMNS)
    labeled.attrs["decision_time_semantics"] = "feature_ready_index"
    labeled.attrs["reference_market"] = None
    return labeled


def build_label_day(bundle, *, day, config_path, output):
    """Create one training-day label panel from the shared consumer bundle.

    Actual outcome times remain columns for per-head training purge. This
    function never fits a model or reads a held-out label panel.
    """
    from data.facts import _digest, save_private_json
    from data.runtime import ConsumerBundle
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    if day not in TRAIN_DAYS:
        raise ValueError("label materialization here is training-day only")
    source = ConsumerBundle(bundle)
    import yaml
    from features.feature_engineer import ROOT
    config = yaml.safe_load(Path(config_path).read_text())
    model_root = Path(config["ml"]["model_dir"]).expanduser()
    if not model_root.is_absolute():
        model_root = ROOT / model_root
    calibration_path = model_root / "fill_prob_params.json"
    validate_calibration_consumer(json.loads(calibration_path.read_text()), source.manifest, day,
                                 tick_size=config.get("tick_size"))
    source.source_paths()
    frames = source.table("features").to_pandas()
    visible = source.table("bars").to_pandas()
    raw_outcomes = source.table("outcome_bars")
    if (raw_outcomes.schema.metadata or {}).get(b"purpose") != b"offline_outcome_only_not_strategy_visible":
        raise ValueError("offline outcome Bar identity required")
    outcomes = raw_outcomes.to_pandas()
    outcomes.index = pd.to_datetime(outcomes.pop("start_ns"), unit="ns", utc=True)
    for column in ("open", "high", "low", "close", "volume", "turnover"):
        outcomes[column] = pd.to_numeric(outcomes[column], errors="coerce")
    start = pd.Timestamp(day, tz="UTC").value
    end = start + 86_400_000_000_000
    frames = frames.loc[(frames.cutoff_ns >= start) & (frames.cutoff_ns < end)]
    if frames.empty or np.any(frames.cutoff_ns.to_numpy() % 10_000_000_000):
        raise ValueError("declared ten-second training decisions required")
    output = Path(output)
    if output.exists():
        raise FileExistsError("new label output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name+'.'+uuid4().hex+'.part')
    stage.mkdir(mode=0o700)
    try:
        labeled = label_frames(frames, outcomes, config_path=config_path, visible_bars=visible)
        labeled.to_parquet(stage/'labels.parquet', compression='zstd')
        metadata = {"schema": "f03.public_label_day.v1", "visibility": "local_only_do_not_publish",
            "day": day, "rows": len(labeled), "feature_cols": list(FEATURE_COLUMNS),
            "consumer_manifest_path": str(source.root/'manifest.json'),
            "consumer_manifest_sha256": _digest(source.root/'manifest.json'),
            "quote_config_sha256": _digest(Path(config_path)),
            "label_quote_calibration_sha256": _digest(calibration_path),
            "labels_sha256": _digest(stage/'labels.parquet'),
            "decision_time_semantics": "feature_ready_index", "reference_market": None,
            "outcome_boundary_policy": "per_head_actual_end_strictly_before_training_day_end",
            "valid_labels_before_training_purge": {c: int(labeled[c].notna().sum()) for c in labeled
                if c.startswith('label_') and not c.startswith('label_outcome_end_')}}
        save_private_json(stage/'manifest.json', metadata)
        os.rename(stage, output)
        return metadata
    except BaseException:
        shutil.rmtree(stage)
        raise


def training_input_plan(template, day):
    """Reuse the frozen delivery profile with causal prior-file warmup only."""
    from datetime import date, timedelta
    from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
    if day not in TRAIN_DAYS:
        raise ValueError("feature batch is restricted to declared training days")
    start = pd.Timestamp(day, tz="UTC").value
    first = max(TRAIN_DAYS[0], (date.fromisoformat(day)-timedelta(days=1)).isoformat())
    return {**template, "source_start_day": first, "source_end_day": day,
            "start_ns": start - (120_000_000_000 if first < day else 0),
            "end_ns": start + 86_400_000_000_000, "include_outcome_bars": True}


def derive_training_day(template, day, output):
    from data.runtime import ConsumerBundle, derive_inputs
    plan = training_input_plan(template, day)
    destination = Path(output)/day
    if destination.exists():
        bundle = ConsumerBundle(destination)
        if bundle.manifest["plan"] != plan:
            raise ValueError("existing feature day belongs to a different plan")
        bundle.source_paths()
        for name in ("features", "bars", "depth", "outcome_bars"):
            bundle.table(name)
        return day, "verified_existing"
    if shutil.disk_usage(Path(output).parent).free < 10*1024**3:
        raise RuntimeError("training input batch requires 10 GiB free reserve")
    derive_inputs(plan, destination)
    return day, "completed"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate new-source training labels, without fitting or economics")
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--day')
    parser.add_argument('--config', type=Path)
    parser.add_argument('--derive-plan', type=Path)
    parser.add_argument('--days', nargs='+')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.derive_plan:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
        days = args.days or list(TRAIN_DAYS)
        if len(days) != len(set(days)) or not set(days) <= set(TRAIN_DAYS) or not 1 <= args.workers <= 6:
            parser.error("unique training days and 1..6 bounded workers required")
        template = json.loads(args.derive_plan.read_text())
        args.output.mkdir(parents=True, exist_ok=True)
        with ProcessPoolExecutor(args.workers) as pool:
            futures = [pool.submit(derive_training_day, template, day, args.output) for day in days]
            for future in as_completed(futures):
                day, status = future.result()
                print(json.dumps({"day": day, "status": status}), flush=True)
        return
    if not all((args.bundle, args.day, args.config)):
        parser.error("label generation requires --bundle, --day and --config")
    result = build_label_day(args.bundle, day=args.day, config_path=args.config, output=args.output)
    print(json.dumps({"day": args.day, "rows": result['rows'], "status": "labels_generated_not_fitted"}))


if __name__ == '__main__':
    main()
