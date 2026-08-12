"""Feature variants for raw-taker and depth interaction experiments."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from market_fusion import default_reference_symbol, normalize_symbol  # noqa: E402
from features.feature_engineer import (  # noqa: E402
    TAKER_TEMPO_FEATURE_COLS,
    TAKER_TEMPO_FEATURE_MAP,
    _as_utc_index,
)


TAKER_TEMPO_WINDOWS_SEC = (5, 10, 30, 60)

LOW_GAIN_TAKER_DROP_COLS = tuple(
    [f"taker_quote_imbalance_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC]
    + [f"taker_signed_quote_sum_{window}s" for window in (5, 10)]
    + [f"taker_buy_iceberg_pressure_sum_{window}s" for window in (5, 10)]
    + [f"taker_sell_iceberg_pressure_sum_{window}s" for window in (5, 10)]
)

L2_INTERACTION_FEATURE_COLS = tuple(
    col
    for window in TAKER_TEMPO_WINDOWS_SEC
    for col in (
        f"x_taker_buy_sweep_l2imb_l3_{window}s",
        f"x_taker_sell_sweep_l2imb_l3_{window}s",
        f"x_taker_count_abs_l2imb_{window}s",
        f"x_taker_run_abs_l2imb_{window}s",
        f"x_taker_buy_sweep_thin_depth_{window}s",
        f"x_taker_sell_sweep_thin_depth_{window}s",
    )
)

REF_DIVERGENCE_FEATURE_COLS = tuple(
    col
    for window in TAKER_TEMPO_WINDOWS_SEC
    for col in (
        f"x_taker_quote_imbalance_div_ref_{window}s",
        f"x_taker_signed_quote_div_ref_{window}s",
        f"x_taker_trade_count_div_ref_{window}s",
        f"x_taker_buy_sweep_div_ref_{window}s",
        f"x_taker_sell_sweep_div_ref_{window}s",
    )
)

FEATURE_VARIANT_CONTRACT_SCHEMA = "narrowgate_taker_feature_ablation.v1"

TAKER_FEATURE_ABLATION_VARIANTS = (
    "base",
    "drop_low_gain_taker",
    "add_l2_interactions",
    "add_ref_divergence",
    "add_all_interactions",
    "drop_low_gain_add_all",
)
VALID_VARIANTS = frozenset(TAKER_FEATURE_ABLATION_VARIANTS)


def normalize_variant(variant: Optional[str]) -> str:
    value = (variant or "base").strip().lower()
    if value not in VALID_VARIANTS:
        raise ValueError(f"Unknown feature variant: {variant!r}")
    return value


def dropped_feature_cols(variant: Optional[str]) -> tuple[str, ...]:
    variant = normalize_variant(variant)
    if variant in {"drop_low_gain_taker", "drop_low_gain_add_all"}:
        return LOW_GAIN_TAKER_DROP_COLS
    return ()


def added_feature_cols(variant: Optional[str]) -> tuple[str, ...]:
    variant = normalize_variant(variant)
    cols: list[str] = []
    if variant in {"add_l2_interactions", "add_all_interactions", "drop_low_gain_add_all"}:
        cols.extend(L2_INTERACTION_FEATURE_COLS)
    if variant in {"add_ref_divergence", "add_all_interactions", "drop_low_gain_add_all"}:
        cols.extend(REF_DIVERGENCE_FEATURE_COLS)
    return tuple(cols)


def _series(frame: pd.DataFrame, name: str) -> pd.Series:
    if name in frame.columns:
        return frame[name].astype(np.float32)
    return pd.Series(0.0, index=frame.index, dtype=np.float32)


def _l2_interactions(frame: pd.DataFrame) -> pd.DataFrame:
    l2_imb_l3 = _series(frame, "l2_imbalance_l3").fillna(0.0).clip(-1.0, 1.0)
    abs_imb = l2_imb_l3.abs()
    near_depth = _series(frame, "l2_near_depth_total").fillna(0.0).clip(lower=0.0)
    thin_depth_score = pd.Series(
        np.where(near_depth.to_numpy() > 0.0, 1.0 / (1.0 + np.log1p(near_depth.to_numpy())), 0.0),
        index=frame.index,
        dtype=np.float32,
    )

    columns: dict[str, pd.Series] = {}
    for window in TAKER_TEMPO_WINDOWS_SEC:
        buy_sweep = _series(frame, f"taker_buy_sweep_score_{window}s").fillna(0.0)
        sell_sweep = _series(frame, f"taker_sell_sweep_score_{window}s").fillna(0.0)
        count = _series(frame, f"taker_trade_count_sum_{window}s").fillna(0.0)
        run = _series(frame, f"taker_max_same_side_run_{window}s").fillna(0.0)
        values = {
            f"x_taker_buy_sweep_l2imb_l3_{window}s": buy_sweep * l2_imb_l3,
            f"x_taker_sell_sweep_l2imb_l3_{window}s": sell_sweep * (-l2_imb_l3),
            f"x_taker_count_abs_l2imb_{window}s": count * abs_imb,
            f"x_taker_run_abs_l2imb_{window}s": run * abs_imb,
            f"x_taker_buy_sweep_thin_depth_{window}s": buy_sweep * thin_depth_score,
            f"x_taker_sell_sweep_thin_depth_{window}s": sell_sweep * thin_depth_score,
        }
        for name, value in values.items():
            columns[name] = value.astype(np.float32)
    return pd.DataFrame(columns, index=frame.index)


def _date_tags_for_index(index: pd.Index) -> list[str]:
    if not isinstance(index, pd.DatetimeIndex) or index.empty:
        return []
    idx = index.tz_convert("UTC") if index.tz is not None else index.tz_localize("UTC")
    start = idx.min().floor("D")
    end = idx.max().floor("D")
    return [ts.strftime("%Y-%m-%d") for ts in pd.date_range(start, end, freq="D", tz="UTC")]


def _load_taker_tempo_for_index(symbol: str, index: pd.Index) -> pd.DataFrame:
    symbol = normalize_symbol(symbol)
    source_cols = list(TAKER_TEMPO_FEATURE_MAP.keys())
    root = data_root(ROOT) / "trade_features" / symbol
    chunks: list[pd.DataFrame] = []
    for tag in _date_tags_for_index(index):
        # variant 训练可以跨多个日度 feature 文件，但每个 parquet 仍按 UTC day
        # 读取并再对齐到训练 split index；不要在这里引入聚合容器。
        path = root / f"{symbol}-trade-tempo-{tag}.parquet"
        if not path.exists():
            continue
        schema_cols = set(pq.ParquetFile(path).schema_arrow.names)
        frame = pd.read_parquet(path, columns=[col for col in source_cols if col in schema_cols])
        frame.index = _as_utc_index(frame.index)
        for col in source_cols:
            if col not in frame.columns:
                frame[col] = 0.0
        chunks.append(frame[source_cols])
    if not chunks:
        return pd.DataFrame({col: 0.0 for col in TAKER_TEMPO_FEATURE_COLS}, index=index)
    tempo = pd.concat(chunks).sort_index()
    tempo = tempo[~tempo.index.duplicated(keep="last")]
    tempo = tempo.rename(columns=TAKER_TEMPO_FEATURE_MAP)
    aligned = tempo[TAKER_TEMPO_FEATURE_COLS].resample("10s").last().reindex(index)
    return aligned.ffill(limit=1).fillna(0.0)


def _ref_divergence(frame: pd.DataFrame, symbol: str, reference_symbol: Optional[str]) -> pd.DataFrame:
    reference = normalize_symbol(reference_symbol, default_reference_symbol(symbol))
    ref = _load_taker_tempo_for_index(reference, frame.index)
    columns: dict[str, pd.Series] = {}
    for window in TAKER_TEMPO_WINDOWS_SEC:
        pairs = {
            f"x_taker_quote_imbalance_div_ref_{window}s": f"taker_quote_imbalance_{window}s",
            f"x_taker_signed_quote_div_ref_{window}s": f"taker_signed_quote_sum_{window}s",
            f"x_taker_trade_count_div_ref_{window}s": f"taker_trade_count_sum_{window}s",
            f"x_taker_buy_sweep_div_ref_{window}s": f"taker_buy_sweep_score_{window}s",
            f"x_taker_sell_sweep_div_ref_{window}s": f"taker_sell_sweep_score_{window}s",
        }
        for out_col, base_col in pairs.items():
            columns[out_col] = (_series(frame, base_col).fillna(0.0) - ref[base_col].astype(np.float32)).astype(
                np.float32,
            )
    return pd.DataFrame(columns, index=frame.index)


def apply_feature_variant(
    frame: pd.DataFrame,
    variant: Optional[str],
    *,
    symbol: str = "BTCUSDC",
    reference_symbol: Optional[str] = None,
) -> pd.DataFrame:
    variant = normalize_variant(variant)
    # 这里只做 deterministic feature transforms，不读取 label / PnL / markout。
    # 因此它可以用于训练 ablation，但是否晋级仍必须看 OOS daily evidence。
    added: list[str] = []
    parts: list[pd.DataFrame] = [frame]
    if variant in {"add_l2_interactions", "add_all_interactions", "drop_low_gain_add_all"}:
        interaction_frame = _l2_interactions(frame)
        added.extend(interaction_frame.columns)
        parts.append(interaction_frame)
    if variant in {"add_ref_divergence", "add_all_interactions", "drop_low_gain_add_all"}:
        divergence_frame = _ref_divergence(frame, symbol, reference_symbol)
        added.extend(divergence_frame.columns)
        parts.append(divergence_frame)
    if len(parts) > 1:
        frame = pd.concat(parts, axis=1)
    drops = [col for col in dropped_feature_cols(variant) if col in frame.columns]
    if drops:
        frame = frame.drop(columns=drops)
    frame.attrs["feature_variant"] = variant
    frame.attrs["feature_variant_added_cols"] = tuple(added)
    frame.attrs["feature_variant_dropped_cols"] = tuple(drops)
    return frame


def infer_variant_from_model_dir(model_dir: Path) -> str:
    bundle_meta = model_dir / "bundle_meta.json"
    if bundle_meta.exists():
        try:
            return normalize_variant(json.loads(bundle_meta.read_text()).get("feature_variant"))
        except Exception:
            return "base"
    for meta_path in sorted(model_dir.glob("*_meta.json")):
        try:
            variant = json.loads(meta_path.read_text()).get("feature_variant")
            if variant:
                return normalize_variant(variant)
        except Exception:
            continue
    return "base"


def write_bundle_meta(model_dir: Path, variant: Optional[str], *, extra: Optional[dict] = None) -> None:
    variant = normalize_variant(variant)
    payload = {
        "feature_variant_contract_schema": FEATURE_VARIANT_CONTRACT_SCHEMA,
        "feature_variant": variant,
        "feature_variant_added_cols": list(added_feature_cols(variant)),
        "feature_variant_dropped_cols": list(dropped_feature_cols(variant)),
    }
    if extra:
        payload.update(extra)
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "bundle_meta.json").write_text(json.dumps(payload, indent=2))


def available_variants() -> tuple[str, ...]:
    return TAKER_FEATURE_ABLATION_VARIANTS


def feature_variant_contract() -> dict:
    return {
        "schema": FEATURE_VARIANT_CONTRACT_SCHEMA,
        "variants": {
            variant: {
                "added_feature_cols": list(added_feature_cols(variant)),
                "dropped_feature_cols": list(dropped_feature_cols(variant)),
            }
            for variant in TAKER_FEATURE_ABLATION_VARIANTS
        },
    }
