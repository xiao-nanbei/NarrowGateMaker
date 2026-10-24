"""Symbol-aware paths used by training and backtest scripts."""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import MutableMapping, Optional


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root

DEFAULT_SYMBOL = os.environ.get("MM_SYMBOL", "BTCUSDC").upper()
ACTIVE_EXECUTION_SYMBOL = "BTCUSDC"


@dataclass(frozen=True)
class SymbolPathSet:
    symbol: str
    feature_dir: Path
    model_dir: Path
    results_dir: Path
    predictions_path: Path


def normalize_symbol(symbol=None) -> str:
    resolved = (symbol or os.environ.get("MM_SYMBOL") or DEFAULT_SYMBOL).upper()
    if resolved != ACTIVE_EXECUTION_SYMBOL:
        raise ValueError(
            f"{resolved} execution paths are archived. This repo now maintains "
            f"{ACTIVE_EXECUTION_SYMBOL} execution only; BTCUSDT remains reference "
            "data inside BTCUSDC."
        )
    return resolved


def feature_dir(symbol=None) -> Path:
    override = os.environ.get("MM_FEATURE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    symbol = normalize_symbol(symbol)
    return data_root(ROOT) / f"features_{symbol.lower()}"


def model_dir(symbol=None) -> Path:
    override = os.environ.get("MM_MODEL_DIR")
    if override:
        return Path(override).expanduser().resolve()
    symbol = normalize_symbol(symbol)
    return ROOT / "models" / f"saved_{symbol.lower()}"


def results_dir(symbol=None) -> Path:
    symbol = normalize_symbol(symbol)
    return data_root(ROOT) / f"backtest_results_{symbol.lower()}"


def paths_for(symbol: Optional[str] = None) -> SymbolPathSet:
    resolved_symbol = normalize_symbol(symbol)
    resolved_results_dir = results_dir(resolved_symbol)
    return SymbolPathSet(
        symbol=resolved_symbol,
        feature_dir=feature_dir(resolved_symbol),
        model_dir=model_dir(resolved_symbol),
        results_dir=resolved_results_dir,
        predictions_path=resolved_results_dir / "test_predictions.parquet",
    )


def update_symbol_globals(
    namespace: MutableMapping[str, object],
    symbol: Optional[str] = None,
    *,
    feature_key: Optional[str] = None,
    model_key: Optional[str] = None,
    results_key: Optional[str] = None,
    predictions_key: Optional[str] = None,
) -> str:
    paths = paths_for(symbol)
    namespace["SYMBOL"] = paths.symbol
    if feature_key:
        namespace[feature_key] = paths.feature_dir
    if model_key:
        namespace[model_key] = paths.model_dir
    if results_key:
        namespace[results_key] = paths.results_dir
    if predictions_key:
        namespace[predictions_key] = paths.predictions_path
    return paths.symbol
