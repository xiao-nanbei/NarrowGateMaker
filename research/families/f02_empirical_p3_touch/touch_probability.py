#!/usr/bin/env python3
"""
Touch probability curves — empirical survival or SU Johnson parametrisation.

Estimates a 10-second bar-excursion touch opportunity at distance δ from the
same-side BBO at window start.  It does not estimate queue-ahead fill or an
order-arrival intensity.

Parametric form (Guéant & Manziuk 2019):
    f(δ) = 1 − Φ(ξ + λ · arcsinh((δ − γ) / δ₀))

where Φ is the standard normal CDF.

Estimation approach:
  1. From 1s bar data, compute the max price excursion in each direction
     over non-overlapping 10 s windows.
  2. Fit SU Johnson (ξ, λ, γ, δ₀) to the excursion distribution via MLE.
  3. Persist fitted parameters for use in maker_engine / RL environment.

Important:
  This is a coarse bar-excursion model. It is useful as a historical reference
  for distance-decay intuition, but live/replay fill selection is governed by
  tick replay queue calibration and exact L2 where available.

Usage:
    python research/families/f02_empirical_p3_touch/touch_probability.py                # fit from all training data
    python research/families/f02_empirical_p3_touch/touch_probability.py --plot          # fit + plot diagnostics
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from models.symbol_paths import (
    DEFAULT_SYMBOL,
    ROOT,
    data_root,
    paths_for,
    update_symbol_globals,
)
from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality
from narrowgate.prediction.touch_probability import (
    P3_EVENT_TYPE as P3_EVENT_TYPE,
    P3_HORIZON_S as P3_HORIZON_S,
    P3_DISTANCE_UNIT as P3_DISTANCE_UNIT,
    P3_DISTANCE_ORIGIN as P3_DISTANCE_ORIGIN,
    P3_SIDE_IDENTITY as P3_SIDE_IDENTITY,
    P3_EMPIRICAL_SCHEMA as P3_EMPIRICAL_SCHEMA,
    TouchProbabilityModel as _RuntimeTouchProbabilityModel,
)

BARS_DIR = data_root(ROOT) / "bars_1s"

SYMBOL = DEFAULT_SYMBOL
_INITIAL_PATHS = paths_for(SYMBOL)
MODEL_DIR = _INITIAL_PATHS.model_dir
RESULTS_DIR = _INITIAL_PATHS.results_dir
update_symbol_globals(globals(), SYMBOL, model_key="MODEL_DIR", results_key="RESULTS_DIR")


def configure_symbol(symbol=None):
    update_symbol_globals(globals(), symbol, model_key="MODEL_DIR", results_key="RESULTS_DIR")

REQUOTE_SEC = 10   # one requote interval

# ═══════════════════════════════════════════════════════════════════
#  SU Johnson fill probability model
# ═══════════════════════════════════════════════════════════════════

class TouchProbabilityModel(_RuntimeTouchProbabilityModel):
    """Historical research API: retain symbol-local paths, not inference copies.

    Existing research and trusted historical class locators remain supported.
    Runtime consumers import the neutral implementation with explicit paths.
    """

    def save(self, path=None):
        return super().save(MODEL_DIR / "touch_probability.json" if path is None else path)

    @classmethod
    def load(cls, path=None, *, require_live_compatible=False):
        return super().load(
            MODEL_DIR / "touch_probability.json" if path is None else path,
            require_live_compatible=require_live_compatible,
        )

# ═══════════════════════════════════════════════════════════════════
#  MLE fitting
# ═══════════════════════════════════════════════════════════════════

def _compute_excursions(bars_path: Path, window_sec: int = REQUOTE_SEC) -> np.ndarray:
    """Compute max price excursion per window from 1s bars.

    For each non-overlapping `window_sec` window:
      excursion_up   = max(high) − first(open)     within window
      excursion_down = first(open) − min(low)       within window
      excursion      = max(excursion_up, excursion_down)

    Returns array of positive excursion values (in USDT).
    """
    # 这里只估计 10s 内价格触达概率，不模拟 queue ahead 或 maker fill gate。
    # 不要把这个结果直接当成 tick replay 的成交概率校准。
    df = filter_frame_for_orderbook_quality(pd.read_parquet(bars_path), SYMBOL, label="fill-probability bar")
    # Expect columns: open, high, low, close at 1s frequency
    for col in ("open", "high", "low"):
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in {bars_path}")

    n = len(df)
    n_windows = n // window_sec
    if n_windows == 0:
        return np.array([])

    # Trim to exact multiple
    trim = n_windows * window_sec
    opens = df["open"].values[:trim].reshape(n_windows, window_sec)
    highs = df["high"].values[:trim].reshape(n_windows, window_sec)
    lows = df["low"].values[:trim].reshape(n_windows, window_sec)

    first_open = opens[:, 0]
    exc_up = highs.max(axis=1) - first_open
    exc_down = first_open - lows.min(axis=1)

    # Combine both sides (symmetric for BTC)
    all_exc = np.concatenate([exc_up, exc_down])
    return all_exc[all_exc > 0]


def fit_from_data(excursions: np.ndarray) -> TouchProbabilityModel:
    """Fit SU Johnson parameters from raw excursion samples via MLE.

    Trims extreme outliers (>p99) and enforces δ₀ > 1.0 to avoid
    degenerate heavy tails where δ·f(δ) has no finite maximum.
    """
    # Trim outliers to get a clean fit
    p99 = np.percentile(excursions, 99)
    data = excursions[excursions <= p99].copy()
    print(f"  Trimmed to ≤ p99={p99:.1f}: {len(data):,} samples "
          f"(removed {len(excursions)-len(data):,})")

    # Log-likelihood of SU Johnson PDF:
    #   log p(x) = log(λ) − log(δ₀) − ½ log(2π)
    #              − ½ log(1 + z²) − ½ (ξ + λ·arcsinh(z))²
    # where z = (x − γ) / δ₀
    def neg_ll(params):
        johnson_transform_location, johnson_transform_scale, gam, d0 = params
        if johnson_transform_scale <= 0.05 or d0 <= 1.0:
            return 1e15
        z = (data - gam) / d0
        arcsinh_z = np.arcsinh(z)
        arg = johnson_transform_location + johnson_transform_scale * arcsinh_z
        ll = (np.log(johnson_transform_scale) - np.log(d0) - 0.5 * np.log(2 * np.pi)
              - 0.5 * np.log(1 + z ** 2) - 0.5 * arg ** 2)
        return -np.sum(ll)

    # Smart initialisation from data moments
    mu = np.mean(data)
    sigma = np.std(data)
    x0 = [0.0, 1.0, mu, max(sigma, 1.0)]

    result = minimize(neg_ll, x0, method="Nelder-Mead",
                      options={"maxiter": 50_000, "xatol": 1e-8, "fatol": 1e-8})
    if not result.success:
        print(f"  Warning: optimiser did not converge: {result.message}")

    johnson_transform_location, johnson_transform_scale, gam, d0 = result.x
    johnson_transform_scale = abs(johnson_transform_scale)
    d0 = max(abs(d0), 1.0)
    return TouchProbabilityModel(johnson_transform_location=johnson_transform_location, johnson_transform_scale=johnson_transform_scale, johnson_distance_location=gam, johnson_distance_scale=d0)


# ═══════════════════════════════════════════════════════════════════
#  CLI entry point
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Fit fill probability model")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL,
                        help=f"Symbol (default {DEFAULT_SYMBOL}; MM_SYMBOL also supported)")
    parser.add_argument("--plot", action="store_true", help="Show diagnostic plots")
    parser.add_argument("--days", type=str, default="",
                        help="Comma-separated UTC days to use, e.g. '2026-05-15,2026-05-16'")
    args = parser.parse_args()
    configure_symbol(args.symbol)

    # Collect excursions from training-period 1s bars
    bar_files = sorted(BARS_DIR.glob(f"{SYMBOL}-1s-*.parquet"))
    if args.days:
        keep = {token.strip() for token in args.days.split(",") if token.strip()}
        invalid = [token for token in keep if len(token) != 10 or "/" in token]
        if invalid:
            raise SystemExit(f"--days expects comma-separated UTC daily tags YYYY-MM-DD: {invalid}")
        bar_files = [f for f in bar_files if any(m in f.name for m in keep)]
    bar_files = filter_paths_for_orderbook_quality(bar_files, SYMBOL, label="fill-probability bar")
    if not bar_files:
        print("No bar files found in", BARS_DIR)
        raise SystemExit(1)

    print(f"Computing excursions from {len(bar_files)} files …")
    all_exc = []
    for bf in bar_files:
        exc = _compute_excursions(bf)
        if len(exc) > 0:
            all_exc.append(exc)
            print(f"  {bf.name}: {len(exc):>8,} excursions  "
                  f"median={np.median(exc):.2f}  p95={np.percentile(exc, 95):.2f}")
    if not all_exc:
        print("No valid excursions found")
        raise SystemExit(1)

    excursions = np.concatenate(all_exc)
    print(f"\nTotal excursions: {len(excursions):,}")
    print(f"  mean={np.mean(excursions):.2f}  "
          f"median={np.median(excursions):.2f}  "
          f"p95={np.percentile(excursions, 95):.2f}  "
          f"max={np.max(excursions):.2f}")

    # Fit
    print("\nFitting SU Johnson distribution …")
    model = fit_from_data(excursions)
    print(f"  {model}")

    delta_star = model.distance_touch_product_argmax()
    kappa_eff = model.touch_log_probability_distance_slope(delta_star)
    print(f"  Optimal δ* = {delta_star:.2f} USDT")
    print(f"  f(δ*) = {model.prob(delta_star):.4f}")
    print(f"  δ* · f(δ*) = {delta_star * model.prob(delta_star):.4f} USDT (expected revenue)")
    print(f"  Effective κ = {kappa_eff:.6f}")

    model.save()

    # Diagnostic plot
    if args.plot:
        try:
            import matplotlib.pyplot as plt

            fig, axes = plt.subplots(1, 3, figsize=(15, 4))

            # 1. Empirical vs fitted CDF
            ax = axes[0]
            sorted_exc = np.sort(excursions)
            ecdf = np.arange(1, len(sorted_exc) + 1) / len(sorted_exc)
            ax.plot(sorted_exc, 1 - ecdf, "b.", markersize=0.3, label="Empirical f(δ)")
            d_grid = np.linspace(0.01, np.percentile(excursions, 99.5), 500)
            ax.plot(d_grid, model.prob(d_grid), "r-", linewidth=2, label="SU Johnson fit")
            ax.set_xlabel("δ (USDT)")
            ax.set_ylabel("f(δ) = P(fill)")
            ax.set_title("Fill Probability")
            ax.legend()
            ax.set_yscale("log")

            # 2. Expected revenue δ·f(δ)
            ax = axes[1]
            revenue = d_grid * model.prob(d_grid)
            ax.plot(d_grid, revenue, "g-", linewidth=2)
            ax.axvline(delta_star, color="r", linestyle="--",
                       label=f"δ*={delta_star:.1f}")
            ax.set_xlabel("δ (USDT)")
            ax.set_ylabel("δ · f(δ)")
            ax.set_title("Expected Execution Revenue")
            ax.legend()

            # 3. Histogram of excursions
            ax = axes[2]
            ax.hist(excursions, bins=200, density=True, alpha=0.6, color="steelblue")
            # SU Johnson PDF
            z = (d_grid - model.johnson_distance_location) / model.johnson_distance_scale
            arcsinh_z = np.arcsinh(z)
            arg = model.johnson_transform_location + model.johnson_transform_scale * arcsinh_z
            pdf = (model.johnson_transform_scale / model.johnson_distance_scale / np.sqrt(2 * np.pi)
                   / np.sqrt(1 + z ** 2) * np.exp(-0.5 * arg ** 2))
            ax.plot(d_grid, pdf, "r-", linewidth=2, label="SU Johnson PDF")
            ax.set_xlabel("Excursion (USDT)")
            ax.set_ylabel("Density")
            ax.set_title("Excursion Distribution")
            ax.legend()

            plt.tight_layout()
            out = RESULTS_DIR / "fill_probability.png"
            out.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(out, dpi=150)
            print(f"\n  Plot saved → {out}")
            plt.show()
        except ImportError:
            print("matplotlib not available, skipping plot")


if __name__ == "__main__":
    main()
