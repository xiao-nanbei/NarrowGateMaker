#!/usr/bin/env python3
"""
Legacy bar-excursion fill probability model — SU Johnson parametrisation.

Estimates a 10-second bar-excursion touch opportunity at distance δ from the
same-side BBO at window start.  It does not estimate queue-ahead fill or an
order-arrival intensity; historical class/field names remain for artifact ABI.

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
    python research/families/f02_empirical_p3_touch/fill_probability.py                # fit from all training data
    python research/families/f02_empirical_p3_touch/fill_probability.py --plot          # fit + plot diagnostics
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import norm

try:
    from models.symbol_paths import (
        DEFAULT_SYMBOL,
        ROOT,
        data_root,
        paths_for,
        update_symbol_globals,
    )
except ImportError:
    from symbol_paths import DEFAULT_SYMBOL, ROOT, data_root, paths_for, update_symbol_globals

try:
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality

BARS_DIR = data_root(ROOT) / "bars_1s"

SYMBOL = DEFAULT_SYMBOL
_INITIAL_PATHS = paths_for(SYMBOL)
MODEL_DIR = _INITIAL_PATHS.model_dir
RESULTS_DIR = _INITIAL_PATHS.results_dir
update_symbol_globals(globals(), SYMBOL, model_key="MODEL_DIR", results_key="RESULTS_DIR")


def configure_symbol(symbol=None):
    update_symbol_globals(globals(), symbol, model_key="MODEL_DIR", results_key="RESULTS_DIR")

REQUOTE_SEC = 10   # one requote interval
P3_EVENT_TYPE = "touch"
P3_HORIZON_S = 10.0
P3_DISTANCE_UNIT = "USDC_per_BTC"
P3_DISTANCE_ORIGIN = "same_side_best_bid_or_ask_at_window_start"
P3_SIDE_IDENTITY = "pooled_buy_sell"
P3_EMPIRICAL_SCHEMA = "narrowgate_p3_touch_calibration.v2"


# ═══════════════════════════════════════════════════════════════════
#  SU Johnson fill probability model
# ═══════════════════════════════════════════════════════════════════

class FillProbabilityModel:
    """Fill-opportunity probability model.

    Legacy artifacts use the SU Johnson bar-excursion curve. Formal causal-v2
    artifacts use an empirical survival curve calibrated from exact trade/BBO
    windows. Queue position is deliberately excluded and calibrated separately.

    Parameters
    ----------
    xi : float       — location of the normal argument
    lam : float      — scale of the normal argument  (> 0)
    gamma : float    — location of the sinh⁻¹ argument
    delta0 : float   — scale of the sinh⁻¹ argument  (> 0)
    """

    def __init__(self, xi: float = 0.0, lam: float = 1.0,
                 gamma: float = 0.0, delta0: float = 1.0, *,
                 model_type: str = "su_johnson",
                 delta_grid=None, probability_grid=None,
                 schema_version: str = "legacy_su_johnson.v1",
                 metadata=None):
        self.xi = xi
        self.lam = lam
        self.gamma = gamma
        self.delta0 = delta0
        self.model_type = str(model_type)
        self.delta_grid = np.asarray(delta_grid or [], dtype=np.float64)
        self.probability_grid = np.asarray(probability_grid or [], dtype=np.float64)
        self.schema_version = str(schema_version)
        self.metadata = dict(metadata or {})
        self.artifact_path: Path | None = None
        self.artifact_sha256 = ""
        if self.model_type == "empirical_survival":
            if self.delta_grid.ndim != 1 or self.probability_grid.ndim != 1:
                raise ValueError("empirical P3 grids must be one-dimensional")
            if len(self.delta_grid) < 3 or len(self.delta_grid) != len(self.probability_grid):
                raise ValueError("empirical P3 grids must have equal length >= 3")
            if np.any(np.diff(self.delta_grid) <= 0.0):
                raise ValueError("empirical P3 delta grid must be strictly increasing")
            if np.any(np.diff(self.probability_grid) > 1e-12):
                raise ValueError("empirical P3 survival probabilities must be non-increasing")
            if np.any((self.probability_grid < 0.0) | (self.probability_grid > 1.0)):
                raise ValueError("empirical P3 probabilities must lie in [0, 1]")

    def semantic_identity(self, *, require_artifact_hash: bool = True) -> dict[str, object]:
        """Return and validate the complete empirical P3 estimand identity."""
        if self.model_type != "empirical_survival":
            raise ValueError("only empirical P3 artifacts have a formal touch identity")
        event_type = str(self.metadata.get("event_type") or "").strip().lower()
        if not event_type and (
            self.schema_version == P3_EMPIRICAL_SCHEMA
            and bool(str(self.metadata.get("touch_source") or "").strip())
            and not bool(self.metadata.get("queue_included", False))
        ):
            # Frozen v2 artifacts predate the explicit event_type field. Their
            # schema and metadata make the inference exact without changing SHA.
            event_type = P3_EVENT_TYPE
        horizon_s = float(self.metadata.get("horizon_s", 0.0) or 0.0)
        distance_unit = str(self.metadata.get("distance_unit") or "").strip()
        distance_origin = str(self.metadata.get("distance_origin") or "").strip()
        if not distance_origin and self.schema_version == P3_EMPIRICAL_SCHEMA:
            # The frozen v2 and public dry-run artifacts predate this explicit
            # field.  Their calibration implementation has one fixed origin.
            distance_origin = P3_DISTANCE_ORIGIN
        side = str(self.metadata.get("side") or "").strip().lower()
        if not side and self.schema_version == P3_EMPIRICAL_SCHEMA:
            side = P3_SIDE_IDENTITY
        queue_included = self.metadata.get("queue_included")
        if queue_included is None and self.schema_version == P3_EMPIRICAL_SCHEMA:
            queue_included = False
        if event_type != P3_EVENT_TYPE:
            raise ValueError(f"empirical P3 event_type must be {P3_EVENT_TYPE!r}")
        if not np.isclose(horizon_s, P3_HORIZON_S, rtol=0.0, atol=1e-12):
            raise ValueError(f"empirical P3 horizon_s must equal {P3_HORIZON_S:g}")
        if distance_unit != P3_DISTANCE_UNIT:
            raise ValueError(
                f"empirical P3 distance_unit must be {P3_DISTANCE_UNIT!r}"
            )
        if distance_origin != P3_DISTANCE_ORIGIN:
            raise ValueError(
                f"empirical P3 distance_origin must be {P3_DISTANCE_ORIGIN!r}"
            )
        if side != P3_SIDE_IDENTITY:
            raise ValueError(
                f"empirical P3 side identity must be {P3_SIDE_IDENTITY!r}"
            )
        if queue_included is not False:
            raise ValueError("empirical P3 queue_included must be false")
        if require_artifact_hash and len(self.artifact_sha256) != 64:
            raise ValueError("empirical P3 artifact_sha256 is unavailable")
        return {
            "event_type": event_type,
            "horizon_s": horizon_s,
            "distance_unit": distance_unit,
            "distance_origin": distance_origin,
            "side": side,
            "queue_included": False,
            "artifact_sha256": self.artifact_sha256,
        }

    # ── core ──

    def prob(self, delta):
        """P(touch opportunity within the calibrated horizon | distance=δ).

        Queue-ahead and touch-to-fill conversion are deliberately outside P3.
        ``delta`` is an absolute quote-price distance (USDC/BTC for BTCUSDC),
        not a tick count.
        """
        delta = np.asarray(delta, dtype=np.float64)
        if self.model_type == "empirical_survival":
            return np.interp(
                delta,
                self.delta_grid,
                self.probability_grid,
                left=float(self.probability_grid[0]),
                right=0.0,
            )
        z = self.xi + self.lam * np.arcsinh((delta - self.gamma) / self.delta0)
        return 1.0 - norm.cdf(z)

    def optimal_delta(self, delta_min=0.1, delta_max=200.0, n=50_000):
        """δ* = argmax δ · f(δ), a touch-distance diagnostic objective.

        This is not expected execution revenue: the touch curve excludes queue
        position, touch-to-fill conversion, quantity, fees, and post-fill value.
        """
        if self.model_type == "empirical_survival":
            delta_min = max(float(delta_min), float(self.delta_grid[0]))
            delta_max = min(float(delta_max), float(self.delta_grid[-1]))
        grid = np.linspace(delta_min, delta_max, n)
        obj = grid * self.prob(grid)
        return float(grid[np.argmax(obj)])

    def effective_kappa(self, delta_star=None):
        """Return local ``-d log(P_touch) / d delta`` near ``delta_star``.

        Its unit is inverse price distance, e.g. ``(USDC/BTC)^-1``.  It is a
        local curve slope used by the AS/GLFT spread approximation, not an
        order-arrival intensity.
        """
        if delta_star is None:
            delta_star = self.optimal_delta()
        eps = (
            max(0.05, float(np.median(np.diff(self.delta_grid))))
            if self.model_type == "empirical_survival"
            else 0.5
        )
        f1 = self.prob(delta_star - eps)
        f2 = self.prob(delta_star + eps)
        if f1 <= 0 or f2 <= 0 or f1 <= f2:
            return 0.01  # fallback
        return float(np.log(f1 / f2) / (2 * eps))

    # ── serialisation ──

    def save(self, path=None):
        if path is None:
            path = MODEL_DIR / "fill_prob_params.json"
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": self.schema_version,
            "model_type": self.model_type,
            "xi": self.xi,
            "lam": self.lam,
            "gamma": self.gamma,
            "delta0": self.delta0,
        }
        if self.model_type == "empirical_survival":
            # Every newly written empirical artifact carries its estimand. Old
            # v2 artifacts remain byte-for-byte frozen and normalize at load.
            self.metadata.setdefault("event_type", P3_EVENT_TYPE)
            self.metadata.setdefault("distance_origin", P3_DISTANCE_ORIGIN)
            self.metadata.setdefault("side", P3_SIDE_IDENTITY)
            self.metadata.setdefault("queue_included", False)
            self.semantic_identity(require_artifact_hash=False)
            payload.update({
                "delta_grid": self.delta_grid.tolist(),
                "probability_grid": self.probability_grid.tolist(),
                "metadata": self.metadata,
                "delta_star": self.optimal_delta(),
                "kappa_eff": self.effective_kappa(),
            })
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        self.artifact_path = path.resolve()
        self.artifact_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        print(f"  Saved fill probability params → {path}")

    @classmethod
    def load(cls, path=None, *, require_live_compatible=False):
        if path is None:
            path = MODEL_DIR / "fill_prob_params.json"
        path = Path(path)
        return cls.from_bytes(
            path.read_bytes(),
            artifact_path=path,
            require_live_compatible=require_live_compatible,
        )

    @classmethod
    def from_bytes(cls, raw: bytes, *, artifact_path=None, require_live_compatible=False):
        """Parse and identify one byte snapshot; live compatibility grants no permission."""
        d = json.loads(raw)
        if not isinstance(d, dict):
            raise ValueError("P3 artifact must be a JSON object")
        if require_live_compatible:
            metadata = d.get("metadata") or {}
            if not isinstance(metadata, dict):
                raise ValueError("P3 metadata must be a JSON object")
            if metadata.get("authority") == "public_dry_run_only":
                raise ValueError("public_dry_run_only P3 fixture cannot enter live deployment")
            for name in ("kappa_eff", "delta_star"):
                value = d.get(name)
                try:
                    scalar = float(value)
                    valid = not isinstance(value, bool) and np.isfinite(scalar) and scalar > 0.0
                except (TypeError, ValueError):
                    valid = False
                if not valid:
                    raise ValueError(f"P3 artifact {name} must be positive and finite")
        if d.get("model_type") == "empirical_survival":
            model = cls(
                xi=float(d.get("xi", 0.0)),
                lam=float(d.get("lam", 1.0)),
                gamma=float(d.get("gamma", 0.0)),
                delta0=float(d.get("delta0", 1.0)),
                model_type="empirical_survival",
                delta_grid=d.get("delta_grid"),
                probability_grid=d.get("probability_grid"),
                schema_version=str(d.get("schema_version", "")),
                metadata=d.get("metadata") or {},
            )
        else:
            model = cls(
                xi=float(d["xi"]),
                lam=float(d["lam"]),
                gamma=float(d["gamma"]),
                delta0=float(d["delta0"]),
                schema_version=str(d.get("schema_version", "legacy_su_johnson.v1")),
            )
        model.artifact_path = Path(artifact_path).resolve() if artifact_path is not None else None
        model.artifact_sha256 = hashlib.sha256(raw).hexdigest()
        if model.model_type == "empirical_survival" or require_live_compatible:
            model.semantic_identity(require_artifact_hash=True)
        if require_live_compatible and (
            not np.all(np.isfinite(model.delta_grid))
            or not np.all(np.isfinite(model.probability_grid))
        ):
            raise ValueError("live P3 empirical grids must be finite")
        return model

    def __repr__(self):
        if self.model_type == "empirical_survival":
            return (
                f"FillProb(type=empirical_survival, points={len(self.delta_grid)}, "
                f"schema={self.schema_version})"
            )
        return (f"FillProb(ξ={self.xi:.4f}, λ={self.lam:.4f}, "
                f"γ={self.gamma:.4f}, δ₀={self.delta0:.4f})")


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


def fit_from_data(excursions: np.ndarray) -> FillProbabilityModel:
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
        xi, lam, gam, d0 = params
        if lam <= 0.05 or d0 <= 1.0:
            return 1e15
        z = (data - gam) / d0
        arcsinh_z = np.arcsinh(z)
        arg = xi + lam * arcsinh_z
        ll = (np.log(lam) - np.log(d0) - 0.5 * np.log(2 * np.pi)
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

    xi, lam, gam, d0 = result.x
    lam = abs(lam)
    d0 = max(abs(d0), 1.0)
    return FillProbabilityModel(xi=xi, lam=lam, gamma=gam, delta0=d0)


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
        sys.exit(1)

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
        sys.exit(1)

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

    delta_star = model.optimal_delta()
    kappa_eff = model.effective_kappa(delta_star)
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
            z = (d_grid - model.gamma) / model.delta0
            arcsinh_z = np.arcsinh(z)
            arg = model.xi + model.lam * arcsinh_z
            pdf = (model.lam / model.delta0 / np.sqrt(2 * np.pi)
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
