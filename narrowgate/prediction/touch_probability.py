"""P3 artifact validation and inference; no training or research path defaults."""

import hashlib
import json
from pathlib import Path

import numpy as np
from scipy.stats import norm

P3_EVENT_TYPE = "touch"
P3_HORIZON_S = 10.0
P3_DISTANCE_UNIT = "USDC_per_BTC"
P3_DISTANCE_ORIGIN = "same_side_best_bid_or_ask_at_window_start"
P3_SIDE_IDENTITY = "pooled_buy_sell"
P3_EMPIRICAL_SCHEMA = "narrowgate_p3_touch_calibration.v4"


class TouchProbabilityModel:
    """Touch-opportunity probability model.

    The parametric model uses the SU Johnson bar-excursion curve. Empirical
    artifacts use a survival curve calibrated from exact trade/BBO
    windows. Queue position is deliberately excluded and calibrated separately.

    Parameters
    ----------
    johnson_transform_location : float       — location of the normal argument
    johnson_transform_scale : float      — scale of the normal argument  (> 0)
    johnson_distance_location : float    — location of the sinh⁻¹ argument
    johnson_distance_scale : float   — scale of the sinh⁻¹ argument  (> 0)
    """

    def __init__(
        self,
        johnson_transform_location: float = 0.0,
        johnson_transform_scale: float = 1.0,
        johnson_distance_location: float = 0.0,
        johnson_distance_scale: float = 1.0,
        *,
        model_type: str = "su_johnson",
        delta_grid=None,
        probability_grid=None,
        schema_version: str = P3_EMPIRICAL_SCHEMA,
        metadata=None,
    ):
        self.johnson_transform_location = johnson_transform_location
        self.johnson_transform_scale = johnson_transform_scale
        self.johnson_distance_location = johnson_distance_location
        self.johnson_distance_scale = johnson_distance_scale
        self.model_type = str(model_type)
        self.delta_grid = np.asarray(delta_grid or [], dtype=np.float64)
        self.probability_grid = np.asarray(probability_grid or [], dtype=np.float64)
        self.schema_version = str(schema_version)
        if self.schema_version != P3_EMPIRICAL_SCHEMA:
            raise ValueError("unsupported P3 schema; explicit offline migration required")
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

    def semantic_identity(self) -> dict[str, object]:
        """Return and validate the complete empirical P3 estimand identity."""
        if self.model_type != "empirical_survival":
            raise ValueError("only empirical P3 artifacts have a formal touch identity")
        event_type = str(self.metadata.get("event_type") or "").strip().lower()
        horizon_s = float(self.metadata.get("horizon_s", 0.0) or 0.0)
        distance_unit = str(self.metadata.get("distance_unit") or "").strip()
        distance_origin = str(self.metadata.get("distance_origin") or "").strip()
        side = str(self.metadata.get("side") or "").strip().lower()
        queue_included = self.metadata.get("queue_included")
        if event_type != P3_EVENT_TYPE:
            raise ValueError(f"empirical P3 event_type must be {P3_EVENT_TYPE!r}")
        if not np.isclose(horizon_s, P3_HORIZON_S, rtol=0.0, atol=1e-12):
            raise ValueError(f"empirical P3 horizon_s must equal {P3_HORIZON_S:g}")
        if distance_unit != P3_DISTANCE_UNIT:
            raise ValueError(f"empirical P3 distance_unit must be {P3_DISTANCE_UNIT!r}")
        if distance_origin != P3_DISTANCE_ORIGIN:
            raise ValueError(f"empirical P3 distance_origin must be {P3_DISTANCE_ORIGIN!r}")
        if side != P3_SIDE_IDENTITY:
            raise ValueError(f"empirical P3 side identity must be {P3_SIDE_IDENTITY!r}")
        if queue_included is not False:
            raise ValueError("empirical P3 queue_included must be false")
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
        z = self.johnson_transform_location + self.johnson_transform_scale * np.arcsinh(
            (delta - self.johnson_distance_location) / self.johnson_distance_scale
        )
        return 1.0 - norm.cdf(z)

    def distance_touch_product_argmax(self, delta_min=0.1, delta_max=200.0, n=50_000):
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

    def touch_log_probability_distance_slope(self, delta_star=None):
        """Return local ``-d log(P_touch) / d delta`` near ``delta_star``.

        Its unit is inverse price distance, e.g. ``(USDC/BTC)^-1``.  It is a
        local curve slope used by the AS/GLFT spread approximation, not an
        order-arrival intensity.
        """
        if delta_star is None:
            delta_star = self.distance_touch_product_argmax()
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

    def save(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": self.schema_version,
            "model_type": self.model_type,
            "johnson_transform_location": self.johnson_transform_location,
            "johnson_transform_scale": self.johnson_transform_scale,
            "johnson_distance_location": self.johnson_distance_location,
            "johnson_distance_scale": self.johnson_distance_scale,
        }
        if self.model_type == "empirical_survival":
            # Calibration producers write a complete estimand; readers never infer it.
            self.metadata.setdefault("event_type", P3_EVENT_TYPE)
            self.metadata.setdefault("distance_origin", P3_DISTANCE_ORIGIN)
            self.metadata.setdefault("side", P3_SIDE_IDENTITY)
            self.metadata.setdefault("queue_included", False)
            self.semantic_identity()
            payload.update(
                {
                    "delta_grid": self.delta_grid.tolist(),
                    "probability_grid": self.probability_grid.tolist(),
                    "metadata": self.metadata,
                    "distance_touch_product_argmax": self.distance_touch_product_argmax(),
                    "touch_log_probability_distance_slope": (
                        self.touch_log_probability_distance_slope()
                    ),
                }
            )
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        self.artifact_path = path.resolve()
        self.artifact_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        print(f"  Saved touch probability parameters → {path}")

    @classmethod
    def load(cls, path, *, require_live_compatible=False):
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
        if d.get("schema_version") != P3_EMPIRICAL_SCHEMA:
            raise ValueError("unsupported P3 schema; explicit offline migration required")
        retired = {"xi", "lam", "gamma", "delta0", "delta_star", "kappa_eff"} & d.keys()
        if retired:
            raise ValueError(f"retired P3 fields: {sorted(retired)}")
        if d.get("model_type") not in {"empirical_survival", "su_johnson"}:
            raise ValueError("unsupported P3 model_type")
        if require_live_compatible:
            metadata = d.get("metadata") or {}
            if not isinstance(metadata, dict):
                raise ValueError("P3 metadata must be a JSON object")
            if metadata.get("authority") == "public_dry_run_only":
                raise ValueError("public_dry_run_only P3 fixture cannot enter live deployment")
            for name in ("touch_log_probability_distance_slope", "distance_touch_product_argmax"):
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
                johnson_transform_location=float(d.get("johnson_transform_location", 0.0)),
                johnson_transform_scale=float(d.get("johnson_transform_scale", 1.0)),
                johnson_distance_location=float(d.get("johnson_distance_location", 0.0)),
                johnson_distance_scale=float(d.get("johnson_distance_scale", 1.0)),
                model_type="empirical_survival",
                delta_grid=d.get("delta_grid"),
                probability_grid=d.get("probability_grid"),
                schema_version=str(d.get("schema_version", "")),
                metadata=d.get("metadata") or {},
            )
        else:
            model = cls(
                johnson_transform_location=float(d["johnson_transform_location"]),
                johnson_transform_scale=float(d["johnson_transform_scale"]),
                johnson_distance_location=float(d["johnson_distance_location"]),
                johnson_distance_scale=float(d["johnson_distance_scale"]),
                schema_version=str(d["schema_version"]),
            )
        model.artifact_path = Path(artifact_path).resolve() if artifact_path is not None else None
        model.artifact_sha256 = hashlib.sha256(raw).hexdigest()
        if model.model_type == "empirical_survival" or require_live_compatible:
            model.semantic_identity()
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
        return (
            f"FillProb(ξ={self.johnson_transform_location:.4f}, "
            f"λ={self.johnson_transform_scale:.4f}, "
            f"γ={self.johnson_distance_location:.4f}, δ₀={self.johnson_distance_scale:.4f})"
        )
