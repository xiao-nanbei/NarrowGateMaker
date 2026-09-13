"""Quote-level EV model helpers.

The preferred bundle decomposes quote EV into two online-safe pieces:

    expected EV = P(fill) * E(30s markout | fill)

That avoids training one very sparse regression where almost every unfilled
quote has a zero label. Runtime loading accepts only this canonical bundle;
historical direct-head artifacts remain research records, not executable ABI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np



def quote_side_prefix(side: str) -> str:
    side_upper = str(side or "bid").upper()
    if side_upper in {"BUY", "BID"}:
        return "bid"
    if side_upper in {"SELL", "ASK"}:
        return "ask"
    raise ValueError(f"Unknown quote EV side={side!r}")


def quote_side_model_names(side: str) -> dict[str, Any]:
    prefix = quote_side_prefix(side)
    return {
        "fill_prob": f"{prefix}_fill_prob",
        "markout_buckets": {
            1: f"{prefix}_fill_markout_bucket_1s",
            5: f"{prefix}_fill_markout_bucket_5s",
            30: f"{prefix}_fill_markout_bucket_30s",
        },
        "extreme_adverse": f"{prefix}_extreme_adverse_given_fill",
    }


DEFAULT_MARKOUT_BUCKET_EDGES = [-100.0, -50.0, -10.0, 10.0, 50.0]
DEFAULT_MARKOUT_BUCKET_VALUES = [-150.0, -75.0, -30.0, 0.0, 30.0, 75.0]

DEFAULT_BID_QUOTE_FEATURES = [
    "raw_half_spread",
    "capped_half_spread",
    "raw_mid_shift",
    "raw_reservation_shift",
    "raw_asym_shift",
    "asym",
    "inventory",
    "inventory_ratio",
    "dir_signal",
    "pred_dir",
    "pred_ret",
    "tox_bid",
    "tox_ask",
    "book_imb",
    "microprice_shift_bps",
    "mo_ema_bid",
    "mo_ema_ask",
    "fair",
    "mid",
    "best_bid",
    "best_ask",
    "raw_pair_spread",
    "capped_pair_spread",
    "final_pair_spread",
    "raw_quote_delta_to_bbo",
    "pre_guard_delta_to_bbo",
    "final_quote_delta_to_bbo",
    "raw_distance_to_mid",
    "final_distance_to_mid",
    "raw_quote_skew",
    "final_quote_skew",
    "favored_by_raw_shift",
    "delta_cap",
    "mid_guard",
    "post_only",
    "final_compressed",
    "final_guard_changed",
    "any_constraint_changed",
]


@dataclass
class QuoteEVPrediction:
    expected_maker_markout_bps_per_opportunity_30s: float = 0.0
    toxic_30s: float = 0.0
    fill_prob: float = 0.0
    fill_markout_1s: float = 0.0
    fill_markout_5s: float = 0.0
    fill_markout_30s: float = 0.0
    toxic_given_fill_30s: float = 0.0
    extreme_adverse_given_fill: float = 0.0
    markout_bucket_probs: dict[int, list[float]] = field(default_factory=dict)

    @property
    def ev_30s(self) -> float:
        """Read-only compatibility alias for historical consumers."""
        return self.expected_maker_markout_bps_per_opportunity_30s


from features.quote_ev import (  # noqa: E402,F401
    clean_feature_value,
    feature_array,
    add_quote_time_interaction_feature_values,
    _first_feature_value,
    _clamp01,
    add_local_flow_quote_feature_values,
    add_local_resiliency_quote_feature_values,
    add_toxic_risk_quote_feature_values,
    add_micro_macro_quote_feature_values,
    materialize_quote_ev_feature_values,
)


class QuoteEVModel:
    """LightGBM quote EV/toxicity model bundle for one side (bid or ask)."""

    def __init__(self, fill_prob_model=None,
                 bucket_models: dict[int, Any] | None = None,
                 extreme_adverse_model=None,
                 fill_prob_features: list[str] | None = None,
                 bucket_features: dict[int, list[str]] | None = None,
                 bucket_values: dict[int, list[float]] | None = None,
                 bucket_classes: dict[int, list[int]] | None = None,
                 extreme_adverse_features: list[str] | None = None,
                 side: str = "bid", missing_policy: str = "reject", input_identity=None):
        if missing_policy not in {"reject", "native_nan", "legacy_zero"}:
            raise ValueError("explicit quote EV missing policy required")
        if missing_policy != "legacy_zero":
            if ((fill_prob_model is not None and not fill_prob_features)
                    or (extreme_adverse_model is not None and not extreme_adverse_features)
                    or any(not (bucket_features or {}).get(h) for h in (bucket_models or {}))):
                raise ValueError("new quote EV models require explicit feature columns")
        self.missing_policy = missing_policy
        self.input_identity = dict(input_identity) if input_identity else None
        self.side = quote_side_prefix(side)
        self.fill_prob_model = fill_prob_model
        self.bucket_models = bucket_models or {}
        self.extreme_adverse_model = extreme_adverse_model
        self.fill_prob_features = fill_prob_features or list(DEFAULT_BID_QUOTE_FEATURES)
        self.bucket_features = bucket_features or {}
        self.bucket_values = bucket_values or {}
        self.bucket_classes = bucket_classes or {}
        self.extreme_adverse_features = extreme_adverse_features or list(DEFAULT_BID_QUOTE_FEATURES)

    @classmethod
    def load_legacy(cls, model_dir: str | Path, side: str = "bid") -> QuoteEVModel:
        """Historical default columns/zero ABI; not new-source compatibility."""
        return cls.load(model_dir, side=side, _legacy=True)

    @classmethod
    def load(cls, model_dir: str | Path, side: str = "bid", *,
             input_identity: dict | None = None, _legacy=False) -> QuoteEVModel:
        import lightgbm as lgb

        identity_keys = ("input_contract_id", "observation_contract_id", "feature_contract_id",
                         "source_manifest_sha256", "training_contract_id", "label_contract_id")
        if not _legacy:
            from data.tardis_input import CONTRACT
            if (not input_identity or input_identity.get("input_contract_id") != CONTRACT
                    or any(not input_identity.get(k) for k in identity_keys)):
                raise ValueError("new quote EV input/source/training/label identity required")
        path = Path(model_dir).expanduser()
        names = quote_side_model_names(side)
        prefix = quote_side_prefix(side)
        fill_prob_path = path / f"{names['fill_prob']}.txt"
        bucket_paths = {
            horizon: path / f"{name}.txt"
            for horizon, name in names["markout_buckets"].items()
        }
        extreme_adverse_path = path / f"{names['extreme_adverse']}.txt"
        required_paths = [fill_prob_path, extreme_adverse_path, *bucket_paths.values()]
        missing_paths = [p.name for p in required_paths if not p.exists()]
        if missing_paths:
            raise FileNotFoundError(
                f"incomplete canonical {prefix} quote EV bundle in {path}: "
                + ", ".join(missing_paths)
            )

        def _load_meta(name: str) -> dict[str, Any]:
            meta_path = path / f"{name}_meta.json"
            if not meta_path.exists():
                if _legacy:
                    return {}
                raise ValueError("new quote EV model metadata required")
            with open(meta_path) as f:
                meta = json.load(f)
            if not _legacy:
                if any(meta.get(k) != input_identity[k] for k in identity_keys):
                    raise ValueError("quote EV model input identity mismatch")
                cols = meta.get("feature_cols")
                if (not isinstance(cols, list) or not cols or len(set(cols)) != len(cols)
                        or not all(isinstance(c, str) and c for c in cols)
                        or meta.get("missing_policy") not in {"native_nan", "reject"}):
                    raise ValueError("new quote EV feature/missing contract required")
            return meta

        def _load_features(name: str) -> list[str]:
            meta = _load_meta(name)
            cols = meta.get("feature_cols") or meta.get("feature_columns") or []
            return list(cols) if cols else list(DEFAULT_BID_QUOTE_FEATURES)

        all_names = [names["fill_prob"], names["extreme_adverse"], *names["markout_buckets"].values()]
        all_meta = {name: _load_meta(name) for name in all_names}
        policies = {m.get("missing_policy") for m in all_meta.values()}
        if not _legacy and len(policies) != 1:
            raise ValueError("mixed quote EV missing policies")

        def load_booster(name):
            model = lgb.Booster(model_file=str(path/(name+".txt")))
            if not _legacy and model.feature_name() != all_meta[name]["feature_cols"]:
                raise ValueError("quote EV booster feature schema mismatch")
            return model

        fill_prob_model = load_booster(names["fill_prob"])
        bucket_models = {
            horizon: load_booster(name)
            for horizon, name in names["markout_buckets"].items()
        }
        bucket_features = {}
        bucket_values = {}
        bucket_classes = {}
        for horizon, name in names["markout_buckets"].items():
            meta = _load_meta(name)
            if not _legacy and (not meta.get("bucket_values") or not meta.get("classes")):
                raise ValueError("new quote EV bucket semantics required")
            bucket_features[horizon] = _load_features(name)
            values = meta.get("bucket_values") or DEFAULT_MARKOUT_BUCKET_VALUES
            bucket_values[horizon] = [float(v) for v in values]
            classes = meta.get("classes")
            if classes is None:
                classes = list(range(len(bucket_values[horizon])))
            bucket_classes[horizon] = [int(c) for c in classes]
        extreme_adverse_model = load_booster(names["extreme_adverse"])
        return cls(
            fill_prob_model=fill_prob_model,
            bucket_models=bucket_models,
            extreme_adverse_model=extreme_adverse_model,
            fill_prob_features=_load_features(names["fill_prob"]),
            bucket_features=bucket_features,
            bucket_values=bucket_values,
            bucket_classes=bucket_classes,
            extreme_adverse_features=_load_features(names["extreme_adverse"]),
            side=prefix,
            missing_policy="legacy_zero" if _legacy else next(iter(policies)),
            input_identity=None if _legacy else input_identity,
        )

    def predict_frame(self, frame, *, decision_ns):
        """Consume the shared validity mask before any quote-EV prediction."""
        from data.observation import model_row

        if self.input_identity is None or self.missing_policy == "legacy_zero":
            raise ValueError("public feature frames require a bound new-input model")
        columns = list(dict.fromkeys([
            *self.fill_prob_features, *self.extreme_adverse_features,
            *(name for names in self.bucket_features.values() for name in names),
        ]))
        metadata = {**self.input_identity, "feature_cols": columns,
                    "missing_policy": self.missing_policy}
        values = model_row(frame, metadata, decision_ns=decision_ns)
        return self.predict(dict(zip(columns, values, strict=True)))

    @staticmethod
    def _bucket_expected_value(probs: np.ndarray, classes: list[int], values: list[float]) -> float:
        probs = np.asarray(probs, dtype=np.float64).reshape(-1)
        if not len(probs):
            return 0.0
        total = 0.0
        for idx, prob in enumerate(probs):
            cls = int(classes[idx]) if idx < len(classes) else idx
            value = values[cls] if 0 <= cls < len(values) else values[-1]
            total += float(prob) * float(value)
        return total

    def predict(self, features: dict[str, Any]) -> QuoteEVPrediction:
        import math

        ev = 0.0
        toxic = 0.0
        fill_prob = 0.0
        fill_markout = 0.0
        fill_markout_1s = 0.0
        fill_markout_5s = 0.0
        toxic_given_fill = 0.0
        extreme_adverse_given_fill = 0.0
        bucket_probs: dict[int, list[float]] = {}

        if self.fill_prob_model is not None:
            fill_prob = float(self.fill_prob_model.predict(feature_array(features, self.fill_prob_features, missing_policy=self.missing_policy))[0])
            if not math.isfinite(fill_prob):
                raise ValueError("nonfinite quote EV fill probability")
            fill_prob = max(0.0, min(1.0, fill_prob))
        if self.fill_prob_model is not None and self.bucket_models:
            for horizon, model in self.bucket_models.items():
                raw = np.asarray(model.predict(
                    feature_array(features, self.bucket_features.get(horizon, DEFAULT_BID_QUOTE_FEATURES), missing_policy=self.missing_policy)
                ))
                probs = raw[0] if raw.ndim == 2 else raw.reshape(-1)
                if not np.isfinite(probs).all():
                    raise ValueError("nonfinite quote EV bucket probability")
                bucket_probs[horizon] = [float(p) for p in probs]
                expected = self._bucket_expected_value(
                    probs,
                    self.bucket_classes.get(horizon, list(range(len(probs)))),
                    self.bucket_values.get(horizon, DEFAULT_MARKOUT_BUCKET_VALUES),
                )
                if horizon == 1:
                    fill_markout_1s = expected
                elif horizon == 5:
                    fill_markout_5s = expected
                elif horizon == 30:
                    fill_markout = expected
            ev = fill_prob * fill_markout
            extreme_adverse_given_fill = float(self.extreme_adverse_model.predict(
                feature_array(features, self.extreme_adverse_features, missing_policy=self.missing_policy)
            )[0])
            if not math.isfinite(extreme_adverse_given_fill):
                raise ValueError("nonfinite quote EV adverse probability")
            extreme_adverse_given_fill = max(0.0, min(1.0, extreme_adverse_given_fill))
            toxic_given_fill = extreme_adverse_given_fill
            toxic = fill_prob * extreme_adverse_given_fill
        toxic = max(0.0, min(1.0, toxic))
        return QuoteEVPrediction(
            expected_maker_markout_bps_per_opportunity_30s=ev,
            toxic_30s=toxic,
            fill_prob=fill_prob,
            fill_markout_1s=fill_markout_1s,
            fill_markout_5s=fill_markout_5s,
            fill_markout_30s=fill_markout,
            toxic_given_fill_30s=toxic_given_fill,
            extreme_adverse_given_fill=extreme_adverse_given_fill,
            markout_bucket_probs=bucket_probs,
        )
