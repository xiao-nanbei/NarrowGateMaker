#!/usr/bin/env python3
"""Fail-closed parity gates for the exact owner-selected BUY E3 artifact.

The gates in this module are mechanics-only.  They bind compiled policy
semantics, frozen Development snapshots, and receive-time EMA state without
reading campaign outcomes or producing a new economic arm.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from itertools import combinations, product
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_repeated_policy_v1 as repeated,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1 as refit,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as owner_full_path,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CausalMultichannelEmaState,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
)
from strategy import boolean_cooldown_buy_e3 as live_runtime

IDENTITY = refit.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.parity_receipt.v1"
RESEARCH_COMPILED_LAYER = "research_compiled"
DEVELOPMENT_SNAPSHOT_LAYER = "development_snapshot"
STREAMING_OFFLINE_LAYER = "streaming_offline"
REPEATED_POLICY_LOCKSTEP_LAYER = "repeated_policy_lockstep"
SELL_OWNER_54_CASE_LAYER = "sell_owner_54_case_unchanged"
PARITY_LAYERS = (
    RESEARCH_COMPILED_LAYER,
    DEVELOPMENT_SNAPSHOT_LAYER,
    STREAMING_OFFLINE_LAYER,
    REPEATED_POLICY_LOCKSTEP_LAYER,
)
RECEIPT_LAYERS = (*PARITY_LAYERS, SELL_OWNER_54_CASE_LAYER)
DEFAULT_VECTOR_LIMIT = 50_000
DEFAULT_RANDOM_VECTORS = 4_096
DEFAULT_STREAMING_CALLBACK_COUNT = 20_482
LOCKSTEP_DAY_SCHEMA = f"{IDENTITY}.repeated_policy_lockstep_day.v1"
_SHA256_LENGTH = 64


class OwnerBuyE3ParityError(RuntimeError):
    """Raised when one exact-artifact parity gate does not close."""


@dataclass(frozen=True, slots=True)
class LoadedExactArtifact:
    manifest_path: Path
    policy_path: Path
    predicate_bundle_path: Path
    manifest_file_sha256: str
    policy_file_sha256: str
    predicate_bundle_file_sha256: str
    artifact_sha256: str
    manifest: Mapping[str, Any]
    policy_document: Mapping[str, Any]
    predicate_bundle_document: Mapping[str, Any]
    policy: BooleanCooldownPolicy
    runtime: live_runtime.LiveBuyE3CooldownPolicy


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise OwnerBuyE3ParityError(f"{label} is not a lowercase SHA256")
    return normalized


def _load_json(path: Path, *, expected_sha256: str, label: str) -> dict[str, Any]:
    expected = _require_sha256(expected_sha256, f"{label} file SHA256")
    if not path.is_file() or _file_sha256(path) != expected:
        raise OwnerBuyE3ParityError(f"{label} file SHA256 drifted")
    try:
        payload = json.loads(path.read_text(encoding="ascii"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3ParityError(f"{label} is unreadable") from exc
    if not isinstance(payload, dict):
        raise OwnerBuyE3ParityError(f"{label} root is not an object")
    return payload


def _parse_buy_policy(document: Mapping[str, Any]) -> BooleanCooldownPolicy:
    raw = document.get("policy")
    if (
        document.get("identity") != IDENTITY
        or not isinstance(raw, Mapping)
        or raw.get("side") != "BUY"
        or raw.get("default_action") != live_runtime.CONTROL_ACTION
    ):
        raise OwnerBuyE3ParityError("BUY E3 policy document identity drifted")
    rules: list[BooleanRule] = []
    try:
        for raw_rule in raw["ordered_first_match_rules"]:
            clauses = tuple(
                sorted(
                    (
                        AndClause(
                            tuple(
                                sorted(
                                    TriLiteral(
                                        predicate=str(literal["predicate"]),
                                        negated=bool(literal.get("negated", False)),
                                    )
                                    for literal in raw_clause["literals"]
                                )
                            )
                        )
                        for raw_clause in raw_rule["clauses"]
                    ),
                    key=lambda clause: clause.key,
                )
            )
            rules.append(BooleanRule(action=str(raw_rule["action"]), clauses=clauses))
    except (KeyError, TypeError, ValueError) as exc:
        raise OwnerBuyE3ParityError("BUY E3 Boolean policy is malformed") from exc
    policy = BooleanCooldownPolicy(side="BUY", rules=tuple(rules))
    if policy.payload() != raw:
        raise OwnerBuyE3ParityError("BUY E3 policy is not in canonical compiler form")
    return policy


def load_exact_artifact(
    *,
    artifact_manifest_path: Path,
    artifact_manifest_file_sha256: str,
    expected_artifact_sha256: str,
    policy_path: Path,
    policy_file_sha256: str,
    predicate_bundle_path: Path,
    predicate_bundle_file_sha256: str,
    warmup_s: float = 2048.0,
    max_feature_age_s: float = 1.0,
) -> LoadedExactArtifact:
    manifest_path = artifact_manifest_path.expanduser().resolve()
    exact_policy_path = policy_path.expanduser().resolve()
    bundle_path = predicate_bundle_path.expanduser().resolve()
    manifest_sha = _require_sha256(artifact_manifest_file_sha256, "artifact manifest file SHA256")
    policy_sha = _require_sha256(policy_file_sha256, "policy file SHA256")
    bundle_sha = _require_sha256(predicate_bundle_file_sha256, "predicate bundle file SHA256")
    artifact_sha = _require_sha256(expected_artifact_sha256, "artifact SHA256")
    manifest = _load_json(manifest_path, expected_sha256=manifest_sha, label="artifact manifest")
    policy_document = _load_json(exact_policy_path, expected_sha256=policy_sha, label="policy")
    predicate_document = _load_json(
        bundle_path, expected_sha256=bundle_sha, label="predicate bundle"
    )
    runtime = live_runtime.LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=manifest_path,
        artifact_manifest_sha256=manifest_sha,
        expected_artifact_sha256=artifact_sha,
        policy_path=exact_policy_path,
        policy_sha256=policy_sha,
        predicate_bundle_path=bundle_path,
        predicate_bundle_sha256=bundle_sha,
        warmup_s=warmup_s,
        max_feature_age_s=max_feature_age_s,
    )
    policy = _parse_buy_policy(policy_document)
    if tuple(policy.predicate_columns) != runtime.evaluator.predicate_columns:
        raise OwnerBuyE3ParityError("research and compiled predicate columns drifted")
    return LoadedExactArtifact(
        manifest_path=manifest_path,
        policy_path=exact_policy_path,
        predicate_bundle_path=bundle_path,
        manifest_file_sha256=manifest_sha,
        policy_file_sha256=policy_sha,
        predicate_bundle_file_sha256=bundle_sha,
        artifact_sha256=artifact_sha,
        manifest=manifest,
        policy_document=policy_document,
        predicate_bundle_document=predicate_document,
        policy=policy,
        runtime=runtime,
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> str:
    destination = path.expanduser().resolve()
    if destination.exists():
        raise OwnerBuyE3ParityError(f"immutable parity receipt exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            payload,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )
    with tempfile.NamedTemporaryFile(
        mode="wb", dir=destination.parent, prefix=f".{destination.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(encoded).hexdigest()


def _receipt(
    *,
    layer: str,
    artifact: LoadedExactArtifact,
    evidence: Mapping[str, Any],
    economic_values_materialized_by_replay: bool = False,
) -> dict[str, Any]:
    if layer not in RECEIPT_LAYERS:
        raise OwnerBuyE3ParityError("unknown BUY E3 parity layer")
    normalized_evidence = json.loads(
        json.dumps(dict(evidence), ensure_ascii=True, allow_nan=False)
    )
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "parity_complete",
        "layer": layer,
        "artifact_sha256": artifact.artifact_sha256,
        "artifact_manifest_file_sha256": artifact.manifest_file_sha256,
        "policy_file_sha256": artifact.policy_file_sha256,
        "predicate_bundle_file_sha256": artifact.predicate_bundle_file_sha256,
        "evidence": normalized_evidence,
        "economic_values_materialized_by_replay": bool(economic_values_materialized_by_replay),
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = refit.document_sha256(receipt, "canonical_receipt_sha256")
    return receipt


def _compiled_structure(policy: BooleanCooldownPolicy) -> tuple[Any, ...]:
    return tuple(
        (
            rule.action,
            tuple(
                tuple((literal.predicate, literal.negated) for literal in clause.literals)
                for clause in rule.clauses
            ),
        )
        for rule in policy.rules
    )


def _state_vectors(
    policy: BooleanCooldownPolicy,
    *,
    vector_limit: int,
    random_vector_count: int,
) -> tuple[tuple[int, ...], ...]:
    columns = policy.predicate_columns
    if vector_limit <= 0 or random_vector_count < 0:
        raise OwnerBuyE3ParityError("parity vector budget is invalid")
    vectors: dict[tuple[int, ...], None] = {}

    def add(values: Sequence[int]) -> None:
        vector = tuple(int(value) for value in values)
        if len(vector) != len(columns) or any(value not in (-1, 0, 1) for value in vector):
            raise OwnerBuyE3ParityError("parity vector is malformed")
        if len(vectors) < vector_limit:
            vectors.setdefault(vector, None)

    if len(columns) <= 10 and 3 ** len(columns) <= vector_limit:
        for values in product((-1, 0, 1), repeat=len(columns)):
            add(values)
        return tuple(vectors)

    for state in (-1, 0, 1):
        add((state,) * len(columns))
    positions = {name: index for index, name in enumerate(columns)}
    for background in (0, 1):
        for index in range(len(columns)):
            for state in (-1, 0, 1):
                values = [background] * len(columns)
                values[index] = state
                add(values)
        for left, right in combinations(range(len(columns)), 2):
            for left_state, right_state in product((-1, 0, 1), repeat=2):
                values = [background] * len(columns)
                values[left] = left_state
                values[right] = right_state
                add(values)
                if len(vectors) >= vector_limit:
                    return tuple(vectors)
    for rule in policy.rules:
        for clause in rule.clauses:
            witness = [0] * len(columns)
            for literal in clause.literals:
                witness[positions[literal.predicate]] = 0 if literal.negated else 1
            add(witness)
            for literal in clause.literals:
                unknown = list(witness)
                unknown[positions[literal.predicate]] = -1
                add(unknown)
                false = list(witness)
                false[positions[literal.predicate]] = 1 if literal.negated else 0
                add(false)
    random_source = random.Random(refit.OWNER_SEED)
    for _ in range(random_vector_count):
        add(tuple(random_source.choice((-1, 0, 1)) for _ in columns))
        if len(vectors) >= vector_limit:
            break
    return tuple(vectors)


def run_research_compiled_parity(
    artifact: LoadedExactArtifact,
    *,
    output_path: Path,
    vector_limit: int = DEFAULT_VECTOR_LIMIT,
    random_vector_count: int = DEFAULT_RANDOM_VECTORS,
) -> Mapping[str, Any]:
    expected_structure = _compiled_structure(artifact.policy)
    if artifact.runtime.evaluator.rules != expected_structure:
        raise OwnerBuyE3ParityError("compiled Boolean rule structure drifted")
    vectors = _state_vectors(
        artifact.policy,
        vector_limit=vector_limit,
        random_vector_count=random_vector_count,
    )
    research = successor.ResearchBooleanCooldownPolicyEvaluator(
        policies={"BUY": artifact.policy, "SELL": None},
        policy_identity=IDENTITY,
        policy_sha256=artifact.policy_file_sha256,
        predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
    )
    signatures: list[tuple[Any, ...]] = []
    baseline_duration_ms = 255_000
    columns = artifact.policy.predicate_columns
    for index, vector in enumerate(vectors):
        values = dict(zip(columns, vector, strict=True))
        expected = research.evaluate_predicates(
            side="BUY",
            predicate_values=values,
            baseline_duration_ms=baseline_duration_ms,
            snapshot_id=f"logical-vector-{index}",
        )
        observed = artifact.runtime.evaluator.evaluate(
            predicate_values=values,
            baseline_duration_ms=baseline_duration_ms,
        )
        signature = (
            expected.action_id,
            expected.duration_ms,
            expected.matched_rule_index,
            expected.fallback_reason,
            expected.support_valid,
        )
        if signature != observed:
            raise OwnerBuyE3ParityError(
                f"research/compiled Boolean mismatch at logical vector {index}"
            )
        signatures.append(signature)
    evidence = {
        "structural_rule_tree_equal": True,
        "predicate_count": len(columns),
        "rule_count": len(artifact.policy.rules),
        "logical_vector_count": len(vectors),
        "logical_vector_sha256": refit.canonical_sha256(vectors),
        "decision_signature_sha256": refit.canonical_sha256(signatures),
        "mismatch_count": 0,
    }
    receipt = _receipt(
        layer=RESEARCH_COMPILED_LAYER,
        artifact=artifact,
        evidence=evidence,
    )
    _atomic_json(output_path, receipt)
    return receipt


def _baseline_duration_ms(row: Mapping[str, Any]) -> int:
    raw = row.get("baseline_duration_ms")
    if isinstance(raw, bool):
        raise OwnerBuyE3ParityError("Development snapshot baseline is invalid")
    try:
        numeric = float(raw)
    except (TypeError, ValueError) as exc:
        raise OwnerBuyE3ParityError("Development snapshot baseline is missing") from exc
    if not math.isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        raise OwnerBuyE3ParityError("Development snapshot baseline is invalid")
    return int(numeric)


def run_development_snapshot_parity(
    artifact: LoadedExactArtifact,
    *,
    mechanics: backend.OutcomeBlindMechanics,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    output_path: Path,
    expected_opportunity_count: int = refit.EXPECTED_OPPORTUNITY_COUNT,
) -> Mapping[str, Any]:
    replay_inputs = mechanics.replay_inputs
    panel = mechanics.panel
    if (
        len(replay_inputs) != expected_opportunity_count
        or not replay_inputs.index.equals(panel.metadata.index)
        or not panel.boolean_features.index.equals(panel.metadata.index)
    ):
        raise OwnerBuyE3ParityError("Development snapshot denominator drifted")
    research = successor.ResearchBooleanCooldownPolicyEvaluator(
        policies={"BUY": artifact.policy, "SELL": None},
        policy_identity=IDENTITY,
        policy_sha256=artifact.policy_file_sha256,
        predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
    )
    signatures: list[tuple[Any, ...]] = []
    buy_count = 0
    sell_count = 0
    unobserved_count = 0
    columns = artifact.policy.predicate_columns
    for snapshot_index, row_series in replay_inputs.iterrows():
        row = row_series.to_dict()
        side = str(row.get("side", "")).upper()
        metadata_side = str(panel.metadata.at[snapshot_index, "side"]).upper()
        if side != metadata_side or side not in {"BUY", "SELL"}:
            raise OwnerBuyE3ParityError("Development snapshot side drifted")
        if side == "SELL":
            sell_count += 1
            signatures.append((str(snapshot_index), side, "exact_owner_unchanged"))
            continue
        buy_count += 1
        baseline = _baseline_duration_ms(row)
        values = predicate_view.materialize_snapshot_predicates(
            predicate_names=columns,
            feature_row=row,
            side=side,
            baseline_duration_ms=baseline,
            bundle=source_predicate_bundle,
        )
        for name, value in values.items():
            if name not in panel.boolean_features:
                raise OwnerBuyE3ParityError(
                    f"selected predicate is absent from Development panel: {name}"
                )
            if int(panel.boolean_features.at[snapshot_index, name]) != int(value):
                raise OwnerBuyE3ParityError(
                    f"Development predicate projection drifted at {snapshot_index}: {name}"
                )
        if any(int(value) == -1 for value in values.values()):
            unobserved_count += 1
            expected_signature = (
                live_runtime.CONTROL_ACTION,
                baseline,
                None,
                "selected_predicate_state_unobserved",
                False,
            )
            observed_signature = expected_signature
        else:
            expected = research.evaluate_predicates(
                side="BUY",
                predicate_values=values,
                baseline_duration_ms=baseline,
                snapshot_id=str(snapshot_index),
            )
            expected_signature = (
                expected.action_id,
                expected.duration_ms,
                expected.matched_rule_index,
                expected.fallback_reason,
                expected.support_valid,
            )
            observed_signature = artifact.runtime.evaluator.evaluate(
                predicate_values=values,
                baseline_duration_ms=baseline,
            )
        if expected_signature != observed_signature:
            raise OwnerBuyE3ParityError(
                f"Development action/duration parity drifted at {snapshot_index}"
            )
        signatures.append((str(snapshot_index), side, *expected_signature))
    if buy_count <= 0 or sell_count <= 0 or buy_count + sell_count != len(replay_inputs):
        raise OwnerBuyE3ParityError("Development side census drifted")
    evidence = {
        "opportunity_count": len(replay_inputs),
        "buy_snapshot_count": buy_count,
        "sell_snapshot_count": sell_count,
        "selected_predicate_count": len(columns),
        "selected_state_unobserved_count": unobserved_count,
        "predicate_projection_mismatch_count": 0,
        "action_duration_mismatch_count": 0,
        "snapshot_signature_sha256": refit.canonical_sha256(signatures),
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "frozen_source_predicate_bundle_sha256": source_predicate_bundle.file_sha256,
    }
    receipt = _receipt(
        layer=DEVELOPMENT_SNAPSHOT_LAYER,
        artifact=artifact,
        evidence=evidence,
    )
    _atomic_json(output_path, receipt)
    return receipt


def _stream_mid(index: int) -> float:
    phase = float(index)
    return 60_000.0 + 7.0 * math.sin(phase / 37.0) + 2.0 * math.cos(phase / 113.0) + 0.0005 * phase


def _feature_value_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-12)
    return left == right


def run_streaming_offline_parity(
    artifact: LoadedExactArtifact,
    *,
    output_path: Path,
    callback_count: int = DEFAULT_STREAMING_CALLBACK_COUNT,
) -> Mapping[str, Any]:
    if callback_count < DEFAULT_STREAMING_CALLBACK_COUNT:
        raise OwnerBuyE3ParityError("streaming parity does not cover full 2048s warmup")
    windows = live_runtime.ReceiveTimeFullMidEmaWindows(
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    offline = CausalMultichannelEmaState(
        block="R0",
        warmup_admitted=True,
        warmup_identity=f"{IDENTITY}.streaming_parity",
    )
    width = live_runtime.BASE_WINDOW_WIDTH_NS
    prior_mid: float | None = None
    completed = 0
    for index in range(callback_count):
        receive_ts_ns = index * width + 1
        mid = _stream_mid(index)
        windows.observe_depth(
            receive_ts_ns=receive_ts_ns,
            bids=((mid - 0.5, 1.0),),
            asks=((mid + 0.5, 1.0),),
            market_generation=index + 1,
            depth_generation=index + 1,
        )
        if prior_mid is not None:
            right_ts_ns = index * width
            completed += 1
            offline.update(
                CausalWindowObservation(
                    left_ts_ns=right_ts_ns - width,
                    right_ts_ns=right_ts_ns,
                    feature_ready_ts_ns=receive_ts_ns,
                    market_generation=completed,
                    depth_generation=completed,
                    values={"mid_usdc_per_btc": prior_mid},
                    warmup_admitted=True,
                )
            )
        prior_mid = mid
    decision_ts_ns = (callback_count - 1) * width + 1
    live_row, reason, feature_ready_ts_ns, feature_age_ms = windows.feature_row(
        decision_ts_ns=decision_ts_ns
    )
    if live_row is None or reason is not None:
        raise OwnerBuyE3ParityError(f"streaming parity did not reach an admitted state: {reason}")
    offline_row = offline.channel_feature_row(
        channel_name="mid_usdc_per_btc",
        side="BUY",
        decision_ts_ns=decision_ts_ns,
    )
    if set(live_row) != set(offline_row):
        raise OwnerBuyE3ParityError("streaming/offline feature schema drifted")
    mismatches = [
        name
        for name in sorted(live_row)
        if not _feature_value_equal(live_row[name], offline_row[name])
    ]
    if mismatches:
        raise OwnerBuyE3ParityError(f"streaming/offline feature values drifted: {mismatches[:5]}")
    audit = windows.audit()
    if (
        audit.get("warmup_time_admitted") != 1
        or audit.get("gap_resets") != 0
        or audit.get("out_of_order_updates") != 0
        or audit.get("invalid_updates") != 0
    ):
        raise OwnerBuyE3ParityError("streaming parity health counters drifted")
    feature_signature = tuple((name, live_row[name]) for name in sorted(live_row))
    evidence = {
        "callback_count": callback_count,
        "completed_window_count": completed,
        "ema_half_life_count": len(live_runtime.EMA_HALF_LIVES_S),
        "ema_pair_count": len(live_runtime.EMA_PAIRS_S),
        "feature_count": len(live_row),
        "feature_ready_ts_ns": feature_ready_ts_ns,
        "feature_age_ms": feature_age_ms,
        "feature_signature_sha256": refit.canonical_sha256(feature_signature),
        "feature_mismatch_count": 0,
        "gap_reset_count": 0,
        "out_of_order_count": 0,
    }
    receipt = _receipt(
        layer=STREAMING_OFFLINE_LAYER,
        artifact=artifact,
        evidence=evidence,
    )
    _atomic_json(output_path, receipt)
    return receipt


class _BoundSnapshotArtifactEvaluator:
    """Evaluate one frozen BUY artifact from a canonical replay snapshot."""

    def __init__(
        self,
        *,
        artifact: LoadedExactArtifact,
        source_predicate_bundle: predicate_view.FrozenPredicateBundle,
        expected_identity_hashes: Mapping[str, str],
        mode: str,
    ) -> None:
        if mode not in {"research", "compiled"}:
            raise OwnerBuyE3ParityError("snapshot evaluator mode is invalid")
        self.policy_identity = IDENTITY
        self.policy_sha256 = artifact.policy_file_sha256
        self.predicate_bundle_sha256 = artifact.predicate_bundle_file_sha256
        self._artifact = artifact
        self._source_predicate_bundle = source_predicate_bundle
        self._expected_identity_hashes = dict(expected_identity_hashes)
        self._mode = mode
        self._evaluations = 0
        self._fallbacks = 0
        self._delegate = successor.ResearchBooleanCooldownPolicyEvaluator(
            policies={"BUY": artifact.policy, "SELL": None},
            policy_identity=IDENTITY,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            expected_identity_hashes=expected_identity_hashes,
        )

    @property
    def binding_valid(self) -> bool:
        return True

    @property
    def binding_error(self) -> None:
        return None

    def _control(
        self,
        *,
        baseline_duration_ms: int,
        snapshot_id: str,
        reason: str,
    ) -> Any:
        self._fallbacks += 1
        return successor.CooldownDurationDecision(
            action_id=live_runtime.CONTROL_ACTION,
            duration_ms=baseline_duration_ms,
            fallback_reason=reason,
            matched_rule_index=None,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            snapshot_id=snapshot_id,
            support_valid=False,
        )

    def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
        self._evaluations += 1
        if not isinstance(snapshot, successor.CooldownAssignmentSnapshotV2):
            raise OwnerBuyE3ParityError("lockstep snapshot type drifted")
        if not snapshot.policy_input_valid or snapshot.policy_input is None:
            raise OwnerBuyE3ParityError("lockstep snapshot policy input is invalid")
        if snapshot.policy_input.snapshot_id != snapshot.snapshot_id:
            raise OwnerBuyE3ParityError("lockstep snapshot identity drifted")
        feature = snapshot.feature_row.to_dict()
        if feature != snapshot.policy_input.feature_row.to_dict():
            raise OwnerBuyE3ParityError("lockstep policy feature row drifted")
        if (
            snapshot.feature_block != "M2"
            or feature.get("feature_block") != "M2"
            or feature.get("support_valid") is not True
            or feature.get("channel_support_valid") is not True
            or feature.get("warmup_admitted") is not True
            or str(feature.get("side", "")).upper() != "BUY"
        ):
            raise OwnerBuyE3ParityError("lockstep snapshot feature support drifted")
        observed_hashes = snapshot.identity_hashes.to_dict()
        if any(
            observed_hashes.get(name) != expected
            for name, expected in self._expected_identity_hashes.items()
        ):
            raise OwnerBuyE3ParityError("lockstep snapshot source hash drifted")
        baseline = _baseline_duration_ms({"baseline_duration_ms": baseline_duration_ms})
        frozen_baseline = _baseline_duration_ms(feature)
        if baseline != frozen_baseline:
            raise OwnerBuyE3ParityError("lockstep baseline duration drifted")
        values = predicate_view.materialize_snapshot_predicates(
            predicate_names=self._artifact.policy.predicate_columns,
            feature_row=feature,
            side="BUY",
            baseline_duration_ms=baseline,
            bundle=self._source_predicate_bundle,
        )
        if any(int(value) == -1 for value in values.values()):
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=str(snapshot.snapshot_id),
                reason="selected_predicate_state_unobserved",
            )
        if self._mode == "research":
            return self._delegate.evaluate_predicates(
                side="BUY",
                predicate_values=values,
                baseline_duration_ms=baseline,
                snapshot_id=str(snapshot.snapshot_id),
            )
        action, duration, matched, reason, support = self._artifact.runtime.evaluator.evaluate(
            predicate_values=values,
            baseline_duration_ms=baseline,
        )
        return successor.CooldownDurationDecision(
            action_id=action,
            duration_ms=duration,
            fallback_reason=reason,
            matched_rule_index=matched,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            snapshot_id=str(snapshot.snapshot_id),
            support_valid=support,
        )

    def audit(self) -> dict[str, Any]:
        return {
            "identity": self.policy_identity,
            "policy_sha256": self.policy_sha256,
            "predicate_bundle_sha256": self.predicate_bundle_sha256,
            "mode": self._mode,
            "evaluations": self._evaluations,
            "selected_state_fallbacks": self._fallbacks,
            "source_predicate_bundle_sha256": (self._source_predicate_bundle.file_sha256),
            "research_only": True,
            "action_authorized": False,
            "live_authorized": False,
        }


def _lockstep_evaluator(
    *,
    artifact: LoadedExactArtifact,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    expected_identity_hashes: Mapping[str, str],
    learning_algorithm_artifact_sha256: str,
    mode: str,
    cutoff_ns: int,
) -> Any:
    artifact_binding = repeated.ArtifactIdentityBinding(
        executed_artifact_scope=repeated.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT,
        executed_policy_identity=IDENTITY,
        executed_policy_sha256=artifact.policy_file_sha256,
        executed_predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
        learning_algorithm_identity=f"{IDENTITY}.formal_v24_learning_algorithm",
        learning_algorithm_artifact_sha256=_require_sha256(
            learning_algorithm_artifact_sha256,
            "learning algorithm artifact SHA256",
        ),
        final_artifact_identity=IDENTITY,
        final_artifact_sha256=artifact.policy_file_sha256,
        exact_final_artifact_oof_available=False,
    )
    target = _BoundSnapshotArtifactEvaluator(
        artifact=artifact,
        source_predicate_bundle=source_predicate_bundle,
        expected_identity_hashes=expected_identity_hashes,
        mode=mode,
    )
    target_delegate = repeated.TargetSideDelegatingEvaluator(
        target_side=repeated.CandidateTargetSide.BUY,
        target_evaluator=target,
        b0_evaluator=replay_adapter._build_exact_owner_artifact_evaluator(
            expected_identity_hashes=expected_identity_hashes
        ),
        artifact_binding=artifact_binding,
    )
    return replay_adapter._TargetDayOnlyEvaluator(
        target_delegate,
        replay_adapter._build_exact_owner_artifact_evaluator(
            expected_identity_hashes=expected_identity_hashes
        ),
        predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
        cutoff_ns=cutoff_ns,
    )


def _assert_frame_lockstep(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    label: str,
) -> str:
    try:
        pd.testing.assert_frame_equal(left, right, check_exact=True, check_like=False)
    except AssertionError as exc:
        raise OwnerBuyE3ParityError(f"{label} lockstep drifted") from exc
    left_sha = replay_adapter._frame_sha256(left)
    if left_sha != replay_adapter._frame_sha256(right):
        raise OwnerBuyE3ParityError(f"{label} frame SHA256 drifted")
    return left_sha


def _summary_signature(summary: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    fields = (
        "terminal_mtm_pnl_usdc",
        "closed_campaign_value_usdc",
        "fills_total",
        "fills_bid",
        "fills_ask",
        "abs_inventory_time_btc_s",
        "max_inventory_btc",
        "final_inventory_btc",
        "repeated_policy_decision_count",
        "cooldown_v2_snapshot_count",
        "cooldown_v2_fallback_snapshot_count",
    )
    missing = sorted(set(fields) - set(summary))
    if missing:
        raise OwnerBuyE3ParityError(f"lockstep summary fields are missing: {missing}")
    return tuple((field, repr(summary[field])) for field in fields)


def _run_lockstep_day(
    *,
    artifact: LoadedExactArtifact,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    learning_algorithm_artifact_sha256: str,
    utc_day: str,
    rows: pd.DataFrame,
    portable_binding: Mapping[str, Any],
    temporary_root: Path,
) -> Mapping[str, Any]:
    request, replay = replay_adapter._canonical_day_projection_from_rows(
        utc_day=utc_day,
        binding=portable_binding,
        rows=rows,
    )
    identity_hashes = replay_adapter._day_identity_hashes(request)
    cutoff_ns = (int(pd.Timestamp(utc_day, tz="UTC").timestamp()) + 86_400) * 1_000_000_000

    def emitter() -> Any:
        return replay_adapter._build_day_snapshot_emitter(
            request,
            replay,
            utc_day=utc_day,
            identity_hashes=identity_hashes,
        )

    window = SimpleNamespace(
        trades=replay.trades,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
        bbo_data=replay.bbo_data,
        l2_data=replay.l2_data,
        var_ti=replay.var_ti,
        var_retsq=replay.var_retsq,
    )
    outputs: dict[str, tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]] = {}
    for mode in ("research", "compiled"):
        outputs[mode] = owner_full_path._simulate_python_arm(
            day=utc_day,
            arm=owner_full_path.CANDIDATE_ARM,
            window=window,
            ml_data=replay.ml_data,
            base=replay.params,
            progress_path=temporary_root / f"{utc_day}-{mode}-progress.json",
            progress_interval_events=owner_full_path.DEFAULT_PROGRESS_INTERVAL_EVENTS,
            emitter=emitter(),
            evaluator=_lockstep_evaluator(
                artifact=artifact,
                source_predicate_bundle=source_predicate_bundle,
                expected_identity_hashes=identity_hashes,
                learning_algorithm_artifact_sha256=(learning_algorithm_artifact_sha256),
                mode=mode,
                cutoff_ns=cutoff_ns,
            ),
        )
    reference_summary, reference_campaigns, reference_fills, reference_decisions = outputs[
        "research"
    ]
    compiled_summary, compiled_campaigns, compiled_fills, compiled_decisions = outputs["compiled"]
    reference_signature = _summary_signature(reference_summary)
    compiled_signature = _summary_signature(compiled_summary)
    if reference_signature != compiled_signature:
        raise OwnerBuyE3ParityError(f"{utc_day} summary lockstep drifted")
    return {
        "summary_signature_sha256": refit.canonical_sha256(reference_signature),
        "campaign_frame_sha256": _assert_frame_lockstep(
            reference_campaigns, compiled_campaigns, label=f"{utc_day} campaign"
        ),
        "fill_frame_sha256": _assert_frame_lockstep(
            reference_fills, compiled_fills, label=f"{utc_day} fill"
        ),
        "decision_frame_sha256": _assert_frame_lockstep(
            reference_decisions, compiled_decisions, label=f"{utc_day} decision"
        ),
        "decision_count": len(reference_decisions),
        "campaign_count": len(reference_campaigns),
        "fill_count": len(reference_fills),
        "mismatch_count": 0,
    }


def _load_lockstep_day_receipt(
    path: Path,
    *,
    utc_day: str,
    artifact: LoadedExactArtifact,
    mechanics_receipt_sha256: str,
    source_predicate_bundle_sha256: str,
    parity_source_sha256: str,
) -> Mapping[str, Any]:
    try:
        receipt = json.loads(path.read_text(encoding="ascii"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3ParityError(f"{utc_day} lockstep receipt is unreadable") from exc
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("schema_version") != LOCKSTEP_DAY_SCHEMA
        or receipt.get("identity") != IDENTITY
        or receipt.get("status") != "day_lockstep_complete"
        or receipt.get("utc_day") != utc_day
        or receipt.get("artifact_sha256") != artifact.artifact_sha256
        or receipt.get("mechanics_receipt_sha256") != mechanics_receipt_sha256
        or receipt.get("source_predicate_bundle_sha256") != source_predicate_bundle_sha256
        or receipt.get("parity_source_sha256") != parity_source_sha256
        or receipt.get("economic_values_exposed") is not False
        or receipt.get("economic_values_used_for_selection") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("canonical_day_receipt_sha256")
        != refit.document_sha256(receipt, "canonical_day_receipt_sha256")
    ):
        raise OwnerBuyE3ParityError(f"{utc_day} lockstep receipt identity drifted")
    return dict(receipt)


def run_repeated_policy_lockstep_parity(
    artifact: LoadedExactArtifact,
    *,
    mechanics: backend.OutcomeBlindMechanics,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    learning_algorithm_artifact_sha256: str,
    day_receipt_dir: Path,
    output_path: Path,
    expected_day_count: int = refit.EXPECTED_DAY_COUNT,
) -> Mapping[str, Any]:
    days = tuple(mechanics.selected_days)
    if len(days) != expected_day_count or days != tuple(sorted(set(days))):
        raise OwnerBuyE3ParityError("lockstep Development day identity drifted")
    if output_path.expanduser().resolve().exists():
        raise OwnerBuyE3ParityError("immutable lockstep final receipt already exists")
    day_root = day_receipt_dir.expanduser().resolve()
    day_root.mkdir(parents=True, exist_ok=True)
    options = replay_adapter._resolve_execution_options(mechanics.replay_inputs)
    parity_source_sha = _file_sha256(Path(__file__).resolve())
    admitted: list[dict[str, str]] = []
    with tempfile.TemporaryDirectory(prefix="f05-buy-e3-lockstep-") as temporary:
        temporary_root = Path(temporary)
        for utc_day in days:
            day_path = day_root / f"{utc_day}.json"
            if day_path.exists():
                day_receipt = _load_lockstep_day_receipt(
                    day_path,
                    utc_day=utc_day,
                    artifact=artifact,
                    mechanics_receipt_sha256=mechanics.mechanics_receipt_sha256,
                    source_predicate_bundle_sha256=source_predicate_bundle.file_sha256,
                    parity_source_sha256=parity_source_sha,
                )
            else:
                rows = mechanics.replay_inputs.loc[
                    mechanics.replay_inputs["utc_day"].astype(str) == utc_day
                ].copy()
                if rows.empty:
                    raise OwnerBuyE3ParityError(f"{utc_day} lockstep rows are missing")
                result = _run_lockstep_day(
                    artifact=artifact,
                    source_predicate_bundle=source_predicate_bundle,
                    learning_algorithm_artifact_sha256=(learning_algorithm_artifact_sha256),
                    utc_day=utc_day,
                    rows=rows,
                    portable_binding=options.binding,
                    temporary_root=temporary_root,
                )
                day_receipt = {
                    "schema_version": LOCKSTEP_DAY_SCHEMA,
                    "identity": IDENTITY,
                    "status": "day_lockstep_complete",
                    "utc_day": utc_day,
                    "artifact_sha256": artifact.artifact_sha256,
                    "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
                    "source_predicate_bundle_sha256": (source_predicate_bundle.file_sha256),
                    "parity_source_sha256": parity_source_sha,
                    "day_input_sha256": str(rows["day_input_sha256"].iloc[0]),
                    "lockstep": dict(result),
                    "economic_values_materialized_by_replay": True,
                    "economic_values_exposed": False,
                    "economic_values_used_for_selection": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                    "action_authorized": False,
                    "live_authorized": False,
                }
                day_receipt["canonical_day_receipt_sha256"] = refit.document_sha256(
                    day_receipt, "canonical_day_receipt_sha256"
                )
                _atomic_json(day_path, day_receipt)
            admitted.append(
                {
                    "utc_day": utc_day,
                    "file_sha256": _file_sha256(day_path),
                    "canonical_day_receipt_sha256": str(
                        day_receipt["canonical_day_receipt_sha256"]
                    ),
                }
            )
    evidence = {
        "day_count": len(admitted),
        "day_receipts": admitted,
        "parity_source_sha256": parity_source_sha,
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "source_predicate_bundle_sha256": source_predicate_bundle.file_sha256,
        "mismatch_count": 0,
        "deadline_lockstep": True,
        "fill_lockstep": True,
        "campaign_lockstep": True,
    }
    receipt = _receipt(
        layer=REPEATED_POLICY_LOCKSTEP_LAYER,
        artifact=artifact,
        evidence=evidence,
        economic_values_materialized_by_replay=True,
    )
    _atomic_json(output_path, receipt)
    return receipt


def validate_parity_receipt(
    path: Path,
    *,
    expected_layer: str,
    expected_artifact_sha256: str,
) -> Mapping[str, Any]:
    try:
        receipt = json.loads(path.expanduser().resolve().read_text(encoding="ascii"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3ParityError("parity receipt is unreadable") from exc
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("identity") != IDENTITY
        or receipt.get("status") != "parity_complete"
        or receipt.get("layer") != expected_layer
        or receipt.get("artifact_sha256") != expected_artifact_sha256
        or receipt.get("economic_values_materialized_by_replay")
        is not (expected_layer == REPEATED_POLICY_LOCKSTEP_LAYER)
        or receipt.get("economic_values_exposed") is not False
        or receipt.get("economic_values_used_for_selection") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_receipt_sha256")
        != refit.document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise OwnerBuyE3ParityError("parity receipt identity drifted")
    return dict(receipt)


def run_sell_owner_54_case_unchanged(
    artifact: LoadedExactArtifact,
    *,
    sell_policy_path: Path,
    sell_predicate_bundle_path: Path,
    output_path: Path,
) -> Mapping[str, Any]:
    """Prove that the pre-existing SELL owner evaluator remains byte-exact."""

    parity = successor.audit_exact_owner_artifact_parity(
        policy_path=sell_policy_path,
        predicate_bundle_path=sell_predicate_bundle_path,
    )
    evidence = asdict(parity)
    if (
        evidence.get("sell_tri_state_cases") != 27
        or evidence.get("buy_tri_state_cases") != 27
        or evidence.get("mismatch_count") != 0
        or evidence.get("documented_semantics_equal") is not True
        or evidence.get("runtime_binding_valid") is not True
    ):
        raise OwnerBuyE3ParityError("pre-existing SELL 54-case parity drifted")
    receipt = _receipt(
        layer=SELL_OWNER_54_CASE_LAYER,
        artifact=artifact,
        evidence=evidence,
        economic_values_materialized_by_replay=False,
    )
    _atomic_json(output_path, receipt)
    return receipt


__all__ = [
    "DEVELOPMENT_SNAPSHOT_LAYER",
    "LoadedExactArtifact",
    "OwnerBuyE3ParityError",
    "PARITY_LAYERS",
    "REPEATED_POLICY_LOCKSTEP_LAYER",
    "RESEARCH_COMPILED_LAYER",
    "SELL_OWNER_54_CASE_LAYER",
    "STREAMING_OFFLINE_LAYER",
    "load_exact_artifact",
    "run_development_snapshot_parity",
    "run_repeated_policy_lockstep_parity",
    "run_research_compiled_parity",
    "run_sell_owner_54_case_unchanged",
    "run_streaming_offline_parity",
    "validate_parity_receipt",
]
