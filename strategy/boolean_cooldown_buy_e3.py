"""Hash-bound BUY E3 cooldown runtime with receive-time EMA state."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

from strategy.native_cooldown import (
    build_hot_path,
    native_fallback_reason,
)

BASE_WINDOW_WIDTH_NS = 100_000_000
CONTROL_ACTION = "CONTROL_85N"
OWNER_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
OWNER_POLICY_SCHEMA = f"{OWNER_IDENTITY}.artifact.v1"
OWNER_BUNDLE_SCHEMA = f"{OWNER_IDENTITY}.selected_predicate_bundle.v1"
OWNER_MANIFEST_SCHEMA = f"{OWNER_IDENTITY}.full_development_refit.v1"
# Deployment authorization and release lineage are private inputs.
SELECTED_CANDIDATE = "E3_HIGHER_ORDER_BOOLEAN"
SELECTED_PROFILE = "e3_high_order_multirule_dnf_v1"
LIVE_FEATURE_TRANSPORT_IDENTITY = "receive_time_100ms_full_mid_ema_bank_v1"
EMA_HALF_LIVES_S = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)
EMA_PAIRS_S = tuple(combinations(EMA_HALF_LIVES_S, 2))
BUY_FIXED_ACTIONS = (
    "FIXED_79S",
    "FIXED_173S",
    "FIXED_223S",
    "FIXED_356S",
    "FIXED_640S",
    "FIXED_709S",
    "FIXED_2048S",
)
BUY_ACTIONS = (CONTROL_ACTION, *BUY_FIXED_ACTIONS)
DIRECT_CAMPAIGN_AGE = "predicate::m0::campaign_age_gt_control_duration"

_DURATION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

@dataclass(frozen=True, slots=True)
class BuyE3CooldownDecision:
    action_id: str
    duration_ms: int
    fallback_reason: str | None
    matched_rule_index: int | None
    support_valid: bool
    policy_sha256: str
    predicate_bundle_sha256: str
    artifact_sha256: str
    feature_ready_ts_ns: int
    feature_age_ms: float


@dataclass(slots=True)
class _PairState:
    effective_sign: int = 0
    arrangement_start_ts_ns: int | None = None
    last_cross_ts_ns: int | None = None
    last_cross_direction: int = 0


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    file_type: int
    uid: int
    gid: int
    mode: int
    link_count: int
    size: int
    mtime_ns: int
    ctime_ns: int


@dataclass(slots=True)
class _OpenedBoundJson:
    path: Path
    label: str
    identity: _FileIdentity
    descriptor: int
    payload: dict[str, Any]


def _absolute_without_resolving(path: str | Path) -> Path:
    expanded = Path(path).expanduser()
    absolute = Path(os.path.abspath(os.fspath(expanded)))
    return absolute.parent.resolve(strict=True) / absolute.name


def _reject_symlink_components(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"{label}_path_contains_symlink")


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=int(value.st_dev),
        inode=int(value.st_ino),
        file_type=stat.S_IFMT(value.st_mode),
        uid=int(value.st_uid),
        gid=int(value.st_gid),
        mode=stat.S_IMODE(value.st_mode),
        link_count=int(value.st_nlink),
        size=int(value.st_size),
        mtime_ns=int(value.st_mtime_ns),
        ctime_ns=int(value.st_ctime_ns),
    )


def _require_private_regular_file(identity: _FileIdentity, label: str) -> None:
    if identity.file_type != stat.S_IFREG:
        raise ValueError(f"{label}_not_regular_file")
    if identity.uid != os.geteuid():
        raise ValueError(f"{label}_owner_mismatch")
    if identity.link_count != 1:
        raise ValueError(f"{label}_link_count_mismatch")
    if identity.mode != 0o600:
        raise ValueError(f"{label}_mode_not_private")


def _read_descriptor(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1 << 20)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _open_bound_json(
    path: Path,
    expected_file_sha256: str,
    label: str,
) -> _OpenedBoundJson:
    expected = str(expected_file_sha256).strip().lower()
    if _SHA256_RE.fullmatch(expected) is None:
        raise ValueError(f"{label}_file_sha256_mismatch")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError(f"{label}_nofollow_unavailable")
    try:
        _reject_symlink_components(path, label)
        before = _file_identity(os.lstat(path))
        _require_private_regular_file(before, label)
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | nofollow,
        )
    except OSError as exc:
        raise ValueError(f"{label}_unreadable") from exc
    try:
        opened = _file_identity(os.fstat(descriptor))
        _require_private_regular_file(opened, label)
        if opened != before:
            raise ValueError(f"{label}_identity_changed_during_open")
        raw = _read_descriptor(descriptor)
        after_read = _file_identity(os.fstat(descriptor))
        after_path = _file_identity(os.lstat(path))
        if after_read != opened or after_path != opened or len(raw) != opened.size:
            raise ValueError(f"{label}_identity_changed_during_read")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError(f"{label}_file_sha256_mismatch")

        def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"{label}_duplicate_json_key")
                result[key] = value
            return result

        try:
            payload = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=reject_duplicates,
                parse_constant=lambda token: (_ for _ in ()).throw(
                    ValueError(f"{label}_non_finite_json_token_{token}")
                ),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"{label}_unreadable") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"{label}_root_not_object")
        return _OpenedBoundJson(
            path=path,
            label=label,
            identity=opened,
            descriptor=descriptor,
            payload=payload,
        )
    except Exception:
        os.close(descriptor)
        raise


def _revalidate_opened_file(opened: _OpenedBoundJson) -> None:
    try:
        _reject_symlink_components(opened.path, opened.label)
        descriptor_identity = _file_identity(os.fstat(opened.descriptor))
        path_identity = _file_identity(os.lstat(opened.path))
    except OSError as exc:
        raise ValueError(f"{opened.label}_identity_revalidation_failed") from exc
    if descriptor_identity != opened.identity or path_identity != opened.identity:
        raise ValueError(f"{opened.label}_identity_changed_after_load")


def _close_opened_files(files: Sequence[_OpenedBoundJson]) -> None:
    for opened in files:
        os.close(opened.descriptor)


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _validate_canonical(payload: Mapping[str, Any], field: str, label: str) -> None:
    observed = str(payload.get(field, ""))
    body = dict(payload)
    body.pop(field, None)
    if _SHA256_RE.fullmatch(observed) is None or _canonical_sha256(body) != observed:
        raise ValueError(f"{label}_canonical_sha256_mismatch")


def _exact_mapping(value: Any, fields: frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        raise ValueError(f"{label}_fields_drifted")
    return value


def _private_release_boundary_marker() -> None:
    """Deployment release validation is supplied by the private repository."""
    return None


def _label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


def _pair_key(fast: float, slow: float) -> str:
    return f"mid_usdc_per_btc__{_label(fast)}__{_label(slow)}"


def _tri_not(value: int) -> int:
    return -1 if value == -1 else 1 - value


def _literal_state(value: int, negated: bool) -> int:
    if value not in (-1, 0, 1):
        raise ValueError("runtime_predicate_not_three_valued")
    return _tri_not(value) if negated else value


def _and_state(values: Sequence[int]) -> int:
    if any(value == 0 for value in values):
        return 0
    return -1 if any(value == -1 for value in values) else 1


def _or_state(values: Sequence[int]) -> int:
    if any(value == 1 for value in values):
        return 1
    return -1 if any(value == -1 for value in values) else 0


class _CompiledBuyE3Evaluator:
    def __init__(
        self,
        *,
        rules: tuple[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]], ...],
        predicate_columns: tuple[str, ...],
        policy_sha256: str,
        predicate_bundle_sha256: str,
        artifact_sha256: str,
    ) -> None:
        self.rules = rules
        self.predicate_columns = predicate_columns
        self.policy_sha256 = policy_sha256
        self.predicate_bundle_sha256 = predicate_bundle_sha256
        self.artifact_sha256 = artifact_sha256

    def evaluate(
        self,
        *,
        predicate_values: Mapping[str, int],
        baseline_duration_ms: int,
    ) -> tuple[str, int, int | None, str | None, bool]:
        if tuple(sorted(predicate_values)) != self.predicate_columns:
            return (
                CONTROL_ACTION,
                baseline_duration_ms,
                None,
                "runtime_predicate_columns_drifted",
                False,
            )
        for index, (action, clauses) in enumerate(self.rules):
            state = _or_state(
                tuple(
                    _and_state(
                        tuple(
                            _literal_state(int(predicate_values[name]), negated)
                            for name, negated in clause
                        )
                    )
                    for clause in clauses
                )
            )
            if state == -1:
                return (
                    CONTROL_ACTION,
                    baseline_duration_ms,
                    None,
                    f"rule_unobserved:{index}",
                    False,
                )
            if state == 1:
                match = _DURATION_RE.fullmatch(action)
                assert match is not None
                return action, int(match.group(1)) * 1_000, index, None, True
        return CONTROL_ACTION, baseline_duration_ms, None, "no_rule_matched", True


class _FullMidEmaState:
    """Lean copy of the frozen research mid-channel recursion."""

    def __init__(self) -> None:
        self._index = {value: index for index, value in enumerate(EMA_HALF_LIVES_S)}
        self.ema: list[float] = []
        self.velocity: list[float] = []
        self.acceleration: list[float] = []
        self.last_ts_ns: int | None = None
        self.current_window_observed = False
        self.pairs = {pair: _PairState() for pair in EMA_PAIRS_S}

    def update(self, *, ts_ns: int, value: float) -> None:
        if not math.isfinite(float(value)):
            raise ValueError("observed_mid_nonfinite")
        timestamp = int(ts_ns)
        current_value = float(value)
        self.current_window_observed = True
        if self.last_ts_ns is None:
            self.ema = [current_value] * len(EMA_HALF_LIVES_S)
            self.velocity = [0.0] * len(EMA_HALF_LIVES_S)
            self.acceleration = [0.0] * len(EMA_HALF_LIVES_S)
            self.last_ts_ns = timestamp
            return
        if timestamp <= self.last_ts_ns:
            raise ValueError("mid_ema_clock_must_increase")
        delta_s = (timestamp - self.last_ts_ns) / 1_000_000_000.0
        prior = tuple(self.ema)
        prior_velocity = tuple(self.velocity)
        for index, half_life in enumerate(EMA_HALF_LIVES_S):
            decay = math.exp(-math.log(2.0) * delta_s / half_life)
            current = decay * prior[index] + (1.0 - decay) * current_value
            velocity = (current - prior[index]) / delta_s
            self.ema[index] = current
            self.velocity[index] = velocity
            self.acceleration[index] = (velocity - prior_velocity[index]) / delta_s
        for fast, slow in EMA_PAIRS_S:
            distance = self.ema[self._index[fast]] - self.ema[self._index[slow]]
            sign = 1 if distance > 0.0 else -1 if distance < 0.0 else 0
            state = self.pairs[(fast, slow)]
            if sign and state.effective_sign == 0:
                state.effective_sign = sign
                state.arrangement_start_ts_ns = timestamp
            elif sign and sign != state.effective_sign:
                state.effective_sign = sign
                state.arrangement_start_ts_ns = timestamp
                state.last_cross_ts_ns = timestamp
                state.last_cross_direction = sign
        self.last_ts_ns = timestamp

    def mark_current_window_unobserved(self) -> None:
        self.current_window_observed = False

    def feature_row(self, *, decision_ts_ns: int) -> dict[str, Any]:
        output: dict[str, Any] = {
            "channel::mid_usdc_per_btc::observed": int(
                self.current_window_observed
                and self.last_ts_ns is not None
                and self.last_ts_ns <= int(decision_ts_ns)
            )
        }
        if not output["channel::mid_usdc_per_btc::observed"]:
            for fast, slow in EMA_PAIRS_S:
                prefix = _pair_key(fast, slow)
                output[f"tri::{prefix}::positive_ordering"] = -1
                output[f"tri::{prefix}::last_cross_positive"] = -1
            return output
        for half_life, value, velocity, acceleration in zip(
            EMA_HALF_LIVES_S,
            self.ema,
            self.velocity,
            self.acceleration,
            strict=True,
        ):
            label = _label(half_life)
            output[f"value::mid_usdc_per_btc::ema::{label}"] = value
            output[f"value::mid_usdc_per_btc::slope::{label}"] = velocity
            output[f"value::mid_usdc_per_btc::curvature::{label}"] = acceleration
        for fast, slow in EMA_PAIRS_S:
            fast_index = self._index[fast]
            slow_index = self._index[slow]
            raw_distance = self.ema[fast_index] - self.ema[slow_index]
            raw_velocity = self.velocity[fast_index] - self.velocity[slow_index]
            raw_acceleration = self.acceleration[fast_index] - self.acceleration[slow_index]
            state = self.pairs[(fast, slow)]
            prefix = _pair_key(fast, slow)
            output[f"tri::{prefix}::positive_ordering"] = (
                -1 if state.effective_sign == 0 else int(state.effective_sign > 0)
            )
            output[f"tri::{prefix}::last_cross_positive"] = (
                -1 if state.last_cross_ts_ns is None else int(state.last_cross_direction > 0)
            )
            output[f"value::{prefix}::cross_age_s"] = (
                None
                if state.last_cross_ts_ns is None
                else (int(decision_ts_ns) - state.last_cross_ts_ns) / 1_000_000_000.0
            )
            output[f"value::{prefix}::arrangement_persistence_s"] = (
                None
                if state.arrangement_start_ts_ns is None
                else (int(decision_ts_ns) - state.arrangement_start_ts_ns) / 1_000_000_000.0
            )
            output[f"value::{prefix}::signed_distance"] = raw_distance
            output[f"value::{prefix}::abs_distance"] = abs(raw_distance)
            output[f"value::{prefix}::signed_distance_velocity"] = raw_velocity
            output[f"value::{prefix}::signed_distance_acceleration"] = raw_acceleration
            expansion_product = raw_distance * raw_velocity
            output[f"tri::{prefix}::expanding"] = int(expansion_product > 0.0)
            output[f"tri::{prefix}::converging"] = int(expansion_product < 0.0)
        return output


class ReceiveTimeFullMidEmaWindows:
    """Finalize 100ms receive-time windows and fail closed across gaps."""

    def __init__(self, *, warmup_s: float, max_feature_age_s: float) -> None:
        if not math.isfinite(float(warmup_s)) or float(warmup_s) < 2048.0:
            raise ValueError("buy_e3_warmup_must_cover_2048_seconds")
        if not math.isfinite(float(max_feature_age_s)) or float(max_feature_age_s) <= 0.0:
            raise ValueError("buy_e3_max_feature_age_s_invalid")
        self.warmup_s = float(warmup_s)
        self.max_feature_age_s = float(max_feature_age_s)
        self._lock = threading.RLock()
        self._state = _FullMidEmaState()
        self._pending_left_ns: int | None = None
        self._pending_mid: float | None = None
        self._feature_ready_ts_ns = 0
        self._warmup_start_right_ns: int | None = None
        self._last_window_right_ns: int | None = None
        self._updates = 0
        self._windows = 0
        self._gap_windows = 0
        self._resets = 0
        self._invalid = 0
        self._out_of_order = 0
        self._gap_count = 0
        self._last_error = ""

    def _reset_locked(self, reason: str) -> None:
        self._state = _FullMidEmaState()
        self._pending_left_ns = None
        self._pending_mid = None
        self._feature_ready_ts_ns = 0
        self._warmup_start_right_ns = None
        self._last_window_right_ns = None
        self._resets += 1
        self._last_error = str(reason)

    def _emit_locked(
        self,
        *,
        left_ns: int,
        feature_ready_ts_ns: int,
        mid: float | None,
        source_gap: bool,
    ) -> None:
        right_ns = int(left_ns + BASE_WINDOW_WIDTH_NS)
        if source_gap:
            self._state.mark_current_window_unobserved()
            self._gap_windows += 1
        else:
            assert mid is not None
            self._state.update(ts_ns=right_ns, value=float(mid))
            if self._warmup_start_right_ns is None:
                self._warmup_start_right_ns = right_ns
        self._last_window_right_ns = right_ns
        self._feature_ready_ts_ns = int(feature_ready_ts_ns)
        self._windows += 1

    def reset(self, reason: str = "runtime_restart") -> None:
        with self._lock:
            self._reset_locked(reason)

    def observe_depth(
        self,
        *,
        receive_ts_ns: int,
        bids: Sequence[tuple[float, float]],
        asks: Sequence[tuple[float, float]],
        market_generation: int,
        depth_generation: int,
    ) -> None:
        del market_generation, depth_generation
        try:
            receive_ns = int(receive_ts_ns)
            if receive_ns <= 0 or not bids or not asks:
                raise ValueError("depth_callback_invalid")
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            if not (math.isfinite(bid) and math.isfinite(ask) and 0.0 < bid < ask):
                raise ValueError("depth_callback_bbo_invalid")
            mid = (bid + ask) / 2.0
            left_ns = (receive_ns // BASE_WINDOW_WIDTH_NS) * BASE_WINDOW_WIDTH_NS
            with self._lock:
                self._updates += 1
                pending = self._pending_left_ns
                if pending is None:
                    self._pending_left_ns = left_ns
                    self._pending_mid = mid
                    return
                if left_ns < pending:
                    self._out_of_order += 1
                    return
                if left_ns == pending:
                    self._pending_mid = mid
                    return
                gap_windows = max(
                    0,
                    (left_ns - pending) // BASE_WINDOW_WIDTH_NS - 1,
                )
                gap_s = gap_windows * BASE_WINDOW_WIDTH_NS / 1_000_000_000.0
                if gap_s > self.max_feature_age_s:
                    self._gap_count += 1
                    self._reset_locked("depth_gap_exceeded_execution_freshness")
                    self._pending_left_ns = left_ns
                    self._pending_mid = mid
                    return
                self._emit_locked(
                    left_ns=pending,
                    feature_ready_ts_ns=receive_ns,
                    mid=self._pending_mid,
                    source_gap=False,
                )
                for offset in range(1, int(gap_windows) + 1):
                    self._emit_locked(
                        left_ns=pending + offset * BASE_WINDOW_WIDTH_NS,
                        feature_ready_ts_ns=receive_ns,
                        mid=None,
                        source_gap=True,
                    )
                self._pending_left_ns = left_ns
                self._pending_mid = mid
        except Exception as exc:
            with self._lock:
                self._invalid += 1
                self._last_error = f"{type(exc).__name__}:{exc}"

    def feature_row(
        self,
        *,
        decision_ts_ns: int,
    ) -> tuple[dict[str, Any] | None, str | None, int, float]:
        with self._lock:
            ready = self._feature_ready_ts_ns
            age_ms = max(0, int(decision_ts_ns) - ready) / 1_000_000.0 if ready > 0 else math.inf
            if ready <= 0 or self._warmup_start_right_ns is None:
                return None, "no_completed_receive_time_window", 0, age_ms
            last_right = self._last_window_right_ns or 0
            elapsed_s = (last_right - self._warmup_start_right_ns) / 1_000_000_000.0
            if elapsed_s < self.warmup_s:
                return None, "receive_time_ema_warmup_incomplete", ready, age_ms
            if age_ms > self.max_feature_age_s * 1_000.0:
                return None, "receive_time_mid_state_stale", ready, age_ms
            return self._state.feature_row(decision_ts_ns=int(decision_ts_ns)), None, ready, age_ms

    def audit(self) -> dict[str, Any]:
        with self._lock:
            last_right = self._last_window_right_ns or 0
            elapsed_s = (
                0.0
                if self._warmup_start_right_ns is None
                else (last_right - self._warmup_start_right_ns) / 1_000_000_000.0
            )
            return {
                "updates": self._updates,
                "completed_windows": self._windows,
                "gap_windows": self._gap_windows,
                "resets": self._resets,
                "invalid_updates": self._invalid,
                "out_of_order_updates": self._out_of_order,
                "gap_resets": self._gap_count,
                "warmup_elapsed_s": elapsed_s,
                "warmup_time_admitted": int(elapsed_s >= self.warmup_s),
                "feature_ready_ts_ns": self._feature_ready_ts_ns,
                "last_error": self._last_error,
            }


def _definition_value(definition: Mapping[str, Any], feature_row: Mapping[str, Any]) -> int:
    raw = feature_row.get(str(definition.get("source_field", "")))
    missing = raw is None or (isinstance(raw, float) and not math.isfinite(raw))
    kind = str(definition.get("kind", ""))
    if kind == "preserved_tri":
        if missing:
            return -1
        numeric = float(raw)
        if numeric not in (-1.0, 0.0, 1.0):
            raise ValueError("selected_tri_state_source_invalid")
        return int(numeric)
    if kind == "categorical_equals":
        if missing:
            return -1
        return int(str(raw).strip().lower() == str(definition.get("category")).lower())
    if missing:
        return -1
    threshold = definition.get("threshold")
    if threshold is None:
        raise ValueError("selected_numeric_threshold_missing")
    numeric = float(raw)
    if not math.isfinite(numeric):
        return -1
    return int(numeric >= float(threshold))


class LiveBuyE3CooldownPolicy:
    """Exact BUY-only E3 artifact; evaluation occurs only on executed fills."""

    def __init__(
        self,
        *,
        evaluator: _CompiledBuyE3Evaluator,
        definitions: Mapping[str, Mapping[str, Any]],
        direct_predicates: frozenset[str],
        artifact_manifest_path: Path,
        artifact_manifest_sha256: str,
        policy_path: Path,
        policy_file_sha256: str,
        predicate_bundle_path: Path,
        predicate_bundle_file_sha256: str,
        warmup_s: float,
        max_feature_age_s: float,
        _bound_file_identities: Mapping[Path, _FileIdentity] | None = None,
        native_runtime: bool | None = None,
    ) -> None:
        self.evaluator = evaluator
        self.definitions = dict(definitions)
        self.direct_predicates = direct_predicates
        expected_files = {
            artifact_manifest_path: artifact_manifest_sha256,
            policy_path: policy_file_sha256,
            predicate_bundle_path: predicate_bundle_file_sha256,
        }
        if _bound_file_identities is None:
            opened_files: list[_OpenedBoundJson] = []
            try:
                for index, (path, expected) in enumerate(expected_files.items()):
                    opened_files.append(
                        _open_bound_json(path, expected, f"buy_e3_bound_file_{index}")
                    )
                for opened in opened_files:
                    _revalidate_opened_file(opened)
                identities = {opened.path: opened.identity for opened in opened_files}
            finally:
                _close_opened_files(opened_files)
        else:
            identities = dict(_bound_file_identities)
        if set(identities) != set(expected_files):
            raise ValueError("buy_e3_bound_file_identity_set_mismatch")
        self._bound_files = {
            path: str(expected).lower() for path, expected in expected_files.items()
        }
        self._bound_file_identities = identities
        self.windows = ReceiveTimeFullMidEmaWindows(
            warmup_s=warmup_s,
            max_feature_age_s=max_feature_age_s,
        )
        self._native_cpp, self._native_hot_path = build_hot_path(
            self,
            profile="BUY",
            warmup_s=warmup_s,
            max_feature_age_s=max_feature_age_s,
            requested=native_runtime,
        )
        self._lock = threading.Lock()
        self._evaluations = 0
        self._supported = 0
        self._nonbaseline = 0
        self._fallback = 0
        self._last_action = CONTROL_ACTION
        self._last_fallback = ""
        self._last_decision_wall_s = 0.0
        self._binding_error = ""
        self._binding_mode = "startup_immutable"
        self._decision_latency_us: deque[float] = deque(maxlen=2_048)

    @classmethod
    def from_files(
        cls,
        *,
        artifact_manifest_path: str | Path,
        artifact_manifest_sha256: str,
        expected_artifact_sha256: str,
        policy_path: str | Path,
        policy_sha256: str,
        predicate_bundle_path: str | Path,
        predicate_bundle_sha256: str,
        warmup_s: float,
        max_feature_age_s: float,
    ) -> LiveBuyE3CooldownPolicy:
        manifest_path = _absolute_without_resolving(artifact_manifest_path)
        policy_file = _absolute_without_resolving(policy_path)
        bundle_file = _absolute_without_resolving(predicate_bundle_path)
        opened_files: list[_OpenedBoundJson] = []
        try:
            for path, expected, label in (
                (manifest_path, artifact_manifest_sha256, "buy_e3_manifest"),
                (policy_file, policy_sha256, "buy_e3_policy"),
                (bundle_file, predicate_bundle_sha256, "buy_e3_bundle"),
            ):
                opened_files.append(_open_bound_json(path, expected, label))
            return cls._from_opened_files(
                opened_files=opened_files,
                manifest_path=manifest_path,
                artifact_manifest_sha256=artifact_manifest_sha256,
                expected_artifact_sha256=expected_artifact_sha256,
                policy_file=policy_file,
                policy_sha256=policy_sha256,
                bundle_file=bundle_file,
                predicate_bundle_sha256=predicate_bundle_sha256,
                warmup_s=warmup_s,
                max_feature_age_s=max_feature_age_s,
            )
        finally:
            _close_opened_files(opened_files)

    @classmethod
    def _from_opened_files(
        cls,
        *,
        opened_files: Sequence[_OpenedBoundJson],
        manifest_path: Path,
        artifact_manifest_sha256: str,
        expected_artifact_sha256: str,
        policy_file: Path,
        policy_sha256: str,
        bundle_file: Path,
        predicate_bundle_sha256: str,
        warmup_s: float,
        max_feature_age_s: float,
    ) -> LiveBuyE3CooldownPolicy:
        if len(opened_files) != 3:
            raise ValueError("buy_e3_bound_file_count_invalid")
        manifest, policy, bundle = (opened.payload for opened in opened_files[:3])
        _validate_canonical(policy, "canonical_sha256", "buy_e3_policy")
        _validate_canonical(bundle, "canonical_sha256", "buy_e3_bundle")
        manifest_body = dict(manifest)
        observed_artifact_sha = str(manifest_body.pop("artifact_sha256", ""))
        if (
            _SHA256_RE.fullmatch(str(expected_artifact_sha256)) is None
            or observed_artifact_sha != str(expected_artifact_sha256).lower()
            or _canonical_sha256(manifest_body) != observed_artifact_sha
        ):
            raise ValueError("buy_e3_artifact_sha256_mismatch")
        if (
            manifest.get("schema_version") != OWNER_MANIFEST_SCHEMA
            or manifest.get("identity") != OWNER_IDENTITY
            or manifest.get("status") != "exact_buy_e3_artifact_frozen"
            or manifest.get("policy_file_sha256") != str(policy_sha256).lower()
            or manifest.get("predicate_bundle_file_sha256") != str(predicate_bundle_sha256).lower()
            or manifest.get("duration_vocabulary") != list(BUY_ACTIONS)
            or manifest.get("default_action") != CONTROL_ACTION
            or manifest.get("research_supported") is not False
            or manifest.get("owner_risk_accepted") is not True
        ):
            raise ValueError("buy_e3_artifact_manifest_identity_drifted")
        if (
            policy.get("schema_version") != OWNER_POLICY_SCHEMA
            or policy.get("identity") != OWNER_IDENTITY
            or policy.get("side") != "BUY"
            or policy.get("selected_candidate") != SELECTED_CANDIDATE
            or policy.get("selected_profile") != SELECTED_PROFILE
            or policy.get("evidence_boundary", {}).get("research_supported") is not False
            or policy.get("evidence_boundary", {}).get("owner_risk_accepted") is not True
        ):
            raise ValueError("buy_e3_policy_identity_drifted")
        if (
            bundle.get("schema_version") != OWNER_BUNDLE_SCHEMA
            or bundle.get("identity") != OWNER_IDENTITY
            or bundle.get("side") != "BUY"
            or bundle.get("selected_candidate") != SELECTED_CANDIDATE
            or bundle.get("selected_profile") != SELECTED_PROFILE
            or bundle.get("ema_half_lives_s") != list(EMA_HALF_LIVES_S)
            or bundle.get("ema_pairs_s") != [list(pair) for pair in EMA_PAIRS_S]
            or bundle.get("ema_pair_count") != 45
            or bundle.get("uses_trade_predicates") is not False
            or bundle.get("uses_depth_predicates") is not False
            or bundle.get("uses_m2_incremental_features") is not False
            or bundle.get("normalization_source", {}).get("reference_days_are_2025") is not True
        ):
            raise ValueError("buy_e3_predicate_bundle_identity_drifted")
        raw_policy = policy.get("policy")
        if not isinstance(raw_policy, Mapping) or raw_policy.get("side") != "BUY":
            raise ValueError("buy_e3_boolean_policy_missing")
        if raw_policy.get("default_action") != CONTROL_ACTION:
            raise ValueError("buy_e3_default_action_drifted")
        raw_rules = raw_policy.get("ordered_first_match_rules")
        if not isinstance(raw_rules, list) or not raw_rules:
            raise ValueError("buy_e3_rules_missing")
        parsed_rules: list[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]]] = []
        used: set[str] = set()
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, Mapping):
                raise ValueError("buy_e3_rule_invalid")
            action = str(raw_rule.get("action", ""))
            if action not in BUY_FIXED_ACTIONS:
                raise ValueError("buy_e3_action_outside_allowlist")
            raw_clauses = raw_rule.get("clauses")
            if not isinstance(raw_clauses, list) or not raw_clauses:
                raise ValueError("buy_e3_clauses_missing")
            clauses: list[tuple[tuple[str, bool], ...]] = []
            for raw_clause in raw_clauses:
                literals = raw_clause.get("literals") if isinstance(raw_clause, Mapping) else None
                if not isinstance(literals, list) or not literals:
                    raise ValueError("buy_e3_literals_missing")
                clause: list[tuple[str, bool]] = []
                for literal in literals:
                    if not isinstance(literal, Mapping):
                        raise ValueError("buy_e3_literal_invalid")
                    name = str(literal.get("predicate", ""))
                    if not name:
                        raise ValueError("buy_e3_literal_name_missing")
                    used.add(name)
                    clause.append((name, bool(literal.get("negated", False))))
                clauses.append(tuple(clause))
            parsed_rules.append((action, tuple(clauses)))
        columns = tuple(sorted(used))
        if list(columns) != bundle.get("predicate_columns"):
            raise ValueError("buy_e3_policy_bundle_columns_drifted")
        definitions: dict[str, Mapping[str, Any]] = {}
        for definition in bundle.get("definitions", []):
            if not isinstance(definition, Mapping):
                raise ValueError("buy_e3_definition_invalid")
            name = str(definition.get("name", ""))
            source = str(definition.get("source_field", ""))
            if (
                not name
                or name in definitions
                or definition.get("clock_group") != "book"
                or "mid_usdc_per_btc" not in source
            ):
                raise ValueError("buy_e3_forbidden_predicate_definition")
            definitions[name] = dict(definition)
        direct = frozenset(
            str(row.get("name", ""))
            for row in bundle.get("direct_predicates", [])
            if isinstance(row, Mapping)
        )
        if direct - {DIRECT_CAMPAIGN_AGE} or set(columns) != set(definitions) | set(direct):
            raise ValueError("buy_e3_selected_predicate_binding_incomplete")
        evaluator = _CompiledBuyE3Evaluator(
            rules=tuple(parsed_rules),
            predicate_columns=columns,
            policy_sha256=str(policy_sha256).lower(),
            predicate_bundle_sha256=str(predicate_bundle_sha256).lower(),
            artifact_sha256=observed_artifact_sha,
        )
        for opened in opened_files:
            _revalidate_opened_file(opened)
        return cls(
            evaluator=evaluator,
            definitions=definitions,
            direct_predicates=direct,
            artifact_manifest_path=manifest_path,
            artifact_manifest_sha256=str(artifact_manifest_sha256).lower(),
            policy_path=policy_file,
            policy_file_sha256=str(policy_sha256).lower(),
            predicate_bundle_path=bundle_file,
            predicate_bundle_file_sha256=str(predicate_bundle_sha256).lower(),
            warmup_s=warmup_s,
            max_feature_age_s=max_feature_age_s,
            _bound_file_identities={opened.path: opened.identity for opened in opened_files},
        )

    @property
    def artifact_sha256(self) -> str:
        return self.evaluator.artifact_sha256

    @property
    def ema_half_lives_s(self) -> tuple[float, ...]:
        """Return the frozen EMA bank consumed by execution backends."""

        return EMA_HALF_LIVES_S

    @property
    def ema_pairs_s(self) -> tuple[tuple[float, float], ...]:
        """Return the frozen pair ordering used by predicate definitions."""

        return EMA_PAIRS_S

    @property
    def deadline_identity(self) -> str:
        return f"BUY_E3:{self.artifact_sha256}"

    def observe_depth(self, **kwargs: Any) -> None:
        if self._native_hot_path is None:
            self.windows.observe_depth(**kwargs)
            return
        try:
            bid = float(kwargs["bids"][0][0])
            ask = float(kwargs["asks"][0][0])
        except (KeyError, IndexError, TypeError, ValueError):
            bid = math.nan
            ask = math.nan
        self._native_hot_path.observe_depth(
            int(kwargs.get("receive_ts_ns", 0)),
            bid,
            ask,
        )

    def evaluate(
        self,
        *,
        side: str,
        baseline_duration_ms: int,
        campaign_age_s: float,
        decision_ts_ns: int,
        snapshot_id: str,
    ) -> BuyE3CooldownDecision:
        decision_started_ns = time.perf_counter_ns()
        del snapshot_id
        baseline = int(baseline_duration_ms)
        if baseline <= 0:
            raise ValueError("baseline_duration_ms_must_be_positive")
        if self._native_hot_path is not None and str(side).upper() == "BUY":
            native = self._native_hot_path.evaluate(
                int(decision_ts_ns),
                float(campaign_age_s),
                baseline,
            )
            reason = native_fallback_reason(self._native_cpp, native)
            matched_rule = (
                None if int(native.matched_rule_index) < 0 else int(native.matched_rule_index)
            )
            action = (
                f"FIXED_{int(native.duration_ms) // 1_000}S"
                if matched_rule is not None
                else CONTROL_ACTION
            )
            support_valid = bool(native.support_valid)
            duration = int(native.duration_ms)
            feature_ready = int(native.feature_ready_ts_ns)
            feature_age_ms = float(native.feature_age_ms)
            with self._lock:
                elapsed_us = (time.perf_counter_ns() - decision_started_ns) / 1_000.0
                self._evaluations += 1
                self._supported += int(support_valid)
                self._nonbaseline += int(action != CONTROL_ACTION)
                self._fallback += int(reason is not None)
                self._last_action = action
                self._last_fallback = reason or ""
                self._last_decision_wall_s = time.time()
                self._decision_latency_us.append(elapsed_us)
            return BuyE3CooldownDecision(
                action_id=action,
                duration_ms=duration,
                fallback_reason=reason,
                matched_rule_index=matched_rule,
                support_valid=support_valid,
                policy_sha256=self.evaluator.policy_sha256,
                predicate_bundle_sha256=self.evaluator.predicate_bundle_sha256,
                artifact_sha256=self.evaluator.artifact_sha256,
                feature_ready_ts_ns=feature_ready,
                feature_age_ms=feature_age_ms,
            )
        reason: str | None = None
        support_valid = False
        matched_rule: int | None = None
        action = CONTROL_ACTION
        duration = baseline
        feature_ready = 0
        feature_age_ms = math.inf
        if str(side).upper() != "BUY":
            reason = "non_buy_control_by_contract"
        else:
            feature_row, reason, feature_ready, feature_age_ms = self.windows.feature_row(
                decision_ts_ns=int(decision_ts_ns)
            )
            if feature_row is not None:
                values = {
                    name: _definition_value(definition, feature_row)
                    for name, definition in self.definitions.items()
                }
                if DIRECT_CAMPAIGN_AGE in self.direct_predicates:
                    age = float(campaign_age_s)
                    values[DIRECT_CAMPAIGN_AGE] = (
                        -1 if not math.isfinite(age) or age < 0.0 else int(age * 1_000.0 > baseline)
                    )
                if any(value == -1 for value in values.values()):
                    reason = "selected_predicate_state_unobserved"
                else:
                    action, duration, matched_rule, reason, support_valid = self.evaluator.evaluate(
                        predicate_values=values,
                        baseline_duration_ms=baseline,
                    )
        with self._lock:
            elapsed_us = (time.perf_counter_ns() - decision_started_ns) / 1_000.0
            self._evaluations += 1
            self._supported += int(support_valid)
            self._nonbaseline += int(action != CONTROL_ACTION)
            self._fallback += int(reason is not None)
            self._last_action = action
            self._last_fallback = reason or ""
            self._last_decision_wall_s = time.time()
            self._decision_latency_us.append(elapsed_us)
        return BuyE3CooldownDecision(
            action_id=action,
            duration_ms=duration,
            fallback_reason=reason,
            matched_rule_index=matched_rule,
            support_valid=support_valid,
            policy_sha256=self.evaluator.policy_sha256,
            predicate_bundle_sha256=self.evaluator.predicate_bundle_sha256,
            artifact_sha256=self.evaluator.artifact_sha256,
            feature_ready_ts_ns=feature_ready,
            feature_age_ms=feature_age_ms,
        )

    def audit(self) -> dict[str, Any]:
        with self._lock:
            ordered_latency = sorted(self._decision_latency_us)
            p99_index = max(0, math.ceil(len(ordered_latency) * 0.99) - 1) if ordered_latency else 0
            decision_age_s = (
                math.inf
                if self._last_decision_wall_s <= 0.0
                else max(0.0, time.time() - self._last_decision_wall_s)
            )
            policy = {
                "enabled": 1,
                "transport_identity": LIVE_FEATURE_TRANSPORT_IDENTITY,
                "evaluations": self._evaluations,
                "supported": self._supported,
                "nonbaseline": self._nonbaseline,
                "fallback": self._fallback,
                "last_action": self._last_action,
                "last_fallback": self._last_fallback,
                "last_decision_age_s": decision_age_s,
                "decision_latency_samples": len(ordered_latency),
                "decision_latency_p99_us": (ordered_latency[p99_index] if ordered_latency else 0.0),
                "artifact_sha256": self.evaluator.artifact_sha256,
                "policy_sha256": self.evaluator.policy_sha256,
                "predicate_bundle_sha256": self.evaluator.predicate_bundle_sha256,
                "binding_mode": self._binding_mode,
                "binding_error": self._binding_error,
            }
        if self._native_hot_path is None:
            windows = self.windows.audit()
        else:
            native = self._native_hot_path.audit()
            warmup_elapsed_s = (
                0.0
                if int(native.warmup_start_right_ts_ns) <= 0
                else (
                    int(native.last_window_right_ts_ns)
                    - int(native.warmup_start_right_ts_ns)
                )
                / 1_000_000_000.0
            )
            windows = {
                "updates": int(native.updates),
                "completed_windows": int(native.completed_windows),
                "gap_windows": int(native.gap_windows),
                "resets": int(native.resets),
                "invalid_updates": int(native.invalid_updates),
                "out_of_order_updates": int(native.out_of_order_updates),
                "gap_resets": int(native.gap_resets),
                "warmup_elapsed_s": warmup_elapsed_s,
                "warmup_time_admitted": int(native.warmup_admitted),
                "feature_ready_ts_ns": int(native.feature_ready_ts_ns),
                "last_error": "",
            }
        return {**policy, "windows": windows}


__all__ = [
    "BUY_ACTIONS",
    "BUY_FIXED_ACTIONS",
    "BuyE3CooldownDecision",
    "CONTROL_ACTION",
    "EMA_HALF_LIVES_S",
    "EMA_PAIRS_S",
    "LIVE_FEATURE_TRANSPORT_IDENTITY",
    "LiveBuyE3CooldownPolicy",
    "OWNER_IDENTITY",
    "ReceiveTimeFullMidEmaWindows",
]
