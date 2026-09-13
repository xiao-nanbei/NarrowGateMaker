"""Fixed F03 calendar, independent two-UTC-day accounts and B-only selection.

This module performs no I/O. Callers bind the actual result/bundle SHA256s,
persist the returned frozen choice once, then open A/C for diagnostics. Calendar
validation reads metadata for all 300 days; selection reads amounts only in B.
Each shard starts flat with the same capital and no orders. Only market/feature
history may be warmed; account checkpoints never cross shard boundaries. Daily
equity remains continuous *inside* a shard. This module validates supplied
metadata/accounting, not an upstream simulation's execution or use authority.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from strategy.model_contract import REQUIRED_MODEL_HEADS

DAY_MS = 86_400_000
CALENDAR = tuple((date(2025, 8, 1) + timedelta(days=i)).isoformat() for i in range(401))
TRAIN_DAYS = CALENDAR[:300:3]
_UNSELECTED = tuple(day for day in CALENDAR[:300] if day not in TRAIN_DAYS)
SPLITS = {"T": TRAIN_DAYS, "A": _UNSELECTED[:50], "B": _UNSELECTED[50:150],
          "C": _UNSELECTED[150:], "F": CALENDAR[300:]}
HALF_LIVES = ("inf", "240", "120", "60")
_STATE_FIELDS = ("cash_usdc", "inventory_btc", "equity_usdc")


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    phase: str
    calendar_days: tuple[str, ...]
    start_ts_ms: int
    end_ts_ms_exclusive: int
    warmup_start_ts_ms: int
    initial_capital_usdc: float

    def to_metadata(self) -> dict[str, Any]:
        return {"schema_version": "f03_independent_utc_shard.v1", **asdict(self),
                "calendar_days": list(self.calendar_days),
                "initial_inventory_btc": 0, "initial_open_order_count": 0,
                "accounting_cash_basis": "pnl_relative_to_initial_capital",
                "history_scope": "market_and_features_only",
                "checkpoint_scope": "same_shard_only",
                "terminal_accounting": "inventory_mtm_no_liquidation"}


@dataclass(frozen=True)
class ShardResult:
    """Actual initialization and terminal receipts, supplied by the existing runner."""

    spec: ShardSpec
    initialization: Mapping[str, Any]
    terminal: Mapping[str, Any]


@dataclass(frozen=True)
class UtcArmResult:
    rows: Sequence[Mapping[str, Any]]
    result_sha256: str
    model_bundle_sha256: str
    head_names: tuple[str, ...]
    shards: tuple[ShardResult, ...] = ()


@dataclass(frozen=True)
class BScore:
    half_life: str
    net_pnl_usdc: float
    delta_vs_uniform_usdc: float


@dataclass(frozen=True)
class BSelection:
    """One whole-bundle choice; never select individual heads on B."""

    selected_half_life: int
    common_contract_sha256: str
    input_identities: tuple[tuple[str, str, str], ...]
    scores: tuple[BScore, ...]
    positive_b_point_increment: bool
    shard_specs: tuple[ShardSpec, ...]

    def to_metadata(self) -> dict[str, Any]:
        return {"schema_version": "f03_time_weighted_b_selection.v2", **asdict(self),
                "shard_specs": [spec.to_metadata() for spec in self.shard_specs],
                "selection_split": "B", "selection_days": list(SPLITS["B"]),
                "ranking": "unweighted_B_net_delta_vs_uniform; exact_tie_longer_half_life",
                "a_c_economics_read": False, "full_path_identity_check": "DEFERRED_UNTIL_AFTER_CHOICE",
                "weighted_hypothesis_status": ("positive_B_point_increment_only" if self.positive_b_point_increment
                                               else "not_supported_in_B"),
                "interpretation": "development_selection_not_independent_evidence",
                "live": False}


def _phase_days(phase: str) -> tuple[str, ...]:
    if phase not in {"development", "final"}:
        raise ValueError("phase must be development or final")
    return CALENDAR[:300] if phase == "development" else CALENDAR[300:]


def build_shard_specs(*, initial_capital_usdc: float, phase: str = "development",
                      warmup_days: int = 1) -> tuple[ShardSpec, ...]:
    """Pair consecutive phase dates, never sparse label-selected dates.

    F01 UTC cash/equity are PnL-relative (zero at a fresh flat start). Adding
    the same fixed positive capital offset to each shard's opening and closing
    equity leaves net PnL unchanged. This capital is a reporting reference,
    not an order-funding constraint or a simulated exchange margin account;
    the existing execution/risk rules are unchanged.
    """
    days = _phase_days(phase)
    capital = _number({"capital": initial_capital_usdc}, "capital")
    if capital <= 0 or isinstance(warmup_days, bool) or not isinstance(warmup_days, int) or warmup_days < 0:
        raise ValueError("positive initial capital and nonnegative integral warmup days are required")
    specs = []
    for offset in range(0, len(days), 2):
        pair = days[offset:offset + 2]
        start = int(datetime.fromisoformat(pair[0]).replace(tzinfo=UTC).timestamp()) * 1000
        specs.append(ShardSpec(f"{phase}-{offset // 2:03d}-{pair[0]}", phase, pair,
                               start, start + len(pair) * DAY_MS,
                               start - warmup_days * DAY_MS, capital))
    return tuple(specs)


def validate_shard_specs(specs: Sequence[ShardSpec], *, phase: str = "development") -> None:
    if not specs:
        raise ValueError("complete independent shard specs are required")
    first = specs[0]
    warmup_ms = _timestamp(first.start_ts_ms) - _timestamp(first.warmup_start_ts_ms)
    if warmup_ms < 0 or warmup_ms % DAY_MS:
        raise ValueError("warmup must contain only preceding complete UTC days")
    expected = build_shard_specs(initial_capital_usdc=first.initial_capital_usdc,
                                 phase=phase, warmup_days=warmup_ms // DAY_MS)
    if tuple(specs) != expected:
        raise ValueError("shards must cover the fixed phase calendar in consecutive two-day accounts")


def validate_shard_resume(spec: ShardSpec, checkpoint: Mapping[str, Any] | None, *,
                          arm: str, model_bundle_sha256: str, common_contract_sha256: str) -> None:
    """Permit crash continuation only inside this exact shard/arm/frozen contract.

    Call before loading any checkpoint. A completed prior shard is never a
    permissible initializer, even if its cut is the next shard's start.
    """
    _sha(model_bundle_sha256)
    _sha(common_contract_sha256)
    warmup_ms = _timestamp(spec.start_ts_ms) - _timestamp(spec.warmup_start_ts_ms)
    if warmup_ms < 0 or warmup_ms % DAY_MS:
        raise ValueError("checkpoint target has an invalid shard warmup")
    planned = build_shard_specs(initial_capital_usdc=spec.initial_capital_usdc,
                                phase=spec.phase, warmup_days=warmup_ms // DAY_MS)
    if spec not in planned:
        raise ValueError("checkpoint target must be an exact planned shard")
    if checkpoint is None:
        return
    if (checkpoint.get("schema_version") != "f03_independent_shard_checkpoint.v1"
            or checkpoint.get("shard_spec") != spec.to_metadata()
            or checkpoint.get("arm") != arm
            or checkpoint.get("model_bundle_sha256") != model_bundle_sha256
            or checkpoint.get("common_contract_sha256") != common_contract_sha256):
        raise ValueError("checkpoint may resume only the same shard, arm and frozen contract")
    cut = _timestamp(checkpoint["cut_ts_ms"])
    if not spec.start_ts_ms < cut < spec.end_ts_ms_exclusive:
        raise ValueError("checkpoint cut must be strictly inside its shard")


def _sha(value: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("result, bundle and common contract identities must be SHA256")
    return value


def _flag(value: Any, expected: bool) -> bool:
    return value is expected or (isinstance(value, str) and value.lower() == str(expected).lower())


def _timestamp(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("UTC timestamp must be integral milliseconds")
    parsed = int(value)
    if not isinstance(value, str) and parsed != value:
        raise ValueError("UTC timestamp must be integral milliseconds")
    return parsed


def validate_calendar_metadata(rows: Sequence[Mapping[str, Any]], *, phase: str = "development") -> None:
    """Reject missing, duplicate, reordered, unexpected or partial dates without amounts."""
    expected = _phase_days(phase)
    if tuple(row["calendar_date"] for row in rows) != expected:
        raise ValueError("calendar must contain every expected UTC date exactly once in order")
    arm = rows[0]["arm"]
    for row, day in zip(rows, expected, strict=True):
        start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) * 1000
        if (row["arm"] != arm or row["schema_version"] != "f01_utc_equity_slices.v1"
                or row["accounting_window"] != "utc_slice_of_continuous_state"):
            raise ValueError("mixed arm or incompatible continuous UTC accounting schema")
        if (not _flag(row["complete_utc_day"], True)
                or _timestamp(row["start_ts_ms"]) != start
                or _timestamp(row["end_ts_ms_exclusive"]) != start + DAY_MS):
            raise ValueError("partial or inconsistent UTC day bounds")
        if (row["funding_mode"] != "frozen_settlement_tape"
                or not _flag(row["terminal_liquidation_applied"], False)):
            raise ValueError("complete funding and mark-only terminal accounting are required")


def _number(row: Mapping[str, Any], field: str) -> float:
    value = row[field]
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite numeric accounting")
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite numeric accounting") from exc
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite numeric accounting")
    return value


def _equal(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-7)


def _row_pnl(row: Mapping[str, Any]) -> float:
    for side, boundary in (("start", "start_ts_ms"), ("end", "end_ts_ms_exclusive")):
        cash, inventory, equity = (_number(row, f"{side}_{field}") for field in _STATE_FIELDS)
        price, clock = row[f"{side}_mark_price"], row[f"{side}_mark_clock_ts_ms"]
        missing_price = price is None or price == "" or (isinstance(price, float) and math.isnan(price))
        if missing_price:
            if inventory != 0.0:
                raise ValueError("nonflat boundary lacks a valuation price")
            mark_value = 0.0
        else:
            price = _number(row, f"{side}_mark_price")
            clock = _number(row, f"{side}_mark_clock_ts_ms")
            if price <= 0 or clock > int(row[boundary]) or (side == "end" and clock == int(row[boundary])):
                raise ValueError("invalid or future valuation mark")
            mark_value = inventory * price
        if not _equal(equity, cash + mark_value):
            raise ValueError("equity must equal cash plus terminal inventory MTM")
    pnl = _number(row, "net_equity_change_usdc")
    if not _equal(pnl, _number(row, "end_equity_usdc") - _number(row, "start_equity_usdc")):
        raise ValueError("net PnL must equal the actual equity change")
    # These signed explanatory flows already entered cash/equity. Do not debit again.
    _number(row, "fees_usdc")
    _number(row, "funding_cashflow_usdc")
    return pnl


def _identities(results: Mapping[str, UtcArmResult]) -> tuple[tuple[str, str, str], ...]:
    if set(results) != set(HALF_LIVES):
        raise ValueError("expected exactly the four frozen whole-bundle half lives")
    identities = []
    for half_life in HALF_LIVES:
        result = results[half_life]
        if tuple(result.head_names) != tuple(REQUIRED_MODEL_HEADS):
            raise ValueError("each candidate must identify the complete ordered 13-head bundle")
        identities.append((half_life, _sha(result.result_sha256), _sha(result.model_bundle_sha256)))
    return tuple(identities)


def validate_shard_metadata(result: UtcArmResult, *, phase: str = "development") -> tuple[ShardSpec, ...]:
    """Validate coverage/initialization declarations, never read outcome amounts."""
    validate_calendar_metadata(result.rows, phase=phase)
    specs = tuple(shard.spec for shard in result.shards)
    validate_shard_specs(specs, phase=phase)
    offset = 0
    for shard in result.shards:
        spec, initialization, terminal = shard.spec, shard.initialization, shard.terminal
        rows = result.rows[offset:offset + len(spec.calendar_days)]
        offset += len(rows)
        if any(row.get("shard_id") != spec.shard_id for row in rows):
            raise ValueError("each UTC row must belong to its exact planned shard")
        if (initialization.get("account_state_source") != "fresh_flat_account"
                or "account_checkpoint_parent" not in initialization
                or initialization["account_checkpoint_parent"] is not None
                or initialization.get("history_scope") != "market_and_features_only"
                or _timestamp(initialization["trading_start_ts_ms"]) != spec.start_ts_ms):
            raise ValueError("each shard requires a fresh account; warmup cannot carry trading state")
        if (_timestamp(terminal["end_ts_ms_exclusive"]) != spec.end_ts_ms_exclusive
                or not _flag(terminal.get("accounting_complete"), True)
                or not _flag(terminal.get("terminal_liquidation_applied"), False)):
            raise ValueError("complete mark-only terminal accounting is required for every shard")
    return specs


def validate_paired_shards(results: Mapping[str, UtcArmResult], *,
                           phase: str = "development") -> tuple[ShardSpec, ...]:
    """Shared metadata-only check for Dev controls/candidates or frozen Final arms."""
    if not results:
        raise ValueError("paired arms are required")
    specs = validate_shard_metadata(next(iter(results.values())), phase=phase)
    arm_names = set()
    for result in results.values():
        if validate_shard_metadata(result, phase=phase) != specs:
            raise ValueError("all arms must use the same shards, capital and warmup")
        arm = result.rows[0]["arm"]
        if arm in arm_names:
            raise ValueError("paired results must identify distinct arms")
        arm_names.add(arm)
    return specs


def select_b_half_life(results: Mapping[str, UtcArmResult], *, common_contract_sha256: str) -> BSelection:
    """Select one finite H, even when uniform beats every finite candidate.

    No sorting, serialization, hashing or numeric conversion of A/C/T economic
    fields occurs here. Hash the input files as opaque bytes at the I/O boundary.
    Full-path amount/continuity validation is deliberately a separate later phase.
    """
    identities = _identities(results)
    _sha(common_contract_sha256)
    specs = validate_paired_shards(results)
    scores = _b_scores(results)
    winner = max((score for score in scores if score.half_life != "inf"),
                 key=lambda score: (score.delta_vs_uniform_usdc, int(score.half_life)))
    return BSelection(int(winner.half_life), common_contract_sha256, identities, scores,
                      winner.delta_vs_uniform_usdc > 0, specs)


def _b_scores(results: Mapping[str, UtcArmResult]) -> tuple[BScore, ...]:
    b_days = frozenset(SPLITS["B"])
    values = {half_life: math.fsum(_row_pnl(row) for row in result.rows if row["calendar_date"] in b_days)
              for half_life, result in results.items()}
    return tuple(BScore(h, values[h], values[h] - values["inf"]) for h in HALF_LIVES)


def validate_continuous_account(rows: Sequence[Mapping[str, Any]], *, phase: str = "development") -> dict[str, Any]:
    """Legacy full-path validator, not the independent-shard F03 reporting path."""
    validate_calendar_metadata(rows, phase=phase)
    return _continuous_account_totals(rows)


def _continuous_account_totals(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    changes = [_row_pnl(row) for row in rows]
    for previous, current in zip(rows, rows[1:], strict=False):
        if any(not _equal(_number(previous, f"end_{field}"), _number(current, f"start_{field}"))
               for field in _STATE_FIELDS):
            raise ValueError("continuous account resets or changes state at a UTC boundary")
    net = math.fsum(changes)
    if not _equal(net, _number(rows[-1], "end_equity_usdc") - _number(rows[0], "start_equity_usdc")):
        raise ValueError("complete calendar net PnL does not telescope to terminal equity")
    peak = _number(rows[0], "start_equity_usdc")
    drawdown = 0.0
    for row in rows:
        equity = _number(row, "end_equity_usdc")
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {"days": len(rows), "net_pnl_usdc": net,
            "fees_usdc": math.fsum(_number(row, "fees_usdc") for row in rows),
            "funding_cashflow_usdc": math.fsum(_number(row, "funding_cashflow_usdc") for row in rows),
            "utc_close_max_drawdown_usdc": drawdown,
            "drawdown_scope": "daily_boundaries_only_not_intraday_path",
            "end_inventory_btc": _number(rows[-1], "end_inventory_btc"),
            "start_equity_usdc": _number(rows[0], "start_equity_usdc"),
            "end_equity_usdc": _number(rows[-1], "end_equity_usdc")}


def validate_independent_shards(result: UtcArmResult, *, phase: str = "development") -> dict[str, Any]:
    """Post-choice amount checks: reset between shards, continuity only within.

    Receipts reconcile every terminal residual inventory/MTM and signed fee and
    funding total to daily rows. They cannot independently prove the fill tape.
    """
    specs = validate_shard_metadata(result, phase=phase)
    reports = []
    offset = 0
    for shard in result.shards:
        rows = result.rows[offset:offset + len(shard.spec.calendar_days)]
        offset += len(rows)
        first, initialization, terminal = rows[0], shard.initialization, shard.terminal
        if (not _equal(_number(initialization, "initial_capital_usdc"), shard.spec.initial_capital_usdc)
                or _number(initialization, "initial_open_order_count") != 0
                or any(_number(first, f"start_{field}") != 0 for field in _STATE_FIELDS)):
            raise ValueError("each shard must start with the same capital, flat, no orders and zero PnL cash")
        full = _continuous_account_totals(rows)
        expected = {"cash_usdc": _number(rows[-1], "end_cash_usdc"),
                    "inventory_btc": full["end_inventory_btc"], "equity_usdc": full["end_equity_usdc"],
                    "net_pnl_usdc": full["net_pnl_usdc"], "fees_usdc": full["fees_usdc"],
                    "funding_cashflow_usdc": full["funding_cashflow_usdc"]}
        if any(not _equal(_number(terminal, key), value) for key, value in expected.items()):
            raise ValueError("shard terminal accounting must reconcile cash, inventory MTM, fees and funding")
        reports.append({"shard_id": shard.spec.shard_id, **full})
    return {"days": len(result.rows), "shard_count": len(reports),
            "initial_capital_usdc_per_shard": specs[0].initial_capital_usdc,
            "net_pnl_usdc": math.fsum(row["net_pnl_usdc"] for row in reports),
            "fees_usdc": math.fsum(row["fees_usdc"] for row in reports),
            "funding_cashflow_usdc": math.fsum(row["funding_cashflow_usdc"] for row in reports),
            "max_shard_utc_close_drawdown_usdc": max(row["utc_close_max_drawdown_usdc"] for row in reports),
            "drawdown_scope": "maximum_within_shard_daily_boundary_drawdown_not_continuous_or_intraday",
            "aggregation": "sum_independent_shard_net_changes_not_last_equity_minus_first",
            "shards": reports}


def report_development_after_selection(results: Mapping[str, UtcArmResult], selection: BSelection,
                                       *, common_contract_sha256: str,
                                       business_b0: UtcArmResult | None = None,
                                       expected_business_b0_bundle_sha256: str | None = None) -> dict[str, Any]:
    """Report A/C without reopening selection or producing a mixed-head winner."""
    if _identities(results) != selection.input_identities or common_contract_sha256 != selection.common_contract_sha256:
        raise ValueError("result/model/common contract changed after the B choice was frozen")
    if validate_paired_shards(results) != selection.shard_specs:
        raise ValueError("shard contract changed after the B choice was frozen")
    if _b_scores(results) != selection.scores:
        raise ValueError("B amounts differ from the frozen selection")
    report = {}
    arms = dict(results)
    baseline_identity = None
    if business_b0 is not None:
        _sha(business_b0.result_sha256)
        _sha(business_b0.model_bundle_sha256)
        if business_b0.model_bundle_sha256 != _sha(expected_business_b0_bundle_sha256):
            raise ValueError("business_b0 bundle differs from the predeclared fixed baseline")
        if validate_shard_metadata(business_b0) != selection.shard_specs:
            raise ValueError("business_b0 must use the same shards, capital and warmup as candidates")
        baseline_identity = {"result_sha256": business_b0.result_sha256,
                             "model_bundle_sha256": business_b0.model_bundle_sha256,
                             "head_names": list(business_b0.head_names)}
        arms["business_b0"] = business_b0
        validate_paired_shards(arms)
    elif expected_business_b0_bundle_sha256 is not None:
        raise ValueError("predeclared business_b0 requires its complete result")
    for half_life, result in arms.items():
        full = validate_independent_shards(result)
        split_net = {split: math.fsum(_row_pnl(row) for row in result.rows if row["calendar_date"] in days)
                     for split, days in SPLITS.items() if split != "F"}
        report[half_life] = {"full_development": full, "split_net_pnl_usdc": split_net}
    return {"selection": selection.to_metadata(), "arms": report,
            "split_attribution": "original_date_labels_within_complete_independent_two_day_shards",
            "business_b0_role": "fixed_control_not_a_half_life_candidate" if business_b0 is not None else "not_supplied",
            "business_b0_identity": baseline_identity,
            "final_evidence": "not_evaluated"}
