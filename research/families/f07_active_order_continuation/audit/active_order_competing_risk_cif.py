"""Pure-Python 100 ms competing-risk CIF mechanics for active orders.

The kernel deliberately owns only probability accounting. Lifecycle code remains
responsible for classifying exchange events and explicitly starting a new risk
spell after a partial fill.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, ClassVar

IDENTITY = "active_order_competing_risk_cif_100ms_v1"
STATE_SCHEMA_VERSION = "active_order_competing_risk_cif_state.v1"
GRID_INTERVAL_MS = 100
GRID_INTERVAL_S = GRID_INTERVAL_MS / 1_000.0

CAUSES = (
    "favorable_fill",
    "adverse_fill",
    "cancel_ack",
    "other_terminal",
)
RISK_PHASES = frozenset({"ACTIVE", "PARTIALLY_FILLED", "CANCEL_PENDING"})
TERMINAL_PHASE = "EXCHANGE_TERMINAL"

_CAUSE_SET = frozenset(CAUSES)
_NUMERIC_TOLERANCE = 32.0 * math.ulp(1.0)


class CIFMechanicsError(ValueError):
    """Base exception for fail-closed CIF mechanics validation."""


class GridEdgeSequenceError(CIFMechanicsError):
    """Raised when an edge is duplicate or precedes the expected edge."""


class MissedGridEdgeError(GridEdgeSequenceError):
    """Raised when the scheduler skips one or more 100 ms grid edges."""


class TerminalStateError(CIFMechanicsError):
    """Raised when an exchange-terminal state is used again."""


def _finite_float(value: object, *, name: str) -> float:
    if isinstance(value, bool):
        raise CIFMechanicsError(f"{name} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CIFMechanicsError(f"{name} must be a finite number") from exc
    if not math.isfinite(result):
        raise CIFMechanicsError(f"{name} must be finite")
    return result


def _edge(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CIFMechanicsError(f"{name} must be an integer grid edge")
    if value < 0:
        raise CIFMechanicsError(f"{name} must be non-negative")
    return value


def _spell_id(value: object) -> str:
    result = str(value).strip()
    if not result or result.lower() == "nan":
        raise CIFMechanicsError("spell_id must be non-empty")
    return result


def _phase(value: object, *, terminal: bool) -> str:
    result = str(value).strip().upper()
    allowed = frozenset({TERMINAL_PHASE}) if terminal else RISK_PHASES
    if result not in allowed:
        raise CIFMechanicsError(
            f"phase must be one of {sorted(allowed)}, got {result!r}"
        )
    return result


def _cause_tuple(values: Mapping[str, object], *, name: str) -> tuple[float, ...]:
    keys = frozenset(values)
    if keys != _CAUSE_SET:
        missing = sorted(_CAUSE_SET - keys)
        extra = sorted(keys - _CAUSE_SET)
        raise CIFMechanicsError(f"{name} cause mismatch: missing={missing} extra={extra}")
    return tuple(_finite_float(values[cause], name=f"{name}.{cause}") for cause in CAUSES)


def _cause_mapping(values: tuple[float, ...]) -> dict[str, float]:
    return dict(zip(CAUSES, values, strict=True))


def _exact_survival_complement(cif: tuple[float, ...]) -> float:
    total_cif = math.fsum(cif)
    if total_cif < 0.0 or total_cif > 1.0 + _NUMERIC_TOLERANCE:
        raise ArithmeticError(f"cumulative incidence leaves unit interval: {total_cif}")
    survival = max(0.0, 1.0 - total_cif)
    for _ in range(3):
        mass = math.fsum((survival, *cif))
        if mass == 1.0:
            return survival
        survival += 1.0 - mass
    raise ArithmeticError("unable to represent exact CIF probability mass")


def _validate_probability_state(
    *, survival: float, cumulative_incidence: tuple[float, ...]
) -> None:
    if len(cumulative_incidence) != len(CAUSES):
        raise CIFMechanicsError("cumulative_incidence has the wrong number of causes")
    if not math.isfinite(survival) or not 0.0 <= survival <= 1.0:
        raise CIFMechanicsError("survival must be finite and within [0, 1]")
    for cause, value in zip(CAUSES, cumulative_incidence, strict=True):
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise CIFMechanicsError(f"cif.{cause} must be finite and within [0, 1]")
    if math.fsum((survival, *cumulative_incidence)) != 1.0:
        raise CIFMechanicsError("survival and CIF must conserve probability mass exactly")


def jointly_normalized_hazards(
    rates_per_s: Mapping[str, object],
) -> tuple[dict[str, float], float]:
    """Convert continuous cause rates into one jointly normalized 100 ms step."""

    rates = _cause_tuple(rates_per_s, name="rates_per_s")
    for cause, value in zip(CAUSES, rates, strict=True):
        if value < 0.0:
            raise CIFMechanicsError(f"rates_per_s.{cause} must be non-negative")
    try:
        total_rate = math.fsum(rates)
    except OverflowError as exc:
        raise CIFMechanicsError("sum of rates_per_s must be finite") from exc
    if not math.isfinite(total_rate):
        raise CIFMechanicsError("sum of rates_per_s must be finite")
    if total_rate == 0.0:
        return _cause_mapping((0.0,) * len(CAUSES)), 1.0

    event_probability = -math.expm1(-GRID_INTERVAL_S * total_rate)
    hazards = [value / total_rate * event_probability for value in rates]
    positive_indices = [index for index, value in enumerate(rates) if value > 0.0]
    residual_index = positive_indices[-1]
    hazards[residual_index] = event_probability - math.fsum(
        value for index, value in enumerate(hazards) if index != residual_index
    )
    if hazards[residual_index] < 0.0 and abs(hazards[residual_index]) <= _NUMERIC_TOLERANCE:
        hazards[residual_index] = 0.0
    if any(not math.isfinite(value) or value < 0.0 for value in hazards):
        raise ArithmeticError("joint hazard normalization produced an invalid probability")

    hazard_total = math.fsum(hazards)
    no_event_probability = 1.0 - hazard_total
    if no_event_probability < 0.0 and abs(no_event_probability) <= _NUMERIC_TOLERANCE:
        no_event_probability = 0.0
    if not 0.0 <= no_event_probability <= 1.0:
        raise ArithmeticError("joint no-event probability leaves unit interval")
    if math.fsum((no_event_probability, *hazards)) != 1.0:
        raise ArithmeticError("joint hazards do not conserve probability mass exactly")
    return _cause_mapping(tuple(hazards)), no_event_probability


@dataclass(frozen=True)
class CIFIntervalResult:
    """One immutable 100 ms probability update."""

    edge: int
    rates_per_s: tuple[float, ...]
    hazards: tuple[float, ...]
    no_event_probability: float
    survival_before: float
    survival_after: float
    cif_before: tuple[float, ...]
    cif_after: tuple[float, ...]

    @property
    def rates_by_cause(self) -> dict[str, float]:
        return _cause_mapping(self.rates_per_s)

    @property
    def hazards_by_cause(self) -> dict[str, float]:
        return _cause_mapping(self.hazards)

    @property
    def cif_before_by_cause(self) -> dict[str, float]:
        return _cause_mapping(self.cif_before)

    @property
    def cif_after_by_cause(self) -> dict[str, float]:
        return _cause_mapping(self.cif_after)

    @property
    def mass_after(self) -> float:
        return math.fsum((self.survival_after, *self.cif_after))


@dataclass(frozen=True)
class ActiveOrderCompetingRiskCIF:
    """Immutable state for one active-order remaining-quantity risk spell."""

    identity: ClassVar[str] = IDENTITY
    schema_version: ClassVar[str] = STATE_SCHEMA_VERSION

    spell_id: str
    phase: str
    remaining_qty: float
    last_edge: int
    survival: float = 1.0
    cumulative_incidence: tuple[float, ...] = (0.0, 0.0, 0.0, 0.0)
    terminal: bool = False
    terminal_cause: str | None = None
    terminal_edge: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "spell_id", _spell_id(self.spell_id))
        object.__setattr__(self, "phase", _phase(self.phase, terminal=self.terminal))
        quantity = _finite_float(self.remaining_qty, name="remaining_qty")
        if quantity < 0.0 or (not self.terminal and quantity <= 0.0):
            raise CIFMechanicsError(
                "remaining_qty must be positive while active and non-negative when terminal"
            )
        object.__setattr__(self, "remaining_qty", quantity)
        object.__setattr__(self, "last_edge", _edge(self.last_edge, name="last_edge"))

        survival = _finite_float(self.survival, name="survival")
        cif = tuple(
            _finite_float(value, name=f"cif.{cause}")
            for cause, value in zip(CAUSES, self.cumulative_incidence, strict=True)
        )
        _validate_probability_state(survival=survival, cumulative_incidence=cif)
        object.__setattr__(self, "survival", survival)
        object.__setattr__(self, "cumulative_incidence", cif)

        if self.terminal:
            if self.terminal_cause not in _CAUSE_SET:
                raise CIFMechanicsError("terminal state requires a supported terminal_cause")
            if self.terminal_edge is None:
                raise CIFMechanicsError("terminal state requires terminal_edge")
            object.__setattr__(
                self, "terminal_edge", _edge(self.terminal_edge, name="terminal_edge")
            )
            if self.terminal_edge != self.last_edge:
                raise CIFMechanicsError("terminal_edge must equal last_edge")
        elif self.terminal_cause is not None or self.terminal_edge is not None:
            raise CIFMechanicsError("active state cannot carry terminal metadata")

    @classmethod
    def start(
        cls,
        *,
        spell_id: str,
        phase: str,
        remaining_qty: float,
        last_edge: int,
    ) -> ActiveOrderCompetingRiskCIF:
        return cls(
            spell_id=spell_id,
            phase=phase,
            remaining_qty=remaining_qty,
            last_edge=last_edge,
        )

    @property
    def cif_by_cause(self) -> dict[str, float]:
        return _cause_mapping(self.cumulative_incidence)

    def advance(
        self,
        *,
        edge: int,
        rates_per_s: Mapping[str, object],
    ) -> tuple[ActiveOrderCompetingRiskCIF, CIFIntervalResult]:
        """Advance exactly one edge; skipped edges fail before any state is changed."""

        if self.terminal:
            raise TerminalStateError("exchange-terminal CIF state cannot advance")
        next_edge = _edge(edge, name="edge")
        expected_edge = self.last_edge + 1
        if next_edge > expected_edge:
            raise MissedGridEdgeError(
                f"missed 100ms grid edge: expected {expected_edge}, got {next_edge}"
            )
        if next_edge < expected_edge:
            raise GridEdgeSequenceError(
                f"duplicate or non-monotone grid edge: expected {expected_edge}, got {next_edge}"
            )

        rates = _cause_tuple(rates_per_s, name="rates_per_s")
        hazards_by_cause, no_event_probability = jointly_normalized_hazards(
            rates_per_s
        )
        hazards = tuple(hazards_by_cause[cause] for cause in CAUSES)
        increments = tuple(self.survival * probability for probability in hazards)
        next_cif = tuple(
            previous + increment
            for previous, increment in zip(
                self.cumulative_incidence, increments, strict=True
            )
        )
        next_survival = _exact_survival_complement(next_cif)
        product_survival = self.survival * no_event_probability
        if not math.isclose(
            next_survival,
            product_survival,
            rel_tol=1e-14,
            abs_tol=_NUMERIC_TOLERANCE,
        ):
            raise ArithmeticError("survival update disagrees with joint no-event probability")
        if next_survival > self.survival:
            raise ArithmeticError("survival must be monotone non-increasing")
        if any(
            current < previous
            for previous, current in zip(
                self.cumulative_incidence, next_cif, strict=True
            )
        ):
            raise ArithmeticError("cumulative incidence must be monotone non-decreasing")
        _validate_probability_state(
            survival=next_survival, cumulative_incidence=next_cif
        )

        next_state = replace(
            self,
            last_edge=next_edge,
            survival=next_survival,
            cumulative_incidence=next_cif,
        )
        result = CIFIntervalResult(
            edge=next_edge,
            rates_per_s=rates,
            hazards=hazards,
            no_event_probability=no_event_probability,
            survival_before=self.survival,
            survival_after=next_survival,
            cif_before=self.cumulative_incidence,
            cif_after=next_cif,
        )
        return next_state, result

    def transition_phase(self, phase: str) -> ActiveOrderCompetingRiskCIF:
        """Apply a non-terminal lifecycle phase transition without changing time."""

        if self.terminal:
            raise TerminalStateError("exchange-terminal CIF state cannot change phase")
        return replace(self, phase=_phase(phase, terminal=False))

    def reset_after_partial_fill(
        self,
        *,
        spell_id: str,
        phase: str,
        remaining_qty: float,
        last_edge: int,
    ) -> ActiveOrderCompetingRiskCIF:
        """Start a new remaining-quantity spell after a caller-observed partial fill."""

        if self.terminal:
            raise TerminalStateError("exchange-terminal CIF state cannot reset a spell")
        new_spell_id = _spell_id(spell_id)
        if new_spell_id == self.spell_id:
            raise CIFMechanicsError("partial-fill reset requires a new spell_id")
        reset_edge = _edge(last_edge, name="last_edge")
        if reset_edge != self.last_edge:
            raise CIFMechanicsError(
                "partial-fill reset must preserve the last evaluated grid edge"
            )
        quantity = _finite_float(remaining_qty, name="remaining_qty")
        if not 0.0 < quantity < self.remaining_qty:
            raise CIFMechanicsError(
                "partial-fill reset requires remaining_qty strictly between zero and prior quantity"
            )
        return ActiveOrderCompetingRiskCIF.start(
            spell_id=new_spell_id,
            phase=_phase(phase, terminal=False),
            remaining_qty=quantity,
            last_edge=reset_edge,
        )

    def terminate(
        self,
        *,
        cause: str,
        remaining_qty: float | None = None,
    ) -> ActiveOrderCompetingRiskCIF:
        """Mark the exchange order terminal; subsequent evaluation is forbidden."""

        if self.terminal:
            raise TerminalStateError("CIF state is already exchange-terminal")
        normalized_cause = str(cause).strip().lower()
        if normalized_cause not in _CAUSE_SET:
            raise CIFMechanicsError(f"unsupported terminal cause: {cause!r}")
        quantity = self.remaining_qty if remaining_qty is None else _finite_float(
            remaining_qty, name="remaining_qty"
        )
        if not 0.0 <= quantity <= self.remaining_qty:
            raise CIFMechanicsError(
                "terminal remaining_qty must be within [0, prior remaining_qty]"
            )
        if normalized_cause in {"favorable_fill", "adverse_fill"} and quantity != 0.0:
            raise CIFMechanicsError(
                "nonzero remaining quantity is a partial fill and requires explicit spell reset"
            )
        return replace(
            self,
            phase=TERMINAL_PHASE,
            remaining_qty=quantity,
            terminal=True,
            terminal_cause=normalized_cause,
            terminal_edge=self.last_edge,
        )

    def checkpoint(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "identity": IDENTITY,
            "grid_interval_ms": GRID_INTERVAL_MS,
            "spell_id": self.spell_id,
            "phase": self.phase,
            "remaining_qty": self.remaining_qty,
            "last_edge": self.last_edge,
            "survival": self.survival,
            "cumulative_incidence": self.cif_by_cause,
            "terminal": self.terminal,
            "terminal_cause": self.terminal_cause,
            "terminal_edge": self.terminal_edge,
        }

    @classmethod
    def restore(cls, payload: Mapping[str, Any]) -> ActiveOrderCompetingRiskCIF:
        expected_fields = {
            "schema_version",
            "identity",
            "grid_interval_ms",
            "spell_id",
            "phase",
            "remaining_qty",
            "last_edge",
            "survival",
            "cumulative_incidence",
            "terminal",
            "terminal_cause",
            "terminal_edge",
        }
        fields = set(payload)
        if fields != expected_fields:
            raise CIFMechanicsError(
                "checkpoint schema mismatch: "
                f"missing={sorted(expected_fields - fields)} extra={sorted(fields - expected_fields)}"
            )
        if payload["schema_version"] != STATE_SCHEMA_VERSION:
            raise CIFMechanicsError("unsupported checkpoint schema_version")
        if payload["identity"] != IDENTITY:
            raise CIFMechanicsError("checkpoint identity mismatch")
        if payload["grid_interval_ms"] != GRID_INTERVAL_MS:
            raise CIFMechanicsError("checkpoint grid interval mismatch")
        if not isinstance(payload["terminal"], bool):
            raise CIFMechanicsError("checkpoint terminal must be boolean")
        raw_cif = payload["cumulative_incidence"]
        if not isinstance(raw_cif, Mapping):
            raise CIFMechanicsError("checkpoint cumulative_incidence must be a mapping")
        cif = _cause_tuple(raw_cif, name="cumulative_incidence")
        return cls(
            spell_id=payload["spell_id"],
            phase=payload["phase"],
            remaining_qty=payload["remaining_qty"],
            last_edge=payload["last_edge"],
            survival=payload["survival"],
            cumulative_incidence=cif,
            terminal=payload["terminal"],
            terminal_cause=payload["terminal_cause"],
            terminal_edge=payload["terminal_edge"],
        )
