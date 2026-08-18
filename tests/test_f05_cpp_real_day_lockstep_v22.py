from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_real_day_lockstep_v22 as lockstep,
)


def _bundle(days: tuple[str, ...]) -> SimpleNamespace:
    rows_by_day = {day: 1 for day in days}
    return SimpleNamespace(
        source_manifest={
            "selected_days": list(days),
            "canonical_manifest_sha256": "a" * 64,
        },
        panel_manifest={
            "canonical_panel_manifest_sha256": "b" * 64,
            "files": {
                "metadata": {
                    "row_key_sha256": "d" * 64,
                    "day_census": {"rows_by_day": rows_by_day},
                }
            },
        },
        execution_manifest={"canonical_execution_manifest_sha256": "c" * 64},
    )


def test_all_panel_builder_walk_validates_every_opportunity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = tuple(f"2026-07-{day:02d}" for day in range(1, 31))
    bundle = _bundle(days)

    def read_rows(_bundle: object, day: str) -> pd.DataFrame:
        ordinal = days.index(day) + 1
        opportunity_id = f"opportunity-{ordinal:02d}"
        return pd.DataFrame(
            {
                "opportunity_id": [opportunity_id],
                "exposure_fill_ordinal": [ordinal],
                "fill_visible_ts_ms": [1_000 + ordinal],
                "campaign_id": [ordinal],
                "side": ["BUY" if ordinal % 2 else "SELL"],
            },
            index=pd.Index([opportunity_id], name="opportunity_id"),
        )

    def build_row(_cpp: object, opportunity: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(
            exposure_fill_ordinal=opportunity["exposure_fill_ordinal"],
            fill_ts_ms=opportunity["fill_visible_ts_ms"],
            campaign_id=opportunity["campaign_id"],
            predicate_values=[],
        )

    validated: list[str] = []
    monkeypatch.setattr(lockstep, "EXPECTED_PANEL_OPPORTUNITIES", len(days))
    monkeypatch.setattr(lockstep, "_read_qualification_rows", read_rows)
    monkeypatch.setattr(
        lockstep.cpp_runtime,
        "build_cpp_runtime_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            policy=SimpleNamespace(predicate_columns=["p0", "p1", "p2"])
        ),
    )
    monkeypatch.setattr(lockstep.cpp_runtime, "build_target_predicate_row", build_row)
    monkeypatch.setattr(
        lockstep.cpp_runtime,
        "validate_target_predicate_row",
        lambda _cpp, _row, opportunity, **_kwargs: validated.append(
            str(opportunity["opportunity_id"])
        ),
    )

    receipt_path = tmp_path / lockstep.BUILDER_PREFLIGHT_RECEIPT_NAME
    fake_cpp = SimpleNamespace(
        F05RepeatedBooleanCooldownRuntime=lambda _config: SimpleNamespace(
            parity_qualified=True,
            binding_error="",
        ),
        validate_f05_cooldown_predicate_rows=lambda _config, _rows: None,
    )
    receipt = lockstep.preflight_all_panel_target_rows(
        bundle,
        cpp=fake_cpp,
        policy_path=tmp_path / "policy.json",
        predicate_path=tmp_path / "predicates.json",
        invariance_receipt={"canonical_receipt_sha256": "e" * 64},
        receipt_path=receipt_path,
    )

    assert receipt["status"] == lockstep.BUILDER_PREFLIGHT_STATUS
    assert receipt["opportunity_count"] == len(days)
    assert receipt["economic_values_read"] is False
    assert receipt["economic_values_persisted"] is False
    assert receipt["cpp_startup_validated_row_count"] == len(days)
    assert receipt["economic_evaluator_call_count"] == 0
    assert receipt["formal_v24_to_v25_invariance_receipt_sha256"] == "e" * 64
    assert len(validated) == len(days)
    assert json.loads(receipt_path.read_text(encoding="ascii")) == receipt


def test_all_panel_builder_walk_rejects_unqualified_runtime_before_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = tuple(f"2026-07-{day:02d}" for day in range(1, 31))
    bundle = _bundle(days)
    monkeypatch.setattr(
        lockstep.cpp_runtime,
        "build_cpp_runtime_config",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        lockstep,
        "_read_qualification_rows",
        lambda *_args, **_kwargs: pytest.fail("panel rows must not be read"),
    )
    fake_cpp = SimpleNamespace(
        F05RepeatedBooleanCooldownRuntime=lambda _config: SimpleNamespace(
            parity_qualified=False,
            binding_error="cpp_qualification_scope_invalid",
        )
    )

    with pytest.raises(
        lockstep.CppRealDayLockstepError,
        match="runtime identity is not parity-qualified",
    ):
        lockstep.preflight_all_panel_target_rows(
            bundle,
            cpp=fake_cpp,
            policy_path=tmp_path / "policy.json",
            predicate_path=tmp_path / "predicates.json",
            invariance_receipt={"canonical_receipt_sha256": "e" * 64},
            receipt_path=tmp_path / lockstep.BUILDER_PREFLIGHT_RECEIPT_NAME,
        )


def test_unhandled_lockstep_exception_is_admitted_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}\n", encoding="ascii")
    output_path = tmp_path / "cpp_real_day_lockstep_receipt.json"

    def fail(*_args: object, stage: dict[str, str], **_kwargs: object) -> None:
        stage["value"] = "all_panel_zero_economic_builder_walk"
        raise ValueError("builder exploded")

    monkeypatch.setattr(lockstep, "_run_lockstep_impl", fail)

    with pytest.raises(lockstep.CppRealDayLockstepError, match="failed closed"):
        lockstep.run_lockstep(manifest_path, output_path=output_path)

    receipt = json.loads(output_path.read_text(encoding="ascii"))
    assert receipt["status"] == "failed_closed_execution_exception"
    assert receipt["failing_phase"] == "all_panel_zero_economic_builder_walk"
    assert receipt["exception_class"] == "ValueError"
    assert receipt["economic_values_persisted"] is False
    assert receipt["validation_read"] is False
    assert receipt["sealed_holdout_read"] is False
    assert receipt["action_authorized"] is False
    assert receipt["live_authorized"] is False
