from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs" / "value_provenance_registry_20260727.json"


def test_value_provenance_registry_has_unique_typed_entries() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "narrowgate_value_provenance.v2"
    allowed = set(payload["classes"])
    entries = payload["entries"]
    ids = [str(entry["id"]) for entry in entries]

    assert len(ids) == len(set(ids))
    assert {str(entry["class"]) for entry in entries} <= allowed
    assert all(entry.get("source") for entry in entries)
    assert "empirical_estimate" not in allowed
    assert {
        "empirical_direct_estimate",
        "empirical_policy_selection",
    } <= allowed
    for entry in entries:
        assert set(entry.get("components", {})) <= allowed


def test_direct_estimates_and_policy_selections_are_not_conflated() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    entries = {str(entry["id"]): entry for entry in payload["entries"]}

    for entry_id in (
        "p3_empirical_survival",
        "aws_tokyo_latency_profile",
        "placement_full_curve_competing_v4_development_result",
    ):
        assert entries[entry_id]["class"] == "empirical_direct_estimate"

    for entry_id in (
        "gamma_live",
        "kappa_legacy_fallback",
        "fill_cooldown_live",
        "buy_fill_selection_threshold",
    ):
        assert entries[entry_id]["class"] == "empirical_policy_selection"


def test_two_v4_families_have_unambiguous_display_identities() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    names = payload["family_display_names"]

    assert names["placement_fill_cif_v4"] == "fixed_horizon_v4"
    assert (
        names["placement_fill_full_curve_competing_cif_v4"]
        == "full_curve_competing_v4"
    )
    assert (
        names["placement_fill_policy_clock_race_v1"]
        == "policy_clock_race_v1"
    )
    assert payload["status"] == "historical_provenance_snapshot_no_current_live_authority"
    assert payload["entry_live_authority_flags_effective_for_current_runtime"] is False
    live_authority_semantics = payload["live_authority_semantics"]
    assert "grants no current authority" in live_authority_semantics
    assert "operational_baseline_current.json" in live_authority_semantics


def test_policy_clock_race_result_has_no_live_authority() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    entries = {str(entry["id"]): entry for entry in payload["entries"]}

    method = entries["placement_policy_clock_race_v1_method"]
    result = entries["placement_policy_clock_race_v1_development_result"]
    assert method["live_authority"] is False
    assert result["live_authority"] is False
    assert result["status"] == "closed_on_development"
    assert result["value_or_formula"]["validation_read"] is False


def test_retired_point_intercept_gate_is_not_misclassified_as_theory() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    entries = {str(entry["id"]): entry for entry in payload["entries"]}
    gate = entries["retired_intercept_gate_v1"]

    assert gate["class"] == "judgmental_engineering"
    assert gate["live_authority"] is False
    assert gate["status"].startswith("retired")


def test_shadow_fill_gate_cannot_authorize_live_action() -> None:
    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))
    entries = {str(entry["id"]): entry for entry in payload["entries"]}

    assert entries["placement_shadow_gate"]["live_authority"] is False
    assert entries["placement_action_value_gate"]["live_authority"] is False
