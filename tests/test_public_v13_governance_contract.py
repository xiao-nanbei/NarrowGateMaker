from __future__ import annotations

import json
import tomllib
from pathlib import Path

import narrowgate

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "research" / "registry.json"
V12_IDENTITY = (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260820_v12.json"
)
V13_IDENTITY = (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260825_v13.json"
)


def _registry() -> dict[str, object]:
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def _family(payload: dict[str, object], family_id: str) -> dict[str, object]:
    return next(row for row in payload["families"] if row["id"] == family_id)


def test_public_release_version_is_v0_1_1_everywhere() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        package_version = tomllib.load(handle)["project"]["version"]

    assert package_version == "0.1.1"
    assert narrowgate.__version__ == package_version
    assert "`v0.1.1`" in (ROOT / "README.md").read_text(encoding="utf-8")
    assert "`v0.1.1`" in (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")


def test_deploy_identity_list_carries_v11_v12_and_v13() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    block = makefile.split("DEPLOY_IDENTITY_FILES :=", 1)[1].split("DEPLOY_FILES +=", 1)[0]

    assert "operational_baseline_identity_20260812_v11.json" in block
    assert "operational_baseline_identity_20260820_v12.json" in block
    assert "operational_baseline_identity_20260825_v13.json" in block


def test_registry_is_an_ordinary_public_successor_with_frozen_retirement_metadata() -> None:
    payload = _registry()
    lineage = payload["publication_lineage"]

    assert payload["as_of"] == "2026-08-25"
    assert (
        lineage["materialization"] == "ordinary_safe_public_json_no_private_source_identity_claim"
    )
    assert lineage["predecessor_public_projection_sha256"] == (
        "3e4e6feefac208d64f92e8fe4b1e93204728711d2625b9d79a865c01e02e39a2"
    )
    assert lineage["predecessor_private_source_sha256"] == (
        "f09829baa37f43dac3c3521985a1fa53df7af7a570b71a78b31912a3f9529770"
    )
    assert lineage["predecessor_private_source_rewritten"] is False
    assert lineage["predecessor_projection_manifest_entry_retired"] is True
    assert lineage["successor_projection_identity_claimed"] is False


def test_registry_separates_current_live_from_immutable_v12_backtest_default() -> None:
    route = _registry()["active_routes"]["operational_baseline"]
    live = route["current_live"]
    backtest = route["backtest_default"]

    assert route["governance_identity"] == V13_IDENTITY
    assert route["identity_hash_authority"] == "operational_baseline_current.json"
    assert live["remote_pointer"] == "${NARROWGATE_LIVE_REMOTE_POINTER}"
    assert live["config_locator"] == "${NARROWGATE_LIVE_CONFIG}"
    assert live["buy_e3_enabled"] is True
    assert live["buy_e1_changed"] is False
    assert live["buy_e2_changed"] is False
    assert live["sell_owner_policy_changed"] is False
    assert live["reducing_quote_semantics_changed"] is False
    assert live["external_venues_enabled"] is False
    assert live["global_flow_shadow_enabled"] is False
    assert live["global_reference_shadow_enabled"] is False
    assert live["companion_enabled"] is False
    assert live["post_lifecycle_capture_is_latest_live_status"] is False
    assert live["nonbaseline_action_occurrence_proven"] is False
    assert live["economic_outcomes_read"] is False
    assert live["economic_values_persisted"] is False
    assert live["economic_effect_proven"] is False
    assert live["backtest_economic_authority"] is False
    assert live["public_governance_grants_live_authority"] is False
    assert live["private_release_and_evidence_chain_required"] is True

    assert backtest["identity"] == V12_IDENTITY
    assert backtest["config_locator"].startswith("${NARROWGATE_PRIVATE_CONFIG_ROOT}/")
    assert backtest["current_live_alias_allowed"] is False
    assert backtest["exact_buy_e3_replay_baseline_available"] is False
    assert backtest["current_live_e3_evidence_is_backtest_economic_authority"] is False
    assert backtest["historical_v12_economic_evidence_reinterpreted"] is False


def test_current_family_summaries_preserve_the_same_authority_split() -> None:
    payload = _registry()
    f03 = _family(payload, "F03")
    f05 = _family(payload, "F05")
    f10 = _family(payload, "F10")

    assert f03["operational_governance_identity"] == V13_IDENTITY
    assert f03["backtest_default_identity"] == V12_IDENTITY
    assert f03["current_live_alias_allowed_for_backtest"] is False
    assert f03["current_live_no_shadow"] is True
    assert f03["dynamic_fill_hazard_shadow_scope"].endswith("not_current_live")

    assert f05["current_live_governance_identity"] == V13_IDENTITY
    assert f05["current_live_buy_e3_enabled"] is True
    assert f05["current_live_no_shadow"] is True
    assert f05["current_live_companion_enabled"] is False
    assert f05["current_live_sell_owner_policy_changed"] is False
    assert f05["current_live_evidence_is_backtest_economic_authority"] is False
    assert f05["exact_buy_e3_replay_baseline_available"] is False

    assert f10["current_live_governance_identity"] == V13_IDENTITY
    assert f10["backtest_default_identity"] == V12_IDENTITY
    assert f10["current_live_no_shadow"] is True
    assert f10["current_live_alias_allowed_for_backtest"] is False


def test_current_host_document_uses_only_logical_current_locators() -> None:
    document = (ROOT / "docs/live_host_and_historical_data_access_20260811.md").read_text(
        encoding="utf-8"
    )

    assert "${NARROWGATE_LIVE_REMOTE_POINTER}" in document
    assert "<current-live-epoch>" in document
    assert "prospective-" not in document
    assert "2 vCPU / 2 GiB" not in document
    assert "latest-liveness" in document
    assert "economic or backtest authority" in document
