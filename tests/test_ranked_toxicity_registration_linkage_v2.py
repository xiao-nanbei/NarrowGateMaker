from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_exposure_guard_registration_linkage_v2 import (
    canonical_spec_sha256,
    sha256_file,
    validate_registration_linkage_v2,
)

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_V2_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_registration_linkage_v2_"
    "amendment_20260802.json"
)
CURRENT_V3_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_registration_linkage_v3_"
    "amendment_20260803.json"
)


def test_historical_v2_linkage_rejects_current_config_bytes() -> None:
    with pytest.raises(ValueError, match="current operational config SHA256 mismatch"):
        validate_registration_linkage_v2(HISTORICAL_V2_AMENDMENT)


def test_historical_v3_linkage_rejects_current_v6_config_bytes() -> None:
    with pytest.raises(ValueError, match="current operational config SHA256 mismatch"):
        validate_registration_linkage_v2(CURRENT_V3_AMENDMENT)


def test_linkage_rejects_an_independently_valid_but_different_model_dir(
    tmp_path: Path,
) -> None:
    amendment = json.loads(CURRENT_V3_AMENDMENT.read_text(encoding="utf-8"))
    source_config = ROOT / amendment["artifact_identities"][
        "current_operational_config"
    ]["path"]
    config = yaml.safe_load(source_config.read_text(encoding="utf-8"))
    config["ml"]["model_dir"] = "models/example_model_bundle"
    changed_config = tmp_path / "config.yaml"
    changed_config.write_text(yaml.safe_dump(config), encoding="utf-8")
    amendment["artifact_identities"]["current_operational_config"] = {
        "path": str(changed_config),
        "sha256": sha256_file(changed_config),
    }
    amendment["canonical_spec_identity_sha256"] = canonical_spec_sha256(
        amendment
    )
    changed_amendment = tmp_path / "amendment.json"
    changed_amendment.write_text(json.dumps(amendment), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match the frozen v12"):
        validate_registration_linkage_v2(changed_amendment)
