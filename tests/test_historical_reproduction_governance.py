import json
from pathlib import Path

import pytest

import research.governance.paths as governance_paths
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import (
    main as direct_fill_main,
)
from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (
    main as queue_value_main,
)
from research.families.f08_side_taker_lifecycle.audit.side_taker_hazard_calibration import (
    main as side_taker_main,
)
from research.governance.historical_reproduction import (
    PROJECT_ROOT,
    HistoricalReproductionError,
    require_historical_reproduction,
    stamp_historical_reproduction_output,
    verify_frozen_source_identity,
)

FROZEN_F06_SPEC = (
    PROJECT_ROOT
    / "research/families/f06_placement_fill_cif/docs/"
    "placement_fill_cif_v1_spec_20260726.json"
)


def test_closed_runner_requires_explicit_historical_mode() -> None:
    with pytest.raises(HistoricalReproductionError, match="is closed"):
        require_historical_reproduction(
            runner_id="f06.direct_fill_cif",
            enabled=False,
            spec_path=FROZEN_F06_SPEC,
        )


def test_f06_cli_guard_runs_before_panel_io(tmp_path: Path) -> None:
    with pytest.raises(HistoricalReproductionError, match="is closed"):
        direct_fill_main(
            [
                "--spec",
                str(FROZEN_F06_SPEC),
                "--panel-dir",
                str(tmp_path / "missing-panel"),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )


def test_f07_and_f08_closed_clis_fail_before_input_io(tmp_path: Path) -> None:
    missing = str(tmp_path / "missing")
    with pytest.raises(HistoricalReproductionError, match="source is retained read-only"):
        queue_value_main(
            [
                "--historical-reproduction",
                "--source-panel",
                missing,
                "--base-queue-bundle",
                missing,
                "--evidence-split",
                missing,
                "--score-profile-contract",
                missing,
                "--output-bundle",
                missing,
                "--output-predictions",
                missing,
                "--output-report",
                missing,
            ]
        )
    with pytest.raises(HistoricalReproductionError, match="source is retained read-only"):
        side_taker_main(
            [
                "--historical-reproduction",
                "--input-panel",
                missing,
                "--output-predictions",
                missing,
                "--output-summary",
                missing,
                "--output-dataset-manifest",
                missing,
            ]
        )


def test_f06_accepts_only_registered_path_and_hash(tmp_path: Path) -> None:
    identity = require_historical_reproduction(
        runner_id="f06.direct_fill_cif",
        enabled=True,
        spec_path=FROZEN_F06_SPEC,
    )
    assert identity["research_authority"] == "historical_evidence_only"
    assert identity["new_experiment_identity_allowed"] is False

    copied = tmp_path / FROZEN_F06_SPEC.name
    copied.write_bytes(FROZEN_F06_SPEC.read_bytes())
    with pytest.raises(HistoricalReproductionError, match="spec path"):
        require_historical_reproduction(
            runner_id="f06.direct_fill_cif",
            enabled=True,
            spec_path=copied,
        )


@pytest.mark.parametrize(
    "runner_id",
    ["f07.queue_value_competing_risk", "f08.side_taker_hazard_calibration"],
)
def test_runner_without_complete_frozen_contract_is_read_only(runner_id: str) -> None:
    with pytest.raises(HistoricalReproductionError, match="source is retained read-only"):
        require_historical_reproduction(
            runner_id=runner_id,
            enabled=True,
            spec_path=None,
        )


def test_f06_revision_without_exact_source_bytes_is_read_only() -> None:
    with pytest.raises(HistoricalReproductionError, match="source is retained read-only"):
        require_historical_reproduction(
            runner_id="f06.full_curve_fill_cif",
            enabled=True,
            spec_path=None,
        )


def test_historical_output_cannot_claim_new_authority(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps({"action_or_live_authorization": False}), encoding="utf-8"
    )
    identity = require_historical_reproduction(
        runner_id="f06.direct_fill_cif",
        enabled=True,
        spec_path=FROZEN_F06_SPEC,
    )
    stamp_historical_reproduction_output(tmp_path, identity)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["research_authority"] == "historical_evidence_only"
    assert report["new_experiment_identity_allowed"] is False
    assert report["action_or_live_authorization"] is False


def test_migrated_runner_verifies_original_frozen_source_from_archive() -> None:
    try:
        identity = verify_frozen_source_identity(
            "models/audit/competing_curve_fill_cif.py",
            "93ea9f2edc6c6f8f2b674e88ca1fdea265a43a8cbb3d48b83d0b329492ad302f",
        )
    except HistoricalReproductionError as exc:
        assert "availability=private_not_distributed" in str(exc)
        return

    assert identity["source"] == "legacy_snapshot_v1"
    assert identity["path"] == "legacy/models/audit/competing_curve_fill_cif.py"
    assert identity["archive_artifact_id"] == "research-layout-legacy-snapshot-v1"
    assert identity["source_availability"] == "private_not_distributed"


def test_migrated_runner_fails_closed_without_private_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        governance_paths,
        "PRIVATE_LEGACY_ARCHIVE_ROOT",
        tmp_path / "missing-private-evidence",
    )
    with pytest.raises(
        HistoricalReproductionError,
        match="availability=private_not_distributed",
    ):
        verify_frozen_source_identity(
            "models/audit/competing_curve_fill_cif.py",
            "93ea9f2edc6c6f8f2b674e88ca1fdea265a43a8cbb3d48b83d0b329492ad302f",
        )
