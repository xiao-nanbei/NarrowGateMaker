from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_paths import data_root
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_execution_prep as prep,
)

NATIVE_DATA_ROOT = data_root()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_repository_contract_freezes_exact_native_40_days() -> None:
    frozen = prep.load_frozen_native_panel()
    assert frozen.profile_id == prep.EXPECTED_PROFILE
    assert len(frozen.days) == 40
    assert frozen.days == tuple(sorted(set(frozen.days)))
    assert frozen.days[0] == "2026-04-17"
    assert frozen.days[-1] == "2026-06-26"


def test_profile_hash_drift_fails_closed(tmp_path: Path) -> None:
    precommit = json.loads(prep.DEFAULT_PRECOMMIT_PATH.read_text(encoding="utf-8"))
    evidence = json.loads(prep.DEFAULT_PROFILE_EVIDENCE_PATH.read_text(encoding="utf-8"))
    precommit_path = tmp_path / "precommit.json"
    evidence_path = tmp_path / "profile.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    precommit["native_development_panel"]["source_profile_sha256"] = "0" * 64
    precommit_path.write_text(json.dumps(precommit), encoding="utf-8")
    with pytest.raises(prep.NativeExecutionPrepError, match="binds source profile"):
        prep.load_frozen_native_panel(
            precommit_path=precommit_path,
            profile_evidence_path=evidence_path,
        )


def test_require_unknown_bundle_fails_before_source_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called = False

    def forbidden(**_: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("source resolver must not run")

    monkeypatch.setattr(prep.source_specs, "build_orico_daily_source_spec", forbidden)
    with pytest.raises(prep.NativeExecutionPrepError, match="fails closed"):
        prep.prepare_native_execution_inputs(
            market_data_root=tmp_path,
            cache_root=tmp_path / "cache" / "native-40d",
            require_bound_model_bundle=True,
        )
    assert called is False


def test_cache_root_must_be_under_market_data_cache(tmp_path: Path) -> None:
    with pytest.raises(prep.NativeExecutionPrepError, match="must stay below"):
        prep._require_cache_root(tmp_path, tmp_path.parent / "elsewhere")


def test_worker_count_fails_closed_before_source_access(tmp_path: Path) -> None:
    with pytest.raises(prep.NativeExecutionPrepError, match="workers"):
        prep.prepare_native_execution_inputs(
            market_data_root=tmp_path,
            cache_root=tmp_path / "cache" / "native-40d",
            workers=9,
        )


def test_feature_only_pipeline_is_atomic_resume_safe_and_training_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    market_root = tmp_path / "market"
    cache_root = market_root / "cache" / "native-40d"
    market_root.mkdir()
    days = tuple(f"2026-05-{day:02d}" for day in range(1, 31)) + tuple(
        f"2026-06-{day:02d}" for day in range(1, 11)
    )
    frozen = prep.FrozenNativePanel(
        days=days,
        precommit_path=tmp_path / "precommit.json",
        precommit_sha256="1" * 64,
        profile_evidence_path=tmp_path / "profile.json",
        profile_evidence_sha256="2" * 64,
        profile_id=prep.EXPECTED_PROFILE,
    )
    monkeypatch.setattr(prep, "load_frozen_native_panel", lambda **_: frozen)
    monkeypatch.setattr(
        prep.shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=1000 * prep.GIB),
    )

    source_calls: list[str] = []
    materialize_calls: list[str] = []

    def build_source(*, target_day: str, market_data_root: Path, profile_id: str):
        source_calls.append(target_day)
        bundle = SimpleNamespace(
            utc_day=target_day,
            identity_sha256=lambda: hashlib.sha256(target_day.encode()).hexdigest(),
        )
        return SimpleNamespace(
            profile_id=profile_id,
            bundle=bundle,
            probe={"physical_materialization_eligible": True, "utc_day": target_day},
        )

    def source_payload(bundle: object) -> dict[str, object]:
        return {"utc_day": bundle.utc_day, "profile_id": prep.EXPECTED_PROFILE}

    def materialize(
        bundle: object,
        *,
        output_dir: Path,
        cutoffs_ms: object,
        batch_rows: int,
        engine: str,
    ):
        del cutoffs_ms, batch_rows
        assert engine == prep.panels.CPP_BATCH_ENGINE
        materialize_calls.append(bundle.utc_day)
        output_dir.mkdir(parents=True, exist_ok=True)
        panel_path = output_dir / "panel.parquet"
        manifest_path = output_dir / "manifest.json"
        panel_path.write_bytes(bundle.utc_day.encode("ascii"))
        manifest_path.write_text(
            json.dumps({"utc_day": bundle.utc_day, "rows": 86_400}),
            encoding="utf-8",
        )
        return SimpleNamespace(
            output_dir=output_dir,
            panel_path=panel_path,
            manifest_path=manifest_path,
            cache_identity_sha256=hashlib.sha256(bundle.utc_day.encode()).hexdigest(),
            row_count=86_400,
            reused=False,
        )

    monkeypatch.setattr(prep.source_specs, "build_orico_daily_source_spec", build_source)
    monkeypatch.setattr(prep.source_specs, "source_spec_payload", source_payload)

    def materialize_exact(built: object, *, output_dir: Path, batch_rows: int):
        return materialize(
            built.bundle,
            output_dir=output_dir,
            cutoffs_ms=None,
            batch_rows=batch_rows,
            engine=prep.panels.CPP_BATCH_ENGINE,
        )

    monkeypatch.setattr(prep, "_materialize_exact_native_panel", materialize_exact)

    result = prep.prepare_native_execution_inputs(
        market_data_root=market_root,
        cache_root=cache_root,
    )
    assert source_calls == list(days)
    assert materialize_calls == list(days)
    assert result["status"] == "feature_panels_complete_model_bundle_unbound"
    assert result["completed_day_count"] == 40
    assert result["execution_input_eligible"] is False
    assert result["blockers"] == ["model_bundle_identity_unknown"]
    assert result["training_performed"] is False

    manifest_path = cache_root / prep.MANIFEST_FILENAME
    success_path = cache_root / prep.SUCCESS_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert success_path.read_text(encoding="ascii").strip() == _sha256(manifest_path)
    assert manifest["labels_read"] is False
    assert manifest["predictions_read"] is False
    assert manifest["economic_outcomes_read"] is False
    assert manifest["training_performed"] is False
    assert manifest["training_authorized"] is False
    assert len(manifest["identity_payload"]["feature_panels"]) == 40

    # A byte-identical rerun reuses the admitted top-level identity.
    second = prep.prepare_native_execution_inputs(
        market_data_root=market_root,
        cache_root=cache_root,
    )
    assert second["execution_prep_manifest_sha256"] == result["execution_prep_manifest_sha256"]


def test_cli_has_no_training_mode() -> None:
    parser_source = Path(prep.__file__).read_text(encoding="utf-8")
    assert "train_research_bundle" not in parser_source
    assert 'training_performed": False' in parser_source


@pytest.mark.skipif(
    not NATIVE_DATA_ROOT.is_dir(),
    reason="configured exact-root integration data is unavailable",
)
def test_exact_native_probe_bridge_preserves_original_bundle(tmp_path: Path) -> None:
    built = prep.source_specs.build_orico_daily_source_spec(
        target_day="2026-04-17",
        market_data_root=NATIVE_DATA_ROOT,
        profile_id=prep.EXPECTED_PROFILE,
    )
    generic_probe = prep.panels.sources.probe_source_bundle
    observed: dict[str, object] = {}

    def fake_materializer(bundle: object, **_: object):
        observed["bundle"] = bundle
        observed["probe"] = prep.panels.sources.probe_source_bundle(bundle)
        panel_path = tmp_path / "panel.parquet"
        manifest_path = tmp_path / "manifest.json"
        panel_path.write_bytes(b"panel")
        manifest_path.write_text("{}", encoding="utf-8")
        return SimpleNamespace(
            output_dir=tmp_path,
            panel_path=panel_path,
            manifest_path=manifest_path,
            cache_identity_sha256="3" * 64,
            row_count=86_400,
            reused=False,
        )

    original_materializer = prep.panels.materialize_daily_panel
    prep.panels.materialize_daily_panel = fake_materializer
    try:
        prep._materialize_exact_native_panel(
            built,
            output_dir=tmp_path,
            batch_rows=4_096,
        )
    finally:
        prep.panels.materialize_daily_panel = original_materializer
    assert observed["bundle"] is built.bundle
    assert observed["probe"] == built.probe
    assert prep.panels.sources.probe_source_bundle is generic_probe
