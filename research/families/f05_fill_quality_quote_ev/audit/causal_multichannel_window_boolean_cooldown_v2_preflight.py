#!/usr/bin/env python3
"""Outcome-blind preflight for the multichannel cooldown v2 successor."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import tempfile
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from models.audit.experiment_scorecard_v2 import score_profile_contract
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_2025_predicate_artifacts as predicate_materializer,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as features,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_label_panel as label_panel,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_mechanics_receipt as mechanics_receipt,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_features as native_features,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_nested_oof as nested_oof,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_predicates as predicates,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_replay_emitter as replay_emitter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_sequence_support as sequence_support,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_shared_prefix as shared_prefix,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_snapshot as snapshot,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_source_manifest as source_manifest,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_checkpoint as checkpoint,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_label_panel_runner as panel_runner,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_labels as strict_labels,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_study as study,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_windows as windows,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_strict_native_latency_baseline_50d as strict_baseline,
)
from research.governance.paths import resolve_research_path
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = features.IDENTITY
SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_spec_20260810.json"
)
FEATURE_SEMANTICS_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "feature_semantics_amendment_v1_20260811.json"
)
FEATURE_SEMANTICS_AMENDMENT_SHA256 = (
    "5d24db38bd0511db441ded022b7e402ea15dd26ea5f5df22e988cf50ec0de685"
)
EXECUTION_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v1_20260811.json"
)
EXECUTION_AMENDMENT_SHA256 = (
    "70f765b50cb6271aa9033f7a236d3f416d07303aedb56830c1cbe1636085ee41"
)
EXECUTION_AMENDMENT_V2 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v2_20260811.json"
)
EXECUTION_AMENDMENT_V2_SHA256 = (
    "1086b39cd5f26f0e678c47d1885bbb4b99c88d129866a6a5ce4d21b0953993a7"
)
EXECUTION_AMENDMENT_V3 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v3_20260811.json"
)
EXECUTION_AMENDMENT_V3_SHA256 = (
    "58949b176c9f810881e887a4fcab56debd6941ca22ce9b31d66d065bc0e7e829"
)
EXECUTION_AMENDMENT_V4 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v4_20260811.json"
)
EXECUTION_AMENDMENT_V4_SHA256 = (
    "5ab2371de9e736d2f1d45e79a931ebe6073a83bb17fbbcb3cb148c8b343ceb9f"
)
EXECUTION_AMENDMENT_V5 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v5_20260811.json"
)
EXECUTION_AMENDMENT_V5_SHA256 = (
    "67436ebe25da5963c881f665b96cedc79ebc6a8bee93620149d6657b20dd20ed"
)
EXECUTION_AMENDMENT_V6 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v6_20260811.json"
)
EXECUTION_AMENDMENT_V6_SHA256 = (
    "7e333deee8e42df61889bbc42753a0fc26000f013df5b523a0e70aaf59e8ebea"
)
EXECUTION_AMENDMENT_V7 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v7_20260811.json"
)
EXECUTION_AMENDMENT_V7_SHA256 = (
    "5b9ee2732a837e13817c2adf2581e6488dd74114df00942a0960ed5e38710c12"
)
EXECUTION_AMENDMENT_V8 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v8_20260811.json"
)
EXECUTION_AMENDMENT_V8_SHA256 = (
    "864a0256eb0b4f209e8bc26875449c5bca05255f3d91a56030c78fc35bfb957b"
)
EXECUTION_AMENDMENT_V9 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v9_20260811.json"
)
EXECUTION_AMENDMENT_V9_SHA256 = (
    "04c7833677bea6d14d125a91ba5196b3e7cb989c22d1e88dd80951dddd87b34c"
)
DEFAULT_OUTPUT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_preflight_20260810.json"
)
DURABLE_BENCHMARK_ROOT = (
    source_manifest.DEFAULT_DATA_ROOT
    / "reports"
    / "causal_multichannel_window_boolean_cooldown_duration_v2_20260810"
    / "engineering_benchmarks"
)
STRICT_BENCHMARK_SEARCH_ROOTS = (
    DURABLE_BENCHMARK_ROOT,
    Path(tempfile.gettempdir()),
)
STRICT_BENCHMARK_DIRECTORY_GLOB = (
    "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark*"
)
V11_MECHANICS_RECEIPT_ROOT = mechanics_receipt.DEFAULT_OUTPUT
FORMAL_PANEL_MANIFEST = (
    strict_labels.DEFAULT_OUTPUT
    / "panel_runner"
    / "formal_full_support_41d_v9"
    / "panel_manifest.json"
)
FORMAL_SOURCE_PREBUILD_MANIFEST = (
    strict_labels.DEFAULT_OUTPUT
    / "panel_runner"
    / "formal_full_support_41d"
    / "native_cache_prebuild_union_v3"
    / "manifest.json"
)
PREDICATE_ADMISSION_ROOT = predicate_materializer.DEFAULT_OUTPUT_ROOT
STUDY_ADMISSION_ROOT = (
    source_manifest.DEFAULT_DATA_ROOT
    / "reports"
    / "causal_multichannel_window_boolean_cooldown_duration_v2_20260810"
    / "nested_chronological_oof_v2_execution_v9"
)

# The executable binding is duplicated here deliberately: the Spec is the
# research identity, while this preflight fails before execution if either the
# Spec declaration or the imported implementation has drifted.
IMPLEMENTATION_BINDINGS: dict[str, dict[str, str]] = {
    "features": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_features.py",
        "sha256": "fbcfadd8277b2a7ef35d2b16f584e661852b0a3066327e3e2c2f729b717964c0",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_features.py",
        "test_sha256": "df7ab0c23f06deb3c2b3a3e17bf06d9dc9968465c06d8331955201ca99d654b7",
    },
    "windows": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_windows.py",
        "sha256": "46148f56b96e85a57f22b1654f903cf36b831f11d6dd04d3ab4263baf2af8c78",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_windows.py",
        "test_sha256": "f018c8f7c7c0802ffcaa71d4470124e7cc284d5b6d84980809b068cfb40d3b8d",
    },
    "native_features": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_native_features.py",
        "sha256": "0d9ee2b7497b78b9274f0af6dc60500f232d83f9b12e857cd7b75d6c4251e689",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_native_features.py",
        "test_sha256": "c79242a2266d5bdfa8ed07c594b6e93cc883a838ad731633ebf0f4bb814ae930",
    },
    "snapshot": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_snapshot.py",
        "sha256": "5f4c10d3d55f02203807c67e4d32d09d1ba4097dea669e74ff8eca35dcd5f7ea",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_snapshot.py",
        "test_sha256": "a8faf85a3aba2213095cda68a1889facaa5a0481c39cba38bd1bd3f99ccffd25",
    },
    "source_manifest": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_source_manifest.py",
        "sha256": "86f992b5df347b82b15dfcee60106d96494ec895648bb350fbc743501cf2f32c",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_source_manifest.py",
        "test_sha256": "6e8083b8d73920cfa72d728766f9b72368f9330c8accd5b61c8d54f75a6105f9",
    },
    "predicate_materializer": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_2025_predicate_artifacts.py",
        "sha256": "176a347092183fb43454236c32d83b1754bf095ac555d1bb6e654b01da7b2bf9",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_2025_predicate_artifacts.py",
        "test_sha256": "bbd2235476134343a2329589e9f761eefcf7435c35e70788fdc9e5467997c5c9",
    },
    "strict_checkpoint": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_strict_checkpoint.py",
        "sha256": "02734c28a40b200551ff39013acc12fb631e28583720ecc6b940510574a0c4aa",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_strict_checkpoint.py",
        "test_sha256": "16ff1c7c041eaaf6e42e2f580eada7e77daab786e3a9212e80a4832dec527714",
    },
    "replay_emitter": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_replay_emitter.py",
        "sha256": "51784343a37635ca813023df8f8c494c75a37aa943baba7c96b0c6c5250cefcb",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_replay_emitter.py",
        "test_sha256": "c1c101a03d05b2897816547ce9a20daef574f72e6d77d12073ef2da8d17ee651",
    },
    "shared_prefix": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_shared_prefix.py",
        "sha256": "218ceba4491faa7ae359ce9c60bf48e947bf321f970186874eae8fd3e52a9816",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_shared_prefix.py",
        "test_sha256": "96245955c666e877f3c229b50007c7bf2f25040155aa5223c3b23366860f9fcd",
    },
    "strict_labels": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_strict_labels.py",
        "sha256": "94e0063452ec434898f1aabc0c5dda9c75ef906db094eedaa3b14aa4bc656035",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_strict_labels.py",
        "test_sha256": "1fe06a603af8a2112245b7cd23246fd3b4e55b56d7859eb414300edc7df8253a",
    },
    "label_panel": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_label_panel.py",
        "sha256": "a3f7c47940a62b61e8f55bb7c430eae5ac579e03fee97bc80f51c948b65b0c2d",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_label_panel.py",
        "test_sha256": "3c4cd7d547d457732d88693c066ff10beb34ac94b893a0dd551404fa00feb8d8",
    },
    "mechanics_receipt": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_mechanics_receipt.py",
        "sha256": "ab38333cd30c3f9120412dee446e1afb2a2af3fac9ce99fa4c1cc49583563cb5",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_mechanics_receipt.py",
        "test_sha256": "e52236759a33e8591151634fad20c3b632fe8561f29ddf9c246ea4c749ea60ba",
    },
    "predicates": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_predicates.py",
        "sha256": "4b8d0bf871690c2833b9dc7ae1e2f179fb81c88094b606ae9e22216da5106ab8",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_predicates.py",
        "test_sha256": "c7768c90558673556a8b5adfdd6ffe5e0f2095483ebbdc9dcef5f8bdfcc9099d",
    },
    "nested_oof": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_nested_oof.py",
        "sha256": "f196dd3d0924e30e58dd62dcc3f73c9d452ab75a38397d213906f0a352e986db",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_nested_oof.py",
        "test_sha256": "f9adeee733e6823e4cf783b6074f4b97b924c91cc2d24509a37060de48ac6e2e",
    },
    "strict_label_panel_runner": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_strict_label_panel_runner.py",
        "sha256": "a61bac08f74ba345ddaea647e674d7ad9b50e528cc63d2c614f398d1ce53fdde",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_strict_label_panel_runner.py",
        "test_sha256": "403f866c8260a0f0c95ab51b371e238fca2c3a6417396477d35bf443f1a31772",
    },
    "native_sequence_support_mapping": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_sequence_support.py",
        "sha256": "4638262688a89fb0b8eabaa0a1fbcbd9c8fcbc82563191fb10d15ede054f1ac9",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_sequence_support.py",
        "test_sha256": "4f430e1b5c8daa80eedf38a7839895e371600e5715934562a893d9efc15de52d",
    },
    "study": {
        "path": "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_study.py",
        "sha256": "2e14210695496aed4f82d1afa09d334115f537bb991aa31fc3c95e244e51c36d",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_study.py",
        "test_sha256": "a6eebeb240902e7e9b7210263debb83f39f7bb7d110257de5115606304a9f17d",
    },
    "execution_replay_abi": {
        "path": "models/backtest_tick.py",
        "sha256": "379daa3c31bd1261b7d755d11bff476a803836c3992306e6b130a3ea7b1c7f1b",
        "test_path": "tests/test_causal_multichannel_window_boolean_cooldown_replay_binding.py",
        "test_sha256": "d41bb89c1bea0951a5ca7df7d3578cdee2a08a130afcbd39fca4ccbc6a407b96",
    },
    "native_queue_scheduler": {
        "path": "models/exchange_book_replay.py",
        "sha256": "a2c53a603dbdf6903faadedb976704de404951622f2cd671e0612fb18a4b5910",
        "test_path": "tests/test_exchange_book_replay.py",
        "test_sha256": "9e61f79d59cb3d4a9806b2b0b5d93b9892019bbb682c5de3b42cede751e3b526",
    },
}

_COMPONENT_MODULES = {
    "features": features,
    "windows": windows,
    "native_features": native_features,
    "snapshot": snapshot,
    "source_manifest": source_manifest,
    "predicate_materializer": predicate_materializer,
    "strict_checkpoint": checkpoint,
    "replay_emitter": replay_emitter,
    "shared_prefix": shared_prefix,
    "strict_labels": strict_labels,
    "label_panel": label_panel,
    "mechanics_receipt": mechanics_receipt,
    "predicates": predicates,
    "nested_oof": nested_oof,
    "strict_label_panel_runner": panel_runner,
    "native_sequence_support_mapping": sequence_support,
    "study": study,
}

_SPEC_BINDING_LOCATORS = {
    "features": ("feature_implementation",),
    "windows": ("implementation_bindings", "causal_window_extractor"),
    "native_features": ("implementation_bindings", "strict_native_M2_extractor"),
    "snapshot": ("implementation_bindings", "assignment_snapshot"),
    "source_manifest": (
        "implementation_bindings",
        "outcome_blind_source_manifest_builder",
    ),
    "predicate_materializer": (
        "implementation_bindings",
        "outcome_blind_2025_predicate_materializer",
    ),
    "strict_checkpoint": (
        "implementation_bindings",
        "strict_checkpoint_metadata",
    ),
    "replay_emitter": ("implementation_bindings", "replay_assignment_emitter"),
    "shared_prefix": ("implementation_bindings", "posix_shared_prefix"),
    "strict_labels": ("implementation_bindings", "strict_one_shot_label_runner"),
    "label_panel": ("implementation_bindings", "strict_label_panel"),
    "predicates": ("implementation_bindings", "predicate_learner"),
    "nested_oof": ("implementation_bindings", "nested_chronological_oof"),
    "strict_label_panel_runner": (
        "implementation_bindings",
        "strict_label_panel_runner",
    ),
    "study": ("implementation_bindings", "multiday_study"),
    "execution_replay_abi": ("implementation_bindings", "execution_replay_abi"),
    "native_queue_scheduler": (
        "implementation_bindings",
        "native_queue_scheduler",
    ),
}


class PreflightError(RuntimeError):
    """Raised when a frozen v2 dependency or permission has drifted."""


def _benchmark_parent_completion_audit(run_root: Path) -> dict[str, Any]:
    manifest_paths = [
        path
        for path in run_root.glob(
            "support_identity=*/feature_block=*/days/*/manifest.json"
        )
        if not any(
            part.startswith(".") for part in path.relative_to(run_root).parts
        )
    ]
    if len(manifest_paths) != 1:
        return {
            "verified": False,
            "reason": "benchmark requires exactly one atomically admitted day",
            "day_manifest_count": len(manifest_paths),
        }
    manifest_path = manifest_paths[0]
    day_root = manifest_path.parent
    success_path = day_root / "_SUCCESS"
    snapshots_path = day_root / "assignment_snapshots.parquet"
    if not success_path.is_file() or not snapshots_path.is_file():
        return {
            "verified": False,
            "reason": "benchmark day admission is incomplete",
            "day_manifest_path": str(manifest_path),
        }
    try:
        manifest = _load_json(manifest_path)
        success = _load_json(success_path)
        snapshot_binding = manifest["assignment_snapshots"]
        execution = manifest["shared_prefix_execution_audit"]
        parent_stop = manifest["parent_stop_audit"]
        strict_queue = manifest["strict_native_queue"]
        target_day = datetime.fromisoformat(str(manifest["target_day"])).replace(
            tzinfo=UTC
        )
        snapshots = pd.read_parquet(
            snapshots_path,
            columns=["m0_context_json"],
        )
    except (KeyError, TypeError, ValueError, OSError) as exc:
        return {
            "verified": False,
            "reason": f"benchmark day admission cannot be read: {type(exc).__name__}",
            "day_manifest_path": str(manifest_path),
        }
    assignment_timestamps: list[int] = []
    assignment_side_roles: list[tuple[str, str]] = []
    try:
        for raw in snapshots["m0_context_json"]:
            context = json.loads(str(raw))
            assignment_timestamps.append(int(context["assignment_ts_ns"]))
            assignment_side_roles.append(
                (str(context["side"]), str(context["role_at_fill"]))
            )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {
            "verified": False,
            "reason": f"benchmark assignment timestamps are invalid: {type(exc).__name__}",
            "day_manifest_path": str(manifest_path),
        }
    day_start_ns = int(target_day.timestamp() * 1_000_000_000)
    day_end_ns = int((target_day + timedelta(days=1)).timestamp() * 1_000_000_000)
    timestamps_in_target_day = bool(
        assignment_timestamps
        and min(assignment_timestamps) >= day_start_ns
        and max(assignment_timestamps) < day_end_ns
    )
    label_manifests = manifest.get("one_shot_label_manifests", [])
    opportunity_limit = int(manifest.get("max_opportunities") or 0)
    dispatched_side_roles = assignment_side_roles[:opportunity_limit]
    side_role_counts = {
        f"{side}_{role}": dispatched_side_roles.count((side, role))
        for side in ("BUY", "SELL")
        for role in ("opener", "add")
    }
    missing_count = int(strict_queue.get("missing_queue_seed_count", -1))
    missing_trace = strict_queue.get("missing_queue_seed_trace")
    checks = {
        "success_hash_matches": success.get("manifest_sha256") == _sha256(manifest_path),
        "snapshot_hash_matches": snapshot_binding.get("sha256") == _sha256(snapshots_path),
        "snapshot_row_count_matches": int(snapshot_binding.get("rows", -1))
        == len(snapshots),
        "opportunity_limit_covers_first_sell_add": opportunity_limit >= 48,
        "all_limited_label_manifests_admitted": len(label_manifests)
        == opportunity_limit,
        "all_limited_opportunities_dispatched": int(
            execution.get("opportunities_dispatched", -1)
        )
        == opportunity_limit,
        "all_limited_supervisors_completed": int(
            execution.get("supervisor_processes_completed", -1)
        )
        == opportunity_limit,
        "buy_sell_opener_add_all_observed": all(
            count > 0 for count in side_role_counts.values()
        ),
        "zero_pending_supervisors": int(execution.get("pending_supervisors", -1))
        == 0,
        "all_assignment_timestamps_in_target_day": timestamps_in_target_day,
        "parent_stop_triggered": parent_stop.get("triggered") is True,
        "parent_stop_configured_at_target_boundary": int(
            parent_stop.get("configured_stop_ts_ms", 0)
        )
        * 1_000_000
        == day_end_ns,
        "parent_stop_not_early": int(parent_stop.get("trigger_ts_ms", 0))
        >= int(parent_stop.get("configured_stop_ts_ms", 0)),
        "zero_assignments_after_target_boundary": int(
            parent_stop.get("new_assignments_after_target_day_boundary", -1)
        )
        == 0,
        "parent_native_queue_missing_seed_zero": missing_count == 0,
        "parent_native_queue_missing_trace_complete": isinstance(
            missing_trace, list
        )
        and len(missing_trace) == missing_count,
        "parent_native_source_gap_zero": int(
            strict_queue.get("source_gap_events", -1)
        )
        == 0,
    }
    return {
        "verified": all(checks.values()),
        "day_manifest_path": str(manifest_path),
        "target_day": target_day.date().isoformat(),
        "snapshot_rows": len(snapshots),
        "minimum_assignment_ts_ns": min(assignment_timestamps, default=None),
        "maximum_assignment_ts_ns": max(assignment_timestamps, default=None),
        "target_day_end_ts_ns": day_end_ns,
        "dispatched_side_role_counts": side_role_counts,
        "checks": checks,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PreflightError(f"cannot load JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise PreflightError(f"JSON root must be an object: {path}")
    return payload


def _repo_path(value: str) -> Path:
    return resolve_research_path(str(value), require_exists=False)


def _source_identity(path: Path, *, role: str) -> str:
    try:
        return source_identity_sha256(path)
    except (OSError, PublicMachineProjectionError) as exc:
        raise PreflightError(f"{role} source identity is unavailable: {path}") from exc


def _exact_source_document(path: Path, *, role: str) -> Path:
    try:
        return source_document_path(path, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise PreflightError(f"{role} exact source is unavailable: {path}") from exc


def _load_source_json(path: Path, *, role: str) -> dict[str, Any]:
    return _load_json(_exact_source_document(path, role=role))


def _require_hash(path_value: str, expected: str, *, role: str) -> Path:
    path = _repo_path(path_value)
    if not path.is_file():
        raise PreflightError(f"{role} is missing: {path}")
    observed = _source_identity(path, role=role)
    _exact_source_document(path, role=role)
    if observed != str(expected):
        raise PreflightError(
            f"{role} hash drifted: expected={expected} observed={observed}"
        )
    return path


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _declared_spec_binding(
    spec: Mapping[str, Any], locator: tuple[str, ...]
) -> Mapping[str, Any]:
    value: Any = spec
    for key in locator:
        if not isinstance(value, Mapping) or key not in value:
            raise PreflightError(
                f"Spec implementation binding is missing: {'.'.join(locator)}"
            )
        value = value[key]
    if not isinstance(value, Mapping):
        raise PreflightError(
            f"Spec implementation binding is not an object: {'.'.join(locator)}"
        )
    return value


def _validate_amendments(spec: Mapping[str, Any]) -> dict[str, Any]:
    spec_sha256 = _source_identity(SPEC, role="v2 frozen Spec")
    feature_amendment_path = _require_hash(
        str(FEATURE_SEMANTICS_AMENDMENT),
        FEATURE_SEMANTICS_AMENDMENT_SHA256,
        role="v2 feature-semantics amendment",
    )
    execution_amendment_path = _require_hash(
        str(EXECUTION_AMENDMENT),
        EXECUTION_AMENDMENT_SHA256,
        role="v2 execution amendment",
    )
    execution_amendment_v2_path = _require_hash(
        str(EXECUTION_AMENDMENT_V2),
        EXECUTION_AMENDMENT_V2_SHA256,
        role="v2 execution amendment successor",
    )
    execution_amendment_v3_path = _require_hash(
        str(EXECUTION_AMENDMENT_V3),
        EXECUTION_AMENDMENT_V3_SHA256,
        role="v2 execution amendment identity-hardening successor",
    )
    execution_amendment_v4_path = _require_hash(
        str(EXECUTION_AMENDMENT_V4),
        EXECUTION_AMENDMENT_V4_SHA256,
        role="v2 continuous-comparator execution successor",
    )
    execution_amendment_v5_path = _require_hash(
        str(EXECUTION_AMENDMENT_V5),
        EXECUTION_AMENDMENT_V5_SHA256,
        role="v2 source-and-OOF identity-hardening successor",
    )
    execution_amendment_v6_path = _require_hash(
        str(EXECUTION_AMENDMENT_V6),
        EXECUTION_AMENDMENT_V6_SHA256,
        role="v2 native-sequence execution successor",
    )
    execution_amendment_v7_path = _require_hash(
        str(EXECUTION_AMENDMENT_V7),
        EXECUTION_AMENDMENT_V7_SHA256,
        role="v2 target-receipt ABI successor",
    )
    execution_amendment_v8_path = _require_hash(
        str(EXECUTION_AMENDMENT_V8),
        EXECUTION_AMENDMENT_V8_SHA256,
        role="v2 strict-arm admission successor",
    )
    execution_amendment_v9_path = _require_hash(
        str(EXECUTION_AMENDMENT_V9),
        EXECUTION_AMENDMENT_V9_SHA256,
        role="v2 queue-trace and formal-schema successor",
    )
    feature_amendment = _load_source_json(
        feature_amendment_path, role="v2 feature-semantics amendment"
    )
    execution_amendment = _load_source_json(
        execution_amendment_path, role="v2 execution amendment"
    )
    execution_amendment_v2 = _load_source_json(
        execution_amendment_v2_path, role="v2 execution amendment successor"
    )
    execution_amendment_v3 = _load_source_json(
        execution_amendment_v3_path,
        role="v2 execution amendment identity-hardening successor",
    )
    execution_amendment_v4 = _load_source_json(
        execution_amendment_v4_path,
        role="v2 continuous-comparator execution successor",
    )
    execution_amendment_v5 = _load_source_json(
        execution_amendment_v5_path,
        role="v2 source-and-OOF identity-hardening successor",
    )
    execution_amendment_v6 = _load_source_json(
        execution_amendment_v6_path,
        role="v2 native-sequence execution successor",
    )
    execution_amendment_v7 = _load_source_json(
        execution_amendment_v7_path, role="v2 target-receipt ABI successor"
    )
    execution_amendment_v8 = _load_source_json(
        execution_amendment_v8_path, role="v2 strict-arm admission successor"
    )
    execution_amendment_v9 = _load_source_json(
        execution_amendment_v9_path,
        role="v2 queue-trace and formal-schema successor",
    )

    for role, payload in (
        ("feature-semantics amendment", feature_amendment),
        ("execution amendment", execution_amendment),
        ("execution amendment successor", execution_amendment_v2),
        (
            "execution amendment identity-hardening successor",
            execution_amendment_v3,
        ),
        (
            "continuous-comparator execution successor",
            execution_amendment_v4,
        ),
        (
            "source-and-OOF identity-hardening successor",
            execution_amendment_v5,
        ),
        ("native-sequence execution successor", execution_amendment_v6),
        ("target-receipt ABI successor", execution_amendment_v7),
        ("strict-arm admission successor", execution_amendment_v8),
        ("queue-trace and formal-schema successor", execution_amendment_v9),
    ):
        if payload.get("identity") != IDENTITY:
            raise PreflightError(f"{role} identity drifted")
        base_spec = payload.get("base_spec")
        if not isinstance(base_spec, Mapping):
            raise PreflightError(f"{role} base Spec binding is missing")
        if _repo_path(str(base_spec.get("path"))) != SPEC:
            raise PreflightError(f"{role} base Spec path drifted")
        if base_spec.get("sha256") != spec_sha256:
            raise PreflightError(f"{role} base Spec hash drifted")
        permissions = payload.get("permissions")
        if not isinstance(permissions, Mapping) or any(permissions.values()):
            raise PreflightError(f"{role} permissions must remain false")

    feature_binding = execution_amendment.get("feature_semantics_amendment")
    if not isinstance(feature_binding, Mapping):
        raise PreflightError("execution amendment feature binding is missing")
    if _repo_path(str(feature_binding.get("path"))) != feature_amendment_path:
        raise PreflightError("execution amendment feature path drifted")
    if feature_binding.get("sha256") != FEATURE_SEMANTICS_AMENDMENT_SHA256:
        raise PreflightError("execution amendment feature hash drifted")

    predecessor_binding = execution_amendment_v2.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_binding, Mapping):
        raise PreflightError("execution amendment successor predecessor is missing")
    if _repo_path(str(predecessor_binding.get("path"))) != execution_amendment_path:
        raise PreflightError("execution amendment successor predecessor path drifted")
    if predecessor_binding.get("sha256") != EXECUTION_AMENDMENT_SHA256:
        raise PreflightError("execution amendment successor predecessor hash drifted")

    predecessor_v3_binding = execution_amendment_v3.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v3_binding, Mapping):
        raise PreflightError(
            "execution identity-hardening successor predecessor is missing"
        )
    if (
        _repo_path(str(predecessor_v3_binding.get("path")))
        != execution_amendment_v2_path
    ):
        raise PreflightError(
            "execution identity-hardening successor predecessor path drifted"
        )
    if predecessor_v3_binding.get("sha256") != EXECUTION_AMENDMENT_V2_SHA256:
        raise PreflightError(
            "execution identity-hardening successor predecessor hash drifted"
        )

    predecessor_v4_binding = execution_amendment_v4.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v4_binding, Mapping):
        raise PreflightError("continuous-comparator successor predecessor is missing")
    if (
        _repo_path(str(predecessor_v4_binding.get("path")))
        != execution_amendment_v3_path
    ):
        raise PreflightError(
            "continuous-comparator successor predecessor path drifted"
        )
    if predecessor_v4_binding.get("sha256") != EXECUTION_AMENDMENT_V3_SHA256:
        raise PreflightError(
            "continuous-comparator successor predecessor hash drifted"
        )

    predecessor_v5_binding = execution_amendment_v5.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v5_binding, Mapping):
        raise PreflightError("source-and-OOF successor predecessor is missing")
    if (
        _repo_path(str(predecessor_v5_binding.get("path")))
        != execution_amendment_v4_path
    ):
        raise PreflightError("source-and-OOF successor predecessor path drifted")
    if predecessor_v5_binding.get("sha256") != EXECUTION_AMENDMENT_V4_SHA256:
        raise PreflightError("source-and-OOF successor predecessor hash drifted")

    predecessor_v6_binding = execution_amendment_v6.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v6_binding, Mapping):
        raise PreflightError("native-sequence successor predecessor is missing")
    if (
        _repo_path(str(predecessor_v6_binding.get("path")))
        != execution_amendment_v5_path
    ):
        raise PreflightError("native-sequence successor predecessor path drifted")
    if predecessor_v6_binding.get("sha256") != EXECUTION_AMENDMENT_V5_SHA256:
        raise PreflightError("native-sequence successor predecessor hash drifted")

    predecessor_v7_binding = execution_amendment_v7.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v7_binding, Mapping):
        raise PreflightError("target-receipt ABI successor predecessor is missing")
    if (
        _repo_path(str(predecessor_v7_binding.get("path")))
        != execution_amendment_v6_path
    ):
        raise PreflightError("target-receipt ABI successor predecessor path drifted")
    if predecessor_v7_binding.get("sha256") != EXECUTION_AMENDMENT_V6_SHA256:
        raise PreflightError("target-receipt ABI successor predecessor hash drifted")

    predecessor_v8_binding = execution_amendment_v8.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v8_binding, Mapping):
        raise PreflightError("strict-arm admission successor predecessor is missing")
    if (
        _repo_path(str(predecessor_v8_binding.get("path")))
        != execution_amendment_v7_path
    ):
        raise PreflightError("strict-arm admission successor predecessor path drifted")
    if predecessor_v8_binding.get("sha256") != EXECUTION_AMENDMENT_V7_SHA256:
        raise PreflightError("strict-arm admission successor predecessor hash drifted")

    predecessor_v9_binding = execution_amendment_v9.get(
        "predecessor_execution_amendment"
    )
    if not isinstance(predecessor_v9_binding, Mapping):
        raise PreflightError(
            "queue-trace and formal-schema successor predecessor is missing"
        )
    if (
        _repo_path(str(predecessor_v9_binding.get("path")))
        != execution_amendment_v8_path
    ):
        raise PreflightError(
            "queue-trace and formal-schema successor predecessor path drifted"
        )
    if predecessor_v9_binding.get("sha256") != EXECUTION_AMENDMENT_V8_SHA256:
        raise PreflightError(
            "queue-trace and formal-schema successor predecessor hash drifted"
        )

    feature_bindings = feature_amendment.get("implementation_bindings")
    if not isinstance(feature_bindings, Mapping):
        raise PreflightError("feature amendment implementation bindings are missing")
    for name in ("features", "predicates", "snapshot"):
        declared = feature_bindings.get(name)
        expected = IMPLEMENTATION_BINDINGS[name]
        if not isinstance(declared, Mapping):
            raise PreflightError(f"feature amendment {name} binding is missing")
        if declared.get("path") != expected["path"]:
            raise PreflightError(f"feature amendment {name} path drifted")
        if name == "snapshot" and declared.get("sha256") != expected["sha256"]:
            raise PreflightError(f"feature amendment {name} hash drifted")

    replacements = execution_amendment.get("implementation_binding_replacements")
    expected_replacements = {
        "nested_chronological_oof": "nested_oof",
        "study_runner": "study",
    }
    if not isinstance(replacements, Mapping) or set(replacements) != set(
        expected_replacements
    ):
        raise PreflightError("execution amendment replacement set drifted")
    for amendment_name, component_name in expected_replacements.items():
        declared = replacements[amendment_name]
        expected = IMPLEMENTATION_BINDINGS[component_name]
        if not isinstance(declared, Mapping):
            raise PreflightError(
                f"execution amendment {amendment_name} binding is invalid"
            )
        for field in ("path", "sha256", "test_path", "test_sha256"):
            if component_name in {"nested_oof", "study"} and field in {
                "sha256",
                "test_sha256",
            }:
                # Later amendments replace these implementations. Historical
                # amendment files remain immutable and hash-bound; v5 binds
                # the effective executable hashes below.
                continue
            if declared.get(field) != expected[field]:
                raise PreflightError(
                    f"execution amendment {amendment_name} {field} drifted"
                )

    reporting = execution_amendment.get("reporting_contract")
    expected_reporting = {
        "nominal_mechanics_denominator": (40, 10, 50, False),
        "exact_label_economic_denominator": (33, 8, 41, True),
        "reduced_support_diagnostic_denominator": (7, 2, 9, False),
    }
    if not isinstance(reporting, Mapping):
        raise PreflightError("execution amendment reporting contract is missing")
    for name, expected in expected_reporting.items():
        row = reporting.get(name)
        if not isinstance(row, Mapping):
            raise PreflightError(f"execution amendment {name} is missing")
        observed = (
            int(row.get("prefix", -1)),
            int(row.get("added", -1)),
            int(row.get("pooled", -1)),
            bool(row.get("economic_statistics_allowed", False)),
        )
        if observed != expected:
            raise PreflightError(f"execution amendment {name} drifted")

    successor_replacements = execution_amendment_v2.get(
        "implementation_binding_replacements"
    )
    if not isinstance(successor_replacements, Mapping) or set(
        successor_replacements
    ) != {"strict_label_panel_runner"}:
        raise PreflightError("execution amendment successor replacement set drifted")
    panel_binding = successor_replacements["strict_label_panel_runner"]
    expected_panel_binding = IMPLEMENTATION_BINDINGS["strict_label_panel_runner"]
    if not isinstance(panel_binding, Mapping):
        raise PreflightError("execution amendment successor panel binding is invalid")
    for field in ("path", "sha256", "test_path", "test_sha256"):
        if field in {"sha256", "test_sha256"}:
            # v2 remains immutable historical provenance. v6 binds the
            # corrected overlap-only source segmentation implementation.
            continue
        if panel_binding.get(field) != expected_panel_binding[field]:
            raise PreflightError(
                f"execution amendment successor panel {field} drifted"
            )

    parallelism = execution_amendment_v2.get("parallelism_contract")
    expected_parallelism = {
        "formal_day_worker_default": 1,
        "formal_day_worker_cap": 2,
        "formal_day_worker_contract_changed": False,
        "prebuild_worker_default": 2,
        "prebuild_worker_cap": 4,
        "prebuild_parallel_unit": "disjoint contiguous source-day segment",
        "prebuild_economic_outcomes_read": False,
        "prebuild_duration_arms_run": False,
        "prebuild_hour_cache_publish": "per-hour lock plus atomic rename",
        "prebuild_segment_receipt_publish": "parent-validated atomic JSON",
        "interrupted_prebuild_may_resume_from_valid_hour_cache": True,
    }
    if parallelism != expected_parallelism:
        raise PreflightError("execution amendment successor parallelism drifted")

    hardening_replacements = execution_amendment_v3.get(
        "implementation_binding_replacements"
    )
    expected_hardening_replacements = {
        "shared_prefix_executor": "shared_prefix",
        "strict_label_runner": "strict_labels",
        "label_panel": "label_panel",
        "study_runner": "study",
        "python_replay": "execution_replay_abi",
    }
    if not isinstance(hardening_replacements, Mapping) or set(
        hardening_replacements
    ) != set(expected_hardening_replacements):
        raise PreflightError(
            "execution identity-hardening successor replacement set drifted"
        )
    for amendment_name, component_name in expected_hardening_replacements.items():
        declared = hardening_replacements[amendment_name]
        expected = IMPLEMENTATION_BINDINGS[component_name]
        if not isinstance(declared, Mapping):
            raise PreflightError(
                f"execution identity-hardening {amendment_name} binding is invalid"
            )
        for field in ("path", "sha256", "test_path", "test_sha256"):
            if (
                component_name
                in {
                    "study",
                    "strict_labels",
                    "shared_prefix",
                    "label_panel",
                    "execution_replay_abi",
                }
                and field in {"sha256", "test_sha256"}
            ):
                # Later immutable amendments bind the effective executables.
                continue
            if declared.get(field) != expected[field]:
                raise PreflightError(
                    f"execution identity-hardening {amendment_name} {field} drifted"
                )

    hardening = execution_amendment_v3.get("formal_identity_hardening")
    expected_hardening = {
        "shared_prefix_schema": (
            f"{IDENTITY}.posix_cow_shared_prefix.v4"
        ),
        "opportunity_manifest_schema": (
            f"{IDENTITY}.strict_native_one_shot_opportunity.v4"
        ),
        "arm_result_schema": f"{IDENTITY}.strict_native_one_shot_arm.v5",
        "label_panel_schema": f"{IDENTITY}.strict_native_label_panel.v2",
    }
    if not isinstance(hardening, Mapping):
        raise PreflightError("formal identity-hardening contract is missing")
    for field, expected in expected_hardening.items():
        if hardening.get(field) != expected:
            raise PreflightError(f"formal identity-hardening {field} drifted")
    gate_contract = execution_amendment_v3.get("post_outer_oof_gate_contract")
    if not isinstance(gate_contract, Mapping):
        raise PreflightError("post-outer-OOF gate contract is missing")
    family_selection = gate_contract.get("feature_family_selection")
    if not isinstance(family_selection, Mapping) or family_selection.get(
        "incremental_comparison_count"
    ) != 2:
        raise PreflightError("feature-family selection contract drifted")
    cache_compatibility = execution_amendment_v3.get(
        "cache_and_benchmark_compatibility"
    )
    if not isinstance(cache_compatibility, Mapping) or not (
        cache_compatibility.get("raw_native_source_cache_semantics_changed") is False
        and cache_compatibility.get(
            "in_progress_outcome_blind_source_prebuild_reusable"
        )
        is True
        and cache_compatibility.get(
            "benchmark_v10_may_be_reused_as_formal_label_or_identity_evidence"
        )
        is False
    ):
        raise PreflightError("cache/benchmark compatibility contract drifted")

    comparator_replacements = execution_amendment_v4.get(
        "implementation_binding_replacements"
    )
    if not isinstance(comparator_replacements, Mapping) or set(
        comparator_replacements
    ) != {"study_runner"}:
        raise PreflightError(
            "continuous-comparator successor replacement set drifted"
        )
    comparator_study_binding = comparator_replacements["study_runner"]
    expected_study_binding = IMPLEMENTATION_BINDINGS["study"]
    if not isinstance(comparator_study_binding, Mapping):
        raise PreflightError("continuous-comparator study binding is invalid")
    for field in ("path", "sha256", "test_path", "test_sha256"):
        if field in {"sha256", "test_sha256"}:
            continue
        if comparator_study_binding.get(field) != expected_study_binding[field]:
            raise PreflightError(
                f"continuous-comparator study {field} drifted"
            )
    comparator = execution_amendment_v4.get("continuous_state_comparator")
    capacity = comparator.get("capacity_grid") if isinstance(comparator, Mapping) else None
    if not isinstance(comparator, Mapping) or not isinstance(capacity, Mapping):
        raise PreflightError("continuous-comparator contract is missing")
    if (
        comparator.get("model_family")
        != "raw_state_multioutput_regression_tree_diagnostic"
        or capacity.get("max_depth") != [2, 4]
        or capacity.get("min_samples_leaf") != 20
        or comparator.get("may_replace_boolean_policy") is not False
        or comparator.get("may_grant_action_or_live") is not False
    ):
        raise PreflightError("continuous-comparator contract drifted")
    output_schemas = execution_amendment_v4.get("output_schema_replacements")
    if (
        not isinstance(output_schemas, Mapping)
        or output_schemas.get("report_schema") != study.REPORT_SCHEMA
        or output_schemas.get("admission_schema") != study.MANIFEST_SCHEMA
        or output_schemas.get("new_artifact")
        != "continuous_outer_oof_rows.parquet"
    ):
        raise PreflightError("continuous-comparator output schema drifted")
    runtime = execution_amendment_v4.get("runtime_abi")
    if not isinstance(runtime, Mapping):
        raise PreflightError("continuous-comparator runtime ABI is missing")
    runtime_module_name = str(runtime.get("decision_tree_module", ""))
    runtime_module = importlib.import_module(runtime_module_name)
    runtime_path = Path(str(runtime_module.__file__))
    if (
        runtime.get("sklearn_version") != study.sklearn.__version__
        or runtime_module_name != study.DecisionTreeRegressor.__module__
        or not runtime_path.is_file()
        or _sha256(runtime_path) != runtime.get("decision_tree_module_sha256")
    ):
        raise PreflightError("continuous-comparator runtime ABI drifted")

    v5_replacements = execution_amendment_v5.get(
        "implementation_binding_replacements"
    )
    expected_v5_replacements = {
        "features": "features",
        "native_features": "native_features",
        "assignment_snapshot": "snapshot",
        "source_manifest_builder": "source_manifest",
        "outcome_blind_2025_predicate_materializer": "predicate_materializer",
        "replay_assignment_emitter": "replay_emitter",
        "predicate_learner": "predicates",
        "nested_chronological_oof": "nested_oof",
        "study_runner": "study",
        "execution_replay_abi": "execution_replay_abi",
    }
    if not isinstance(v5_replacements, Mapping) or set(v5_replacements) != set(
        expected_v5_replacements
    ):
        raise PreflightError("source-and-OOF successor replacement set drifted")
    for amendment_name, component_name in expected_v5_replacements.items():
        declared = v5_replacements[amendment_name]
        expected = IMPLEMENTATION_BINDINGS[component_name]
        if not isinstance(declared, Mapping):
            raise PreflightError(
                f"source-and-OOF {amendment_name} binding is invalid"
            )
        for field in ("path", "sha256", "test_path", "test_sha256"):
            if component_name in {
                "nested_oof",
                "study",
                "execution_replay_abi",
            } and field in {"sha256", "test_sha256"}:
                # v9 binds the effective formal-consumer and replay hashes.
                continue
            if declared.get(field) != expected[field]:
                raise PreflightError(
                    f"source-and-OOF {amendment_name} {field} drifted"
                )

    warmup_contract = execution_amendment_v5.get("raw_warmup_admission_contract")
    if not isinstance(warmup_contract, Mapping) or not (
        warmup_contract.get("calendar_cutoff_inference_allowed") is False
        and warmup_contract.get("first_target_window_binds_admission") is True
        and warmup_contract.get("post_binding_admission_change_allowed") is False
        and warmup_contract.get("unadmitted_fallback") == "CONTROL_85N"
    ):
        raise PreflightError("raw warmup admission contract drifted")
    owner_contract = execution_amendment_v5.get("m0_cooldown_owner_contract")
    if not isinstance(owner_contract, Mapping) or not (
        owner_contract.get("learner_input_required") is True
        and owner_contract.get("bounded_categories")
        == ["none", "existing_same_side_lineage"]
        and owner_contract.get("arbitrary_owner_strings_allowed") is False
    ):
        raise PreflightError("M0 cooldown owner contract drifted")
    outer_support = execution_amendment_v5.get("outer_oof_support_contract")
    if not isinstance(outer_support, Mapping) or not (
        outer_support.get("campaign_day_minima_use_acted_support") is True
        and outer_support.get("zero_action_outer_fold_reselection_allowed") is False
        and outer_support.get("validation_read") is False
        and outer_support.get("sealed_holdout_read") is False
    ):
        raise PreflightError("outer OOF action-support contract drifted")
    reduced_m2 = execution_amendment_v5.get("reduced_M2_contract")
    if not isinstance(reduced_m2, Mapping) or not (
        reduced_m2.get("deferred_channels_encoded_as_zero") is False
        and reduced_m2.get("full_M2_claim_allowed") is False
    ):
        raise PreflightError("reduced M2 disclosure contract drifted")
    cache_compatibility_v5 = execution_amendment_v5.get(
        "cache_and_label_compatibility"
    )
    if not isinstance(cache_compatibility_v5, Mapping) or not (
        cache_compatibility_v5.get("raw_native_hour_cache_reusable") is True
        and cache_compatibility_v5.get(
            "interrupted_source_manifest_admission_reusable"
        )
        is False
        and cache_compatibility_v5.get(
            "interrupted_2025_predicate_admission_reusable"
        )
        is False
        and cache_compatibility_v5.get("formal_labels_previously_generated") is False
    ):
        raise PreflightError("v5 cache/label compatibility contract drifted")

    v6_replacements = execution_amendment_v6.get(
        "implementation_binding_replacements"
    )
    expected_v6_replacements = {
        "strict_label_panel_runner": "strict_label_panel_runner",
        "native_sequence_support_mapping": "native_sequence_support_mapping",
    }
    if not isinstance(v6_replacements, Mapping) or set(v6_replacements) != set(
        expected_v6_replacements
    ):
        raise PreflightError("native-sequence successor replacement set drifted")
    for amendment_name, component_name in expected_v6_replacements.items():
        declared = v6_replacements[amendment_name]
        expected = IMPLEMENTATION_BINDINGS[component_name]
        if not isinstance(declared, Mapping):
            raise PreflightError(
                f"native-sequence {amendment_name} binding is invalid"
            )
        for field in ("path", "sha256", "test_path", "test_sha256"):
            if (
                component_name == "strict_label_panel_runner"
                and field in {"sha256", "test_sha256"}
            ):
                # v9 binds the effective formal runner.
                continue
            if declared.get(field) != expected[field]:
                raise PreflightError(
                    f"native-sequence {amendment_name} {field} drifted"
                )

    sequence_binding = execution_amendment_v6.get(
        "native_sequence_support_artifact"
    )
    if not isinstance(sequence_binding, Mapping):
        raise PreflightError("native sequence-support artifact binding is missing")
    sequence_path = _require_hash(
        str(sequence_binding.get("path")),
        str(sequence_binding.get("file_sha256")),
        role="native sequence-support mapping",
    )
    sequence_report = _load_json(sequence_path)
    if (
        sequence_report.get("identity") != IDENTITY
        or sequence_report.get("schema_version") != sequence_support.SCHEMA_VERSION
    ):
        raise PreflightError("native sequence-support artifact identity drifted")
    canonical_sequence_report = dict(sequence_report)
    observed_canonical_sha256 = canonical_sequence_report.pop(
        "canonical_report_sha256", None
    )
    expected_canonical_sha256 = sequence_support.canonical_sha256(
        canonical_sequence_report
    )
    if not (
        observed_canonical_sha256 == expected_canonical_sha256
        == sequence_binding.get("canonical_report_sha256")
    ):
        raise PreflightError("native sequence-support canonical hash drifted")
    upstream = sequence_report.get("upstream_sequence_audit")
    if not isinstance(upstream, Mapping):
        raise PreflightError("native sequence-support upstream binding is missing")
    upstream_json = _require_hash(
        str(upstream.get("json_path")),
        str(sequence_binding.get("upstream_sequence_audit_json_sha256")),
        role="upstream native sequence-audit JSON",
    )
    upstream_csv = _require_hash(
        str(upstream.get("csv_path")),
        str(sequence_binding.get("upstream_sequence_audit_csv_sha256")),
        role="upstream native sequence-audit CSV",
    )
    if not (
        upstream.get("json_sha256") == _sha256(upstream_json)
        and upstream.get("csv_sha256") == _sha256(upstream_csv)
    ):
        raise PreflightError("native sequence-support upstream hashes drifted")
    expected_sequence_counts = {
        "requested_days": 50,
        "frozen_formal_days": 41,
        "formal_sequence_supported_days": 41,
        "frozen_reduced_days": 9,
        "reduced_days_with_sequence_failure": 2,
        "reduced_days_sequence_unconfirmed": 2,
    }
    if sequence_report.get("counts") != expected_sequence_counts:
        raise PreflightError("native sequence-support counts drifted")
    if sequence_report.get("sequence_reduced_days") != [
        "2026-04-20",
        "2026-04-23",
    ]:
        raise PreflightError("native sequence-support failed-day mapping drifted")
    if sequence_report.get("sequence_unconfirmed_days") != [
        "2026-05-06",
        "2026-05-13",
    ]:
        raise PreflightError("native sequence-support unconfirmed mapping drifted")
    sequence_permissions = sequence_report.get("permissions")
    if not isinstance(sequence_permissions, Mapping) or any(
        sequence_permissions.values()
    ):
        raise PreflightError("native sequence-support permissions drifted")
    for field in (
        "requested_days",
        "frozen_formal_days",
        "formal_sequence_supported_days",
        "frozen_reduced_days",
    ):
        if int(sequence_binding.get(field, -1)) != expected_sequence_counts[field]:
            raise PreflightError(f"native sequence-support amendment {field} drifted")
    if sequence_binding.get("known_D_plus_1_sequence_failure_targets") != [
        "2026-04-20",
        "2026-04-23",
    ]:
        raise PreflightError("native sequence-support amendment failures drifted")
    if sequence_binding.get("upstream_sequence_unconfirmed_reduced_targets") != [
        "2026-05-06",
        "2026-05-13",
    ]:
        raise PreflightError("native sequence-support amendment unknowns drifted")
    if sequence_binding.get("economic_outcomes_read") is not False:
        raise PreflightError("native sequence-support read economic outcomes")

    source_contract = execution_amendment_v6.get(
        "source_window_segmentation_contract"
    )
    if not isinstance(source_contract, Mapping) or not (
        source_contract.get("calendar_adjacency_without_interval_overlap_may_coalesce")
        is False
        and source_contract.get("formal_unique_source_days") == 57
        and source_contract.get("formal_unique_source_hours") == 1368
        and source_contract.get("formal_segment_count") == 8
        and source_contract.get("D_and_D_plus_1_strict_counter_delta_required")
        == 0
        and source_contract.get("prebuild_schema")
        == f"{panel_runner.RUNNER_IDENTITY}.native_cache_prebuild_union.v3"
    ):
        raise PreflightError("native source-window segmentation contract drifted")
    ordered_days = spec.get("ordered_utc_days", {})
    strict_source = spec.get("source_separation", {}).get("strict_native_2026", {})
    reduced_days = frozenset(strict_source.get("reduced_support_days", ()))
    formal_days = tuple(
        str(day)
        for day in (
            *ordered_days.get("prefix40", ()),
            *ordered_days.get("added10", ()),
        )
        if str(day) not in reduced_days
    )
    source_plan = panel_runner._source_union_plan(formal_days, formal=True)
    expected_segments = [
        ("2026-04-16", "2026-04-20"),
        ("2026-04-21", "2026-04-23"),
        ("2026-04-30", "2026-05-06"),
        ("2026-05-28", "2026-05-31"),
        ("2026-06-01", "2026-06-03"),
        ("2026-06-04", "2026-06-26"),
        ("2026-07-02", "2026-07-10"),
        ("2026-07-15", "2026-07-17"),
    ]
    observed_segments = [
        (segment.start_day, segment.end_day) for segment in source_plan.segments
    ]
    if not (
        observed_segments == expected_segments
        and len(source_plan.unique_source_days) == 57
        and source_plan.unique_source_hours == 1368
    ):
        raise PreflightError("native source-window execution plan drifted")
    cache_compatibility_v6 = execution_amendment_v6.get(
        "cache_and_label_compatibility"
    )
    if not isinstance(cache_compatibility_v6, Mapping) or not (
        cache_compatibility_v6.get("immutable_native_hour_cache_reusable") is True
        and cache_compatibility_v6.get("v2_partial_segment_receipts_reusable")
        is False
        and cache_compatibility_v6.get("v3_segment_and_target_receipts_required")
        is True
        and cache_compatibility_v6.get("formal_labels_previously_generated")
        is False
        and cache_compatibility_v6.get("nested_oof_previously_run") is False
    ):
        raise PreflightError("v6 cache/label compatibility contract drifted")

    v7_replacements = execution_amendment_v7.get(
        "implementation_binding_replacements"
    )
    if not isinstance(v7_replacements, Mapping) or set(v7_replacements) != {
        "strict_label_runner"
    }:
        raise PreflightError("target-receipt ABI replacement set drifted")
    v7_strict_label_binding = v7_replacements["strict_label_runner"]
    expected_strict_label_binding = IMPLEMENTATION_BINDINGS["strict_labels"]
    if not isinstance(v7_strict_label_binding, Mapping):
        raise PreflightError("target-receipt ABI strict-label binding is invalid")
    for field in ("path", "sha256", "test_path", "test_sha256"):
        if field in {"sha256", "test_sha256"}:
            # v9 binds the effective strict-label consumer.
            continue
        if v7_strict_label_binding.get(field) != expected_strict_label_binding[field]:
            raise PreflightError(f"target-receipt ABI strict-label {field} drifted")
    receipt_contract = execution_amendment_v7.get("target_receipt_abi_contract")
    expected_receipt_schema = (
        f"{panel_runner.RUNNER_IDENTITY}.native_cache_target_72h_receipt.v3"
    )
    if not isinstance(receipt_contract, Mapping) or not (
        receipt_contract.get("producer") == "strict_label_panel_runner"
        and receipt_contract.get("consumer") == "strict_label_runner"
        and receipt_contract.get("required_schema") == expected_receipt_schema
        and receipt_contract.get("v2_target_receipt_accepted") is False
        and receipt_contract.get("v3_target_receipt_required") is True
        and receipt_contract.get(
            "duplicate_72h_scheduler_scan_allowed_after_v3_receipt_validation"
        )
        is False
        and receipt_contract.get("economic_outcomes_read") is False
    ):
        raise PreflightError("target-receipt ABI contract drifted")
    cache_compatibility_v7 = execution_amendment_v7.get(
        "cache_and_label_compatibility"
    )
    if not isinstance(cache_compatibility_v7, Mapping) or not (
        cache_compatibility_v7.get("immutable_native_hour_cache_reusable") is True
        and cache_compatibility_v7.get("v2_partial_segment_receipts_reusable")
        is False
        and cache_compatibility_v7.get("v2_target_receipts_reusable") is False
        and cache_compatibility_v7.get("v3_segment_and_target_receipts_required")
        is True
        and cache_compatibility_v7.get("formal_labels_previously_generated")
        is False
        and cache_compatibility_v7.get("nested_oof_previously_run") is False
    ):
        raise PreflightError("v7 cache/label compatibility contract drifted")

    v8_replacements = execution_amendment_v8.get(
        "implementation_binding_replacements"
    )
    if not isinstance(v8_replacements, Mapping) or set(v8_replacements) != {
        "shared_prefix"
    }:
        raise PreflightError("strict-arm admission replacement set drifted")
    v8_shared_prefix_binding = v8_replacements["shared_prefix"]
    expected_shared_prefix_binding = IMPLEMENTATION_BINDINGS["shared_prefix"]
    if not isinstance(v8_shared_prefix_binding, Mapping):
        raise PreflightError("strict-arm admission shared-prefix binding is invalid")
    for field in ("path", "sha256", "test_path", "test_sha256"):
        if field in {"sha256", "test_sha256"}:
            # v9 binds the effective queue-trace executor.
            continue
        if (
            v8_shared_prefix_binding.get(field)
            != expected_shared_prefix_binding[field]
        ):
            raise PreflightError(
                f"strict-arm admission shared-prefix {field} drifted"
            )
    v8_hardening = execution_amendment_v8.get(
        "formal_identity_hardening_replacement"
    )
    expected_v8_hardening = {
        "shared_prefix_schema": shared_prefix.SCHEMA_VERSION,
        "opportunity_manifest_schema": (
            shared_prefix.OPPORTUNITY_MANIFEST_SCHEMA_VERSION
        ),
        "arm_result_schema": shared_prefix.ARM_RESULT_SCHEMA_VERSION,
    }
    if v8_hardening != expected_v8_hardening:
        raise PreflightError("strict-arm admission schema replacement drifted")
    arm_admission = execution_amendment_v8.get("strict_arm_admission_contract")
    if not isinstance(arm_admission, Mapping) or not (
        arm_admission.get("source_gap_sequence_and_clock_failures") == "fatal"
        and arm_admission.get("treatment_queue_missing_seed")
        == "retain_arm_as_unsupported"
        and arm_admission.get("treatment_queue_invalidation_or_ambiguity")
        == "retain_arm_as_unsupported"
        and arm_admission.get("unsupported_arm_exact_queue_claim_allowed") is False
        and arm_admission.get("unsupported_arm_point_label_allowed") is False
        and arm_admission.get("unrelated_exact_opportunity_invalidated") is False
        and arm_admission.get("all_opportunities_retained_in_mechanics_denominator")
        is True
    ):
        raise PreflightError("strict-arm admission contract drifted")
    supervisor_failure = execution_amendment_v8.get(
        "supervisor_failure_contract"
    )
    if not isinstance(supervisor_failure, Mapping) or not (
        supervisor_failure.get("partial_staging_retained") is True
        and supervisor_failure.get("atomic_error_record") is True
        and supervisor_failure.get("parent_receives_specific_failure") is True
        and supervisor_failure.get("implicit_partial_recovery_allowed") is False
    ):
        raise PreflightError("supervisor failure contract drifted")
    cache_compatibility_v8 = execution_amendment_v8.get(
        "cache_and_label_compatibility"
    )
    if not isinstance(cache_compatibility_v8, Mapping) or not (
        cache_compatibility_v8.get("immutable_native_hour_cache_reusable") is True
        and cache_compatibility_v8.get("v3_segment_and_target_receipts_reusable")
        is True
        and cache_compatibility_v8.get(
            "outcome_blind_2025_predicate_artifacts_reusable"
        )
        is True
        and cache_compatibility_v8.get(
            "v7_formal_opportunity_or_arm_manifests_reusable"
        )
        is False
        and cache_compatibility_v8.get("v7_partial_staging_reusable") is False
        and cache_compatibility_v8.get("v8_formal_labels_previously_generated")
        is False
        and cache_compatibility_v8.get("nested_oof_previously_run") is False
    ):
        raise PreflightError("v8 cache/label compatibility contract drifted")

    v9_replacements = execution_amendment_v9.get(
        "implementation_binding_replacements"
    )
    expected_v9_replacements = {
        "python_replay": "execution_replay_abi",
        "shared_prefix_executor": "shared_prefix",
        "strict_label_runner": "strict_labels",
        "label_panel": "label_panel",
        "strict_label_panel_runner": "strict_label_panel_runner",
        "study_runner": "study",
        "nested_chronological_oof": "nested_oof",
        "mechanics_receipt_runner": "mechanics_receipt",
    }
    if not isinstance(v9_replacements, Mapping) or set(v9_replacements) != set(
        expected_v9_replacements
    ):
        raise PreflightError(
            "queue-trace and formal-schema successor replacement set drifted"
        )
    for amendment_name, component_name in expected_v9_replacements.items():
        declared = v9_replacements[amendment_name]
        expected = IMPLEMENTATION_BINDINGS[component_name]
        if not isinstance(declared, Mapping):
            raise PreflightError(
                f"queue-trace and formal-schema {amendment_name} binding is invalid"
            )
        for field in ("path", "sha256", "test_path", "test_sha256"):
            if declared.get(field) != expected[field]:
                raise PreflightError(
                    f"queue-trace and formal-schema {amendment_name} {field} drifted"
                )

    expected_v9_schemas = {
        "shared_prefix_schema": shared_prefix.SCHEMA_VERSION,
        "opportunity_manifest_schema": (
            shared_prefix.OPPORTUNITY_MANIFEST_SCHEMA_VERSION
        ),
        "arm_result_schema": shared_prefix.ARM_RESULT_SCHEMA_VERSION,
        "strict_day_schema": strict_labels.DAY_SCHEMA_VERSION,
        "label_panel_schema": label_panel.PANEL_IDENTITY,
        "panel_runner_progress_schema": panel_runner.PROGRESS_SCHEMA_VERSION,
        "panel_runner_panel_schema": panel_runner.PANEL_SCHEMA_VERSION,
    }
    if (
        execution_amendment_v9.get("formal_identity_hardening_replacement")
        != expected_v9_schemas
    ):
        raise PreflightError(
            "queue-trace and formal-schema successor schema set drifted"
        )

    trace_contract = execution_amendment_v9.get("strict_queue_trace_contract")
    required_trace_fields = [
        "order_id",
        "side",
        "price",
        "price_tick",
        "activate_ts_ms",
        "status",
        "reason",
        "asof_exchange_ts_ns",
        "segment_id",
        "snapshot_min_tick",
        "snapshot_max_tick",
    ]
    if not isinstance(trace_contract, Mapping) or not (
        trace_contract.get("parent_or_common_prefix_queue_failure") == "fatal"
        and trace_contract.get("treatment_suffix_queue_failure")
        == "retain_only_affected_arm_as_unsupported"
        and trace_contract.get("assignment_trace_cursor_required") is True
        and trace_contract.get("treatment_trace_is_assignment_cursor_suffix") is True
        and trace_contract.get("trace_must_be_unbounded_and_untruncated") is True
        and trace_contract.get(
            "trace_row_count_must_equal_treatment_missing_count"
        )
        is True
        and trace_contract.get("duplicate_order_id_activate_ts_ms_allowed") is False
        and trace_contract.get("required_trace_fields") == required_trace_fields
    ):
        raise PreflightError("queue-trace successor trace contract drifted")

    unsupported_contract = execution_amendment_v9.get(
        "unsupported_label_contract"
    )
    if not isinstance(unsupported_contract, Mapping) or not (
        unsupported_contract.get("economic_point_label_status")
        == "unsupported_redacted"
        and unsupported_contract.get("assignment_to_washout_value_usdc") is None
        and unsupported_contract.get("exact_queue_claim_allowed") is False
        and unsupported_contract.get(
            "opportunity_and_arm_retained_for_denominator_audit"
        )
        is True
        and unsupported_contract.get("unrelated_arm_or_opportunity_invalidated")
        is False
    ):
        raise PreflightError("queue-trace successor unsupported-label contract drifted")

    formal_identity = execution_amendment_v9.get("formal_execution_identity")
    if not isinstance(formal_identity, Mapping) or not (
        formal_identity.get("formal_run_directory")
        == panel_runner.FORMAL_RUN_DIRECTORY_NAME
        and formal_identity.get("day_execution_identity")
        == strict_labels.FORMAL_EXECUTION_IDENTITY
        and formal_identity.get("day_output_layout")
        == (
            "support_identity=<support>/feature_block=M2/"
            "execution_identity=v9/days/<day>"
        )
        and formal_identity.get("v7_or_v8_formal_output_reusable") is False
        and formal_identity.get("old_schema_or_progress_reusable") is False
    ):
        raise PreflightError("queue-trace successor formal identity drifted")

    mechanics_receipt = execution_amendment_v9.get("mechanics_receipt_contract")
    if not isinstance(mechanics_receipt, Mapping) or not (
        mechanics_receipt.get("receipt_identity")
        == "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_v11"
        and mechanics_receipt.get("required_before_formal_execution") is True
        and mechanics_receipt.get("full_support_day_required") is True
        and mechanics_receipt.get(
            "complete_side_specific_eight_arm_bundle_required"
        )
        is True
        and mechanics_receipt.get("queue_trace_and_redaction_contract_verified")
        is True
        and mechanics_receipt.get("aggregate_economic_statistics_read") is False
    ):
        raise PreflightError("queue-trace successor mechanics-receipt contract drifted")

    cache_compatibility_v9 = execution_amendment_v9.get(
        "cache_and_label_compatibility"
    )
    if not isinstance(cache_compatibility_v9, Mapping) or not (
        cache_compatibility_v9.get("immutable_native_hour_cache_reusable") is True
        and cache_compatibility_v9.get("v3_segment_and_target_receipts_reusable")
        is True
        and cache_compatibility_v9.get(
            "outcome_blind_2025_predicate_artifacts_reusable"
        )
        is True
        and cache_compatibility_v9.get(
            "v7_formal_opportunity_or_arm_manifests_reusable"
        )
        is False
        and cache_compatibility_v9.get(
            "v8_formal_opportunity_or_arm_manifests_reusable"
        )
        is False
        and cache_compatibility_v9.get("v7_or_v8_partial_staging_reusable")
        is False
        and cache_compatibility_v9.get("v9_formal_labels_previously_generated")
        is False
        and cache_compatibility_v9.get("nested_oof_previously_run_under_v9")
        is False
    ):
        raise PreflightError("v9 cache/label compatibility contract drifted")

    return {
        "feature_semantics": {
            "path": str(feature_amendment_path),
            "sha256": FEATURE_SEMANTICS_AMENDMENT_SHA256,
            "payload": feature_amendment,
        },
        "execution": {
            "path": str(execution_amendment_path),
            "sha256": EXECUTION_AMENDMENT_SHA256,
            "payload": execution_amendment,
        },
        "execution_v2": {
            "path": str(execution_amendment_v2_path),
            "sha256": EXECUTION_AMENDMENT_V2_SHA256,
            "payload": execution_amendment_v2,
        },
        "execution_v3": {
            "path": str(execution_amendment_v3_path),
            "sha256": EXECUTION_AMENDMENT_V3_SHA256,
            "payload": execution_amendment_v3,
        },
        "execution_v4": {
            "path": str(execution_amendment_v4_path),
            "sha256": EXECUTION_AMENDMENT_V4_SHA256,
            "payload": execution_amendment_v4,
        },
        "execution_v5": {
            "path": str(execution_amendment_v5_path),
            "sha256": EXECUTION_AMENDMENT_V5_SHA256,
            "payload": execution_amendment_v5,
        },
        "execution_v6": {
            "path": str(execution_amendment_v6_path),
            "sha256": EXECUTION_AMENDMENT_V6_SHA256,
            "payload": execution_amendment_v6,
            "sequence_support": {
                "path": str(sequence_path),
                "file_sha256": _sha256(sequence_path),
                "canonical_report_sha256": observed_canonical_sha256,
                "counts": expected_sequence_counts,
            },
            "source_union": {
                "formal_segment_count": len(source_plan.segments),
                "formal_unique_source_days": len(source_plan.unique_source_days),
                "formal_unique_source_hours": source_plan.unique_source_hours,
                "segments": observed_segments,
            },
        },
        "execution_v7": {
            "path": str(execution_amendment_v7_path),
            "sha256": EXECUTION_AMENDMENT_V7_SHA256,
            "payload": execution_amendment_v7,
            "target_receipt_schema": expected_receipt_schema,
        },
        "execution_v8": {
            "path": str(execution_amendment_v8_path),
            "sha256": EXECUTION_AMENDMENT_V8_SHA256,
            "payload": execution_amendment_v8,
        },
        "execution_v9": {
            "path": str(execution_amendment_v9_path),
            "sha256": EXECUTION_AMENDMENT_V9_SHA256,
            "payload": execution_amendment_v9,
        },
    }


def _validate_component_bindings(
    spec: Mapping[str, Any],
    amendments: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    expected_names = {
        "features",
        "windows",
        "native_features",
        "snapshot",
        "source_manifest",
        "predicate_materializer",
        "strict_checkpoint",
        "replay_emitter",
        "shared_prefix",
        "strict_labels",
        "label_panel",
        "mechanics_receipt",
        "predicates",
        "nested_oof",
        "strict_label_panel_runner",
        "native_sequence_support_mapping",
        "study",
        "execution_replay_abi",
        "native_queue_scheduler",
    }
    if set(IMPLEMENTATION_BINDINGS) != expected_names:
        raise PreflightError("v2 executable component binding set drifted")
    if set(_COMPONENT_MODULES) != expected_names - {
        "execution_replay_abi",
        "native_queue_scheduler",
    }:
        raise PreflightError("v2 imported component set drifted")

    audit: dict[str, dict[str, Any]] = {}
    for name in sorted(expected_names):
        binding = IMPLEMENTATION_BINDINGS[name]
        implementation = _require_hash(
            binding["path"],
            binding["sha256"],
            role=f"{name} implementation",
        )
        test_path = _require_hash(
            binding["test_path"],
            binding["test_sha256"],
            role=f"{name} tests",
        )
        module = _COMPONENT_MODULES.get(name)
        if module is not None and getattr(module, "IDENTITY", None) != IDENTITY:
            raise PreflightError(f"{name} implementation identity drifted")

        execution_payload = amendments["execution"]["payload"]
        execution_v2_payload = amendments["execution_v2"]["payload"]
        execution_v3_payload = amendments["execution_v3"]["payload"]
        execution_v4_payload = amendments["execution_v4"]["payload"]
        execution_v5_payload = amendments["execution_v5"]["payload"]
        execution_v6_payload = amendments["execution_v6"]["payload"]
        execution_v7_payload = amendments["execution_v7"]["payload"]
        execution_v8_payload = amendments["execution_v8"]["payload"]
        execution_v9_payload = amendments["execution_v9"]["payload"]
        amendment_binding_name = {
            "nested_oof": "nested_chronological_oof",
            "study": "study_runner",
        }.get(name)
        successor_binding_name = (
            "strict_label_panel_runner"
            if name == "strict_label_panel_runner"
            else None
        )
        hardening_binding_name = {
            "shared_prefix": "shared_prefix_executor",
            "strict_labels": "strict_label_runner",
            "label_panel": "label_panel",
            "study": "study_runner",
            "execution_replay_abi": "python_replay",
        }.get(name)
        comparator_binding_name = "study_runner" if name == "study" else None
        v5_binding_name = {
            "features": "features",
            "native_features": "native_features",
            "snapshot": "assignment_snapshot",
            "source_manifest": "source_manifest_builder",
            "predicate_materializer": "outcome_blind_2025_predicate_materializer",
            "replay_emitter": "replay_assignment_emitter",
            "predicates": "predicate_learner",
            "nested_oof": "nested_chronological_oof",
            "study": "study_runner",
            "execution_replay_abi": "execution_replay_abi",
        }.get(name)
        v6_binding_name = {
            "strict_label_panel_runner": "strict_label_panel_runner",
            "native_sequence_support_mapping": (
                "native_sequence_support_mapping"
            ),
        }.get(name)
        v7_binding_name = "strict_label_runner" if name == "strict_labels" else None
        v8_binding_name = "shared_prefix" if name == "shared_prefix" else None
        v9_binding_name = {
            "execution_replay_abi": "python_replay",
            "shared_prefix": "shared_prefix_executor",
            "strict_labels": "strict_label_runner",
            "label_panel": "label_panel",
            "strict_label_panel_runner": "strict_label_panel_runner",
            "study": "study_runner",
            "nested_oof": "nested_chronological_oof",
            "mechanics_receipt": "mechanics_receipt_runner",
        }.get(name)
        row: dict[str, Any] = {
            "path": str(implementation),
            "sha256": _sha256(implementation),
            "test_path": str(test_path),
            "test_sha256": _sha256(test_path),
            "identity_verified": True,
            "executable_binding_verified": True,
            "declared_in_spec": name in _SPEC_BINDING_LOCATORS,
            "declared_in_execution_amendment": (
                amendment_binding_name is not None
                or successor_binding_name is not None
                or hardening_binding_name is not None
                or comparator_binding_name is not None
                or v5_binding_name is not None
                or v6_binding_name is not None
                or v7_binding_name is not None
                or v8_binding_name is not None
                or v9_binding_name is not None
            ),
        }
        if name in _SPEC_BINDING_LOCATORS:
            if v9_binding_name is not None:
                declared = execution_v9_payload[
                    "implementation_binding_replacements"
                ][v9_binding_name]
                declaration = "queue-trace and formal-schema successor"
            elif v8_binding_name is not None:
                declared = execution_v8_payload[
                    "implementation_binding_replacements"
                ][v8_binding_name]
                declaration = "strict-arm admission successor"
            elif v7_binding_name is not None:
                declared = execution_v7_payload[
                    "implementation_binding_replacements"
                ][v7_binding_name]
                declaration = "target-receipt ABI successor"
            elif v6_binding_name is not None:
                declared = execution_v6_payload[
                    "implementation_binding_replacements"
                ][v6_binding_name]
                declaration = "native-sequence execution successor"
            elif v5_binding_name is not None:
                declared = execution_v5_payload[
                    "implementation_binding_replacements"
                ][v5_binding_name]
                declaration = "source-and-OOF identity-hardening successor"
            elif comparator_binding_name is not None:
                declared = execution_v4_payload[
                    "implementation_binding_replacements"
                ][comparator_binding_name]
                declaration = "continuous-comparator execution successor"
            elif hardening_binding_name is not None:
                declared = execution_v3_payload[
                    "implementation_binding_replacements"
                ][hardening_binding_name]
                declaration = "execution identity-hardening successor"
            elif successor_binding_name is not None:
                declared = execution_v2_payload[
                    "implementation_binding_replacements"
                ][successor_binding_name]
                declaration = "execution amendment successor"
            elif amendment_binding_name is None:
                declared = _declared_spec_binding(
                    spec, _SPEC_BINDING_LOCATORS[name]
                )
                declaration = "Spec"
            else:
                declared = execution_payload["implementation_binding_replacements"][
                    amendment_binding_name
                ]
                declaration = "execution amendment"
            if declared.get("path") != binding["path"]:
                raise PreflightError(f"{name} {declaration} path drifted")
            if declared.get("test_path") != binding["test_path"]:
                raise PreflightError(f"{name} {declaration} test path drifted")
            if declared.get("sha256") != binding["sha256"]:
                raise PreflightError(
                    f"{name} {declaration} implementation hash drifted"
                )
            if declared.get("test_sha256") != binding["test_sha256"]:
                raise PreflightError(f"{name} {declaration} test hash drifted")
            row["effective_implementation_sha256_matches"] = True
            row["effective_test_sha256_matches"] = True
        elif v9_binding_name is not None or v6_binding_name is not None:
            if v9_binding_name is not None:
                declared = execution_v9_payload[
                    "implementation_binding_replacements"
                ][v9_binding_name]
                successor_name = "queue-trace and formal-schema successor"
            else:
                declared = execution_v6_payload[
                    "implementation_binding_replacements"
                ][v6_binding_name]
                successor_name = "native-sequence execution successor"
            for field in ("path", "sha256", "test_path", "test_sha256"):
                if declared.get(field) != binding[field]:
                    raise PreflightError(
                        f"{name} {successor_name} {field} drifted"
                    )
            row["effective_implementation_sha256_matches"] = True
            row["effective_test_sha256_matches"] = True
        audit[name] = row
    return audit


def _validate_implementation_capabilities() -> dict[str, Any]:
    expected_contracts = {
        "native_features": native_features.SCHEMA_VERSION,
        "predicate_materializer": predicate_materializer.SCHEMA_VERSION,
        "replay_emitter": replay_emitter.REPLAY_EMITTER_SCHEMA_VERSION,
        "posix_cow_shared_prefix": shared_prefix.SCHEMA_VERSION,
        "strict_labels": strict_labels.RUNNER_IDENTITY,
        "label_panel": label_panel.PANEL_IDENTITY,
        "mechanics_receipt": mechanics_receipt.RECEIPT_SCHEMA_VERSION,
        "predicates": predicates.ARTIFACT_SCHEMA,
        "nested_oof": nested_oof.LEARNER_IDENTITY,
        "strict_label_panel_runner": panel_runner.RUNNER_IDENTITY,
        "native_sequence_support_mapping": sequence_support.SCHEMA_VERSION,
        "study": study.STUDY_IDENTITY,
    }
    if any(not str(value).startswith(IDENTITY) for value in expected_contracts.values()):
        raise PreflightError("v2 implementation contract identity drifted")
    if not callable(replay_emitter.CooldownV2ReplayEmitter):
        raise PreflightError("replay assignment snapshot emitter is unavailable")
    if not callable(shared_prefix.PosixCooldownSharedPrefixExecutor):
        raise PreflightError("POSIX copy-on-write executor is unavailable")
    if not callable(predicate_materializer.materialize_predicate_artifacts):
        raise PreflightError("2025 predicate materializer is unavailable")
    if not callable(study.run_study):
        raise PreflightError("multiday nested-OOF study is unavailable")
    if not callable(study._run_continuous_comparator):
        raise PreflightError("continuous-state comparator is unavailable")
    if not callable(sequence_support.build_mapping):
        raise PreflightError("native sequence-support mapping is unavailable")
    if not callable(mechanics_receipt.run_receipt) or not callable(
        mechanics_receipt.validate_receipt
    ):
        raise PreflightError("bounded v11 mechanics receipt runner is unavailable")
    if panel_runner.FEATURE_BLOCK != "M2" or panel_runner.MAX_OPPORTUNITIES is not None:
        raise PreflightError("formal strict-label panel scope drifted")
    if checkpoint.SIMULATOR_STATE_STATUS != "identity_only_not_serialized":
        raise PreflightError("portable simulator serialization status drifted")
    return {
        "contract_identities": expected_contracts,
        "replay_assignment_snapshot_emitter_implemented": True,
        "strict_native_M2_feature_extractor_implemented": True,
        "outcome_blind_2025_predicate_materializer_implemented": True,
        "multiday_nested_oof_study_implemented": True,
        "continuous_state_comparator_implemented": True,
        "native_sequence_support_mapping_implemented": True,
        "posix_copy_on_write_shared_prefix_implemented": True,
        "posix_copy_on_write_available_on_host": os.name == "posix",
        "portable_simulator_state_serialization_implemented": False,
        "portable_checkpoint_restore_authority": False,
        "live_assignment_snapshot_emitter_implemented": False,
        "portable_serialization_blocks_strict_one_shot": False,
        "live_emitter_blocks_strict_one_shot": False,
    }


def _latest_strict_benchmark_queue_audit() -> dict[str, Any]:
    v11_receipt_is_in_search_scope = (
        V11_MECHANICS_RECEIPT_ROOT.parent in STRICT_BENCHMARK_SEARCH_ROOTS
    )
    if v11_receipt_is_in_search_scope and (
        V11_MECHANICS_RECEIPT_ROOT / "_RECEIPT_SUCCESS"
    ).is_file():
        try:
            receipt = mechanics_receipt.validate_receipt(
                V11_MECHANICS_RECEIPT_ROOT
            )
        except Exception as exc:
            return {
                "evidence_found": True,
                "run_path": str(V11_MECHANICS_RECEIPT_ROOT),
                "receipt_identity": mechanics_receipt.RECEIPT_IDENTITY,
                "receipt_schema_version": mechanics_receipt.RECEIPT_SCHEMA_VERSION,
                "denominator_generation_mechanics_verified": False,
                "execution_admission_verified": False,
                "reason": f"bounded v11 receipt validation failed: {exc}",
            }
        receipt_audit = receipt["audit"]
        return {
            "evidence_found": True,
            "run_path": str(V11_MECHANICS_RECEIPT_ROOT),
            "receipt_identity": mechanics_receipt.RECEIPT_IDENTITY,
            "receipt_schema_version": mechanics_receipt.RECEIPT_SCHEMA_VERSION,
            "receipt_sha256": _sha256(
                V11_MECHANICS_RECEIPT_ROOT / "admission_receipt.json"
            ),
            "opportunity_bundle_count": 1,
            "complete_opportunity_bundle_count": 1,
            "strict_exact_opportunity_bundle_count": int(
                receipt_audit["eligible_arm_count"] == 8
            ),
            "arm_count": int(receipt_audit["arm_count"]),
            "unsupported_arm_count": int(
                receipt_audit["unsupported_arm_count"]
            ),
            "redacted_arm_count": int(receipt_audit["redacted_arm_count"]),
            "queue_missing_trace_row_count": int(
                receipt_audit["queue_missing_trace_row_count"]
            ),
            "denominator_generation_mechanics_verified": True,
            "execution_admission_verified": True,
            "aggregate_economic_values_read": False,
            "eligible_arm_point_values_accessed": False,
            "permissions": dict(receipt["permissions"]),
        }

    runs: dict[Path, list[Path]] = {}
    for search_root in STRICT_BENCHMARK_SEARCH_ROOTS:
        if not search_root.is_dir():
            continue
        for run_root in search_root.glob(STRICT_BENCHMARK_DIRECTORY_GLOB):
            if not run_root.is_dir():
                continue
            arm_paths = [
                path
                for path in run_root.rglob("arm-*.json")
                if not any(
                    part.startswith(".")
                    for part in path.relative_to(run_root).parts
                )
            ]
            if arm_paths:
                runs[run_root] = arm_paths
    durable_runs = {
        run_root: arm_paths
        for run_root, arm_paths in runs.items()
        if run_root.parent == DURABLE_BENCHMARK_ROOT
    }
    if durable_runs:
        runs = durable_runs
    if not runs:
        return {
            "evidence_found": False,
            "denominator_generation_mechanics_verified": False,
            "reason": "no strict benchmark arm bundle was found",
        }

    latest_run, arm_paths = max(
        runs.items(),
        key=lambda item: max(path.stat().st_mtime_ns for path in item[1]),
    )
    groups: dict[Path, list[Path]] = {}
    for arm_path in arm_paths:
        groups.setdefault(arm_path.parent, []).append(arm_path)

    all_rows: list[dict[str, Any]] = []
    malformed: list[str] = []
    counter_names = (
        "queue_missing_count",
        "queue_invalidated_order_count",
        "queue_ambiguous_event_count",
        "cancel_trade_ambiguous_order_count",
        "cancel_book_ambiguous_order_count",
    )
    reason_by_counter = {
        "queue_missing_count": "exchange_book_queue_missing_count",
        "queue_invalidated_order_count": (
            "exchange_book_queue_invalidated_order_count"
        ),
        "queue_ambiguous_event_count": (
            "exchange_book_queue_ambiguous_event_count"
        ),
        "cancel_trade_ambiguous_order_count": (
            "exchange_book_cancel_trade_ambiguous_order_count"
        ),
        "cancel_book_ambiguous_order_count": (
            "exchange_book_cancel_book_ambiguous_order_count"
        ),
    }
    bundles: list[dict[str, Any]] = []
    for directory, paths in sorted(groups.items(), key=lambda item: str(item[0])):
        rows: list[dict[str, Any]] = []
        bundle_malformed: list[str] = []
        for path in sorted(paths):
            try:
                payload = _load_json(path)
            except PreflightError:
                malformed.append(str(path))
                bundle_malformed.append(str(path))
                continue
            if payload.get("identity") != IDENTITY:
                malformed.append(str(path))
                bundle_malformed.append(str(path))
                continue
            contract = payload.get("strict_execution_contract")
            if not isinstance(contract, Mapping):
                malformed.append(str(path))
                bundle_malformed.append(str(path))
                continue
            row = {
                "path": str(path),
                "arm_id": str(payload.get("arm_id", "")),
                "strict_native_label_eligible": (
                    contract.get("strict_native_label_eligible") is True
                ),
                "queue_missing_count": int(
                    contract.get("exchange_book_queue_missing_count", 0)
                ),
                "queue_invalidated_order_count": int(
                    contract.get("exchange_book_queue_invalidated_order_count", 0)
                ),
                "queue_ambiguous_event_count": int(
                    contract.get("exchange_book_queue_ambiguous_event_count", 0)
                ),
                "cancel_trade_ambiguous_order_count": int(
                    contract.get(
                        "exchange_book_cancel_trade_ambiguous_order_count", 0
                    )
                ),
                "cancel_book_ambiguous_order_count": int(
                    contract.get(
                        "exchange_book_cancel_book_ambiguous_order_count", 0
                    )
                ),
                "unsupported_reasons": tuple(
                    str(value)
                    for value in contract.get(
                        "strict_native_label_unsupported_reasons", []
                    )
                ),
            }
            rows.append(row)
            all_rows.append(row)

        arm_ids = {row["arm_id"] for row in rows}
        complete_arm_set = bool(
            arm_ids == set(features.BUY_DURATION_POLICY_IDS)
            or arm_ids == set(features.SELL_DURATION_POLICY_IDS)
        )
        fail_closed_rows = True
        for row in rows:
            bad_counters = {
                name for name in counter_names if int(row[name]) > 0
            }
            required_reasons = {reason_by_counter[name] for name in bad_counters}
            unsupported_reasons = set(row["unsupported_reasons"])
            if bad_counters and row["strict_native_label_eligible"]:
                fail_closed_rows = False
            if not required_reasons.issubset(unsupported_reasons):
                fail_closed_rows = False
            if row["strict_native_label_eligible"] and unsupported_reasons:
                fail_closed_rows = False
        totals = {
            name: sum(int(row[name]) for row in rows) for name in counter_names
        }
        exact = bool(
            not bundle_malformed
            and complete_arm_set
            and len(rows) == 8
            and all(value == 0 for value in totals.values())
            and all(row["strict_native_label_eligible"] for row in rows)
        )
        fail_closed = bool(
            not bundle_malformed
            and complete_arm_set
            and len(rows) == 8
            and fail_closed_rows
        )
        bundles.append(
            {
                "bundle_path": str(directory),
                "arm_count": len(rows),
                "complete_side_specific_arm_set": complete_arm_set,
                "queue_totals": totals,
                "unsupported_arm_count": sum(
                    not row["strict_native_label_eligible"] for row in rows
                ),
                "strict_exact_eight_arm_label_eligible": exact,
                "queue_fail_closed_verified": fail_closed,
            }
        )

    totals = {
        name: sum(int(row[name]) for row in all_rows) for name in counter_names
    }
    denominator_mechanics = bool(
        bundles
        and not malformed
        and all(bundle["queue_fail_closed_verified"] for bundle in bundles)
    )
    complete_bundle_count = sum(
        bundle["complete_side_specific_arm_set"] for bundle in bundles
    )
    strict_exact_bundle_count = sum(
        bundle["strict_exact_eight_arm_label_eligible"] for bundle in bundles
    )
    parent_completion = _benchmark_parent_completion_audit(latest_run)
    execution_admission_verified = bool(
        denominator_mechanics
        and complete_bundle_count >= 48
        and strict_exact_bundle_count >= 1
        and parent_completion["verified"]
    )
    return {
        "evidence_found": True,
        "run_path": str(latest_run),
        "opportunity_bundle_count": len(bundles),
        "complete_opportunity_bundle_count": complete_bundle_count,
        "strict_exact_opportunity_bundle_count": strict_exact_bundle_count,
        "arm_count": len(all_rows),
        "malformed_arm_paths": malformed,
        "queue_totals": totals,
        "unsupported_arm_count": sum(
            not row["strict_native_label_eligible"] for row in all_rows
        ),
        "unsupported_reasons": sorted(
            {
                str(reason)
                for row in all_rows
                for reason in row["unsupported_reasons"]
            }
        ),
        "denominator_generation_mechanics_verified": denominator_mechanics,
        "execution_admission_requirements": {
            "minimum_complete_opportunity_bundles": 48,
            "minimum_strict_exact_opportunity_bundles": 1,
            "required_dispatched_side_role_cells": [
                "BUY_opener",
                "BUY_add",
                "SELL_opener",
                "SELL_add",
            ],
            "all_complete_bundles_fail_closed": True,
        },
        "execution_admission_verified": execution_admission_verified,
        "parent_completion_audit": parent_completion,
        "all_bundles_strict_exact": bool(
            bundles
            and all(
                bundle["strict_exact_eight_arm_label_eligible"]
                for bundle in bundles
            )
        ),
        "bundles": bundles,
    }


def _validate_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    if spec.get("identity") != IDENTITY:
        raise PreflightError("v2 identity drifted")
    if spec.get("research_type") != "exploratory_successor":
        raise PreflightError("v2 exploratory research intent drifted")
    predecessor = spec["predecessor_boundary"]
    _require_hash(
        predecessor["v1_spec_path"],
        predecessor["v1_spec_sha256"],
        role="v1 frozen spec",
    )
    _require_hash(
        predecessor["v1_exploratory_report_path"],
        predecessor["v1_exploratory_report_sha256"],
        role="v1 frozen exploratory report",
    )
    baseline = spec["baseline"]
    _require_hash(
        baseline["operational_identity_path"],
        baseline["operational_identity_sha256"],
        role="operational baseline identity",
    )
    _require_hash(
        baseline["config_path"],
        baseline["config_sha256"],
        role="AWS Tokyo baseline config",
    )
    duration = spec["duration_vocabulary"]
    _require_hash(
        duration["source_path"],
        duration["source_sha256"],
        role="outcome-blind duration artifact",
    )
    if tuple(duration["BUY"]) != features.BUY_DURATION_POLICY_IDS:
        raise PreflightError("BUY duration vocabulary drifted")
    if tuple(duration["SELL"]) != features.SELL_DURATION_POLICY_IDS:
        raise PreflightError("SELL duration vocabulary drifted")

    amendments = _validate_amendments(spec)
    component_bindings = _validate_component_bindings(spec, amendments)
    source = spec["source_separation"]["strict_native_2026"]
    panel_path = _require_hash(
        source["panel_spec_path"],
        source["panel_spec_sha256"],
        role="50-day panel spec",
    )
    _require_hash(
        source["strict_spec_path"],
        source["strict_spec_sha256"],
        role="strict-native baseline spec",
    )
    panel = _load_json(panel_path)
    prefix = tuple(spec["ordered_utc_days"]["prefix40"])
    added = tuple(spec["ordered_utc_days"]["added10"])
    if len(prefix) != 40 or len(added) != 10 or len(set((*prefix, *added))) != 50:
        raise PreflightError("v2 40/10/50 denominator is not unique")
    if prefix != tuple(panel["immutable_prefix"]["ordered_utc_days"]):
        raise PreflightError("v2 prefix40 drifted from the current baseline")
    if added != tuple(panel["added_panel"]["ordered_utc_days"]):
        raise PreflightError("v2 added10 drifted from the current baseline")
    reduced = frozenset(str(day) for day in source["reduced_support_days"])
    full_support = tuple(day for day in (*prefix, *added) if day not in reduced)
    if (
        len(reduced) != 9
        or len(full_support) != 41
        or sum(day in prefix for day in full_support) != 33
        or sum(day in added for day in full_support) != 8
    ):
        raise PreflightError("v2 33/8/41 full-support identity drifted")
    scorecard = spec["scorecard"]
    _require_hash(
        scorecard["implementation_path"],
        scorecard["implementation_sha256"],
        role="scorecard v2 implementation",
    )
    score = score_profile_contract(scorecard["profile_id"])
    if score["profile_sha256"] != scorecard["profile_sha256"]:
        raise PreflightError("score profile hash drifted")
    permissions = spec["permissions"]
    forbidden_true = [
        key
        for key in (
            "economic_outcomes_read",
            "development_labels_generated",
            "nested_OOF_run",
            "validation_read",
            "sealed_holdout_read",
            "unified_policy_frozen",
            "repeated_policy_run",
            "transport_passed",
            "research_supported",
            "action_authorized",
            "live_authorized",
        )
        if permissions.get(key) is not False
    ]
    if forbidden_true:
        raise PreflightError(f"v2 locked permissions drifted: {forbidden_true}")
    schema = features.feature_schema()
    if schema["window_contract"]["base_window_width_ns"] != (
        spec["causal_window"]["base_window_width_ns"]
    ):
        raise PreflightError("feature implementation window width drifted")
    if schema["ema_half_lives_s"] != spec["causal_window"]["half_lives_s"]:
        raise PreflightError("feature implementation EMA bank drifted")
    if schema["top_k_depth_levels"] != spec["causal_window"]["top_k_depth_levels"]:
        raise PreflightError("top-k depth identity drifted")
    provider = spec["source_separation"]["provider_2025"]
    source_path = _require_hash(
        provider["day_manifest_path"],
        provider["day_manifest_file_sha256"],
        role="outcome-blind 2025 source manifest",
    )
    manifest = _load_json(source_path)
    source_manifest.validate_manifest(manifest, rehash_sources=False)
    if manifest["canonical_manifest_sha256"] != provider[
        "day_manifest_canonical_sha256"
    ]:
        raise PreflightError("2025 source canonical identity drifted")
    if int(manifest["target_day_count"]) != int(provider["target_day_count"]):
        raise PreflightError("2025 target-day count drifted")
    if manifest["clock_contract"]["book_trade_joint_visibility_authority"]:
        raise PreflightError("2025 source overstated joint clock authority")
    formula = windows.window_formula_contract()
    if formula["top_k_depth_levels"] != spec["causal_window"][
        "top_k_depth_levels"
    ]:
        raise PreflightError("window formula top-k drifted")
    assignment_schema = snapshot.snapshot_schema("M2")
    if assignment_schema["economic_outcomes_allowed"]:
        raise PreflightError("assignment snapshot permits economic outcomes")
    if checkpoint.SIMULATOR_STATE_STATUS != "identity_only_not_serialized":
        raise PreflightError("checkpoint capability was overstated")
    return {
        "prefix40_days": len(prefix),
        "added10_days": len(added),
        "pooled50_days": len((*prefix, *added)),
        "formal_full_support_days": len(full_support),
        "formal_prefix_days": sum(day in prefix for day in full_support),
        "formal_added_days": sum(day in added for day in full_support),
        "score_profile": score,
        "feature_schema": schema,
        "window_formula_contract": formula,
        "assignment_snapshot_schema": assignment_schema,
        "amendments": {
            "feature_semantics": {
                "path": amendments["feature_semantics"]["path"],
                "sha256": amendments["feature_semantics"]["sha256"],
            },
            "execution": {
                "path": amendments["execution"]["path"],
                "sha256": amendments["execution"]["sha256"],
            },
            "execution_v2": {
                "path": amendments["execution_v2"]["path"],
                "sha256": amendments["execution_v2"]["sha256"],
            },
            "execution_v3": {
                "path": amendments["execution_v3"]["path"],
                "sha256": amendments["execution_v3"]["sha256"],
            },
            "execution_v4": {
                "path": amendments["execution_v4"]["path"],
                "sha256": amendments["execution_v4"]["sha256"],
            },
            "execution_v5": {
                "path": amendments["execution_v5"]["path"],
                "sha256": amendments["execution_v5"]["sha256"],
            },
            "execution_v6": {
                "path": amendments["execution_v6"]["path"],
                "sha256": amendments["execution_v6"]["sha256"],
                "sequence_support": amendments["execution_v6"][
                    "sequence_support"
                ],
                "source_union": amendments["execution_v6"]["source_union"],
            },
            "execution_v7": {
                "path": amendments["execution_v7"]["path"],
                "sha256": amendments["execution_v7"]["sha256"],
                "target_receipt_schema": amendments["execution_v7"][
                    "target_receipt_schema"
                ],
            },
        },
        "component_bindings": component_bindings,
        "outcome_blind_2025_source_manifest": {
            "path": str(source_path),
            "file_sha256": _sha256(source_path),
            "canonical_manifest_sha256": manifest[
                "canonical_manifest_sha256"
            ],
            "target_day_count": int(manifest["target_day_count"]),
            "unique_source_day_count": int(manifest["unique_source_day_count"]),
            "book_trade_joint_visibility_authority": False,
        },
    }


def _artifact_readiness() -> dict[str, Any]:
    formal_source_prebuild: dict[str, Any] = {
        "path": str(FORMAL_SOURCE_PREBUILD_MANIFEST),
        "admitted": False,
    }
    if FORMAL_SOURCE_PREBUILD_MANIFEST.is_file():
        try:
            spec = _load_source_json(SPEC, role="v2 frozen Spec")
            formal_days, _, _, _ = panel_runner._formal_day_universe(spec)
            plan = panel_runner._source_union_plan(formal_days, formal=True)
            manifest = _load_json(FORMAL_SOURCE_PREBUILD_MANIFEST)
            panel_runner._validate_prebuild_manifest(
                manifest,
                plan=plan,
                formal=True,
                native_cache=panel_runner.DEFAULT_NATIVE_CACHE,
            )
            formal_source_prebuild.update(
                {
                    "admitted": True,
                    "sha256": _sha256(FORMAL_SOURCE_PREBUILD_MANIFEST),
                    "target_day_count": int(manifest["day_count"]),
                    "prefix_day_count": int(
                        manifest["prefix40_full_support_count"]
                    ),
                    "added_day_count": int(
                        manifest["added10_full_support_count"]
                    ),
                    "source_day_count": int(
                        manifest["unique_source_day_count"]
                    ),
                    "source_hour_count": int(manifest["unique_source_hours"]),
                    "segment_count": int(manifest["segment_count"]),
                    "target_receipt_count": len(manifest["days"]),
                    "strict_zero_counters": dict(
                        manifest["strict_zero_counters"]
                    ),
                    "economic_outcomes_read": bool(
                        manifest["economic_outcomes_read"]
                    ),
                    "arms_run": bool(manifest["arms_run"]),
                }
            )
        except Exception as exc:
            formal_source_prebuild["validation_error"] = str(exc)

    formal_panel: dict[str, Any] = {
        "path": str(FORMAL_PANEL_MANIFEST),
        "admitted": False,
    }
    if FORMAL_PANEL_MANIFEST.is_file():
        try:
            manifest, sources = study._validate_formal_panel_manifest(
                FORMAL_PANEL_MANIFEST
            )
            formal_panel.update(
                {
                    "admitted": True,
                    "sha256": _sha256(FORMAL_PANEL_MANIFEST),
                    "day_count": len(sources),
                    "prefix_day_count": int(
                        manifest["prefix40_full_support_count"]
                    ),
                    "added_day_count": int(
                        manifest["added10_full_support_count"]
                    ),
                }
            )
        except Exception as exc:
            formal_panel["validation_error"] = str(exc)

    predicate_admission: dict[str, Any] = {
        "root": str(PREDICATE_ADMISSION_ROOT),
        "admitted": False,
    }
    candidates = []
    if PREDICATE_ADMISSION_ROOT.is_dir():
        candidates = sorted(
            (
                path
                for path in PREDICATE_ADMISSION_ROOT.iterdir()
                if path.is_dir()
                and not path.name.startswith(".")
                and (path / "manifest.json").is_file()
            ),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
    for candidate in candidates:
        try:
            manifest = predicate_materializer.validate_admission(
                candidate,
                source_manifest_path=predicate_materializer.DEFAULT_SOURCE_MANIFEST,
                rehash_sources=False,
            )
            bundle = manifest["study_predicate_bundle"]
            bundle_path = candidate / str(bundle["path"])
            study._load_predicate_bundle(bundle_path)
            predicate_admission.update(
                {
                    "admitted": True,
                    "path": str(candidate),
                    "manifest_sha256": _sha256(candidate / "manifest.json"),
                    "study_predicate_bundle_path": str(bundle_path),
                    "study_predicate_bundle_sha256": str(bundle["sha256"]),
                }
            )
            break
        except Exception as exc:
            predicate_admission["validation_error"] = str(exc)

    nested: dict[str, Any] = {
        "path": str(STUDY_ADMISSION_ROOT),
        "completed": False,
    }
    if STUDY_ADMISSION_ROOT.is_dir():
        try:
            manifest = study.validate_study_output(STUDY_ADMISSION_ROOT)
            nested.update(
                {
                    "completed": True,
                    "manifest_sha256": _sha256(
                        STUDY_ADMISSION_ROOT / "manifest.json"
                    ),
                    "research_supported": bool(
                        manifest.get("permissions", {}).get(
                            "research_supported", False
                        )
                    ),
                }
            )
        except Exception as exc:
            nested["validation_error"] = str(exc)
    return {
        "formal_source_prebuild": formal_source_prebuild,
        "formal_label_panel": formal_panel,
        "predicate_admission": predicate_admission,
        "nested_oof": nested,
    }


def preflight() -> dict[str, Any]:
    spec = _load_source_json(SPEC, role="v2 frozen Spec")
    audit = _validate_spec(spec)
    capabilities = _validate_implementation_capabilities()
    strict = strict_baseline.preflight()
    if strict.get("days") != 50 or strict.get("strict_complete_days") != 50:
        raise PreflightError("strict-native 50-day source preflight failed")
    benchmark = _latest_strict_benchmark_queue_audit()
    artifacts = _artifact_readiness()
    local_free = shutil.disk_usage(ROOT).free
    orico_root = source_manifest.DEFAULT_DATA_ROOT
    orico_free = shutil.disk_usage(orico_root).free if orico_root.exists() else 0
    component_bindings_verified = all(
        row["executable_binding_verified"]
        for row in audit["component_bindings"].values()
    )
    strict_one_shot_execution_eligible = bool(
        component_bindings_verified
        and capabilities["replay_assignment_snapshot_emitter_implemented"]
        and capabilities["posix_copy_on_write_shared_prefix_implemented"]
        and capabilities["posix_copy_on_write_available_on_host"]
        and strict.get("days") == 50
        and strict.get("strict_complete_days") == 50
    )
    benchmark_fail_closed_verified = bool(
        benchmark.get("denominator_generation_mechanics_verified", False)
    )
    benchmark_execution_admission_verified = bool(
        benchmark.get("execution_admission_verified", False)
        and benchmark.get("receipt_identity")
        == mechanics_receipt.RECEIPT_IDENTITY
        and benchmark.get("receipt_schema_version")
        == mechanics_receipt.RECEIPT_SCHEMA_VERSION
    )
    formal_41_day_labels_admitted = bool(
        artifacts["formal_label_panel"]["admitted"]
    )
    formal_source_prebuild_admitted = bool(
        artifacts["formal_source_prebuild"]["admitted"]
    )
    real_predicate_artifact_bound = bool(
        artifacts["predicate_admission"]["admitted"]
    )
    nested_oof_completed = bool(artifacts["nested_oof"]["completed"])
    strict_label_execution_eligible = bool(
        strict_one_shot_execution_eligible
        and benchmark_execution_admission_verified
    )
    nested_oof_execution_eligible = bool(
        formal_41_day_labels_admitted
        and real_predicate_artifact_bound
    )
    formal_research_execution_eligible = nested_oof_execution_eligible
    permissions = dict(spec["permissions"])
    authority = {
        "research_supported": permissions["research_supported"],
        "action_authorized": permissions["action_authorized"],
        "live_authorized": permissions["live_authorized"],
    }
    if any(authority.values()):
        raise PreflightError("v2 research/action/live authority must remain false")
    return {
        "schema_version": f"{IDENTITY}.preflight.v2",
        "identity": IDENTITY,
        "spec_path": str(SPEC),
        "spec_sha256": _source_identity(SPEC, role="v2 frozen Spec"),
        "contract_audit": audit,
        "strict_native_source_preflight": strict,
        "storage": {
            "local_free_bytes": int(local_free),
            "orico_free_bytes": int(orico_free),
            "large_artifact_destination": str(orico_root),
        },
        "implementation_capabilities": capabilities,
        "latest_strict_benchmark": benchmark,
        "artifacts": artifacts,
        "readiness": {
            "component_bindings_verified": component_bindings_verified,
            "feature_mechanics_eligible": True,
            "multichannel_2025_manifest_bound": True,
            "cooldown_assignment_snapshot_contract_bound": True,
            "cooldown_assignment_snapshot_replay_emitter_bound": True,
            "D_minus_1_D_D_plus_1_source_identity_contract_bound": True,
            "native_sequence_support_mapping_bound": True,
            "source_union_segmentation_v3_bound": True,
            "target_receipt_abi_v3_bound": True,
            "formal_source_prebuild_admitted": (
                formal_source_prebuild_admitted
            ),
            "shared_prefix_checkpoint_metadata_contract_bound": True,
            "posix_copy_on_write_shared_prefix_ready": True,
            "portable_simulator_state_serialization_ready": False,
            "live_assignment_snapshot_emitter_ready": False,
            "strict_one_shot_execution_eligible": (
                strict_one_shot_execution_eligible
            ),
            "strict_label_execution_eligible": (
                strict_label_execution_eligible
            ),
            "latest_strict_benchmark_fail_closed_verified": (
                benchmark_fail_closed_verified
            ),
            "latest_strict_benchmark_execution_admission_verified": (
                benchmark_execution_admission_verified
            ),
            "formal_41_day_labels_admitted": formal_41_day_labels_admitted,
            "real_predicate_artifact_bound": real_predicate_artifact_bound,
            "nested_oof_completed": nested_oof_completed,
            "nested_oof_execution_eligible": nested_oof_execution_eligible,
            "formal_research_execution_eligible": (
                formal_research_execution_eligible
            ),
        },
        "blockers": [
            *(
                []
                if benchmark_execution_admission_verified
                else [
                    "generate and atomically admit the bounded v11 strict-native "
                    "mechanics receipt for one full-support opportunity and its "
                    "complete side-specific eight-arm bundle"
                ]
            ),
            *(
                []
                if formal_41_day_labels_admitted
                else ["generate and atomically admit the frozen 41-day strict-native labels"]
            ),
            *(
                []
                if real_predicate_artifact_bound
                else ["fit and bind real predicate artifacts from the frozen 2025 reference populations"]
            ),
            *(
                []
                if nested_oof_completed
                else ["run nested chronological OOF after labels and predicate artifacts are frozen"]
            ),
        ],
        "non_blocking_missing_capabilities": [
            "portable simulator-state serialization and restore",
            "live CooldownAssignmentSnapshotV2 emitter",
        ],
        "authority": authority,
        "permissions": permissions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()
    payload = preflight()
    if not args.no_write:
        _atomic_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
