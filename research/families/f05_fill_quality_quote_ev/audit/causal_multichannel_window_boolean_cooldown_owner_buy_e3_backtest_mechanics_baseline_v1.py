"""Executable, fail-closed BUY-E3 backtest mechanics baseline.

This module is deliberately narrower than an operational-baseline pointer.  It
binds the exact owner-selected BUY E3 bytes, the exact active configuration,
the frozen 30-day Development mechanics evidence, and the existing repeated
policy ABI.  It grants mechanics/config replay availability only.  It neither
reads economic outputs nor grants research, action, live, occurrence, or
promotion authority.

Private locators are caller supplied.  No host-absolute private path or
private byte is a module constant, a public default, or a returned public
identity.  The one fixed bundle locator below is relative to the owner-private
trust root and therefore reveals no host topology.
"""

from __future__ import annotations

import copy
import errno
import fcntl
import hashlib
import importlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Final

import yaml

from live import config as live_config
from models import backtest_config
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_repeated_policy_v1 as repeated_policy,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_amendment_v2 as parity_v2,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity_v1,
)

IDENTITY: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_backtest_mechanics_baseline_v1"
)
SCHEMA_VERSION: Final = f"{IDENTITY}.contract.v1"
DAY_OVERLAY_SCHEMA: Final = f"{IDENTITY}.day_overlay.v1"
V14_GOVERNANCE_SCHEMA: Final = f"{IDENTITY}.v14_mechanics_governance_receipt.v1"
V14_PRIVATE_MANIFEST_SCHEMA: Final = f"{IDENTITY}.v14_private_bundle_manifest.v1"
V14_PRIVATE_TRANSACTION_SCHEMA: Final = f"{IDENTITY}.v14_private_transaction.v1"
OWNER_PRIVATE_INPUT_SCHEMA: Final = f"{IDENTITY}.owner_private_relative_inputs.v1"
OWNER_PRIVATE_INPUT_COUNT: Final = 12
PRIVATE_EVIDENCE_ROOT_ENV: Final = "NARROWGATE_PRIVATE_EVIDENCE_ROOT"
METADATA_REPOSITORY_ROOT_ENV: Final = "NARROWGATE_METADATA_REPOSITORY_ROOT"
FORMAL_PRIVATE_BUNDLE_RELATIVE: Final = PurePosixPath(
    "direct_no_shadow_live_evidence_v6_20260824/backtest_mechanics_default_v1"
)
V14_COLD_PUBLISHER_TAG: Final = "f05-owner-buy-e3-backtest-mechanics-source-v1-final-20260825"
V14_EFFECTIVE_AT_UTC: Final = "2026-08-25T00:00:01Z"
V14_PUBLIC_CONTRACT_FILE_SHA256: Final = (
    "36daa37cd381448a6e306847150e4c76579f0f8653ca0c15491f399086c90699"
)
V13_PREDECESSOR_FILE_SHA256: Final = (
    "1767d53713f2f02fe49b93e0f37d9a65b46ea4c470cf35f0417646f1e9281079"
)
V13_RECONCILIATION_VALIDATOR_SHA256: Final = (
    "f93d8215b8c55f5128b136a900c38e47a17092b6c94b00784668f495619bc59b"
)

PREDECESSOR_V12_CONFIG_FILE_SHA256: Final = (
    "800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85"
)
ACTIVE_SOURCE_CONFIG_FILE_SHA256: Final = (
    "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
)
ACTIVE_CONFIG_CANONICAL_MAPPING_SHA256: Final = (
    "f5cb3d238edfdff4c2353e2d7cd5c2d948d36ef0be1e7733ce0dda73ea7a21c2"
)
HOST_NEUTRAL_CONFIG_MAPPING_SHA256: Final = (
    "bbfff858a35a387abf8b38b19d44cc4f60414d21d91d63de0a9613e15f665a05"
)
HOST_NEUTRAL_CONFIG_FILE_SHA256: Final = (
    "7b67b51197d93b99b04da323e02d3d970d7179ef4740720cc596a8ea3695a53f"
)
HOST_NEUTRAL_CONFIG_SIZE_BYTES: Final = 16_175

EXACT_E3_ARTIFACT_SHA256: Final = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
EXACT_E3_FILE_SHA256: Final = MappingProxyType(
    {
        "manifest": "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929",
        "policy": "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b",
        "predicate_bundle": ("4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915"),
    }
)
EXACT_E3_SIZE_BYTES: Final = MappingProxyType(
    {"manifest": 3_528, "policy": 236_318, "predicate_bundle": 57_418}
)
EXACT_B0_POLICY_FILE_SHA256: Final = (
    "877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29"
)
EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256: Final = (
    "ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37"
)
EXACT_B0_PREDICATE_BUNDLE_CANONICAL_SHA256: Final = (
    "96d66218318f4dda7377082691482b15e586f73be985bdcec7912d1a39e94aa8"
)

LEARNING_ALGORITHM_ARTIFACT_SHA256: Final = (
    "de056921335450619f7d8099d545125f1d7d6045ebc448dc2526e63c4cb72072"
)

FORMAL_E3_MECHANICS_DAYS: Final = (
    "2026-06-27",
    "2026-06-28",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
    "2026-07-11",
    "2026-07-12",
    "2026-07-13",
    "2026-07-14",
    "2026-07-15",
    "2026-07-17",
    "2026-07-18",
    "2026-07-19",
    "2026-07-20",
    "2026-07-21",
    "2026-07-22",
    "2026-07-23",
    "2026-07-24",
    "2026-07-25",
    "2026-07-26",
    "2026-07-27",
    "2026-07-28",
    "2026-07-29",
    "2026-07-30",
    "2026-07-31",
    "2026-08-01",
    "2026-08-02",
    "2026-08-03",
    "2026-08-04",
    "2026-08-05",
)

SOURCE_CONFIG_DELTA_PATHS: Final = (
    "external_venues.enabled",
    "lifecycle_journal_v2.baseline_identity_sha256",
    "multi_market.global_flow_shadow_enabled",
    "multi_market.global_reference_shadow_enabled",
    "strategy.buy_e3_cooldown_artifact_manifest_path",
    "strategy.buy_e3_cooldown_artifact_manifest_sha256",
    "strategy.buy_e3_cooldown_artifact_sha256",
    "strategy.buy_e3_cooldown_ema_warmup_s",
    "strategy.buy_e3_cooldown_evidence_route",
    "strategy.buy_e3_cooldown_policy_enabled",
    "strategy.buy_e3_cooldown_policy_path",
    "strategy.buy_e3_cooldown_policy_sha256",
    "strategy.buy_e3_cooldown_predicate_bundle_path",
    "strategy.buy_e3_cooldown_predicate_bundle_sha256",
    "strategy.dynamic_fill_hazard_shadow_enabled",
)
SOURCE_CONFIG_DELTA_PATHS_SHA256: Final = (
    "be2c9553007f9ad1f03a7a8b291d13ea0ad2929b97d2a15a01484aab9db474d6"
)
REPLAY_ABI_SOURCE_DELTA_PATHS: Final = (
    "buy_e3_cooldown_artifact_manifest_path",
    "buy_e3_cooldown_artifact_manifest_sha256",
    "buy_e3_cooldown_artifact_sha256",
    "buy_e3_cooldown_policy_enabled",
    "buy_e3_cooldown_policy_path",
    "buy_e3_cooldown_policy_sha256",
    "buy_e3_cooldown_predicate_bundle_path",
    "buy_e3_cooldown_predicate_bundle_sha256",
    "dynamic_fill_hazard_shadow_enabled",
)
REPLAY_ABI_SOURCE_DELTA_PATHS_SHA256: Final = (
    "bcd3458d9b5a28e0fe24cf24c4c422a3a36cbb152913ff2759e9abf4fa9df2eb"
)
REPLAY_ABI_FINAL_DELTA_PATHS: Final = REPLAY_ABI_SOURCE_DELTA_PATHS[:-1]
REPLAY_ABI_FINAL_DELTA_PATHS_SHA256: Final = (
    "afaf618aca694bd1f6f96fc84243517640a71a47d859ae110698b1fc661cf07f"
)
E3_CONFIG_DELTA_PATHS: Final = tuple(
    path for path in SOURCE_CONFIG_DELTA_PATHS if path.startswith("strategy.buy_e3_")
)
DIAGNOSTIC_DISABLE_DELTA_PATHS: Final = (
    "external_venues.enabled",
    "multi_market.global_flow_shadow_enabled",
    "multi_market.global_reference_shadow_enabled",
    "strategy.dynamic_fill_hazard_shadow_enabled",
)
WRITER_IDENTITY_DELTA_PATHS: Final = ("lifecycle_journal_v2.baseline_identity_sha256",)

HOST_NEUTRAL_MUTATIONS: Final = MappingProxyType(
    {
        "strategy.boolean_cooldown_policy_path": (
            "narrowgate-private://exact-owner-b0/policy.json"
        ),
        "strategy.boolean_cooldown_predicate_bundle_path": (
            "narrowgate-private://exact-owner-b0/predicate_bundle.json"
        ),
        "strategy.buy_e3_cooldown_artifact_manifest_path": (
            "narrowgate-private://exact-owner-buy-e3/artifact_manifest.json"
        ),
        "strategy.buy_e3_cooldown_policy_path": (
            "narrowgate-private://exact-owner-buy-e3/policy.json"
        ),
        "strategy.buy_e3_cooldown_predicate_bundle_path": (
            "narrowgate-private://exact-owner-buy-e3/predicate_bundle.json"
        ),
        "lifecycle_journal_v2.enabled": False,
        "lifecycle_journal_v2.required_mount": ("/narrowgate/replay/f05-buy-e3-baseline-v1"),
        "lifecycle_journal_v2.root": ("/narrowgate/replay/f05-buy-e3-baseline-v1/order-lifecycle"),
        "lifecycle_journal_v2.prospective_epoch_root": (
            "/narrowgate/replay/f05-buy-e3-baseline-v1/prospective-epochs"
        ),
        "lifecycle_journal_v2.remote_spool_allowlisted_roots": [
            "/narrowgate/replay/f05-buy-e3-baseline-v1"
        ],
        "lifecycle_journal_v2.baseline_identity_path": (
            "narrowgate-public://operational-baseline/v13"
        ),
        "lifecycle_journal_v2.baseline_identity_sha256": (
            "1767d53713f2f02fe49b93e0f37d9a65b46ea4c470cf35f0417646f1e9281079"
        ),
    }
)
HOST_NEUTRAL_CHANGED_PATHS: Final = tuple(HOST_NEUTRAL_MUTATIONS)

RESEARCH_ACTION_FLAGS: Final = (
    "buy_soft_widen_release_probe_enabled",
    "conditional_p3_reach_gate_enabled",
    "conditional_p3_reach_budget_policy_enabled",
    "cooldown_duration_fork_enabled",
    "ema_add_wait_fork_enabled",
    "local_action_ope_enabled",
    "queue_value_keep_cancel_enabled",
    "safe_add_rearm_randomized_enabled",
    "sell_add_skip_ope_enabled",
    "state_conditioned_quote_policy_enabled",
    "state_conditioned_rearm_enabled",
    "variance_time_lineage_randomized_enabled",
)
REPLAY_ENGINE_OVERLAY: Final = MappingProxyType(
    {
        "ml_enabled": True,
        "execution_trade_source": "trades",
        "market_context_warmup_days": 1,
        "replay_event_clock": "merged",
        "queue_ahead_mode": "exact_level",
        "queue_l2_cancel_ahead_enabled": False,
        "exchange_book_queue_mode": "disabled",
        "collect_curves": False,
        "dynamic_fill_hazard_action_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": False,
        "dynamic_fill_hazard_cpp_parity_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "sync_adjust_replay_mode": "disabled",
        "markout_side_asymmetry_sign": 1.0,
        **{name: False for name in RESEARCH_ACTION_FLAGS},
    }
)
REPLAY_ENGINE_OVERLAY_SHA256: Final = (
    "1a05187343aa57fa2924bb78068126c2487177bf93e40bd335331608ce89eea2"
)
EXACT_ACTIVE_REPLAY_ABI_SHA256: Final = (
    "6a15b4d7933dfe036efed649cd6220667d45f45073a66eab4651461031291337"
)

AVAILABILITY: Final = MappingProxyType(
    {"mechanics_replay_available": True, "config_replay_available": True}
)
PERMISSIONS: Final = MappingProxyType(
    {
        "economic_authority": False,
        "research_authority": False,
        "action_authority": False,
        "live_authority": False,
        "nonbaseline_occurrence_authority": False,
        "promotion_authority": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "economic_values_read": False,
        "economic_values_exposed": False,
        "hypothetical_live_scoring": False,
        "shadow_or_companion_authority": False,
    }
)
V14_PERMISSIONS: Final = MappingProxyType(
    {
        "backtest_mechanics_available": True,
        "backtest_default_arm_resolution_authorized": True,
        **dict(PERMISSIONS),
    }
)

V13_SCHEMA_VERSION: Final = "narrowgate_operational_baseline_identity.v13"
V13_BASELINE_ID: Final = "btc_usdc_live_v6_no_shadow_backtest_v12_split_20260825"
V13_EFFECTIVE_AT_UTC: Final = "2026-08-25T00:00:00Z"
V13_OPERATIONAL_STATUS: Final = "active_split_live_and_backtest_authority_locator"
V13_PROMOTION_CLASS: Final = (
    "governance_locator_reconciliation_no_new_strategy_or_economic_authority"
)
V13_PERMISSIONS: Final = MappingProxyType(
    {
        "operational_baseline_active": True,
        "governance_locator_publication_authorized": True,
        "baseline_promotion_authorized": False,
        "backtest_default_control_authorized": True,
        "current_live_authority_granted_by_this_identity": False,
        "private_release_authority_required": True,
        "research_prediction_authority": False,
        "research_live_authority": False,
        "research_action_experiment_authorized": False,
        "buy_e3_backtest_economic_authority": False,
        "buy_e3_nonbaseline_action_occurrence_authority": False,
        "historical_v12_backtest_control_rewritten": False,
    }
)

PARITY_RECEIPT_BINDINGS: Final = MappingProxyType(
    {
        parity_v1.RESEARCH_COMPILED_LAYER: {
            "file_sha256": ("7629b2667a4b59d1a4967d8e87e4dfd2e975ea483302675ab4e0c87dc0042c56"),
            "canonical_sha256": (
                "86558c59c0156ec60f2f5dcff6934fdd7f9b59594ba8cd667acd12253e321297"
            ),
        },
        parity_v1.DEVELOPMENT_SNAPSHOT_LAYER: {
            "file_sha256": ("875b06cd558e9628e8338f7d9b3818f00c11a162e03acfba58697086a4625dc3"),
            "canonical_sha256": (
                "758d97f3f4ca208f6ad1a319b0f42659eb2ca3bb7a68de8413542cd3b533df6c"
            ),
        },
        parity_v1.STREAMING_OFFLINE_LAYER: {
            "file_sha256": ("478a46853bd484ccc325eb17400d107d16d9eef7280e6c2072abaa65fb0654c2"),
            "canonical_sha256": (
                "1adc0f03a9844b8605281e6e7d1fddeba0b83262ca44539d42d98148b7936ec6"
            ),
        },
    }
)
LAYER4_CONTRACT_FILE_SHA256: Final = (
    "5a331486a343814ec33b7eb00b294a5af341f21a17fafaeda4a77696807013f7"
)
LAYER4_CONTRACT_CANONICAL_SHA256: Final = (
    "df82ffde7fa488590a9c350c1b8afe50c9fff3a07351afe156bdc2af53469b53"
)
LAYER4_FINAL_FILE_SHA256: Final = "e0de72be169092a04bbc3231e59c122f3db287ac9c6725885bb856d22e107c7b"
LAYER4_FINAL_CANONICAL_SHA256: Final = (
    "c3277ae190cd1211f7039cf35174355f2aa01bfaf48b253ea93ce02e292d9505"
)
LAYER4_DAY_RECEIPTS_SHA256: Final = (
    "127db7b4b967f669718cdb65f4e1bc25bb9652eb18a50cf26b9002fb124ef8e4"
)

OWNER_PRIVATE_INPUT_ROLES: Final = (
    "v13_reconciliation_manifest",
    "predecessor_v12_config",
    "active_source_config",
    "e3_artifact_manifest",
    "e3_policy",
    "e3_predicate_bundle",
    "b0_policy",
    "b0_predicate_bundle",
    "parity_research_compiled",
    "parity_development_snapshot",
    "parity_streaming_offline",
    "amended_layer4_root",
)
OWNER_METADATA_INPUT_ROLES: Final = frozenset(
    {"predecessor_v12_config", "b0_policy", "b0_predicate_bundle"}
)
OWNER_PRIVATE_FILE_SHA256: Final = MappingProxyType(
    {
        "predecessor_v12_config": PREDECESSOR_V12_CONFIG_FILE_SHA256,
        "active_source_config": ACTIVE_SOURCE_CONFIG_FILE_SHA256,
        "e3_artifact_manifest": EXACT_E3_FILE_SHA256["manifest"],
        "e3_policy": EXACT_E3_FILE_SHA256["policy"],
        "e3_predicate_bundle": EXACT_E3_FILE_SHA256["predicate_bundle"],
        "b0_policy": EXACT_B0_POLICY_FILE_SHA256,
        "b0_predicate_bundle": EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256,
        "parity_research_compiled": PARITY_RECEIPT_BINDINGS[parity_v1.RESEARCH_COMPILED_LAYER][
            "file_sha256"
        ],
        "parity_development_snapshot": PARITY_RECEIPT_BINDINGS[
            parity_v1.DEVELOPMENT_SNAPSHOT_LAYER
        ]["file_sha256"],
        "parity_streaming_offline": PARITY_RECEIPT_BINDINGS[parity_v1.STREAMING_OFFLINE_LAYER][
            "file_sha256"
        ],
    }
)

RUNTIME_SOURCE_SHA256: Final = MappingProxyType(
    {
        "live/config.py": ("9160b8884e877e4230efee1505d569dbf349c6e4e41e4f95192e95b95b3df425"),
        "live/runtime_policy.py": (
            "23bf62c1e0bfdd0bcc94ef203d39e22f61f9296bf3545157c373ca4f45912964"
        ),
        "models/backtest_config.py": (
            "13833cf65f6245539d72e318c8ebc66d047d60e2df28dc5b6c66d514f18becdc"
        ),
        "models/backtest_tick.py": (
            "55f3a64572444ad0ea26bdbf0525b691b3bba74d9a602eae35af2ca29ea72c0f"
        ),
        "strategy/boolean_cooldown_buy_e3.py": (
            "85cd44c6695caa3f50942b2dc1cf489f6d1af113db53cd07b891d44d1ccfaf94"
        ),
        "strategy/boolean_cooldown_live.py": (
            "7802eb19973b21a0e1051ae6ec252ec63e9949f42cafe4c2b08e329c054fc113"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_replay_emitter.py": (
            "51784343a37635ca813023df8f8c494c75a37aa943baba7c96b0c6c5250cefcb"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_replay_adapter_v1.py": (
            "15f201b200fb4c4928cbc086e9ef22272d05d588109bfe4cd2acd94fb14edd5d"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_panel_builder_v1.py": (
            "1cbaf779d316e08465aab9cdbb05491f11a5fcf97336a9f8a0050efd78f6bd17"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_native_observation_cache.py": (
            "80ed5ce1ba15dcfd4edc5e98857eea302cb85f77fac3570623d7d9e2de90c194"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "repeated_policy_v1.py": (
            "9516cdd3bef17525bf78a9f4a468c24507af46440575df58b972569be2842f16"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_predicate_view_v1.py": (
            "646a1728fedd1ffadcb5891bb6a545610b1a4131bb955afd6490194d5b1ff2f2"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1.py": (
            "01485964342fd35d048be0e44e0d78d37629eb14fafa2a41b4314d103c1f41c8"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_runtime_policy.py": (
            "c7b118793c374c19e4f5d54d6a4e4313d94dba7e2f32291e0e166ddacd993a5f"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py": (
            "8c66a339820ade7a9b3fcc4bb0e6cce97c87a79fd1f5323576759936af5a66b4"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "parity_amendment_v2.py": (
            "ad8b1e21cdcd2b11d54e6f668c4fb3876f36c08bfc8cf037f33277afb2d37741"
        ),
    }
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_PRIVATE_BYTES = 32 << 20


class OwnerBuyE3MechanicsBaselineError(RuntimeError):
    """Raised when any immutable mechanics-baseline binding does not close."""


@dataclass(frozen=True, slots=True)
class ExactE3ArtifactPaths:
    manifest: Path
    policy: Path
    predicate_bundle: Path


@dataclass(frozen=True, slots=True)
class ExactB0ArtifactPaths:
    policy: Path
    predicate_bundle: Path


@dataclass(frozen=True, slots=True)
class ParityEvidencePaths:
    research_compiled: Path
    development_snapshot: Path
    streaming_offline: Path
    layer4_contract: Path
    layer4_final: Path
    layer4_day_receipt_dir: Path


@dataclass(frozen=True, slots=True)
class OwnerPrivateInputs:
    v13_reconciliation_manifest: Path
    predecessor_v12_config: Path
    active_source_config: Path
    e3_artifact_paths: ExactE3ArtifactPaths
    b0_artifact_paths: ExactB0ArtifactPaths
    parity_evidence_paths: ParityEvidencePaths
    relative_locators: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class _Snapshot:
    data: bytes
    sha256: str
    size_bytes: int
    document: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ParityEvidenceBinding:
    synthetic_receipts: Mapping[str, Mapping[str, str]]
    layer4_contract_file_sha256: str
    layer4_contract_canonical_sha256: str
    layer4_final_file_sha256: str
    layer4_final_canonical_sha256: str
    layer4_day_receipts_sha256: str
    formal_e3_mechanics_panel_days: tuple[str, ...]
    reduced_support: bool = True


@dataclass(frozen=True, slots=True)
class DayMechanicsOverlay:
    utc_day: str
    continuation_day: str
    target_start_ns: int
    target_cutoff_ns: int
    params: Mapping[str, Any]
    snapshot_emitter: Any
    compiled_evaluator: Any
    identity_hashes: Mapping[str, str]
    receipt: Mapping[str, Any]

    def backtest_tick_params_overlay(self) -> dict[str, Any]:
        """Return the existing repeated-policy ABI fields plus trace capacity."""

        return {
            "cooldown_v2_snapshot_emitter": self.snapshot_emitter,
            "cooldown_duration_policy_evaluator": self.compiled_evaluator,
            "trace_cooldown_duration_opportunities_max": int(
                self.params["trace_cooldown_duration_opportunities_max"]
            ),
        }


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise OwnerBuyE3MechanicsBaselineError("value is not canonical JSON") from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def document_sha256(value: Mapping[str, Any], field: str) -> str:
    body = dict(value)
    body.pop(field, None)
    return canonical_sha256(body)


def _require_sha(value: Any, label: str) -> str:
    digest = str(value).strip().lower()
    if _SHA_RE.fullmatch(digest) is None:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not a lowercase SHA256")
    return digest


def _strict_utc_z(value: Any, *, label: str) -> str:
    text = str(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", text) is None:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not canonical UTC-Z")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not canonical UTC-Z") from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != text:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not canonical UTC-Z")
    return text


def _identity(stat_result: os.stat_result) -> tuple[int, ...]:
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_mode,
        stat_result.st_nlink,
        stat_result.st_uid,
        stat_result.st_gid,
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
    )


def _lexical_parts(path: Path, *, label: str) -> tuple[str, ...]:
    raw = os.fspath(path)
    if "\x00" in raw or not raw.startswith("/"):
        raise OwnerBuyE3MechanicsBaselineError(f"{label} path must be absolute")
    parts = raw.split("/")[1:]
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise OwnerBuyE3MechanicsBaselineError(f"{label} path is not lexical-canonical")
    return tuple(parts)


def _secure_snapshot(
    path: Path,
    *,
    expected_sha256: str | None,
    label: str,
    expected_size: int | None = None,
    require_mode_0600: bool = True,
    require_trusted_parent: bool | None = None,
    expected_mode: int | None = None,
    max_bytes: int = _MAX_PRIVATE_BYTES,
) -> _Snapshot:
    """Read once through lexical no-follow dirfds and rebind every path edge."""

    expected = (
        _require_sha(expected_sha256, f"{label} expected SHA256")
        if expected_sha256 is not None
        else None
    )
    parts = _lexical_parts(Path(path), label=label)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptors: list[int] = []
    edges: list[tuple[int, str, tuple[int, ...]]] = []
    try:
        current = os.open("/", directory_flags)
        descriptors.append(current)
        for component in parts[:-1]:
            try:
                child = os.open(component, directory_flags, dir_fd=current)
            except FileNotFoundError as exc:
                raise OwnerBuyE3MechanicsBaselineError(f"{label} is missing") from exc
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise OwnerBuyE3MechanicsBaselineError(
                        f"{label} has a symlink or non-directory ancestor"
                    ) from exc
                raise
            child_identity = _identity(os.fstat(child))
            edges.append((current, component, child_identity))
            descriptors.append(child)
            current = child
        name = parts[-1]
        try:
            descriptor = os.open(name, file_flags, dir_fd=current)
        except FileNotFoundError as exc:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} is missing") from exc
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise OwnerBuyE3MechanicsBaselineError(
                    f"{label} is a symlink or unsafe file"
                ) from exc
            raise
        descriptors.append(descriptor)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} must be a single-link regular file")
        observed_mode = stat.S_IMODE(before.st_mode)
        if expected_mode is not None and observed_mode != expected_mode:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} mode must be {expected_mode:04o}")
        if require_mode_0600 and observed_mode != 0o600:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} mode must be 0600")
        if before.st_uid != os.getuid():
            raise OwnerBuyE3MechanicsBaselineError(f"{label} owner is not the current uid")
        parent_metadata = os.fstat(current)
        trusted_parent = (
            require_mode_0600 if require_trusted_parent is None else bool(require_trusted_parent)
        )
        if trusted_parent and (
            parent_metadata.st_uid != os.getuid() or stat.S_IMODE(parent_metadata.st_mode) & 0o022
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                f"{label} private trust root owner or permissions are unsafe"
            )
        if before.st_size <= 0 or before.st_size > max_bytes:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} size is outside bounds")
        if expected_size is not None and before.st_size != int(expected_size):
            raise OwnerBuyE3MechanicsBaselineError(f"{label} size drifted")
        blocks: list[bytes] = []
        observed = 0
        while True:
            block = os.read(descriptor, min(1 << 20, max_bytes + 1 - observed))
            if not block:
                break
            observed += len(block)
            if observed > max_bytes:
                raise OwnerBuyE3MechanicsBaselineError(f"{label} exceeded size bound")
            blocks.append(block)
        after = os.fstat(descriptor)
        if _identity(before) != _identity(after) or observed != before.st_size:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} changed during read")
        try:
            lexical_final = os.stat(name, dir_fd=current, follow_symlinks=False)
        except OSError as exc:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} path changed during read") from exc
        if _identity(lexical_final) != _identity(before):
            raise OwnerBuyE3MechanicsBaselineError(f"{label} path changed during read")
        for parent_fd, component, expected_identity in edges:
            try:
                observed_edge = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise OwnerBuyE3MechanicsBaselineError(
                    f"{label} ancestor changed during read"
                ) from exc
            if _identity(observed_edge) != expected_identity:
                raise OwnerBuyE3MechanicsBaselineError(f"{label} ancestor changed during read")
        data = b"".join(blocks)
        digest = hashlib.sha256(data).hexdigest()
        if expected is not None and digest != expected:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} SHA256 drifted")
        return _Snapshot(data=data, sha256=digest, size_bytes=len(data))
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise OwnerBuyE3MechanicsBaselineError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _parse_strict_json(data: bytes, *, label: str) -> Mapping[str, Any]:
    def reject_constant(value: str) -> Any:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} has non-finite JSON: {value}")

    try:
        value = json.loads(
            data.decode("ascii"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not strict ASCII JSON") from exc
    if not isinstance(value, Mapping):
        raise OwnerBuyE3MechanicsBaselineError(f"{label} JSON root is not an object")
    return dict(value)


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: yaml.SafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in output:
            raise OwnerBuyE3MechanicsBaselineError(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _parse_strict_yaml(data: bytes, *, label: str) -> Mapping[str, Any]:
    try:
        value = yaml.load(data.decode("utf-8"), Loader=_UniqueKeySafeLoader)
    except (UnicodeError, yaml.YAMLError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is not strict YAML") from exc
    if not isinstance(value, Mapping):
        raise OwnerBuyE3MechanicsBaselineError(f"{label} YAML root is not a mapping")
    _canonical_bytes(value)
    return dict(value)


def _mapping_difference_paths(left: Any, right: Any, prefix: str = "") -> list[str]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        differences: list[str] = []
        for key in sorted(set(left) | set(right), key=str):
            child = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                differences.append(child)
            else:
                differences.extend(_mapping_difference_paths(left[key], right[key], child))
        return differences
    return [prefix] if left != right else []


def _set_nested(mapping: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cursor: dict[str, Any] = mapping
    for part in parts[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            raise OwnerBuyE3MechanicsBaselineError(f"projection locator is missing: {path}")
        cursor = child
    if parts[-1] not in cursor:
        raise OwnerBuyE3MechanicsBaselineError(f"projection locator is missing: {path}")
    cursor[parts[-1]] = copy.deepcopy(value)


def _has_nested(mapping: Mapping[str, Any], path: str) -> bool:
    cursor: Any = mapping
    for part in path.split("."):
        if not isinstance(cursor, Mapping) or part not in cursor:
            return False
        cursor = cursor[part]
    return True


def _assert_host_neutral_projection(
    projected: Mapping[str, Any], *, require_all_mutations: bool = True
) -> None:
    """Reject host absolute paths and unresolved private artifact locators."""

    invalid: list[str] = []

    def visit(value: Any, path: str = "") -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                visit(child, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{path}[{index}]")
        elif isinstance(value, str):
            if (
                (value.startswith("/") and not value.startswith("/narrowgate/replay/"))
                or value.startswith("~")
                or re.match(r"^[A-Za-z]:[\\/]", value)
                or value.startswith("models/private/")
                or value.startswith("live/private/")
            ):
                invalid.append(path)

    visit(projected)
    if invalid:
        raise OwnerBuyE3MechanicsBaselineError(
            "host-neutral projection retains unsafe locator(s): " + ", ".join(sorted(invalid))
        )
    applicable = {
        path: value
        for path, value in HOST_NEUTRAL_MUTATIONS.items()
        if require_all_mutations or _has_nested(projected, path)
    }
    if require_all_mutations and len(applicable) != len(HOST_NEUTRAL_MUTATIONS):
        raise OwnerBuyE3MechanicsBaselineError("host-neutral projection mutation binding drifted")
    if any(_nested_value(projected, path) != value for path, value in applicable.items()):
        raise OwnerBuyE3MechanicsBaselineError("host-neutral projection mutation binding drifted")


def _nested_value(mapping: Mapping[str, Any], path: str) -> Any:
    cursor: Any = mapping
    for part in path.split("."):
        if not isinstance(cursor, Mapping) or part not in cursor:
            raise OwnerBuyE3MechanicsBaselineError(f"projection locator is missing: {path}")
        cursor = cursor[part]
    return cursor


def _project_host_neutral_config(source: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
    projected = copy.deepcopy(dict(source))
    journal = projected.get("lifecycle_journal_v2")
    if not isinstance(journal, Mapping) or journal.get("enabled") is not True:
        raise OwnerBuyE3MechanicsBaselineError(
            "active config lacks the expected enabled lifecycle writer"
        )
    for path, value in HOST_NEUTRAL_MUTATIONS.items():
        _set_nested(projected, path, value)
    changed = tuple(_mapping_difference_paths(source, projected))
    if changed != tuple(sorted(HOST_NEUTRAL_CHANGED_PATHS)):
        raise OwnerBuyE3MechanicsBaselineError(
            f"host-neutral projection escaped allowlist: {changed}"
        )
    _assert_host_neutral_projection(projected)
    if canonical_sha256(projected) != HOST_NEUTRAL_CONFIG_MAPPING_SHA256:
        raise OwnerBuyE3MechanicsBaselineError("host-neutral config mapping SHA256 drifted")
    serialized = (
        json.dumps(
            projected,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )
    if (
        len(serialized) != HOST_NEUTRAL_CONFIG_SIZE_BYTES
        or hashlib.sha256(serialized).hexdigest() != HOST_NEUTRAL_CONFIG_FILE_SHA256
    ):
        raise OwnerBuyE3MechanicsBaselineError("host-neutral config file identity drifted")
    return projected, serialized


def _validate_source_config(source: Mapping[str, Any]) -> None:
    if canonical_sha256(source) != ACTIVE_CONFIG_CANONICAL_MAPPING_SHA256:
        raise OwnerBuyE3MechanicsBaselineError("active config canonical mapping drifted")
    strategy = source.get("strategy")
    multi = source.get("multi_market")
    external = source.get("external_venues")
    if not isinstance(strategy, Mapping) or not isinstance(multi, Mapping):
        raise OwnerBuyE3MechanicsBaselineError("active config strategy mapping is missing")
    expected = {
        "buy_e3_cooldown_policy_enabled": True,
        "buy_e3_cooldown_artifact_manifest_sha256": EXACT_E3_FILE_SHA256["manifest"],
        "buy_e3_cooldown_artifact_sha256": EXACT_E3_ARTIFACT_SHA256,
        "buy_e3_cooldown_policy_sha256": EXACT_E3_FILE_SHA256["policy"],
        "buy_e3_cooldown_predicate_bundle_sha256": EXACT_E3_FILE_SHA256["predicate_bundle"],
        "buy_e3_cooldown_ema_warmup_s": 2048.0,
        "boolean_cooldown_policy_enabled": True,
        "boolean_cooldown_policy_sha256": EXACT_B0_POLICY_FILE_SHA256,
        "boolean_cooldown_predicate_bundle_sha256": (EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256),
        "dynamic_fill_hazard_shadow_enabled": False,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    if any(strategy.get(key) != value for key, value in expected.items()):
        raise OwnerBuyE3MechanicsBaselineError("active config owner/E3 identity drifted")
    if (
        not isinstance(external, Mapping)
        or external.get("enabled") is not False
        or multi.get("global_flow_shadow_enabled") is not False
        or multi.get("global_reference_shadow_enabled") is not False
    ):
        raise OwnerBuyE3MechanicsBaselineError("active config is not the no-shadow source")


def _stage_file(root: Path, relative: PurePosixPath, data: bytes) -> Path:
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise OwnerBuyE3MechanicsBaselineError("staged relative path is unsafe")
    parent = root.joinpath(*relative.parts[:-1])
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(parent, 0o700)
    path = parent / relative.name
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


def _normalized_replay_params(
    params: Mapping[str, Any], raw_config: Mapping[str, Any]
) -> dict[str, Any]:
    normalized = dict(params)
    normalized["rng_seed"] = int(normalized.get("rng_seed", 42) or 42)
    normalized["_config_path"] = f"narrowgate://{IDENTITY}/host-neutral-config"
    normalized["_config_source"] = "exact_owner_e3_host_neutral_projection"
    normalized["_config_explicit"] = True
    ml = raw_config.get("ml")
    if isinstance(ml, Mapping) and isinstance(ml.get("model_dir"), str):
        normalized["model_dir"] = ml["model_dir"]
        normalized["resolved_model_dir"] = ml["model_dir"]
    for name in (
        "operational_baseline_pointer_path",
        "operational_baseline_identity_path",
        "operational_replay_baseline_path",
    ):
        normalized.pop(name, None)
    return normalized


def _load_replay_params(config_path: Path, raw_config: Mapping[str, Any]) -> dict[str, Any]:
    """Build the replay ABI without mutating process-global owner authority."""

    try:
        parsed = live_config._parse(copy.deepcopy(dict(raw_config)))
        params = live_config.to_backtest_params(parsed)
    except (TypeError, ValueError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "exact config cannot be projected into the live-to-backtest ABI"
        ) from exc
    params["_config_path"] = str(config_path)
    params["_config_source"] = "exact_owner_e3_host_neutral_projection"
    params["_config_explicit"] = True
    params["strict_calibration"] = False
    params["strict_calibration_validated"] = False
    params["symbol"] = "BTCUSDC"
    backtest_config.apply_tick_defaults(params, require_historical_bbo=True)
    return _normalized_replay_params(params, raw_config)


def _finalized_replay_params(params: Mapping[str, Any]) -> dict[str, Any]:
    finalized = dict(params)
    finalized.update(REPLAY_ENGINE_OVERLAY)
    return finalized


def _stage_b0(
    root: Path,
    paths: ExactB0ArtifactPaths,
) -> tuple[Path, Path, predicate_view.FrozenPredicateBundle]:
    policy = _secure_snapshot(
        paths.policy,
        expected_sha256=EXACT_B0_POLICY_FILE_SHA256,
        label="exact B0 policy",
    )
    bundle = _secure_snapshot(
        paths.predicate_bundle,
        expected_sha256=EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256,
        label="exact B0 predicate bundle",
    )
    policy_document = _parse_strict_json(policy.data, label="exact B0 policy")
    bundle_document = _parse_strict_json(bundle.data, label="exact B0 predicate bundle")
    b0_root = root / "b0"
    b0_root.mkdir(mode=0o700)
    staged_policy = _stage_file(b0_root, PurePosixPath("policy.json"), policy.data)
    staged_bundle = _stage_file(b0_root, PurePosixPath("predicate_bundle.json"), bundle.data)
    for group in ("book", "trade"):
        entries = bundle_document.get(group)
        if not isinstance(entries, Mapping) or set(entries) != {"BUY", "SELL"}:
            raise OwnerBuyE3MechanicsBaselineError("B0 predicate artifact census drifted")
        for side in ("BUY", "SELL"):
            entry = entries[side]
            if not isinstance(entry, Mapping):
                raise OwnerBuyE3MechanicsBaselineError("B0 predicate artifact entry drifted")
            relative = PurePosixPath(str(entry.get("path", "")))
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise OwnerBuyE3MechanicsBaselineError("B0 predicate artifact path is unsafe")
            original = Path(paths.predicate_bundle).parent.joinpath(*relative.parts)
            child = _secure_snapshot(
                original,
                expected_sha256=str(entry.get("sha256", "")),
                label=f"B0 {group}.{side} predicate artifact",
            )
            _parse_strict_json(child.data, label=f"B0 {group}.{side} predicate artifact")
            _stage_file(b0_root, relative, child.data)
    frozen = predicate_view.load_frozen_predicate_bundle(
        staged_bundle,
        expected_file_sha256=EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256,
    )
    if (
        frozen.canonical_sha256 != EXACT_B0_PREDICATE_BUNDLE_CANONICAL_SHA256
        or policy_document.get("identity")
        != "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
    ):
        raise OwnerBuyE3MechanicsBaselineError("B0 artifact identity drifted")
    return staged_policy, staged_bundle, frozen


def _stage_e3(root: Path, paths: ExactE3ArtifactPaths) -> parity_v1.LoadedExactArtifact:
    snapshots: dict[str, _Snapshot] = {}
    for role, path in (
        ("manifest", paths.manifest),
        ("policy", paths.policy),
        ("predicate_bundle", paths.predicate_bundle),
    ):
        snapshot = _secure_snapshot(
            path,
            expected_sha256=EXACT_E3_FILE_SHA256[role],
            expected_size=EXACT_E3_SIZE_BYTES[role],
            label=f"exact E3 {role}",
        )
        document = _parse_strict_json(snapshot.data, label=f"exact E3 {role}")
        snapshots[role] = _Snapshot(
            data=snapshot.data,
            sha256=snapshot.sha256,
            size_bytes=snapshot.size_bytes,
            document=document,
        )
    e3_root = root / "e3"
    e3_root.mkdir(mode=0o700)
    staged = {
        role: _stage_file(e3_root, PurePosixPath(f"{role}.json"), snapshot.data)
        for role, snapshot in snapshots.items()
    }
    artifact = parity_v1.load_exact_artifact(
        artifact_manifest_path=staged["manifest"],
        artifact_manifest_file_sha256=EXACT_E3_FILE_SHA256["manifest"],
        expected_artifact_sha256=EXACT_E3_ARTIFACT_SHA256,
        policy_path=staged["policy"],
        policy_file_sha256=EXACT_E3_FILE_SHA256["policy"],
        predicate_bundle_path=staged["predicate_bundle"],
        predicate_bundle_file_sha256=EXACT_E3_FILE_SHA256["predicate_bundle"],
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    if (
        artifact.manifest != snapshots["manifest"].document
        or artifact.policy_document != snapshots["policy"].document
        or artifact.predicate_bundle_document != snapshots["predicate_bundle"].document
    ):
        raise OwnerBuyE3MechanicsBaselineError("proven E3 loader changed snapshot bytes")
    return artifact


def _verify_runtime_sources(repository_root: Path) -> Mapping[str, str]:
    root = Path(repository_root)
    _lexical_parts(root, label="runtime repository root")
    observed: dict[str, str] = {}
    for relative, expected in RUNTIME_SOURCE_SHA256.items():
        snapshot = _secure_snapshot(
            root / relative,
            expected_sha256=expected,
            label=f"runtime source {relative}",
            require_mode_0600=False,
        )
        observed[relative] = snapshot.sha256
    return MappingProxyType(observed)


def _verify_execution_module_origins(repository_root: Path) -> Mapping[str, str]:
    root = Path(repository_root).absolute()
    modules = {
        "live/config.py": live_config,
        "live/runtime_policy.py": importlib.import_module("live.runtime_policy"),
        "models/backtest_config.py": backtest_config,
        "models/backtest_tick.py": importlib.import_module("models.backtest_tick"),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_predicate_view_v1.py": predicate_view,
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_replay_adapter_v1.py": replay_adapter,
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_panel_builder_v1.py": importlib.import_module(
            replay_adapter.FIXED_PANEL_BUILDER_MODULE
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_native_observation_cache.py": (
            importlib.import_module(replay_adapter.FIXED_OBSERVATION_CACHE_MODULE)
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py": parity_v1,
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "parity_amendment_v2.py": parity_v2,
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_replay_emitter.py": (
            importlib.import_module(replay_adapter.FIXED_SNAPSHOT_EMITTER_MODULE)
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "repeated_policy_v1.py": importlib.import_module(
            "research.families.f05_fill_quality_quote_ev.audit."
            "causal_multichannel_window_boolean_cooldown_full_multiscale_"
            "successor_repeated_policy_v1"
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1.py": (
            importlib.import_module(
                "research.families.f05_fill_quality_quote_ev.audit."
                "causal_multichannel_window_boolean_cooldown_full_multiscale_"
                "successor_v1"
            )
        ),
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_runtime_policy.py": (
            importlib.import_module(
                "research.families.f05_fill_quality_quote_ev.audit."
                "causal_multichannel_window_boolean_cooldown_runtime_policy"
            )
        ),
        "strategy/boolean_cooldown_buy_e3.py": importlib.import_module(
            "strategy.boolean_cooldown_buy_e3"
        ),
        "strategy/boolean_cooldown_live.py": importlib.import_module(
            "strategy.boolean_cooldown_live"
        ),
    }
    observed: dict[str, str] = {}
    for relative, module in modules.items():
        raw_origin = getattr(module, "__file__", None)
        if not isinstance(raw_origin, str):
            raise OwnerBuyE3MechanicsBaselineError(
                f"execution module origin is unavailable: {relative}"
            )
        origin = Path(raw_origin).absolute()
        expected = (root / relative).absolute()
        if origin != expected:
            raise OwnerBuyE3MechanicsBaselineError(f"execution module origin drifted: {relative}")
        observed[relative] = str(origin)
    factory_origin = Path(__file__).absolute()
    factory_relative = Path(
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1.py"
    )
    if factory_origin != (root / factory_relative).absolute():
        raise OwnerBuyE3MechanicsBaselineError("factory execution module origin drifted")
    observed[factory_relative.as_posix()] = str(factory_origin)
    return MappingProxyType(observed)


def _validate_v1_receipt_snapshot(
    path: Path,
    *,
    layer: str,
    workspace: Path,
) -> Mapping[str, Any]:
    binding = PARITY_RECEIPT_BINDINGS[layer]
    snapshot = _secure_snapshot(
        path,
        expected_sha256=binding["file_sha256"],
        label=f"{layer} parity receipt",
    )
    document = _parse_strict_json(snapshot.data, label=f"{layer} parity receipt")
    staged = _stage_file(
        workspace,
        PurePosixPath(f"evidence/{layer}.json"),
        snapshot.data,
    )
    validated = parity_v1.validate_parity_receipt(
        staged,
        expected_layer=layer,
        expected_artifact_sha256=EXACT_E3_ARTIFACT_SHA256,
    )
    if (
        validated != document
        or document.get("canonical_receipt_sha256") != binding["canonical_sha256"]
        or document.get("economic_values_exposed") is not False
        or document.get("economic_values_used_for_selection") is not False
        or document.get("validation_read") is not False
        or document.get("sealed_holdout_read") is not False
        or document.get("action_authorized") is not False
        or document.get("live_authorized") is not False
    ):
        raise OwnerBuyE3MechanicsBaselineError(f"{layer} parity boundary drifted")
    return document


def _validate_parity_evidence(
    paths: ParityEvidencePaths,
    *,
    workspace: Path,
) -> ParityEvidenceBinding:
    synthetic_paths = {
        parity_v1.RESEARCH_COMPILED_LAYER: paths.research_compiled,
        parity_v1.DEVELOPMENT_SNAPSHOT_LAYER: paths.development_snapshot,
        parity_v1.STREAMING_OFFLINE_LAYER: paths.streaming_offline,
    }
    synthetic: dict[str, Mapping[str, str]] = {}
    for layer, path in synthetic_paths.items():
        document = _validate_v1_receipt_snapshot(
            path,
            layer=layer,
            workspace=workspace,
        )
        synthetic[layer] = MappingProxyType(
            {
                "file_sha256": PARITY_RECEIPT_BINDINGS[layer]["file_sha256"],
                "canonical_sha256": str(document["canonical_receipt_sha256"]),
            }
        )
    contract_snapshot = _secure_snapshot(
        paths.layer4_contract,
        expected_sha256=LAYER4_CONTRACT_FILE_SHA256,
        label="amended Layer4 contract",
    )
    final_snapshot = _secure_snapshot(
        paths.layer4_final,
        expected_sha256=LAYER4_FINAL_FILE_SHA256,
        label="amended Layer4 final receipt",
    )
    contract_document = _parse_strict_json(contract_snapshot.data, label="amended Layer4 contract")
    final_document = _parse_strict_json(final_snapshot.data, label="amended Layer4 final receipt")
    contract = contract_document
    final = final_document
    exact_artifact = contract.get("exact_artifact")
    evidence = final.get("evidence")
    if (
        contract != contract_document
        or final != final_document
        or contract.get("canonical_contract_sha256") != LAYER4_CONTRACT_CANONICAL_SHA256
        or final.get("canonical_receipt_sha256") != LAYER4_FINAL_CANONICAL_SHA256
        or contract.get("ordered_development_days") != list(FORMAL_E3_MECHANICS_DAYS)
        or not isinstance(exact_artifact, Mapping)
        or exact_artifact.get("artifact_sha256") != EXACT_E3_ARTIFACT_SHA256
        or not isinstance(evidence, Mapping)
        or evidence.get("day_count") != len(FORMAL_E3_MECHANICS_DAYS)
        or evidence.get("day_receipts_sha256") != LAYER4_DAY_RECEIPTS_SHA256
        or final.get("evidence_boundary")
        != {
            "economic_values_exposed": False,
            "economic_values_used_for_selection": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "hypothetical_live_scoring": False,
        }
        or final.get("permissions")
        != {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        }
    ):
        raise OwnerBuyE3MechanicsBaselineError("amended Layer4 evidence drifted")
    admitted = evidence.get("day_receipts")
    if not isinstance(admitted, list) or [binding.get("utc_day") for binding in admitted] != list(
        FORMAL_E3_MECHANICS_DAYS
    ):
        raise OwnerBuyE3MechanicsBaselineError("amended Layer4 day order drifted")
    day_root = Path(paths.layer4_day_receipt_dir)
    _lexical_parts(day_root, label="amended Layer4 day receipt root")
    try:
        observed_names = {entry.name for entry in day_root.iterdir()}
    except OSError as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "amended Layer4 day receipt root is missing"
        ) from exc
    expected_names = {f"{day}.json" for day in FORMAL_E3_MECHANICS_DAYS}
    if observed_names != expected_names:
        raise OwnerBuyE3MechanicsBaselineError("amended Layer4 day receipt set drifted")
    validated_day_bindings: list[dict[str, Any]] = []
    for utc_day, binding in zip(FORMAL_E3_MECHANICS_DAYS, admitted, strict=True):
        if not isinstance(binding, Mapping) or binding.get("file_name") != f"{utc_day}.json":
            raise OwnerBuyE3MechanicsBaselineError("amended Layer4 day receipt binding drifted")
        day_snapshot = _secure_snapshot(
            day_root / f"{utc_day}.json",
            expected_sha256=str(binding.get("file_sha256", "")),
            label=f"{utc_day} amended Layer4 day receipt",
        )
        day_document = _parse_strict_json(
            day_snapshot.data, label=f"{utc_day} amended Layer4 day receipt"
        )
        expected_day = parity_v2._day_receipt_payload(
            contract=contract,
            contract_file_sha256=contract_snapshot.sha256,
            utc_day=utc_day,
            day_input_sha256=str(day_document.get("day_input_sha256", "")),
            result=day_document.get("lockstep", {}),
        )
        if day_document != expected_day:
            raise OwnerBuyE3MechanicsBaselineError(
                f"{utc_day} amended Layer4 day receipt semantics drifted"
            )
        expected_binding = {
            "utc_day": utc_day,
            "file_name": f"{utc_day}.json",
            "file_sha256": day_snapshot.sha256,
            "canonical_day_receipt_sha256": day_document["canonical_day_receipt_sha256"],
        }
        if dict(binding) != expected_binding:
            raise OwnerBuyE3MechanicsBaselineError(
                f"{utc_day} amended Layer4 day file binding drifted"
            )
        validated_day_bindings.append(expected_binding)
    if canonical_sha256(validated_day_bindings) != LAYER4_DAY_RECEIPTS_SHA256:
        raise OwnerBuyE3MechanicsBaselineError(
            "amended Layer4 aggregate day receipt identity drifted"
        )
    return ParityEvidenceBinding(
        synthetic_receipts=MappingProxyType(synthetic),
        layer4_contract_file_sha256=contract_snapshot.sha256,
        layer4_contract_canonical_sha256=LAYER4_CONTRACT_CANONICAL_SHA256,
        layer4_final_file_sha256=final_snapshot.sha256,
        layer4_final_canonical_sha256=LAYER4_FINAL_CANONICAL_SHA256,
        layer4_day_receipts_sha256=LAYER4_DAY_RECEIPTS_SHA256,
        formal_e3_mechanics_panel_days=FORMAL_E3_MECHANICS_DAYS,
    )


def _explicit_path_lockstep_evaluator(
    *,
    artifact: parity_v1.LoadedExactArtifact,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    expected_identity_hashes: Mapping[str, str],
    b0_policy_path: Path,
    b0_predicate_bundle_path: Path,
    cutoff_ns: int,
) -> Any:
    """Build the proven lockstep graph without mutating adapter globals."""

    artifact_binding = repeated_policy.ArtifactIdentityBinding(
        executed_artifact_scope=(
            repeated_policy.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
        ),
        executed_policy_identity=parity_v1.IDENTITY,
        executed_policy_sha256=artifact.policy_file_sha256,
        executed_predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
        learning_algorithm_identity=(f"{parity_v1.IDENTITY}.formal_v24_learning_algorithm"),
        learning_algorithm_artifact_sha256=LEARNING_ALGORITHM_ARTIFACT_SHA256,
        final_artifact_identity=parity_v1.IDENTITY,
        final_artifact_sha256=artifact.policy_file_sha256,
        exact_final_artifact_oof_available=False,
    )
    target = parity_v1._BoundSnapshotArtifactEvaluator(
        artifact=artifact,
        source_predicate_bundle=source_predicate_bundle,
        expected_identity_hashes=expected_identity_hashes,
        mode="compiled",
    )

    def b0() -> Any:
        return replay_adapter._ExactOwnerArtifactEvaluator(
            expected_identity_hashes=expected_identity_hashes,
            policy_path=b0_policy_path,
            predicate_bundle_path=b0_predicate_bundle_path,
        )

    delegated = repeated_policy.TargetSideDelegatingEvaluator(
        target_side=repeated_policy.CandidateTargetSide.BUY,
        target_evaluator=target,
        b0_evaluator=b0(),
        artifact_binding=artifact_binding,
    )
    return replay_adapter._TargetDayOnlyEvaluator(
        delegated,
        b0(),
        predicate_bundle_sha256=artifact.predicate_bundle_file_sha256,
        cutoff_ns=cutoff_ns,
    )


class OwnerBuyE3MechanicsBaseline:
    """A loaded exact baseline that builds one fresh overlay per admitted day."""

    def __init__(
        self,
        *,
        workspace: tempfile.TemporaryDirectory[str],
        artifact: parity_v1.LoadedExactArtifact,
        source_predicate_bundle: predicate_view.FrozenPredicateBundle,
        staged_b0_policy: Path,
        staged_b0_bundle: Path,
        staged_predecessor_source_config: Path,
        projected_config_path: Path,
        projected_config_bytes: bytes,
        base_params: Mapping[str, Any],
        runtime_sources: Mapping[str, str],
        parity_evidence: ParityEvidenceBinding,
        replay_abi_sha256: str,
        runtime_repository_root: Path,
        module_origins: Mapping[str, str],
    ) -> None:
        self._workspace = workspace
        self.artifact = artifact
        self.source_predicate_bundle = source_predicate_bundle
        self._staged_b0_policy = staged_b0_policy
        self._staged_b0_bundle = staged_b0_bundle
        self._staged_predecessor_source_config = staged_predecessor_source_config
        self.projected_config_path = projected_config_path
        self._projected_config_bytes = projected_config_bytes
        self.base_params = MappingProxyType(dict(base_params))
        self.runtime_sources = MappingProxyType(dict(runtime_sources))
        self.parity_evidence = parity_evidence
        self.replay_abi_sha256 = replay_abi_sha256
        self._runtime_repository_root = Path(runtime_repository_root).absolute()
        self._module_origins = MappingProxyType(dict(module_origins))
        self._closed = False

    @property
    def identity(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "identity": IDENTITY,
                "schema_version": SCHEMA_VERSION,
                "active_source_config_file_sha256": ACTIVE_SOURCE_CONFIG_FILE_SHA256,
                "host_neutral_config_mapping_sha256": (HOST_NEUTRAL_CONFIG_MAPPING_SHA256),
                "host_neutral_config_file_sha256": HOST_NEUTRAL_CONFIG_FILE_SHA256,
                "source_config_delta_paths_sha256": SOURCE_CONFIG_DELTA_PATHS_SHA256,
                "source_replay_abi_delta_paths_sha256": (REPLAY_ABI_SOURCE_DELTA_PATHS_SHA256),
                "final_replay_abi_delta_paths_sha256": (REPLAY_ABI_FINAL_DELTA_PATHS_SHA256),
                "replay_abi_sha256": self.replay_abi_sha256,
                "artifact_sha256": EXACT_E3_ARTIFACT_SHA256,
                "formal_e3_mechanics_panel_day_count": len(FORMAL_E3_MECHANICS_DAYS),
                "reduced_support": True,
                "availability": dict(AVAILABILITY),
                "permissions": dict(PERMISSIONS),
            }
        )

    @property
    def projected_config_bytes(self) -> bytes:
        return bytes(self._projected_config_bytes)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._workspace.cleanup()

    def __enter__(self) -> OwnerBuyE3MechanicsBaseline:
        if self._closed:
            raise OwnerBuyE3MechanicsBaselineError("mechanics baseline is closed")
        if (
            _verify_execution_module_origins(self._runtime_repository_root) != self._module_origins
            or _verify_runtime_sources(self._runtime_repository_root) != self.runtime_sources
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                "day-overlay execution source changed after factory load"
            )
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def build_day_overlay(self, request: Any, replay: Any, *, utc_day: str) -> DayMechanicsOverlay:
        """Install a fresh cooldown-v2 emitter and compiled E3 evaluator.

        The admitted contract is daily-reset: D-1 is market/feature warmup in
        the frozen observation cache, D is the only E3 assignment interval,
        and D+1 is common washout where new assignments use exact B0.  A
        restart must call this method again and replay the complete day; no
        partial emitter/evaluator state is resumable.
        """

        if self._closed:
            raise OwnerBuyE3MechanicsBaselineError("mechanics baseline is closed")
        try:
            day = date.fromisoformat(str(utc_day))
        except ValueError as exc:
            raise OwnerBuyE3MechanicsBaselineError("utc_day is invalid") from exc
        normalized = day.isoformat()
        if normalized not in FORMAL_E3_MECHANICS_DAYS:
            raise OwnerBuyE3MechanicsBaselineError(
                "day is outside the reduced-support formal E3 mechanics panel"
            )
        continuation = (day + timedelta(days=1)).isoformat()
        if (
            str(getattr(request, "utc_day", "")) != normalized
            or str(getattr(replay, "utc_day", "")) != normalized
            or str(getattr(replay, "continuation_day", "")) != continuation
        ):
            raise OwnerBuyE3MechanicsBaselineError("D/D+1 replay boundary drifted")
        replay_params = getattr(replay, "params", None)
        if isinstance(replay_params, Mapping) and any(
            replay_params.get(name) is not None
            for name in (
                "cooldown_v2_snapshot_emitter",
                "cooldown_duration_policy_evaluator",
            )
        ):
            raise OwnerBuyE3MechanicsBaselineError("replay already has a policy overlay")
        identity_hashes = dict(replay_adapter._day_identity_hashes(request))
        if not identity_hashes or any(
            _SHA_RE.fullmatch(str(value)) is None for value in identity_hashes.values()
        ):
            raise OwnerBuyE3MechanicsBaselineError("day replay identity hashes drifted")
        target_start_ns = (
            int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1_000_000_000
        )
        target_cutoff_ns = target_start_ns + 86_400 * 1_000_000_000
        emitter = replay_adapter._build_day_snapshot_emitter(
            request,
            replay,
            utc_day=normalized,
            identity_hashes=identity_hashes,
        )
        evaluator = _explicit_path_lockstep_evaluator(
            artifact=self.artifact,
            source_predicate_bundle=self.source_predicate_bundle,
            expected_identity_hashes=identity_hashes,
            cutoff_ns=target_cutoff_ns,
            b0_policy_path=self._staged_b0_policy,
            b0_predicate_bundle_path=self._staged_b0_bundle,
        )
        if not bool(getattr(evaluator, "binding_valid", False)):
            raise OwnerBuyE3MechanicsBaselineError("compiled E3/B0 evaluator binding failed")
        if (
            _verify_execution_module_origins(self._runtime_repository_root) != self._module_origins
            or _verify_runtime_sources(self._runtime_repository_root) != self.runtime_sources
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                "day-overlay execution source changed during construction"
            )
        try:
            trace_capacity = max(1, len(replay.trades))
        except (AttributeError, TypeError) as exc:
            raise OwnerBuyE3MechanicsBaselineError("replay trade census is unavailable") from exc
        params = dict(self.base_params)
        params.update(
            {
                "cooldown_v2_snapshot_emitter": emitter,
                "cooldown_duration_policy_evaluator": evaluator,
                "trace_cooldown_duration_opportunities_max": trace_capacity,
            }
        )
        receipt = {
            "schema_version": DAY_OVERLAY_SCHEMA,
            "identity": IDENTITY,
            "utc_day": normalized,
            "continuation_day": continuation,
            "target_assignment_interval": (f"{normalized}T00:00:00Z/{continuation}T00:00:00Z"),
            "daily_fresh_emitter": True,
            "daily_fresh_compiled_evaluator": True,
            "d_minus_1_natural_utc_warmup_from_frozen_observation_cache": True,
            "warmup_cutoff_ns": target_start_ns,
            "buy_e3_warmup_s": 2048.0,
            "d_plus_1_new_e3_assignments_allowed": False,
            "d_plus_1_exact_b0_washout": True,
            "sell_delegates_exact_b0": True,
            "restart_requires_complete_day_replay": True,
            "partial_policy_state_resume_authorized": False,
            "artifact_sha256": EXACT_E3_ARTIFACT_SHA256,
            "replay_abi_sha256": self.replay_abi_sha256,
            "identity_hashes_sha256": canonical_sha256(identity_hashes),
            "availability": dict(AVAILABILITY),
            "permissions": dict(PERMISSIONS),
        }
        receipt["canonical_day_overlay_sha256"] = document_sha256(
            receipt, "canonical_day_overlay_sha256"
        )
        return DayMechanicsOverlay(
            utc_day=normalized,
            continuation_day=continuation,
            target_start_ns=target_start_ns,
            target_cutoff_ns=target_cutoff_ns,
            params=MappingProxyType(params),
            snapshot_emitter=emitter,
            compiled_evaluator=evaluator,
            identity_hashes=MappingProxyType(identity_hashes),
            receipt=MappingProxyType(receipt),
        )

    def run_default_day_replay(
        self,
        request: Any,
        replay: Any,
        *,
        utc_day: str,
    ) -> Mapping[str, Any]:
        """Execute the formal Python tick consumer with the exact E3 overlay."""

        overlay = self.build_day_overlay(request, replay, utc_day=utc_day)
        params = dict(overlay.params)
        if (
            params.get("cooldown_v2_snapshot_emitter") is not overlay.snapshot_emitter
            or params.get("cooldown_duration_policy_evaluator") is not overlay.compiled_evaluator
            or overlay.receipt.get("artifact_sha256") != EXACT_E3_ARTIFACT_SHA256
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                "default tick consumer did not receive the exact E3 ABI overlay"
            )
        backtest = importlib.import_module("models.backtest_tick")
        result = backtest._simulate_tick_with_engine(
            "python",
            replay.trades,
            replay.var_ts_ms,
            replay.var_ssq,
            params,
            ml_data=replay.ml_data,
            bbo_data=replay.bbo_data,
            l2_data=replay.l2_data,
            var_ti=replay.var_ti,
            var_retsq=replay.var_retsq,
        )
        if not isinstance(result, Mapping):
            raise OwnerBuyE3MechanicsBaselineError(
                "default tick consumer returned a malformed result"
            )
        policy_audit = result.get("_cooldown_duration_policy_audit")
        emitter_audit = result.get("_cooldown_v2_snapshot_emitter_audit")

        def positive_count(audit: Mapping[str, Any], name: str) -> bool:
            value = audit.get(name)
            return type(value) is int and value > 0

        if (
            not isinstance(policy_audit, Mapping)
            or policy_audit.get("policy_sha256") != EXACT_E3_FILE_SHA256["policy"]
            or policy_audit.get("predicate_bundle_sha256")
            != EXACT_E3_FILE_SHA256["predicate_bundle"]
            or policy_audit.get("target_side") != "BUY"
            or policy_audit.get("opposite_side_delegates_exact_b0") is not True
            or policy_audit.get("d_plus_1_new_target_assignments_allowed") is not False
            or not isinstance(emitter_audit, Mapping)
            or not positive_count(policy_audit, "target_side_evaluations")
            or not positive_count(policy_audit, "b0_delegated_evaluations")
            or not positive_count(policy_audit, "d_plus_1_exact_b0_fallback_count")
            or not positive_count(emitter_audit, "snapshots_emitted")
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                "default tick consumer E3/B0 execution audit drifted"
            )
        completed = {
            "_default_buy_e3_mechanics_receipt": dict(overlay.receipt),
            "_cooldown_duration_policy_audit": dict(policy_audit),
            "_cooldown_v2_snapshot_emitter_audit": dict(emitter_audit),
            "_default_mechanics_authorities": dict(PERMISSIONS),
        }
        return MappingProxyType(completed)


def create_owner_buy_e3_backtest_mechanics_baseline(
    *,
    runtime_repository_root: Path,
    predecessor_v12_config_path: Path,
    active_source_config_path: Path,
    e3_artifact_paths: ExactE3ArtifactPaths,
    b0_artifact_paths: ExactB0ArtifactPaths,
    parity_evidence_paths: ParityEvidencePaths,
) -> OwnerBuyE3MechanicsBaseline:
    """Load the complete private contract or fail without a public fallback."""

    module_origins = _verify_execution_module_origins(runtime_repository_root)
    runtime_sources = _verify_runtime_sources(runtime_repository_root)
    predecessor_snapshot = _secure_snapshot(
        predecessor_v12_config_path,
        expected_sha256=PREDECESSOR_V12_CONFIG_FILE_SHA256,
        label="exact v12 predecessor config",
    )
    active_snapshot = _secure_snapshot(
        active_source_config_path,
        expected_sha256=ACTIVE_SOURCE_CONFIG_FILE_SHA256,
        label="exact active E3 source config",
    )
    predecessor = _parse_strict_yaml(
        predecessor_snapshot.data, label="exact v12 predecessor config"
    )
    active = _parse_strict_yaml(active_snapshot.data, label="exact active E3 source config")
    source_delta = tuple(_mapping_difference_paths(predecessor, active))
    if (
        source_delta != SOURCE_CONFIG_DELTA_PATHS
        or canonical_sha256(list(source_delta)) != SOURCE_CONFIG_DELTA_PATHS_SHA256
        or set(source_delta)
        != set(E3_CONFIG_DELTA_PATHS)
        | set(DIAGNOSTIC_DISABLE_DELTA_PATHS)
        | set(WRITER_IDENTITY_DELTA_PATHS)
    ):
        raise OwnerBuyE3MechanicsBaselineError("exact v12-to-E3 config delta drifted")
    _validate_source_config(active)
    projected_active, projected_active_bytes = _project_host_neutral_config(active)
    projected_predecessor = copy.deepcopy(dict(predecessor))
    for path, value in HOST_NEUTRAL_MUTATIONS.items():
        if _has_nested(projected_predecessor, path):
            _set_nested(projected_predecessor, path, value)
    _assert_host_neutral_projection(projected_predecessor, require_all_mutations=False)

    workspace = tempfile.TemporaryDirectory(prefix="narrowgate-e3-mechanics-baseline-")
    root = Path(workspace.name)
    try:
        active_config = _stage_file(
            root,
            PurePosixPath("config/active.host_neutral.json"),
            projected_active_bytes,
        )
        predecessor_bytes = (
            json.dumps(
                projected_predecessor,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
            + b"\n"
        )
        predecessor_config = _stage_file(
            root,
            PurePosixPath("config/predecessor.host_neutral.json"),
            predecessor_bytes,
        )
        predecessor_source_config = _stage_file(
            root,
            PurePosixPath("config/predecessor.exact-source.yaml"),
            predecessor_snapshot.data,
        )
        active_source_params = _load_replay_params(active_config, projected_active)
        predecessor_source_params = _load_replay_params(predecessor_config, projected_predecessor)
        source_replay_delta = tuple(
            _mapping_difference_paths(predecessor_source_params, active_source_params)
        )
        if (
            source_replay_delta != REPLAY_ABI_SOURCE_DELTA_PATHS
            or canonical_sha256(list(source_replay_delta)) != REPLAY_ABI_SOURCE_DELTA_PATHS_SHA256
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                f"v12-to-active source replay ABI drifted: {source_replay_delta}"
            )
        active_params = _finalized_replay_params(active_source_params)
        predecessor_params = _finalized_replay_params(predecessor_source_params)
        finalized_replay_delta = tuple(_mapping_difference_paths(predecessor_params, active_params))
        if (
            finalized_replay_delta != REPLAY_ABI_FINAL_DELTA_PATHS
            or canonical_sha256(list(finalized_replay_delta)) != REPLAY_ABI_FINAL_DELTA_PATHS_SHA256
            or active_params.get("dynamic_fill_hazard_shadow_enabled") is not False
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                f"v12-to-active finalized replay ABI drifted: {finalized_replay_delta}"
            )
        replay_abi_sha = canonical_sha256(active_params)
        if (
            canonical_sha256(dict(REPLAY_ENGINE_OVERLAY)) != REPLAY_ENGINE_OVERLAY_SHA256
            or replay_abi_sha != EXACT_ACTIVE_REPLAY_ABI_SHA256
        ):
            raise OwnerBuyE3MechanicsBaselineError("active-config replay ABI identity drifted")
        artifact = _stage_e3(root, e3_artifact_paths)
        staged_b0_policy, staged_b0_bundle, source_bundle = _stage_b0(root, b0_artifact_paths)
        parity_evidence = _validate_parity_evidence(
            parity_evidence_paths,
            workspace=root,
        )
        if (
            _verify_execution_module_origins(runtime_repository_root) != module_origins
            or _verify_runtime_sources(runtime_repository_root) != runtime_sources
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                "execution module origin or source changed during factory load"
            )
        return OwnerBuyE3MechanicsBaseline(
            workspace=workspace,
            artifact=artifact,
            source_predicate_bundle=source_bundle,
            staged_b0_policy=staged_b0_policy,
            staged_b0_bundle=staged_b0_bundle,
            staged_predecessor_source_config=predecessor_source_config,
            projected_config_path=active_config,
            projected_config_bytes=projected_active_bytes,
            base_params=active_params,
            runtime_sources=runtime_sources,
            parity_evidence=parity_evidence,
            replay_abi_sha256=replay_abi_sha,
            runtime_repository_root=runtime_repository_root,
            module_origins=module_origins,
        )
    except BaseException:
        workspace.cleanup()
        raise


def _private_relative_locator(value: Any, *, role: str) -> str:
    text = str(value)
    candidate = PurePosixPath(text)
    if (
        not text
        or "\\" in text
        or "\x00" in text
        or text != candidate.as_posix()
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            f"owner-private {role} locator is not canonical relative POSIX"
        )
    return text


def _resolve_owner_private_inputs(
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    relative_locators: Mapping[str, Any],
) -> OwnerPrivateInputs:
    if set(relative_locators) != set(OWNER_PRIVATE_INPUT_ROLES):
        raise OwnerBuyE3MechanicsBaselineError(
            "owner-private input locator roles drifted from the exact 12-input contract"
        )
    root = Path(durable_evidence_root).absolute()
    metadata_root = Path(metadata_repository_root).absolute()
    _lexical_parts(root, label="owner-private durable evidence root")
    _lexical_parts(metadata_root, label="owner-private metadata repository root")
    root_fd = _open_trusted_directory(
        root, label="owner-private durable evidence root", exact_mode=0o700
    )
    os.close(root_fd)
    metadata_fd = _open_trusted_directory(
        metadata_root, label="owner-private metadata repository root", exact_mode=None
    )
    os.close(metadata_fd)
    locators = {
        role: _private_relative_locator(relative_locators[role], role=role)
        for role in OWNER_PRIVATE_INPUT_ROLES
    }
    resolved = {
        role: (metadata_root if role in OWNER_METADATA_INPUT_ROLES else root).joinpath(
            *PurePosixPath(value).parts
        )
        for role, value in locators.items()
    }
    layer4 = resolved["amended_layer4_root"]
    layer4_fd = _open_trusted_directory(
        layer4, label="amended Layer4 private root", exact_mode=0o700
    )
    os.close(layer4_fd)
    return OwnerPrivateInputs(
        v13_reconciliation_manifest=resolved["v13_reconciliation_manifest"],
        predecessor_v12_config=resolved["predecessor_v12_config"],
        active_source_config=resolved["active_source_config"],
        e3_artifact_paths=ExactE3ArtifactPaths(
            manifest=resolved["e3_artifact_manifest"],
            policy=resolved["e3_policy"],
            predicate_bundle=resolved["e3_predicate_bundle"],
        ),
        b0_artifact_paths=ExactB0ArtifactPaths(
            policy=resolved["b0_policy"],
            predicate_bundle=resolved["b0_predicate_bundle"],
        ),
        parity_evidence_paths=ParityEvidencePaths(
            research_compiled=resolved["parity_research_compiled"],
            development_snapshot=resolved["parity_development_snapshot"],
            streaming_offline=resolved["parity_streaming_offline"],
            layer4_contract=layer4 / "layer4_contract.json",
            layer4_final=layer4 / "layer4_repeated_policy_lockstep.json",
            layer4_day_receipt_dir=layer4 / "layer4_days",
        ),
        relative_locators=MappingProxyType(locators),
    )


def _validate_committed_v13_reconciliation(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    inputs: OwnerPrivateInputs,
) -> Mapping[str, Any]:
    """Validate that the exact v13 private transaction is already committed.

    The v13 validator is run in a child process so its environment-scoped owner
    authority can never become visible to another thread in this process.
    """

    manifest_snapshot = _secure_snapshot(
        inputs.v13_reconciliation_manifest,
        expected_sha256=None,
        label="committed v13 reconciliation manifest",
    )
    manifest = _parse_strict_json(
        manifest_snapshot.data, label="committed v13 reconciliation manifest"
    )
    transaction = manifest.get("transaction")
    outputs = transaction.get("outputs") if isinstance(transaction, Mapping) else None
    active_source = (
        transaction.get("active_config_source") if isinstance(transaction, Mapping) else None
    )
    if (
        not isinstance(outputs, Mapping)
        or outputs.get("backtest_v12_archive") != str(inputs.predecessor_v12_config.absolute())
        or not isinstance(active_source, Mapping)
        or active_source.get("path") != str(inputs.active_source_config.absolute())
        or active_source.get("sha256") != ACTIVE_SOURCE_CONFIG_FILE_SHA256
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 reconciliation does not cross-bind the exact v12/active config pair"
        )
    repository_root = Path(runtime_repository_root).absolute()
    script = repository_root / "scripts/f05_reconcile_live_config_locator_v1.py"
    script_snapshot = _secure_snapshot(
        script,
        expected_sha256=V13_RECONCILIATION_VALIDATOR_SHA256,
        label="v13 reconciliation validator source",
        require_mode_0600=False,
        expected_mode=0o644,
    )
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        # The v13 transaction is rooted one level above the owner-private f05
        # trust root.  Its exact manifest still cross-binds every absolute
        # output; this split avoids copying or symlinking the canonical v12
        # archive into durable evidence.
        PRIVATE_EVIDENCE_ROOT_ENV: str(Path(durable_evidence_root).absolute().parent),
        "GIT_NO_REPLACE_OBJECTS": "1",
    }
    bootstrap = (
        "import runpy,sys;"
        "sys.path.insert(0,sys.argv[1]);"
        "script=sys.argv[2];"
        "sys.argv=[script,*sys.argv[3:]];"
        "runpy.run_path(script,run_name='__main__')"
    )
    try:
        with tempfile.TemporaryDirectory(prefix="narrowgate-v13-gate-pycache-") as cache:
            os.chmod(cache, 0o700)
            completed = subprocess.run(
                (
                    sys.executable,
                    "-I",
                    "-B",
                    "-X",
                    f"pycache_prefix={cache}",
                    "-c",
                    bootstrap,
                    str(repository_root),
                    str(script),
                    "run",
                    "--manifest",
                    str(inputs.v13_reconciliation_manifest),
                ),
                cwd=repository_root,
                env=environment,
                check=True,
                capture_output=True,
            )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 reconciliation is not recursively committed"
        ) from exc
    result = _parse_strict_json(completed.stdout, label="v13 reconciliation committed-state result")
    child_manifest = result.get("manifest")
    if (
        not isinstance(child_manifest, Mapping)
        or child_manifest.get("file_sha256") != manifest_snapshot.sha256
        or child_manifest.get("size_bytes") != manifest_snapshot.size_bytes
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 child validation used a different reconciliation manifest snapshot"
        )
    state = result.get("state_before")
    immutable = state.get("immutable") if isinstance(state, Mapping) else None
    pending = state.get("pending") if isinstance(state, Mapping) else None
    if (
        result.get("writes_performed") is not False
        or not isinstance(immutable, Mapping)
        or not immutable
        or set(immutable.values()) != {"published_nlink1"}
        or state.get("receipt") != "published_nlink1"
        or state.get("stable_alias") != "successor"
        or state.get("pointer") != "successor"
        or state.get("catalog") != "successor"
        or not isinstance(pending, Mapping)
        or set(pending.values()) != {"absent"}
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 reconciliation transaction is not fully committed"
        )
    receipt = result.get("receipt")
    if not isinstance(receipt, Mapping):
        raise OwnerBuyE3MechanicsBaselineError("v13 committed receipt binding is missing")
    manifest_after = _secure_snapshot(
        inputs.v13_reconciliation_manifest,
        expected_sha256=manifest_snapshot.sha256,
        expected_size=manifest_snapshot.size_bytes,
        label="committed v13 reconciliation manifest post-validation",
    )
    script_after = _secure_snapshot(
        script,
        expected_sha256=script_snapshot.sha256,
        expected_size=script_snapshot.size_bytes,
        label="v13 reconciliation validator source post-validation",
        require_mode_0600=False,
        expected_mode=0o644,
    )
    if manifest_after.data != manifest_snapshot.data or script_after.data != script_snapshot.data:
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 reconciliation authority changed during committed-state validation"
        )
    return MappingProxyType(
        {
            "manifest_file_sha256": manifest_snapshot.sha256,
            "manifest_size_bytes": manifest_snapshot.size_bytes,
            "manifest_canonical_sha256": canonical_sha256(manifest),
            "receipt_file_sha256": _require_sha(
                receipt.get("file_sha256"), "v13 committed receipt file SHA256"
            ),
            "receipt_canonical_sha256": _require_sha(
                receipt.get("canonical_sha256"), "v13 committed receipt canonical SHA256"
            ),
            "validator_source_sha256": script_snapshot.sha256,
            "transaction_committed": True,
        }
    )


def _owner_private_input_contract(
    *,
    inputs: OwnerPrivateInputs,
    v13_commit: Mapping[str, Any],
) -> Mapping[str, Any]:
    entries: dict[str, Any] = {}
    for role in OWNER_PRIVATE_INPUT_ROLES:
        entry: dict[str, Any] = {
            "relative_locator": inputs.relative_locators[role],
            "locator_base": (
                "metadata_repository" if role in OWNER_METADATA_INPUT_ROLES else "durable_evidence"
            ),
            "absolute_locator_persisted": False,
            "kind": "directory" if role == "amended_layer4_root" else "file",
        }
        if role in OWNER_PRIVATE_FILE_SHA256:
            entry["file_sha256"] = OWNER_PRIVATE_FILE_SHA256[role]
        elif role == "v13_reconciliation_manifest":
            entry["file_sha256"] = v13_commit["manifest_file_sha256"]
            entry["size_bytes"] = v13_commit["manifest_size_bytes"]
        else:
            entry["recursive_binding"] = {
                "contract_file_sha256": LAYER4_CONTRACT_FILE_SHA256,
                "final_file_sha256": LAYER4_FINAL_FILE_SHA256,
                "day_receipts_sha256": LAYER4_DAY_RECEIPTS_SHA256,
                "day_count": len(FORMAL_E3_MECHANICS_DAYS),
            }
        entries[role] = entry
    contract: dict[str, Any] = {
        "schema_version": OWNER_PRIVATE_INPUT_SCHEMA,
        "identity": IDENTITY,
        "status": "exact_owner_private_relative_inputs_recursively_bound",
        "locator_bases": {
            "durable_evidence": {
                "environment_variable": PRIVATE_EVIDENCE_ROOT_ENV,
                "roles": [
                    role
                    for role in OWNER_PRIVATE_INPUT_ROLES
                    if role not in OWNER_METADATA_INPUT_ROLES
                ],
            },
            "metadata_repository": {
                "environment_variable": METADATA_REPOSITORY_ROOT_ENV,
                "roles": [
                    role for role in OWNER_PRIVATE_INPUT_ROLES if role in OWNER_METADATA_INPUT_ROLES
                ],
            },
            "absolute_locator_persisted": False,
        },
        "input_count": OWNER_PRIVATE_INPUT_COUNT,
        "inputs": entries,
        "v13_committed_predecessor": dict(v13_commit),
        "economic_or_holdout_inputs_present": False,
        "permissions": dict(PERMISSIONS),
    }
    contract["canonical_owner_private_inputs_sha256"] = document_sha256(
        contract, "canonical_owner_private_inputs_sha256"
    )
    return MappingProxyType(contract)


def _baseline_capability_receipt(
    baseline: OwnerBuyE3MechanicsBaseline,
    *,
    private_input_contract: Mapping[str, Any],
) -> Mapping[str, Any]:
    if type(baseline) is not OwnerBuyE3MechanicsBaseline or baseline._closed:
        raise OwnerBuyE3MechanicsBaselineError(
            "mechanics publication requires a live exact factory instance"
        )
    if (
        baseline.replay_abi_sha256 != EXACT_ACTIVE_REPLAY_ABI_SHA256
        or dict(baseline.identity).get("permissions") != dict(PERMISSIONS)
        or baseline.artifact.artifact_sha256 != EXACT_E3_ARTIFACT_SHA256
        or baseline.parity_evidence.layer4_day_receipts_sha256 != LAYER4_DAY_RECEIPTS_SHA256
        or _verify_runtime_sources(baseline._runtime_repository_root) != baseline.runtime_sources
        or _verify_execution_module_origins(baseline._runtime_repository_root)
        != baseline._module_origins
    ):
        raise OwnerBuyE3MechanicsBaselineError("loaded mechanics capability drifted")
    for path, expected, label in (
        (
            baseline._staged_predecessor_source_config,
            PREDECESSOR_V12_CONFIG_FILE_SHA256,
            "loaded exact v12 predecessor source config",
        ),
        (
            baseline.projected_config_path,
            HOST_NEUTRAL_CONFIG_FILE_SHA256,
            "loaded host-neutral projection",
        ),
        (baseline.artifact.policy_path, EXACT_E3_FILE_SHA256["policy"], "loaded E3 policy"),
        (
            baseline.artifact.predicate_bundle_path,
            EXACT_E3_FILE_SHA256["predicate_bundle"],
            "loaded E3 predicate bundle",
        ),
        (baseline._staged_b0_policy, EXACT_B0_POLICY_FILE_SHA256, "loaded B0 policy"),
        (
            baseline._staged_b0_bundle,
            EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256,
            "loaded B0 predicate bundle",
        ),
    ):
        _secure_snapshot(path, expected_sha256=expected, label=label)
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.loaded_capability_receipt.v1",
        "identity": IDENTITY,
        "factory_class_exact": True,
        "factory_closed": False,
        "active_replay_abi_sha256": EXACT_ACTIVE_REPLAY_ABI_SHA256,
        "runtime_source_set_sha256": canonical_sha256(dict(baseline.runtime_sources)),
        "execution_module_origin_keyset_sha256": canonical_sha256(sorted(baseline._module_origins)),
        "artifact_sha256": EXACT_E3_ARTIFACT_SHA256,
        "amended_layer4_day_receipts_sha256": LAYER4_DAY_RECEIPTS_SHA256,
        "owner_private_inputs_sha256": private_input_contract[
            "canonical_owner_private_inputs_sha256"
        ],
        "default_day_overlay_factory_executable": True,
        "support": {"reduced_support": True, "formal_day_count": 30},
        "availability": dict(AVAILABILITY),
        "permissions": dict(PERMISSIONS),
    }
    receipt["canonical_loaded_capability_sha256"] = document_sha256(
        receipt, "canonical_loaded_capability_sha256"
    )
    return MappingProxyType(receipt)


def load_owner_buy_e3_default_from_private_inputs(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    relative_locators: Mapping[str, Any],
    expected_private_input_contract: Mapping[str, Any] | None = None,
) -> tuple[OwnerBuyE3MechanicsBaseline, Mapping[str, Any], Mapping[str, Any]]:
    """Resolve the exact private manifest into the executable default E3 arm."""

    inputs = _resolve_owner_private_inputs(
        durable_evidence_root, metadata_repository_root, relative_locators
    )
    v13_commit = _validate_committed_v13_reconciliation(
        runtime_repository_root=runtime_repository_root,
        durable_evidence_root=durable_evidence_root,
        inputs=inputs,
    )
    contract = _owner_private_input_contract(inputs=inputs, v13_commit=v13_commit)
    if expected_private_input_contract is not None and dict(contract) != dict(
        expected_private_input_contract
    ):
        raise OwnerBuyE3MechanicsBaselineError("published owner-private input contract drifted")
    loaded = create_owner_buy_e3_backtest_mechanics_baseline(
        runtime_repository_root=runtime_repository_root,
        predecessor_v12_config_path=inputs.predecessor_v12_config,
        active_source_config_path=inputs.active_source_config,
        e3_artifact_paths=inputs.e3_artifact_paths,
        b0_artifact_paths=inputs.b0_artifact_paths,
        parity_evidence_paths=inputs.parity_evidence_paths,
    )
    try:
        capability = _baseline_capability_receipt(loaded, private_input_contract=contract)
    except BaseException:
        loaded.close()
        raise
    return loaded, contract, capability


def _cold_subprocess_capability_smoke(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    relative_locators: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Re-load the capability in a sanitized, isolated Python interpreter."""

    bootstrap = """
import contextlib
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
request = json.load(sys.stdin)
def execute():
    from research.families.f05_fill_quality_quote_ev.audit import causal_multichannel_window_boolean_cooldown_owner_buy_e3_backtest_mechanics_baseline_v1 as baseline
    from research.families.f05_fill_quality_quote_ev.audit import causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_b0_mechanics_adapter_v1 as b0_projection
    from research.families.f05_fill_quality_quote_ev.audit import causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as panel_builder
    loaded, contract, capability = baseline.load_owner_buy_e3_default_from_private_inputs(
        runtime_repository_root=Path(request["runtime_repository_root"]),
        durable_evidence_root=Path(request["durable_evidence_root"]),
        metadata_repository_root=Path(request["metadata_repository_root"]),
        relative_locators=request["relative_locators"],
    )
    try:
        defaults = panel_builder._default_cli_paths()
        inputs = panel_builder.validate_inputs(
            source_manifest_path=defaults["source_manifest"],
            book_view_root=defaults["book_view_root"],
            native_observation_manifest_path=defaults["native_observation_manifest"],
            native_observation_root=defaults["native_observation_root"],
            features_manifest_path=defaults["features_manifest"],
            owner_artifacts=panel_builder.OwnerArtifactPaths(
                policy=loaded._staged_b0_policy,
                predicate_bundle=loaded._staged_b0_bundle,
                private_config=loaded._staged_predecessor_source_config,
            ),
        )
        smoke_day = baseline.FORMAL_E3_MECHANICS_DAYS[0]
        day_request = panel_builder._day_request(inputs, smoke_day)
        replay = b0_projection._materialize_replay_inputs(day_request)
        overlay = loaded.build_day_overlay(day_request, replay, utc_day=smoke_day)
        if (
            overlay.params.get("cooldown_v2_snapshot_emitter") is not overlay.snapshot_emitter
            or overlay.params.get("cooldown_duration_policy_evaluator") is not overlay.compiled_evaluator
            or overlay.receipt.get("sell_delegates_exact_b0") is not True
            or overlay.receipt.get("d_plus_1_exact_b0_washout") is not True
        ):
            raise RuntimeError("cold day-overlay execution smoke drifted")
        root = Path(request["runtime_repository_root"]).absolute()
        origins = {}
        for name, module in tuple(sys.modules.items()):
            if not name.startswith(("live", "models", "research", "strategy", "data_paths")):
                continue
            raw = getattr(module, "__file__", None)
            if raw is None:
                continue
            origin = Path(raw).absolute()
            try:
                relative = origin.relative_to(root).as_posix()
            except ValueError as exc:
                raise RuntimeError(f"repo module escaped cold checkout: {name}") from exc
            origins[name] = relative
        return {
            "capability": dict(capability),
            "owner_private_inputs": dict(contract),
            "loaded_repo_module_count": len(origins),
            "loaded_repo_module_origins_sha256": baseline.canonical_sha256(origins),
            "all_loaded_repo_modules_within_cold_root": True,
            "day_overlay_smoke": {
                "utc_day": smoke_day,
                "artifact_sha256": overlay.receipt["artifact_sha256"],
                "canonical_day_overlay_sha256": overlay.receipt[
                    "canonical_day_overlay_sha256"
                ],
                "buy_e3_installed": True,
                "sell_delegates_exact_b0": True,
                "d_plus_1_exact_b0_washout": True,
            },
        }
    finally:
        loaded.close()
with contextlib.redirect_stdout(sys.stderr):
    output = execute()
print(json.dumps(output, sort_keys=True, separators=(",", ":"), allow_nan=False))
"""
    request = {
        "runtime_repository_root": str(Path(runtime_repository_root).absolute()),
        "durable_evidence_root": str(Path(durable_evidence_root).absolute()),
        "metadata_repository_root": str(Path(metadata_repository_root).absolute()),
        "relative_locators": dict(relative_locators),
    }
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    }
    for name in (
        "NARROWGATE_MARKETDATA_ROOT",
        "NARROWGATE_DATA_ROOT",
        "NARROWGATE_CACHE_ROOT",
        "NARROWGATE_REPLAY_DAG_CACHE_DIR",
        "NARROWGATE_STORAGE_ROOT",
        "NARROWGATE_EPHEMERAL_ROOT",
        "TMPDIR",
    ):
        value = str(os.environ.get(name, "") or "").strip()
        if value:
            environment[name] = value
    try:
        with tempfile.TemporaryDirectory(prefix="narrowgate-e3-smoke-pycache-") as cache:
            os.chmod(cache, 0o700)
            completed = subprocess.run(
                (
                    sys.executable,
                    "-I",
                    "-B",
                    "-X",
                    f"pycache_prefix={cache}",
                    "-c",
                    bootstrap,
                    str(Path(runtime_repository_root).absolute()),
                ),
                cwd=Path(runtime_repository_root).absolute(),
                env=environment,
                input=_canonical_bytes(request),
                check=True,
                capture_output=True,
            )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "cold isolated mechanics capability smoke failed"
        ) from exc
    output = _parse_strict_json(completed.stdout, label="cold isolated mechanics capability smoke")
    if (
        set(output)
        != {
            "capability",
            "owner_private_inputs",
            "loaded_repo_module_count",
            "loaded_repo_module_origins_sha256",
            "all_loaded_repo_modules_within_cold_root",
            "day_overlay_smoke",
        }
        or output.get("all_loaded_repo_modules_within_cold_root") is not True
        or not isinstance(output.get("loaded_repo_module_count"), int)
        or output.get("loaded_repo_module_count", 0) <= 0
        or not isinstance(output.get("day_overlay_smoke"), Mapping)
        or output.get("day_overlay_smoke", {}).get("buy_e3_installed") is not True
        or output.get("day_overlay_smoke", {}).get("sell_delegates_exact_b0") is not True
        or output.get("day_overlay_smoke", {}).get("d_plus_1_exact_b0_washout") is not True
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "cold isolated mechanics capability smoke identity drifted"
        )
    _require_sha(
        output.get("loaded_repo_module_origins_sha256"),
        "cold loaded repo module origin set SHA256",
    )
    return MappingProxyType(dict(output))


def _augment_capability_with_cold_smoke(
    capability: Mapping[str, Any], smoke: Mapping[str, Any]
) -> Mapping[str, Any]:
    if smoke.get("capability") != capability or smoke.get("owner_private_inputs", {}).get(
        "canonical_owner_private_inputs_sha256"
    ) != capability.get("owner_private_inputs_sha256"):
        raise OwnerBuyE3MechanicsBaselineError(
            "cold isolated capability differs from the publisher capability"
        )
    result = dict(capability)
    result.pop("canonical_loaded_capability_sha256", None)
    result["cold_isolated_subprocess"] = {
        "all_loaded_repo_modules_within_cold_root": True,
        "loaded_repo_module_count": smoke["loaded_repo_module_count"],
        "loaded_repo_module_origins_sha256": smoke["loaded_repo_module_origins_sha256"],
        "day_overlay_smoke": dict(smoke["day_overlay_smoke"]),
    }
    result["canonical_loaded_capability_sha256"] = document_sha256(
        result, "canonical_loaded_capability_sha256"
    )
    return MappingProxyType(result)


def _open_trusted_directory(path: Path, *, label: str, exact_mode: int | None = 0o700) -> int:
    """Open an owner-controlled directory through lexical no-follow ancestors."""

    parts = _lexical_parts(Path(path), label=label)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptors: list[int] = []
    try:
        current = os.open("/", flags)
        descriptors.append(current)
        for component in parts:
            try:
                child = os.open(component, flags, dir_fd=current)
            except FileNotFoundError as exc:
                raise OwnerBuyE3MechanicsBaselineError(f"{label} is missing") from exc
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise OwnerBuyE3MechanicsBaselineError(
                        f"{label} has a symlink or non-directory ancestor"
                    ) from exc
                raise
            descriptors.append(child)
            current = child
        metadata = os.fstat(current)
        observed_mode = stat.S_IMODE(metadata.st_mode)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or observed_mode & 0o022
            or (exact_mode is not None and observed_mode != exact_mode)
        ):
            raise OwnerBuyE3MechanicsBaselineError(f"{label} owner or permissions are unsafe")
        result = os.dup(current)
        os.set_inheritable(result, False)
        return result
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _publication_lock(parent_fd: int, target_name: str) -> int:
    lock_name = f".{target_name}.publish.lock"
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(
            lock_name,
            flags | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=parent_fd,
        )
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    except FileExistsError:
        try:
            descriptor = os.open(lock_name, flags, dir_fd=parent_fd)
        except OSError as exc:
            raise OwnerBuyE3MechanicsBaselineError("v14 publication lock is unsafe") from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise OwnerBuyE3MechanicsBaselineError("v14 publication lock is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _directory_binding(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _open_bundle_directory(
    parent_fd: int,
    name: str,
    *,
    create: bool,
) -> tuple[int, tuple[int, ...], bool]:
    created = False
    if create:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
            created = True
        except FileExistsError:
            pass
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 private bundle prefix is not a safe directory"
        ) from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        os.close(descriptor)
        raise OwnerBuyE3MechanicsBaselineError("v14 private bundle directory owner or mode drifted")
    return descriptor, _directory_binding(metadata), created


def _rebind_bundle_directory(
    parent_fd: int,
    name: str,
    expected: tuple[int, ...],
) -> None:
    try:
        observed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 private bundle path changed during publication"
        ) from exc
    if not stat.S_ISDIR(observed.st_mode) or _directory_binding(observed) != expected:
        raise OwnerBuyE3MechanicsBaselineError("v14 private bundle path changed during publication")


def _snapshot_private_file_at(
    directory_fd: int,
    name: str,
    *,
    label: str,
    allowed_nlinks: frozenset[int] = frozenset({1}),
    max_bytes: int = _MAX_PRIVATE_BYTES,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except FileNotFoundError as exc:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is missing") from exc
    except OSError as exc:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} is unsafe") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink not in allowed_nlinks
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise OwnerBuyE3MechanicsBaselineError(f"{label} metadata drifted")
        blocks: list[bytes] = []
        observed_size = 0
        while True:
            block = os.read(descriptor, min(1 << 20, max_bytes + 1 - observed_size))
            if not block:
                break
            blocks.append(block)
            observed_size += len(block)
            if observed_size > max_bytes:
                raise OwnerBuyE3MechanicsBaselineError(f"{label} size drifted")
        after = os.fstat(descriptor)
        if _identity(before) != _identity(after) or observed_size != before.st_size:
            raise OwnerBuyE3MechanicsBaselineError(f"{label} changed during read")
        lexical = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _identity(lexical) != _identity(before):
            raise OwnerBuyE3MechanicsBaselineError(f"{label} path changed during read")
        return b"".join(blocks), before
    finally:
        os.close(descriptor)


def _read_private_file_at(
    directory_fd: int,
    name: str,
    *,
    expected_sha256: str,
    expected_size: int,
    label: str,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    data, metadata = _snapshot_private_file_at(
        directory_fd,
        name,
        label=label,
        allowed_nlinks=allowed_nlinks,
        max_bytes=max(expected_size, 1),
    )
    if metadata.st_size != expected_size:
        raise OwnerBuyE3MechanicsBaselineError(f"{label} size drifted")
    if hashlib.sha256(data).hexdigest() != _require_sha(
        expected_sha256, f"{label} expected SHA256"
    ):
        raise OwnerBuyE3MechanicsBaselineError(f"{label} SHA256 drifted")
    return data, metadata


def _staging_prefix(name: str, expected_sha: str) -> str:
    return f".{name}.staging-{expected_sha}-"


def _write_staging_private_file(
    directory_fd: int,
    staging_name: str,
    data: bytes,
) -> None:
    descriptor = os.open(
        staging_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory_fd,
    )
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OwnerBuyE3MechanicsBaselineError("v14 pending private artifact write stalled")
            offset += written
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise OwnerBuyE3MechanicsBaselineError("v14 staging private artifact metadata drifted")
    finally:
        os.close(descriptor)


def _install_private_file_at(directory_fd: int, name: str, data: bytes) -> None:
    """Install exact bytes with the shared create-only staging protocol.

    Bytes are written and fsynced under a unique staging name before a hardlink
    atomically creates the deterministic pending name.  A deterministic pending
    is therefore always the complete expected file; mismatches are poison and
    are never deleted.  Only owner/0600/nlink1 orphan staging files may be
    removed, and only while both pending and final are absent under the bundle
    publication flock.
    """

    expected_sha = hashlib.sha256(data).hexdigest()
    pending_name = f".{name}.pending-{expected_sha}"
    entries = set(os.listdir(directory_fd))
    staging_prefix = _staging_prefix(name, expected_sha)
    staging_names = sorted(entry for entry in entries if entry.startswith(staging_prefix))
    if name in entries:
        if staging_names:
            raise OwnerBuyE3MechanicsBaselineError(
                f"v14 private artifact has orphan staging after publication: {name}"
            )
        if pending_name in entries:
            final_data, final_stat = _read_private_file_at(
                directory_fd,
                name,
                expected_sha256=expected_sha,
                expected_size=len(data),
                label=f"v14 private artifact {name}",
                allowed_nlinks=frozenset({2}),
            )
            pending_data, pending_stat = _read_private_file_at(
                directory_fd,
                pending_name,
                expected_sha256=expected_sha,
                expected_size=len(data),
                label=f"v14 pending artifact {name}",
                allowed_nlinks=frozenset({2}),
            )
            if (
                final_data != data
                or pending_data != data
                or (final_stat.st_dev, final_stat.st_ino)
                != (pending_stat.st_dev, pending_stat.st_ino)
            ):
                raise OwnerBuyE3MechanicsBaselineError(
                    f"v14 private artifact recovery conflict: {name}"
                )
            os.unlink(pending_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        observed, _metadata = _read_private_file_at(
            directory_fd,
            name,
            expected_sha256=expected_sha,
            expected_size=len(data),
            label=f"v14 private artifact {name}",
        )
        if observed != data:
            raise OwnerBuyE3MechanicsBaselineError(f"v14 private artifact content conflict: {name}")
        return
    if pending_name in entries:
        pending, pending_stat = _read_private_file_at(
            directory_fd,
            pending_name,
            expected_sha256=expected_sha,
            expected_size=len(data),
            label=f"v14 pending artifact {name}",
            allowed_nlinks=frozenset({1, 2}),
        )
        if pending != data:
            raise OwnerBuyE3MechanicsBaselineError(f"v14 pending artifact drifted: {name}")
        if pending_stat.st_nlink == 2:
            if len(staging_names) != 1:
                raise OwnerBuyE3MechanicsBaselineError(
                    f"v14 pending artifact staging recovery drifted: {name}"
                )
            staging, staging_stat = _read_private_file_at(
                directory_fd,
                staging_names[0],
                expected_sha256=expected_sha,
                expected_size=len(data),
                label=f"v14 staging artifact {name}",
                allowed_nlinks=frozenset({2}),
            )
            if staging != data or (pending_stat.st_dev, pending_stat.st_ino) != (
                staging_stat.st_dev,
                staging_stat.st_ino,
            ):
                raise OwnerBuyE3MechanicsBaselineError(
                    f"v14 pending artifact staging recovery conflict: {name}"
                )
            os.unlink(staging_names[0], dir_fd=directory_fd)
            os.fsync(directory_fd)
        elif staging_names:
            raise OwnerBuyE3MechanicsBaselineError(
                f"v14 pending artifact has orphan staging: {name}"
            )
    else:
        for staging_name in staging_names:
            metadata = os.stat(staging_name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise OwnerBuyE3MechanicsBaselineError(
                    f"v14 orphan staging artifact is unsafe: {name}"
                )
            os.unlink(staging_name, dir_fd=directory_fd)
        if staging_names:
            os.fsync(directory_fd)
        staging_name = f"{staging_prefix}{os.getpid()}-{os.urandom(16).hex()}"
        _write_staging_private_file(directory_fd, staging_name, data)
        os.fsync(directory_fd)
        try:
            os.link(
                staging_name,
                pending_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise OwnerBuyE3MechanicsBaselineError(
                f"v14 deterministic pending appeared concurrently: {name}"
            ) from exc
        os.fsync(directory_fd)
        staging, staging_stat = _read_private_file_at(
            directory_fd,
            staging_name,
            expected_sha256=expected_sha,
            expected_size=len(data),
            label=f"v14 staging artifact {name}",
            allowed_nlinks=frozenset({2}),
        )
        pending, pending_stat = _read_private_file_at(
            directory_fd,
            pending_name,
            expected_sha256=expected_sha,
            expected_size=len(data),
            label=f"v14 pending artifact {name}",
            allowed_nlinks=frozenset({2}),
        )
        if (
            staging != data
            or pending != data
            or (staging_stat.st_dev, staging_stat.st_ino)
            != (pending_stat.st_dev, pending_stat.st_ino)
        ):
            raise OwnerBuyE3MechanicsBaselineError(
                f"v14 staging-to-pending transition drifted: {name}"
            )
        os.unlink(staging_name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        _read_private_file_at(
            directory_fd,
            pending_name,
            expected_sha256=expected_sha,
            expected_size=len(data),
            label=f"v14 pending artifact {name}",
        )
    try:
        os.link(
            pending_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except FileExistsError:
        pass
    os.fsync(directory_fd)
    final, final_stat = _read_private_file_at(
        directory_fd,
        name,
        expected_sha256=expected_sha,
        expected_size=len(data),
        label=f"v14 private artifact {name}",
        allowed_nlinks=frozenset({2}),
    )
    pending, pending_stat = _read_private_file_at(
        directory_fd,
        pending_name,
        expected_sha256=expected_sha,
        expected_size=len(data),
        label=f"v14 pending artifact {name}",
        allowed_nlinks=frozenset({2}),
    )
    if (
        final != data
        or pending != data
        or (final_stat.st_dev, final_stat.st_ino) != (pending_stat.st_dev, pending_stat.st_ino)
    ):
        raise OwnerBuyE3MechanicsBaselineError(f"v14 private artifact install conflict: {name}")
    os.unlink(pending_name, dir_fd=directory_fd)
    os.fsync(directory_fd)
    _read_private_file_at(
        directory_fd,
        name,
        expected_sha256=expected_sha,
        expected_size=len(data),
        label=f"v14 private artifact {name}",
    )


def _serialized_document(document: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(document),
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 private document is not strict canonical JSON"
        ) from exc


def _public_contract_binding(
    contract_path: Path, *, expected_file_sha256: str
) -> Mapping[str, Any]:
    expected = _require_sha(expected_file_sha256, "public contract file SHA256")
    if expected != V14_PUBLIC_CONTRACT_FILE_SHA256:
        raise OwnerBuyE3MechanicsBaselineError(
            "public mechanics contract caller SHA256 is not the frozen contract"
        )
    snapshot = _secure_snapshot(
        contract_path,
        expected_sha256=expected,
        label="public mechanics contract",
        require_mode_0600=False,
        expected_mode=0o644,
    )
    document = _parse_strict_json(snapshot.data, label="public mechanics contract")
    if (
        document.get("schema_version") != SCHEMA_VERSION
        or document.get("identity") != IDENTITY
        or document.get("status") != "executable_fail_closed_mechanics_factory_contract"
        or document.get("canonical_contract_sha256")
        != document_sha256(document, "canonical_contract_sha256")
        or document.get("runtime_source_sha256") != RUNTIME_SOURCE_SHA256
        or document.get("availability") != AVAILABILITY
        or document.get("permissions") != PERMISSIONS
        or document.get("support", {}).get("reduced_support") is not True
        or document.get("support", {}).get("day_count") != 30
        or document.get("configuration", {}).get("active_source_file_sha256")
        != ACTIVE_SOURCE_CONFIG_FILE_SHA256
        or document.get("configuration", {}).get("host_neutral_projection_file_sha256")
        != HOST_NEUTRAL_CONFIG_FILE_SHA256
    ):
        raise OwnerBuyE3MechanicsBaselineError("public mechanics contract drifted")
    return {
        "identity": IDENTITY,
        "file_sha256": snapshot.sha256,
        "canonical_sha256": document["canonical_contract_sha256"],
    }


def _validate_v13_predecessor(
    path: Path, *, expected_file_sha256: str
) -> tuple[Mapping[str, Any], _Snapshot]:
    expected = _require_sha(expected_file_sha256, "v13 predecessor file SHA256")
    if expected != V13_PREDECESSOR_FILE_SHA256:
        raise OwnerBuyE3MechanicsBaselineError(
            "v13 predecessor caller SHA256 is not the frozen identity"
        )
    snapshot = _secure_snapshot(
        path,
        expected_sha256=expected,
        label="v13 predecessor identity",
        require_mode_0600=False,
        require_trusted_parent=False,
        expected_mode=0o644,
    )
    document = _parse_strict_json(snapshot.data, label="v13 predecessor identity")
    current_live = document.get("current_live")
    backtest_default = document.get("backtest_default")
    current_config = current_live.get("config") if isinstance(current_live, Mapping) else None
    if (
        document.get("schema_version") != V13_SCHEMA_VERSION
        or document.get("baseline_id") != V13_BASELINE_ID
        or document.get("effective_at_utc") != V13_EFFECTIVE_AT_UTC
        or document.get("operational_status") != V13_OPERATIONAL_STATUS
        or document.get("promotion_class") != V13_PROMOTION_CLASS
        or document.get("permissions") != V13_PERMISSIONS
        or not isinstance(current_live, Mapping)
        or not isinstance(current_config, Mapping)
        or current_config.get("sha256") != ACTIVE_SOURCE_CONFIG_FILE_SHA256
        or current_live.get("buy_e3_enabled") is not True
        or current_live.get("economic_outcomes_read") is not False
        or current_live.get("economic_values_persisted") is not False
        or current_live.get("private_release_and_evidence_chain_remain_authority") is not True
        or not isinstance(backtest_default, Mapping)
        or backtest_default.get("config_sha256") != PREDECESSOR_V12_CONFIG_FILE_SHA256
        or backtest_default.get("exact_buy_e3_replay_baseline_available") is not False
        or backtest_default.get("current_live_config_may_replace_backtest_default") is not False
        or backtest_default.get("current_live_evidence_is_backtest_economic_authority") is not False
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 predecessor is not the active locator-only v13 identity"
        )
    return document, snapshot


def create_v14_mechanics_governance_receipt(
    *,
    predecessor_v13_identity_path: Path,
    predecessor_v13_file_sha256: str,
    public_contract_path: Path,
    public_contract_file_sha256: str,
    effective_at_utc: str,
) -> Mapping[str, Any]:
    """Build, but do not publish, the sequential v14 mechanics receipt."""

    effective = _strict_utc_z(effective_at_utc, label="v14 effective_at_utc")
    if effective != V14_EFFECTIVE_AT_UTC:
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 effective_at_utc is not the frozen sequential activation instant"
        )
    predecessor, predecessor_snapshot = _validate_v13_predecessor(
        predecessor_v13_identity_path,
        expected_file_sha256=predecessor_v13_file_sha256,
    )
    contract = _public_contract_binding(
        public_contract_path,
        expected_file_sha256=public_contract_file_sha256,
    )
    receipt: dict[str, Any] = {
        "schema_version": V14_GOVERNANCE_SCHEMA,
        "identity": f"{IDENTITY}.v14_owner_requested_mechanics_only",
        "status": "sequential_v14_mechanics_default_active",
        "successor_baseline_version": "v14",
        "promotion_class": "owner_requested_mechanics_only",
        "effective_at_utc": effective,
        "predecessor": {
            "schema_version": predecessor["schema_version"],
            "baseline_id": predecessor["baseline_id"],
            "operational_status": predecessor["operational_status"],
            "promotion_class": predecessor["promotion_class"],
            "identity_file_sha256": predecessor_snapshot.sha256,
            "identity_canonical_sha256": canonical_sha256(predecessor),
            "authority_split_sha256": canonical_sha256(
                {
                    "current_live": predecessor["current_live"],
                    "backtest_default": predecessor["backtest_default"],
                }
            ),
            "locator_reconciliation_only_predecessor": True,
            "historical_identity_modified": False,
        },
        "mechanics_contract": dict(contract),
        "default_arm": {
            "identity": IDENTITY,
            "candidate_only": False,
            "resolver_activated": True,
            "target_side": "BUY",
            "opposite_side_delegation": "exact_owner_B0",
            "formal_e3_mechanics_panel_day_count": len(FORMAL_E3_MECHANICS_DAYS),
            "reduced_support": True,
            "current_v12_50_day_economic_control_replaced": False,
        },
        "config": {
            "active_source_file_sha256": ACTIVE_SOURCE_CONFIG_FILE_SHA256,
            "host_neutral_projection_mapping_sha256": (HOST_NEUTRAL_CONFIG_MAPPING_SHA256),
            "host_neutral_projection_file_sha256": HOST_NEUTRAL_CONFIG_FILE_SHA256,
            "source_config_delta_paths_sha256": SOURCE_CONFIG_DELTA_PATHS_SHA256,
            "source_replay_abi_delta_paths_sha256": (REPLAY_ABI_SOURCE_DELTA_PATHS_SHA256),
            "final_replay_abi_delta_paths_sha256": (REPLAY_ABI_FINAL_DELTA_PATHS_SHA256),
            "projection_availability": "private_not_distributed",
        },
        "permissions": dict(V14_PERMISSIONS),
    }
    receipt["canonical_governance_receipt_sha256"] = document_sha256(
        receipt, "canonical_governance_receipt_sha256"
    )
    return MappingProxyType(receipt)


def validate_v14_mechanics_governance_receipt(
    receipt: Mapping[str, Any],
    *,
    predecessor_v13_identity_path: Path,
    predecessor_v13_file_sha256: str,
    public_contract_path: Path,
    public_contract_file_sha256: str,
) -> Mapping[str, Any]:
    expected_predecessor = _require_sha(predecessor_v13_file_sha256, "v13 predecessor file SHA256")
    expected_contract = _require_sha(public_contract_file_sha256, "public contract file SHA256")
    predecessor_document, predecessor_snapshot = _validate_v13_predecessor(
        predecessor_v13_identity_path,
        expected_file_sha256=expected_predecessor,
    )
    contract_binding = _public_contract_binding(
        public_contract_path,
        expected_file_sha256=expected_contract,
    )
    predecessor = receipt.get("predecessor")
    contract = receipt.get("mechanics_contract")
    default_arm = receipt.get("default_arm")
    config = receipt.get("config")
    effective = _strict_utc_z(receipt.get("effective_at_utc"), label="v14 effective_at_utc")
    if (
        receipt.get("schema_version") != V14_GOVERNANCE_SCHEMA
        or receipt.get("status") != "sequential_v14_mechanics_default_active"
        or receipt.get("successor_baseline_version") != "v14"
        or receipt.get("promotion_class") != "owner_requested_mechanics_only"
        or not isinstance(predecessor, Mapping)
        or predecessor.get("schema_version") != V13_SCHEMA_VERSION
        or predecessor.get("baseline_id") != V13_BASELINE_ID
        or predecessor.get("operational_status") != V13_OPERATIONAL_STATUS
        or predecessor.get("promotion_class") != V13_PROMOTION_CLASS
        or predecessor.get("identity_file_sha256") != expected_predecessor
        or predecessor.get("identity_canonical_sha256") != canonical_sha256(predecessor_document)
        or predecessor.get("authority_split_sha256")
        != canonical_sha256(
            {
                "current_live": predecessor_document["current_live"],
                "backtest_default": predecessor_document["backtest_default"],
            }
        )
        or predecessor.get("identity_file_sha256") != predecessor_snapshot.sha256
        or predecessor.get("locator_reconciliation_only_predecessor") is not True
        or not isinstance(contract, Mapping)
        or contract.get("identity") != IDENTITY
        or contract.get("file_sha256") != expected_contract
        or dict(contract) != dict(contract_binding)
        or not isinstance(default_arm, Mapping)
        or default_arm.get("candidate_only") is not False
        or default_arm.get("resolver_activated") is not True
        or default_arm.get("reduced_support") is not True
        or default_arm.get("formal_e3_mechanics_panel_day_count") != 30
        or default_arm.get("current_v12_50_day_economic_control_replaced") is not False
        or not isinstance(config, Mapping)
        or config.get("active_source_file_sha256") != ACTIVE_SOURCE_CONFIG_FILE_SHA256
        or config.get("host_neutral_projection_file_sha256") != HOST_NEUTRAL_CONFIG_FILE_SHA256
        or receipt.get("permissions") != V14_PERMISSIONS
        or receipt.get("canonical_governance_receipt_sha256")
        != document_sha256(receipt, "canonical_governance_receipt_sha256")
    ):
        raise OwnerBuyE3MechanicsBaselineError("v14 mechanics governance receipt drifted")
    expected_receipt = create_v14_mechanics_governance_receipt(
        predecessor_v13_identity_path=predecessor_v13_identity_path,
        predecessor_v13_file_sha256=expected_predecessor,
        public_contract_path=public_contract_path,
        public_contract_file_sha256=expected_contract,
        effective_at_utc=effective,
    )
    if dict(receipt) != dict(expected_receipt):
        raise OwnerBuyE3MechanicsBaselineError(
            "v14 mechanics governance receipt has extra or missing fields"
        )
    return dict(receipt)


def _validate_cold_publisher(value: Mapping[str, Any]) -> Mapping[str, str]:
    expected_keys = {
        "execution_commit",
        "execution_tree",
        "annotated_tag",
        "annotated_tag_object",
        "factory_git_blob",
        "factory_source_sha256",
        "runtime_source_set_sha256",
        "execution_module_origin_keyset_sha256",
    }
    if set(value) != expected_keys or value.get("annotated_tag") != V14_COLD_PUBLISHER_TAG:
        raise OwnerBuyE3MechanicsBaselineError("cold publisher identity drifted")
    git_object_re = re.compile(r"^[0-9a-f]{40,64}$")
    for field in (
        "execution_commit",
        "execution_tree",
        "annotated_tag_object",
        "factory_git_blob",
    ):
        if git_object_re.fullmatch(str(value.get(field, ""))) is None:
            raise OwnerBuyE3MechanicsBaselineError(f"cold publisher {field} is not a Git object")
    for field in (
        "factory_source_sha256",
        "runtime_source_set_sha256",
        "execution_module_origin_keyset_sha256",
    ):
        _require_sha(value.get(field), f"cold publisher {field}")
    if value.get("runtime_source_set_sha256") != canonical_sha256(dict(RUNTIME_SOURCE_SHA256)):
        raise OwnerBuyE3MechanicsBaselineError("cold publisher runtime source set drifted")
    return MappingProxyType({key: str(value[key]) for key in sorted(expected_keys)})


def capture_cold_publisher(repository_root: Path, *, annotated_tag: str) -> Mapping[str, str]:
    """Bind a clean annotated-tag checkout without following the root locator."""

    root = Path(repository_root).absolute()
    _lexical_parts(root, label="cold publisher repository root")
    if annotated_tag != V14_COLD_PUBLISHER_TAG:
        raise OwnerBuyE3MechanicsBaselineError("cold publisher tag identity drifted")
    git_environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_")
        and key
        not in {
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        }
    }
    git_environment["GIT_NO_REPLACE_OBJECTS"] = "1"
    git_environment["GIT_OPTIONAL_LOCKS"] = "0"

    def git_text(*arguments: str) -> str:
        try:
            result = subprocess.run(
                ("git", *arguments),
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
                env=git_environment,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise OwnerBuyE3MechanicsBaselineError("cold publisher Git binding failed") from exc
        return result.stdout.strip()

    def git_bytes(*arguments: str) -> bytes:
        try:
            result = subprocess.run(
                ("git", *arguments),
                cwd=root,
                check=True,
                capture_output=True,
                env=git_environment,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise OwnerBuyE3MechanicsBaselineError("cold publisher Git binding failed") from exc
        return result.stdout

    origin_before = _verify_execution_module_origins(root)
    runtime_before = _verify_runtime_sources(root)
    if Path(git_text("rev-parse", "--show-toplevel")).absolute() != root:
        raise OwnerBuyE3MechanicsBaselineError("cold publisher Git worktree root drifted")
    if git_text("status", "--porcelain=v1", "--untracked-files=all"):
        raise OwnerBuyE3MechanicsBaselineError("private publisher checkout is dirty")
    head = git_text("rev-parse", "HEAD")
    tree = git_text("rev-parse", "HEAD^{tree}")
    tag_ref = f"refs/tags/{annotated_tag}"
    if git_text("cat-file", "-t", tag_ref) != "tag":
        raise OwnerBuyE3MechanicsBaselineError("private publisher tag is not annotated")
    tag_object = git_text("rev-parse", tag_ref)
    if git_text("rev-parse", f"{tag_ref}^{{commit}}") != head:
        raise OwnerBuyE3MechanicsBaselineError("private publisher tag does not peel to HEAD")
    factory_relative = (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1.py"
    )
    factory_blob = git_bytes("show", f"HEAD:{factory_relative}")
    factory_sha = hashlib.sha256(factory_blob).hexdigest()
    factory_snapshot = _secure_snapshot(
        root / factory_relative,
        expected_sha256=factory_sha,
        label="cold publisher mechanics factory source",
        require_mode_0600=False,
        expected_mode=0o644,
    )
    if factory_snapshot.data != factory_blob:
        raise OwnerBuyE3MechanicsBaselineError("cold publisher factory blob drifted")
    origin_after = _verify_execution_module_origins(root)
    runtime_after = _verify_runtime_sources(root)
    if (
        origin_before != origin_after
        or runtime_before != runtime_after
        or Path(git_text("rev-parse", "--show-toplevel")).absolute() != root
        or git_text("rev-parse", "HEAD") != head
        or git_text("rev-parse", "HEAD^{tree}") != tree
        or git_text("rev-parse", tag_ref) != tag_object
        or git_text("status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise OwnerBuyE3MechanicsBaselineError(
            "cold publisher execution sources changed during capture"
        )
    return _validate_cold_publisher(
        {
            "execution_commit": head,
            "execution_tree": tree,
            "annotated_tag": annotated_tag,
            "annotated_tag_object": tag_object,
            "factory_git_blob": git_text("rev-parse", f"HEAD:{factory_relative}"),
            "factory_source_sha256": factory_sha,
            "runtime_source_set_sha256": canonical_sha256(dict(runtime_before)),
            "execution_module_origin_keyset_sha256": canonical_sha256(sorted(origin_before)),
        }
    )


_DEFAULT_BUNDLE_FILE_NAMES: Final = (
    "transaction.json",
    "owner_private_inputs.json",
    "config.host_neutral.replay_projection.json",
    "loaded_capability_receipt.json",
    "v14_mechanics_governance_receipt.json",
    "manifest.json",
)


def _default_bundle_payloads(
    *,
    private_input_contract: Mapping[str, Any],
    projection_bytes: bytes,
    capability_receipt: Mapping[str, Any],
    governance_receipt: Mapping[str, Any],
    public_contract_binding: Mapping[str, Any],
    cold_publisher: Mapping[str, Any],
) -> tuple[Mapping[str, bytes], Mapping[str, Any]]:
    publisher = _validate_cold_publisher(cold_publisher)
    owner_inputs_bytes = _serialized_document(private_input_contract)
    capability_bytes = _serialized_document(capability_receipt)
    governance_bytes = _serialized_document(governance_receipt)
    if (
        hashlib.sha256(projection_bytes).hexdigest() != HOST_NEUTRAL_CONFIG_FILE_SHA256
        or len(projection_bytes) != HOST_NEUTRAL_CONFIG_SIZE_BYTES
    ):
        raise OwnerBuyE3MechanicsBaselineError("published host-neutral projection drifted")
    transaction: dict[str, Any] = {
        "schema_version": V14_PRIVATE_TRANSACTION_SCHEMA,
        "identity": IDENTITY,
        "state_machine": "locked_create_only_files_then_manifest_last",
        "effective_at_utc": V14_EFFECTIVE_AT_UTC,
        "owner_private_inputs_file_sha256": hashlib.sha256(owner_inputs_bytes).hexdigest(),
        "loaded_capability_file_sha256": hashlib.sha256(capability_bytes).hexdigest(),
        "governance_receipt_file_sha256": hashlib.sha256(governance_bytes).hexdigest(),
        "host_neutral_projection_file_sha256": HOST_NEUTRAL_CONFIG_FILE_SHA256,
        "mechanics_contract_file_sha256": public_contract_binding["file_sha256"],
        "publisher_execution_tree": publisher["execution_tree"],
        "publisher_annotated_tag_object": publisher["annotated_tag_object"],
    }
    transaction["canonical_transaction_sha256"] = document_sha256(
        transaction, "canonical_transaction_sha256"
    )
    transaction_bytes = _serialized_document(transaction)
    pre_manifest: dict[str, bytes] = {
        "transaction.json": transaction_bytes,
        "owner_private_inputs.json": owner_inputs_bytes,
        "config.host_neutral.replay_projection.json": projection_bytes,
        "loaded_capability_receipt.json": capability_bytes,
        "v14_mechanics_governance_receipt.json": governance_bytes,
    }
    artifacts = {
        name.removesuffix(".json"): {
            "file_name": name,
            "file_sha256": hashlib.sha256(data).hexdigest(),
            "size_bytes": len(data),
            "mode": "0600",
            "nlink": 1,
        }
        for name, data in pre_manifest.items()
    }
    manifest: dict[str, Any] = {
        "schema_version": V14_PRIVATE_MANIFEST_SCHEMA,
        "identity": IDENTITY,
        "status": "current_default_buy_e3_mechanics_bundle_complete",
        "effective_at_utc": V14_EFFECTIVE_AT_UTC,
        "publisher": dict(publisher),
        "mechanics_contract": dict(public_contract_binding),
        "owner_private_inputs": {
            "canonical_sha256": private_input_contract["canonical_owner_private_inputs_sha256"],
            "input_count": OWNER_PRIVATE_INPUT_COUNT,
            "absolute_locator_persisted": False,
        },
        "loaded_capability": {
            "canonical_sha256": capability_receipt["canonical_loaded_capability_sha256"],
            "default_day_overlay_factory_executable": True,
        },
        "governance": {
            "canonical_sha256": governance_receipt["canonical_governance_receipt_sha256"],
            "predecessor_v13_committed": True,
            "resolver_activated": True,
        },
        "artifacts": artifacts,
        "support": {
            "formal_e3_mechanics_panel": True,
            "reduced_support": True,
            "day_count": 30,
            "v12_50_day_economic_control_retained": True,
        },
        "availability": "private_not_distributed",
        "permissions": dict(V14_PERMISSIONS),
    }
    manifest["canonical_manifest_sha256"] = document_sha256(manifest, "canonical_manifest_sha256")
    all_files = dict(pre_manifest)
    all_files["manifest.json"] = _serialized_document(manifest)
    return MappingProxyType(all_files), MappingProxyType(manifest)


def _read_complete_default_bundle(destination: Path) -> Mapping[str, bytes]:
    target = Path(destination)
    parts = _lexical_parts(target, label="default mechanics private bundle")
    parent_fd = _open_trusted_directory(
        target.parent, label="default mechanics private bundle parent", exact_mode=0o700
    )
    lock_fd = _publication_lock(parent_fd, parts[-1])
    bundle_fd = -1
    try:
        bundle_fd, binding, _created = _open_bundle_directory(parent_fd, parts[-1], create=False)
        if set(os.listdir(bundle_fd)) != set(_DEFAULT_BUNDLE_FILE_NAMES):
            raise OwnerBuyE3MechanicsBaselineError("default mechanics private bundle is incomplete")
        observed: dict[str, bytes] = {}
        for name in _DEFAULT_BUNDLE_FILE_NAMES:
            data, _metadata = _snapshot_private_file_at(
                bundle_fd, name, label=f"default mechanics bundle {name}"
            )
            _parse_strict_json(data, label=f"default mechanics bundle {name}")
            observed[name] = data
        manifest = _parse_strict_json(
            observed["manifest.json"], label="default mechanics bundle manifest"
        )
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise OwnerBuyE3MechanicsBaselineError("default bundle artifact index is missing")
        for name in _DEFAULT_BUNDLE_FILE_NAMES[:-1]:
            row = artifacts.get(name.removesuffix(".json"))
            data = observed[name]
            if (
                not isinstance(row, Mapping)
                or row.get("file_name") != name
                or row.get("file_sha256") != hashlib.sha256(data).hexdigest()
                or row.get("size_bytes") != len(data)
                or row.get("mode") != "0600"
                or row.get("nlink") != 1
            ):
                raise OwnerBuyE3MechanicsBaselineError(
                    f"default bundle artifact binding drifted: {name}"
                )
        _rebind_bundle_directory(parent_fd, parts[-1], binding)
        return MappingProxyType(observed)
    finally:
        if bundle_fd >= 0:
            os.close(bundle_fd)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
        os.close(parent_fd)


def _relative_locators_from_contract(contract: Mapping[str, Any]) -> Mapping[str, str]:
    expected_bases = {
        "durable_evidence": {
            "environment_variable": PRIVATE_EVIDENCE_ROOT_ENV,
            "roles": [
                role for role in OWNER_PRIVATE_INPUT_ROLES if role not in OWNER_METADATA_INPUT_ROLES
            ],
        },
        "metadata_repository": {
            "environment_variable": METADATA_REPOSITORY_ROOT_ENV,
            "roles": [
                role for role in OWNER_PRIVATE_INPUT_ROLES if role in OWNER_METADATA_INPUT_ROLES
            ],
        },
        "absolute_locator_persisted": False,
    }
    if (
        contract.get("schema_version") != OWNER_PRIVATE_INPUT_SCHEMA
        or contract.get("status") != "exact_owner_private_relative_inputs_recursively_bound"
        or contract.get("input_count") != OWNER_PRIVATE_INPUT_COUNT
        or contract.get("locator_bases") != expected_bases
        or contract.get("canonical_owner_private_inputs_sha256")
        != document_sha256(contract, "canonical_owner_private_inputs_sha256")
        or contract.get("permissions") != PERMISSIONS
    ):
        raise OwnerBuyE3MechanicsBaselineError("owner-private input contract drifted")
    entries = contract.get("inputs")
    if not isinstance(entries, Mapping) or set(entries) != set(OWNER_PRIVATE_INPUT_ROLES):
        raise OwnerBuyE3MechanicsBaselineError("owner-private input role set drifted")
    locators: dict[str, str] = {}
    for role in OWNER_PRIVATE_INPUT_ROLES:
        entry = entries[role]
        expected_base = (
            "metadata_repository" if role in OWNER_METADATA_INPUT_ROLES else "durable_evidence"
        )
        if (
            not isinstance(entry, Mapping)
            or entry.get("locator_base") != expected_base
            or entry.get("absolute_locator_persisted") is not False
        ):
            raise OwnerBuyE3MechanicsBaselineError("owner-private locator authority drifted")
        locators[role] = _private_relative_locator(entry.get("relative_locator"), role=role)
    return MappingProxyType(locators)


def _validate_default_bundle_documents(
    *,
    observed: Mapping[str, bytes],
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    predecessor_v13_identity_path: Path,
    public_contract_path: Path,
    cold_repository_root: Path,
) -> tuple[OwnerBuyE3MechanicsBaseline, Mapping[str, Any]]:
    private_inputs = _parse_strict_json(
        observed["owner_private_inputs.json"], label="published owner-private inputs"
    )
    relative_locators = _relative_locators_from_contract(private_inputs)
    loaded, rebound_inputs, capability = load_owner_buy_e3_default_from_private_inputs(
        runtime_repository_root=runtime_repository_root,
        durable_evidence_root=durable_evidence_root,
        metadata_repository_root=metadata_repository_root,
        relative_locators=relative_locators,
        expected_private_input_contract=private_inputs,
    )
    try:
        published_capability = _parse_strict_json(
            observed["loaded_capability_receipt.json"],
            label="published loaded capability receipt",
        )
        published_body = dict(published_capability)
        published_body.pop("canonical_loaded_capability_sha256", None)
        cold_smoke = published_body.pop("cold_isolated_subprocess", None)
        capability_body = dict(capability)
        capability_body.pop("canonical_loaded_capability_sha256", None)
        if (
            published_body != capability_body
            or not isinstance(cold_smoke, Mapping)
            or cold_smoke.get("all_loaded_repo_modules_within_cold_root") is not True
            or not isinstance(cold_smoke.get("loaded_repo_module_count"), int)
            or cold_smoke.get("loaded_repo_module_count", 0) <= 0
            or _SHA_RE.fullmatch(str(cold_smoke.get("loaded_repo_module_origins_sha256", "")))
            is None
            or not isinstance(cold_smoke.get("day_overlay_smoke"), Mapping)
            or cold_smoke.get("day_overlay_smoke", {}).get("utc_day") != FORMAL_E3_MECHANICS_DAYS[0]
            or cold_smoke.get("day_overlay_smoke", {}).get("artifact_sha256")
            != EXACT_E3_ARTIFACT_SHA256
            or _SHA_RE.fullmatch(
                str(cold_smoke.get("day_overlay_smoke", {}).get("canonical_day_overlay_sha256", ""))
            )
            is None
            or cold_smoke.get("day_overlay_smoke", {}).get("buy_e3_installed") is not True
            or cold_smoke.get("day_overlay_smoke", {}).get("sell_delegates_exact_b0") is not True
            or cold_smoke.get("day_overlay_smoke", {}).get("d_plus_1_exact_b0_washout") is not True
            or published_capability.get("canonical_loaded_capability_sha256")
            != document_sha256(published_capability, "canonical_loaded_capability_sha256")
        ):
            raise OwnerBuyE3MechanicsBaselineError("published loaded capability receipt drifted")
        capability = published_capability
        governance = _parse_strict_json(
            observed["v14_mechanics_governance_receipt.json"],
            label="published v14 mechanics governance receipt",
        )
        validated_governance = validate_v14_mechanics_governance_receipt(
            governance,
            predecessor_v13_identity_path=predecessor_v13_identity_path,
            predecessor_v13_file_sha256=V13_PREDECESSOR_FILE_SHA256,
            public_contract_path=public_contract_path,
            public_contract_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
        )
        public_contract = _public_contract_binding(
            public_contract_path,
            expected_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
        )
        cold = capture_cold_publisher(cold_repository_root, annotated_tag=V14_COLD_PUBLISHER_TAG)
        expected, manifest = _default_bundle_payloads(
            private_input_contract=rebound_inputs,
            projection_bytes=loaded.projected_config_bytes,
            capability_receipt=capability,
            governance_receipt=validated_governance,
            public_contract_binding=public_contract,
            cold_publisher=cold,
        )
        if dict(observed) != dict(expected):
            raise OwnerBuyE3MechanicsBaselineError(
                "default mechanics private bundle recursive identity drifted"
            )
        return loaded, manifest
    except BaseException:
        loaded.close()
        raise


def validate_v14_private_bundle(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    predecessor_v13_identity_path: Path,
    public_contract_path: Path,
    cold_repository_root: Path,
) -> Mapping[str, Any]:
    """Recursively validate the immutable current default mechanics bundle."""

    destination = (
        Path(durable_evidence_root).absolute().joinpath(*FORMAL_PRIVATE_BUNDLE_RELATIVE.parts)
    )
    first = _read_complete_default_bundle(destination)
    loaded, manifest = _validate_default_bundle_documents(
        observed=first,
        runtime_repository_root=runtime_repository_root,
        durable_evidence_root=durable_evidence_root,
        metadata_repository_root=metadata_repository_root,
        predecessor_v13_identity_path=predecessor_v13_identity_path,
        public_contract_path=public_contract_path,
        cold_repository_root=cold_repository_root,
    )
    try:
        second = _read_complete_default_bundle(destination)
        if dict(first) != dict(second):
            raise OwnerBuyE3MechanicsBaselineError(
                "default mechanics bundle changed during recursive validation"
            )
        return MappingProxyType(dict(manifest))
    finally:
        loaded.close()


def load_published_owner_buy_e3_default(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    predecessor_v13_identity_path: Path,
    public_contract_path: Path,
    cold_repository_root: Path,
) -> OwnerBuyE3MechanicsBaseline:
    """Load the current default E3 mechanics arm; never fall back to v12."""

    destination = (
        Path(durable_evidence_root).absolute().joinpath(*FORMAL_PRIVATE_BUNDLE_RELATIVE.parts)
    )
    first = _read_complete_default_bundle(destination)
    loaded, _manifest = _validate_default_bundle_documents(
        observed=first,
        runtime_repository_root=runtime_repository_root,
        durable_evidence_root=durable_evidence_root,
        metadata_repository_root=metadata_repository_root,
        predecessor_v13_identity_path=predecessor_v13_identity_path,
        public_contract_path=public_contract_path,
        cold_repository_root=cold_repository_root,
    )
    try:
        if dict(first) != dict(_read_complete_default_bundle(destination)):
            raise OwnerBuyE3MechanicsBaselineError("default mechanics bundle changed while loading")
        return loaded
    except BaseException:
        loaded.close()
        raise


def publish_v14_private_bundle(
    *,
    runtime_repository_root: Path,
    durable_evidence_root: Path,
    metadata_repository_root: Path,
    relative_locators: Mapping[str, Any],
    predecessor_v13_identity_path: Path,
    public_contract_path: Path,
    cold_repository_root: Path,
    _failure_hook: Any = None,
) -> Mapping[str, Any]:
    """Publish one recoverable, create-only, manifest-last default bundle."""

    durable_root = Path(durable_evidence_root).absolute()
    durable_fd = _open_trusted_directory(
        durable_root, label="owner-private durable evidence root", exact_mode=0o700
    )
    os.close(durable_fd)
    destination = durable_root.joinpath(*FORMAL_PRIVATE_BUNDLE_RELATIVE.parts)
    if destination.parent.name != "direct_no_shadow_live_evidence_v6_20260824":
        raise OwnerBuyE3MechanicsBaselineError("formal private bundle locator drifted")
    parts = _lexical_parts(destination, label="formal default mechanics bundle")
    parent_fd = _open_trusted_directory(
        destination.parent,
        label="formal default mechanics bundle parent",
        exact_mode=0o700,
    )
    lock_fd = _publication_lock(parent_fd, parts[-1])
    bundle_fd = -1
    loaded: OwnerBuyE3MechanicsBaseline | None = None
    try:
        bundle_fd, binding, _created = _open_bundle_directory(parent_fd, parts[-1], create=True)
        cold = capture_cold_publisher(cold_repository_root, annotated_tag=V14_COLD_PUBLISHER_TAG)
        loaded, private_inputs, capability = load_owner_buy_e3_default_from_private_inputs(
            runtime_repository_root=runtime_repository_root,
            durable_evidence_root=durable_evidence_root,
            metadata_repository_root=metadata_repository_root,
            relative_locators=relative_locators,
        )
        capability = _augment_capability_with_cold_smoke(
            capability,
            _cold_subprocess_capability_smoke(
                runtime_repository_root=runtime_repository_root,
                durable_evidence_root=durable_evidence_root,
                metadata_repository_root=metadata_repository_root,
                relative_locators=relative_locators,
            ),
        )
        governance = create_v14_mechanics_governance_receipt(
            predecessor_v13_identity_path=predecessor_v13_identity_path,
            predecessor_v13_file_sha256=V13_PREDECESSOR_FILE_SHA256,
            public_contract_path=public_contract_path,
            public_contract_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
            effective_at_utc=V14_EFFECTIVE_AT_UTC,
        )
        public_contract = _public_contract_binding(
            public_contract_path,
            expected_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
        )
        files, manifest = _default_bundle_payloads(
            private_input_contract=private_inputs,
            projection_bytes=loaded.projected_config_bytes,
            capability_receipt=capability,
            governance_receipt=governance,
            public_contract_binding=public_contract,
            cold_publisher=cold,
        )
        entries = set(os.listdir(bundle_fd))
        allowed = set(_DEFAULT_BUNDLE_FILE_NAMES)
        allowed |= {
            f".{name}.pending-{hashlib.sha256(data).hexdigest()}" for name, data in files.items()
        }
        recognized_staging = {
            entry
            for entry in entries
            if any(
                entry.startswith(_staging_prefix(name, hashlib.sha256(data).hexdigest()))
                for name, data in files.items()
            )
        }
        if entries - allowed - recognized_staging:
            raise OwnerBuyE3MechanicsBaselineError(
                "default mechanics bundle prefix has conflicting entries"
            )
        if "manifest.json" in entries:
            manifest_data = files["manifest.json"]
            manifest_pending = ".manifest.json.pending-" + hashlib.sha256(manifest_data).hexdigest()
            if entries - set(_DEFAULT_BUNDLE_FILE_NAMES) - {manifest_pending}:
                raise OwnerBuyE3MechanicsBaselineError(
                    "committed default bundle has a non-final prefix"
                )
            _install_private_file_at(bundle_fd, "manifest.json", manifest_data)
            _rebind_bundle_directory(parent_fd, parts[-1], binding)
            entries = set(os.listdir(bundle_fd))
            if entries != set(_DEFAULT_BUNDLE_FILE_NAMES):
                raise OwnerBuyE3MechanicsBaselineError(
                    "committed default bundle has a non-final prefix"
                )
            for name, data in files.items():
                observed, _metadata = _read_private_file_at(
                    bundle_fd,
                    name,
                    expected_sha256=hashlib.sha256(data).hexdigest(),
                    expected_size=len(data),
                    label=f"committed default bundle {name}",
                )
                if observed != data:
                    raise OwnerBuyE3MechanicsBaselineError(
                        f"committed default bundle drifted: {name}"
                    )
            return MappingProxyType(dict(manifest))
        for name in _DEFAULT_BUNDLE_FILE_NAMES[:-1]:
            _install_private_file_at(bundle_fd, name, files[name])
            _rebind_bundle_directory(parent_fd, parts[-1], binding)
            if _failure_hook is not None:
                _failure_hook(name)
        loaded.close()
        loaded = None
        verified, second_inputs, second_capability = load_owner_buy_e3_default_from_private_inputs(
            runtime_repository_root=runtime_repository_root,
            durable_evidence_root=durable_evidence_root,
            metadata_repository_root=metadata_repository_root,
            relative_locators=relative_locators,
        )
        try:
            second_capability = _augment_capability_with_cold_smoke(
                second_capability,
                _cold_subprocess_capability_smoke(
                    runtime_repository_root=runtime_repository_root,
                    durable_evidence_root=durable_evidence_root,
                    metadata_repository_root=metadata_repository_root,
                    relative_locators=relative_locators,
                ),
            )
            second_cold = capture_cold_publisher(
                cold_repository_root, annotated_tag=V14_COLD_PUBLISHER_TAG
            )
            second_governance = create_v14_mechanics_governance_receipt(
                predecessor_v13_identity_path=predecessor_v13_identity_path,
                predecessor_v13_file_sha256=V13_PREDECESSOR_FILE_SHA256,
                public_contract_path=public_contract_path,
                public_contract_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
                effective_at_utc=V14_EFFECTIVE_AT_UTC,
            )
            second_contract = _public_contract_binding(
                public_contract_path,
                expected_file_sha256=V14_PUBLIC_CONTRACT_FILE_SHA256,
            )
            second_files, second_manifest = _default_bundle_payloads(
                private_input_contract=second_inputs,
                projection_bytes=verified.projected_config_bytes,
                capability_receipt=second_capability,
                governance_receipt=second_governance,
                public_contract_binding=second_contract,
                cold_publisher=second_cold,
            )
            if dict(files) != dict(second_files) or dict(manifest) != dict(second_manifest):
                raise OwnerBuyE3MechanicsBaselineError(
                    "default mechanics authorities changed before manifest commit"
                )
            _install_private_file_at(bundle_fd, "manifest.json", files["manifest.json"])
            _rebind_bundle_directory(parent_fd, parts[-1], binding)
            if _failure_hook is not None:
                _failure_hook("manifest.json")
            if (
                capture_cold_publisher(cold_repository_root, annotated_tag=V14_COLD_PUBLISHER_TAG)
                != second_cold
            ):
                raise OwnerBuyE3MechanicsBaselineError(
                    "cold publisher changed after manifest commit"
                )
        finally:
            verified.close()
        return MappingProxyType(dict(manifest))
    finally:
        if loaded is not None:
            loaded.close()
        if bundle_fd >= 0:
            os.close(bundle_fd)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
        os.close(parent_fd)


__all__ = [
    "AVAILABILITY",
    "DayMechanicsOverlay",
    "ExactB0ArtifactPaths",
    "ExactE3ArtifactPaths",
    "FORMAL_E3_MECHANICS_DAYS",
    "HOST_NEUTRAL_CONFIG_FILE_SHA256",
    "HOST_NEUTRAL_CONFIG_MAPPING_SHA256",
    "IDENTITY",
    "OwnerBuyE3MechanicsBaseline",
    "OwnerBuyE3MechanicsBaselineError",
    "PERMISSIONS",
    "ParityEvidencePaths",
    "SOURCE_CONFIG_DELTA_PATHS_SHA256",
    "V13_PREDECESSOR_FILE_SHA256",
    "V14_COLD_PUBLISHER_TAG",
    "V14_PUBLIC_CONTRACT_FILE_SHA256",
    "capture_cold_publisher",
    "create_owner_buy_e3_backtest_mechanics_baseline",
    "create_v14_mechanics_governance_receipt",
    "publish_v14_private_bundle",
    "validate_v14_private_bundle",
    "validate_v14_mechanics_governance_receipt",
]
