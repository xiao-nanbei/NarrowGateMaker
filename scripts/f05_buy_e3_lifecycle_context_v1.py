#!/usr/bin/env python3
"""Prepare a portable, non-authoritative lifecycle admission context.

The prepare step runs on the host that can read the ORICO admission tree.  It
reopens the formal admission and its three adjacent identity files through the
existing lifecycle validator, verifies the complete 65-file runtime identity
against both the working checkout and the frozen eacb commit, and emits one
create-only private receipt.  That exact receipt may be transported to the
live host.  It is evidence transport only and never replaces the ORICO
admission as lifecycle authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from scripts import f05_buy_e3_active_capture_v8 as active_capture_v8
from scripts import f05_buy_e3_evidence_completion as lifecycle_io

OWNER: Final = active_capture_v8.OWNER
SCHEMA_VERSION: Final = f"{OWNER}.portable_lifecycle_admission_context.v1"
STATUS: Final = "formal_lifecycle_admission_context_prepared_for_transport_only"
CANONICAL_FIELD: Final = "canonical_lifecycle_admission_context_sha256"

EXECUTION_COMMIT: Final = active_capture_v8.DIRECT_SUCCESSOR_EXECUTION_COMMIT
EXECUTION_TREE: Final = active_capture_v8.DIRECT_SUCCESSOR_EXECUTION_TREE
RUNTIME_EXECUTION: Final = {
    "execution_commit": EXECUTION_COMMIT,
    "execution_tree": EXECUTION_TREE,
    "annotated_operational_tag": active_capture_v8.DIRECT_SUCCESSOR_ANNOTATED_TAG,
    "annotated_operational_tag_object": active_capture_v8.DIRECT_SUCCESSOR_TAG_OBJECT,
    "tag_peeled_commit": EXECUTION_COMMIT,
}
ACTIVE_CONFIG_SHA256: Final = active_capture_v8.ACTIVE_CONFIG_SHA256
RUNTIME_CODE_SCHEMA: Final = "narrowgate_prospective_runtime_code_identity.v1"
RUNTIME_CODE_SHA256: Final = "aaf1dd51ce43db4ec2239901198e3ed6333ca4736bb11395d84f5baa64416b74"
RUNTIME_SOURCE_FILES_CANONICAL_SHA256: Final = (
    "ffb3b0a50189b13010b05511ae1b11fe0a785b4f93bedccae620bac85759b20d"
)
RUNTIME_SOURCE_FILE_COUNT: Final = 65

SAFE_ACTION_STATE: Final = {
    "external_venues.enabled": False,
    "external_venues.shadow_only": True,
    "multi_market.enabled": True,
    "multi_market.global_flow_shadow_enabled": False,
    "multi_market.global_reference_shadow_enabled": False,
    "strategy.buy_e3_cooldown_policy_enabled": True,
    "strategy.boolean_cooldown_policy_enabled": True,
    "strategy.buy_fill_selection_live_enabled": False,
    "strategy.buy_fill_selection_shadow_enabled": False,
    "strategy.cross_venue_fair_price_shadow_enabled": False,
    "strategy.dynamic_fill_hazard_action_enabled": False,
    "strategy.dynamic_fill_hazard_shadow_enabled": False,
    "strategy.post_fill_quote_response_enabled": False,
    "strategy.state_conditioned_policy_mode": "disabled",
    "depth_execution.shadow_enabled": False,
    "logging.inventory_campaign_shadow_enabled": False,
    "logging.market_tape_enabled": False,
    "logging.exact_opportunity_tape_enabled": False,
    "lifecycle_journal_v2.enabled": True,
}
SAFE_ACTION_SHADOW_ENABLED_STATE: Final = {
    name: value for name, value in SAFE_ACTION_STATE.items() if name.endswith("shadow_enabled")
}
SAFE_EXTERNAL_SOURCE_RECORDING_STATE: Final = [
    {
        "source_index": 0,
        "venue": "bitget",
        "instrument_type": "perp",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
    {
        "source_index": 1,
        "venue": "bybit",
        "instrument_type": "perp",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
    {
        "source_index": 2,
        "venue": "bitget",
        "instrument_type": "spot",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
    {
        "source_index": 3,
        "venue": "bybit",
        "instrument_type": "spot",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
    {
        "source_index": 4,
        "venue": "okx",
        "instrument_type": "perp",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
    {
        "source_index": 5,
        "venue": "okx",
        "instrument_type": "spot",
        "symbol": "BTCUSDT",
        "role": "reference",
        "source_enabled": True,
        "record_enabled": False,
        "record_trades": True,
    },
]

EXPECTED_RUNTIME_SOURCE_SHA256: Final = {
    "execution/__init__.py": "7349e85e11c5467df719c154b9052c7865023ff11524e3b3a6b7ab54c2f14ddc",
    "execution/active_order_depth_path.py": "147719b82f50f8374fc117cc9d9a1a5d905e9de07df9f5e9eff9fa9e5ade61a7",
    "execution/chunked_parquet_journal.py": "8596df6d1faedb54bb2c2e6282707f1959cc8b1899b7c91d24d8c620aaf9bada",
    "execution/exact_opportunity_tape.py": "9df7753b9e9560804a2df5168f1d5da2b428d17945eb1f905fb6395fe2cbb492",
    "execution/exact_opportunity_tape_runtime.py": "0c59b3e0745864bc5aa85c25f3430e0b50cafa48179bdf47fc6ced7d85037a86",
    "execution/order_lifecycle.py": "9d97b7178fa64af0878d5c21efba6c334490d6cfdd8c4d1badf77d708a456817",
    "execution/order_lifecycle_journal.py": "6acbdf1158549557dab7f8469108b1ae5600039514b9efd34edb1609cf96ed6e",
    "execution/order_lifecycle_journal_storage_v2.py": "660729ac078d5a4896fa2de30717ae9856a8df9d8dcfb185d5d13fcf00014086",
    "execution/order_lifecycle_journal_v2.py": "b8536b3bce6fba34f4fdebc3063a967668b3254174eb3c46d1d33a604436b46b",
    "execution/order_lifecycle_journal_v2_strict_native.py": "f97e47a2fd753116381bab807a9b96cfdcbda97646992f239bfd50c015a6c1a1",
    "execution/order_lifecycle_journal_writer_v2.py": "f84dcafe53c670a627abbfe1d154fa22a119fb211a3f70af95a65ab5ab32cb32",
    "execution/order_lifecycle_journal_writer_v2_replay_day_buffered.py": "27c292be987dcfc447aa7fc43a0d7413cce76eaa85b0bf959781466bee9a3bba",
    "execution/order_lifecycle_journal_writer_v2_replay_single_owner.py": "f46f508870c3cf4f7ce16670e6a1dc701144841a5f0b637d2bb6f66ee02934ec",
    "execution/order_lifecycle_journal_writer_v2_strict_native.py": "7e7044bcb69b655780c035ce6f6a14c5bb319acbee59ea5b1cf5bfdb8084e71e",
    "execution/order_lifecycle_live_writer_v2.py": "bf5382ebf0922653f9edf85728ee1eaee41f35070de9b6f7101f3cce12fdd4ae",
    "execution/order_lifecycle_quantity_contract.py": "dcb37675dca018142a1e44ae207f9c4bdf3eda3a48426676d2069cf17ba04e52",
    "execution/order_lifecycle_remote_spool_v2.py": "42803e63667c5614d4721fb2c7b91e630db446dd4082b369c74061dd2b543363",
    "features/__init__.py": "7160ebf79a1f2dd9e2fafc1813eb1b7a71d490bc7f870541efb623d03d27c618",
    "features/feature_dag.py": "97411199bfd44f81aa3bed3e45301601293c3146d41bf77695615ef97bbc5f56",
    "features/feature_engineer.py": "130f3d4d07087c4aa75e4c90ec6594422d7fbd590c2ed7290ad20fdc15579136",
    "features/preprocess.py": "6f39f4f6941a6d00d0a40d939fc38d2059c05307c3c297e26b37bdb1cd0d9733",
    "features/preprocess_metrics.py": "2056e606447d5ec7a02de234e4ab12bff049dc3e52e1c83c4217de85d66ea380",
    "live/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "live/config.py": "9160b8884e877e4230efee1505d569dbf349c6e4e41e4f95192e95b95b3df425",
    "live/main.py": "2035bed0b74b85f003855e48782fe4a769f500648e775962b7f3b30a066abc72",
    "live/orderbook/__init__.py": "f791ca3c1e9600d592d9c3e4c2cb93386514db659ca1d42cce3e265b69edc8e4",
    "live/orderbook/binance_usdm.py": "00a7d593b5fba46b076abb602bd08a04ac0048883124efa709a7b02cfd24d1e8",
    "live/runtime_policy.py": "23bf62c1e0bfdd0bcc94ef203d39e22f61f9296bf3545157c373ca4f45912964",
    "live/venues/__init__.py": "4881db7df6b96c7ae668bf8045e1823142fbc045ab511866897dc399ac9105d1",
    "live/venues/bitget.py": "edf781a2583e26fcd9a00990421ea8121ef35d0202c3ea92425c508bfec25dfa",
    "live/venues/bybit.py": "85e970fc5a3cf01dc425c13681b24e46caf3f8dbb5af12a3556b652e9f51511a",
    "live/venues/common.py": "82e1fa5059799bab8148afa6e85d442a643787cb5ab44ef76efd5cb8d98b8f97",
    "live/venues/okx.py": "c8405c62c8ba00fc22ae851fd436a604f101c84105c64ea0ac73125fa9bcc54d",
    "live/ws_handler.py": "c817f147394cc892489b5fbdf13e572a9f6bd391529182880ff7f87a4618d294",
    "market_fusion.py": "3bef291f7b9686f9c16645d2d695c308d46136301ba2bcc31c4462a4f65d7880",
    "models/replay/baseline_epoch_manifest.py": "7393602838e9985ae4685a863d6d9b30496c640434abdc7349cdee46ad967429",
    "models/replay/prospective_baseline_epoch.py": "d93344a1503b3b623d5c4c75e9d3c91d08b1c2ab772d0db32bdc7a57ead14093",
    "strategy/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "strategy/boolean_cooldown_buy_e3.py": "85cd44c6695caa3f50942b2dc1cf489f6d1af113db53cd07b891d44d1ccfaf94",
    "strategy/boolean_cooldown_coverage.py": "669745bfdadbd633279db7c505d56ccf22b0c1efd580be405bde1b31090e385a",
    "strategy/boolean_cooldown_live.py": "7802eb19973b21a0e1051ae6ec252ec63e9949f42cafe4c2b08e329c054fc113",
    "strategy/boolean_cooldown_successor.py": "77b707c5f0adc0bac4fc3ed01335aa69de54e233ca9d85dffe3911b8da2b456c",
    "strategy/buy_soft_widen_release.py": "42f7bddb8f75808bbc6cb9b80c68caeb9c2182f399d35813e5677b88fd2e53fc",
    "strategy/campaign_repair.py": "fbb10d21df3c234ddc2bc97518d8048ee2b4b5e07563c1a3a5eed35c8919fb15",
    "strategy/conditional_p3_reach_budget_policy.py": "84779f355a8d02e50648e31484dc37b9f39e0f70e7bdd449a65bb0af9ad0cf1d",
    "strategy/conditional_p3_reach_gate.py": "2479427c916f0c7119553a456ddead26a8e6f82179cf73416bd4277afe249a4b",
    "strategy/cross_venue_fair_price.py": "fd72138572382eff3d02f749a1272598a0cf0c0d8b9b80c24d2fd389da45ecaa",
    "strategy/dynamic_fill_hazard_model.py": "a0c54430a68493757d4682798d2b7274c4e3c834fc502d2ad2ffd3ba37654b88",
    "strategy/external_adverse_quote_edge_guard.py": "b83e2b7af539f0ed4f35bbd4ffc4f96e5aa96dc2d611374261fe8c52e89d925a",
    "strategy/fill_cooldown.py": "64151d14579e5f95c961d2a07634e01aa6989e22528842d8536fd27a56060ccd",
    "strategy/fill_selection_model.py": "e59a5e05ee0b1a1b152359591045e6d219400275ed1777fb7c597493b5891743",
    "strategy/global_flow.py": "bce56e4e1a4942c7e1c61d72ea1b0704664bc0ba221acd051d14447ddb02f690",
    "strategy/global_reference.py": "9e6220946bffc25de3f17e101e270f5ad6d0cacf93f5c1042d4b40c7f02bb3ea",
    "strategy/inventory_manager.py": "cf60a38bd48e9e9327400833a18e02fdc91be910b7bbaf5470af34cb25551903",
    "strategy/maker_engine.py": "1915758dde60eeb8f9c8dbc69b7fa3ddc988862bcd2fd62b9398aa3d7b19dad0",
    "strategy/model_contract.py": "408c364c4210327316c152aec3d94e5e1076941f707044287f4f9d2c481f523e",
    "strategy/multi_market_policy.py": "29603907e68eefa740edc987e86f8e29572c0759ca321b48402e58a9aed8f300",
    "strategy/order_manager.py": "350558d6ec0208ca9c1cabadcb1feba43c95e97226380fa1e505100fe35eb983",
    "strategy/placement_fill_probability.py": "a2f8bb5968cf226885c935b4efb016d53bafbd2759bcccfebc9fc9423fe4739f",
    "strategy/policy_guards.py": "154783a11c23e870ccac8b11df83f770f485dfe24d873c3f51ac28a7c3f7669e",
    "strategy/post_fill_quote_response.py": "eb4bfc0ce747b88d47b40b2a08e1542e803b3139ab79045254280740b267a98f",
    "strategy/quote_core.py": "40d9481f8e1e77936953faef23552cc131458f33cda7863c13fd9a06b7f39bbd",
    "strategy/replay_controls.py": "14ef3578dfdb74ff28bef42d276f9e6ec0f4e012c54e2e0e5a80015ae8d002c5",
    "strategy/signal.py": "50dab228e88985d1cd8ddf660bb87f9f9d314a1add5c19d331352f523b1fe856",
    "strategy/state_conditioned_quote_policy.py": "219ca178c652986e23d8987f56ad5dcee58ea14ea4eaceff0a1655c2e605f624",
}

CONTENT_BINDING_FIELDS: Final = {
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
}
CHECKS: Final = {
    "formal_orico_lifecycle_admission_reopened": True,
    "adjacent_runtime_epoch_identity_files_reopened_by_formal_validator": True,
    "lifecycle_admission_exact7_bound": True,
    "runtime_code_stored_aggregate_exact": True,
    "runtime_source_map_exact65": True,
    "runtime_source_map_canonical_exact": True,
    "all_runtime_sources_match_working_and_eacb_bytes": True,
    "runtime_checkout_head_tree_annotated_tag_and_clean_exact": True,
    "runtime_source_lexical_components_non_symlink": True,
    "context_generated_at_or_after_lifecycle_admission": True,
    "safe_action_state_exact": True,
    "external_shadow_only_inert_because_master_disabled": True,
    "data_source_identity_canonical_matches_epoch": True,
    "all_external_source_record_enabled_flags_false": True,
    "external_record_trades_true_reported_and_inert": True,
    "external_stream_and_recording_effectively_disabled": True,
    "all_action_shadow_enabled_fields_false": True,
    "portable_context_only_not_lifecycle_authority": True,
}
PERMISSIONS: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "hypothetical_live_actions_scored": False,
    "lifecycle_authority_created": False,
    "orico_admission_replaced": False,
    "active_process_identity_read": False,
    "lifecycle_admission_pid_start_binding_present": False,
    "direct_lifecycle_admission_to_active_process_binding_claimed": False,
}
TOP_LEVEL_FIELDS: Final = {
    "schema_version",
    "identity",
    "status",
    "generated_utc",
    "lifecycle_admission",
    "lifecycle_projection",
    "runtime_execution",
    "checks",
    "permissions",
    "evidence_boundary",
    CANONICAL_FIELD,
}
PROJECTION_FIELDS: Final = {
    "admitted_ts_ns",
    "session_id",
    "baseline_epoch_id",
    "config_sha256",
    "runtime_code_sha256",
    "runtime_code_schema_version",
    "runtime_source_files",
    "runtime_source_file_count",
    "runtime_source_files_canonical_sha256",
    "action_enablement_sha256",
    "epoch_start_ts_ns",
    "writer_runtime_identity_sha256",
    "writer_identity_file_sha256",
    "epoch_manifest_file_sha256",
    "identity_evidence_file_sha256",
    "safe_action_state",
    "action_shadow_enabled_state",
    "external_shadow_only_inert",
    "data_source_identity_sha256",
    "external_source_recording_state",
    "external_source_count",
    "source_settings_inert_because_external_master_false",
    "record_trades_inert_because_master_false_and_record_enabled_false",
    "external_effective_stream_and_recording_disabled",
}
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")


class LifecycleContextError(RuntimeError):
    """Raised when portable lifecycle context is not exact and non-authoritative."""


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise LifecycleContextError(f"{label} is not a lowercase SHA256")
    return normalized


def _canonical_sha256(value: Any) -> str:
    return _sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    )


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    try:
        parsed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise LifecycleContextError(f"{label} is invalid") from exc
    if not normalized.endswith("Z") or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise LifecycleContextError(f"{label} is not canonical UTC")
    return normalized


def _timestamp_ns(value: Any, label: str) -> int:
    normalized = _timestamp(value, label)
    parsed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    delta = parsed - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _git(repository: Path, *args: str) -> bytes:
    try:
        return subprocess.run(
            ("git", *args),
            cwd=repository,
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise LifecycleContextError("frozen runtime git object could not be read") from exc


def _regular_file_bytes(path: Path, label: str) -> bytes:
    candidate = path.absolute()
    current = Path(candidate.anchor)
    for part in candidate.parts[1:-1]:
        current /= part
        try:
            component = os.lstat(current)
        except OSError as exc:
            raise LifecycleContextError(f"{label} ancestor is missing") from exc
        if stat.S_ISLNK(component.st_mode) or not stat.S_ISDIR(component.st_mode):
            raise LifecycleContextError(f"{label} has a symlink/non-directory ancestor")
    try:
        lexical = os.lstat(candidate)
    except OSError as exc:
        raise LifecycleContextError(f"{label} is missing") from exc
    if stat.S_ISLNK(lexical.st_mode) or not stat.S_ISREG(lexical.st_mode) or lexical.st_nlink != 1:
        raise LifecycleContextError(f"{label} is not a regular non-symlink single-link file")
    descriptor = os.open(
        candidate,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            lexical.st_dev,
            lexical.st_ino,
            lexical.st_size,
        ):
            raise LifecycleContextError(f"{label} changed while opened")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(raw) != opened.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns):
            raise LifecycleContextError(f"{label} changed while read")
        return raw
    finally:
        os.close(descriptor)


def _runtime_checkout_identity(repository: Path) -> tuple[str, str, str, str, bytes]:
    return (
        _git(repository, "rev-parse", "HEAD").decode().strip(),
        _git(repository, "rev-parse", "HEAD^{tree}").decode().strip(),
        _git(
            repository,
            "rev-parse",
            f"{active_capture_v8.DIRECT_SUCCESSOR_ANNOTATED_TAG}^{{tag}}",
        )
        .decode()
        .strip(),
        _git(
            repository,
            "rev-parse",
            f"{active_capture_v8.DIRECT_SUCCESSOR_ANNOTATED_TAG}^{{}}",
        )
        .decode()
        .strip(),
        _git(repository, "status", "--porcelain=v1", "--untracked-files=all"),
    )


def _require_exact_runtime_checkout_identity(repository: Path) -> tuple[str, str, str, str, bytes]:
    identity = _runtime_checkout_identity(repository)
    if identity != (
        EXECUTION_COMMIT,
        EXECUTION_TREE,
        active_capture_v8.DIRECT_SUCCESSOR_TAG_OBJECT,
        EXECUTION_COMMIT,
        b"",
    ):
        raise LifecycleContextError("runtime checkout is not clean exact eacb/tag")
    return identity


def validate_runtime_source_checkout(repository_root: Path) -> dict[str, str]:
    lexical_repository = repository_root.expanduser().absolute()
    current = Path(lexical_repository.anchor)
    for part in lexical_repository.parts[1:]:
        current /= part
        try:
            component = os.lstat(current)
        except OSError as exc:
            raise LifecycleContextError("runtime checkout path is missing") from exc
        if stat.S_ISLNK(component.st_mode):
            raise LifecycleContextError("runtime checkout has a symlink component")
    if not stat.S_ISDIR(os.lstat(lexical_repository).st_mode):
        raise LifecycleContextError("runtime checkout is not a directory")
    repository = lexical_repository.resolve(strict=True)
    initial_identity = _require_exact_runtime_checkout_identity(repository)
    observed: dict[str, str] = {}
    for relative, expected_sha in EXPECTED_RUNTIME_SOURCE_SHA256.items():
        pure = PurePosixPath(relative)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise LifecycleContextError("runtime source path is not repository-relative")
        current = repository
        for part in pure.parts:
            current /= part
            try:
                component = os.lstat(current)
            except OSError as exc:
                raise LifecycleContextError(
                    f"runtime source component is missing: {relative}"
                ) from exc
            if stat.S_ISLNK(component.st_mode):
                raise LifecycleContextError(f"runtime source has a symlink component: {relative}")
        if not current.resolve(strict=True).is_relative_to(repository):
            raise LifecycleContextError(f"runtime source escapes repository: {relative}")
        working_sha = _sha256(
            _regular_file_bytes(repository / relative, f"runtime source {relative}")
        )
        committed_sha = _sha256(_git(repository, "show", f"{EXECUTION_COMMIT}:{relative}"))
        if working_sha != expected_sha or committed_sha != expected_sha:
            raise LifecycleContextError(
                f"runtime source differs from working/eacb bytes: {relative}"
            )
        observed[relative] = expected_sha
    if (
        len(observed) != RUNTIME_SOURCE_FILE_COUNT
        or observed != EXPECTED_RUNTIME_SOURCE_SHA256
        or _canonical_sha256(observed) != RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        or _canonical_sha256({"schema_version": RUNTIME_CODE_SCHEMA, "files": observed})
        != RUNTIME_CODE_SHA256
    ):
        raise LifecycleContextError("runtime source exact65 aggregate drifted")
    final_identity = _require_exact_runtime_checkout_identity(repository)
    if final_identity != initial_identity:
        raise LifecycleContextError("runtime checkout changed while sources were read")
    return observed


def _formal_lifecycle_context(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str, list[dict[str, Any]]]:
    try:
        payload, binding = lifecycle_io._validate_lifecycle_admission(path)  # noqa: SLF001
        admission_root = path.expanduser().absolute().parent.resolve(strict=True)
        evidence_path = (
            admission_root
            / "source"
            / "prospective_baseline_epochs"
            / str(binding["baseline_epoch_id"])
            / "identity_evidence.json"
        )
        evidence, evidence_raw = lifecycle_io._read_admitted_json(  # noqa: SLF001
            evidence_path,
            admission_root=admission_root,
            label="admitted lifecycle identity evidence for portable context",
        )
        epoch, _epoch_raw = lifecycle_io._read_admitted_json(  # noqa: SLF001
            evidence_path.with_name("epoch_manifest.json"),
            admission_root=admission_root,
            label="admitted lifecycle epoch manifest for portable context",
        )
    except Exception as exc:
        raise LifecycleContextError("formal ORICO lifecycle admission is invalid") from exc
    action = evidence.get("action_enablement")
    fields = action.get("fields") if isinstance(action, Mapping) else None
    safe = (
        {name: fields.get(name) for name in SAFE_ACTION_STATE}
        if isinstance(fields, Mapping)
        else None
    )
    data_source = evidence.get("data_source_identity")
    external = data_source.get("external_venues") if isinstance(data_source, Mapping) else None
    sources = external.get("sources") if isinstance(external, Mapping) else None
    epoch_identity = epoch.get("identity") if isinstance(epoch, Mapping) else None
    data_source_sha = _canonical_sha256(data_source) if isinstance(data_source, Mapping) else ""
    recording_state = (
        [
            {
                "source_index": index,
                "venue": source.get("venue"),
                "instrument_type": source.get("instrument_type"),
                "symbol": source.get("symbol"),
                "role": source.get("role"),
                "source_enabled": source.get("enabled"),
                "record_enabled": source.get("record_enabled"),
                "record_trades": source.get("record_trades"),
            }
            for index, source in enumerate(sources)
            if isinstance(source, Mapping)
        ]
        if isinstance(sources, list)
        else None
    )
    shadow_enabled_state = (
        {str(name): value for name, value in fields.items() if str(name).endswith("shadow_enabled")}
        if isinstance(fields, Mapping)
        else None
    )
    if (
        hashlib.sha256(evidence_raw).hexdigest() != binding.get("identity_evidence_file_sha256")
        or hashlib.sha256(_epoch_raw).hexdigest() != binding.get("epoch_manifest_file_sha256")
        or not isinstance(action, Mapping)
        or action.get("schema_version") != "narrowgate_action_enablement_identity.v1"
        or safe != SAFE_ACTION_STATE
        or safe["external_venues.enabled"] is not False
        or safe["external_venues.shadow_only"] is not True
        or shadow_enabled_state != SAFE_ACTION_SHADOW_ENABLED_STATE
        or any(value is not False for value in shadow_enabled_state.values())
        or not isinstance(data_source, Mapping)
        or data_source.get("schema_version") != "narrowgate_live_data_source_identity.v1"
        or not isinstance(epoch_identity, Mapping)
        or epoch_identity.get("data_source_identity_sha256") != data_source_sha
        or not isinstance(external, Mapping)
        or external.get("enabled") is not False
        or external.get("shadow_only") is not True
        or recording_state != SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        or any(row["record_enabled"] is not False for row in recording_state)
        or any(row["record_trades"] is not True for row in recording_state)
    ):
        raise LifecycleContextError("admitted lifecycle safe action state drifted")
    return payload, binding, safe, data_source_sha, recording_state


def _lifecycle_binding_projection(
    payload: Mapping[str, Any],
    binding: Mapping[str, Any],
    safe_action_state: Mapping[str, Any],
    data_source_identity_sha256: str,
    external_source_recording_state: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    files = binding.get("runtime_code_files")
    admission = {
        "schema_version": binding.get("schema_version"),
        "status": None,
        "file_sha256": binding.get("file_sha256"),
        "canonical_field": binding.get("canonical_field"),
        "canonical_sha256": binding.get("canonical_sha256"),
        "size_bytes": binding.get("size_bytes"),
        "mode": binding.get("mode"),
    }
    if (
        admission["schema_version"] != lifecycle_io.LIFECYCLE_SCHEMA
        or admission["canonical_field"] != "admission_identity_sha256"
        or admission["mode"] != "0644"
        or not isinstance(admission["size_bytes"], int)
        or admission["size_bytes"] <= 0
        or any(
            _SHA256_RE.fullmatch(str(admission[name])) is None
            for name in ("file_sha256", "canonical_sha256")
        )
        or not isinstance(files, Mapping)
        or dict(files) != EXPECTED_RUNTIME_SOURCE_SHA256
        or binding.get("runtime_code_sha256") != RUNTIME_CODE_SHA256
        or binding.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or dict(safe_action_state) != SAFE_ACTION_STATE
        or _require_sha256(
            data_source_identity_sha256,
            "lifecycle data source identity",
        )
        != data_source_identity_sha256
        or [dict(row) for row in external_source_recording_state]
        != SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        or any(
            name in binding
            for name in ("pid", "pid_start_ticks", "active_pid", "active_pid_start_ticks")
        )
        or not str(binding.get("baseline_epoch_id", "")).startswith("prospective-")
        or type(payload.get("admitted_ts_ns")) is not int
        or payload["admitted_ts_ns"] <= 0
        or type(binding.get("epoch_start_ts_ns")) is not int
        or binding["epoch_start_ts_ns"] <= 0
        or binding["epoch_start_ts_ns"] >= payload["admitted_ts_ns"]
    ):
        raise LifecycleContextError("formal lifecycle admission projection drifted")
    projection = {
        "admitted_ts_ns": payload["admitted_ts_ns"],
        "session_id": str(binding["session_id"]),
        "baseline_epoch_id": str(binding["baseline_epoch_id"]),
        "config_sha256": str(binding["config_sha256"]),
        "runtime_code_sha256": str(binding["runtime_code_sha256"]),
        "runtime_code_schema_version": RUNTIME_CODE_SCHEMA,
        "runtime_source_files": dict(files),
        "runtime_source_file_count": len(files),
        "runtime_source_files_canonical_sha256": _canonical_sha256(dict(files)),
        "action_enablement_sha256": str(binding["action_enablement_sha256"]),
        "epoch_start_ts_ns": binding["epoch_start_ts_ns"],
        "writer_runtime_identity_sha256": str(binding["writer_runtime_identity_sha256"]),
        "writer_identity_file_sha256": str(binding["writer_identity_file_sha256"]),
        "epoch_manifest_file_sha256": str(binding["epoch_manifest_file_sha256"]),
        "identity_evidence_file_sha256": str(binding["identity_evidence_file_sha256"]),
        "safe_action_state": dict(safe_action_state),
        "action_shadow_enabled_state": dict(SAFE_ACTION_SHADOW_ENABLED_STATE),
        "external_shadow_only_inert": True,
        "data_source_identity_sha256": data_source_identity_sha256,
        "external_source_recording_state": [dict(row) for row in external_source_recording_state],
        "external_source_count": len(external_source_recording_state),
        "source_settings_inert_because_external_master_false": True,
        "record_trades_inert_because_master_false_and_record_enabled_false": True,
        "external_effective_stream_and_recording_disabled": True,
    }
    for name in (
        "action_enablement_sha256",
        "writer_runtime_identity_sha256",
        "writer_identity_file_sha256",
        "epoch_manifest_file_sha256",
        "identity_evidence_file_sha256",
    ):
        _require_sha256(projection[name], f"lifecycle {name}")
    return admission, projection


def validate_content_projection(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != TOP_LEVEL_FIELDS:
        raise LifecycleContextError("lifecycle context receipt fields drifted")
    payload = dict(raw)
    generated_ns = _timestamp_ns(
        payload.get("generated_utc"),
        "lifecycle context generated timestamp",
    )
    admission = payload.get("lifecycle_admission")
    projection = payload.get("lifecycle_projection")
    if (
        not isinstance(admission, Mapping)
        or set(admission) != CONTENT_BINDING_FIELDS
        or not isinstance(projection, Mapping)
        or set(projection) != PROJECTION_FIELDS
    ):
        raise LifecycleContextError("lifecycle context projection fields drifted")
    for name in ("file_sha256", "canonical_sha256"):
        _require_sha256(admission.get(name), f"lifecycle admission {name}")
    for name in (
        "config_sha256",
        "runtime_code_sha256",
        "runtime_source_files_canonical_sha256",
        "action_enablement_sha256",
        "writer_runtime_identity_sha256",
        "writer_identity_file_sha256",
        "epoch_manifest_file_sha256",
        "identity_evidence_file_sha256",
    ):
        _require_sha256(projection.get(name), f"lifecycle projection {name}")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER
        or payload.get("status") != STATUS
        or admission.get("schema_version") != lifecycle_io.LIFECYCLE_SCHEMA
        or admission.get("status") is not None
        or admission.get("canonical_field") != "admission_identity_sha256"
        or admission.get("mode") != "0644"
        or type(admission.get("size_bytes")) is not int
        or admission["size_bytes"] <= 0
        or projection.get("runtime_code_schema_version") != RUNTIME_CODE_SCHEMA
        or projection.get("runtime_code_sha256") != RUNTIME_CODE_SHA256
        or projection.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or projection.get("runtime_source_file_count") != RUNTIME_SOURCE_FILE_COUNT
        or projection.get("runtime_source_files") != EXPECTED_RUNTIME_SOURCE_SHA256
        or projection.get("runtime_source_files_canonical_sha256")
        != RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        or projection.get("safe_action_state") != SAFE_ACTION_STATE
        or projection.get("action_shadow_enabled_state") != SAFE_ACTION_SHADOW_ENABLED_STATE
        or projection.get("external_shadow_only_inert") is not True
        or _require_sha256(
            projection.get("data_source_identity_sha256"),
            "lifecycle data source identity",
        )
        != projection.get("data_source_identity_sha256")
        or projection.get("external_source_recording_state") != SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        or projection.get("external_source_count") != len(SAFE_EXTERNAL_SOURCE_RECORDING_STATE)
        or projection.get("source_settings_inert_because_external_master_false") is not True
        or projection.get("record_trades_inert_because_master_false_and_record_enabled_false")
        is not True
        or projection.get("external_effective_stream_and_recording_disabled") is not True
        or not str(projection.get("baseline_epoch_id", "")).startswith("prospective-")
        or type(projection.get("admitted_ts_ns")) is not int
        or type(projection.get("epoch_start_ts_ns")) is not int
        or not (0 < projection["epoch_start_ts_ns"] < projection["admitted_ts_ns"])
        or generated_ns < projection["admitted_ts_ns"]
        or payload.get("runtime_execution") != RUNTIME_EXECUTION
        or payload.get("checks") != CHECKS
        or payload.get("permissions") != PERMISSIONS
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(CANONICAL_FIELD) != _document_sha256(payload, CANONICAL_FIELD)
    ):
        raise LifecycleContextError("lifecycle context receipt identity drifted")
    return payload


def build_lifecycle_context(
    *,
    lifecycle_admission_path: Path,
    runtime_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    (
        lifecycle_payload,
        lifecycle_binding,
        safe_action_state,
        data_source_identity_sha256,
        external_source_recording_state,
    ) = _formal_lifecycle_context(lifecycle_admission_path)
    validate_runtime_source_checkout(runtime_repository_root)
    admission, projection = _lifecycle_binding_projection(
        lifecycle_payload,
        lifecycle_binding,
        safe_action_state,
        data_source_identity_sha256,
        external_source_recording_state,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": generated_utc or _now(),
        "lifecycle_admission": admission,
        "lifecycle_projection": projection,
        "runtime_execution": dict(RUNTIME_EXECUTION),
        "checks": dict(CHECKS),
        "permissions": dict(PERMISSIONS),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = _document_sha256(payload, CANONICAL_FIELD)
    return validate_content_projection(payload)


def validate_lifecycle_context(
    path: Path,
    *,
    runtime_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _binding = lifecycle_io._binding(  # noqa: SLF001
            path,
            label="portable lifecycle admission context",
            canonical_field=CANONICAL_FIELD,
            expected_schema=SCHEMA_VERSION,
            expected_status=STATUS,
        )
    except Exception as exc:
        raise LifecycleContextError("portable lifecycle context bytes are invalid") from exc
    observed = validate_content_projection(payload)
    validate_runtime_source_checkout(runtime_repository_root)
    return observed


def validate_lifecycle_context_against_admission(
    context_path: Path,
    *,
    lifecycle_admission_path: Path,
    runtime_repository_root: Path,
) -> dict[str, Any]:
    context = validate_lifecycle_context(
        context_path,
        runtime_repository_root=runtime_repository_root,
    )
    (
        lifecycle_payload,
        lifecycle_binding,
        safe_action_state,
        data_source_identity_sha256,
        external_source_recording_state,
    ) = _formal_lifecycle_context(lifecycle_admission_path)
    admission, projection = _lifecycle_binding_projection(
        lifecycle_payload,
        lifecycle_binding,
        safe_action_state,
        data_source_identity_sha256,
        external_source_recording_state,
    )
    if (
        context.get("lifecycle_admission") != admission
        or context.get("lifecycle_projection") != projection
    ):
        raise LifecycleContextError("portable lifecycle context differs from ORICO admission")
    return context


def finalize_lifecycle_context(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_lifecycle_context(**kwargs)
    try:
        file_sha = lifecycle_io._write(output_path, payload)  # noqa: SLF001
    except Exception as exc:
        raise LifecycleContextError("lifecycle context create-only write failed") from exc
    observed = validate_lifecycle_context(
        output_path,
        runtime_repository_root=kwargs["runtime_repository_root"],
    )
    if observed != payload:
        raise LifecycleContextError("lifecycle context changed after write")
    return payload, file_sha


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare-context")
    prepare.add_argument("--lifecycle-admission", type=Path, required=True)
    prepare.add_argument("--runtime-repository-root", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    validate = commands.add_parser("validate-context")
    validate.add_argument("--context", type=Path, required=True)
    validate.add_argument("--runtime-repository-root", type=Path, required=True)
    compare = commands.add_parser("validate-against-admission")
    compare.add_argument("--context", type=Path, required=True)
    compare.add_argument("--lifecycle-admission", type=Path, required=True)
    compare.add_argument("--runtime-repository-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare-context":
        payload, file_sha = finalize_lifecycle_context(
            output_path=args.output,
            lifecycle_admission_path=args.lifecycle_admission,
            runtime_repository_root=args.runtime_repository_root,
        )
    elif args.command == "validate-context":
        payload = validate_lifecycle_context(
            args.context,
            runtime_repository_root=args.runtime_repository_root,
        )
    else:
        payload = validate_lifecycle_context_against_admission(
            args.context,
            lifecycle_admission_path=args.lifecycle_admission,
            runtime_repository_root=args.runtime_repository_root,
        )
    if args.command != "prepare-context":
        try:
            reopened, binding = lifecycle_io._binding(  # noqa: SLF001
                args.context,
                label="portable lifecycle admission context",
                canonical_field=CANONICAL_FIELD,
                expected_schema=SCHEMA_VERSION,
                expected_status=STATUS,
            )
        except Exception as exc:
            raise LifecycleContextError("lifecycle context changed after validation") from exc
        if reopened != payload:
            raise LifecycleContextError("lifecycle context changed after validation")
        file_sha = str(binding["file_sha256"])
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": payload["status"],
                "file_sha256": file_sha,
                "canonical_sha256": payload[CANONICAL_FIELD],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
