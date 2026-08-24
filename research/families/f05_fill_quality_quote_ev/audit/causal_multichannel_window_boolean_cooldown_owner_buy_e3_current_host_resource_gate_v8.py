"""Current-host resource gate v8 for the fully no-shadow BUY E3 path.

This additive successor leaves the historical v1/v2/v3 gates unchanged.  It is
run from a clean, annotated collector checkout while the live repository stays
on the frozen direct-successor runtime authority.  The live
process is freshly restarted with BUY E3 disabled and remains the same PID
throughout a concurrent four-file benchmark.  Only aggregate timing/resource
evidence is persisted; the collector never connects to a market stream and
never persists benchmark actions or economic values.

The direct-successor release is an input authority, not an output of this
gate.  A later evidence-completion receipt may bind this resource receipt, so
there is no release/resource dependency cycle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import resource
import stat
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

import yaml

from scripts import f05_buy_e3_direct_owner_release_v3 as direct_release_v3
from scripts import f05_buy_e3_no_shadow_post_release_config_correction as config_successor
from strategy.boolean_cooldown_buy_e3 import (
    BASE_WINDOW_WIDTH_NS,
    CONTROL_ACTION,
    OWNER_IDENTITY,
    LiveBuyE3CooldownPolicy,
)

PROCESS_SCHEMA: Final = f"{OWNER_IDENTITY}.current_host_process_snapshot.v6"
PROCESS_STATUS: Final = "all_shadow_evaluators_disabled_runtime_process_snapshot_complete"
PROCESS_CANONICAL_FIELD: Final = "canonical_process_snapshot_sha256"
BENCHMARK_SCHEMA: Final = f"{OWNER_IDENTITY}.exact_four_file_host_benchmark.v6"
BENCHMARK_STATUS: Final = "all_shadow_evaluators_disabled_four_file_aggregate_benchmark_passed"
BENCHMARK_CANONICAL_FIELD: Final = "canonical_benchmark_receipt_sha256"
RESOURCE_SCHEMA: Final = f"{OWNER_IDENTITY}.current_host_concurrent_resource_gate.v8"
RESOURCE_STATUS: Final = "fresh_all_shadow_evaluators_disabled_concurrent_gate_passed"
RESOURCE_CANONICAL_FIELD: Final = "canonical_resource_receipt_sha256"
BENCHMARK_PRODUCER_MODULE: Final = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
    "current_host_resource_gate_v8"
)

CURRENT_INSTANCE_ID: Final = "i-00fe03a8b2fb49a31"
CURRENT_INSTANCE_TYPE: Final = "c7i-flex.large"
CURRENT_LOGICAL_CPU_COUNT: Final = 2
MIN_HOST_MEM_TOTAL_MIB: Final = 3_500.0
MAX_HOST_MEM_TOTAL_MIB: Final = 4_500.0

DIRECT_SUCCESSOR_EXECUTION_COMMIT: Final = "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de"
DIRECT_SUCCESSOR_EXECUTION_TREE: Final = "0343bd5586b337385cf2aa0d7a643f5c32b0da77"
DIRECT_SUCCESSOR_ANNOTATED_TAG: Final = "f05-owner-buy-e3-no-shadow-runtime-v3-20260824"
DIRECT_SUCCESSOR_TAG_OBJECT: Final = "3878ea05252ef8f274b6f74ee7a984431c53b892"
DIRECT_SUCCESSOR_RELEASE_FILE_SHA256: Final = (
    "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
)
DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256: Final = (
    "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
)
DIRECT_SUCCESSOR_RELEASE_SCHEMA: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v3"
)
DIRECT_SUCCESSOR_RELEASE_STATUS: Final = (
    "owner_authorized_direct_live_no_shadow_runtime_pending_evidence"
)
EXACT_ARTIFACT_SHA256: Final = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
EXACT_DEPLOYED_FILE_SHA256: Final = {
    "manifest": "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929",
    "policy": "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b",
    "predicate_bundle": "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915",
    "direct_active_release": DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
}
EXPECTED_DISABLED_CONFIG_SHA256: Final = (
    "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
)
CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256: Final = {
    "buy_e3_runtime": {
        "path": "strategy/boolean_cooldown_buy_e3.py",
        "sha256": "85cd44c6695caa3f50942b2dc1cf489f6d1af113db53cd07b891d44d1ccfaf94",
    },
    "order_lifecycle": {
        "path": "execution/order_lifecycle.py",
        "sha256": "9d97b7178fa64af0878d5c21efba6c334490d6cfdd8c4d1badf77d708a456817",
    },
    "order_lifecycle_journal_v2": {
        "path": "execution/order_lifecycle_journal_v2.py",
        "sha256": "b8536b3bce6fba34f4fdebc3063a967668b3254174eb3c46d1d33a604436b46b",
    },
    "order_lifecycle_journal_v2_strict_native": {
        "path": "execution/order_lifecycle_journal_v2_strict_native.py",
        "sha256": "f97e47a2fd753116381bab807a9b96cfdcbda97646992f239bfd50c015a6c1a1",
    },
    "order_lifecycle_live_writer_v2": {
        "path": "execution/order_lifecycle_live_writer_v2.py",
        "sha256": "bf5382ebf0922653f9edf85728ee1eaee41f35070de9b6f7101f3cce12fdd4ae",
    },
    "maker_engine": {
        "path": "strategy/maker_engine.py",
        "sha256": "1915758dde60eeb8f9c8dbc69b7fa3ddc988862bcd2fd62b9398aa3d7b19dad0",
    },
    "signal_engine": {
        "path": "strategy/signal.py",
        "sha256": "50dab228e88985d1cd8ddf660bb87f9f9d314a1add5c19d331352f523b1fe856",
    },
    "global_flow": {
        "path": "strategy/global_flow.py",
        "sha256": "bce56e4e1a4942c7e1c61d72ea1b0704664bc0ba221acd051d14447ddb02f690",
    },
    "global_reference": {
        "path": "strategy/global_reference.py",
        "sha256": "9e6220946bffc25de3f17e101e270f5ad6d0cacf93f5c1042d4b40c7f02bb3ea",
    },
    "live_config": {
        "path": "live/config.py",
        "sha256": "9160b8884e877e4230efee1505d569dbf349c6e4e41e4f95192e95b95b3df425",
    },
    "live_main": {
        "path": "live/main.py",
        "sha256": "2035bed0b74b85f003855e48782fe4a769f500648e775962b7f3b30a066abc72",
    },
    "live_runtime_policy": {
        "path": "live/runtime_policy.py",
        "sha256": "23bf62c1e0bfdd0bcc94ef203d39e22f61f9296bf3545157c373ca4f45912964",
    },
    "live_run": {
        "path": "live/run.sh",
        "sha256": "1215255c0bbc940e78d681480a6ce54d5c9e93b9fc1baa93f0e101880ee09f85",
    },
    "live_ws_handler": {
        "path": "live/ws_handler.py",
        "sha256": "c817f147394cc892489b5fbdf13e572a9f6bd391529182880ff7f87a4618d294",
    },
    "sell_runtime": {
        "path": "strategy/boolean_cooldown_live.py",
        "sha256": "7802eb19973b21a0e1051ae6ec252ec63e9949f42cafe4c2b08e329c054fc113",
    },
}

MIN_MEM_AVAILABLE_MIB: Final = 512.0
MAX_LIVE_RSS_MIB: Final = 512.0
MAX_BENCHMARK_RSS_MIB: Final = 256.0
MAX_COMBINED_RSS_MIB: Final = 768.0
MIN_RATE_MULTIPLIER: Final = 2.0
MAX_CALLBACK_P99_US: Final = 2_000.0
MAX_DECISION_P99_US: Final = 10_000.0
EXACT_DECISION_COUNT: Final = 1_000
MAX_RECEIPT_BYTES: Final = 32 << 20

# Every disabled diagnostic evaluator field is both absolute-zero and
# delta-zero.  This is deliberately stricter than historical gates, which
# allowed a non-zero global-flow baseline.  The explicit enabled/error/reason
# fields distinguish a configured disable from live/main's error fallback.
GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS: Final = (
    "globalFlowNative",
    "globalFlowMarkets",
    "globalFlowTradeBatches",
    "globalFlowTradeEvents",
    "globalFlowTradeAccepted",
    "globalFlowBookEvents",
    "globalFlowBookAccepted",
    "globalFlowOOO",
    "globalFlowStaleTrades",
    "globalFlowTradeOverflow",
    "globalFlowBookOverflow",
)
GLOBAL_FLOW_STATE_ZERO_FIELDS: Final = (
    "globalFlowShadowEnabled",
    "globalFlowStateError",
    "globalFlow100Valid",
    "globalFlow100FreshSpot",
    "globalFlow100FreshPerp",
)
GLOBAL_FLOW_VALUE_ZERO_FIELDS: Final = (
    "globalFlow100Pressure",
    "globalFlow100PendingBps",
    "globalFlow100SpotPressure",
    "globalFlow100PerpPressure",
    "globalFlow100SpotAgreement",
    "globalFlow100PerpAgreement",
)
GLOBAL_REFERENCE_ZERO_FIELDS: Final = (
    "globalRefShadowEnabled",
    "globalRefStateError",
    "globalRefValid",
    "globalRefFreshSpot",
    "globalRefFreshPerp",
    "globalRefBasisSamples",
)
GLOBAL_REFERENCE_VALUE_ZERO_FIELDS: Final = (
    "globalRefConfidence",
    "globalSpotMoveBps",
    "globalPerpMoveBps",
    "globalPerpSpotDivBps",
    "globalResidualBps",
    "globalRefDispersionBps",
)
SHADOW_DISABLED_REASON: Final = "disabled_by_config"

# Counters and gauges preserved in the resource receipt.  The global-flow
# fields must remain exactly zero; all other monotonic counters must have a
# zero window delta.
WINDOW_ZERO_COUNTERS: Final = (
    "fillHazardInvalid",
    "fillHazardActionInvalidHold",
    "booleanCooldownInvalid",
    "buyE3CooldownInvalid",
    "marketTapeDropped",
    "marketTapeInvalid",
    "externalErrors",
    "externalRecordDropped",
    *GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
    "booleanCooldownResets",
    "buyE3CooldownResets",
    "deepBookGaps",
    "deepBookResyncs",
    "deepBookStaleRestarts",
    "orderLifecycleV2Drops",
    "orderLifecycleV2Errors",
)
EXPECTED_NAMED_DROP_INVALID_OVERFLOW: Final = frozenset(
    {
        "fillHazardInvalid",
        "fillHazardActionInvalidHold",
        "booleanCooldownInvalid",
        "buyE3CooldownInvalid",
        "marketTapeDropped",
        "marketTapeInvalid",
        "externalRecordDropped",
        "globalFlowTradeOverflow",
        "globalFlowBookOverflow",
    }
)

RESOURCE_CHECK_NAMES: Final = (
    "current_instance_id_exact",
    "current_instance_type_exact",
    "exact_2_vcpu_host",
    "host_memory_class_4gib",
    "runtime_checkout_exact_direct_successor",
    "collector_is_clean_annotated_with_exact_v4_runtime_sources",
    "exact_four_deployed_files_bound",
    "immutable_direct_successor_authority_has_no_resource_dependency",
    "fresh_disabled_process_proven",
    "buy_e3_disabled_throughout",
    "external_venues_disabled_throughout",
    "global_flow_shadow_disabled_throughout",
    "global_reference_shadow_disabled_throughout",
    "global_flow_absolute_zero_throughout",
    "global_reference_absolute_zero_throughout",
    "external_sources_absolute_zero_throughout",
    "external_error_counters_absolute_zero_throughout",
    "sell_owner_enabled_throughout",
    "same_live_pid_and_start_ticks_throughout",
    "benchmark_process_overlap_proven",
    "post_benchmark_fresh_health_observed",
    "min_mem_available_at_least_512mib",
    "live_rss_at_most_512mib",
    "benchmark_rss_at_most_256mib",
    "combined_rss_at_most_768mib",
    "oom_window_delta_zero",
    "swap_window_delta_zero",
    "all_drop_invalid_overflow_window_deltas_zero",
    "absolute_counter_baseline_preserved",
    "deep_book_buffer_zero_throughout",
    "true_2x_observed_callback_rate",
    "callback_p99_at_most_2ms",
    "exactly_1000_decisions",
    "decision_p99_at_most_10ms",
    "aggregate_only_no_live_stream_or_action_rows",
)
RESOURCE_OBSERVED_FIELDS: Final = (
    "sample_count",
    "min_mem_available_mib",
    "max_live_rss_mib",
    "max_benchmark_rss_mib",
    "max_combined_rss_mib",
    "oom_absolute_baseline",
    "oom_absolute_final",
    "oom_window_delta",
    "swap_in_absolute_baseline",
    "swap_in_absolute_final",
    "swap_in_window_delta",
    "swap_out_absolute_baseline",
    "swap_out_absolute_final",
    "swap_out_window_delta",
    "max_deep_book_buffer",
    "observed_live_callback_rate_hz",
    "benchmark_achieved_rate_hz",
    "achieved_to_observed_rate",
    "callback_p99_us",
    "decision_count",
    "decision_p99_us",
)
RESOURCE_CAPTURE_FIELDS: Final = (
    "collector_pid",
    "benchmark_pid",
    "benchmark_pid_start_ticks",
    "live_pid",
    "live_pid_start_ticks",
    "benchmark_command_sha256",
    "benchmark_launch_monotonic_ns",
    "benchmark_exit_monotonic_ns",
    "rate_boundary_main_health_generation",
    "rate_boundary_main_health_line_sha256",
    "rate_boundary_lifecycle_health_generation",
    "rate_boundary_lifecycle_health_line_sha256",
    "rate_first_main_health_generation",
    "rate_first_main_health_line_sha256",
    "rate_first_lifecycle_health_generation",
    "rate_first_lifecycle_health_line_sha256",
    "rate_second_main_health_generation",
    "rate_second_main_health_line_sha256",
    "rate_second_lifecycle_health_generation",
    "rate_second_lifecycle_health_line_sha256",
    "rate_window_update_delta",
    "rate_window_elapsed_s",
    "rate_window_same_live_pid_and_start_ticks",
    "baseline_main_health_generation",
    "final_main_health_generation",
    "baseline_lifecycle_health_generation",
    "final_lifecycle_health_generation",
    "baseline_main_health_line_sha256",
    "final_main_health_line_sha256",
    "baseline_lifecycle_health_line_sha256",
    "final_lifecycle_health_line_sha256",
    "benchmark_returncode",
    "benchmark_stdout_sha256",
    "benchmark_stderr_sha256",
    "sample_series_sha256",
    "sample_count",
    "health_source",
    "market_stream_connection_created",
)

AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_owner_no_shadow_release_v3",
    "runtime_authority_release_file_sha256": DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
    "runtime_authority_release_canonical_sha256": DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256,
    "resource_receipt_is_post_authority_completion_evidence": True,
    "resource_receipt_is_not_embedded_in_direct_successor_release": True,
    "direct_successor_release_does_not_depend_on_resource_receipt": True,
    "later_evidence_completion_may_bind_resource_receipt": True,
}

EVIDENCE_BOUNDARY: Final = {
    "aggregate_only": True,
    "connected_to_live_market_stream": False,
    "hypothetical_live_actions_scored": False,
    "benchmark_action_rows_persisted": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "new_economic_arm_run": False,
    "action_authorized_by_resource_receipt": False,
    "live_authorized_by_resource_receipt": False,
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_KV_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9]*)=([^\s]+)")
_LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s")
_HEALTH_MARKER = "[main] INFO HEALTH "
_LIFECYCLE_MARKER = "[main] INFO ORDER_LIFECYCLE_JOURNAL_V2_HEALTH "


class BuyE3CurrentHostResourceGateError(RuntimeError):
    """Raised when current-host resource evidence cannot be proven exactly."""


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
            raise BuyE3CurrentHostResourceGateError(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise BuyE3CurrentHostResourceGateError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _read_json(path: Path, *, require_mode_0600: bool = True) -> tuple[dict[str, Any], Path]:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3CurrentHostResourceGateError(f"JSON path is not a regular file: {path}")
    target = candidate.resolve(strict=True)
    before = target.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise BuyE3CurrentHostResourceGateError(f"JSON file identity is unsafe: {target.name}")
    if before.st_size <= 0 or before.st_size > MAX_RECEIPT_BYTES:
        raise BuyE3CurrentHostResourceGateError(f"JSON file size is invalid: {target.name}")
    if require_mode_0600 and stat.S_IMODE(before.st_mode) != 0o600:
        raise BuyE3CurrentHostResourceGateError(f"JSON mode is not 0600: {target.name}")
    try:
        payload = json.loads(
            target.read_text(encoding="ascii"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                BuyE3CurrentHostResourceGateError(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BuyE3CurrentHostResourceGateError(f"unreadable JSON: {target.name}") from exc
    after = target.stat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise BuyE3CurrentHostResourceGateError(f"JSON changed while read: {target.name}")
    if not isinstance(payload, dict):
        raise BuyE3CurrentHostResourceGateError(f"JSON root is not an object: {target.name}")
    return payload, target


def atomic_write_receipt(path: Path, payload: Mapping[str, Any]) -> str:
    target = path.expanduser().absolute()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.parent.is_symlink() or target.exists() or target.is_symlink():
        raise BuyE3CurrentHostResourceGateError(
            f"immutable receipt path is unsafe or already exists: {target.name}"
        )
    temporary = target.with_name(f".{target.name}.tmp.{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            json.dump(dict(payload), handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
    if stat.S_IMODE(target.stat().st_mode) != 0o600:
        raise BuyE3CurrentHostResourceGateError("receipt permission drifted from 0600")
    return file_sha256(target)


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise BuyE3CurrentHostResourceGateError(f"{label} is not SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise BuyE3CurrentHostResourceGateError(f"{label} is not a Git object id")
    return normalized


def _strict_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BuyE3CurrentHostResourceGateError(f"{label} is not an integer >= {minimum}")
    return value


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BuyE3CurrentHostResourceGateError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise BuyE3CurrentHostResourceGateError(f"{label} is non-finite")
    return result


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BuyE3CurrentHostResourceGateError(f"{label} is not a mapping")
    return dict(value)


def _exact_mapping(value: Any, fields: Sequence[str], label: str) -> dict[str, Any]:
    output = _mapping(value, label)
    if set(output) != set(fields):
        raise BuyE3CurrentHostResourceGateError(f"{label} fields drifted")
    return output


def _run_git(root: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", *arguments),
        cwd=root,
        check=check,
        capture_output=True,
        text=True,
    )


def capture_git_execution(
    repository_root: Path,
    *,
    annotated_tag: str,
    runtime_authority: bool,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    commit = _run_git(root, "rev-parse", "HEAD").stdout.strip()
    tree = _run_git(root, "rev-parse", "HEAD^{tree}").stdout.strip()
    if _run_git(root, "status", "--porcelain=v1", "--untracked-files=all").stdout:
        raise BuyE3CurrentHostResourceGateError("execution checkout is not clean")
    tag_ref = f"refs/tags/{annotated_tag}"
    if _run_git(root, "cat-file", "-t", tag_ref).stdout.strip() != "tag":
        raise BuyE3CurrentHostResourceGateError("execution tag is not annotated")
    tag_object = _run_git(root, "rev-parse", tag_ref).stdout.strip()
    peeled = _run_git(root, "rev-parse", f"{tag_ref}^{{commit}}").stdout.strip()
    if peeled != commit:
        raise BuyE3CurrentHostResourceGateError("execution tag does not peel to HEAD")
    ancestor = (
        _run_git(
            root,
            "merge-base",
            "--is-ancestor",
            DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            commit,
            check=False,
        ).returncode
        == 0
    )
    if runtime_authority:
        if not ancestor:
            raise BuyE3CurrentHostResourceGateError(
                "runtime execution is not direct-successor lineage"
            )
        if (
            commit != DIRECT_SUCCESSOR_EXECUTION_COMMIT
            or tree != DIRECT_SUCCESSOR_EXECUTION_TREE
            or annotated_tag != DIRECT_SUCCESSOR_ANNOTATED_TAG
            or tag_object != DIRECT_SUCCESSOR_TAG_OBJECT
        ):
            raise BuyE3CurrentHostResourceGateError(
                "live runtime checkout is not exact direct-successor"
            )
    return {
        "repository_root": str(root),
        "execution_commit": _require_git_sha(commit, "execution commit"),
        "execution_tree": _require_git_sha(tree, "execution tree"),
        "annotated_tag": annotated_tag,
        "annotated_tag_object": _require_git_sha(tag_object, "tag object"),
        "tag_peeled_commit": _require_git_sha(peeled, "peeled commit"),
        "direct_successor_commit_is_ancestor": ancestor,
        "runtime_authority_checkout": bool(runtime_authority),
    }


def bind_current_successor_runtime_sources(
    *,
    runtime_repository_root: Path,
    collector_repository_root: Path,
) -> dict[str, Any]:
    """Prove collector imports and live files are the no-shadow successor bytes."""

    runtime_root = runtime_repository_root.expanduser().resolve(strict=True)
    collector_root = collector_repository_root.expanduser().resolve(strict=True)
    rows: dict[str, dict[str, Any]] = {}
    for role, frozen in CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items():
        relative = str(frozen["path"])
        expected = str(frozen["sha256"])
        runtime_path = runtime_root / relative
        collector_path = collector_root / relative
        runtime_working = file_sha256(runtime_path)
        collector_working = file_sha256(collector_path)
        runtime_git = hashlib.sha256(
            _run_git(
                runtime_root,
                "show",
                f"{DIRECT_SUCCESSOR_EXECUTION_COMMIT}:{relative}",
            ).stdout.encode()
        ).hexdigest()
        collector_git = hashlib.sha256(
            _run_git(collector_root, "show", f"HEAD:{relative}").stdout.encode()
        ).hexdigest()
        if {runtime_working, collector_working, runtime_git, collector_git} != {expected}:
            raise BuyE3CurrentHostResourceGateError(
                f"current successor runtime source drifted or imported stale bytes: {role}"
            )
        rows[role] = {
            "role": role,
            "repository_relative_path": relative,
            "sha256": expected,
            "runtime_working_matches_direct_successor": True,
            "collector_working_matches_direct_successor": True,
            "collector_head_matches_direct_successor": True,
        }
    return {
        "direct_successor_execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "sparse_window_repair_bound": True,
        "pre_sparse_attempt4_runtime_rejected": True,
        "files": rows,
        "runtime_source_manifest_sha256": canonical_sha256(rows),
    }


def _bind_file(path: Path, *, role: str, expected_sha256: str) -> dict[str, Any]:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3CurrentHostResourceGateError(f"{role} is not a regular deployed file")
    target = candidate.resolve(strict=True)
    metadata = target.stat()
    if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise BuyE3CurrentHostResourceGateError(f"{role} must be single-link mode 0600")
    observed = file_sha256(target)
    if observed != expected_sha256:
        raise BuyE3CurrentHostResourceGateError(f"{role} file SHA256 drifted")
    return {
        "role": role,
        "absolute_path": str(target),
        "file_sha256": observed,
        "size_bytes": metadata.st_size,
        "mode": "0600",
    }


def _validate_direct_release_payload(payload: Mapping[str, Any]) -> None:
    supplement = config_successor.FROZEN_RUNTIME_SUPPLEMENT_BINDING
    if (
        set(payload) != set(direct_release_v3.TOP_LEVEL_FIELDS)
        or payload.get("schema_version") != DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or payload.get("identity") != DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or payload.get("status") != DIRECT_SUCCESSOR_RELEASE_STATUS
        or payload.get("canonical_active_release_sha256")
        != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or document_sha256(payload, "canonical_active_release_sha256")
        != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or payload.get("research_supported") is not False
        or payload.get("formal_hierarchy_passed") is not False
        or payload.get("formal_hard_gates_passed") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("outcome_informed_owner_override") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
        or payload.get("authorization_basis") != direct_release_v3.AUTHORIZATION_BASIS
        or payload.get("scope") != direct_release_v3.SCOPE
        or payload.get("historical_evidence") != direct_release_v3.HISTORICAL_EVIDENCE
        or payload.get("runtime_fix_contract") != direct_release_v3.RUNTIME_FIX_CONTRACT
        or not isinstance(supplement, Mapping)
        or payload.get("runtime_fix_supplement") != dict(supplement)
        or payload.get("no_shadow_runtime_contract") != direct_release_v3.NO_SHADOW_RUNTIME_CONTRACT
        or payload.get("pending_current_runtime_evidence")
        != direct_release_v3.PENDING_CURRENT_RUNTIME_EVIDENCE
        or payload.get("rollback") != direct_release_v3.ROLLBACK
        or payload.get("evidence_boundary") != direct_release_v3.EVIDENCE_BOUNDARY
        or "incomplete_evidence" in payload
        or "panel_rebuild_continues" in json.dumps(payload, sort_keys=True)
    ):
        raise BuyE3CurrentHostResourceGateError("direct-successor release-v3 identity drifted")
    expected_execution = {
        "execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "execution_tree": DIRECT_SUCCESSOR_EXECUTION_TREE,
        "annotated_operational_tag": DIRECT_SUCCESSOR_ANNOTATED_TAG,
        "annotated_operational_tag_object": DIRECT_SUCCESSOR_TAG_OBJECT,
        "tag_peeled_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
    }
    if payload.get("execution") != expected_execution:
        raise BuyE3CurrentHostResourceGateError("direct-successor release execution drifted")
    config_pair = _mapping(payload.get("config_pair"), "direct-successor config pair")
    disabled_config = _mapping(config_pair.get("disabled"), "release disabled config")
    active_config = _mapping(config_pair.get("active"), "release active config")
    if (
        config_pair.get("schema_version") != "f05_buy_e3_no_shadow_config_pair.v1"
        or config_pair.get("status") != "exact_no_shadow_config_pair_frozen"
        or disabled_config.get("file_sha256") != EXPECTED_DISABLED_CONFIG_SHA256
        or active_config.get("file_sha256") != config_successor.CORRECTED_ACTIVE_SHA256
        or disabled_config.get("semantic_sha256")
        != config_successor.CORRECTED_DISABLED_SEMANTIC_SHA256
        or active_config.get("semantic_sha256") != config_successor.CORRECTED_ACTIVE_SEMANTIC_SHA256
        or config_pair.get("old_to_new_semantic_additions")
        != list(config_successor.ADDED_FALSE_PATHS)
        or config_pair.get("active_disabled_only_difference")
        != config_successor.ACTIVE_DISABLED_DIFFERENCE
        or config_pair.get("release_fields_present_in_yaml") is not False
    ):
        raise BuyE3CurrentHostResourceGateError("direct-successor config pair drifted")
    artifact = _mapping(payload.get("exact_artifact"), "direct-successor exact artifact")
    roles = _mapping(artifact.get("roles"), "direct-successor artifact roles")
    if artifact.get("artifact_sha256") != EXACT_ARTIFACT_SHA256 or set(roles) != {
        "manifest",
        "policy",
        "predicate_bundle",
    }:
        raise BuyE3CurrentHostResourceGateError("direct-successor artifact identity drifted")
    for role in ("manifest", "policy", "predicate_bundle"):
        binding = _mapping(roles.get(role), f"direct-successor {role} binding")
        if (
            binding.get("role") != role
            or binding.get("file_sha256") != EXACT_DEPLOYED_FILE_SHA256[role]
            or binding.get("mode") != "0600"
        ):
            raise BuyE3CurrentHostResourceGateError(f"direct-successor {role} binding drifted")
    if roles["manifest"].get("canonical_sha256") != EXACT_ARTIFACT_SHA256:
        raise BuyE3CurrentHostResourceGateError("direct-successor manifest canonical drifted")


def bind_exact_deployed_files(
    *,
    runtime_repository_root: Path,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    direct_active_release_path: Path,
) -> dict[str, Any]:
    root = runtime_repository_root.expanduser().resolve(strict=True)
    paths = {
        "manifest": manifest_path,
        "policy": policy_path,
        "predicate_bundle": predicate_bundle_path,
        "direct_active_release": direct_active_release_path,
    }
    bindings = {
        role: _bind_file(path, role=role, expected_sha256=EXACT_DEPLOYED_FILE_SHA256[role])
        for role, path in paths.items()
    }
    resolved = [Path(binding["absolute_path"]) for binding in bindings.values()]
    if len(set(resolved)) != 4 or any(not path.is_relative_to(root) for path in resolved):
        raise BuyE3CurrentHostResourceGateError(
            "four deployed paths must be unique files below the runtime repository"
        )
    release_payload, _ = _read_json(resolved[3])
    _validate_direct_release_payload(release_payload)
    runtime = LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=resolved[0],
        artifact_manifest_sha256=EXACT_DEPLOYED_FILE_SHA256["manifest"],
        expected_artifact_sha256=EXACT_ARTIFACT_SHA256,
        policy_path=resolved[1],
        policy_sha256=EXACT_DEPLOYED_FILE_SHA256["policy"],
        predicate_bundle_path=resolved[2],
        predicate_bundle_sha256=EXACT_DEPLOYED_FILE_SHA256["predicate_bundle"],
        active_release_path=resolved[3],
        active_release_file_sha256=DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
        active_release_canonical_sha256=DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256,
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    identity = runtime.active_release_identity
    if (
        runtime.artifact_sha256 != EXACT_ARTIFACT_SHA256
        or identity.get("file_sha256") != DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or identity.get("file_canonical_sha256") != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or identity.get("execution_commit") != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or identity.get("execution_tree") != DIRECT_SUCCESSOR_EXECUTION_TREE
        or identity.get("annotated_operational_tag") != DIRECT_SUCCESSOR_ANNOTATED_TAG
        or identity.get("annotated_operational_tag_object") != DIRECT_SUCCESSOR_TAG_OBJECT
    ):
        raise BuyE3CurrentHostResourceGateError("four-file runtime loader identity drifted")
    return {
        "artifact_sha256": EXACT_ARTIFACT_SHA256,
        "files": bindings,
        "direct_release_canonical_sha256": DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256,
        "binding_manifest_sha256": canonical_sha256(bindings),
    }


def _load_yaml(path: Path) -> dict[str, Any]:
    candidate = path.expanduser().resolve(strict=True)
    try:
        payload = yaml.load(candidate.read_text(encoding="utf-8"), Loader=_UniqueKeySafeLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise BuyE3CurrentHostResourceGateError("live config is unreadable") from exc
    if not isinstance(payload, dict):
        raise BuyE3CurrentHostResourceGateError("live config root is not a mapping")
    return payload


def _validate_disabled_config(path: Path) -> str:
    target = path.expanduser().resolve(strict=True)
    observed = file_sha256(target)
    if observed != EXPECTED_DISABLED_CONFIG_SHA256:
        raise BuyE3CurrentHostResourceGateError("disabled config SHA256 drifted")
    payload = _load_yaml(target)
    strategy = _mapping(payload.get("strategy"), "strategy config")
    expected_flags = {
        "buy_e3_cooldown_policy_enabled": False,
        "boolean_cooldown_policy_enabled": True,
        "buy_fill_selection_shadow_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": False,
        "cross_venue_fair_price_shadow_enabled": False,
    }
    if any(strategy.get(name) is not value for name, value in expected_flags.items()):
        raise BuyE3CurrentHostResourceGateError("disabled/no-shadow config flags drifted")
    logging = _mapping(payload.get("logging"), "logging config")
    expected_logging_flags = {
        "inventory_campaign_shadow_enabled": False,
        "market_tape_enabled": False,
    }
    if any(logging.get(name) is not value for name, value in expected_logging_flags.items()):
        raise BuyE3CurrentHostResourceGateError("disabled/no-shadow config flags drifted")
    external = _mapping(payload.get("external_venues"), "external_venues config")
    if external.get("enabled") is not False:
        raise BuyE3CurrentHostResourceGateError("external venue shadow input is enabled")
    multi_market = _mapping(payload.get("multi_market"), "multi_market config")
    for name in (
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
    ):
        if name not in multi_market or multi_market.get(name) is not False:
            raise BuyE3CurrentHostResourceGateError(
                f"multi_market.{name} is not explicitly disabled"
            )
    exact_values = {
        "buy_e3_cooldown_artifact_manifest_sha256": EXACT_DEPLOYED_FILE_SHA256["manifest"],
        "buy_e3_cooldown_artifact_sha256": EXACT_ARTIFACT_SHA256,
        "buy_e3_cooldown_policy_sha256": EXACT_DEPLOYED_FILE_SHA256["policy"],
        "buy_e3_cooldown_predicate_bundle_sha256": EXACT_DEPLOYED_FILE_SHA256["predicate_bundle"],
    }
    if any(strategy.get(name) != value for name, value in exact_values.items()):
        raise BuyE3CurrentHostResourceGateError("disabled config artifact binding drifted")
    return observed


def _validate_config_correction(
    path: Path,
    *,
    collector_execution: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        payload, binding = config_successor.validate_content_receipt(path)
    except Exception as exc:
        raise BuyE3CurrentHostResourceGateError("config correction receipt is invalid") from exc
    if payload.get("collector_execution") != dict(collector_execution):
        raise BuyE3CurrentHostResourceGateError(
            "config correction collector execution does not match resource collector"
        )
    corrected = _mapping(payload.get("corrected_config_pair"), "corrected config pair")
    disabled = _mapping(corrected.get("disabled"), "corrected disabled config")
    active = _mapping(corrected.get("active"), "corrected active config")
    authority = _mapping(payload.get("runtime_authority"), "config runtime authority")
    if (
        disabled.get("file_sha256") != EXPECTED_DISABLED_CONFIG_SHA256
        or active.get("file_sha256") != config_successor.CORRECTED_ACTIVE_SHA256
        or disabled.get("semantic_sha256") != config_successor.CORRECTED_DISABLED_SEMANTIC_SHA256
        or active.get("semantic_sha256") != config_successor.CORRECTED_ACTIVE_SEMANTIC_SHA256
        or authority.get("file_sha256") != DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or authority.get("canonical_sha256") != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    ):
        raise BuyE3CurrentHostResourceGateError("config correction semantic identity drifted")
    return dict(binding)


def _proc_stat_fields(proc_root: Path, pid: int) -> list[str]:
    try:
        raw = (proc_root / str(pid) / "stat").read_text(encoding="ascii")
    except OSError as exc:
        raise BuyE3CurrentHostResourceGateError("process stat disappeared") from exc
    close = raw.rfind(")")
    fields = raw[close + 2 :].split() if close >= 0 else []
    if len(fields) <= 21:
        raise BuyE3CurrentHostResourceGateError("process stat is malformed")
    return fields


def _proc_start_ticks(proc_root: Path, pid: int) -> int:
    # starttime is field 22 overall, or index 19 after removing pid/comm.
    return _strict_int(int(_proc_stat_fields(proc_root, pid)[19]), "process start ticks", minimum=1)


def _proc_cmdline(proc_root: Path, pid: int) -> list[str]:
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()
    except OSError as exc:
        raise BuyE3CurrentHostResourceGateError("process command line disappeared") from exc
    values = [part.decode("utf-8", errors="strict") for part in raw.split(b"\0") if part]
    if not values:
        raise BuyE3CurrentHostResourceGateError("process command line is empty")
    return values


def _cmdline_config(arguments: Sequence[str]) -> Path:
    try:
        index = list(arguments).index("--config")
        value = arguments[index + 1]
    except (ValueError, IndexError) as exc:
        raise BuyE3CurrentHostResourceGateError("live command line lacks --config") from exc
    return Path(value).expanduser().absolute()


def _stable_process_identity(payload: Mapping[str, Any]) -> str:
    fields = {
        "pid": payload["pid"],
        "pid_start_ticks": payload["pid_start_ticks"],
        "cmdline_sha256": payload["cmdline_sha256"],
        "cwd": payload["cwd"],
        "python_executable": payload["python_executable"],
        "config_path": payload["config_path"],
        "config_sha256": payload["config_sha256"],
    }
    return canonical_sha256(fields)


def capture_process_snapshot(
    *,
    runtime_repository_root: Path,
    runtime_annotated_tag: str,
    pid_file: Path,
    config_path: Path,
    expected_buy_e3_enabled: bool,
    proc_root: Path = Path("/proc"),
    generated_utc: str | None = None,
) -> dict[str, Any]:
    root = runtime_repository_root.expanduser().resolve(strict=True)
    try:
        pid = int(pid_file.expanduser().resolve(strict=True).read_text(encoding="ascii").strip())
    except (OSError, ValueError) as exc:
        raise BuyE3CurrentHostResourceGateError("pid file is unreadable") from exc
    _strict_int(pid, "live PID", minimum=1)
    command = _proc_cmdline(proc_root, pid)
    if not any(value.endswith("live/main.py") or value == "live/main.py" for value in command):
        raise BuyE3CurrentHostResourceGateError("PID does not identify live/main.py")
    observed_config = _cmdline_config(command).resolve(strict=True)
    expected_config = config_path.expanduser().resolve(strict=True)
    if observed_config != expected_config:
        raise BuyE3CurrentHostResourceGateError("live process uses another config")
    cwd = (proc_root / str(pid) / "cwd").resolve(strict=True)
    executable = (proc_root / str(pid) / "exe").resolve(strict=True)
    if cwd != root:
        raise BuyE3CurrentHostResourceGateError("live process cwd is not runtime repository")
    execution = capture_git_execution(
        root,
        annotated_tag=runtime_annotated_tag,
        runtime_authority=True,
    )
    config_sha = file_sha256(expected_config)
    if expected_buy_e3_enabled:
        config_payload = _load_yaml(expected_config)
        strategy = _mapping(config_payload.get("strategy"), "strategy config")
        if strategy.get("buy_e3_cooldown_policy_enabled") is not True:
            raise BuyE3CurrentHostResourceGateError("expected active process is not active")
    else:
        config_sha = _validate_disabled_config(expected_config)
    payload: dict[str, Any] = {
        "schema_version": PROCESS_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": PROCESS_STATUS,
        "captured_utc": generated_utc or _utc_now(),
        "pid": pid,
        "pid_start_ticks": _proc_start_ticks(proc_root, pid),
        "cmdline_sha256": canonical_sha256(command),
        "cwd": str(cwd),
        "python_executable": str(executable),
        "config_path": str(expected_config),
        "config_sha256": config_sha,
        "buy_e3_enabled": bool(expected_buy_e3_enabled),
        "runtime_execution": execution,
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["stable_process_identity_sha256"] = _stable_process_identity(payload)
    payload[PROCESS_CANONICAL_FIELD] = document_sha256(payload, PROCESS_CANONICAL_FIELD)
    return payload


def write_process_snapshot(output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = capture_process_snapshot(**kwargs)
    file_hash = atomic_write_receipt(output_path, payload)
    validate_process_snapshot(output_path)
    return payload, file_hash


def validate_process_snapshot(path: Path) -> dict[str, Any]:
    payload, _ = _read_json(path)
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "captured_utc",
        "pid",
        "pid_start_ticks",
        "cmdline_sha256",
        "cwd",
        "python_executable",
        "config_path",
        "config_sha256",
        "buy_e3_enabled",
        "runtime_execution",
        "evidence_boundary",
        "stable_process_identity_sha256",
        PROCESS_CANONICAL_FIELD,
    }
    execution = _mapping(payload.get("runtime_execution"), "process runtime execution")
    expected_execution = {
        "repository_root": payload.get("cwd"),
        "execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "execution_tree": DIRECT_SUCCESSOR_EXECUTION_TREE,
        "annotated_tag": DIRECT_SUCCESSOR_ANNOTATED_TAG,
        "annotated_tag_object": DIRECT_SUCCESSOR_TAG_OBJECT,
        "tag_peeled_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "direct_successor_commit_is_ancestor": True,
        "runtime_authority_checkout": True,
    }
    if (
        set(payload) != expected_fields
        or payload.get("schema_version") != PROCESS_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != PROCESS_STATUS
        or _strict_int(payload.get("pid"), "process PID", minimum=1) <= 0
        or _strict_int(payload.get("pid_start_ticks"), "process start ticks", minimum=1) <= 0
        or _require_sha256(payload.get("cmdline_sha256"), "command hash")
        != payload.get("cmdline_sha256")
        or not PurePosixPath(str(payload.get("cwd", ""))).is_absolute()
        or not PurePosixPath(str(payload.get("python_executable", ""))).is_absolute()
        or not PurePosixPath(str(payload.get("config_path", ""))).is_absolute()
        or _require_sha256(payload.get("config_sha256"), "config hash")
        != payload.get("config_sha256")
        or type(payload.get("buy_e3_enabled")) is not bool
        or execution != expected_execution
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get("stable_process_identity_sha256") != _stable_process_identity(payload)
        or payload.get(PROCESS_CANONICAL_FIELD) != document_sha256(payload, PROCESS_CANONICAL_FIELD)
    ):
        raise BuyE3CurrentHostResourceGateError("process snapshot identity drifted")
    return dict(payload)


def _numeric(value: str) -> int | float | str:
    try:
        number = float(value)
    except ValueError:
        return value
    if not math.isfinite(number):
        return value
    return int(number) if number.is_integer() else number


def _parse_main_health(line: str, *, generation: int) -> dict[str, Any]:
    if _HEALTH_MARKER not in line:
        raise BuyE3CurrentHostResourceGateError("main HEALTH line marker is missing")
    timestamp = _LOG_TS_RE.match(line)
    if timestamp is None:
        raise BuyE3CurrentHostResourceGateError("main HEALTH timestamp is missing")
    parsed = {key: _numeric(value) for key, value in _KV_RE.findall(line)}
    named_safety_counters = {
        name
        for name in parsed
        if name.endswith(("Dropped", "Invalid", "Overflow")) or "Invalid" in name
    }
    if named_safety_counters != EXPECTED_NAMED_DROP_INVALID_OVERFLOW:
        raise BuyE3CurrentHostResourceGateError(
            "main HEALTH drop/invalid/overflow counter set drifted"
        )
    required = {
        "booleanCooldownEnabled",
        "booleanCooldownUpdates",
        "buyE3CooldownEnabled",
        "deepBookBuffer",
        "externalSources",
        "globalRefReason",
        "globalFlowReason",
        *GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *WINDOW_ZERO_COUNTERS[:-2],
    }
    missing = required - set(parsed)
    if missing:
        raise BuyE3CurrentHostResourceGateError(
            f"main HEALTH line lacks resource fields: {sorted(missing)}"
        )
    absolute_zero_fields = (
        "externalSources",
        *GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
    )
    nonzero = sorted(name for name in absolute_zero_fields if parsed.get(name) != 0)
    if nonzero:
        raise BuyE3CurrentHostResourceGateError(
            "disabled shadow evaluator HEALTH fields are non-zero: " + ", ".join(nonzero)
        )
    if (
        parsed.get("globalFlowReason") != SHADOW_DISABLED_REASON
        or parsed.get("globalRefReason") != SHADOW_DISABLED_REASON
    ):
        raise BuyE3CurrentHostResourceGateError("disabled shadow evaluator HEALTH reason drifted")
    return {
        "generation": generation,
        "wall_timestamp_s": datetime.strptime(timestamp.group(1), "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=UTC)
        .timestamp(),
        "line_sha256": hashlib.sha256(line.encode("utf-8")).hexdigest(),
        "boolean_cooldown_enabled": int(parsed["booleanCooldownEnabled"]),
        "boolean_cooldown_updates": int(parsed["booleanCooldownUpdates"]),
        "buy_e3_enabled": int(parsed["buyE3CooldownEnabled"]),
        "deep_book_buffer": int(parsed["deepBookBuffer"]),
        "shadow_disabled_state": {
            **{name: int(parsed[name]) for name in absolute_zero_fields},
            "globalFlowReason": str(parsed["globalFlowReason"]),
            "globalRefReason": str(parsed["globalRefReason"]),
        },
        "counter_values": {name: int(parsed[name]) for name in WINDOW_ZERO_COUNTERS[:-2]},
    }


def _parse_lifecycle_health(line: str, *, generation: int) -> dict[str, Any]:
    if _LIFECYCLE_MARKER not in line:
        raise BuyE3CurrentHostResourceGateError("lifecycle HEALTH line marker is missing")
    parsed = {key: _numeric(value) for key, value in _KV_RE.findall(line)}
    if "drops" not in parsed or "errors" not in parsed:
        raise BuyE3CurrentHostResourceGateError("lifecycle HEALTH line lacks counters")
    return {
        "generation": generation,
        "line_sha256": hashlib.sha256(line.encode("utf-8")).hexdigest(),
        "counter_values": {
            "orderLifecycleV2Drops": int(parsed["drops"]),
            "orderLifecycleV2Errors": int(parsed["errors"]),
        },
    }


class LiveHealthTail:
    """Read existing aggregate HEALTH log lines; never connect to a live stream."""

    def __init__(self, path: Path, *, initial_tail_bytes: int = 8 << 20) -> None:
        candidate = path.expanduser().absolute()
        if candidate.is_symlink() or not candidate.is_file():
            raise BuyE3CurrentHostResourceGateError("live log is not a regular non-symlink file")
        self.path = candidate.resolve(strict=True)
        metadata = self.path.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise BuyE3CurrentHostResourceGateError("live log is not a regular file")
        self._device = metadata.st_dev
        self._inode = metadata.st_ino
        self.offset = 0
        self.pending = b""
        self.main_generation = 0
        self.lifecycle_generation = 0
        self.main: dict[str, Any] | None = None
        self.lifecycle: dict[str, Any] | None = None
        size = metadata.st_size
        start = max(0, size - int(initial_tail_bytes))
        with self.path.open("rb") as handle:
            handle.seek(start)
            data = handle.read()
            self.offset = handle.tell()
        if start:
            _discarded, separator, data = data.partition(b"\n")
            if not separator:
                data = b""
        self._consume(data)
        if self.main is None or self.lifecycle is None:
            raise BuyE3CurrentHostResourceGateError("live log lacks required HEALTH states")

    def _consume(self, payload: bytes) -> None:
        rows = (self.pending + payload).splitlines(keepends=True)
        self.pending = b""
        if rows and not rows[-1].endswith((b"\n", b"\r")):
            self.pending = rows.pop()
        for raw in rows:
            if _HEALTH_MARKER.encode() in raw:
                # Historical runtimes share the log but do not expose the
                # explicit disabled/error markers.  They cannot become a v8
                # baseline; ignore them and fail later if no successor row is
                # observed.
                if b"globalFlowShadowEnabled=" not in raw:
                    continue
                self.main_generation += 1
                self.main = _parse_main_health(
                    raw.rstrip(b"\r\n").decode("utf-8", errors="strict"),
                    generation=self.main_generation,
                )
            elif _LIFECYCLE_MARKER.encode() in raw:
                self.lifecycle_generation += 1
                self.lifecycle = _parse_lifecycle_health(
                    raw.rstrip(b"\r\n").decode("utf-8", errors="strict"),
                    generation=self.lifecycle_generation,
                )

    def snapshot(self) -> dict[str, Any]:
        metadata = self.path.stat()
        size = metadata.st_size
        if (
            metadata.st_dev != self._device
            or metadata.st_ino != self._inode
            or not stat.S_ISREG(metadata.st_mode)
            or size < self.offset
        ):
            raise BuyE3CurrentHostResourceGateError("live log rotated during resource gate")
        if size > self.offset:
            with self.path.open("rb") as handle:
                handle.seek(self.offset)
                payload = handle.read()
                self.offset = handle.tell()
            self._consume(payload)
        if self.main is None or self.lifecycle is None:
            raise BuyE3CurrentHostResourceGateError("live HEALTH state disappeared")
        counters = {
            **self.main["counter_values"],
            **self.lifecycle["counter_values"],
        }
        return {
            "main_generation": self.main["generation"],
            "main_wall_timestamp_s": self.main["wall_timestamp_s"],
            "main_line_sha256": self.main["line_sha256"],
            "lifecycle_generation": self.lifecycle["generation"],
            "lifecycle_line_sha256": self.lifecycle["line_sha256"],
            "boolean_cooldown_enabled": self.main["boolean_cooldown_enabled"],
            "boolean_cooldown_updates": self.main["boolean_cooldown_updates"],
            "buy_e3_enabled": self.main["buy_e3_enabled"],
            "deep_book_buffer": self.main["deep_book_buffer"],
            "shadow_disabled_state": dict(self.main["shadow_disabled_state"]),
            "counter_values": counters,
        }


def _capture_current_process_rate_window(
    *,
    health_tail: Any,
    disabled_process: Mapping[str, Any],
    runtime_repository_root: Path,
    disabled_config_path: Path,
    proc_root: Path,
    sample_interval_s: float,
    timeout_s: float,
    sleep: Callable[[float], None],
    monotonic_ns: Callable[[], int],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], float]:
    """Capture two consecutive fresh HEALTH rows from one current process.

    The first snapshot is a boundary only.  It may belong to a predecessor PID
    and is never used in the rate calculation.  Both rate rows must be appended
    after that boundary while the exact disabled PID/start-ticks identity stays
    unchanged.  This makes a predecessor-high/current-low counter transition
    harmless without accepting a negative delta.
    """

    expected_identity = disabled_process.get("stable_process_identity_sha256")

    def assert_current_process() -> None:
        identity = _process_identity_from_live(
            runtime_repository_root=runtime_repository_root,
            pid=_strict_int(disabled_process.get("pid"), "disabled PID", minimum=1),
            config_path=disabled_config_path,
            proc_root=proc_root,
        )
        if _stable_live_identity(identity) != expected_identity:
            raise BuyE3CurrentHostResourceGateError(
                "live process changed during callback-rate window"
            )

    assert_current_process()
    boundary = dict(health_tail.snapshot())
    assert_current_process()
    deadline = monotonic_ns() + int(_finite(timeout_s, "rate timeout") * 1e9)
    first: dict[str, Any] | None = None
    while monotonic_ns() <= deadline:
        assert_current_process()
        candidate = dict(health_tail.snapshot())
        assert_current_process()
        generation = _strict_int(candidate.get("main_generation"), "HEALTH generation", minimum=1)
        lifecycle_generation = _strict_int(
            candidate.get("lifecycle_generation"), "lifecycle HEALTH generation", minimum=1
        )
        if generation <= _strict_int(
            boundary.get("main_generation"), "rate boundary generation", minimum=1
        ) or lifecycle_generation <= _strict_int(
            boundary.get("lifecycle_generation"),
            "rate lifecycle boundary generation",
            minimum=1,
        ):
            sleep(min(sample_interval_s, 0.25))
            continue
        first = candidate
        break
    if first is None:
        raise BuyE3CurrentHostResourceGateError(
            "no first current-process HEALTH state for callback rate"
        )

    while monotonic_ns() <= deadline:
        assert_current_process()
        candidate = dict(health_tail.snapshot())
        assert_current_process()
        first_generation = _strict_int(
            first.get("main_generation"), "first rate generation", minimum=1
        )
        candidate_generation = _strict_int(
            candidate.get("main_generation"), "second rate generation", minimum=1
        )
        first_lifecycle_generation = _strict_int(
            first.get("lifecycle_generation"), "first lifecycle generation", minimum=1
        )
        candidate_lifecycle_generation = _strict_int(
            candidate.get("lifecycle_generation"),
            "second lifecycle generation",
            minimum=1,
        )
        if (
            candidate_generation <= first_generation
            or candidate_lifecycle_generation <= first_lifecycle_generation
        ):
            sleep(min(sample_interval_s, 0.25))
            continue
        if (
            candidate_generation != first_generation + 1
            or candidate_lifecycle_generation != first_lifecycle_generation + 1
        ):
            # The tail advanced by multiple lines between polls.  We cannot
            # reconstruct the skipped aggregate row, so restart the pair from
            # the newest observed state and require its immediate successor.
            first = candidate
            continue
        update_delta = _strict_int(
            candidate.get("boolean_cooldown_updates"),
            "second callback updates",
            minimum=0,
        ) - _strict_int(
            first.get("boolean_cooldown_updates"),
            "first callback updates",
            minimum=0,
        )
        if update_delta < 0:
            raise BuyE3CurrentHostResourceGateError(
                "current-process callback-rate delta is negative"
            )
        elapsed_s = _finite(
            candidate.get("main_wall_timestamp_s"), "second HEALTH timestamp"
        ) - _finite(first.get("main_wall_timestamp_s"), "first HEALTH timestamp")
        if update_delta == 0 or elapsed_s <= 0.0:
            raise BuyE3CurrentHostResourceGateError(
                "observed current-process callback rate did not advance"
            )
        return boundary, first, candidate, update_delta / elapsed_s
    raise BuyE3CurrentHostResourceGateError(
        "no second consecutive current-process HEALTH state for callback rate"
    )


def _percentile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise BuyE3CurrentHostResourceGateError("latency sample is empty")
    ordered = sorted(float(value) for value in values)
    return ordered[max(0, math.ceil(len(ordered) * probability) - 1)]


def _max_rss_mib() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / 1024.0 if Path("/proc").is_dir() else value / (1024.0 * 1024.0)


def _event_mid(seconds: float) -> float:
    return (
        70_000.0
        + 35.0 * math.sin(seconds * 2.0 * math.pi / 17.0)
        + 80.0 * math.sin(seconds * 2.0 * math.pi / 311.0)
    )


def _observe(policy: LiveBuyE3CooldownPolicy, ts_ns: int, seconds: float) -> None:
    mid = _event_mid(seconds)
    policy.observe_depth(
        receive_ts_ns=int(ts_ns),
        bids=((mid - 0.5, 1.0),),
        asks=((mid + 0.5, 1.0),),
        market_generation=max(1, int(ts_ns // BASE_WINDOW_WIDTH_NS)),
        depth_generation=max(1, int(ts_ns // BASE_WINDOW_WIDTH_NS)),
    )


def run_exact_four_file_benchmark(
    *,
    collector_repository_root: Path,
    runtime_repository_root: Path,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    direct_active_release_path: Path,
    observed_live_rate_hz: float,
    output_path: Path,
    paced_duration_s: float = 15.0,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Time the exact deployed four-file path without retaining action rows."""

    observed_rate = _finite(observed_live_rate_hz, "observed callback rate")
    if observed_rate <= 0.0:
        raise BuyE3CurrentHostResourceGateError("observed callback rate must be positive")
    duration = max(2.0, _finite(paced_duration_s, "paced duration"))
    target_rate = max(100.0, MIN_RATE_MULTIPLIER * observed_rate)
    deployed = bind_exact_deployed_files(
        runtime_repository_root=runtime_repository_root,
        manifest_path=manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        direct_active_release_path=direct_active_release_path,
    )
    runtime_sources = bind_current_successor_runtime_sources(
        runtime_repository_root=runtime_repository_root,
        collector_repository_root=collector_repository_root,
    )
    files = deployed["files"]
    policy = LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=files["manifest"]["absolute_path"],
        artifact_manifest_sha256=files["manifest"]["file_sha256"],
        expected_artifact_sha256=EXACT_ARTIFACT_SHA256,
        policy_path=files["policy"]["absolute_path"],
        policy_sha256=files["policy"]["file_sha256"],
        predicate_bundle_path=files["predicate_bundle"]["absolute_path"],
        predicate_bundle_sha256=files["predicate_bundle"]["file_sha256"],
        active_release_path=files["direct_active_release"]["absolute_path"],
        active_release_file_sha256=files["direct_active_release"]["file_sha256"],
        active_release_canonical_sha256=DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256,
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    cold = policy.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=200.0,
        decision_ts_ns=1,
        snapshot_id="aggregate-benchmark-cold",
    )
    if cold.action_id != CONTROL_ACTION or cold.fallback_reason is None:
        raise BuyE3CurrentHostResourceGateError("cold benchmark did not fail closed to B0")
    base_ns = 10_000_000_000
    warmup_windows = 20_490
    for ordinal in range(warmup_windows + 1):
        _observe(policy, base_ns + ordinal * BASE_WINDOW_WIDTH_NS + 1, ordinal / 10.0)
    synthetic_ns = base_ns + (warmup_windows + 1) * BASE_WINDOW_WIDTH_NS + 1
    warm = policy.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=400.0,
        decision_ts_ns=synthetic_ns,
        snapshot_id="aggregate-benchmark-warm",
    )
    if warm.fallback_reason in {
        "no_completed_receive_time_window",
        "receive_time_ema_warmup_incomplete",
        "selected_predicate_state_unobserved",
    }:
        raise BuyE3CurrentHostResourceGateError("benchmark warmup did not identify state")

    callback_count = max(1, math.ceil(target_rate * duration))
    callback_latency_us: list[float] = []
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    for ordinal in range(callback_count):
        deadline = wall_started + ordinal / target_rate
        remaining = deadline - time.perf_counter()
        if remaining > 0.0005:
            time.sleep(remaining - 0.00025)
        while time.perf_counter() < deadline:
            pass
        synthetic_ns += max(1, round(1_000_000_000.0 / target_rate))
        started_ns = time.perf_counter_ns()
        _observe(policy, synthetic_ns, 2049.0 + ordinal / target_rate)
        callback_latency_us.append((time.perf_counter_ns() - started_ns) / 1_000.0)
    wall_elapsed = max(time.perf_counter() - wall_started, 1e-9)
    cpu_elapsed = max(time.process_time() - cpu_started, 0.0)

    decision_latency_us: list[float] = []
    for ordinal in range(EXACT_DECISION_COUNT):
        started_ns = time.perf_counter_ns()
        # The result is intentionally discarded.  This is an isolated synthetic
        # timing call, not a live-stream score and not a persisted action row.
        policy.evaluate(
            side="BUY",
            baseline_duration_ms=85_000 * (1 + ordinal % 4),
            campaign_age_s=float(ordinal % 600),
            decision_ts_ns=synthetic_ns,
            snapshot_id=f"aggregate-benchmark-{ordinal}",
        )
        decision_latency_us.append((time.perf_counter_ns() - started_ns) / 1_000.0)

    achieved_rate = callback_count / wall_elapsed
    callback_p99 = _percentile(callback_latency_us, 0.99)
    decision_p99 = _percentile(decision_latency_us, 0.99)
    checks = {
        "exact_four_deployed_files_bound": len(files) == 4,
        "true_2x_observed_callback_rate": achieved_rate >= MIN_RATE_MULTIPLIER * observed_rate,
        "callback_p99_at_most_2ms": callback_p99 <= MAX_CALLBACK_P99_US,
        "exactly_1000_decisions": len(decision_latency_us) == EXACT_DECISION_COUNT,
        "decision_p99_at_most_10ms": decision_p99 <= MAX_DECISION_P99_US,
        "aggregate_only_no_action_rows": True,
    }
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise BuyE3CurrentHostResourceGateError("four-file benchmark failed: " + ", ".join(failed))
    payload: dict[str, Any] = {
        "schema_version": BENCHMARK_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": BENCHMARK_STATUS,
        "generated_utc": generated_utc or _utc_now(),
        "authority_design": dict(AUTHORITY_DESIGN),
        "runtime_sources": runtime_sources,
        "exact_deployed_files": deployed,
        "thresholds": {
            "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": MAX_CALLBACK_P99_US,
            "exact_decision_count": EXACT_DECISION_COUNT,
            "max_decision_p99_us": MAX_DECISION_P99_US,
        },
        "callback_benchmark": {
            "observed_live_rate_hz": observed_rate,
            "target_rate_hz": target_rate,
            "callback_count": callback_count,
            "duration_s": wall_elapsed,
            "achieved_rate_hz": achieved_rate,
            "achieved_to_observed_rate": achieved_rate / observed_rate,
            "latency_p50_us": statistics.median(callback_latency_us),
            "latency_p99_us": callback_p99,
            "latency_max_us": max(callback_latency_us),
            "cpu_percent_total_host_scale": (
                cpu_elapsed / wall_elapsed / CURRENT_LOGICAL_CPU_COUNT * 100.0
            ),
        },
        "decision_benchmark": {
            "decision_count": len(decision_latency_us),
            "latency_p50_us": statistics.median(decision_latency_us),
            "latency_p99_us": decision_p99,
            "latency_max_us": max(decision_latency_us),
        },
        "benchmark_process_max_rss_mib": _max_rss_mib(),
        "checks": checks,
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[BENCHMARK_CANONICAL_FIELD] = document_sha256(payload, BENCHMARK_CANONICAL_FIELD)
    atomic_write_receipt(output_path, payload)
    validate_benchmark_receipt(output_path)
    return payload


def _validate_exact_deployed_binding(raw: Any) -> dict[str, Any]:
    binding = _exact_mapping(
        raw,
        (
            "artifact_sha256",
            "files",
            "direct_release_canonical_sha256",
            "binding_manifest_sha256",
        ),
        "exact deployed files",
    )
    files = _exact_mapping(
        binding["files"], tuple(EXACT_DEPLOYED_FILE_SHA256), "exact deployed file roles"
    )
    for role, expected_sha in EXACT_DEPLOYED_FILE_SHA256.items():
        row = _exact_mapping(
            files[role],
            ("role", "absolute_path", "file_sha256", "size_bytes", "mode"),
            f"deployed {role}",
        )
        if (
            row["role"] != role
            or not PurePosixPath(str(row["absolute_path"])).is_absolute()
            or row["file_sha256"] != expected_sha
            or _strict_int(row["size_bytes"], f"{role} size", minimum=1) <= 0
            or row["mode"] != "0600"
        ):
            raise BuyE3CurrentHostResourceGateError(f"deployed {role} binding drifted")
    if (
        binding["artifact_sha256"] != EXACT_ARTIFACT_SHA256
        or binding["direct_release_canonical_sha256"] != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or binding["binding_manifest_sha256"] != canonical_sha256(files)
    ):
        raise BuyE3CurrentHostResourceGateError("exact deployed binding drifted")
    return binding


def _validate_runtime_source_binding(raw: Any) -> dict[str, Any]:
    binding = _exact_mapping(
        raw,
        (
            "direct_successor_execution_commit",
            "sparse_window_repair_bound",
            "pre_sparse_attempt4_runtime_rejected",
            "files",
            "runtime_source_manifest_sha256",
        ),
        "current successor runtime sources",
    )
    files = _exact_mapping(
        binding["files"], tuple(CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256), "runtime source roles"
    )
    for role, frozen in CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items():
        row = _exact_mapping(
            files[role],
            (
                "role",
                "repository_relative_path",
                "sha256",
                "runtime_working_matches_direct_successor",
                "collector_working_matches_direct_successor",
                "collector_head_matches_direct_successor",
            ),
            f"runtime source {role}",
        )
        if (
            row["role"] != role
            or row["repository_relative_path"] != frozen["path"]
            or row["sha256"] != frozen["sha256"]
            or row["runtime_working_matches_direct_successor"] is not True
            or row["collector_working_matches_direct_successor"] is not True
            or row["collector_head_matches_direct_successor"] is not True
        ):
            raise BuyE3CurrentHostResourceGateError(f"runtime source binding drifted: {role}")
    if (
        binding["direct_successor_execution_commit"] != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or binding["sparse_window_repair_bound"] is not True
        or binding["pre_sparse_attempt4_runtime_rejected"] is not True
        or binding["runtime_source_manifest_sha256"] != canonical_sha256(files)
    ):
        raise BuyE3CurrentHostResourceGateError("runtime source manifest drifted")
    return binding


def validate_benchmark_receipt(path: Path) -> dict[str, Any]:
    payload, _ = _read_json(path)
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "authority_design",
        "runtime_sources",
        "exact_deployed_files",
        "thresholds",
        "callback_benchmark",
        "decision_benchmark",
        "benchmark_process_max_rss_mib",
        "checks",
        "evidence_boundary",
        BENCHMARK_CANONICAL_FIELD,
    }
    _validate_runtime_source_binding(payload.get("runtime_sources"))
    deployed = _validate_exact_deployed_binding(payload.get("exact_deployed_files"))
    thresholds = _exact_mapping(
        payload.get("thresholds"),
        (
            "min_achieved_to_observed_rate",
            "max_callback_p99_us",
            "exact_decision_count",
            "max_decision_p99_us",
        ),
        "benchmark thresholds",
    )
    callback = _exact_mapping(
        payload.get("callback_benchmark"),
        (
            "observed_live_rate_hz",
            "target_rate_hz",
            "callback_count",
            "duration_s",
            "achieved_rate_hz",
            "achieved_to_observed_rate",
            "latency_p50_us",
            "latency_p99_us",
            "latency_max_us",
            "cpu_percent_total_host_scale",
        ),
        "callback benchmark",
    )
    decision = _exact_mapping(
        payload.get("decision_benchmark"),
        ("decision_count", "latency_p50_us", "latency_p99_us", "latency_max_us"),
        "decision benchmark",
    )
    checks = _mapping(payload.get("checks"), "benchmark checks")
    expected_checks = {
        "exact_four_deployed_files_bound",
        "true_2x_observed_callback_rate",
        "callback_p99_at_most_2ms",
        "exactly_1000_decisions",
        "decision_p99_at_most_10ms",
        "aggregate_only_no_action_rows",
    }
    observed_rate = _finite(callback.get("observed_live_rate_hz"), "observed callback rate")
    achieved_rate = _finite(callback.get("achieved_rate_hz"), "achieved callback rate")
    if (
        set(payload) != expected_fields
        or payload.get("schema_version") != BENCHMARK_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != BENCHMARK_STATUS
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or deployed["artifact_sha256"] != EXACT_ARTIFACT_SHA256
        or thresholds
        != {
            "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": MAX_CALLBACK_P99_US,
            "exact_decision_count": EXACT_DECISION_COUNT,
            "max_decision_p99_us": MAX_DECISION_P99_US,
        }
        or observed_rate <= 0.0
        or achieved_rate < MIN_RATE_MULTIPLIER * observed_rate
        or _finite(callback.get("achieved_to_observed_rate"), "rate ratio")
        != achieved_rate / observed_rate
        or _finite(callback.get("latency_p99_us"), "callback p99") > MAX_CALLBACK_P99_US
        or _strict_int(decision.get("decision_count"), "decision count", minimum=1)
        != EXACT_DECISION_COUNT
        or _finite(decision.get("latency_p99_us"), "decision p99") > MAX_DECISION_P99_US
        or set(checks) != expected_checks
        or any(checks.get(name) is not True for name in expected_checks)
        or _finite(payload.get("benchmark_process_max_rss_mib"), "benchmark RSS") < 0.0
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(BENCHMARK_CANONICAL_FIELD)
        != document_sha256(payload, BENCHMARK_CANONICAL_FIELD)
    ):
        raise BuyE3CurrentHostResourceGateError("benchmark receipt identity drifted")
    return dict(payload)


def _proc_status_rss_mib(proc_root: Path, pid: int) -> float:
    try:
        lines = (proc_root / str(pid) / "status").read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise BuyE3CurrentHostResourceGateError("process status disappeared") from exc
    for line in lines:
        match = re.fullmatch(r"VmRSS:\s+(\d+)\s+kB", line)
        if match:
            return int(match.group(1)) / 1024.0
    raise BuyE3CurrentHostResourceGateError("process status lacks VmRSS")


def _meminfo(proc_root: Path) -> dict[str, int]:
    try:
        lines = (proc_root / "meminfo").read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise BuyE3CurrentHostResourceGateError("host meminfo unavailable") from exc
    values: dict[str, int] = {}
    for line in lines:
        match = re.fullmatch(r"([^:]+):\s+(\d+)\s+kB", line)
        if match:
            values[match.group(1)] = int(match.group(2))
    missing = {"MemTotal", "MemAvailable"} - set(values)
    if missing:
        raise BuyE3CurrentHostResourceGateError(f"meminfo lacks {sorted(missing)}")
    return values


def _vmstat(proc_root: Path) -> dict[str, int]:
    try:
        lines = (proc_root / "vmstat").read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise BuyE3CurrentHostResourceGateError("host vmstat unavailable") from exc
    values: dict[str, int] = {}
    for line in lines:
        fields = line.split()
        if len(fields) == 2 and fields[0] in {"oom_kill", "pswpin", "pswpout"}:
            values[fields[0]] = int(fields[1])
    missing = {"oom_kill", "pswpin", "pswpout"} - set(values)
    if missing:
        raise BuyE3CurrentHostResourceGateError(f"vmstat lacks {sorted(missing)}")
    return values


def linux_resource_sample(
    *, live_pid: int, benchmark_pid: int, proc_root: Path = Path("/proc")
) -> dict[str, Any]:
    memory = _meminfo(proc_root)
    vmstat = _vmstat(proc_root)
    return {
        "mem_available_mib": memory["MemAvailable"] / 1024.0,
        "live_rss_mib": _proc_status_rss_mib(proc_root, live_pid),
        "benchmark_rss_mib": _proc_status_rss_mib(proc_root, benchmark_pid),
        "oom_kill": vmstat["oom_kill"],
        "swap_in": vmstat["pswpin"],
        "swap_out": vmstat["pswpout"],
    }


def host_identity(
    *,
    instance_id: str,
    instance_type: str,
    proc_root: Path = Path("/proc"),
    dmi_root: Path = Path("/sys/devices/virtual/dmi/id"),
) -> dict[str, Any]:
    memory = _meminfo(proc_root)
    try:
        dmi_instance_id = (dmi_root / "board_asset_tag").read_text(encoding="ascii").strip()
        dmi_instance_type = (dmi_root / "product_name").read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise BuyE3CurrentHostResourceGateError(
            "direct Linux DMI host identity unavailable"
        ) from exc
    if (
        not dmi_instance_id
        or not dmi_instance_type
        or dmi_instance_id != str(instance_id)
        or dmi_instance_type != str(instance_type)
    ):
        raise BuyE3CurrentHostResourceGateError("caller host identity disagrees with Linux DMI")
    return {
        "instance_id": dmi_instance_id,
        "instance_type": dmi_instance_type,
        "logical_cpu_count": int(os.cpu_count() or 0),
        "mem_total_mib": memory["MemTotal"] / 1024.0,
        "instance_identity_source": "linux_dmi_board_asset_tag_and_product_name",
        "dmi_board_asset_tag_sha256": hashlib.sha256(
            f"{dmi_instance_id}\n".encode("ascii")
        ).hexdigest(),
        "dmi_product_name_sha256": hashlib.sha256(
            f"{dmi_instance_type}\n".encode("ascii")
        ).hexdigest(),
    }


def _process_identity_from_live(
    *,
    runtime_repository_root: Path,
    pid: int,
    config_path: Path,
    proc_root: Path,
) -> dict[str, Any]:
    root = runtime_repository_root.expanduser().resolve(strict=True)
    command = _proc_cmdline(proc_root, pid)
    cwd = (proc_root / str(pid) / "cwd").resolve(strict=True)
    executable = (proc_root / str(pid) / "exe").resolve(strict=True)
    observed_config = _cmdline_config(command).resolve(strict=True)
    expected_config = config_path.expanduser().resolve(strict=True)
    if cwd != root or observed_config != expected_config:
        raise BuyE3CurrentHostResourceGateError("live process path identity drifted")
    return {
        "pid": pid,
        "pid_start_ticks": _proc_start_ticks(proc_root, pid),
        "cmdline_sha256": canonical_sha256(command),
        "cwd": str(cwd),
        "python_executable": str(executable),
        "config_path": str(expected_config),
        "config_sha256": file_sha256(expected_config),
    }


def _stable_live_identity(payload: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(payload))


def _benchmark_command(
    *,
    python_executable: Path,
    collector_repository_root: Path,
    runtime_repository_root: Path,
    deployed: Mapping[str, Any],
    observed_rate_hz: float,
    benchmark_output_path: Path,
    paced_duration_s: float,
) -> list[str]:
    files = deployed["files"]
    return [
        str(python_executable),
        "-m",
        BENCHMARK_PRODUCER_MODULE,
        "benchmark",
        "--runtime-repository-root",
        str(runtime_repository_root),
        "--artifact-manifest",
        files["manifest"]["absolute_path"],
        "--policy",
        files["policy"]["absolute_path"],
        "--predicate-bundle",
        files["predicate_bundle"]["absolute_path"],
        "--direct-active-release",
        files["direct_active_release"]["absolute_path"],
        "--observed-live-rate-hz",
        repr(float(observed_rate_hz)),
        "--paced-duration-s",
        repr(float(paced_duration_s)),
        "--output",
        str(benchmark_output_path),
        "--collector-repository-root",
        str(collector_repository_root),
    ]


def _counter_window(baseline: Mapping[str, Any], final: Mapping[str, Any]) -> dict[str, Any]:
    before = _exact_mapping(
        baseline.get("counter_values"), WINDOW_ZERO_COUNTERS, "baseline counters"
    )
    after = _exact_mapping(final.get("counter_values"), WINDOW_ZERO_COUNTERS, "final counters")
    normalized_before = {
        name: _strict_int(before[name], f"baseline {name}") for name in WINDOW_ZERO_COUNTERS
    }
    normalized_after = {
        name: _strict_int(after[name], f"final {name}") for name in WINDOW_ZERO_COUNTERS
    }
    deltas = {
        name: normalized_after[name] - normalized_before[name] for name in WINDOW_ZERO_COUNTERS
    }
    return {
        "absolute_baseline": normalized_before,
        "absolute_final": normalized_after,
        "window_delta": deltas,
        "baseline_manifest_sha256": canonical_sha256(normalized_before),
        "final_manifest_sha256": canonical_sha256(normalized_after),
        "window_delta_manifest_sha256": canonical_sha256(deltas),
    }


def _shadow_runtime_projection(health: Mapping[str, Any], label: str) -> dict[str, Any]:
    expected_fields = (
        "externalSources",
        *GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_ZERO_FIELDS,
        *GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
        "globalFlowReason",
        "globalRefReason",
    )
    row = _exact_mapping(
        health.get("shadow_disabled_state"), expected_fields, f"{label} shadow runtime"
    )
    for name in expected_fields[:-2]:
        if _strict_int(row[name], f"{label} {name}") != 0:
            raise BuyE3CurrentHostResourceGateError(
                f"{label} disabled shadow runtime is non-zero: {name}"
            )
    if (
        row["globalFlowReason"] != SHADOW_DISABLED_REASON
        or row["globalRefReason"] != SHADOW_DISABLED_REASON
    ):
        raise BuyE3CurrentHostResourceGateError(f"{label} disabled shadow runtime reason drifted")
    return dict(row)


def build_resource_receipt(
    *,
    host: Mapping[str, Any],
    runtime_execution: Mapping[str, Any],
    collector_execution: Mapping[str, Any],
    config_correction: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    exact_deployed_files: Mapping[str, Any],
    prior_process: Mapping[str, Any],
    disabled_process: Mapping[str, Any],
    pre_live_identity: Mapping[str, Any],
    post_live_identity: Mapping[str, Any],
    baseline_health: Mapping[str, Any],
    final_health: Mapping[str, Any],
    samples: Sequence[Mapping[str, Any]],
    capture: Mapping[str, Any],
    benchmark_receipt: Mapping[str, Any],
    generated_utc: str | None = None,
) -> dict[str, Any]:
    if len(samples) < 2:
        raise BuyE3CurrentHostResourceGateError("concurrent sample window is too short")
    runtime_execution_row = _validate_execution_shape(runtime_execution, runtime=True)
    collector_execution_row = _validate_execution_shape(collector_execution, runtime=False)
    config_correction_row = _exact_mapping(
        config_correction,
        tuple(config_successor.CONTENT_BINDING_FIELDS),
        "config correction binding",
    )
    runtime_source_binding = _validate_runtime_source_binding(runtime_sources)
    deployed_file_binding = _validate_exact_deployed_binding(exact_deployed_files)
    host_row = dict(host)
    mem_total = _finite(host_row.get("mem_total_mib"), "host total memory")
    prior_pid = _strict_int(prior_process.get("pid"), "prior PID", minimum=1)
    disabled_pid = _strict_int(disabled_process.get("pid"), "disabled PID", minimum=1)
    prior_start = _strict_int(prior_process.get("pid_start_ticks"), "prior start ticks", minimum=1)
    disabled_start = _strict_int(
        disabled_process.get("pid_start_ticks"), "disabled start ticks", minimum=1
    )
    if prior_pid == disabled_pid or prior_start == disabled_start:
        raise BuyE3CurrentHostResourceGateError("disabled process is not a fresh restart")
    if disabled_process.get("buy_e3_enabled") is not False:
        raise BuyE3CurrentHostResourceGateError("resource process is not disabled")
    stable_disabled = disabled_process.get("stable_process_identity_sha256")
    for current in (pre_live_identity, post_live_identity):
        if (
            current.get("pid") != disabled_pid
            or current.get("pid_start_ticks") != disabled_start
            or _stable_live_identity(current) != stable_disabled
        ):
            raise BuyE3CurrentHostResourceGateError("live process changed during resource window")
    if (
        baseline_health.get("buy_e3_enabled") != 0
        or final_health.get("buy_e3_enabled") != 0
        or baseline_health.get("boolean_cooldown_enabled") != 1
        or final_health.get("boolean_cooldown_enabled") != 1
    ):
        raise BuyE3CurrentHostResourceGateError("policy enablement changed during resource window")
    counters = _counter_window(baseline_health, final_health)
    baseline_shadow = _shadow_runtime_projection(baseline_health, "baseline")
    final_shadow = _shadow_runtime_projection(final_health, "final")
    sample_rows = [dict(sample) for sample in samples]
    mem_available = [_finite(row.get("mem_available_mib"), "MemAvailable") for row in sample_rows]
    live_rss = [_finite(row.get("live_rss_mib"), "live RSS") for row in sample_rows]
    benchmark_rss = [_finite(row.get("benchmark_rss_mib"), "benchmark RSS") for row in sample_rows]
    combined = [left + right for left, right in zip(live_rss, benchmark_rss, strict=True)]
    oom = [_strict_int(row.get("oom_kill"), "OOM counter") for row in sample_rows]
    swap_in = [_strict_int(row.get("swap_in"), "swap-in counter") for row in sample_rows]
    swap_out = [_strict_int(row.get("swap_out"), "swap-out counter") for row in sample_rows]
    deep_buffer = [
        _strict_int(row.get("deep_book_buffer"), "deep-book buffer") for row in sample_rows
    ]
    callback = _mapping(benchmark_receipt.get("callback_benchmark"), "callback benchmark")
    decision = _mapping(benchmark_receipt.get("decision_benchmark"), "decision benchmark")
    checks = {
        "current_instance_id_exact": host_row.get("instance_id") == CURRENT_INSTANCE_ID,
        "current_instance_type_exact": host_row.get("instance_type") == CURRENT_INSTANCE_TYPE,
        "exact_2_vcpu_host": host_row.get("logical_cpu_count") == CURRENT_LOGICAL_CPU_COUNT,
        "host_memory_class_4gib": MIN_HOST_MEM_TOTAL_MIB <= mem_total <= MAX_HOST_MEM_TOTAL_MIB,
        "runtime_checkout_exact_direct_successor": (
            runtime_execution_row.get("execution_commit") == DIRECT_SUCCESSOR_EXECUTION_COMMIT
            and runtime_execution_row.get("execution_tree") == DIRECT_SUCCESSOR_EXECUTION_TREE
            and runtime_execution_row.get("annotated_tag") == DIRECT_SUCCESSOR_ANNOTATED_TAG
            and runtime_execution_row.get("annotated_tag_object") == DIRECT_SUCCESSOR_TAG_OBJECT
        ),
        "collector_is_clean_annotated_with_exact_v4_runtime_sources": (
            collector_execution_row.get("direct_successor_commit_is_ancestor") is False
            and collector_execution_row.get("runtime_authority_checkout") is False
            and runtime_source_binding.get("direct_successor_execution_commit")
            == DIRECT_SUCCESSOR_EXECUTION_COMMIT
        ),
        "exact_four_deployed_files_bound": len(
            _mapping(deployed_file_binding.get("files"), "deployed files")
        )
        == 4,
        "immutable_direct_successor_authority_has_no_resource_dependency": True,
        "fresh_disabled_process_proven": prior_pid != disabled_pid
        and prior_start != disabled_start,
        "buy_e3_disabled_throughout": True,
        "external_venues_disabled_throughout": True,
        "global_flow_shadow_disabled_throughout": all(
            row["globalFlowShadowEnabled"] == 0
            and row["globalFlowStateError"] == 0
            and row["globalFlowReason"] == SHADOW_DISABLED_REASON
            for row in (baseline_shadow, final_shadow)
        ),
        "global_reference_shadow_disabled_throughout": all(
            row["globalRefShadowEnabled"] == 0
            and row["globalRefStateError"] == 0
            and row["globalRefReason"] == SHADOW_DISABLED_REASON
            for row in (baseline_shadow, final_shadow)
        ),
        "global_flow_absolute_zero_throughout": all(
            row[name] == 0
            for row in (baseline_shadow, final_shadow)
            for name in (*GLOBAL_FLOW_STATE_ZERO_FIELDS, *GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS)
        )
        and all(
            counters[role][name] == 0
            for role in ("absolute_baseline", "absolute_final", "window_delta")
            for name in GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS
        ),
        "global_reference_absolute_zero_throughout": all(
            row[name] == 0
            for row in (baseline_shadow, final_shadow)
            for name in GLOBAL_REFERENCE_ZERO_FIELDS
        ),
        "external_sources_absolute_zero_throughout": all(
            row["externalSources"] == 0 for row in (baseline_shadow, final_shadow)
        ),
        "external_error_counters_absolute_zero_throughout": all(
            counters[role][name] == 0
            for role in ("absolute_baseline", "absolute_final", "window_delta")
            for name in ("externalErrors", "externalRecordDropped")
        ),
        "sell_owner_enabled_throughout": True,
        "same_live_pid_and_start_ticks_throughout": True,
        "benchmark_process_overlap_proven": any(value > 0.0 for value in benchmark_rss),
        "post_benchmark_fresh_health_observed": (
            _strict_int(final_health.get("main_generation"), "final HEALTH generation", minimum=1)
            > _strict_int(
                baseline_health.get("main_generation"), "baseline HEALTH generation", minimum=1
            )
            and _strict_int(
                final_health.get("lifecycle_generation"),
                "final lifecycle HEALTH generation",
                minimum=1,
            )
            > _strict_int(
                baseline_health.get("lifecycle_generation"),
                "baseline lifecycle HEALTH generation",
                minimum=1,
            )
        ),
        "min_mem_available_at_least_512mib": min(mem_available) >= MIN_MEM_AVAILABLE_MIB,
        "live_rss_at_most_512mib": max(live_rss) <= MAX_LIVE_RSS_MIB,
        "benchmark_rss_at_most_256mib": max(benchmark_rss) <= MAX_BENCHMARK_RSS_MIB,
        "combined_rss_at_most_768mib": max(combined) <= MAX_COMBINED_RSS_MIB,
        "oom_window_delta_zero": max(oom) == min(oom),
        "swap_window_delta_zero": max(swap_in) == min(swap_in) and max(swap_out) == min(swap_out),
        "all_drop_invalid_overflow_window_deltas_zero": all(
            value == 0 for value in counters["window_delta"].values()
        ),
        "absolute_counter_baseline_preserved": len(counters["absolute_baseline"])
        == len(WINDOW_ZERO_COUNTERS),
        "deep_book_buffer_zero_throughout": all(value == 0 for value in deep_buffer),
        "true_2x_observed_callback_rate": _finite(
            callback.get("achieved_to_observed_rate"), "callback rate ratio"
        )
        >= MIN_RATE_MULTIPLIER,
        "callback_p99_at_most_2ms": _finite(callback.get("latency_p99_us"), "callback p99")
        <= MAX_CALLBACK_P99_US,
        "exactly_1000_decisions": decision.get("decision_count") == EXACT_DECISION_COUNT,
        "decision_p99_at_most_10ms": _finite(decision.get("latency_p99_us"), "decision p99")
        <= MAX_DECISION_P99_US,
        "aggregate_only_no_live_stream_or_action_rows": True,
    }
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise BuyE3CurrentHostResourceGateError(
            "current-host resource gate failed: " + ", ".join(failed)
        )
    observed = {
        "sample_count": len(sample_rows),
        "min_mem_available_mib": min(mem_available),
        "max_live_rss_mib": max(live_rss),
        "max_benchmark_rss_mib": max(benchmark_rss),
        "max_combined_rss_mib": max(combined),
        "oom_absolute_baseline": oom[0],
        "oom_absolute_final": oom[-1],
        "oom_window_delta": oom[-1] - oom[0],
        "swap_in_absolute_baseline": swap_in[0],
        "swap_in_absolute_final": swap_in[-1],
        "swap_in_window_delta": swap_in[-1] - swap_in[0],
        "swap_out_absolute_baseline": swap_out[0],
        "swap_out_absolute_final": swap_out[-1],
        "swap_out_window_delta": swap_out[-1] - swap_out[0],
        "max_deep_book_buffer": max(deep_buffer),
        "observed_live_callback_rate_hz": callback["observed_live_rate_hz"],
        "benchmark_achieved_rate_hz": callback["achieved_rate_hz"],
        "achieved_to_observed_rate": callback["achieved_to_observed_rate"],
        "callback_p99_us": callback["latency_p99_us"],
        "decision_count": decision["decision_count"],
        "decision_p99_us": decision["latency_p99_us"],
    }
    payload: dict[str, Any] = {
        "schema_version": RESOURCE_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": RESOURCE_STATUS,
        "generated_utc": generated_utc or _utc_now(),
        "authority_design": dict(AUTHORITY_DESIGN),
        "host": host_row,
        "runtime_execution": runtime_execution_row,
        "collector_execution": collector_execution_row,
        "config_correction": config_correction_row,
        "runtime_sources": runtime_source_binding,
        "exact_deployed_files": deployed_file_binding,
        "fresh_disabled_process": {
            "prior_process_identity_sha256": prior_process["stable_process_identity_sha256"],
            "prior_pid": prior_pid,
            "prior_pid_start_ticks": prior_start,
            "disabled_process_identity_sha256": stable_disabled,
            "disabled_pid": disabled_pid,
            "disabled_pid_start_ticks": disabled_start,
            "disabled_config_path": disabled_process["config_path"],
            "disabled_config_sha256": disabled_process["config_sha256"],
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
        "capture": dict(capture),
        "thresholds": {
            "min_host_mem_total_mib": MIN_HOST_MEM_TOTAL_MIB,
            "max_host_mem_total_mib": MAX_HOST_MEM_TOTAL_MIB,
            "min_mem_available_mib": MIN_MEM_AVAILABLE_MIB,
            "max_live_rss_mib": MAX_LIVE_RSS_MIB,
            "max_benchmark_rss_mib": MAX_BENCHMARK_RSS_MIB,
            "max_combined_rss_mib": MAX_COMBINED_RSS_MIB,
            "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": MAX_CALLBACK_P99_US,
            "exact_decision_count": EXACT_DECISION_COUNT,
            "max_decision_p99_us": MAX_DECISION_P99_US,
        },
        "observed": observed,
        "counter_window": counters,
        "shadow_runtime": {
            "baseline": baseline_shadow,
            "final": final_shadow,
            "baseline_manifest_sha256": canonical_sha256(baseline_shadow),
            "final_manifest_sha256": canonical_sha256(final_shadow),
            "all_numeric_fields_absolute_zero": True,
            "disabled_reason_exact": True,
        },
        "checks": checks,
        "benchmark_binding": {
            "schema_version": BENCHMARK_SCHEMA,
            "status": BENCHMARK_STATUS,
            "canonical_benchmark_receipt_sha256": benchmark_receipt[BENCHMARK_CANONICAL_FIELD],
            "exact_deployed_binding_manifest_sha256": exact_deployed_files[
                "binding_manifest_sha256"
            ],
        },
        "sample_series_sha256": canonical_sha256(sample_rows),
        "sample_rows_persisted": False,
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[RESOURCE_CANONICAL_FIELD] = document_sha256(payload, RESOURCE_CANONICAL_FIELD)
    return payload


def _validate_execution_shape(raw: Any, *, runtime: bool) -> dict[str, Any]:
    row = _exact_mapping(
        raw,
        (
            "repository_root",
            "execution_commit",
            "execution_tree",
            "annotated_tag",
            "annotated_tag_object",
            "tag_peeled_commit",
            "direct_successor_commit_is_ancestor",
            "runtime_authority_checkout",
        ),
        "execution identity",
    )
    commit = _require_git_sha(row["execution_commit"], "execution commit")
    tree = _require_git_sha(row["execution_tree"], "execution tree")
    tag_object = _require_git_sha(row["annotated_tag_object"], "tag object")
    peeled = _require_git_sha(row["tag_peeled_commit"], "peeled commit")
    if (
        not PurePosixPath(str(row["repository_root"])).is_absolute()
        or not str(row["annotated_tag"]).strip()
        or peeled != commit
        or not isinstance(row["direct_successor_commit_is_ancestor"], bool)
        or row["runtime_authority_checkout"] is not runtime
    ):
        raise BuyE3CurrentHostResourceGateError("execution identity drifted")
    if runtime and (
        row["direct_successor_commit_is_ancestor"] is not True
        or commit != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or tree != DIRECT_SUCCESSOR_EXECUTION_TREE
        or row["annotated_tag"] != DIRECT_SUCCESSOR_ANNOTATED_TAG
        or tag_object != DIRECT_SUCCESSOR_TAG_OBJECT
    ):
        raise BuyE3CurrentHostResourceGateError("runtime execution is not direct-successor")
    if not runtime and row["direct_successor_commit_is_ancestor"] is not False:
        raise BuyE3CurrentHostResourceGateError(
            "collector execution must record its independent non-ancestor lineage"
        )
    return row


def validate_resource_receipt(
    path: Path,
    *,
    config_correction_path: Path,
    expected_collector_execution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate an immutable aggregate receipt for later evidence completion."""

    payload, _ = _read_json(path)
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "authority_design",
        "host",
        "runtime_execution",
        "collector_execution",
        "config_correction",
        "runtime_sources",
        "exact_deployed_files",
        "fresh_disabled_process",
        "capture",
        "thresholds",
        "observed",
        "counter_window",
        "shadow_runtime",
        "checks",
        "benchmark_binding",
        "sample_series_sha256",
        "sample_rows_persisted",
        "evidence_boundary",
        RESOURCE_CANONICAL_FIELD,
    }
    host = _exact_mapping(
        payload.get("host"),
        (
            "instance_id",
            "instance_type",
            "logical_cpu_count",
            "mem_total_mib",
            "instance_identity_source",
            "dmi_board_asset_tag_sha256",
            "dmi_product_name_sha256",
        ),
        "host identity",
    )
    runtime_execution = _validate_execution_shape(payload.get("runtime_execution"), runtime=True)
    collector_execution = _validate_execution_shape(
        payload.get("collector_execution"), runtime=False
    )
    if expected_collector_execution is not None and collector_execution != dict(
        expected_collector_execution
    ):
        raise BuyE3CurrentHostResourceGateError("collector execution cross-binding drifted")
    config_correction = _validate_config_correction(
        config_correction_path,
        collector_execution=collector_execution,
    )
    runtime_sources = _validate_runtime_source_binding(payload.get("runtime_sources"))
    deployed = _validate_exact_deployed_binding(payload.get("exact_deployed_files"))
    process = _exact_mapping(
        payload.get("fresh_disabled_process"),
        (
            "prior_process_identity_sha256",
            "prior_pid",
            "prior_pid_start_ticks",
            "disabled_process_identity_sha256",
            "disabled_pid",
            "disabled_pid_start_ticks",
            "disabled_config_path",
            "disabled_config_sha256",
            "fresh_pid",
            "fresh_start_ticks",
            "same_pid_pre_post",
        ),
        "fresh disabled process",
    )
    counters = _exact_mapping(
        payload.get("counter_window"),
        (
            "absolute_baseline",
            "absolute_final",
            "window_delta",
            "baseline_manifest_sha256",
            "final_manifest_sha256",
            "window_delta_manifest_sha256",
        ),
        "counter window",
    )
    baseline = _exact_mapping(
        counters["absolute_baseline"], WINDOW_ZERO_COUNTERS, "absolute counter baseline"
    )
    final = _exact_mapping(
        counters["absolute_final"], WINDOW_ZERO_COUNTERS, "absolute counter final"
    )
    deltas = _exact_mapping(counters["window_delta"], WINDOW_ZERO_COUNTERS, "counter deltas")
    for name in WINDOW_ZERO_COUNTERS:
        before = _strict_int(baseline[name], f"baseline {name}")
        after = _strict_int(final[name], f"final {name}")
        if deltas[name] != after - before or deltas[name] != 0:
            raise BuyE3CurrentHostResourceGateError(f"counter window advanced: {name}")
        if name in (
            *GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
            "externalErrors",
            "externalRecordDropped",
        ) and (before != 0 or after != 0):
            raise BuyE3CurrentHostResourceGateError(
                f"disabled shadow/input counter is not absolute zero: {name}"
            )
    shadow_runtime = _exact_mapping(
        payload.get("shadow_runtime"),
        (
            "baseline",
            "final",
            "baseline_manifest_sha256",
            "final_manifest_sha256",
            "all_numeric_fields_absolute_zero",
            "disabled_reason_exact",
        ),
        "shadow runtime",
    )
    shadow_baseline = _shadow_runtime_projection(
        {"shadow_disabled_state": shadow_runtime["baseline"]}, "receipt baseline"
    )
    shadow_final = _shadow_runtime_projection(
        {"shadow_disabled_state": shadow_runtime["final"]}, "receipt final"
    )
    if (
        shadow_runtime["baseline_manifest_sha256"] != canonical_sha256(shadow_baseline)
        or shadow_runtime["final_manifest_sha256"] != canonical_sha256(shadow_final)
        or shadow_runtime["all_numeric_fields_absolute_zero"] is not True
        or shadow_runtime["disabled_reason_exact"] is not True
        or any(
            shadow_baseline[name] != baseline[name] or shadow_final[name] != final[name]
            for name in GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS
        )
    ):
        raise BuyE3CurrentHostResourceGateError("shadow runtime receipt drifted")
    capture = _exact_mapping(payload.get("capture"), RESOURCE_CAPTURE_FIELDS, "resource capture")
    observed = _exact_mapping(
        payload.get("observed"), RESOURCE_OBSERVED_FIELDS, "resource observations"
    )
    checks = _exact_mapping(payload.get("checks"), RESOURCE_CHECK_NAMES, "resource checks")
    benchmark = _exact_mapping(
        payload.get("benchmark_binding"),
        (
            "schema_version",
            "status",
            "canonical_benchmark_receipt_sha256",
            "exact_deployed_binding_manifest_sha256",
        ),
        "benchmark binding",
    )
    thresholds = _mapping(payload.get("thresholds"), "resource thresholds")
    expected_thresholds = {
        "min_host_mem_total_mib": MIN_HOST_MEM_TOTAL_MIB,
        "max_host_mem_total_mib": MAX_HOST_MEM_TOTAL_MIB,
        "min_mem_available_mib": MIN_MEM_AVAILABLE_MIB,
        "max_live_rss_mib": MAX_LIVE_RSS_MIB,
        "max_benchmark_rss_mib": MAX_BENCHMARK_RSS_MIB,
        "max_combined_rss_mib": MAX_COMBINED_RSS_MIB,
        "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
        "max_callback_p99_us": MAX_CALLBACK_P99_US,
        "exact_decision_count": EXACT_DECISION_COUNT,
        "max_decision_p99_us": MAX_DECISION_P99_US,
    }
    oom_baseline = _strict_int(observed.get("oom_absolute_baseline"), "OOM baseline")
    oom_final = _strict_int(observed.get("oom_absolute_final"), "OOM final")
    swap_in_baseline = _strict_int(observed.get("swap_in_absolute_baseline"), "swap-in baseline")
    swap_in_final = _strict_int(observed.get("swap_in_absolute_final"), "swap-in final")
    swap_out_baseline = _strict_int(observed.get("swap_out_absolute_baseline"), "swap-out baseline")
    swap_out_final = _strict_int(observed.get("swap_out_absolute_final"), "swap-out final")
    if (
        set(payload) != expected_fields
        or payload.get("schema_version") != RESOURCE_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != RESOURCE_STATUS
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("config_correction") != config_correction
        or host.get("instance_id") != CURRENT_INSTANCE_ID
        or host.get("instance_type") != CURRENT_INSTANCE_TYPE
        or host.get("logical_cpu_count") != CURRENT_LOGICAL_CPU_COUNT
        or host.get("instance_identity_source") != "linux_dmi_board_asset_tag_and_product_name"
        or host.get("dmi_board_asset_tag_sha256")
        != hashlib.sha256(f"{CURRENT_INSTANCE_ID}\n".encode("ascii")).hexdigest()
        or host.get("dmi_product_name_sha256")
        != hashlib.sha256(f"{CURRENT_INSTANCE_TYPE}\n".encode("ascii")).hexdigest()
        or not MIN_HOST_MEM_TOTAL_MIB
        <= _finite(host.get("mem_total_mib"), "host total memory")
        <= MAX_HOST_MEM_TOTAL_MIB
        or runtime_execution["execution_commit"] != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or runtime_sources["direct_successor_execution_commit"] != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or deployed["direct_release_canonical_sha256"] != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or _require_sha256(process.get("prior_process_identity_sha256"), "prior process")
        != process.get("prior_process_identity_sha256")
        or _require_sha256(process.get("disabled_process_identity_sha256"), "disabled process")
        != process.get("disabled_process_identity_sha256")
        or process.get("prior_pid") == process.get("disabled_pid")
        or process.get("prior_pid_start_ticks") == process.get("disabled_pid_start_ticks")
        or process.get("disabled_config_sha256") != EXPECTED_DISABLED_CONFIG_SHA256
        or not PurePosixPath(str(process.get("disabled_config_path", ""))).is_absolute()
        or process.get("fresh_pid") is not True
        or process.get("fresh_start_ticks") is not True
        or process.get("same_pid_pre_post") is not True
        or _strict_int(capture.get("collector_pid"), "collector PID", minimum=1) <= 0
        or _strict_int(capture.get("benchmark_pid"), "benchmark PID", minimum=1) <= 0
        or _strict_int(capture.get("benchmark_pid_start_ticks"), "benchmark start ticks", minimum=1)
        <= 0
        or capture.get("live_pid") != process.get("disabled_pid")
        or capture.get("live_pid_start_ticks") != process.get("disabled_pid_start_ticks")
        or _require_sha256(capture.get("benchmark_command_sha256"), "benchmark command")
        != capture.get("benchmark_command_sha256")
        or _strict_int(capture.get("benchmark_launch_monotonic_ns"), "benchmark launch", minimum=1)
        >= _strict_int(capture.get("benchmark_exit_monotonic_ns"), "benchmark exit", minimum=1)
        or _strict_int(
            capture.get("rate_boundary_main_health_generation"),
            "rate boundary generation",
            minimum=1,
        )
        >= _strict_int(
            capture.get("rate_first_main_health_generation"),
            "first rate generation",
            minimum=1,
        )
        or _strict_int(
            capture.get("rate_boundary_lifecycle_health_generation"),
            "rate lifecycle boundary generation",
            minimum=1,
        )
        >= _strict_int(
            capture.get("rate_first_lifecycle_health_generation"),
            "first rate lifecycle generation",
            minimum=1,
        )
        or _strict_int(
            capture.get("rate_second_main_health_generation"),
            "second rate generation",
            minimum=1,
        )
        != _strict_int(
            capture.get("rate_first_main_health_generation"),
            "first rate generation",
            minimum=1,
        )
        + 1
        or _strict_int(
            capture.get("rate_second_lifecycle_health_generation"),
            "second rate lifecycle generation",
            minimum=1,
        )
        != _strict_int(
            capture.get("rate_first_lifecycle_health_generation"),
            "first rate lifecycle generation",
            minimum=1,
        )
        + 1
        or capture.get("rate_second_main_health_generation")
        != capture.get("baseline_main_health_generation")
        or capture.get("rate_second_lifecycle_health_generation")
        != capture.get("baseline_lifecycle_health_generation")
        or capture.get("rate_second_main_health_line_sha256")
        != capture.get("baseline_main_health_line_sha256")
        or capture.get("rate_second_lifecycle_health_line_sha256")
        != capture.get("baseline_lifecycle_health_line_sha256")
        or _strict_int(
            capture.get("rate_window_update_delta"), "rate-window update delta", minimum=1
        )
        <= 0
        or _finite(capture.get("rate_window_elapsed_s"), "rate-window elapsed seconds") <= 0.0
        or not math.isclose(
            _strict_int(
                capture.get("rate_window_update_delta"),
                "rate-window update delta",
                minimum=1,
            )
            / _finite(capture.get("rate_window_elapsed_s"), "rate-window elapsed seconds"),
            _finite(
                observed.get("observed_live_callback_rate_hz"),
                "observed live callback rate",
            ),
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or capture.get("rate_window_same_live_pid_and_start_ticks") is not True
        or _strict_int(
            capture.get("baseline_main_health_generation"), "baseline generation", minimum=1
        )
        >= _strict_int(capture.get("final_main_health_generation"), "final generation", minimum=1)
        or _strict_int(
            capture.get("baseline_lifecycle_health_generation"),
            "baseline lifecycle generation",
            minimum=1,
        )
        >= _strict_int(
            capture.get("final_lifecycle_health_generation"),
            "final lifecycle generation",
            minimum=1,
        )
        or any(
            _require_sha256(capture.get(name), name) != capture.get(name)
            for name in (
                "baseline_main_health_line_sha256",
                "final_main_health_line_sha256",
                "rate_boundary_main_health_line_sha256",
                "rate_first_main_health_line_sha256",
                "rate_second_main_health_line_sha256",
                "rate_boundary_lifecycle_health_line_sha256",
                "rate_first_lifecycle_health_line_sha256",
                "rate_second_lifecycle_health_line_sha256",
                "baseline_lifecycle_health_line_sha256",
                "final_lifecycle_health_line_sha256",
                "benchmark_stdout_sha256",
                "benchmark_stderr_sha256",
                "sample_series_sha256",
            )
        )
        or capture.get("benchmark_returncode") != 0
        or capture.get("sample_count") != observed.get("sample_count")
        or capture.get("health_source") != "existing_aggregate_log_only"
        or capture.get("market_stream_connection_created") is not False
        or thresholds != expected_thresholds
        or _strict_int(observed.get("sample_count"), "sample count", minimum=2) < 2
        or _finite(observed.get("min_mem_available_mib"), "min MemAvailable")
        < MIN_MEM_AVAILABLE_MIB
        or _finite(observed.get("max_live_rss_mib"), "max live RSS") > MAX_LIVE_RSS_MIB
        or _finite(observed.get("max_benchmark_rss_mib"), "max benchmark RSS")
        > MAX_BENCHMARK_RSS_MIB
        or _finite(observed.get("max_combined_rss_mib"), "max combined RSS") > MAX_COMBINED_RSS_MIB
        or observed.get("oom_window_delta") != 0
        or observed.get("swap_in_window_delta") != 0
        or observed.get("swap_out_window_delta") != 0
        or oom_final - oom_baseline != observed.get("oom_window_delta")
        or swap_in_final - swap_in_baseline != observed.get("swap_in_window_delta")
        or swap_out_final - swap_out_baseline != observed.get("swap_out_window_delta")
        or observed.get("max_deep_book_buffer") != 0
        or _finite(observed.get("achieved_to_observed_rate"), "rate ratio") < MIN_RATE_MULTIPLIER
        or _finite(observed.get("callback_p99_us"), "callback p99") > MAX_CALLBACK_P99_US
        or observed.get("decision_count") != EXACT_DECISION_COUNT
        or _finite(observed.get("decision_p99_us"), "decision p99") > MAX_DECISION_P99_US
        or any(checks[name] is not True for name in RESOURCE_CHECK_NAMES)
        or benchmark.get("schema_version") != BENCHMARK_SCHEMA
        or benchmark.get("status") != BENCHMARK_STATUS
        or _require_sha256(benchmark.get("canonical_benchmark_receipt_sha256"), "benchmark receipt")
        != benchmark.get("canonical_benchmark_receipt_sha256")
        or benchmark.get("exact_deployed_binding_manifest_sha256")
        != deployed["binding_manifest_sha256"]
        or counters["baseline_manifest_sha256"] != canonical_sha256(baseline)
        or counters["final_manifest_sha256"] != canonical_sha256(final)
        or counters["window_delta_manifest_sha256"] != canonical_sha256(deltas)
        or _require_sha256(payload.get("sample_series_sha256"), "sample series")
        != payload.get("sample_series_sha256")
        or payload.get("sample_series_sha256") != capture.get("sample_series_sha256")
        or payload.get("sample_rows_persisted") is not False
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(RESOURCE_CANONICAL_FIELD)
        != document_sha256(payload, RESOURCE_CANONICAL_FIELD)
    ):
        raise BuyE3CurrentHostResourceGateError("resource receipt identity drifted")
    return dict(payload)


def capture_concurrent_resource_gate(
    *,
    collector_repository_root: Path,
    collector_annotated_tag: str,
    runtime_repository_root: Path,
    pid_file: Path,
    disabled_config_path: Path,
    config_correction_path: Path,
    prior_process_receipt_path: Path,
    live_log_path: Path,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    direct_active_release_path: Path,
    instance_id: str,
    instance_type: str,
    benchmark_output_path: Path,
    output_path: Path,
    python_executable: Path | None = None,
    paced_duration_s: float = 15.0,
    sample_interval_s: float = 0.1,
    rate_window_timeout_s: float = 150.0,
    post_health_timeout_s: float = 150.0,
    proc_root: Path = Path("/proc"),
    dmi_root: Path = Path("/sys/devices/virtual/dmi/id"),
    _popen: Callable[..., Any] = subprocess.Popen,
    _sleep: Callable[[float], None] = time.sleep,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
    _resource_sampler: Callable[..., Mapping[str, Any]] | None = None,
    _health_tail_factory: Callable[[Path], Any] = LiveHealthTail,
) -> dict[str, Any]:
    interval = _finite(sample_interval_s, "sample interval")
    if interval <= 0.0:
        raise BuyE3CurrentHostResourceGateError("sample interval must be positive")
    collector_root = collector_repository_root.expanduser().resolve(strict=True)
    runtime_root = runtime_repository_root.expanduser().resolve(strict=True)
    collector_execution = capture_git_execution(
        collector_root,
        annotated_tag=collector_annotated_tag,
        runtime_authority=False,
    )
    config_correction = _validate_config_correction(
        config_correction_path,
        collector_execution=collector_execution,
    )
    runtime_execution = capture_git_execution(
        runtime_root,
        annotated_tag=DIRECT_SUCCESSOR_ANNOTATED_TAG,
        runtime_authority=True,
    )
    runtime_sources = bind_current_successor_runtime_sources(
        runtime_repository_root=runtime_root,
        collector_repository_root=collector_root,
    )
    prior = validate_process_snapshot(prior_process_receipt_path)
    disabled = capture_process_snapshot(
        runtime_repository_root=runtime_root,
        runtime_annotated_tag=DIRECT_SUCCESSOR_ANNOTATED_TAG,
        pid_file=pid_file,
        config_path=disabled_config_path,
        expected_buy_e3_enabled=False,
        proc_root=proc_root,
    )
    if prior["pid"] == disabled["pid"] or prior["pid_start_ticks"] == disabled["pid_start_ticks"]:
        raise BuyE3CurrentHostResourceGateError("disabled process is not fresh")
    live_pid = disabled["pid"]
    pre_live_identity = _process_identity_from_live(
        runtime_repository_root=runtime_root,
        pid=live_pid,
        config_path=disabled_config_path,
        proc_root=proc_root,
    )
    if _stable_live_identity(pre_live_identity) != disabled["stable_process_identity_sha256"]:
        raise BuyE3CurrentHostResourceGateError("disabled process changed before capture")
    deployed = bind_exact_deployed_files(
        runtime_repository_root=runtime_root,
        manifest_path=manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        direct_active_release_path=direct_active_release_path,
    )
    host = host_identity(
        instance_id=instance_id,
        instance_type=instance_type,
        proc_root=proc_root,
        dmi_root=dmi_root,
    )
    if (
        host["instance_id"] != CURRENT_INSTANCE_ID
        or host["instance_type"] != CURRENT_INSTANCE_TYPE
        or host["logical_cpu_count"] != CURRENT_LOGICAL_CPU_COUNT
        or not MIN_HOST_MEM_TOTAL_MIB <= host["mem_total_mib"] <= MAX_HOST_MEM_TOTAL_MIB
    ):
        raise BuyE3CurrentHostResourceGateError(
            "capture host identity is not current c7i-flex.large"
        )
    health_tail = _health_tail_factory(live_log_path)
    rate_boundary, first_rate, second_rate, observed_rate = _capture_current_process_rate_window(
        health_tail=health_tail,
        disabled_process=disabled,
        runtime_repository_root=runtime_root,
        disabled_config_path=disabled_config_path,
        proc_root=proc_root,
        sample_interval_s=interval,
        timeout_s=rate_window_timeout_s,
        sleep=_sleep,
        monotonic_ns=_monotonic_ns,
    )
    update_delta = second_rate["boolean_cooldown_updates"] - first_rate["boolean_cooldown_updates"]
    rate_elapsed_s = second_rate["main_wall_timestamp_s"] - first_rate["main_wall_timestamp_s"]
    baseline_health = second_rate
    benchmark_output = benchmark_output_path.expanduser().absolute()
    resource_output = output_path.expanduser().absolute()
    for target in (benchmark_output, resource_output):
        if target.exists() or target.is_symlink():
            raise BuyE3CurrentHostResourceGateError(
                f"immutable output already exists: {target.name}"
            )
    executable = (
        python_executable.expanduser().absolute()
        if python_executable is not None
        else Path(sys.executable).absolute()
    )
    if not executable.is_file():
        raise BuyE3CurrentHostResourceGateError("collector Python is unavailable")
    command = _benchmark_command(
        python_executable=executable,
        collector_repository_root=collector_root,
        runtime_repository_root=runtime_root,
        deployed=deployed,
        observed_rate_hz=observed_rate,
        benchmark_output_path=benchmark_output,
        paced_duration_s=paced_duration_s,
    )
    launch_ns = _monotonic_ns()
    process: Any | None = None
    samples: list[dict[str, Any]] = []
    sampler = _resource_sampler or (
        lambda live_pid, benchmark_pid: linux_resource_sample(
            live_pid=live_pid,
            benchmark_pid=benchmark_pid,
            proc_root=proc_root,
        )
    )
    try:
        process = _popen(
            command,
            cwd=collector_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        benchmark_pid = _strict_int(process.pid, "benchmark PID", minimum=1)
        if benchmark_pid == live_pid:
            raise BuyE3CurrentHostResourceGateError("benchmark reused live PID")
        benchmark_start_ticks = _proc_start_ticks(proc_root, benchmark_pid)
        while process.poll() is None:
            live_identity = _process_identity_from_live(
                runtime_repository_root=runtime_root,
                pid=live_pid,
                config_path=disabled_config_path,
                proc_root=proc_root,
            )
            if _stable_live_identity(live_identity) != disabled["stable_process_identity_sha256"]:
                raise BuyE3CurrentHostResourceGateError("live process changed during benchmark")
            if _proc_start_ticks(proc_root, benchmark_pid) != benchmark_start_ticks:
                raise BuyE3CurrentHostResourceGateError("benchmark PID was reused")
            health = dict(health_tail.snapshot())
            metrics = dict(sampler(live_pid, benchmark_pid))
            samples.append(
                {
                    "monotonic_ns": _monotonic_ns(),
                    **metrics,
                    "deep_book_buffer": health["deep_book_buffer"],
                }
            )
            _sleep(interval)
        benchmark_exit_ns = _monotonic_ns()
        stdout, stderr = process.communicate(timeout=15.0)
        if process.returncode != 0:
            raise BuyE3CurrentHostResourceGateError("four-file benchmark subprocess failed")
        if len(samples) < 2:
            raise BuyE3CurrentHostResourceGateError("benchmark overlap has fewer than two samples")
        exit_health = dict(health_tail.snapshot())
        exit_main_generation = exit_health["main_generation"]
        exit_lifecycle_generation = exit_health["lifecycle_generation"]
        post_deadline = _monotonic_ns() + int(_finite(post_health_timeout_s, "post timeout") * 1e9)
        final_health: dict[str, Any] | None = None
        while _monotonic_ns() <= post_deadline:
            before_health_identity = _process_identity_from_live(
                runtime_repository_root=runtime_root,
                pid=live_pid,
                config_path=disabled_config_path,
                proc_root=proc_root,
            )
            if (
                _stable_live_identity(before_health_identity)
                != disabled["stable_process_identity_sha256"]
            ):
                raise BuyE3CurrentHostResourceGateError(
                    "live process changed before post-benchmark HEALTH"
                )
            candidate = dict(health_tail.snapshot())
            after_health_identity = _process_identity_from_live(
                runtime_repository_root=runtime_root,
                pid=live_pid,
                config_path=disabled_config_path,
                proc_root=proc_root,
            )
            if (
                _stable_live_identity(after_health_identity)
                != disabled["stable_process_identity_sha256"]
            ):
                raise BuyE3CurrentHostResourceGateError(
                    "live process changed after post-benchmark HEALTH"
                )
            if (
                candidate["main_generation"] > exit_main_generation
                and candidate["lifecycle_generation"] > exit_lifecycle_generation
            ):
                final_health = candidate
                break
            _sleep(min(interval, 0.25))
        if final_health is None:
            raise BuyE3CurrentHostResourceGateError("no fresh post-benchmark HEALTH state")
        post_live_identity = _process_identity_from_live(
            runtime_repository_root=runtime_root,
            pid=live_pid,
            config_path=disabled_config_path,
            proc_root=proc_root,
        )
        benchmark = validate_benchmark_receipt(benchmark_output)
        if benchmark["exact_deployed_files"] != deployed:
            raise BuyE3CurrentHostResourceGateError("benchmark four-file binding changed")
        if benchmark["runtime_sources"] != runtime_sources:
            raise BuyE3CurrentHostResourceGateError("benchmark runtime source binding changed")
        if not math.isclose(
            _finite(
                benchmark["callback_benchmark"].get("observed_live_rate_hz"),
                "benchmark observed live rate",
            ),
            observed_rate,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise BuyE3CurrentHostResourceGateError(
                "benchmark did not use the captured live callback rate"
            )
        capture = {
            "collector_pid": os.getpid(),
            "benchmark_pid": benchmark_pid,
            "benchmark_pid_start_ticks": benchmark_start_ticks,
            "live_pid": live_pid,
            "live_pid_start_ticks": disabled["pid_start_ticks"],
            "benchmark_command_sha256": canonical_sha256(command),
            "benchmark_launch_monotonic_ns": launch_ns,
            "benchmark_exit_monotonic_ns": benchmark_exit_ns,
            "rate_boundary_main_health_generation": rate_boundary["main_generation"],
            "rate_boundary_main_health_line_sha256": rate_boundary["main_line_sha256"],
            "rate_boundary_lifecycle_health_generation": rate_boundary["lifecycle_generation"],
            "rate_boundary_lifecycle_health_line_sha256": rate_boundary["lifecycle_line_sha256"],
            "rate_first_main_health_generation": first_rate["main_generation"],
            "rate_first_main_health_line_sha256": first_rate["main_line_sha256"],
            "rate_first_lifecycle_health_generation": first_rate["lifecycle_generation"],
            "rate_first_lifecycle_health_line_sha256": first_rate["lifecycle_line_sha256"],
            "rate_second_main_health_generation": second_rate["main_generation"],
            "rate_second_main_health_line_sha256": second_rate["main_line_sha256"],
            "rate_second_lifecycle_health_generation": second_rate["lifecycle_generation"],
            "rate_second_lifecycle_health_line_sha256": second_rate["lifecycle_line_sha256"],
            "rate_window_update_delta": update_delta,
            "rate_window_elapsed_s": rate_elapsed_s,
            "rate_window_same_live_pid_and_start_ticks": True,
            "baseline_main_health_generation": baseline_health["main_generation"],
            "final_main_health_generation": final_health["main_generation"],
            "baseline_lifecycle_health_generation": baseline_health["lifecycle_generation"],
            "final_lifecycle_health_generation": final_health["lifecycle_generation"],
            "baseline_main_health_line_sha256": baseline_health["main_line_sha256"],
            "final_main_health_line_sha256": final_health["main_line_sha256"],
            "baseline_lifecycle_health_line_sha256": baseline_health["lifecycle_line_sha256"],
            "final_lifecycle_health_line_sha256": final_health["lifecycle_line_sha256"],
            "benchmark_returncode": process.returncode,
            "benchmark_stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
            "benchmark_stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
            "sample_series_sha256": canonical_sha256(samples),
            "sample_count": len(samples),
            "health_source": "existing_aggregate_log_only",
            "market_stream_connection_created": False,
        }
        payload = build_resource_receipt(
            host=host,
            runtime_execution=runtime_execution,
            collector_execution=collector_execution,
            config_correction=config_correction,
            runtime_sources=runtime_sources,
            exact_deployed_files=deployed,
            prior_process=prior,
            disabled_process=disabled,
            pre_live_identity=pre_live_identity,
            post_live_identity=post_live_identity,
            baseline_health=baseline_health,
            final_health=final_health,
            samples=samples,
            capture=capture,
            benchmark_receipt=benchmark,
        )
        atomic_write_receipt(resource_output, payload)
        validate_resource_receipt(
            resource_output,
            config_correction_path=config_correction_path,
            expected_collector_execution=collector_execution,
        )
        return payload
    except Exception:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10.0)
        raise


def _add_four_files(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--runtime-repository-root", type=Path, required=True)
    parser.add_argument("--artifact-manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--predicate-bundle", type=Path, required=True)
    parser.add_argument("--direct-active-release", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    process = subparsers.add_parser("snapshot-process")
    process.add_argument("--runtime-repository-root", type=Path, required=True)
    process.add_argument("--runtime-annotated-tag", default=DIRECT_SUCCESSOR_ANNOTATED_TAG)
    process.add_argument("--pid-file", type=Path, required=True)
    process.add_argument("--config", type=Path, required=True)
    process.add_argument("--expected-buy-e3-enabled", type=int, choices=(0, 1), required=True)
    process.add_argument("--output", type=Path, required=True)

    benchmark = subparsers.add_parser("benchmark")
    _add_four_files(benchmark)
    benchmark.add_argument("--collector-repository-root", type=Path, required=True)
    benchmark.add_argument("--observed-live-rate-hz", type=float, required=True)
    benchmark.add_argument("--paced-duration-s", type=float, default=15.0)
    benchmark.add_argument("--output", type=Path, required=True)

    capture = subparsers.add_parser("capture-concurrent")
    _add_four_files(capture)
    capture.add_argument("--collector-repository-root", type=Path, required=True)
    capture.add_argument("--collector-annotated-tag", required=True)
    capture.add_argument("--pid-file", type=Path, required=True)
    capture.add_argument("--disabled-config", type=Path, required=True)
    capture.add_argument("--config-correction", type=Path, required=True)
    capture.add_argument("--prior-process-receipt", type=Path, required=True)
    capture.add_argument("--live-log", type=Path, required=True)
    capture.add_argument("--instance-id", required=True)
    capture.add_argument("--instance-type", required=True)
    capture.add_argument("--benchmark-output", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    capture.add_argument("--python", type=Path)
    capture.add_argument("--paced-duration-s", type=float, default=15.0)
    capture.add_argument("--sample-interval-s", type=float, default=0.1)
    capture.add_argument("--rate-window-timeout-s", type=float, default=150.0)
    capture.add_argument("--post-health-timeout-s", type=float, default=150.0)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--receipt", type=Path, required=True)
    validate.add_argument("--config-correction", type=Path)
    validate.add_argument(
        "--kind", choices=("process", "benchmark", "resource"), default="resource"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "snapshot-process":
        payload, file_hash = write_process_snapshot(
            output_path=args.output,
            runtime_repository_root=args.runtime_repository_root,
            runtime_annotated_tag=args.runtime_annotated_tag,
            pid_file=args.pid_file,
            config_path=args.config,
            expected_buy_e3_enabled=bool(args.expected_buy_e3_enabled),
        )
        print(json.dumps({"status": payload["status"], "file_sha256": file_hash}, sort_keys=True))
        return 0
    if args.command == "benchmark":
        payload = run_exact_four_file_benchmark(
            collector_repository_root=args.collector_repository_root,
            runtime_repository_root=args.runtime_repository_root,
            manifest_path=args.artifact_manifest,
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
            direct_active_release_path=args.direct_active_release,
            observed_live_rate_hz=args.observed_live_rate_hz,
            output_path=args.output,
            paced_duration_s=args.paced_duration_s,
        )
        print(json.dumps({"status": payload["status"]}, sort_keys=True))
        return 0
    if args.command == "capture-concurrent":
        payload = capture_concurrent_resource_gate(
            collector_repository_root=args.collector_repository_root,
            collector_annotated_tag=args.collector_annotated_tag,
            runtime_repository_root=args.runtime_repository_root,
            pid_file=args.pid_file,
            disabled_config_path=args.disabled_config,
            config_correction_path=args.config_correction,
            prior_process_receipt_path=args.prior_process_receipt,
            live_log_path=args.live_log,
            manifest_path=args.artifact_manifest,
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
            direct_active_release_path=args.direct_active_release,
            instance_id=args.instance_id,
            instance_type=args.instance_type,
            benchmark_output_path=args.benchmark_output,
            output_path=args.output,
            python_executable=args.python,
            paced_duration_s=args.paced_duration_s,
            sample_interval_s=args.sample_interval_s,
            rate_window_timeout_s=args.rate_window_timeout_s,
            post_health_timeout_s=args.post_health_timeout_s,
        )
        print(json.dumps({"status": payload["status"]}, sort_keys=True))
        return 0
    if args.kind == "process":
        payload = validate_process_snapshot(args.receipt)
    elif args.kind == "benchmark":
        payload = validate_benchmark_receipt(args.receipt)
    else:
        if args.config_correction is None:
            raise BuyE3CurrentHostResourceGateError(
                "resource validation requires --config-correction"
            )
        payload = validate_resource_receipt(
            args.receipt,
            config_correction_path=args.config_correction,
        )
    canonical_field = {
        "process": PROCESS_CANONICAL_FIELD,
        "benchmark": BENCHMARK_CANONICAL_FIELD,
        "resource": RESOURCE_CANONICAL_FIELD,
    }[args.kind]
    print(payload[canonical_field])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUTHORITY_DESIGN",
    "BENCHMARK_CANONICAL_FIELD",
    "BENCHMARK_SCHEMA",
    "BENCHMARK_STATUS",
    "BuyE3CurrentHostResourceGateError",
    "PROCESS_CANONICAL_FIELD",
    "PROCESS_SCHEMA",
    "PROCESS_STATUS",
    "RESOURCE_CANONICAL_FIELD",
    "RESOURCE_CHECK_NAMES",
    "RESOURCE_SCHEMA",
    "RESOURCE_STATUS",
    "WINDOW_ZERO_COUNTERS",
    "bind_exact_deployed_files",
    "build_resource_receipt",
    "capture_concurrent_resource_gate",
    "capture_git_execution",
    "capture_process_snapshot",
    "run_exact_four_file_benchmark",
    "validate_benchmark_receipt",
    "validate_process_snapshot",
    "validate_resource_receipt",
    "write_process_snapshot",
]
