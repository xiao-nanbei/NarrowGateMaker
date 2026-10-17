"""Strict-native schema binding over the shared event-stream lockstep implementation."""
from functools import partial

from execution.order_lifecycle_journal_v2_strict_native import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    validate_order_lifecycle_journal_v2_payload,
)
from research.families.f07_active_order_continuation.audit import order_lifecycle_v2_cpp_event_stream_binding as core

CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION = core.CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION
CPP_EVENT_STREAM_LOCKSTEP_REPORT_VERSION = core.CPP_EVENT_STREAM_LOCKSTEP_REPORT_VERSION
CPP_EVENT_STREAM_MIRROR_ABI_VERSION = "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2"
CONTRACT = core.EventStreamContract(
    columns=ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    schema_version=ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    abi_version=CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    projection_columns=core.DEFAULT_CONTRACT.projection_columns + (
        "simulator_queue_source", "exact_queue_path_valid"),
    validate=validate_order_lifecycle_journal_v2_payload,
)
journal_schema_sha256 = partial(core.journal_schema_sha256, contract=CONTRACT)
projection_schema_sha256 = partial(core.projection_schema_sha256, contract=CONTRACT)
audit_cpp_event_stream_lockstep = partial(core.audit_cpp_event_stream_lockstep, contract=CONTRACT)
build_cpp_event_stream_binding_artifact = partial(core.build_cpp_event_stream_binding_artifact, contract=CONTRACT)
