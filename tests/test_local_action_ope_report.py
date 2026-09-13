from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.local_action_ope_report import (
    _resolve_action_family,
    _validate_queue_ope_evidence,
    parse_args,
)
from models.replay_policies import LOCAL_ACTIONS


def _queue_panel(
    family: str = "queue_value_keep_cancel",
) -> pd.DataFrame:
    candidate = (
        "cancel_until_state_exit"
        if family == "queue_value_keep_cancel"
        else "cancel_then_baseline_reenter"
    )
    return pd.DataFrame(
        {
            "action": ["keep", candidate],
            "behavior_propensity": [0.5, 0.5],
            "behavior_prob_keep": [0.5, 0.5],
            f"behavior_prob_{candidate}": [0.5, 0.5],
            "native_exchange_outcome_supported": [1, 1],
            "native_exchange_seed_supported": [1, 1],
            "exchange_book_queue_path_valid": [1, 1],
            "exchange_book_queue_ambiguous": [0, 0],
            "native_exchange_support_reason": ["supported", "supported"],
            "queue_runtime_event_source": [
                "native_exchange_exact_level",
                "native_exchange_exact_level",
            ],
        }
    )


def _metadata(family: str, rows: int = 2) -> dict[str, object]:
    return {
        "action_family": family,
        "queue_runtime_event_source_expected": "native_exchange_exact_level",
        "queue_runtime_event_sources_observed": [
            "native_exchange_exact_level"
        ],
        "ope_block_reason": "",
        "native_source_integrity": {
            "passed": True,
            "exchange_book_source_gap_events": 0,
            "exchange_book_invalid_sequence_messages": 0,
            "exchange_book_sequence_gaps": 0,
            "exchange_book_message_time_reversals": 0,
        },
        "native_action_support": {
            "rows": rows,
            "outcome_supported_rows": rows,
            "seed_gate": True,
            "path_gate": True,
            "ambiguous_rows": 0,
            "invalid_path_rows": 0,
        },
    }


@pytest.mark.parametrize(
    "family", ["queue_value_keep_cancel", "queue_value_cancel_reenter"]
)
def test_queue_ope_accepts_only_fully_supported_runner_evidence(family: str) -> None:
    panel = _queue_panel(family)
    resolved = _resolve_action_family(panel)

    _validate_queue_ope_evidence(
        panel,
        family_name=resolved["name"],
        metadata=_metadata(family),
        panel_label="panel",
    )


def test_queue_ope_requires_companion_metadata() -> None:
    panel = _queue_panel()

    with pytest.raises(ValueError, match="requires companion metadata JSON"):
        _validate_queue_ope_evidence(
            panel,
            family_name="queue_value_keep_cancel",
            metadata=None,
            panel_label="panel",
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("block", "blocked by main runner"),
        ("source", "source integrity did not pass"),
        ("source_count", "source integrity has failures"),
        ("seed_gate", "seed_gate did not pass"),
        ("path_gate", "path_gate did not pass"),
        ("supported_rows", "unsupported/post-treatment censored"),
        ("ambiguous_rows", "unsupported/post-treatment censoring"),
        ("runtime_source", "runtime event source does not match metadata"),
    ],
)
def test_queue_ope_rejects_blocked_or_censored_metadata(
    mutation: str, message: str
) -> None:
    panel = _queue_panel()
    metadata = _metadata("queue_value_keep_cancel")
    if mutation == "block":
        metadata["ope_block_reason"] = "action_dependent_native_path_censoring"
    elif mutation == "source":
        metadata["native_source_integrity"]["passed"] = False
    elif mutation == "source_count":
        metadata["native_source_integrity"]["exchange_book_sequence_gaps"] = 1
    elif mutation == "seed_gate":
        metadata["native_action_support"]["seed_gate"] = False
    elif mutation == "path_gate":
        metadata["native_action_support"]["path_gate"] = False
    elif mutation == "supported_rows":
        metadata["native_action_support"]["outcome_supported_rows"] = 1
    elif mutation == "ambiguous_rows":
        metadata["native_action_support"]["ambiguous_rows"] = 1
    elif mutation == "runtime_source":
        panel["queue_runtime_event_source"] = "policy_visible_top_book"

    with pytest.raises(ValueError, match=message):
        _validate_queue_ope_evidence(
            panel,
            family_name="queue_value_keep_cancel",
            metadata=metadata,
            panel_label="panel",
        )


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        (
            "native_exchange_outcome_supported",
            0,
            "unsupported/post-treatment censored",
        ),
        ("native_exchange_seed_supported", 0, "must be 1"),
        ("exchange_book_queue_path_valid", 0, "must be 1"),
        ("exchange_book_queue_ambiguous", 1, "must be 0"),
        ("native_exchange_support_reason", "native_path_invalid", "support reasons"),
    ],
)
def test_queue_ope_rejects_unsupported_panel_rows(
    column: str, value: object, message: str
) -> None:
    panel = _queue_panel()
    panel.loc[1, column] = value

    with pytest.raises(ValueError, match=message):
        _validate_queue_ope_evidence(
            panel,
            family_name="queue_value_keep_cancel",
            metadata=_metadata("queue_value_keep_cancel"),
            panel_label="panel",
        )


def test_non_queue_family_keeps_metadata_optional() -> None:
    panel = pd.DataFrame(
        {
            "action": list(LOCAL_ACTIONS),
            **{
                f"behavior_prob_{action}": [0.25] * len(LOCAL_ACTIONS)
                for action in LOCAL_ACTIONS
            },
        }
    )

    _validate_queue_ope_evidence(
        panel,
        family_name="local_quote",
        metadata=None,
        panel_label="panel",
    )


def test_cli_accepts_single_and_fixed_metadata_paths(tmp_path) -> None:
    single = parse_args(
        [
            "--panel-csv",
            str(tmp_path / "panel.csv"),
            "--panel-metadata-json",
            str(tmp_path / "panel.metadata.json"),
            "--output-prefix",
            str(tmp_path / "single"),
        ]
    )
    assert single.panel_metadata_json == tmp_path / "panel.metadata.json"

    fixed = parse_args(
        [
            "--training-panel-csv",
            str(tmp_path / "train.csv"),
            "--training-panel-metadata-json",
            str(tmp_path / "train.metadata.json"),
            "--holdout-panel-csv",
            str(tmp_path / "holdout.csv"),
            "--holdout-panel-metadata-json",
            str(tmp_path / "holdout.metadata.json"),
            "--output-prefix",
            str(tmp_path / "fixed"),
        ]
    )
    assert fixed.training_panel_metadata_json == tmp_path / "train.metadata.json"
    assert fixed.holdout_panel_metadata_json == tmp_path / "holdout.metadata.json"
