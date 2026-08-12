import math
from pathlib import Path

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_same_date_validity_diagnostic as diagnostic,
)


def test_validity_diagnostic_spec_is_frozen_and_evidence_only():
    spec = diagnostic._load_spec(diagnostic.DEFAULT_SPEC_PATH)

    assert spec["economic_outputs_prohibited"] is True
    assert spec["known_before_freeze"]["same_date_rate_result_already_read"] is True
    assert (
        spec["known_before_freeze"][
            "native_activation_and_invalidation_counters_not_yet_read"
        ]
        is True
    )
    assert not any(spec["permissions"].values())


def test_validity_diagnostic_bound_implementations_match():
    spec = diagnostic._load_spec(diagnostic.DEFAULT_SPEC_PATH)

    for key in (
        "same_date_mechanics_spec",
        "same_date_mechanics_implementation",
        "diagnostic_implementation",
    ):
        identity = spec[key]
        path = Path(identity["path"])
        assert path.is_file()
        assert diagnostic.same_date.sha256_file(path) == identity["sha256"]


def test_safe_ratio_handles_supported_and_empty_denominators():
    assert diagnostic._safe_ratio(2, 4) == 0.5
    assert math.isnan(diagnostic._safe_ratio(1, 0))
