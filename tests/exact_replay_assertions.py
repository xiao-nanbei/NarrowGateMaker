"""Exact replay comparisons: unknown diagnostics remain unknown, never zero."""
import math


def assert_exact_replay_value(actual, expected):
    if isinstance(expected, dict):
        assert isinstance(actual, dict)
        assert actual.keys() == expected.keys()
        for key in expected:
            assert_exact_replay_value(actual[key], expected[key])
    elif isinstance(expected, (list, tuple)):
        assert type(actual) is type(expected)
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected, strict=True):
            assert_exact_replay_value(left, right)
    elif isinstance(expected, float) and math.isnan(expected):
        assert isinstance(actual, float) and math.isnan(actual)
    else:
        assert actual == expected
