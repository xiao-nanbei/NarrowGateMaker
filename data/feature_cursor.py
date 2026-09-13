"""As-of access to accepted feature bundles for research consumers."""

from bisect import bisect_right
from hashlib import sha256
from pathlib import Path

from data.observation import model_row
from data.runtime import ConsumerBundle


class FeatureCursor:
    """A bounded consumer bundle; missing context never selects a future row."""

    def __init__(self, root: str | Path):
        self.bundle = ConsumerBundle(root)
        self.input_manifest_id = sha256((self.bundle.root / "manifest.json").read_bytes()).hexdigest()
        self.bundle.source_paths()
        self.frames = tuple(self.bundle.frames())
        self.clocks = tuple(frame.cutoff_ns for frame in self.frames)
        if any(a >= b for a, b in zip(self.clocks, self.clocks[1:], strict=False)):
            raise ValueError("feature cutoffs must strictly increase")

    def require_binding(self, records):
        for record in records:
            if record.get("input_manifest_id") != self.input_manifest_id:
                raise ValueError("experiment record belongs to another input manifest")

    def at(self, decision_ns: int, *, max_age_ns: int):
        if type(decision_ns) is not int or type(max_age_ns) is not int or max_age_ns < 0:
            raise ValueError("integer decision clock and nonnegative feature age required")
        index = bisect_right(self.clocks, decision_ns) - 1
        if index < 0 or decision_ns - self.clocks[index] > max_age_ns:
            raise ValueError("missing or stale causal feature context")
        return self.frames[index]

    def row(self, decision_ns, *, columns, missing_policy, max_age_ns):
        columns = list(columns)
        if missing_policy not in {"reject", "native_nan"}:
            raise ValueError("explicit feature missing policy required")
        metadata = {key: self.bundle.manifest[key] for key in (
            "input_contract_id", "observation_contract_id", "feature_contract_id")}
        metadata.update(feature_cols=list(columns), missing_policy=missing_policy)
        frame = self.at(decision_ns, max_age_ns=max_age_ns)
        return dict(zip(columns, model_row(frame, metadata, decision_ns=decision_ns), strict=True))
