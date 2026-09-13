"""Create a continuous consumer from adjacent packages, never concatenate cold frames."""

from hashlib import sha256
from pathlib import Path

from data.runtime import ConsumerBundle, derive_inputs


def derive_consumer_sequence(roots, output):
    """Re-run the shared scheduler once over explicitly ordered, bound facts.

    Package intervals must abut in caller order. Shared warmup fact bundles are
    included once, with their original ordering checked. Windows, in-flight
    observations and book state therefore do not reset at a package boundary.
    A new create-only output is intentional: independently cold-started feature
    files cannot be made continuous just by concatenating their rows.
    """
    bundles = [ConsumerBundle(root) for root in roots]
    if not bundles:
        raise ValueError("nonempty ordered consumer sequence required")
    first = bundles[0].manifest
    profile = first["plan"]["observation_profile"]
    market = first["plan"]["market_id"]
    sources, positions, parents = [], {}, []
    end = first["plan"]["start_ns"]
    for bundle in bundles:
        meta, plan = bundle.manifest, bundle.manifest["plan"]
        if plan["start_ns"] != end or plan["end_ns"] <= end:
            raise ValueError("consumer intervals must be ordered, adjacent and nonoverlapping")
        if plan["market_id"] != market or plan["observation_profile"] != profile:
            raise ValueError("sequence market/delivery profile mismatch")
        if plan.get("include_outcome_bars", False) != first["plan"].get("include_outcome_bars", False):
            raise ValueError("sequence outcome-bar contract mismatch")
        for key in ("input_contract_id", "observation_contract_id", "feature_contract_id"):
            if meta[key] != first[key]:
                raise ValueError("sequence consumer contract mismatch")
        paths = bundle.source_paths()
        prior = -1
        for path, source in zip(paths, meta["source_bundles"], strict=True):
            key = str(path)
            if key not in positions:
                positions[key] = len(sources)
                sources.append({"path": key, "sha256": source["sha256"]})
            index = positions[key]
            if index <= prior or sources[index]["sha256"] != source["sha256"]:
                raise ValueError("inconsistent sequence source order or identity")
            prior = index
        end = plan["end_ns"]
        parents.append(sha256((bundle.root / "manifest.json").read_bytes()).hexdigest())
    plan = {"source_bundles": sources, "market_id": market,
            "observation_profile": profile, "start_ns": first["plan"]["start_ns"],
            "end_ns": end, "consumer_parent_ids": parents,
            "include_outcome_bars": first["plan"].get("include_outcome_bars", False),
            "sequence_contract": "continuous_source_replay.v1"}
    return derive_inputs(plan, Path(output))
