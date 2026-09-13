"""Source-authority contract for normalized historical order books."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from data import normalized_l2_registry as l2_registry


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def book_source_contract(
    day: str,
    *,
    bbo_dir: Path,
    l2_dir: Path,
) -> dict[str, Any]:
    """Read one target day's immutable normalized-book authority."""
    resolved_bbo = bbo_dir.expanduser().resolve()
    resolved_l2 = l2_dir.expanduser().resolve()
    if resolved_bbo.parent != resolved_l2.parent:
        raise SystemExit(
            "historical BBO/L2 must share one versioned dataset root: "
            f"bbo={resolved_bbo} l2={resolved_l2}"
        )
    dataset_root = resolved_bbo.parent
    manifest_path = dataset_root / "manifest.json"
    quality_path = dataset_root / "daily_quality.csv"
    manifest: dict[str, Any] = {}
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(
                f"invalid normalized book manifest {manifest_path}: {exc}"
            ) from exc

    row: dict[str, str] = {}
    if quality_path.is_file():
        with quality_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or "day" not in reader.fieldnames:
                raise SystemExit(
                    f"normalized book quality lacks day: {quality_path}"
                )
            matches = [
                dict(candidate)
                for candidate in reader
                if str(candidate.get("day", "")) == day
            ]
        if len(matches) != 1:
            raise SystemExit(
                f"{day}: normalized book quality row count must be one under "
                f"{dataset_root}, observed {len(matches)}"
            )
        row = matches[0]

    dataset_version = str(
        manifest.get("dataset_version")
        or manifest.get("dataset_id")
        or dataset_root.name
    )
    authority = str(row.get("source_authority") or "").strip()
    if not authority:
        authority = (
            "native_formal_lifecycle"
            if dataset_version == l2_registry.DATASET_VERSION
            else "unclassified"
        )
    return {
        "dataset_root": str(dataset_root),
        "dataset_version": dataset_version,
        "source_authority": authority,
        "formal_lifecycle_replay_eligible": _bool_value(
            row.get("formal_lifecycle_replay_eligible")
            if "formal_lifecycle_replay_eligible" in row
            else row.get("formal_eligible")
        ),
        "provider_sensitivity_replay_eligible": _bool_value(
            row.get("provider_sensitivity_replay_eligible")
            if "provider_sensitivity_replay_eligible" in row
            else row.get("provider_normalized_replay_candidate")
        ),
        "exact_queue_policy_eligible": _bool_value(
            row.get("exact_queue_policy_eligible")
        ),
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256(manifest_path)
        if manifest_path.is_file()
        else "",
        "quality_path": str(quality_path),
        "quality_sha256": _sha256(quality_path)
        if quality_path.is_file()
        else "",
    }


def enforce_book_source_contract(
    day: str,
    params: dict[str, Any],
    *,
    bbo_dir: Path,
    l2_dir: Path,
) -> dict[str, Any]:
    """Fail closed when queue semantics exceed the selected source authority."""
    contract = book_source_contract(day, bbo_dir=bbo_dir, l2_dir=l2_dir)
    queue_mode = str(
        params.get("queue_ahead_mode", "exact_level") or "exact_level"
    ).strip().lower()
    if queue_mode in {"provider", "provider_visible_level"}:
        queue_mode = "provider_visible_level"
    elif queue_mode in {"exact", "exact_level"}:
        queue_mode = "exact_level"
    else:
        raise SystemExit(
            "queue_ahead_mode must be exact_level or provider_visible_level"
        )

    authority = str(contract["source_authority"])
    is_provider = authority == "provider_normalized_causal"
    if is_provider and queue_mode != "provider_visible_level":
        raise SystemExit(
            f"{day}: provider-normalized book requires "
            "queue_ahead_mode=provider_visible_level; exact_level would "
            "misstate provider authority"
        )
    if queue_mode == "provider_visible_level" and not is_provider:
        raise SystemExit(
            f"{day}: provider_visible_level requires source_authority="
            f"provider_normalized_causal, observed {authority}"
        )
    if is_provider and bool(params.get("queue_l2_cancel_ahead_enabled", False)):
        raise SystemExit(
            f"{day}: provider-normalized state snapshots cannot authorize "
            "queue_l2_cancel_ahead_enabled"
        )

    params["queue_ahead_mode"] = queue_mode
    params["_book_source_authority"] = authority
    params["_book_dataset_version"] = str(contract["dataset_version"])
    params["_book_manifest_sha256"] = str(contract["manifest_sha256"])
    params["_book_exact_queue_policy_eligible"] = bool(
        contract["exact_queue_policy_eligible"]
    )
    if is_provider:
        params["replay_evidence_scope"] = "provider_normalized_sensitivity"
        params["replay_promotion_eligible"] = False
    return contract
