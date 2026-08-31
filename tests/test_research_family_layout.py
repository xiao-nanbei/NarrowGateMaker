from __future__ import annotations

import hashlib
import importlib
import json
import tarfile
from pathlib import Path

import pytest

from research.governance.paths import (
    PRIVATE_LEGACY_ARCHIVE_ROOT,
    PRIVATE_LEGACY_ARCHIVES,
    archived_bytes,
    private_legacy_archive_identity,
    resolve_private_legacy_archive,
    resolve_research_path,
    verify_path_identity,
)

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "research" / "registry.json"
LAYOUT_V1 = ROOT / "research" / "governance" / "migrations" / "layout_v1.json"
LAYOUT_V2 = ROOT / "research" / "governance" / "migrations" / "layout_v2.json"


def _payload(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_every_registered_family_is_an_importable_owned_package() -> None:
    payload = _payload(REGISTRY)
    assert payload["schema"] == "narrowgate.research_family_registry.v2"

    families = payload["families"]
    assert isinstance(families, list)
    assert len(families) == 11

    ids: set[str] = set()
    directories: set[str] = set()
    packages: set[str] = set()
    for family in families:
        assert isinstance(family, dict)
        family_id = str(family["id"])
        directory = str(family["directory"])
        package = str(family["package"])
        assert family_id not in ids
        assert directory not in directories
        assert package not in packages
        ids.add(family_id)
        directories.add(directory)
        packages.add(package)

        family_root = ROOT / directory
        assert family_root.is_dir()
        assert (family_root / "__init__.py").is_file()
        assert (family_root / "README.md").is_file()
        assert (family_root / "docs").is_dir()
        assert any(path.suffix in {".py", ".cpp", ".hpp"} for path in family_root.rglob("*"))
        assert importlib.import_module(package) is not None


def test_hard_layout_v2_has_no_root_research_packages_or_symlinks() -> None:
    payload = _payload(LAYOUT_V2)
    assert payload["schema"] == "narrowgate.research_layout_migration.v2"
    mappings = payload["mappings"]
    assert isinstance(mappings, list)
    assert len(mappings) == payload["mapping_count"] == 250

    for row in mappings:
        legacy = ROOT / str(row["legacy_path"])
        canonical = ROOT / str(row["canonical_path"])
        assert not legacy.exists()
        assert not legacy.is_symlink()
        if row.get("canonical_availability") == "private_not_distributed":
            assert not canonical.exists()
            with pytest.raises(FileNotFoundError, match="private_not_distributed"):
                resolve_research_path(str(row["legacy_path"]))
            with pytest.raises(FileNotFoundError, match="private_not_distributed"):
                resolve_research_path(str(row["canonical_path"]))
            continue
        archive_identity = private_legacy_archive_identity(str(row["canonical_path"]))
        if archive_identity is not None:
            assert not canonical.exists()
            if not (
                PRIVATE_LEGACY_ARCHIVE_ROOT / archive_identity.filename
            ).is_file():
                with pytest.raises(FileNotFoundError, match="private_not_distributed"):
                    resolve_research_path(str(row["legacy_path"]))
                continue
            assert resolve_research_path(str(row["legacy_path"])) == (
                resolve_private_legacy_archive(str(row["canonical_path"]))
            )
        else:
            assert canonical.is_file()
            assert not canonical.is_symlink()
            assert resolve_research_path(str(row["legacy_path"])) == canonical.resolve()

    assert not list(ROOT.glob("research_*"))
    assert not any(path.is_symlink() for path in (ROOT / "research").rglob("*"))


def test_layout_v1_paths_chain_through_layout_v2() -> None:
    payload = _payload(LAYOUT_V1)
    layout_v2 = _payload(LAYOUT_V2)
    assert payload["schema"] == "narrowgate.research_path_migration.v1"
    mappings = payload["mappings"]
    assert isinstance(mappings, list)
    assert len(mappings) == payload["mapping_count"] == 198
    private_v2_legacy_paths = {
        str(row["legacy_path"])
        for row in layout_v2["mappings"]
        if row.get("canonical_availability") == "private_not_distributed"
    }

    for row in mappings:
        legacy = ROOT / str(row["legacy_path"])
        assert not legacy.exists()
        assert not legacy.is_symlink()
        if str(row["canonical_path"]) in private_v2_legacy_paths:
            with pytest.raises(FileNotFoundError, match="private_not_distributed"):
                resolve_research_path(str(row["legacy_path"]))
            continue
        resolved = resolve_research_path(str(row["legacy_path"]))
        assert resolved.is_file()
        assert resolved == resolve_research_path(str(row["canonical_path"]))


def test_frozen_absolute_repo_path_resolves_in_renamed_checkout() -> None:
    frozen = Path(
        "/opt/frozen-checkout/NarrowGate_BTCUSDC/"
        "research/families/f10_live_replay_attribution/README.md"
    )

    assert resolve_research_path(frozen) == (
        ROOT
        / "research"
        / "families"
        / "f10_live_replay_attribution"
        / "README.md"
    ).resolve()


@pytest.mark.parametrize("manifest_path", [LAYOUT_V1, LAYOUT_V2])
def test_layout_snapshot_preserves_every_boundary_payload(manifest_path: Path) -> None:
    payload = _payload(manifest_path)
    snapshot = payload["legacy_snapshot"]
    assert isinstance(snapshot, dict)
    try:
        archive = resolve_research_path(str(snapshot["path"]))
    except FileNotFoundError as exc:
        assert "availability=private_not_distributed" in str(exc)
        mappings = payload["mappings"]
        assert isinstance(mappings, list)
        with pytest.raises(FileNotFoundError, match="private_not_distributed"):
            archived_bytes(
                str(mappings[0]["legacy_path"]),
                str(mappings[0]["sha256"]),
            )
        return
    assert archive.stat().st_size == int(snapshot["size_bytes"])
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == str(snapshot["sha256"])

    with tarfile.open(archive, "r:gz") as handle:
        members = {member.name for member in handle.getmembers() if member.isfile()}
    mappings = payload["mappings"]
    assert isinstance(mappings, list)
    expected_members = {str(row["archive_member"]) for row in mappings}
    assert members == expected_members

    for row in mappings:
        frozen = archived_bytes(str(row["legacy_path"]), str(row["sha256"]))
        assert len(frozen) == int(row["size_bytes"])


def test_exact_layout_archives_are_private_and_publicly_described() -> None:
    public_archive_root = ROOT / "research" / "governance" / "archive"
    assert (public_archive_root / "README.md").is_file()
    for logical_path, identity in PRIVATE_LEGACY_ARCHIVES.items():
        assert not (ROOT / logical_path).exists()
        if not (PRIVATE_LEGACY_ARCHIVE_ROOT / identity.filename).is_file():
            with pytest.raises(FileNotFoundError, match="private_not_distributed"):
                resolve_private_legacy_archive(logical_path)
            continue
        archive = resolve_private_legacy_archive(logical_path)
        assert archive.name == identity.filename
        assert archive.stat().st_size == identity.size_bytes
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == identity.sha256


def test_build_and_deploy_use_canonical_research_sources() -> None:
    cmake = (ROOT / "cpp" / "CMakeLists.txt").read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

    for stem in (
        "request_state_features",
        "risk_set_expansion",
        "sparse_order_lifecycle",
    ):
        canonical = f"../research/families/f06_placement_fill_cif/cpp/{stem}.cpp"
        assert canonical in cmake
        assert f"../research_06_placement_fill_cif/cpp/{stem}.cpp" not in cmake

    assert "scripts/live_deploy_common.py source-release" in makefile
    assert "publish-source:" in makefile
    assert "publish-source-dry:" in makefile
    assert "$(wildcard research/families/" not in makefile
    assert "$(EC2_DIR)/research/families/" not in makefile


def test_frozen_hash_cannot_masquerade_as_changed_canonical_source() -> None:
    layout_v1 = _payload(LAYOUT_V1)
    row_v1 = next(
        item
        for item in layout_v1["mappings"]
        if item["legacy_path"] == "models/audit/buy_conditional_widen_cate.py"
    )
    with pytest.raises((RuntimeError, FileNotFoundError)) as error_v1:
        verify_path_identity(str(row_v1["legacy_path"]), str(row_v1["sha256"]))
    assert (
        "frozen pre-migration code identity" in str(error_v1.value)
        or "availability=private_not_distributed" in str(error_v1.value)
    )

    layout_v2 = _payload(LAYOUT_V2)
    row_v2 = next(
        item
        for item in layout_v2["mappings"]
        if item["legacy_path"]
        == "research_06_placement_fill_cif/audit/ordered_common_support_fill_surface.py"
    )
    with pytest.raises((RuntimeError, FileNotFoundError)) as error_v2:
        verify_path_identity(str(row_v2["legacy_path"]), str(row_v2["sha256"]))
    assert (
        "frozen pre-migration code identity" in str(error_v2.value)
        or "availability=private_not_distributed" in str(error_v2.value)
    )
