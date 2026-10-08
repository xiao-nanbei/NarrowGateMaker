from pathlib import Path

from research.families.f05_fill_quality_quote_ev.audit.order_level_panel import _manifest_days


def test_manifest_days_honors_replay_eligibility(tmp_path: Path) -> None:
    manifest = tmp_path / "days.csv"
    manifest.write_text(
        "day,replay_eligible\n"
        "2026-07-03,True\n"
        "2026-07-04,False\n"
        "2026-07-05,True\n",
        encoding="utf-8",
    )

    assert _manifest_days(manifest) == ["2026-07-03", "2026-07-05"]
