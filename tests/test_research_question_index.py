"""Reader-facing research questions must remain discoverable and scoped."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def family_registers():
    registry = json.loads((ROOT / "research/registry.json").read_text())
    return [
        (family, json.loads((ROOT / family["research_questions"]).read_text()))
        for family in registry["families"]
    ]


def test_every_family_has_one_owned_question_register():
    seen = set()
    for family, register in family_registers():
        assert family["research_questions"] == (
            family["directory"] + "/research_questions.json"
        )
        assert register["family"] == family["id"]
        assert register["execution_authorized_by_index"] is False
        for question in register["questions"]:
            assert question["id"].startswith(family["id"] + "-")
            assert question["id"] not in seen
            seen.add(question["id"])
            assert question["status"] in {"pending_validation", "in_progress", "resolved"}
            assert question["question"] and question["question_zh"]
            assert question["gaps"] and question["next_validation"]
            if question["status"] == "resolved":
                assert question["resolution"]["source_ref"]
                assert question["resolution"]["scope"]
            else:
                assert question["resolution"] is None


def test_question_sources_are_locatable_without_private_paths():
    for _, register in family_registers():
        for question in register["questions"]:
            assert question["source_refs"]
            assert len(question["source_refs"]) == len(set(question["source_refs"]))
            for source in question["source_refs"]:
                assert not source.startswith(("/", "file:"))
                assert "/private/" not in source
                if not source.startswith("https://"):
                    assert (ROOT / source).is_file(), source


def test_current_evidence_is_not_reclassified_as_pending_results():
    for _, register in family_registers():
        for evidence in register["current_evidence"]:
            assert (ROOT / evidence["source_ref"]).is_file()
            assert evidence["boundary"]
    summary = json.loads((ROOT / "research/final107_comparison.json").read_text())
    assert summary["closure"]["economic_status"] == "closed_complete"
    assert summary["fresh_holdout"] is False
    assert all(candidate["economic_status"] == "closed_complete"
               for candidate in summary["candidates"])


def test_existing_markout_and_cooldown_question_identities_survive():
    questions = {
        q["id"]: q for _, register in family_registers()
        for q in register["questions"]
    }
    for identity in (
        "F02-SCALAR-ADAPTER-CURRENT-SOURCE",
        "F05-MULTISCALE-COOLDOWN-CURRENT-SOURCE",
        "F05-STALE-QUOTE-ADVERSE-FILL",
        "F05-MARKOUT-HOLDING-LIFECYCLE",
    ):
        assert questions[identity]["owner_artifact_id"]
