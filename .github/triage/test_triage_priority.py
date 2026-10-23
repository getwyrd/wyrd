"""Tests for triage_priority.py: everything after the model call, offline.

Run: python -I -m unittest discover -s .github/triage
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import triage_priority as tp  # noqa: E402

CONFIG = tp.load_config()
AREAS = tp.area_names(CONFIG)
NO_FLAGS = {f: False for f in tp.FLAGS}


def verdict(**over) -> dict:
    v = {
        "type": "bug", "severity": "major", "reach": "common", "areas": ["gateway"],
        "flags": dict(NO_FLAGS), "unsure": False, "missing": "",
        "secret_in_issue": False, "vulnerability_report": False,
        "reasons": {"type": "The gateway answers a supported request wrongly.",
                    "severity": "A supported S3 operation fails with no workaround.",
                    "reach": "Any client listing a large bucket hits it.",
                    "area": "The S3 gateway's listing code.",
                    "overrides": "None apply."},
    }
    v.update(over)
    return v


def issue(**over) -> tp.Issue:
    i = tp.Issue(number=7, title="ListObjectsV2 ignores the continuation token",
                 body="Steps: list a bucket with more than 1000 keys.", labels=set(), assignees=[])
    for k, val in over.items():
        setattr(i, k, val)
    return i


def plan(i: tp.Issue | None = None, **v) -> tp.Plan:
    return tp.build_plan(i or issue(), tp.validate(verdict(**v), AREAS), CONFIG,
                         model=tp.MODEL, usage="1 in, 1 out", truncated=False)


class Table(unittest.TestCase):
    def test_matches_adr_0048(self) -> None:
        expected = {
            ("critical", "common"): 0, ("critical", "edge"): 1, ("critical", "adversarial"): 1,
            ("major", "common"): 1, ("major", "edge"): 2, ("major", "adversarial"): 2,
            ("minor", "common"): 2, ("minor", "edge"): 3, ("minor", "adversarial"): 3,
            ("trivial", "common"): 3, ("trivial", "edge"): 3, ("trivial", "adversarial"): 3,
        }
        for (sev, reach), p in expected.items():
            with self.subTest(sev=sev, reach=reach):
                self.assertEqual(tp.compute_priority(sev, reach, NO_FLAGS, set())[0], p)

    def test_security_lifts_adversarial_major_to_p1(self) -> None:
        prio, base, applied = tp.compute_priority(
            "major", "adversarial", dict(NO_FLAGS, security=True), set())
        self.assertEqual((prio, base), (1, 2))
        self.assertIn("security", applied[0])

    def test_durability_lifts_edge_minor_to_p1(self) -> None:
        flags = dict(NO_FLAGS, durability_or_consistency=True)
        self.assertEqual(tp.compute_priority("minor", "edge", flags, set())[0], 1)

    def test_release_gating_label_is_a_floor(self) -> None:
        self.assertEqual(tp.compute_priority("trivial", "common", NO_FLAGS, {"release-gating"})[0], 1)

    def test_floor_never_lowers(self) -> None:
        prio, _, applied = tp.compute_priority("critical", "common", dict(NO_FLAGS, security=True), set())
        self.assertEqual((prio, applied), (0, []))

    def test_every_flag_has_a_floor(self) -> None:
        self.assertLessEqual(set(tp.FLAGS), set(tp.FLOORS))


class Validate(unittest.TestCase):
    def test_rejects_unknown_values(self) -> None:
        for bad in ({"severity": "blocker"}, {"reach": "rare"}, {"type": "chore"},
                    {"areas": ["kernel"]}):
            with self.subTest(bad=bad), self.assertRaises(tp.InvalidVerdict):
                tp.validate(verdict(**bad), AREAS)

    def test_rejects_missing_fields(self) -> None:
        for field in ("flags", "secret_in_issue", "vulnerability_report"):
            v = verdict()
            del v[field]
            with self.subTest(field=field), self.assertRaises(tp.InvalidVerdict):
                tp.validate(v, AREAS)

    def test_at_most_two_areas(self) -> None:
        got = tp.validate(verdict(areas=["gateway", "metadata", "client"]), AREAS)["areas"]
        self.assertEqual(got, ["gateway", "metadata"])

    def test_schema_enums_match_config(self) -> None:
        schema = tp.verdict_schema(AREAS)
        self.assertEqual(schema["properties"]["areas"]["items"]["enum"], AREAS)
        self.assertEqual(set(schema["required"]), set(schema["properties"]))


class Echo(unittest.TestCase):
    BODY = ("PUT to bucket acme-prod-logs fails on gw-3.internal.example with key "
            "AKIA4XQ7EXAMPLE and the log line: commit rejected at version 48213.")

    def test_withholds_a_reason_that_quotes_the_issue(self) -> None:
        r = tp.scrub("The log line commit rejected at version shows the failure.", self.BODY)
        self.assertIn("withheld", r)

    def test_withholds_a_reason_that_repeats_a_number(self) -> None:
        self.assertIn("withheld", tp.scrub("The commit at 48213 is refused.", self.BODY))

    def test_withholds_a_single_token_secret(self) -> None:
        for reason in ("The access key AKIA4XQ7EXAMPLE is rejected by the gateway.",
                       "The gateway gw-3.internal.example refuses the PUT.",
                       "Writes to acme-prod-logs fail at commit."):
            with self.subTest(reason=reason):
                self.assertIn("withheld", tp.scrub(reason, self.BODY))

    def test_hyphenated_words_are_not_identifiers(self) -> None:
        body = "A half-committed write is visible to a read-only client."
        r = "A half-committed write shows up to a read-only reader."
        self.assertEqual(tp.scrub(r, body), r)

    def test_keeps_a_general_reason(self) -> None:
        r = "A PUT through the gateway fails at commit time."
        self.assertEqual(tp.scrub(r, self.BODY), r)

    def test_neutralises_mentions_and_markup(self) -> None:
        r = tp.scrub("ping @eduralph <img src=x> | `code` **bold**", "")
        self.assertNotIn("@e", r)
        for ch in "<>|`*":
            self.assertNotIn(ch, r)

    def test_one_word_is_not_a_reason(self) -> None:
        for bare in ("major", "bug", "", "area gateway"):
            with self.subTest(bare=bare):
                self.assertEqual(tp.scrub(bare, ""), "(no reason given)")

    def test_schema_describes_each_reason(self) -> None:
        props = tp.verdict_schema(AREAS)["properties"]["reasons"]["properties"]
        for key, spec in props.items():
            with self.subTest(reason=key):
                self.assertIn("sentence", spec["description"])

    def test_caps_length(self) -> None:
        self.assertLessEqual(len(tp.scrub("word " * 100, "")), tp.MAX_REASON)


class SecurityNotes(unittest.TestCase):
    def test_vulnerability_report_is_labelled_but_not_restated(self) -> None:
        p = plan(severity="critical", reach="adversarial", vulnerability_report=True,
                 flags=dict(NO_FLAGS, security=True))
        self.assertIn("priority:P1", p.add_labels)
        self.assertIn("SECURITY.md", p.comment)
        for reason in verdict()["reasons"].values():
            self.assertNotIn(reason, p.comment)

    def test_vulnerability_report_hides_what_is_missing(self) -> None:
        p = plan(vulnerability_report=True, unsure=True, missing="Which header triggers the bypass.")
        self.assertNotIn("header", p.comment)

    def test_secret_note_asks_to_rotate(self) -> None:
        self.assertIn("rotate", plan(secret_in_issue=True).comment)

    def test_no_notes_on_an_ordinary_issue(self) -> None:
        c = plan().comment
        self.assertNotIn("SECURITY.md", c)
        self.assertNotIn("rotate", c)


class Skip(unittest.TestCase):
    def test_skips(self) -> None:
        cases = {
            "already has a priority": issue(labels={"priority:P2"}),
            "is a pull request": issue(is_pr=True),
            "is closed": issue(state="closed"),
            "was opened by a bot": issue(author_is_bot=True),
            "was already triaged": issue(triage_count=1),
        }
        for why, i in cases.items():
            with self.subTest(why=why):
                self.assertEqual(tp.skip_reason(i), why)

    def test_needs_triage_is_retried_on_an_edit(self) -> None:
        self.assertIsNone(tp.skip_reason(issue(triage_count=1, labels={"needs-triage"}), retry=True))

    def test_backfill_and_reopen_never_retry(self) -> None:
        # A back-fill batch used to pick a needs-triage issue up again and comment again.
        self.assertEqual(tp.skip_reason(issue(triage_count=1, labels={"needs-triage"})),
                         "was already triaged")

    def test_edit_does_not_retry_a_placed_issue(self) -> None:
        self.assertEqual(tp.skip_reason(issue(triage_count=1), retry=True), "was already triaged")

    def test_retries_are_capped(self) -> None:
        i = issue(triage_count=tp.MAX_RETRIES + 1, labels={"needs-triage"})
        self.assertEqual(tp.skip_reason(i, retry=True), "was retried too often")
        i = issue(triage_count=tp.MAX_RETRIES, labels={"needs-triage"})
        self.assertIsNone(tp.skip_reason(i, retry=True))

    def test_only_the_workflow_comments_count(self) -> None:
        bot, person = {"login": tp.BOT_LOGIN}, {"login": "someone"}
        comments = [{"body": tp.MARKER + " x", "user": bot},
                    {"body": "copied: " + tp.MARKER, "user": person},
                    {"body": "no marker", "user": bot},
                    {"body": None, "user": None}]
        self.assertEqual(tp.triage_comments(comments), 1)

    def test_fresh_issue_is_triaged(self) -> None:
        self.assertIsNone(tp.skip_reason(issue()))


class ClearsNeedsTriage(unittest.TestCase):
    def test_a_placed_priority_clears_needs_triage(self) -> None:
        p = plan(i=issue(labels={"needs-triage"}))
        self.assertEqual(p.remove_labels, ["needs-triage"])
        self.assertTrue(any(l.startswith("priority:") for l in p.add_labels))

    def test_still_unsure_keeps_it(self) -> None:
        p = plan(i=issue(labels={"needs-triage"}), unsure=True, missing="Which version ran.")
        self.assertEqual(p.remove_labels, [])

    def test_nothing_removed_on_a_fresh_issue(self) -> None:
        self.assertEqual(plan().remove_labels, [])


class BuildPlan(unittest.TestCase):
    def test_labels_and_priority(self) -> None:
        p = plan()
        self.assertEqual(p.add_labels, ["bug", "area:gateway", "sev:major", "reach:common", "priority:P1"])
        self.assertEqual(p.assignee, "eduralph")
        self.assertTrue(p.comment.startswith(tp.MARKER))
        self.assertIn("major × common is P1", p.comment)

    def test_label_descriptions_fit_github_limit(self) -> None:
        for group in ("severity", "reach", "area", "priority", "other"):
            for name, spec in CONFIG[group].items():
                with self.subTest(label=name):
                    self.assertLessEqual(len(spec["description"]), 100)

    def test_every_label_it_adds_exists_in_config(self) -> None:
        known = {n for g in ("severity", "reach", "area", "priority", "other") for n in CONFIG[g]}
        known |= set(tp.TYPES)
        p = plan(i=issue(), flags=dict(NO_FLAGS, security=True), areas=["security", "gateway"])
        self.assertLessEqual(set(p.add_labels), known)

    def test_keeps_existing_type_and_assignee(self) -> None:
        p = plan(i=issue(labels={"enhancement"}, assignees=["someone"]))
        self.assertNotIn("bug", p.add_labels)
        self.assertIsNone(p.assignee)
        self.assertIn("It reads more like bug", p.comment)

    def test_no_type_note_when_the_label_agrees(self) -> None:
        self.assertNotIn("reads more like", plan(i=issue(labels={"bug"})).comment)

    def test_unsure_sets_needs_triage_and_no_priority(self) -> None:
        p = plan(unsure=True, missing="Which backend the cluster runs.")
        self.assertIn("needs-triage", p.add_labels)
        self.assertFalse(any(l.startswith(("priority:", "sev:", "reach:")) for l in p.add_labels))

    def test_override_is_explained(self) -> None:
        p = plan(severity="critical", reach="edge", flags=dict(NO_FLAGS, durability_or_consistency=True))
        self.assertIn("priority:P1", p.add_labels)
        p = plan(severity="major", reach="adversarial", flags=dict(NO_FLAGS, security=True))
        self.assertIn("priority:P1", p.add_labels)
        self.assertIn("Raised by: security bug", p.comment)

    def test_never_touches_the_milestone(self) -> None:
        self.assertFalse(hasattr(plan(), "milestone"))

    def test_input_is_capped_and_says_so(self) -> None:
        text, truncated = tp.issue_input(issue(body="x" * (tp.MAX_BODY + 50)))
        self.assertTrue(truncated)
        self.assertIn("cut at", text)
        self.assertLess(len(text), tp.MAX_BODY + tp.MAX_TITLE + 200)

    def test_unclassified_plan(self) -> None:
        p = tp.unclassified_plan(issue(), "the model declined to classify it", model="m", usage="u")
        self.assertEqual(p.add_labels, ["needs-triage"])
        self.assertIn(tp.MARKER, p.comment)


if __name__ == "__main__":
    unittest.main()
