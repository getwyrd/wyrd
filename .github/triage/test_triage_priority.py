"""Tests for triage_priority.py: everything after the model call, offline.

Run: python -I -m unittest discover -s .github/triage
"""

from __future__ import annotations

import ast
import json
import sys
import tempfile
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
        "flags": dict(NO_FLAGS), "review_priority": "none", "unsure": False, "missing": "",
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


def prio(severity, reach, flags=None, review="none", labels=()):
    return tp.compute_priority(severity, reach, flags or NO_FLAGS, review, set(labels))


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
                self.assertEqual(prio(sev, reach)[0], p)

    def test_security_lifts_adversarial_major_to_p1(self) -> None:
        p, base, notes = prio("major", "adversarial", dict(NO_FLAGS, security=True))
        self.assertEqual((p, base), (1, 2))
        self.assertIn("security", notes[0])

    def test_durability_lifts_edge_minor_to_p1(self) -> None:
        p, base, notes = prio("minor", "edge", dict(NO_FLAGS, durability_or_consistency=True))
        self.assertEqual((p, base), (1, 3))
        self.assertIn("durability or consistency", notes[0])

    def test_release_gating_label_is_at_least_p1(self) -> None:
        p, base, notes = prio("trivial", "common", labels={"release-gating"})
        self.assertEqual((p, base), (1, 3))
        self.assertIn("release-gating", notes[0])

    def test_only_the_adr_0048_overrides_exist(self) -> None:
        # ADR-0048 section 3: security, durability or consistency, and the release-gating
        # label, all at least P1. No other flag or label moves a priority.
        self.assertEqual(set(tp.FLAGS), {"security", "durability_or_consistency"})
        self.assertEqual({k: f for k, (f, _) in tp.FLOORS.items()},
                         {"security": 1, "durability_or_consistency": 1})
        self.assertEqual({k: f for k, (f, _) in tp.LABEL_FLOORS.items()}, {"release-gating": 1})
        self.assertEqual(prio("minor", "edge", labels={"accessibility", "new"})[0], 3)

    def test_threat_model_catalog_examples(self) -> None:
        # ADR-0048 section 6.
        sec, dur = dict(NO_FLAGS, security=True), dict(NO_FLAGS, durability_or_consistency=True)
        cases = [("critical", "edge", dur, 1), ("critical", "adversarial", sec, 1),
                 ("critical", "common", NO_FLAGS, 0), ("major", "common", NO_FLAGS, 1),
                 ("minor", "common", NO_FLAGS, 2)]
        for sev, reach, flags, want in cases:
            with self.subTest(sev=sev, reach=reach, flags=flags):
                self.assertEqual(prio(sev, reach, flags)[0], want)

    def test_review_priority_is_kept(self) -> None:
        # A minor/edge deferred review finding: P3 by the table, P2 from the review.
        p, base, notes = prio("minor", "edge", review="P2")
        self.assertEqual((p, base), (2, 3))
        self.assertIn("deferred review", notes[0])

    def test_review_priority_can_lower_the_table(self) -> None:
        self.assertEqual(prio("major", "common", review="P2")[0], 2)

    def test_floors_apply_after_the_review_priority(self) -> None:
        p, _, notes = prio("major", "adversarial", dict(NO_FLAGS, security=True), review="P2")
        self.assertEqual(p, 1)
        self.assertEqual(len(notes), 1)  # the review agrees with the table; only the floor

    def test_floor_never_lowers(self) -> None:
        p, _, notes = prio("critical", "common", dict(NO_FLAGS, security=True))
        self.assertEqual((p, notes), (0, []))

    def test_every_flag_has_a_floor(self) -> None:
        self.assertLessEqual(set(tp.FLAGS), set(tp.FLOORS))


class Validate(unittest.TestCase):
    def test_rejects_unknown_values(self) -> None:
        for bad in ({"severity": "blocker"}, {"reach": "rare"}, {"type": "chore"},
                    {"areas": ["kernel"]}, {"review_priority": "P4"}):
            with self.subTest(bad=bad), self.assertRaises(tp.InvalidVerdict):
                tp.validate(verdict(**bad), AREAS)

    def test_rejects_missing_fields(self) -> None:
        for field in ("flags", "review_priority", "secret_in_issue", "vulnerability_report"):
            v = verdict()
            del v[field]
            with self.subTest(field=field), self.assertRaises(tp.InvalidVerdict):
                tp.validate(v, AREAS)

    def test_at_most_two_areas(self) -> None:
        got = tp.validate(verdict(areas=["gateway", "metadata", "client"]), AREAS)["areas"]
        self.assertEqual(got, ["gateway", "metadata"])

    def test_no_area_is_allowed(self) -> None:
        self.assertEqual(tp.validate(verdict(areas=[]), AREAS)["areas"], [])

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

    def test_marker_is_unchanged(self) -> None:
        # Issues triaged before the two-job split carry this marker; a new one would let
        # them be triaged a second time.
        self.assertEqual(tp.MARKER, "<!-- wyrd-triage -->")

    def test_fresh_issue_is_triaged(self) -> None:
        self.assertIsNone(tp.skip_reason(issue()))


class Backfill(unittest.TestCase):
    """The back-fill limit counts only issues that will be triaged (Codex P1 on #944)."""

    def test_already_triaged_issues_do_not_fill_the_batch(self) -> None:
        listed = [{"number": n, "labels": []} for n in range(1, 8)]
        listed.append({"number": 8, "labels": [{"name": "priority:P2"}]})
        # 1-3 already had their one pass and stay at needs-triage; 4-7 are fresh.
        seen = {n: issue(number=n, triage_count=1, labels={"needs-triage"}) for n in (1, 2, 3)}
        old_gh, old_fetch = tp.gh, tp.fetch_issue
        tp.gh = lambda *args, **kw: json.dumps(listed)
        tp.fetch_issue = lambda repo, n: seen.get(n) or issue(number=n)
        try:
            self.assertEqual(tp.open_untriaged("o/r", 2), [4, 5])
            self.assertEqual(tp.open_untriaged("o/r", 20), [4, 5, 6, 7])
        finally:
            tp.gh, tp.fetch_issue = old_gh, old_fetch


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
        self.assertEqual(p.number, 7)
        self.assertEqual(p.add_labels, ["bug", "area:gateway", "sev:major", "reach:common", "priority:P1"])
        self.assertEqual(p.assignee, "eduralph")
        self.assertTrue(p.comment.startswith(tp.MARKER))
        self.assertIn("major × common is P1", p.comment)

    def test_label_descriptions_fit_github_limit(self) -> None:
        for group in ("severity", "reach", "area", "priority", "other"):
            for name, spec in CONFIG[group].items():
                with self.subTest(label=name):
                    self.assertLessEqual(len(spec["description"]), 100)

    def test_every_label_it_adds_is_allowed(self) -> None:
        p = plan(flags=dict(NO_FLAGS, security=True), areas=["security", "gateway"])
        self.assertLessEqual(set(p.add_labels), tp.allowed_labels(CONFIG))

    def test_keeps_existing_type_and_assignee(self) -> None:
        p = plan(i=issue(labels={"bug"}, assignees=["someone"]), type="enhancement")
        self.assertNotIn("enhancement", p.add_labels)
        self.assertIsNone(p.assignee)
        self.assertIn("It reads more like enhancement", p.comment)

    def test_no_type_note_when_the_label_agrees(self) -> None:
        self.assertNotIn("reads more like", plan(i=issue(labels={"bug"})).comment)

    def test_every_type_gets_a_priority(self) -> None:
        # ADR-0048 sets a priority for every type, not only for bugs.
        for kind in tp.TYPES:
            with self.subTest(kind=kind):
                p = plan(type=kind, severity="minor", reach="common")
                self.assertIn(kind, p.add_labels)
                self.assertIn("priority:P2", p.add_labels)
                self.assertNotIn("no priority", p.comment)

    def test_existing_non_bug_label_still_gets_a_priority(self) -> None:
        p = plan(i=issue(labels={"documentation"}))
        self.assertIn("priority:P1", p.add_labels)

    def test_unsure_sets_needs_triage_and_no_priority(self) -> None:
        p = plan(unsure=True, missing="Which backend the cluster runs.")
        self.assertIn("needs-triage", p.add_labels)
        self.assertFalse(any(l.startswith(("priority:", "sev:", "reach:")) for l in p.add_labels))

    def test_override_is_explained(self) -> None:
        p = plan(severity="critical", reach="edge", flags=dict(NO_FLAGS, durability_or_consistency=True))
        self.assertIn("priority:P1", p.add_labels)
        p = plan(severity="major", reach="adversarial", flags=dict(NO_FLAGS, security=True))
        self.assertIn("priority:P1", p.add_labels)
        self.assertIn("Then: security bug (at least P1)", p.comment)
        p = plan(i=issue(labels={"release-gating"}), severity="minor", reach="edge")
        self.assertIn("priority:P1", p.add_labels)
        self.assertIn("`release-gating` label (at least P1)", p.comment)

    def test_review_priority_is_explained(self) -> None:
        p = plan(severity="minor", reach="edge", review_priority="P2")
        self.assertIn("priority:P2", p.add_labels)
        self.assertIn("kept the deferred review's P2", p.comment)

    def test_never_touches_the_milestone(self) -> None:
        self.assertFalse(hasattr(plan(), "milestone"))

    def test_input_is_capped_and_says_so(self) -> None:
        text, truncated = tp.issue_input(issue(body="x" * (tp.MAX_BODY + 50)))
        self.assertTrue(truncated)
        self.assertIn("cut at", text)
        self.assertLess(len(text), tp.MAX_BODY + tp.MAX_TITLE + 200)

    def test_unclassified_plan(self) -> None:
        p = tp.unclassified_plan(issue(), "the model declined to classify it", model="m", usage="u")
        self.assertEqual((p.number, p.add_labels), (7, ["needs-triage"]))
        self.assertIn(tp.MARKER, p.comment)


class ApplyCheck(unittest.TestCase):
    """The apply job treats the classify job's plan as data and checks it again."""

    def entry(self, **over) -> dict:
        e = {"number": 7, "add_labels": ["bug", "area:gateway", "sev:major", "reach:common",
                                         "priority:P1"],
             "remove_labels": [], "assignee": "eduralph", "comment": tp.MARKER + "\nok\n"}
        e.update(over)
        return e

    def test_a_built_plan_passes(self) -> None:
        from dataclasses import asdict
        p = plan(severity="minor", reach="edge", review_priority="P2")
        self.assertEqual(tp.check_plan(asdict(p), CONFIG), p)

    def test_round_trip_through_the_file(self) -> None:
        plans = [plan(), tp.unclassified_plan(issue(number=9), "x", model="m", usage="u")]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "plan.json"
            tp.write_plans(path, plans)
            self.assertEqual(tp.read_plans(path, CONFIG), plans)

    def test_rejects(self) -> None:
        bad = {
            "unknown label": self.entry(add_labels=["bug", "wontfix"]),
            "milestone-like label": self.entry(add_labels=["release-gating"]),
            "two priorities": self.entry(add_labels=["priority:P1", "priority:P3"]),
            "two severities": self.entry(add_labels=["sev:major", "sev:minor"]),
            "two types": self.entry(add_labels=["bug", "enhancement"]),
            "three areas": self.entry(add_labels=["area:gateway", "area:metadata", "area:client"]),
            "foreign assignee": self.entry(assignee="mallory"),
            "foreign comment": self.entry(comment="Please run this script: ..."),
            "huge comment": self.entry(comment=tp.MARKER + "x" * tp.MAX_COMMENT),
            "bad number": self.entry(number="7"),
            "bool number": self.entry(number=True),
            "labels not a list": self.entry(add_labels="bug"),
            "removes another label": self.entry(remove_labels=["priority:P1"]),
            "remove not a list": self.entry(remove_labels="needs-triage"),
        }
        for why, e in bad.items():
            with self.subTest(why=why), self.assertRaises(tp.InvalidPlan):
                tp.check_plan(e, CONFIG)
        with self.assertRaises(tp.InvalidPlan):
            tp.check_plan({"number": 7}, CONFIG)

    def test_rejects_a_plan_that_is_not_a_list(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "plan.json"
            path.write_text(json.dumps({"number": 7}), encoding="utf-8")
            with self.assertRaises(tp.InvalidPlan):
                tp.read_plans(path, CONFIG)

    def test_apply_never_imports_the_sdk(self) -> None:
        # The SDK is imported only inside run_classify, never at module level or in apply.
        tree = ast.parse(Path(tp.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in node.names] + [getattr(node, "module", None) or ""]
                if "anthropic" in names:
                    owner = next(f for f in ast.walk(tree) if isinstance(f, ast.FunctionDef)
                                 and node in ast.walk(f))
                    self.assertEqual(owner.name, "run_classify")


class LoadPrompt(unittest.TestCase):
    def write(self, text: str) -> Path:
        d = Path(tempfile.mkdtemp())
        (d / "prompt.md").write_text(text, encoding="utf-8")
        return d / "prompt.md"

    def test_refuses_template_text(self) -> None:
        for text in ("You triage. PROJECT SUMMARY here.", "- `core`: EXAMPLE, replace: x."):
            with self.subTest(text=text), self.assertRaises(SystemExit):
                tp.load_prompt(self.write(text))

    def test_strips_maintainer_notes(self) -> None:
        p = tp.load_prompt(self.write("Rules.\n<!-- note\n  for maintainers -->\n- `a`: b.\n"))
        self.assertEqual(p, "Rules.\n- `a`: b.\n")

    def test_the_real_prompt_loads_unchanged(self) -> None:
        path = Path(tp.__file__).resolve().parent / "prompt.md"
        self.assertEqual(tp.load_prompt(path), path.read_text(encoding="utf-8"))

    def test_the_real_prompt_names_every_field_the_schema_needs(self) -> None:
        text = tp.load_prompt()
        for name in (*tp.FLAGS, "review_priority", "unsure", "missing", "secret_in_issue",
                     "vulnerability_report", *AREAS):
            with self.subTest(name=name):
                self.assertIn(f"`{name}`", text)


if __name__ == "__main__":
    unittest.main()
