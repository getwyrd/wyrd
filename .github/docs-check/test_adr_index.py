"""Tests for the ADR index check (adr_index.py next to this file).

Each case builds a throwaway git repo from a consistent set of ADRs plus one change, runs
the check as a subprocess and looks for the expected problem in its output. The script is
never imported, so a missing script fails each test instead of breaking collection.

    python3 -I -m unittest discover -s .github/docs-check -v
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "adr_index.py"
ADR = "docs/design/adr"


def adr(num: str, title: str, status: str, extra: str = "") -> str:
    return (f"---\ncreated: 01.01.2026 00:00\ntype: adr\nstatus: {status}\n{extra}"
            f"tags:\n  - adr\n---\n# {num}. {title}\n\n## Context\n\nText.\n\n---\n\n"
            "# Not the title\n")


def index(*rows: str, extra: str = "") -> str:
    return ("---\ntype: index\n---\n# Architecture Decision Records\n\n## Index\n\n"
            "| # | Title | Status |\n|---|-------|--------|\n" + "".join(r + "\n" for r in rows)
            + extra)


# A consistent set: one of each status, 0003 superseded by 0004.
GOOD = {
    f"{ADR}/0001-record-decisions.md": adr("0001", "Record decisions", "Accepted"),
    f"{ADR}/0002-license.md": adr("0002", "License", "Proposed"),
    f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded", "superseded-by: 0004\n"),
    f"{ADR}/0004-new-way.md": adr("0004", "New way", "Accepted", "supersedes: ADR-0003\n"),
}
GOOD_ROWS = [
    "| [0001](0001-record-decisions.md) | Record decisions | Accepted |",
    "| [0002](0002-license.md) | License, with a longer summary than the heading | Proposed |",
    "| [0003](0003-old-way.md) | Old way | Superseded by [0004](0004-new-way.md) |",
    "| [0004](0004-new-way.md) | New way | **Accepted** |",
]
CANDIDATES = ("\n## ADR candidates\n\n| Candidate | Source | Why |\n|---|---|---|\n"
              "| Something | [proposal 1](../proposals/draft/0001-x.md) | Proposed |\n")


def with_index(files: dict[str, str], rows: list[str], extra: str = CANDIDATES) -> dict[str, str]:
    return {**files, f"{ADR}/README.md": index(*rows, extra=extra)}


def replace_row(num: str, row: str) -> list[str]:
    return [row if r.startswith(f"| [{num}]") else r for r in GOOD_ROWS]


MUST_FAIL = {
    "ADR not in the index": (
        with_index(GOOD, GOOD_ROWS[:-1]), "0004-new-way.md: not listed"),
    "ADR listed twice": (
        with_index(GOOD, GOOD_ROWS + [GOOD_ROWS[0]]), "is already listed on line"),
    "row for a missing file": (
        with_index(GOOD, GOOD_ROWS + ["| [0009](0009-gone.md) | Gone | Proposed |"]),
        "row links to 0009-gone.md"),
    "index status differs": (
        with_index(GOOD, replace_row("0002", "| [0002](0002-license.md) | License | Accepted |")),
        "status `Accepted` differs from the file's `Proposed`"),
    "comment after the status": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Proposed   # Proposed | Accepted")}, GOOD_ROWS),
        "move the comment to its own line"),
    "status names the successor in other words": (
        with_index({**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by the new way")}, GOOD_ROWS),
        "is not exactly Proposed, Accepted, Superseded or `Superseded by ADR-NNNN`"),
    "text after the named successor": (
        with_index({**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by ADR-0004 (in part)")}, GOOD_ROWS),
        "is not exactly Proposed, Accepted, Superseded or `Superseded by ADR-NNNN`"),
    "status names another successor than the index": (
        with_index({**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by ADR-0002")}, GOOD_ROWS),
        "the successors in `superseded-by:` or the status differ from the index"),
    "status and superseded-by disagree": (
        with_index({**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by ADR-0004", "superseded-by: 0002\n")}, GOOD_ROWS),
        "`superseded-by:` differs from the ADRs `status:` names"),
    "file name not NNNN-kebab": (
        with_index({**GOOD, f"{ADR}/5-Bad_Name.md": adr("0005", "Bad", "Proposed")}, GOOD_ROWS),
        "5-Bad_Name.md: not named NNNN-kebab-title.md"),
    "two ADRs share a number": (
        with_index({**GOOD, f"{ADR}/0002-other.md": adr("0002", "Other", "Proposed")},
                   GOOD_ROWS + ["| [0002](0002-other.md) | Other | Proposed |"]),
        "number 0002 is used by"),
    "heading without the number": (
        with_index({**GOOD, f"{ADR}/0002-license.md": "---\ntype: adr\nstatus: Proposed\n---\n# License\n"}, GOOD_ROWS),
        "first heading is not `# 0002. Title`"),
    "no frontmatter": (
        with_index({**GOOD, f"{ADR}/0002-license.md": "# 0002. License\n"}, GOOD_ROWS),
        "0002-license.md: no YAML frontmatter"),
    "type is not adr": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Proposed").replace("type: adr", "type: proposal")}, GOOD_ROWS),
        "`type:` is not `adr`"),
    "superseded row names no successor": (
        with_index(GOOD, replace_row("0003", "| [0003](0003-old-way.md) | Old way | Superseded |")),
        "must name its successor"),
    "successor does not name it back": (
        with_index({**GOOD, f"{ADR}/0004-new-way.md": adr("0004", "New way", "Accepted")}, GOOD_ROWS),
        "0004-new-way.md: `supersedes:` does not name 0003"),
    "superseded-by differs from the index": (
        with_index({**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded", "superseded-by: 0002\n")}, GOOD_ROWS),
        "the successors in `superseded-by:` or the status differ from the index"),
    "index names a successor that does not exist": (
        with_index(GOOD, replace_row("0003", "| [0003](0003-old-way.md) | Old way | Superseded by 0004 and 0042 |")),
        "names successor 0042, which does not exist"),
    "index-only supersession by a Proposed ADR": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Proposed", "supersedes: 0001\n")},
                   replace_row("0001", "| [0001](0001-record-decisions.md) | Record decisions | Superseded by [0002](0002-license.md) |")),
        "Accepted ADR 0001 is shown Superseded, but no successor the row names is Accepted yet"),
    "index-only supersession the successor does not name back": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Accepted")},
                   replace_row("0001", "| [0001](0001-record-decisions.md) | Record decisions | Superseded by [0002](0002-license.md) |")
                   [:1] + [GOOD_ROWS[1].replace("Proposed", "Accepted")] + GOOD_ROWS[2:]),
        "0002-license.md: `supersedes:` does not name 0001"),
    "Proposed file shown Superseded in the index": (
        with_index(GOOD, replace_row("0002", "| [0002](0002-license.md) | License | Superseded by [0004](0004-new-way.md) |")),
        "status `Superseded by 0004` differs from the file's `Proposed`"),
    "accepted ADR supersedes one not stamped": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Accepted", "supersedes: 0001\n")},
                   replace_row("0002", "| [0002](0002-license.md) | License | Accepted |")),
        "0001-record-decisions.md: still Accepted, but Accepted ADR 0002 supersedes it"),
    "supersedes a number that does not exist": (
        with_index({**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Accepted", "supersedes: 0042\n")},
                   replace_row("0002", "| [0002](0002-license.md) | License | Accepted |")),
        "`supersedes:` names 0042, which does not exist"),
    "status read from the Status column, not the last": (
        {**GOOD, f"{ADR}/README.md": index(*[r.replace("| Proposed |", "| Accepted | Proposed |")
                                              if "0002" in r else r + " |" for r in GOOD_ROWS])
         .replace("| # | Title | Status |", "| # | Title | Status | Notes |")},
        "status `Accepted` differs from the file's `Proposed`"),
    "no index at all": (dict(GOOD), "README.md: missing"),
}

MUST_PASS = {
    "consistent set": with_index(GOOD, GOOD_ROWS),
    "no ADRs at all": {"README.md": "# Readme\n"},
    "snake_case in an index title": with_index(GOOD, replace_row(
        "0002", "| [0002](0002-license.md) | {{LICENSE_NAME}} and prompt_toolkit | Proposed |")),
    "status names the successor (nl-shell's and stemma's form)": with_index(
        {**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by ADR-0004")}, GOOD_ROWS),
    "status names two successors": with_index(
        {**GOOD, f"{ADR}/0003-old-way.md": adr("0003", "Old way", "Superseded by ADR-0002 and ADR-0004"),
         f"{ADR}/0002-license.md": adr("0002", "License", "Proposed", "supersedes: ADR-0003\n")},
        replace_row("0003", "| [0003](0003-old-way.md) | Old way | Superseded by [0002](0002-license.md), [0004](0004-new-way.md) |")),
    "a Notes column after Status": {**GOOD, f"{ADR}/README.md": index(
        *[r + " a note |" for r in GOOD_ROWS]).replace("| # | Title | Status |", "| # | Title | Status | Notes |")},
    "supersession recorded in the index, the old file untouched (ADR-0001)": with_index(
        {**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Accepted", "supersedes: 0001\n")},
        [GOOD_ROWS[0].replace("| Accepted |", "| Superseded by [0002](0002-license.md) |"),
         GOOD_ROWS[1].replace("Proposed", "Accepted")] + GOOD_ROWS[2:]),
    "a Proposed ADR may name what it would supersede": with_index(
        {**GOOD, f"{ADR}/0002-license.md": adr("0002", "License", "Proposed", "supersedes: 0001\n")},
        GOOD_ROWS),
}


class AdrIndex(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.env = {
            **os.environ,
            "HOME": str(self.tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "adr-index test",
            "GIT_AUTHOR_EMAIL": "adr-index@example.invalid",
            "GIT_COMMITTER_NAME": "adr-index test",
            "GIT_COMMITTER_EMAIL": "adr-index@example.invalid",
        }
        self.count = 0

    def check(self, files: dict[str, str]) -> subprocess.CompletedProcess[str]:
        self.count += 1
        repo = self.tmp / f"repo{self.count}"
        repo.mkdir()

        def run(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(args, cwd=repo, capture_output=True, text=True,
                                  env=self.env, timeout=60)

        run("git", "init", "-q").check_returncode()
        for name, body in files.items():
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
        run("git", "add", "-A").check_returncode()
        run("git", "commit", "-q", "-m", "fixture").check_returncode()
        return run(sys.executable, "-I", str(SCRIPT))

    def test_problem_fails_and_is_named(self) -> None:
        for name, (files, expected) in MUST_FAIL.items():
            with self.subTest(name):
                result = self.check(files)
                out = result.stdout + result.stderr
                self.assertEqual(result.returncode, 1, out)
                self.assertIn(expected, result.stdout, out)

    def test_consistent_index_passes(self) -> None:
        for name, files in MUST_PASS.items():
            with self.subTest(name):
                result = self.check(files)
                out = result.stdout + result.stderr
                self.assertEqual(result.returncode, 0, out)
                self.assertIn(" 0 problems", result.stdout, out)


if __name__ == "__main__":
    unittest.main()
