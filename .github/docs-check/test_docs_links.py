"""Tests for the relative-link check (docs_links.py next to this file).

Each case builds a throwaway git repo, commits a few files and runs the check as a
subprocess. The script is never imported, so a missing script fails each test instead of
breaking collection. Every must-fail case checks that the file and the link are named in
the output, so a missing or crashing script cannot satisfy it.

    python3 -I -m unittest discover -s .github/docs-check -v

Taken from nl-shell's tests/test_docs_links.py, turned from pytest into unittest so CI
installs nothing.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "docs_links.py"


def run(repo: Path, *args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=repo, capture_output=True, text=True, env=env, timeout=60)


class DocsLinks(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        # A clean HOME keeps the user's git config out of the fixtures.
        self.env = {
            **os.environ,
            "HOME": str(self.tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "docs-links test",
            "GIT_AUTHOR_EMAIL": "docs-links@example.invalid",
            "GIT_COMMITTER_NAME": "docs-links test",
            "GIT_COMMITTER_EMAIL": "docs-links@example.invalid",
        }
        self.count = 0

    def check(self, files: dict[str, str]) -> subprocess.CompletedProcess[str]:
        """Commit `files` into a fresh repo and run the check there."""
        self.count += 1
        repo = self.tmp / f"repo{self.count}"
        repo.mkdir()
        run(repo, "git", "init", "-q", env=self.env).check_returncode()
        for name, body in files.items():
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
        run(repo, "git", "add", "-A", env=self.env).check_returncode()
        run(repo, "git", "commit", "-q", "-m", "fixture", env=self.env).check_returncode()
        return run(repo, sys.executable, "-I", str(SCRIPT), env=self.env)

    def test_broken_link_fails_and_names_file_and_link(self) -> None:
        for name, (body, dest) in MUST_FAIL.items():
            with self.subTest(name):
                result = self.check({**BASE_FILES, DOC: body})
                out = result.stdout + result.stderr
                self.assertEqual(result.returncode, 1, out)
                self.assertIn(f"{DOC}:", result.stdout, out)
                self.assertIn(f"broken link: {dest}", result.stdout, out)

    def test_link_that_is_not_checked_or_resolves_passes(self) -> None:
        for name, body in MUST_PASS.items():
            with self.subTest(name):
                result = self.check({**BASE_FILES, DOC: body})
                out = result.stdout + result.stderr
                self.assertEqual(result.returncode, 0, out)
                self.assertIn("0 broken", result.stdout, out)

    def test_backslash_escapes_in_dest_are_undone(self) -> None:
        # `gu\_ide.md` names the tracked file `gu_ide.md` once the escape is undone.
        result = self.check(
            {**BASE_FILES, "docs/gu_ide.md": "# G\n", DOC: "See [g](gu\\_ide.md).\n"}
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("1 relative links", result.stdout)
        self.assertIn("0 broken", result.stdout)

    def test_fence_inside_comment_does_not_hide_a_later_broken_link(self) -> None:
        # Comments are removed before fences are looked for, so no stray fence opens and
        # the broken link after the comment is still checked.
        result = self.check({**BASE_FILES, DOC: "<!--\n```\n-->\n[x](after-comment.md)\n"})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(f"{DOC}:4: broken link: after-comment.md", result.stdout)


# Tracked targets every fixture has: a file, a file in a subdirectory, and so a directory.
BASE_FILES = {
    "README.md": "# Readme\n",
    "docs/guide.md": "# Guide\n\n## Setup\n",
    "docs/rendered.md.jinja": "# {{ name }}\n",
}

DOC = "docs/page.md"

MUST_FAIL = {
    "inline": ("See [the guide](missing.md) here.\n", "missing.md"),
    "image": ("![diagram](img/missing.png)\n", "img/missing.png"),
    "reference definition": ("Text [t].\n\n[t]: missing.md\n", "missing.md"),
    "fragment": ("See [x](missing.md#x).\n", "missing.md#x"),
    "only the .jinja suffix is added": ("See [x](rendered).\n", "rendered"),
    "nested-list reference definition": (
        "- item\n  - nested item\n\n    [t]: nested-missing.md\n",
        "nested-missing.md",
    ),
    "after inline code on the same line": (
        "Run `make` then read [this](after-code.md).\n",
        "after-code.md",
    ),
    "balanced parentheses in dest": ("See [one](missing(1).md).\n", "missing(1).md"),
    "with a title": ('See [g](title-missing.md "The guide").\n', "title-missing.md"),
    # A fence must close: a broken link after the closing fence is still checked.
    "after a closed backtick fence": (
        "```\n[a](guide.md)\n```\n[y](after-fence.md)\n",
        "after-fence.md",
    ),
    "after a closed tilde fence": (
        "~~~\n[a](guide.md)\n~~~\n[y](after-tilde.md)\n",
        "after-tilde.md",
    ),
    "after a closed fence on a list-marker line": (
        "- ```\n  [a](guide.md)\n  ```\n\n[y](after-list-fence.md)\n",
        "after-list-fence.md",
    ),
    "after a closed fence in a block quote": (
        "> ```\n> [a](guide.md)\n> ```\n\n[y](after-quote-fence.md)\n",
        "after-quote-fence.md",
    ),
    # A fence closes only on a run at least as long as the opener.
    "after a longer fence closes past a shorter inner run": (
        "````\n```\n[a](guide.md)\n````\n[y](after-long-fence.md)\n",
        "after-long-fence.md",
    ),
    # A backslash-escaped backtick does not open a code span.
    "after an escaped backtick": ("\\`see [x](esc-missing.md) `y`\n", "esc-missing.md"),
    # A code span does not run across a blank line.
    "backtick run does not span paragraphs": (
        "`a\n\n[x](para-missing.md) `\n",
        "para-missing.md",
    ),
    "reference definition after quote marker": ("> [t]: missing.md\n", "missing.md"),
    # A closing run longer than the opener still closes the fence.
    "after a longer closing run": (
        "```\n[a](guide.md)\n````\n[y](after-longer.md)\n",
        "after-longer.md",
    ),
    # The link text is rescanned, so an image inside a link (a badge) is checked too.
    "image inside a link (badge)": ("[![b](missing.png)](guide.md)\n", "missing.png"),
}


MUST_PASS = {
    "external https and mailto": (
        "[site](https://example.com/missing.md) and [mail](mailto:someone@example.com)\n"
    ),
    "pure fragment": "Jump to [setup](#setup).\n",
    "valid file": "See [guide](guide.md) and [readme](../README.md).\n",
    "valid directory": "See [docs](../docs) and [root](../) and [here](./).\n",
    "valid file with fragment": "See [setup](guide.md#setup).\n",
    "file copier renders from a .jinja source": "See [it](rendered.md) and [src](rendered.md.jinja).\n",
    "comment spanning lines": "Before.\n<!--\nSee [x](missing.md).\n-->\nAfter.\n",
    "unclosed comment masks the rest": "Before.\n<!-- open\n\n[x](missing.md)\n\nmore\n",
    "backtick fence": "```markdown\n[x](missing.md)\n```\n",
    "tilde fence": "~~~\n[x](missing.md)\n~~~\n",
    "fence on a list-marker line": "- ```\n  [x](missing.md)\n  ```\n",
    "fence in a block quote": "> ```\n> [x](missing.md)\n> ```\n",
    "fence inside a comment": "<!--\n```\n-->\n[ok](guide.md)\n",
    "inline code single backticks": "Write `[x](missing.md)` literally.\n",
    "inline code double backticks": "Write ``[x](missing.md) with ` inside`` literally.\n",
    "footnote definition": "Text[^1].\n\n[^1]: missing.md\n",
    "escaped bracket": "Not a link: \\[x](missing.md).\n",
    "no closing paren (prose)": "See [x](missing.md for more.\n",
    "valid file with a title": 'See [g](guide.md "The guide") and [s](guide.md (Setup)).\n',
    "shorter inner run does not close a fence": "````\n```\n[x](missing.md)\n````\n",
    "other fence character does not close a fence": "```\n~~~\n[x](missing.md)\n```\n",
    # Closed by a longer run, so inline code cannot pair the two runs: only the fence
    # rule (with `>` markers stripped) hides this link.
    "fence in a block quote closed by a longer run": "> ```\n> [x](missing.md)\n> ````\n",
}


if __name__ == "__main__":
    unittest.main()
