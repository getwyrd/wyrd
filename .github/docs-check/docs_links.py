#!/usr/bin/env python3
"""Check that relative links in the tracked Markdown files resolve. Stdlib only.

Run from inside any git checkout (a worktree is fine; only `git` is used):

    python3 -I .github/docs-check/docs_links.py

Every tracked `.md` file (`git ls-files`) is scanned with this rule, applied in order:

1. Remove `<!-- ... -->` comments, even across lines. An unclosed `<!--` hides
   everything after it.
2. Remove fenced code. On each line, first strip leading whitespace, any `>` quote
   markers and one list marker (`-`, `*`, `+`, `1.`, `1)`). If the line then starts with
   3+ backticks or tildes, a fence opens. It closes at the next line, stripped the same
   way, that starts with a run of the same character at least as long. An unclosed fence
   runs to the end of the file.
3. Remove inline code: a backtick run up to the next run of the same length within the
   same paragraph (no blank line between). A backslash-escaped backtick does not open a
   span. An unmatched run is literal text.
4. Check what is left:
   - inline and image links `[t](dest)` / `![t](dest)`. A backslash-escaped `[` does
     not start a link. Balanced parentheses in `dest` are allowed. After `dest` and any
     whitespace, a `)` or a title opener (`"`, `'`, `(`) must follow, or it is not a
     link (so the prose `[x](a.md for more.` is skipped);
   - reference definitions `[t]: dest` at any indentation, after any `>` markers, but
     not `[^...]:` footnotes;
   - anything with a scheme (`https:`, `mailto:`, ...) and pure `#...` links are skipped;
   - otherwise the `#fragment` is stripped, backslash escapes are undone, the path is
     resolved relative to the file's directory, and it must be a tracked file or a
     directory that holds one. A tracked `X.jinja` also counts as `X`: copier renders the
     one into the other, so in a template the link is right in every rendered project.

Known gaps (by design, not bugs):
- indented (4-space) code blocks are not recognized, so a link inside one is checked;
- `<...>` destinations and percent-encoding are not handled, and link titles are only
  recognized by their opening quote or paren, not parsed;
- an unclosed `<!--` or fence masks the rest of the file;
- root-absolute links (`/docs/x.md`) are resolved relative to the file's directory like
  any other path, so they are reported broken;
- `#fragment` anchors are not checked against headings;
- anything else CommonMark does that the rule above does not.

Exit status: 0 = every relative link resolves, 1 = broken links (listed on stdout as
`file:line: broken link: dest`), 2 = git error.
"""

from __future__ import annotations

import posixpath
import re
import subprocess
import sys
from dataclasses import dataclass

COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
QUOTE_MARKERS_RE = re.compile(r"^(?:>\s*)*")
LIST_MARKER_RE = re.compile(r"^(?:[-*+]|\d+[.)])(?:\s+|$)")
FENCE_RE = re.compile(r"^(`{3,}|~{3,})")
REFDEF_RE = re.compile(r"^\s*(?:>\s*)*\[([^\]]+)\]:[ \t]*(\S+)")
SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
ESCAPE_RE = re.compile(r"\\([!-/:-@\[-`{-~])")
ASCII_PUNCT = frozenset("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")


@dataclass(frozen=True)
class Link:
    line: int  # 1-based
    dest: str


# --- step 1: comments -----------------------------------------------------------------


def strip_comments(text: str) -> str:
    """Remove `<!-- ... -->`, keeping its newlines so line numbers stay right."""
    text = COMMENT_RE.sub(lambda m: "\n" * m.group(0).count("\n"), text)
    start = text.find("<!--")
    if start != -1:  # unclosed: hides the rest of the file
        text = text[:start] + "\n" * text.count("\n", start)
    return text


# --- step 2: fenced code --------------------------------------------------------------


def fence_view(line: str) -> str:
    """The line with leading whitespace, `>` markers and one list marker stripped."""
    rest = QUOTE_MARKERS_RE.sub("", line.lstrip())
    rest = LIST_MARKER_RE.sub("", rest, count=1)
    return rest.lstrip()


def strip_fences(text: str) -> str:
    """Blank every line of a fenced block, its fence lines included."""
    out = []
    fence: str | None = None  # the opening run, while inside a fence
    for line in text.split("\n"):
        view = fence_view(line)
        if fence is None:
            m = FENCE_RE.match(view)
            if m:
                fence = m.group(1)
                out.append("")
            else:
                out.append(line)
            continue
        run = re.match(re.escape(fence[0]) + "+", view)
        if run and len(run.group(0)) >= len(fence):
            fence = None
        out.append("")
    return "\n".join(out)


# --- step 3: inline code --------------------------------------------------------------


def blank(s: str) -> str:
    return "".join("\n" if c == "\n" else " " for c in s)


def strip_inline_code(para: str) -> str:
    """Blank backtick code spans in one paragraph (no blank lines inside)."""
    out = []
    i, n = 0, len(para)
    while i < n:
        c = para[i]
        if c == "\\" and i + 1 < n and para[i + 1] in ASCII_PUNCT:
            out.append(para[i : i + 2])
            i += 2
            continue
        if c != "`":
            out.append(c)
            i += 1
            continue
        j = i
        while j < n and para[j] == "`":
            j += 1
        size = j - i
        close = find_run(para, j, size)
        if close == -1:  # unmatched: literal text
            out.append(para[i:j])
            i = j
            continue
        out.append(blank(para[i : close + size]))
        i = close + size
    return "".join(out)


def find_run(s: str, start: int, size: int) -> int:
    """Index of the next backtick run of exactly `size` at or after `start`, else -1."""
    i, n = start, len(s)
    while i < n:
        if s[i] != "`":
            i += 1
            continue
        j = i
        while j < n and s[j] == "`":
            j += 1
        if j - i == size:
            return i
        i = j
    return -1


# --- step 4: links --------------------------------------------------------------------


def inline_links(para: str) -> list[tuple[int, str]]:
    """(offset, dest) for every `[t](dest)` / `![t](dest)` in one paragraph."""
    found = []
    n = len(para)
    i = 0
    while i < n:
        c = para[i]
        if c == "\\":
            i += 2
            continue
        if c != "[":
            i += 1
            continue
        close = matching_bracket(para, i)
        if close != -1 and close + 1 < n and para[close + 1] == "(":
            dest = parse_dest(para, close + 2)
            if dest:
                found.append((i, dest))
        i += 1  # rescan the link text too: `[![img](a.png)](b)` holds two links
    return found


def matching_bracket(s: str, start: int) -> int:
    depth = 0
    i, n = start, len(s)
    while i < n:
        c = s[i]
        if c == "\\":
            i += 2
            continue
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def parse_dest(s: str, start: int) -> str:
    """The destination starting at `start` (just after `(`); "" if there is none.

    After the destination and any whitespace, the next character must be the closing `)`
    or the start of a title (`"`, `'`, `(`). Otherwise it is prose, not a link:
    `[x](missing.md for more.` has no destination.
    """
    i, n = start, len(s)
    while i < n and s[i] in " \t\n":
        i += 1
    begin = i
    depth = 0
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n:
            i += 2
            continue
        if c in " \t\n":
            break
        if c == "(":
            depth += 1
        elif c == ")":
            if depth == 0:
                break
            depth -= 1
        i += 1
    end = i
    while i < n and s[i] in " \t\n":
        i += 1
    if i >= n or s[i] not in ")\"'(":
        return ""  # no `)` or title follows: not a link
    return s[begin:end]


def find_links(text: str) -> list[Link]:
    """Every candidate link in a Markdown file, after steps 1-3."""
    lines = strip_fences(strip_comments(text)).split("\n")
    links: list[Link] = []
    para: list[str] = []
    para_start = 0

    def flush() -> None:
        if not para:
            return
        body = strip_inline_code("\n".join(para))
        for offset, dest in inline_links(body):
            links.append(Link(para_start + body.count("\n", 0, offset) + 1, dest))
        for k, line in enumerate(body.split("\n")):
            m = REFDEF_RE.match(line)
            if m and not m.group(1).startswith("^"):
                links.append(Link(para_start + k + 1, m.group(2)))
        para.clear()

    for idx, line in enumerate(lines):
        if line.strip():
            if not para:
                para_start = idx
            para.append(line)
        else:
            flush()
    flush()
    return sorted(links, key=lambda link: link.line)


def target_of(dest: str) -> str | None:
    """The repo-relative path a link points at, or None if it is not checked."""
    if dest.startswith("#") or SCHEME_RE.match(dest):
        return None
    return ESCAPE_RE.sub(r"\1", dest.split("#", 1)[0])


# --- driver ---------------------------------------------------------------------------


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def main() -> int:
    try:
        root = git("rev-parse", "--show-toplevel").strip()
        tracked = [p for p in git("-C", root, "ls-files", "-z").split("\0") if p]
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"docs_links: git failed: {exc}", file=sys.stderr)
        return 2

    files = set(tracked)
    dirs = {"."}
    for path in tracked:
        parent = posixpath.dirname(path)
        while parent and parent not in dirs:
            dirs.add(parent)
            parent = posixpath.dirname(parent)

    broken = []
    checked = 0
    md_files = sorted(p for p in tracked if p.endswith(".md"))
    for path in md_files:
        try:
            with open(posixpath.join(root, path), encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except FileNotFoundError:
            continue  # tracked but deleted in the working tree
        base = posixpath.dirname(path)
        for link in find_links(text):
            target = target_of(link.dest)
            if target is None:
                continue
            checked += 1
            resolved = posixpath.normpath(posixpath.join(base, target))
            if resolved not in files and resolved not in dirs and f"{resolved}.jinja" not in files:
                broken.append(f"{path}:{link.line}: broken link: {link.dest}")

    for line in broken:
        print(line)
    print(
        f"docs_links: {checked} relative links in {len(md_files)} Markdown files, "
        f"{len(broken)} broken"
    )
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
