#!/usr/bin/env python3
"""Check that docs/design/adr/README.md matches the ADR files. Stdlib only.

Run from inside any git checkout (a worktree is fine; only `git` is used):

    python3 -I .github/docs-check/adr_index.py

It reads the tracked files under docs/design/adr/ and reports, as `file: message`:

- a file other than README.md that isn't named `NNNN-kebab-title.md`;
- an ADR without YAML frontmatter, without `type: adr`, or whose `status:` is not exactly
  Proposed, Accepted, Superseded, or `Superseded by ADR-NNNN` (further successors joined
  by `and` or `,`; the form nl-shell's and stemma's immutability checks require). A
  comment after the value counts as part of it, which is how docs-immutability reads it
  too;
- two ADRs with the same number, or a first heading that isn't `# NNNN. Title`;
- in the table under `## Index`: an ADR missing or listed twice, a row for a file that
  doesn't exist, or a status that differs from the file (titles may differ: an index
  often carries a longer summary than the heading). One difference is allowed when the
  index's frontmatter says `supersession: index-only`, for an ADR process that freezes
  Accepted files (as wyrd's does): an Accepted ADR that a later one replaces keeps its file
  untouched and is shown `Superseded by NNNN` in the index only. Without that setting the
  old file's `status:` is stamped as well, and the index must match it;
- an ADR the index shows Superseded whose row names no successor or one that doesn't
  exist, whose successor's `supersedes:` doesn't name it back, or whose `superseded-by:`
  (or the successors its status names) differs from the index; and an Accepted file shown
  Superseded with no successor that was accepted (Accepted, or Superseded since), since
  only an accepted replacement supersedes it;
- an Accepted ADR whose `supersedes:` names an ADR the index doesn't show Superseded
  (the supersession records both sides in the same pull request).

Other sections of the index (ADR candidates, "Why not ...?") are not read.

Exit status: 0 = consistent (or no ADRs), 1 = problems (listed on stdout), 2 = git error.
"""

from __future__ import annotations

import posixpath
import re
import subprocess
import sys

ADR_DIR = "docs/design/adr"
INDEX = f"{ADR_DIR}/README.md"
NAME_RE = re.compile(r"^(\d{4})-[a-z0-9]+(?:-[a-z0-9]+)*\.md$")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
STATUSES = {"proposed": "Proposed", "accepted": "Accepted", "superseded": "Superseded"}
# `Superseded by ADR-0013`, `Superseded by ADR-0016 and ADR-0017`, `..., ADR-0017`.
SUPERSEDED_BY_RE = re.compile(r"superseded by (adr-\d{4}(?:\s*(?:,|and|, and)\s*adr-\d{4})*)",
                              re.IGNORECASE)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def frontmatter(text: str) -> dict[str, str] | None:
    """Top-level `key: value` lines of the YAML frontmatter, or None without one."""
    lines = text.split("\n")
    if not lines or lines[0].rstrip() != "---":
        return None
    fields: dict[str, str] = {}
    for line in lines[1:]:
        if line.rstrip() == "---":
            return fields
        m = re.match(r"^([A-Za-z_-]+):[ \t]*(.*)$", line)
        if m:
            fields[m[1].lower()] = m[2].rstrip()
    return None  # never closed


def heading(text: str) -> str | None:
    """The first `# ` heading after the frontmatter."""
    body = text.split("\n---", 1)[-1] if text.startswith("---") else text
    for line in body.split("\n"):
        if line.startswith("# "):
            return line[2:].strip()
    return None


def numbers(value: str) -> set[int]:
    """ADR numbers named in a `supersedes:` / `superseded-by:` value or a status cell."""
    return {int(n) for n in re.findall(r"\d{1,4}", value)}


def plain(cell: str) -> str:
    """A table cell without Markdown links, emphasis or code marks."""
    cell = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", cell)
    cell = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", cell)  # _emphasis_, not snake_case
    return re.sub(r"[*`]", "", cell).strip()


def index_rows(text: str) -> list[tuple[int, str, str]]:
    """(line, linked file, status) for each table row under `## Index`.

    The status is the column headed `Status`, or the last column when no header says so.
    """
    rows = []
    inside = False
    status_col = -1
    for n, line in enumerate(text.split("\n"), 1):
        if line.startswith("## "):
            inside = line[3:].strip().lower() == "index"
            status_col = -1
            continue
        if not inside or not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        link = LINK_RE.search(cells[0]) if cells else None
        if link is None:
            names = [plain(c).lower() for c in cells]
            if "status" in names:
                status_col = names.index("status")
            continue  # header or separator row
        if len(cells) < 3:
            continue
        status = cells[status_col] if status_col < len(cells) else ""
        rows.append((n, posixpath.normpath(link[1]), plain(status)))
    return rows


def check(root: str, tracked: list[str]) -> list[str]:
    problems: list[str] = []
    adrs: dict[str, dict] = {}  # file name -> parsed ADR
    by_number: dict[int, list[str]] = {}

    for path in sorted(p for p in tracked if posixpath.dirname(p) == ADR_DIR):
        name = posixpath.basename(path)
        if name == "README.md" or not name.endswith(".md"):
            continue
        m = NAME_RE.match(name)
        if not m:
            problems.append(f"{path}: not named NNNN-kebab-title.md")
            continue
        try:
            with open(posixpath.join(root, path), encoding="utf-8") as fh:
                text = fh.read()
        except FileNotFoundError:
            continue  # tracked but deleted in the working tree
        num = int(m[1])
        by_number.setdefault(num, []).append(name)
        fields = frontmatter(text)
        if fields is None:
            problems.append(f"{path}: no YAML frontmatter")
            continue
        if fields.get("type", "").lower() != "adr":
            problems.append(f"{path}: frontmatter `type:` is not `adr`")
        raw = fields.get("status", "")
        status = STATUSES.get(raw.lower())
        superseded_by = numbers(fields.get("superseded-by", ""))
        named = SUPERSEDED_BY_RE.fullmatch(raw)
        if named:
            status = "Superseded"
            in_status = numbers(named[1])
            if superseded_by and superseded_by != in_status:
                problems.append(f"{path}: `superseded-by:` differs from the ADRs `status:` names")
            superseded_by = in_status
        if status is None:
            problems.append(
                f"{path}: `status: {raw}` is not exactly Proposed, Accepted, Superseded or "
                "`Superseded by ADR-NNNN`"
                + (" (move the comment to its own line)" if "#" in raw else "")
            )
        title = heading(text)
        if title is None or not title.startswith(f"{m[1]}. "):
            problems.append(f"{path}: first heading is not `# {m[1]}. Title`")
        adrs[name] = {"path": path, "num": num, "status": status,
                      "supersedes": numbers(fields.get("supersedes", "")),
                      "superseded_by": superseded_by}

    for num, names in sorted(by_number.items()):
        if len(names) > 1:
            problems.append(f"{ADR_DIR}: number {num:04d} is used by {', '.join(names)}")

    if not adrs:
        return problems
    if INDEX not in tracked:
        return problems + [f"{INDEX}: missing; every ADR is listed there"]
    with open(posixpath.join(root, INDEX), encoding="utf-8") as fh:
        index_text = fh.read()
    rows = index_rows(index_text)
    # Whether this repo records a supersession in the index alone, leaving the frozen file as is.
    index_only_ok = ((frontmatter(index_text) or {}).get("supersession", "").lower()
                     == "index-only")

    by_num = {a["num"]: name for name, a in adrs.items()}
    listed: dict[str, int] = {}
    shown_as: dict[str, str | None] = {}  # file name -> status the index shows
    for line, target, status_cell in rows:
        where = f"{INDEX}:{line}"
        name = posixpath.basename(target)
        if posixpath.dirname(target) not in ("", ".") or name not in adrs:
            problems.append(f"{where}: row links to {target}, which is not an ADR in {ADR_DIR}/")
            continue
        if name in listed:
            problems.append(f"{where}: {name} is already listed on line {listed[name]}")
            continue
        listed[name] = line
        adr = adrs[name]
        words = status_cell.split()
        shown = STATUSES.get(words[0].lower().rstrip(".,:;")) if words else None
        shown_as[name] = shown
        # Where Accepted files are frozen, a supersession lives in the index alone.
        index_only = index_only_ok and adr["status"] == "Accepted" and shown == "Superseded"
        if adr["status"] and shown != adr["status"] and not index_only:
            problems.append(
                f"{where}: status `{status_cell}` differs from the file's `{adr['status']}`"
            )
        if adr["status"] == "Superseded" or shown == "Superseded":
            named = numbers(" ".join(words[1:]))
            if not named:
                problems.append(f"{where}: superseded ADR {adr['num']:04d} must name its "
                                "successor (`Superseded by NNNN`)")
            for n in sorted(named - by_num.keys()):
                problems.append(f"{where}: names successor {n:04d}, which does not exist")
            successors = named & by_num.keys()
            # A successor superseded in turn was accepted first, so it still counts.
            if index_only and successors and not any(
                    adrs[by_num[n]]["status"] in ("Accepted", "Superseded") for n in successors):
                problems.append(f"{where}: Accepted ADR {adr['num']:04d} is shown Superseded, "
                                "but no successor the row names has been accepted")
            for n in sorted(successors):
                if adr["num"] not in adrs[by_num[n]]["supersedes"]:
                    problems.append(f"{adrs[by_num[n]]['path']}: `supersedes:` does not name "
                                    f"{adr['num']:04d}, which the index says it replaces")
            if adr["superseded_by"] and named and adr["superseded_by"] != named:
                problems.append(f"{adr['path']}: the successors in `superseded-by:` or the status "
                                "differ from the index")

    for name, adr in sorted(adrs.items()):
        if name not in listed:
            problems.append(f"{adr['path']}: not listed in the {INDEX} index")
        if adr["status"] == "Accepted":
            for n in sorted(adr["supersedes"]):
                old = adrs.get(by_num.get(n, ""))
                if old is None:
                    problems.append(f"{adr['path']}: `supersedes:` names {n:04d}, which does not exist")
                elif shown_as.get(posixpath.basename(old["path"]), old["status"]) != "Superseded":
                    problems.append(f"{old['path']}: still {old['status']}, but Accepted "
                                    f"ADR {adr['num']:04d} supersedes it; "
                                    + ("show it" if index_only_ok else "stamp its `status:` and show it")
                                    + f" `Superseded by {adr['num']:04d}` in the index")
    return problems


def main() -> int:
    try:
        root = git("rev-parse", "--show-toplevel").strip()
        tracked = [p for p in git("-C", root, "ls-files", "-z").split("\0") if p]
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"adr_index: git failed: {exc}", file=sys.stderr)
        return 2
    problems = check(root, tracked)
    for line in problems:
        print(line)
    count = sum(1 for p in tracked if NAME_RE.match(posixpath.basename(p))
                and posixpath.dirname(p) == ADR_DIR)
    print(f"adr_index: {count} ADRs, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
