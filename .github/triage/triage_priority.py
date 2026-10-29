#!/usr/bin/env python3
"""Automated issue priority triage (ADR-0048 sections 4 and 5).

Two commands, run by two jobs of .github/workflows/triage-priority.yml, because GitHub
scopes the token's permissions per job, not per step:

    classify   reads issues, asks Claude once per issue, writes the plan as JSON.
               Runs with a read-only token; the only other credential is the Claude API
               identity token. It never writes to GitHub.
    apply      reads that plan, checks every label against the fixed sets in
               labels.json, and applies labels, an assignee and one comment. Runs with
               issues: write and never imports the Anthropic SDK or calls a model.

For each issue that has no `priority:*` label, the model returns a JSON verdict (type,
severity, reach, area, flags, the review priority of a deferred finding). The priority
is computed HERE from ADR-0048's table and overrides, never by the model. The model
has no tools. The script never sets a milestone, and withholds any reason that repeats
the issue's own text, so a log line, hostname or key pasted into a report is not copied
into the comment. A report that looks like an undisclosed vulnerability is labelled but
not restated (ADR-0048 section 4).

    triage_priority.py classify --issue 42 --out plan.json
    triage_priority.py classify --all --limit 20 --dry-run
    triage_priority.py apply --plan plan.json
Tests: python -I -m unittest discover -s .github/triage
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent

MODEL = "claude-sonnet-5-5"
# Classification: low effort is enough and keeps thinking short. max_tokens covers that
# thinking plus a ~300-token verdict; hitting it is handled as "could not classify".
EFFORT = "low"
MAX_TOKENS = 4096
# Input cap, the main cost lever. The cut is stated in the comment, never silent.
MAX_TITLE = 300
MAX_BODY = 6000
MAX_REASON = 200
MAX_COMMENT = 6000
MARKER = "<!-- wyrd-triage -->"
# Text the project template ships in prompt.md and labels.json until they are filled in.
TEMPLATE_MARKERS = ("PROJECT SUMMARY", "EXAMPLE, replace")
# Only the workflow's own comments count as triage comments; anyone can type the marker.
BOT_LOGIN = "github-actions[bot]"
# An issue left at needs-triage is triaged again only when it is edited, at most this often.
MAX_RETRIES = 2

TYPES = ("bug", "enhancement", "documentation", "research")
SEVERITIES = ("critical", "major", "minor", "trivial")
REACHES = ("common", "edge", "adversarial")
FLAGS = ("security", "durability_or_consistency")
REVIEW_PRIORITIES = ("none", "P0", "P1", "P2", "P3")
# ADR-0048 sets a priority for every type: for an enhancement, documentation or research
# issue, severity is how much it matters if it is not done (prompt.md).
PRIORITISED_TYPES = TYPES

# ADR-0048 section 2: severity x reach -> priority (0 = P0).
TABLE = {
    "critical": {"common": 0, "edge": 1, "adversarial": 1},
    "major": {"common": 1, "edge": 2, "adversarial": 2},
    "minor": {"common": 2, "edge": 3, "adversarial": 3},
    "trivial": {"common": 3, "edge": 3, "adversarial": 3},
}
# ADR-0048 section 3: each override is a floor (the priority is at least this). A flag
# comes from the model; a label is one the issue already has.
FLOORS = {
    "security": (1, "security bug"),
    "durability_or_consistency": (1, "durability or consistency bug"),
}
LABEL_FLOORS = {
    "release-gating": (1, "`release-gating` label"),
}


def load_config(path: Path = HERE / "labels.json") -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def area_names(config: dict) -> list[str]:
    return [name.removeprefix("area:") for name in config["area"]]


def allowed_labels(config: dict) -> set[str]:
    """Every label the bot may add: the types plus the configured groups."""
    names = set(TYPES)
    for group in ("severity", "reach", "area", "priority", "other"):
        names |= set(config[group])
    return names


def verdict_schema(areas: list[str]) -> dict:
    """JSON schema for the model's verdict (structured output)."""
    # Without a description per field, low effort fills each reason with a single word.
    about = {
        "type": "why it is a bug, an enhancement, documentation or research",
        "severity": "what goes wrong for the user and why that is this severity",
        "reach": "who runs into it and why that is this reach",
        "area": "which part of Wyrd it is in",
        "overrides": "which overrides apply and why, or that none apply",
    }
    reasons = {k: {"type": "string",
                   "description": f"One plain sentence of 8 to 30 words: {v}. Never quote the issue."}
               for k, v in about.items()}
    return {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": list(TYPES)},
            "severity": {"type": "string", "enum": list(SEVERITIES)},
            "reach": {"type": "string", "enum": list(REACHES)},
            "areas": {"type": "array", "items": {"type": "string", "enum": areas}},
            "flags": {
                "type": "object",
                "properties": {f: {"type": "boolean"} for f in FLAGS},
                "required": list(FLAGS),
                "additionalProperties": False,
            },
            "review_priority": {"type": "string", "enum": list(REVIEW_PRIORITIES)},
            "unsure": {"type": "boolean"},
            "missing": {"type": "string"},
            "secret_in_issue": {"type": "boolean"},
            "vulnerability_report": {"type": "boolean"},
            "reasons": {
                "type": "object",
                "properties": reasons,
                "required": list(reasons),
                "additionalProperties": False,
            },
        },
        "required": ["type", "severity", "reach", "areas", "flags", "review_priority", "unsure",
                     "missing", "secret_in_issue", "vulnerability_report", "reasons"],
        "additionalProperties": False,
    }


class InvalidVerdict(ValueError):
    pass


def validate(verdict: dict, areas: list[str]) -> dict:
    """Check the verdict against the fixed label sets. Structured output should already
    guarantee this; it is checked again because nothing the model returns is trusted."""
    try:
        if verdict["type"] not in TYPES:
            raise InvalidVerdict(f"type {verdict['type']!r}")
        if verdict["severity"] not in SEVERITIES:
            raise InvalidVerdict(f"severity {verdict['severity']!r}")
        if verdict["reach"] not in REACHES:
            raise InvalidVerdict(f"reach {verdict['reach']!r}")
        if verdict["review_priority"] not in REVIEW_PRIORITIES:
            raise InvalidVerdict(f"review_priority {verdict['review_priority']!r}")
        chosen = [a for a in verdict["areas"] if a in areas]
        if len(chosen) != len(verdict["areas"]):
            raise InvalidVerdict(f"areas {verdict['areas']!r}")
        flags = {f: verdict["flags"][f] is True for f in FLAGS}
        reasons = {k: str(verdict["reasons"][k]) for k in ("type", "severity", "reach", "area", "overrides")}
        return {
            "type": verdict["type"],
            "severity": verdict["severity"],
            "reach": verdict["reach"],
            "areas": list(dict.fromkeys(chosen))[:2],
            "flags": flags,
            "review_priority": verdict["review_priority"],
            "unsure": verdict["unsure"] is True,
            "missing": str(verdict["missing"]),
            "secret_in_issue": verdict["secret_in_issue"] is True,
            "vulnerability_report": verdict["vulnerability_report"] is True,
            "reasons": reasons,
        }
    except (KeyError, TypeError) as e:
        raise InvalidVerdict(f"missing or malformed field: {e}") from e


def compute_priority(severity: str, reach: str, flags: dict[str, bool], review_priority: str,
                     existing_labels: set[str]) -> tuple[int, int, list[str]]:
    """ADR-0048 sections 2 and 3. Returns (priority, table_priority, notes).

    ADR-0048 section 3 gives no order for the overrides; this one applies the table, then
    a deferred review finding keeps the review's priority, then the floors, so a floor
    still holds for a deferred finding."""
    base = TABLE[severity][reach]
    prio, notes = base, []
    if review_priority != "none":
        kept = int(review_priority[1])
        if kept != prio:
            notes.append(f"kept the deferred review's P{kept}")
        prio = kept
    floors = [FLOORS[k] for k, on in flags.items() if on and k in FLOORS]
    floors += [LABEL_FLOORS[name] for name in sorted(existing_labels) if name in LABEL_FLOORS]
    for floor, label in sorted(set(floors)):
        if floor < prio:
            prio = floor
            notes.append(f"{label} (at least P{floor})")
    return prio, base, notes


# --- never echo the issue back ------------------------------------------------------------

_WORD = re.compile(r"[\w'-]+", re.UNICODE)
SHINGLE = 5


def _words(text: str) -> list[str]:
    return [w.lower() for w in _WORD.findall(text)]


def _shingles(words: list[str], n: int = SHINGLE) -> set[tuple[str, ...]]:
    return {tuple(words[i:i + n]) for i in range(len(words) - n + 1)}


IDENT_MIN = 8
_IDENT_MARK = re.compile(r"[\d./_:=@+]")
_EDGE = ".,;:!?()[]{}<>\"'`*"


def _identifiers(text: str) -> set[str]:
    """Single tokens that look like keys, hostnames, paths or IDs: IDENT_MIN characters or
    more with a digit, one of . / _ : = @ +, or two hyphens (a bucket or host name). A
    word with one hyphen ("read-only") does not count."""
    tokens = (t.strip(_EDGE).lower() for t in text.split())
    return {t for t in tokens
            if len(t) >= IDENT_MIN and (_IDENT_MARK.search(t) or t.count("-") >= 2)}


def echoes(reason: str, issue_text: str) -> bool:
    """True when the reason repeats the issue: a run of SHINGLE words, a number of three
    or more digits (years, dates, identifiers), or one identifier-like token such as a
    key or hostname, that also appears in the issue."""
    if _shingles(_words(reason)) & _shingles(_words(issue_text)):
        return True
    issue_numbers = set(re.findall(r"\d{3,}", issue_text))
    if any(n in issue_numbers for n in re.findall(r"\d{3,}", reason)):
        return True
    return bool(_identifiers(reason) & _identifiers(issue_text))


def scrub(reason: str, issue_text: str) -> str:
    """Make a model-written reason safe to post: no echo of the issue, no mentions, no
    Markdown or HTML that could change the comment, one line, bounded length."""
    if echoes(reason, issue_text):
        return "(reason withheld: it repeated text from the issue)"
    text = " ".join(reason.split())
    text = re.sub(r"[<>`|*_\[\]#]", "", text)
    text = text.replace("@", "@​")
    if len(text) > MAX_REASON:
        text = text[:MAX_REASON - 1].rstrip() + "…"
    # A word or two ("major", "bug") is a label name, not a reason.
    return text if len(text.split()) >= 3 else "(no reason given)"


# --- the plan: what to write to the issue -------------------------------------------------

@dataclass
class Issue:
    number: int
    title: str
    body: str
    labels: set[str]
    assignees: list[str]
    state: str = "open"
    is_pr: bool = False
    author_is_bot: bool = False
    triage_count: int = 0  # the workflow's own triage comments on the issue


@dataclass
class Plan:
    number: int = 0
    add_labels: list[str] = field(default_factory=list)
    remove_labels: list[str] = field(default_factory=list)
    assignee: str | None = None
    comment: str = ""


def skip_reason(issue: Issue, retry: bool = False) -> str | None:
    """Why ``issue`` is not triaged, or None. ``retry`` is True only for an edit to the
    issue: the one event that may bring the information a needs-triage issue lacked."""
    if issue.is_pr:
        return "is a pull request"
    if issue.state != "open":
        return "is closed"
    if issue.author_is_bot:
        return "was opened by a bot"
    if any(label.startswith("priority:") for label in issue.labels):
        return "already has a priority"
    # One automated pass per issue. A back-fill or a reopen never triages an issue again,
    # so it cannot re-spend tokens or post the same comment twice; only an edit retries
    # one left at needs-triage, and at most MAX_RETRIES times.
    if issue.triage_count:
        if not retry or "needs-triage" not in issue.labels:
            return "was already triaged"
        if issue.triage_count > MAX_RETRIES:
            return "was retried too often"
    return None


def issue_input(issue: Issue) -> tuple[str, bool]:
    """The user message: the issue as delimited data, capped. Returns (text, truncated)."""
    title = issue.title[:MAX_TITLE]
    body = issue.body or ""
    truncated = len(body) > MAX_BODY
    body = body[:MAX_BODY]
    note = f"\n[The description was cut at {MAX_BODY} characters.]" if truncated else ""
    return f"<issue>\n<title>{title}</title>\n<body>\n{body}\n</body>\n</issue>{note}", truncated


def _row(label: str, why: str) -> str:
    return f"| `{label}` | {why} |"


def build_plan(issue: Issue, verdict: dict, config: dict, *, model: str, usage: str,
               truncated: bool) -> Plan:
    issue_text = f"{issue.title}\n{issue.body or ''}"
    if verdict["vulnerability_report"]:
        # Labels yes, explanation no: a public comment must not restate the details.
        reasons = {k: "Not restated here; see the note below." for k in verdict["reasons"]}
    else:
        reasons = {k: scrub(v, issue_text) for k, v in verdict["reasons"].items()}
    plan, rows = Plan(number=issue.number), []

    have_type = [t for t in TYPES if t in issue.labels]
    kind = have_type[0] if have_type else verdict["type"]
    if have_type:
        # The label wins, but a disagreement is worth a line: templates set the type.
        why = "Kept the type the issue already had."
        if verdict["type"] != have_type[0]:
            why += f" It reads more like {verdict['type']}: {reasons['type']}"
        rows.append(_row(have_type[0], why))
    else:
        plan.add_labels.append(verdict["type"])
        rows.append(_row(verdict["type"], reasons["type"]))

    for area in verdict["areas"]:
        plan.add_labels.append(f"area:{area}")
    if verdict["areas"]:
        rows.append(_row(", ".join(f"area:{a}" for a in verdict["areas"]).replace(", ", "`, `"),
                         reasons["area"]))

    if kind not in PRIORITISED_TYPES:
        rows.append(_row("no priority", f"ADR-0048 does not define a priority for {kind} "
                                        "issues; a maintainer sets it if needed."))
    elif verdict["unsure"]:
        plan.add_labels.append("needs-triage")
        missing = ("not restated" if verdict["vulnerability_report"]
                   else scrub(verdict["missing"], issue_text))
        rows.append(_row("needs-triage", f"Severity or reach could not be placed. Missing: {missing}"))
    else:
        prio, base, notes = compute_priority(verdict["severity"], verdict["reach"],
                                             verdict["flags"], verdict["review_priority"],
                                             issue.labels)
        if "needs-triage" in issue.labels:
            plan.remove_labels.append("needs-triage")
        plan.add_labels += [f"sev:{verdict['severity']}", f"reach:{verdict['reach']}",
                            f"priority:P{prio}"]
        rows.append(_row(f"sev:{verdict['severity']}", reasons["severity"]))
        rows.append(_row(f"reach:{verdict['reach']}", reasons["reach"]))
        why = f"{verdict['severity']} × {verdict['reach']} is P{base} in the ADR-0048 table."
        if notes:
            why += " Then: " + "; ".join(notes) + f". {reasons['overrides']}"
        rows.append(_row(f"priority:P{prio}", why))

    if not issue.assignees:
        owners = config.get("owners", {})
        area = verdict["areas"][0] if verdict["areas"] else ""
        plan.assignee = owners.get(f"area:{area}") or owners.get("*")

    lines = [
        MARKER,
        "**Automated triage** under ADR-0048. A maintainer makes the final call: change any "
        "label to correct it. The milestone is left to the maintainer.",
        "",
        "| Label | Why |",
        "|---|---|",
        *rows,
    ]
    if verdict["vulnerability_report"]:
        lines += ["", "This issue may describe a security vulnerability. Please report "
                      "vulnerabilities privately as SECURITY.md describes; a maintainer may move "
                      "this report to a private advisory."]
    if verdict["secret_in_issue"]:
        lines += ["", "This issue seems to contain a credential or other secret. Please rotate "
                      "it and edit it out of the issue; editing alone does not remove it from "
                      "the issue's history."]
    if truncated:
        lines += ["", f"Only the first {MAX_BODY:,} characters of the description were read."]
    lines += ["", f"<sub>{model} · {usage}</sub>"]
    plan.comment = "\n".join(lines) + "\n"
    return plan


def unclassified_plan(issue: Issue, why: str, *, model: str, usage: str) -> Plan:
    """When the model declined or ran out of room: flag for a human, guess nothing."""
    return Plan(number=issue.number, add_labels=["needs-triage"], comment="\n".join([
        MARKER,
        f"**Automated triage** could not classify this issue ({why}). A maintainer will "
        "triage it by hand under ADR-0048.",
        "",
        f"<sub>{model} · {usage}</sub>",
    ]) + "\n")


# --- the plan handed from the classify job to the apply job -------------------------------

class InvalidPlan(ValueError):
    pass


def check_plan(entry: dict, config: dict) -> Plan:
    """The apply job's own check of one plan entry. The plan comes from another job, so it
    is data: every label must be one the bot may add, with at most one of each kind, and
    the comment must be the bot's own."""
    try:
        number = entry["number"]
        labels = entry["add_labels"]
        assignee = entry["assignee"]
        remove = entry["remove_labels"]
        comment = entry["comment"]
    except (KeyError, TypeError) as e:
        raise InvalidPlan(f"missing field: {e}") from e
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        raise InvalidPlan(f"issue number {number!r}")
    if not isinstance(labels, list) or not all(isinstance(l, str) for l in labels):
        raise InvalidPlan("labels are not a list of strings")
    unknown = set(labels) - allowed_labels(config)
    if unknown:
        raise InvalidPlan(f"labels outside the scheme: {sorted(unknown)}")
    for prefix in ("sev:", "reach:", "priority:"):
        if sum(l.startswith(prefix) for l in labels) > 1:
            raise InvalidPlan(f"more than one {prefix} label")
    if sum(l in TYPES for l in labels) > 1:
        raise InvalidPlan("more than one type label")
    if sum(l.startswith("area:") for l in labels) > 2:
        raise InvalidPlan("more than two area labels")
    if not isinstance(remove, list) or not set(remove) <= {"needs-triage"}:
        raise InvalidPlan(f"may only remove needs-triage, not {remove!r}")
    if assignee is not None and assignee not in set(config.get("owners", {}).values()):
        raise InvalidPlan(f"assignee {assignee!r} is not in the owner table")
    if not isinstance(comment, str) or not comment.startswith(MARKER) or len(comment) > MAX_COMMENT:
        raise InvalidPlan("comment is not the bot's own")
    return Plan(number=number, add_labels=list(labels), remove_labels=list(remove),
                assignee=assignee, comment=comment)


def write_plans(path: Path, plans: list[Plan]) -> None:
    path.write_text(json.dumps([asdict(p) for p in plans], indent=2) + "\n", encoding="utf-8")


def read_plans(path: Path, config: dict) -> list[Plan]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise InvalidPlan("the plan file is not a list")
    return [check_plan(entry, config) for entry in data]


# --- Claude -------------------------------------------------------------------------------

@dataclass
class Result:
    verdict: dict | None
    failure: str | None
    model: str
    usage: str


def classify_issue(client, prompt: str, issue: Issue, areas: list[str]) -> tuple[Result, bool]:
    text, truncated = issue_input(issue)
    response = client.beta.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        # Server-side fallback: a cyber-category decline (security reports can trigger
        # one) is retried on Anthropic's recommended model instead of failing.
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        output_config={"effort": EFFORT,
                       "format": {"type": "json_schema", "schema": verdict_schema(areas)}},
        # The rules are identical on every call; caching pays off during a back-fill run.
        system=[{"type": "text", "text": prompt, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": text}],
    )
    u = response.usage
    usage = (f"{u.input_tokens + (u.cache_read_input_tokens or 0) + (u.cache_creation_input_tokens or 0):,}"
             f" tokens in ({u.cache_read_input_tokens or 0:,} cached), {u.output_tokens:,} out")
    model = response.model
    if response.stop_reason == "refusal":
        return Result(None, "the model declined to classify it", model, usage), truncated
    if response.stop_reason == "max_tokens":
        return Result(None, "the model ran out of output room", model, usage), truncated
    raw = next((b.text for b in response.content if b.type == "text"), "")
    try:
        verdict = validate(json.loads(raw), areas)
    except (json.JSONDecodeError, InvalidVerdict) as e:
        print(f"#{issue.number}: invalid verdict: {e}", file=sys.stderr)
        return Result(None, "the model's answer did not validate", model, usage), truncated
    return Result(verdict, None, model, usage), truncated


# --- GitHub (via gh) ----------------------------------------------------------------------

def gh(*args: str, input: str | None = None) -> str:
    try:
        return subprocess.run(["gh", *args], input=input, capture_output=True, text=True,
                              check=True).stdout
    except subprocess.CalledProcessError as e:
        # The traceback alone shows only the command; GitHub's reason is on stderr.
        print(f"gh {args[0]} {args[1] if len(args) > 1 else ''} failed: {e.stderr.strip()}",
              file=sys.stderr)
        raise


def triage_comments(comments: list[dict]) -> int:
    """How many of ``comments`` are the workflow's own triage comments."""
    return sum(1 for c in comments
               if MARKER in (c.get("body") or "") and (c.get("user") or {}).get("login") == BOT_LOGIN)


def fetch_issue(repo: str, number: int) -> Issue:
    data = json.loads(gh("api", f"repos/{repo}/issues/{number}"))
    comments = json.loads(gh("api", "--paginate", "--slurp", f"repos/{repo}/issues/{number}/comments"))
    flat = [c for page in comments for c in page]
    return Issue(
        number=number,
        title=data.get("title") or "",
        body=data.get("body") or "",
        labels={label["name"] for label in data.get("labels", [])},
        assignees=[a["login"] for a in data.get("assignees", [])],
        state=data.get("state", "open"),
        is_pr="pull_request" in data,
        author_is_bot=(data.get("user") or {}).get("type") == "Bot",
        triage_count=triage_comments(flat),
    )


def open_untriaged(repo: str, limit: int) -> list[int]:
    """Up to ``limit`` open issues a back-fill should triage, oldest first. The limit is
    applied after skipping, so issues that stay without a priority after their one
    automated pass (needs-triage, or a type with no priority) never fill a batch."""
    data = json.loads(gh("issue", "list", "-R", repo, "--state", "open", "--limit", "500",
                         "--json", "number,labels"))
    candidates = sorted(i["number"] for i in data
                        if not any(l["name"].startswith("priority:") for l in i["labels"]))
    numbers = []
    for number in candidates:
        if len(numbers) == limit:
            break
        if skip_reason(fetch_issue(repo, number)) is None:
            numbers.append(number)
    return numbers


def ensure_labels(repo: str, config: dict) -> None:
    existing = {l["name"] for l in json.loads(gh("label", "list", "-R", repo, "--limit", "500",
                                                 "--json", "name"))}
    for group in ("severity", "reach", "area", "priority", "other"):
        for name, spec in config[group].items():
            if name not in existing:
                gh("label", "create", name, "-R", repo, "--color", spec["color"],
                   "--description", spec["description"])
                print(f"created label {name}")


def apply_plan(repo: str, plan: Plan) -> None:
    # Re-read the issue: a human may have set a priority since the classify job read it.
    # The classify job already decided whether a retry was due, so allow it here.
    fresh = fetch_issue(repo, plan.number)
    if (why := skip_reason(fresh, retry=True)):
        print(f"#{plan.number}: skipped at apply time ({why})")
        return
    if plan.add_labels:
        gh("issue", "edit", str(plan.number), "-R", repo, "--add-label", ",".join(plan.add_labels))
    if plan.remove_labels:
        gh("issue", "edit", str(plan.number), "-R", repo, "--remove-label", ",".join(plan.remove_labels))
    if plan.assignee and not fresh.assignees:
        try:
            gh("issue", "edit", str(plan.number), "-R", repo, "--add-assignee", plan.assignee)
        except subprocess.CalledProcessError as e:
            print(f"#{plan.number}: could not assign {plan.assignee}: {e.stderr.strip()}",
                  file=sys.stderr)
    gh("issue", "comment", str(plan.number), "-R", repo, "--body-file", "-", input=plan.comment)


# --- commands -----------------------------------------------------------------------------

def load_prompt(path: Path = HERE / "prompt.md") -> str:
    """The prompt with its maintainer notes removed; refuse one still holding template text."""
    text = path.read_text(encoding="utf-8")
    if any(marker in text for marker in TEMPLATE_MARKERS):
        raise SystemExit("prompt.md still has template text (PROJECT SUMMARY or EXAMPLE); "
                         "fill it in before running triage")
    return re.sub(r"<!--.*?-->\n?", "", text, flags=re.S)


def run_classify(args) -> int:
    import anthropic  # here, so the apply job and the tests need no SDK installed

    config = load_config()
    if any("EXAMPLE" in spec["description"] for spec in config["area"].values()):
        raise SystemExit("labels.json still has the template's example area; replace it first")
    areas = area_names(config)
    prompt = load_prompt()
    client = anthropic.Anthropic(max_retries=3, timeout=120)

    numbers = open_untriaged(args.repo, args.limit) if args.all else [args.issue]
    if not numbers:
        print("no open issues without a priority")
    plans, failed = [], 0
    for number in numbers:
        issue = fetch_issue(args.repo, number)
        if (why := skip_reason(issue, retry=args.retry)):
            print(f"#{number}: skipped ({why})")
            continue
        try:
            result, truncated = classify_issue(client, prompt, issue, areas)
        except anthropic.RateLimitError as e:
            print(f"#{number}: rate limited, stopping: {e.message}", file=sys.stderr)
            failed += 1
            break
        except anthropic.APIStatusError as e:
            print(f"#{number}: API error {e.status_code}: {e.message}", file=sys.stderr)
            failed += 1
            continue
        except anthropic.APIConnectionError as e:
            print(f"#{number}: connection error: {e}", file=sys.stderr)
            failed += 1
            continue
        if result.verdict is None:
            plan = unclassified_plan(issue, result.failure, model=result.model, usage=result.usage)
        else:
            plan = build_plan(issue, result.verdict, config, model=result.model,
                              usage=result.usage, truncated=truncated)
        print(f"#{number}: {', '.join(plan.add_labels)}; assignee {plan.assignee}; {result.usage}")
        if args.dry_run:
            print(plan.comment)
        plans.append(plan)
    # Whatever was classified is handed on, even when a later issue failed.
    if args.out and not args.dry_run:
        write_plans(Path(args.out), plans)
    return 1 if failed else 0


def run_apply(args) -> int:
    config = load_config()
    plans = read_plans(Path(args.plan), config)
    if not plans:
        print("nothing to apply")
        return 0
    ensure_labels(args.repo, config)
    for plan in plans:
        apply_plan(args.repo, plan)
        print(f"#{plan.number}: applied {', '.join(plan.add_labels)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    sub = ap.add_subparsers(dest="command", required=True)
    c = sub.add_parser("classify", help="ask the model; write the plan, never GitHub")
    target = c.add_mutually_exclusive_group(required=True)
    target.add_argument("--issue", type=int)
    target.add_argument("--all", action="store_true", help="open issues without a priority")
    c.add_argument("--limit", type=int, default=20, help="cap for --all")
    c.add_argument("--dry-run", action="store_true", help="print the plan, write no file")
    c.add_argument("--out", help="where to write the plan (JSON)")
    c.add_argument("--retry", action="store_true",
                   help="the issue was edited: retry it if it is at needs-triage")
    a = sub.add_parser("apply", help="check the plan and write it to GitHub; no model")
    a.add_argument("--plan", required=True)
    args = ap.parse_args(argv)
    if not args.repo:
        ap.error("--repo or $GITHUB_REPOSITORY is required")
    return run_classify(args) if args.command == "classify" else run_apply(args)


if __name__ == "__main__":
    sys.exit(main())
