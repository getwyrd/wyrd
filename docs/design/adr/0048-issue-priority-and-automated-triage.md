---
created: 22.10.2026 23:50
type: adr
status: Proposed
tags:
  - adr
  - governance
  - triage
---
# 0048. Issue priority and automated triage: severity and reach set P0–P3, and a GitHub Action applies it with one Claude API call per issue

## Context

Wyrd has over two hundred open issues, from the maintainer, from review findings deferred out of pull requests, from the PDCA loop (wyrd-pdca) and from the nightly quality jobs, which file their own failures. An issue gets `bug`, `enhancement`, `documentation` or `research`, and the `new` label while it has no milestone. Nothing records how urgent it is, so ordering the backlog means rereading issues one by one.

Triage needs the rules written down, so that two people, or the same person on different days, put the same issue at the same priority, and so that a bot can apply them with the maintainer making the final call. The structure is the one Stemma's issue-priority and automated-triage decisions and nl-shell already use: severity and reach give a priority from a fixed table, and a short list of overrides moves some issues up. The severity meanings, area labels and overrides below are Wyrd's own, taken from what Wyrd promises and what its threat model protects (architecture §14).

Four things shape the scheme and how it is run.

- **What Wyrd promises.** Its reason to exist is that a write is atomic and, once acknowledged, durable (architecture §5.1, ADR-0015). A lost acknowledged write, a torn write that becomes visible, or a chunk that cannot be reconstructed breaks that promise outright. Such bugs mostly need a combination of faults and timing that normal use rarely produces, so a plain impact-times-likelihood table would rank them low; the deterministic simulation exists precisely because these are the bugs that matter (ADR-0009).
- **What an attacker can reach.** The threat model's attack catalog (§14.5) lists attacks that only happen when someone sets them up: a crafted fragment, a compromised D server, a hostile tenant. They are rare by construction and still the attacks the design is built against.
- **The repository is public.** Anyone can open an issue, so whatever reads issues reads text written by strangers, and whatever an issue triggers, a stranger can trigger. Issue text is already public, so sending it to a model provider exposes little that was not already exposed. But a report can carry a credential pasted from a log, or describe a vulnerability that belongs in private disclosure (SECURITY.md).
- **Whoever sets the milestone starts the work.** The PDCA loop picks up issues on the open milestone, so the milestone must stay a human decision.

## Decision

We will qualify every issue with three labels, derive a priority from two of them with a fixed table, and apply a short list of overrides (sections 1–3). Section 4 says what an automated triager may and may not do, and section 5 how ours runs.

### 1. Labels describing the issue

**Severity: how bad it is when it happens**

| Label | Meaning |
|---|---|
| `sev:critical` | An acknowledged write is lost; a torn or half-committed write becomes visible, or a stale or lost write is read back as current (ADR-0015); a chunk can no longer be reconstructed; stored bytes are silently altered; data or keys reach a principal not entitled to them, across tenants included (ADR-0021, ADR-0022); authentication or authorisation is bypassed; crafted input reaches code execution or memory corruption |
| `sev:major` | A safety mechanism is off or bypassable without data loss yet: checksum verification, fencing, lease or grace-window checks, admission control, mTLS without fallback (ADR-0006, ADR-0019, ADR-0025); a server crashes or panics; scrub, repair or GC fails or stops silently; fault tolerance is below the configured *k+m*; an on-disk or wire format diverges from its spec; an S3 operation the gateway claims to support is wrong with no workaround |
| `sev:minor` | Wrong behaviour with a workaround, a performance or resource regression, or misleading telemetry or errors |
| `sev:trivial` | Cosmetic, wording, docs, or a test-only gap |

**Reach: how likely someone hits it**

| Label | Meaning |
|---|---|
| `reach:common` | Normal operation with a default or documented configuration triggers it |
| `reach:edge` | Needs an unusual configuration, workload, fault combination or timing |
| `reach:adversarial` | Only triggers when someone sets it up on purpose: a hostile client or tenant, crafted fragments or requests, a compromised D server or internal node (§14.3) |

**Area: where the issue lives** (for routing and assignment)

Following the components of the building block view (architecture §5.1, §5.3): `area:metadata` (the metadata store and its backends), `area:client` (erasure coding, commit, read), `area:dserver` (D servers and the `ChunkStore` seam), `area:custodian` (GC, scrub, reconstruct, rebalance), `area:replication`, `area:coordination`, `area:gateway` (L1, S3), `area:chunk-format` (the on-disk format and its spec), `area:proto`, `area:security` (identity, mTLS, CA, KMS and access control), `area:telemetry`, `area:testkit` (simulation and the conformance suites), `area:validate`, `area:server` (composition and the binary), `area:deploy`, plus `area:docs` and `area:ci`. An issue gets one or two.

### 2. Priority from severity and reach

| | common | edge | adversarial |
|---|---|---|---|
| **critical** | P0 | P1 | P1 |
| **major** | P1 | P2 | P2 |
| **minor** | P2 | P3 | P3 |
| **trivial** | P3 | P3 | P3 |

- **P0:** drop other work, fix now, block the release.
- **P1:** goes on the current milestone.
- **P2:** next milestone, or whenever it fits.
- **P3:** backlog.

The labels are `priority:P0` to `priority:P3`, the same names as in Stemma, so a priority means the same in both repositories. P1 and P2 line up roughly with Codex's review badges, so the meaning is the same whether a review finding or a bug report produced the issue.

### 3. Overrides

- **Security bugs get at least P1, even when `reach:adversarial`.** The attacks in the threat model's catalog (§14.5) all need someone to set them up; that is what makes them attacks.
- **Durability and consistency bugs get at least P1, even when `reach:edge`.** Anything that can lose an acknowledged write, make a torn write visible, read a stale write as current, or leave a chunk unrecoverable. Fault combinations are edge cases by nature, and they are what Wyrd exists to survive.
- **`release-gating` means at least P1**, whatever the table says.
- **Deferred review findings** keep the review's priority, unless the triager can show the pull request changed the impact.
- **When unsure, leave the priority off.** If the triager cannot place severity or reach, they set `needs-triage` and say what is missing. Never guess a lower priority.

### 4. Automated triage

A bot that triages does this on each new issue:

1. Set type (`bug`, `enhancement`, `documentation` or `research`) unless one is already set, plus severity, reach, area and priority. The issue templates set `bug` or `enhancement`, so most issues arrive typed; the bot keeps that label, and says in its comment when the issue reads as a different type.
2. Post one comment with a line of reasoning per label, so the maintainer can check the call quickly and correct it by changing a label.
3. Assign from a fixed owner table by area, unless someone is already assigned.
4. Leave the milestone empty. The `new` label therefore stays until the maintainer sets one.

Issue text is untrusted input. The bot's job MUST hold only `contents: read`, `issues: write` and `id-token: write`; it gets no write access to code and no secret beyond its own short-lived tokens. GitHub has no narrower permission than `issues: write`, and that one allows more than the bot uses: it can also edit an issue's title and body, close, reopen or lock it, set its milestone, and edit or delete comments. The bot's code calls only the label, assignee and comment endpoints, with arguments it builds itself, so the model's answer cannot reach the rest. The wider capability is reachable only by compromising the job's code or its dependencies, which are hash-pinned (section 5). Such a compromise could rewrite, close or re-milestone issues, which shows in each issue's history and can be undone; it cannot touch code, releases or secrets. A milestone set this way would start the PDCA loop on that issue, behind the loop's own review gates. What the model returns MUST be validated against the label sets before anything is written, and the priority MUST be computed by code from sections 2 and 3, not taken from the model.

The bot MUST NOT quote the issue in its comment: no log lines, hostnames, keys or identifiers, only descriptions in general terms. When an issue appears to contain a credential or other secret, the comment SHOULD say so and ask the reporter to rotate it and edit it out. When an issue appears to report an undisclosed vulnerability, the bot still sets the labels, but its comment MUST NOT restate the details; it points to SECURITY.md so the maintainer can move the report to a private advisory.

The bot skips pull requests, closed issues, issues opened by bots (the nightly jobs file theirs with their own `nightly-failure` label), and issues that already carry a priority.

### 5. How the bot runs

We will run triage as a GitHub Action, `.github/workflows/triage-priority.yml`, with a Python script, `.github/triage/triage_priority.py`. It runs when an issue is opened, reopened or edited and has no `priority:*` label, and on demand to back-fill open issues.

- **One request per issue.** Claude Sonnet 5.5 at low effort, with no tools, structured JSON output, and the issue title and description capped at fixed lengths; the comment says when it cut the text. The static rules are sent as a cached system prompt, so a run of issues pays for them mostly at the cache rate. An issue is classified once; an edit triggers a new call only while it carries `needs-triage`.
- **The model classifies; the script decides.** The model returns type, severity, reach, area, the override flags, the secret and vulnerability flags, and one reason per label. Each reason field in the output schema says what it should explain, because a bare string field is sometimes filled with a single word. The script validates the answer, computes the priority, and applies labels, assignee and comment with the workflow's own `GITHUB_TOKEN`; `id-token: write` lets the job fetch its identity token and nothing more. A reason that repeats from the issue a run of five words, a number of three or more digits, or any single token that looks like an identifier (eight characters or more, containing a digit, one of `. / _ : = @ +`, or two hyphens, such as a key, hostname, path or bucket name; a word with one hyphen does not count) is withheld whole, so a copied secret never reaches the comment and does not outlive the reporter's edit, one under three words is shown as "(no reason given)", and mentions and markup are neutralised.
- **A decline or a malformed answer sets `needs-triage`** and a short comment; nothing is guessed. A safety-classifier decline, which a security-heavy report can trip, is first retried server-side on Anthropic's recommended fallback model.
- **No stored API key.** The job authenticates by Workload Identity Federation: it fetches GitHub's OIDC identity token (audience `https://api.anthropic.com`) and the Anthropic SDK exchanges it for a Claude API token that acts as a dedicated service account. The federation rule accepts only subject `repo:getwyrd/wyrd:ref:refs/heads/main` and requires the claims `repository_owner_id: 293332526`, `repository_id: 1268237817` and `ref: refs/heads/main`. The numeric IDs matter because the default subject format names the repository, and a renamed or re-created repository could otherwise reuse the name. That covers issue events and manual runs from main, and no pull request, fork or other branch. The rule grants `workspace:developer` for 600 seconds; GitHub's token can be exchanged once, so a run must finish within about eight minutes. The rule, organisation, service-account and workspace IDs are repository variables, not secrets, and the workflow does nothing until they are set. A manual run with `check` ticked makes one 16-token request to test the sign-in.
- **Cost is capped outside the bot as well as inside it.** Anyone can open an issue on a public repository, so the service account acts in a workspace with its own spending limit; that limit, not the script, is the backstop against a flood of issues. Each comment states its token use.
- **Dependencies are pinned and hash-checked.** The SDK and everything it pulls in are compiled with hashes and installed with `--require-hashes`, each version at least two weeks old when chosen. This is the supply-chain care that ADR-0003 and the `cargo-deny` wall give Rust crates, applied to the one Python tool that holds a credential.
- **The label set lives in `.github/triage/labels.json`**, with descriptions under GitHub's 100-character limit, and the workflow creates missing labels.
- **Back-fill is batched and checked first.** The open issues are triaged by manual runs in batches with a limit, the first as a dry run that prints each plan without writing.

We reject running a model on a subscription in the Action (Codex on a ChatGPT plan, for example): its credential is a whole personal account, exposed to every issue author's prompt injection. We reject a stored `ANTHROPIC_API_KEY` secret: it lasts until someone revokes it, while a federated token expires within minutes and works only from this repository's main branch. We reject an agent that reads the code to triage: one judgement per issue does not need it, and reading code is what would make the cost grow.

### 6. The threat model's catalog run through it

As a check of the scheme rather than real triage, here are attacks from the catalog (§14.5) classified as if each had been reported as a bug, plus ordinary ones:

| Issue | Severity / reach | Priority |
|---|---|---|
| A PUT is acknowledged before its commit, and a crash loses it | critical / edge, plus the durability override | P1 |
| GC reclaims a fragment a reader still references | critical / edge, plus the durability override | P1 |
| A crafted fragment with an oversized payload length overruns the reader | critical / adversarial, plus the security override | P1 |
| A compromised D server substitutes a fragment and the read accepts it | critical / adversarial, plus the security override | P1 |
| The S3 gateway lists another tenant's bucket under default config | critical / common | P0 |
| Scrub stops after the first checksum mismatch and never reports it | major / common | P1 |
| A `ListObjectsV2` continuation token is ignored past 1,000 keys | major / common | P1 |
| A telemetry counter double-counts retried writes | minor / common | P2 |

## Consequences

Every issue carries a priority that says why it has it, and the backlog can be sorted by label instead of by rereading issues. Manual and automated triage use the same rules, so either can check the other, and a priority means the same in Wyrd, Stemma and nl-shell.

The durability and security overrides move more issues up than any other rule. They are chosen deliberately: rare data loss and attacker-only bugs compete with common crashes for the current milestone. Leaving the milestone to the maintainer keeps the PDCA loop from starting work on the bot's judgement alone, at the cost of one manual step per issue.

Every issue's title and description is sent to Anthropic's API. For a public repository that adds little exposure; what it does add is any secret a reporter pasted, which the bot asks them to rotate. A vulnerability reported in a public issue is labelled but not restated.

The rules exist twice, in this ADR and in `.github/triage/prompt.md`, and the table and overrides a third time in the script. A change to this ADR has to update both files; the script's tests pin the table, so a drift shows up as a failing test. The model is a constant in the script, so moving to a cheaper or stronger one is a one-line change, to be judged on the comments it produces.

Running the same bot on Stemma, the first runs used about 2,800 to 3,100 input and 140 to 290 output tokens per issue, much of the input read from the cache. Wyrd's prompt is similar in size, so back-filling the open issues should cost a few dollars; that is an estimate until the first batch reports its use. A comment can lack a reason for a label or two, when the model's reason is too short or the check withholds one that repeats the issue; the labels are unaffected.

The bot's credentials and its handling of issue text belong in the threat model; this change adds them to architecture §14.

Follow-on work: create the federation rule and repository variables, run the connection check, back-fill the open issues in batches, and review the first comments before relying on the bot.

Left open:

- **[OPEN]** Whether issues filed by the nightly jobs should be triaged by the bot too, with a fixed severity, rather than skipped.
- **[OPEN]** Whether P3 issues ever close automatically, or stay open until the maintainer closes them.
