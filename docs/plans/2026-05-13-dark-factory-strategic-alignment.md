# Dark Factory Strategic Alignment

Date: 2026-05-13
Status: Informal plan / alignment doc — not a commitment; captures current
shared understanding and the open questions worth resolving next.

## Context

Several alignment conversations over the last week (between me, Dan, and Bonny;
plus external analyses from a peer agent and the StrongDM "How to Build a
Software Factory" article) have moved Kilroy from "good workflow runner" toward
a clearer multi-layer product story. This doc captures where we currently
agree, where we still differ, and what the next phase of work looks like —
abstracted from any specific demo or partner repo.

## What we now agree on

### 1. Kilroy is dark-factory-shaped

The Kilroy repo is opinionated toward dark-factory work. New features get
prioritized against "does this serve the dark factory?" Component reuse for
non-factory purposes is fine but not the product. README and public framing
lead with the factory story; my internal "workflow plumbing" uses survive but
are not top-line.

This resolves the recurring "Kilroy IS the factory vs. Kilroy ENABLES the
factory" tension by accepting that the Kilroy *repo* is on a path to becoming
the full factory product, while the engine layer beneath it remains a general
workflow runner.

### 2. The factory is multi-layer, not monolithic

The cleanest mental model is five layers:

1. **Engine** — runs one workflow package; owns worktrees, providers, auth,
   artifacts, status, scripts, agent nodes. (This is what Kilroy already is.)
2. **Workflow Library** — trusted reusable workflows: investigate, implement,
   coding-relay, fix, review, build-test, etc. Stations on the factory line.
3. **Factory Manager** — takes a seed, builds task packets, asks
   clarification questions, chooses workflows, launches runs, reads evidence,
   decides terminal state, escalates. The manager is what does not exist yet.
4. **Factory Policy / Memory** — `KILROY.md`-style preferences, budget, risk
   posture, rules of engagement, quality bar, deploy permissions.
5. **Human** — sets goals and constraints; reviews only at gates or terminal
   states.

Layers 1 and 2 exist today. Layers 3, 4, and 5's interface are the work ahead.

### 3. QA / DoD is a core pillar, not an also-ran

The biggest update from recent conversations: "done" means the user's intent
is verified in something close to reality, not "tests pass." This is the
StrongDM framing — the output of the factory is not code; it is *validated
change plus evidence*. A factory pass that produces a beautiful diff with no
verification has not produced anything useful.

Practically:
- Verification work is *not* a workflow side-quest; it is a parallel
  workstream that runs alongside implementation.
- The verifier should sit *outside* the implementation surface where possible
  — same agent writing the code and inventing the acceptance criteria
  overfits.
- Scenarios beat tests-in-repo for factory work, because tests can be edited
  to pass; scenarios are closer to a holdout.
- "Digital twin" infrastructure (synthetic users, browser drivers, deployed
  preview environments) becomes a first-class concern as soon as we want
  factory output we can trust.

### 4. Don't build a worse coding agent

Kilroy is the line around the coding agents, not a coding agent. Claude Code,
OpenCode, Codex, and similar are stations on the line. We use their native
tools, prompts, and conventions. We own the routing, the evidence, the
terminal-state decisions, the scheduling, the budgets — not the inner loop of
"what code should I write."

The codergen-style story (where StrongDM rebuilt a Codex-style loop in Go for
boundary control) is something we are *partially* doing already with our
multi-driver agent dispatcher. We should be explicit that this is for control
of the execution boundary, not because we want to compete with Claude Code on
coding-agent quality.

### 5. The seed matters

Bad seed in → garbage out, no matter how good the engine. The "task packet"
concept (intent, source, scope, non-goals, reproduction, allowed tools,
validation, no-op rules, evidence required, output format) is a real primitive
the factory will need. Today we hand the engine a vague prompt and a
workflow name; tomorrow the manager constructs a structured packet and the
engine consumes it.

### 6. The factory has terminal states beyond "done"

A run can legitimately end in:
- `PR_READY` (or equivalent: the change is implemented, validated, evidence
  attached)
- `NO_OP` (the right answer was no change)
- `ESCALATE` (human clarification required)
- `RETRYABLE_FAILURE` (environment or tooling broke; safe to retry)

`NO_OP` is the one we keep underweighting. Without it, the agent manufactures
fake work to satisfy the harness. The current runs basically don't have this
concept; adding it is a small but load-bearing change.

### 7. Persistent factory policy belongs in a `KILROY.md`-style file

Standing budget, deploy permissions, "first do no harm" preferences, model
choices, quality bar, default workflows — all of these want to live in a
versioned per-project file that gets injected into runs. v1 is literally
"prepend it to the prompt" or "tell the manager to read it." Structure can
come later.

## Where we still differ, or haven't fully resolved

### A. Where the factory manager lives

Three plausible homes:
1. As a skill in the user's coding agent (what we have today; cheap, works,
   limited by context and judgment).
2. As a meta-workflow inside Kilroy whose nodes spawn child workflow runs
   (elegant, dogfoods plumbing, but workflow runs are one-shot; doesn't
   naturally hold cross-run state).
3. As a separate long-lived process above Kilroy (cleanest product boundary;
   a real second engine to maintain).

External analysis recommends (3) as the destination; I think (3) is probably
right *eventually*, but jumping there before the pattern is empirically
de-risked is overcommitting. Tactical answer: build the primitives first
(terminal states, evidence bundle, seed schema, KILROY.md, budget envelope),
then prototype the manager loop as a skill + scripts, and let the experience
tell us whether the production version is a process or a meta-workflow or
both. Don't decide the architecture before the pain reveals which problems
are real.

### B. How opinionated should Kilroy be about quality / cost / speed?

Dan's WIP doc asserts "default-good production-ready output" as part of the
product promise. That's a *very* strong promise that implies QA infra,
deployment patterns, design defaults. Aspirational vs. v1 needs to be
explicit. My current view: this is a north-star promise we work toward;
v1 is honest about what it can and cannot guarantee, and exposes "rules of
engagement" that let the user trade off speed/cost/risk explicitly.

### C. Does Kilroy own QA / DoD generation, or just consume what the user
provides?

This is the unresolved question that the QA pillar elevation forces. Options:
- Kilroy owns it (manager generates DoD from seed, validates against it)
- Kilroy consumes it (user supplies DoD; manager runs implement+verify pair)
- Kilroy plugs in (third-party verifier substrates like Gauntlet)

Honest answer: probably all three at different layers. The manager generates a
default DoD from the seed; the user can override or supplement; verifier
plugins handle the actual scenario execution. But this needs a real prototype
before committing.

### D. Convergence as #1 priority

Dan has consistently said convergence is the top problem. Agreed in
principle. The disagreement is about *what convergence work means in
practice* — prompt evals, decomposition, DoD quality, model selection, context
plumbing all live under that umbrella. I'd argue the most leveraged moves are
(a) better seeds / task packets, (b) better evidence so non-convergence is
visible early, and (c) the manager's escalate-vs-retry logic. Pure
prompt-tuning on individual workflow nodes is probably lower-leverage than
those.

## The current shape of work

### What we have

- 9 workflows in the library: investigate, review, implement, implement-codex,
  fix, coding-loop, coding-relay, build-test, multi-tool-exercise.
- A `using-kilroy` skill that covers mechanics (commands, labels, lifecycle)
  but is light on judgment.
- An engine that handles worktrees, providers, auth, artifacts, status,
  driver dispatch (CLI/SDK), and scheduling primitives.
- A growing set of demo and reference DOT files.

### What's missing (in priority order)

**Primitives — load-bearing for any future manager design:**

1. **Structured terminal states** from workflow runs. Today a run finishes;
   tomorrow it returns one of `{SUCCESS, NO_OP, ESCALATE, RETRYABLE_FAILURE,
   FAILED}` with reasoning. Small but central.
2. **Evidence bundle** as the output contract. What ran, what was validated,
   what's still uncertain, what would convince a skeptical human. Today's
   `result.md` is close but not standardized.
3. **Seed / task-packet schema** — even informally. The structured input that
   enters a run, replacing "vague prompt + workflow name."
4. **`KILROY.md` policy injection** into runs — preferences, budget envelope,
   rules of engagement.
5. **Verifier-workflow contract** — a workflow type whose only job is to
   evaluate a result against scenarios and return verdict + evidence.

**Workflow library — the trycycle decomposition is a good roadmap:**

The trycycle subagents map cleanly onto missing Kilroy workflows:

| Trycycle phase | Kilroy workflow |
|---|---|
| planning-initial + review + synthesis | `plan` (new) |
| test-strategy + test-plan | `test-plan` / `dod` (new) |
| executing | `coding-relay` (have) |
| post-impl-review + deepen | `verify` (new — distinct from `review`) |
| nonconvergence-review | `reconsider` (new, optional) |

A `three-views`-style fan-out workflow (parallel opinions from N models, then
synthesis) is also worth having — not because it's part of the factory loop
but because it's a demonstrable engine capability and useful on its own.

**Skill changes:**

The `using-kilroy` skill needs to grow judgment. Today it tells the agent
*how* to use Kilroy. It needs to also tell the agent *which workflow for
which kind of request*. Bias toward 4-5 named patterns the agent can
recognize, not a menu of 9+ workflow names.

A separate `factory-manager` skill is the lightest-weight way to prototype
the manager loop: it sequences plan → coding-relay → verify, decides terminal
state, asks for clarification, manages the budget envelope. This is the
trycycle pattern adapted to Kilroy. If the skill version proves the loop,
hardening into a real manager process becomes an empirical decision rather
than an architectural commitment.

**QA / DTU substrate:**

We don't own this and probably shouldn't, at least at first. The right move
is to define the verifier-workflow contract (input: a result + scenarios;
output: verdict + evidence) and let Gauntlet, browser harnesses, custom
scripts, deployed preview environments, etc. be the implementations. Our job
is the contract and the routing, not the verifier infrastructure.

## Phased direction (informal, not a commitment)

### Phase 0 — Polish what we have

- Sharpen the `using-kilroy` skill with judgment about workflow selection.
- Standardize `result.md` so every run produces a readable evidence summary.
- Smoke-test the install + first-run experience from a fresh clone.
- Add a `three-views`-style fan-out workflow as an engine-capability
  demonstrator.
- Add a trivial `verify` workflow (e.g., "run these commands, check exit
  codes, capture output") so the verifier-workflow shape exists.

This is mostly icing — workflow definitions and skill prose, not engine work.
It is what makes Kilroy *usable* to someone who isn't us.

### Phase 1 — Add the primitives

- Terminal state enum on every workflow run.
- Standardized evidence-bundle schema (could just be a structured `result.md`
  to start).
- Seed / task-packet schema as a documented input convention, even if
  initially loose.
- `KILROY.md` injection — read it from the project root, prepend or expose to
  workflow nodes.
- Budget envelope as a first-class run input that workflows can consult and
  the engine can enforce at a coarse level.

This is real engine work but bounded — each item is a small addition, not an
architectural rebuild. Doing these makes Phase 2 viable; skipping them makes
Phase 2 a swamp.

### Phase 2 — Build the factory manager as a skill

The minimal manager loop:

1. Take seed.
2. Build task packet (intent, scope, non-goals, validation, no-op rules,
   evidence required).
3. Choose one implementation workflow.
4. Launch it in a controlled worktree/branch.
5. Launch a verification workflow against the result (independently, if
   possible).
6. Decide terminal state from both outputs.
7. If continuing, generate the next task packet and loop.

Implement as a skill + helper scripts first. Use real seeds from real work.
Watch where the skill version hits walls (cross-run state, long-lived user
gates, scheduled re-checks, escalation routing). Those walls determine
whether the production version is a separate process, a meta-workflow, or
some hybrid.

### Phase 3 — Harden the manager and grow the workflow library

By this point the manager pattern is empirically de-risked and we know what
infrastructure it actually needs. The workflow library expands to cover the
full trycycle decomposition (`plan`, `test-plan`/`dod`, `verify`,
`reconsider`). Verifier substrates (Gauntlet, browser harnesses, deployed
previews) get integrated through the verifier-workflow contract.

### Phase 4 — Triggers, fleets, scheduled factory work

Per the StrongDM article: triggers come *after* the loop is reliable. Once
manual factory passes are trustworthy, automate the entry points (PR opens,
CI fails, ticket transitions, scheduled re-checks). Fleet scaling (one job
across N repos) is mostly boring infrastructure once the per-job loop is
solid.

## Open questions worth resolving in the next round

1. Is the Kilroy repo's *product promise* "you write a goal, we ship verified
   change" (Dan's framing) or "we provide the factory machinery; you assemble
   the line" (StrongDM's framing)? These are not incompatible but they imply
   different defaults and different first-time-user experiences.
2. What is the QA-substrate strategy concretely? Own a verifier? Plug into
   one (Gauntlet)? Both, with a contract layer in between?
3. Where does the manager live in production: skill, meta-workflow, separate
   process? Defer to Phase 2 evidence.
4. What's the right boundary between Factory Policy (`KILROY.md`) and Factory
   Manager? They're tightly coupled; lighter answer is the manager owns the
   policy, but there may be reasons to split.
5. Public release timing and scope. The release that lands first sets
   expectations for what Kilroy is. Be explicit: is the first cut "engine +
   library" or "engine + library + minimal manager"? Affects how aggressively
   to push manager work.
6. Naming. We've been using "engine," "plumbing," "factory manager," "dark
   factory" interchangeably. The five-layer model gives us cleaner vocabulary
   if we adopt it consistently.

## What this doesn't say

This is not a roadmap with dates. It's not a feature list. It's the current
shape of the strategic thinking, captured before it drifts. Specific work
items, sequencing, and commitments belong in subsequent narrower plan docs.

The intent is that anyone joining the project (or me, in a week, when I've
forgotten half of this) can read this and know:
- What Kilroy is becoming
- Why the layering looks the way it does
- What's load-bearing vs. what's nice-to-have
- What questions are still open and need real evidence to resolve
