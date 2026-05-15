# Kilroy Trust + Roadmap — Field Notes

Date: 2026-05-15
Status: Informal plan / roadmap notes — captures the top-level concerns surfaced
from four overnight agent runs (two Claude, two Codex) on a single dark-mode
bug in a partner repo, plus this week's plan/validate workflow work.

Rides on [2026-05-13-dark-factory-strategic-alignment.md](2026-05-13-dark-factory-strategic-alignment.md) —
same five-layer mental model, same primitives. The new piece here is *evidence
from real runs* about which primitives are load-bearing for the unattended
use case.

## The implicit contract

Kilroy's contract with the user: **drop a note, walk away, come back to
validated code.** Generating code is the default. Investigation — "tell me
what's wrong, don't change anything" — is an explicit opt-out, not the default
branch.

That single commitment drives every concern below. Each game-ending failure
listed is something that prevents the user from walking away.

It also reframes some of this week's agent behavior. When the Claude runs
"pivoted to `investigate`" on a goal of "see if you can find what is wrong
with dark mode," they violated the contract — the user wanted a fix, not a
report. Both Claude runs ended with "want me to apply the fix?" The Codex
Run 2 path (plan → implement → validate) is closer to right, modulo the
speed and orphan-branch issues below.

## Game-ending failures (from the transcripts)

The unattended use case fails when:

1. **Silent infra deaths.** The implement loop's tmux-command-too-long bug
   killed 3 of 4 overnight runs at the 5–20-second mark with a generic
   `implement_loop_did_not_complete`. User wakes up to nothing landed and a
   useless error message.

2. **Polling burns context.** `kilroy runs wait` defaults to a 30-min timeout;
   agents re-issue it on timeout and stare at "still running" for tens of
   cycles. Codex Run 2 spent ~50 polling rounds before the work finished.
   Agent budget is exhausted before the line is.

3. **Trust drift.** When Kilroy looks slow or stuck, agents bail and replicate
   the work locally. Codex Run 1 explicitly `kilroy stop`'d a progressing run
   and re-did it as local TDD. The agent didn't believe the line would finish,
   so it competed.

4. **Asking for help.** Both Claude runs and one Codex run ended with an
   interactive question ("apply the fix?", "merge with roborev?"). For an
   unattended overnight run, any prompt-for-input is end of run.

## Other simple breaks worth knowing

Smaller but cumulative:

- **No clean landing step.** Validated patches end up on `attractor/run/<ULID>`
  branches with no merge affordance. Even successful runs stall at "now what?"
- **Stale worktree litter.** 8 dark-mode branches/worktrees from 4 attempts.
  No reuse, no cleanup — each run adds another.
- **No `NO_OP` terminal state.** Workflows can't refuse misfit inputs
  honestly. The classifier-labels-X-workflow-ignores-X pattern is one
  symptom; another is `implement` accepting an investigation-shaped packet
  and starting to code anyway.
- **Verification can't distinguish "env missing" from "verified and failed."**
  Codex Run 1 papered over Stripe/Firebase env errors as "environmental" and
  moved on. Today's `VALIDATED` can hide unrun scenarios.
- **Validator doesn't follow the validation plan.** Even after this week's
  prompt tightening, the validator improvises rather than executing the plan
  as written. The honesty test (a nonexistent feature) showed the verdict
  was right but the path wasn't from the plan.
- **Workflow discovery precedence is inconsistent.** `kilroy check` and
  `kilroy run` can resolve different copies of the same workflow when
  invoked from different CWDs. Silent staleness.
- Minor: `kilroy run --json` is silently unsupported; agents guess and lose
  a run on first try.

## Pre-demo ASAP

Four things, in order of demo-killer potential:

### 1. Kill the tmux command-too-long bug

The implement workflow embeds the planner prompt into a tmux launcher
command. tmux 3.6a chokes somewhere around 30–40 KB. 3 of 4 overnight runs
died here within 20 seconds.

Fix: stop embedding. Write the prompt to a file under the stage dir, have
the tmux command `cat` or `source` it. Small Go change in the tmux launcher
path. Without this, `implement` fails live on any real-sized task and the
demo doesn't survive a single goal.

### 2. `kilroy runs wait` long-block or push-style completion

The 30-min default forces polling. Add an `--until-done` mode that blocks
without timeout (or default to it), and emit a single event when the run
reaches terminal state. Eliminates the 30-min repoll cycle that drains
agent context in unattended runs.

### 3. Bias the plan classifier toward READY_TO_IMPLEMENT

Today the classifier sees a goal like "fix dark mode" or "see what's wrong"
and labels it `classification: "investigation"` because the wording is
diagnostic-shaped. Per the implicit contract, the default should be "produce
code." Investigation classification — and the `investigate` workflow as
its route — is reserved for goals that explicitly opt out of code changes:
"just tell me what's broken," "read-only audit," "don't touch anything."

This is a prompt change to the classifier plus matching skill guidance.
Small, high leverage. It's what closes the "drop a note → get code back"
loop instead of "drop a note → get a diagnostic report and a question."

### 4. Skill rule: don't bail to local work

The `using-kilroy` skill needs explicit text: *if Kilroy is the worker, you
do not author code, tests, or run verification locally. Wait, or escalate
to the user.* Codex Run 1 is the cautionary tale — agent ran `npm ci`,
read every source file, watched STATUS.md, then `kilroy stop`'d the run
and committed locally-authored code on its own branch.

Those four together are the minimum to make a supervised demo non-embarrassing.

## Roadmap themes (mapped to dark-factory layers)

### Trust primitives — Engine layer

Phase 1 in the dark-factory doc; the transcripts make them concrete:

- **Structured terminal states.** Especially `NO_OP` and `ESCALATE`.
  Without these a workflow can't honestly refuse work — so it either fakes
  it or the agent bails. The taxonomy is on paper; nothing emits these yet.
- **Push completion, not polling.** Single-event delivery on terminal
  state, no 30-min wait windows, no agent-side retry loop. The
  single highest-impact change for unattended runs after the tmux bug.
- **Workflow refusal.** A workflow should be able to inspect its inputs
  and emit `NO_OP` + `route_to=<other workflow>` rather than dutifully
  producing artifacts for a misfit task.

### Manager skill / routing — Layer 3

The transcripts show every agent reinventing the manager loop badly. The
cheapest version of a manager is the `using-kilroy` skill itself. It needs
three explicit rules:

1. **Default: plan → implement → validate → integrate.** Code is the
   product; investigation is an opt-out, not a branch the agent picks on
   the agent's own judgment.
2. **Don't bail.** If Kilroy is the worker, the agent waits or escalates.
   No local TDD, no parallel verification, no `kilroy stop`-then-replicate.
3. **No interactive prompts at terminal states.** Validated work either
   lands or escalates; it doesn't ask permission.

A real factory-manager skill (Phase 2 from the plan doc) owns sequencing,
terminal-state interpretation, retry vs escalate decisions, and budget
envelope. The skill version is the prototype; productizing comes after
the pattern is proven.

### Landing / integration — Layer 1+2 boundary

The "now what?" gap after `validate` is structural. Patches stranded on
synthetic `attractor/run/<ULID>` branches mean even successful runs feel
unfinished.

- A first-class `integrate` workflow: takes a validated run, produces a
  clean feature branch rebased on main, deletes the run-branch, preserves
  evidence in `.kilroy/runs/<id>/` or as a PR comment.
- That's also where **stale-worktree cleanup** lives — `integrate` and
  `discard` have visibility into dead branches.

Without integration, the "drop a note, come back to validated code"
contract has a missing last step.

### Verification honesty — Layer 1

The plan-vs-reality contract isn't holding:

- **Validation-plans written up front drift into spec.** The planner
  guesses at output formats and internal failure modes because no
  implementation exists yet to validate against.
- **Validators improvise rather than execute the plan.** Even the
  tightened prompt didn't fully land.
- **Env-missing reads as a tacit pass.** `VALIDATED` should mean every
  declared scenario ran. A `VALIDATED_PARTIAL` or strict-no-skip stance
  would close the loophole.

Two design directions worth picking between: move validation-plan
authoring to after implementation (so it can see the interface), or
accept that the validator owns the plan and the planner just sets intent.

### Hygiene — across layers

- Make `kilroy check` and `kilroy run` agree on workflow discovery
  precedence. Fail loudly on disagreement.
- Worktree reuse/cleanup as a per-run preflight step.
- Fit and finish: flag parity on `--json`, error messages on the tmux
  launcher path so the next infra bug isn't silent.

## Slogan

**Make the factory trustworthy unattended.**

The user drops a note, walks away, and comes back to a landed change with
evidence — not a stuck agent, an orphan branch, or a locally-replicated
patch. Every item above is in service of that.

The pre-demo list is the minimum for a supervised demo not to embarrass us.
The roadmap is what makes the unattended version real.
