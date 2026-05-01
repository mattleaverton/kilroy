# Kilroy v2: Workflow Platform Reframe

**Date:** 2026-05-01
**Branch:** `feat/v2-reframe`
**Status:** Living design document

## Summary

Kilroy is being re-centered from "an Attractor engine with exposed LLM machinery" into a local-first workflow platform that runs mixed script-and-agent work with sane defaults, saved history, and hidden infrastructure complexity.

The repo already has the right ingredients — workflow packages, run DB, layered architecture, provider/profile seams. This is **re-centering, not reinvention**. Once landed, it is a hard cut: no legacy parallel surface, no migration shims.

The design ethos comes from Will Larson's "agents as scaffolding" pattern. Deterministic code and scripts control flow; agents are inserted where ambiguity helps. Provider/model/auth concerns are not workflow-author concerns. Simple launch is the default; explicit infrastructure tuning is escape-hatch only.

## Posture commitments

- **Audit, not approval.** Everything that happened is observable in the run record. Nothing is gated on cost or routing decisions at the runtime level. The current AGENTS.md "every routing decision must be explicit" guidance was a coding-agent guardrail, not a runtime principle.
- **Agents are the primary audience.** Humans are pleasant-secondary. The design target is an agent juggling a dozen Kilroy calls at once. Output is structured by default, errors are typed, async-with-handle is the default execution shape, polling is the consumption model.
- **Async-with-handle is the default execution shape.** `kilroy run <workflow>` returns a run handle synchronously and execution proceeds in the background. `--wait` is the affordance for humans who want to block.
- **Structured output by default.** JSON to stdout, typed errors, `--pretty` for human consumption.
- **Polling, not streaming.** `kilroy runs wait` and `kilroy runs show` are sufficient. No SSE / live event subscription on the consumption side.
- **CWD-aware invocation.** Kilroy stands where you stand. Repo, workspace, and logs root are derived from invocation context unless overridden.

## What's explicitly out of scope

Defining what Kilroy is *not* doing keeps the core small.

- **Hosting UI.** Kilroy emits data; UI rendering lives in a separate project.
- **Managing credentials.** Kilroy discovers and routes; doesn't store, rotate, or own secrets.
- **Authoring routing opinion.** Kilroy ships an opinion as data; its content evolves through repo PRs, not as part of Kilroy's executable behavior.
- **Streaming live status.** Polling is sufficient.

## Architecture: 5 layers

1. **Execution Core.** DOT graph traversal, state, retries, events, run DB, resume. Stable; opinionated about mechanics, neutral about content.
2. **Workflow Package.** Skill-shaped directory: `graph.dot` + `workflow.toml` + `scripts/`, `prompts/`, `assets/`. Discoverable hierarchically: built-in (compiled in) → user (`$XDG_CONFIG_HOME/kilroy/workflows/`) → project (`.kilroy/workflows/`).
3. **Policy Resolver.** Maps abstract class requests (e.g. `hard_coding`, `quick_easy`) to concrete routes. Reads from baked-in policy data shipped in the binary. Read-only at runtime.
4. **Transport / Auth Layer.** Knows how to talk to model providers along the *(model, driver, transport, auth, history sink)* tuple. Handles fallback and remediation guidance internally; workflow authors don't see it.
5. **Surfaces.** CLI commands and a data API server. UI consumes that data; UI does not live in this layer.

## CLI shape

- `kilroy run <workflow> [args]` — canonical form. Returns a run handle. Async-by-default; `--wait` to block.
- `kilroy <workflow>` — bare form, reserved for a small set of first-class built-in workflows. Hardcoded blessing list, not earned by usage. Ensures workflow names don't collide with built-in verbs.
- `kilroy runs ...` — list, show, wait, prune. Mostly already exists; keep.
- `kilroy auth ...` — discover and route credentials.
- `kilroy workflows ...` — list, describe, validate.
- `kilroy policy ...` — read-only: list classes, show how a class resolves on this machine, explain what a given run picked.

The `attractor` namespace dissolves entirely. Every subcommand moves up to the top level.

## Workflows

A workflow package is a directory with a manifest, graph, and supporting material. Skill-shaped — manifest fields align with Claude skill manifest format where the meaning is the same, diverging only where Kilroy needs more.

Manifests declare:
- **Inputs** — required and optional, with types and descriptions.
- **Outputs** — named, with types.
- **Class needs per agentic node** — e.g. `class: hard_coding`. Specific model name is also accepted but flips fallback behavior.
- **Side-effect class** (optional) — `mutates_git`, `writes_files`, `network_egress`, `idempotent`. Signal for calling agents to plan.

Workflows can ask for either a specific model (strict) or a class (resolved with fallbacks). Specificity drives fallback behavior:
- **Specific model unavailable** → run fails loudly. No fallback.
- **Class request** → resolver picks from the policy's fallback chain among what's available, and the actual choice is recorded in step metadata.

Built-in workflows can be opinionated for now since we are the primary consumer. Designed to spin out into a separate repo later through the same loading mechanism.

## Policy as baked-in data

- Class definitions, fallbacks, and defaults live in source files in the Kilroy repo.
- At `go build`, the policy table is compiled into the binary.
- No per-machine drift, no user/project override mechanism (for now). Updates are normal repo PRs.
- The DB does **not** store policy. It stores runs and step metadata, including which model/provider/transport got picked for each agentic step.
- `kilroy policy` is read-only. Mutations are repo PRs.

**Trade noted:** A contributor or org wanting a different policy must fork or PR upstream. Acceptable given current audience and stage; revisit if real demand for per-deploy override emerges.

## Agent-conversation abstraction

An agentic node executes against a small tuple: *(model, driver, transport, auth, history sink)*.

- **model** — concrete model identifier
- **driver** — what's orchestrating turns (e.g., direct API caller, claude CLI, codex CLI, aider)
- **transport** — how driver and model talk (HTTP, subprocess, tmux pty)
- **auth** — which credential context resolves the call
- **history sink** — where the conversation log goes (jsonl parser X, API stream, etc.)

Today these are tangled across `AgentHandler` (API path) and `TmuxAgentHandler` (CLI/tmux path). Untangling them so each axis is independent is the actual implementation work. Once untangled, adding a new agent CLI / driver / transport is a small contained change rather than a new node type.

This is the abstraction we'll experiment with as a first pass. May refine as we hit edges.

## Auth as helpful plumbing

Kilroy's auth surface is **discovery and routing**, not storage or rotation.

- `kilroy auth list` enumerates auth state on the machine: env vars, CLI tool config locations (`~/.claude`, `~/.codex`, `~/.config/gh`, etc.), API keys.
- Reports what's available and what's broken, with clear remediation guidance.
- Enables swapping between providers/subscriptions/keys.
- Workflows ask for auth abstractly; Kilroy resolves to a concrete env var or config path at runtime.

Kilroy does not store secrets, rotate keys, or own credential lifecycle.

## Run records and retrieval

- Existing CXDB + rundb + per-run logs root remains. It's working well; mostly leave alone.
- Step metadata captures the chosen route (model, provider, transport, auth source) for each agentic step. This is normal step metadata, recorded as the step runs — not a special "snapshot" mechanism.
- Retrieval beyond CLI happens via the data API server. UI lives in a separate project.

## Recursion (Kilroy calling Kilroy)

Allowed. Design not to prevent.

- Inner Kilroy invocation detects parent via env var (e.g., `KILROY_PARENT_RUN_ID`).
- Inner run links to outer in the DB on startup.
- `kilroy runs show <outer>` displays nested children.
- Cancellation does not propagate by default. Stopping outer run does not stop inner ones; outer status reflects unresolved children.

## Concurrency

Multiple sibling Kilroy runs from a single orchestrating agent must be airtight isolated. Separate workspaces, separate logs roots, separate DB rows, no shared mutable state, no port collisions, no lock contention beyond what the DB already handles cleanly.

This is a **load-bearing property** of the agent-primary commitment, not a corner case. Verify before leaning on it.

## Work to be done

These are categories of work, not phases. Each is independently shippable. Order is a suggestion, not a contract.

### CLI surface collapse
- Replace hand-rolled arg parsing with cobra (or similar).
- Drop `attractor` namespace; move subcommands to top level.
- Implement `kilroy run <workflow>` canonical form.
- Implement first-class workflow elevation (hardcoded reserved-words list).
- Default execution: async-with-handle, `--wait` to block, JSON output by default with `--pretty` for humans.

### Workflow registry
- Define `workflow.toml` schema. Align with Claude skill manifest where cheap.
- Implement hierarchical discovery: built-in → user → project.
- Migrate existing demo graphs to package format.
- Implement `kilroy workflows list/describe/validate`.

### Policy data and resolver
- Define the policy data format (class → fallback chain → concrete tuples).
- Author baseline policy and bake it into the binary at build.
- Implement the resolver: class request + machine state → concrete tuple, with fallback choice recorded.
- Implement read-only `kilroy policy` surface.
- Step metadata captures resolution outcome.

### Agent-conversation untangling
- Define `AgentBackend` Go interface organized along the tuple axes.
- Refactor `AgentHandler` and `TmuxAgentHandler` to be picks-of-the-tuple, not separate node types.
- Backend selection becomes config/policy driven, not the `--tmux` boolean.
- Add a third backend impl as a forcing function for the abstraction.

### Auth discovery
- Survey what to detect (env vars + CLI tool config locations).
- Implement `kilroy auth list / use / suggest-fix`.
- Workflow auth requests get resolved abstractly.

### Recursion linkage
- `KILROY_PARENT_RUN_ID` env var contract.
- DB schema for parent/child run linkage.
- `kilroy runs show` displays nested children.

### Concurrency hardening
- Audit shared mutable surfaces (workspace allocation, DB locks, port assignments, log paths).
- Stress test: launch 12+ sibling runs of distinct workflows, verify isolation.

### CWD-aware defaults
- Auto-detect repo from CWD.
- Auto-derive workspace and logs root unless overridden.
- Discover `.kilroy/` config and workflows by upward search.

### Built-in workflows
- Ship an opinionated set: candidates include `investigate`, `review`, `fix`, `summarize`. Final set TBD by Investigation 6.
- These get first-class `kilroy <workflow>` status.

### Output format and structured errors
- JSON output schema for run handles, status, results.
- Typed error format (`{"error": "auth_missing", "class": "anthropic-subscription", "remediation": "..."}`).
- `--pretty` flag for human-readable rendering.

## Investigative efforts (quick-launch isolated)

These are scoped, single-agent investigations to run in isolation via the `quick-launch` skill before committing to specific design choices. Each produces a `result.md` we can review.

Each investigation below is a self-contained prompt ready to drop into a `quick-launch` invocation.

---

### Investigation 1: Workflow manifest layout

**Question:** What should `workflow.toml` look like, given Kilroy's needs and the affinity with Claude skill manifests?

**Prompt:**
> Read 5–10 representative Claude skills (search GitHub for repos with `.claude/skills/` directories or `skills/<name>/SKILL.md` files; the Anthropic claude-code repo and community plugin repos are good sources). Compare their manifest fields and conventions to what Kilroy workflows need: graph reference, class needs per agentic node, inputs/outputs with types, side-effect declarations (`mutates_git`, `writes_files`, `network_egress`, `idempotent`), descriptions for both human and agent consumers.
>
> Propose a `workflow.toml` schema that aligns with Claude skill format where the semantics match and diverges where Kilroy needs more. Output:
> 1. The proposed schema (with field types, required/optional, descriptions).
> 2. An example `workflow.toml` for a hypothetical `investigate` workflow.
> 3. A list of fields that diverge from Claude skill format with rationale.
> 4. A short note on TOML vs YAML vs JSON for this manifest given the audience.

---

### Investigation 2: Auth discovery surface

**Question:** What does `kilroy auth list` actually scan for on a typical developer machine?

**Prompt:**
> Survey the file locations and env-var conventions for major LLM CLIs and adjacent tools: Anthropic Claude CLI (`~/.claude/`), OpenAI Codex CLI, GitHub `gh`, Google Gemini CLI, Aider, OpenCode, and Cursor's CLI integration if any. For each, document:
> - Where credentials are stored (config file path, OS keychain, env var).
> - How to detect "logged in" state without invoking the tool (file presence, file mtime, structured config field).
> - Conventional env vars that override or supply credentials.
> - How scoping works (multiple accounts, per-project tokens, profile switching).
>
> Output:
> 1. A table summarizing each tool.
> 2. A proposed detection algorithm for `kilroy auth list` — what files to stat, what env vars to read, in what order.
> 3. A proposed shape for the JSON output of `kilroy auth list`.
> 4. Edge cases (stale tokens, partial logins, multiple profiles) and how to surface them.

---

### Investigation 3: CWD-aware tool patterns

**Question:** How do tools like git, gh, cargo, npm, kubectl handle CWD-detection and per-project config? What patterns should Kilroy adopt?

**Prompt:**
> Survey the CWD-detection and config-discovery behaviors of git, gh, cargo, npm, kubectl, and 1–2 others worth including (suggest direnv, asdf, or similar). For each, document:
> - How upward search works for project-root markers.
> - How environment overrides interact with discovered config.
> - How project config layers with user config (override vs merge).
> - What happens when no project context is found.
> - How CWD-relative paths in config are resolved.
>
> Output:
> 1. A comparison table.
> 2. A recommended pattern for Kilroy: where it searches upward, what file marks a Kilroy-aware project, how `~/.config/kilroy/` and `<repo>/.kilroy/` layer, what happens outside any project.
> 3. A short justification for the pattern chosen.

---

### Investigation 4: Policy data shape

**Question:** What does the baked-in policy data look like in detail? How does class + fallback + machine-state resolution actually work?

**Prompt:**
> Given the design: a class is a name (e.g. `hard_coding`, `quick_easy`, `frontend_aesthetic`) that resolves to a concrete *(model, provider, transport, auth, history sink)* tuple based on policy data and what's available on the machine. The policy data is shipped in the binary at build time. Resolution happens at runtime; the resolved tuple is recorded in step metadata.
>
> Design the data format and resolution algorithm. Address:
> - How fallback chains are expressed (ordered list of preferred routes per class).
> - How machine state (which providers have working auth) feeds into resolution.
> - How to express "prefer X, fall back to Y if X unavailable, fail if both unavailable."
> - How a workflow can request a specific model (no fallback) vs a class (fallback allowed).
> - How resolution outcome and any fallback events get recorded as step metadata.
>
> Output:
> 1. The data schema (Go structs preferred; YAML or TOML acceptable for source files).
> 2. 3–5 example class definitions (`hard_coding`, `quick_easy`, plus your choice of others).
> 3. Pseudocode for the resolver, including the fallback walk.
> 4. The shape of the step-metadata record for resolution outcome.
> 5. A note on what happens for an unknown class (typed error, not panic).

---

### Investigation 5: Agent-conversation tuple validation

**Question:** Is *(model, driver, transport, auth, history sink)* the right set of axes? Are there missing dimensions? Are any redundant?

**Prompt:**
> Without access to the Kilroy repo: assume two existing agent backends.
> - **API path:** direct HTTP calls to Anthropic / OpenAI / Google APIs, conversation history captured via response streams.
> - **CLI/tmux path:** spawns the `claude` / `codex` / `opencode` binary in a tmux session with `--output-format stream-json`, parses JSONL output for conversation history.
>
> Decompose each into the proposed tuple axes: *(model, driver, transport, auth, history sink)*. Identify what's shared, what differs, what's tangled together today.
>
> Then design a hypothetical third backend — e.g., a direct subprocess call to Aider, or an HTTP integration with a self-hosted OpenAI-compatible endpoint via vLLM. Show how it slots into the tuple scheme.
>
> Identify any axis that doesn't carry weight (could be merged or dropped) or any dimension that's missing (something both backends actually vary on but the tuple doesn't capture).
>
> Output:
> 1. Decomposition of the two existing backends along the tuple axes.
> 2. The third-backend design.
> 3. A critique of the tuple — what's right, what's wrong, what to add or remove.

---

### Investigation 6: First-class built-in workflow set

**Question:** What 3–5 workflows deserve first-class `kilroy <workflow>` elevation, and what should they actually do?

**Prompt:**
> Survey common everyday tasks where a coding agent or developer would benefit from invoking Kilroy. Candidates include: investigate, review, debug, summarize, refactor, test, document, fix-flake, audit-deps, scope-pr.
>
> For each candidate, evaluate:
> - Is it sufficiently general to be a stable built-in?
> - What's the typical input contract (a request string? a PR number? a file path?)?
> - What's the output (a written report? a diff? structured findings?)?
> - What graph would it have (rough sketch — which nodes are scripts, which are agentic, what's the flow)?
> - What class needs per agentic node?
> - What's the typical duration/cost class?
>
> Propose a final set of 3–5 workflows for first-class elevation. For each, output a full `workflow.toml` and a graph sketch (DOT or pseudocode), ready to translate into a `built-in/workflows/<name>/` directory.
>
> Be opinionated. Pick the ones that earn the elevation, defend the choices, name what you cut.

---

## Open questions to revisit

- **User/project policy override.** Currently no, drift-prevention wins. Revisit if a contributor org asks for it.
- **Side-effect class declarations** — required or optional on workflow manifests? Decide once we have a few real workflows authored.
- **Streaming vs polling** — locked on polling for now. Revisit if agents juggling N runs find polling expensive in practice.
- **Recursion depth** — should there be a max nesting limit? Probably yes, but pick the number after observing real usage.
- **Routing-opinion authorship eventually** — if the built-in policy ages out faster than release cadence, do we add a fetch-and-cache mode? Defer.

## Notes

This document captures the design direction agreed in conversation on 2026-05-01. It is a living document — update it as concrete work lands or as the model shifts. Treat it as the single source of truth for the v2 reframe; if a decision drifts, update here first, then implement.
