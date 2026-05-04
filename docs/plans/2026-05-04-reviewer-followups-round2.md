# Reviewer follow-ups — round 2

Branch: `feat/v2-reframe`. Date 2026-05-04. Closes the high-severity gaps
the reviewer flagged after the first architecture-closure push, plus the
JSON-by-default output contract and two of the three Tier-1 leftovers.

## What this push closes

### High severity (route resolution)

The reviewer's concern: prelaunch and the dispatcher used different
route-resolution code paths. Prelaunch silently passed every non-class
agent node, so a graph with `agent_tool="made-up-tool" + llm_provider/
openai + llm_model` would survive `kilroy workflows validate` but fail
at execution.

**Fix**: introduced `engine.ResolveAgentRoute(node, exec, deps)` —
single source of truth for the route decision, returning a first-class
`AgentRoute` (NodeID, Source, Class, Provider, Model, Driver, Backend,
ClassResult). Now consumed by:

  - the **Dispatcher** (replaces the old `resolveDriverForDispatch`)
  - **prelaunch** validation (replaces the className-only branch
    that silently no-op'd vague nodes)
  - **TmuxAgentHandler.Execute** (replaces a parallel
    `ResolveAgentClass` call + duplicated stylesheet attribute parsing)
  - **agent_router.resolveNodeRouteInner** (same; keeps
    `backendForProvider` fallback for legacy non-canonical providers)

This finishes the reviewer's "first-class AgentRoute consumed by both
handlers" ask. Vague nodes and unknown `agent_tool=` values now fail
prelaunch loudly with a clear error naming the offending input.

Lenient corner: unknown `llm_provider=` values (kimi, zai, minimax,
custom OpenAI-compat endpoints) are deferred to the runtime. Prelaunch
accepts them. The Dispatcher detects `Driver==""` + non-empty `Provider`
and delegates to codergen (`AgentRouter`), which consults
`cfg.LLM.Providers` for the backend at execution time.
**Reviewer-flagged regression** (commit `9c36f40`, after the initial
push): the original Dispatcher rejected empty-driver routes outright
with `dispatcher: driver "" has no dispatch mapping`, so production
runs of custom providers failed before AgentRouter saw them. Package
tests missed this because `engine.RunWithConfig` uses
`NewDefaultRegistry`, not the layered Dispatcher. The CLI-level
regression test in `cmd/kilroy/run_custom_provider_test.go` now
guards this seam by exec'ing the real binary against an httptest
fake.

### Medium severity (output contract)

v2 §4: structured-data commands emit JSON to stdout by default;
`--pretty` flips to human-friendly key=value text.

**Fix**: `kilroy run` (sync + `--detach`) and `kilroy resume` now print
a stable JSON run handle. `--pretty` switches to the legacy format.
Single-line JSON object parses easily in shell pipelines and from agent
code juggling N runs.

```
$ kilroy run … 2>/dev/null | tail -1
{"run_id":"01KQSG…","logs_root":"./logs","worktree":"logs/worktree","run_branch":"attractor/run/01KQSG…","final_commit":"d5fbaf…","final_status":"success"}

$ kilroy run … --pretty 2>/dev/null
run_id=01KQSG…
logs_root=./logs
worktree=logs/worktree
run_branch=attractor/run/01KQSG…
final_commit=d5fbaf…
final_status=success
```

### Tier-1 leftovers (cleanup)

- **Top-level `kilroy review --graph`**: removed. Collided with the v2
  workflow `kilroy run review`. The internal/attractor/review package
  stays for now but has no callers.
- **`kilroy resume --cxdb` / `--context-id`**: removed from the CLI
  surface. `engine.ResumeFromCXDB` stays as a Go API for tests and
  programmatic callers.
- **`kilroy status --follow|-f` / `--raw` / `--cxdb`** (v2 §2
  polling-not-streaming): **landed**. -1254 LoC: deleted
  attractor_status_cxdb.{go,_test}, attractor_status_follow_test.go,
  the runFollowProgress/runFollowCXDB impls, and all event/CXDB
  formatter helpers. Renamed attractor_status_follow.go →
  attractor_status_snapshot.go to reflect what's left
  (snapshot/watch/--latest helpers).

## Tests added

Engine package (`internal/attractor/engine/`):

- `agent_route_test.go` (8 cases): covers DriverForAgentTool/SDKProvider
  mappings, agent_tool→CLI routing, llm_provider→SDK routing, vague
  failure, unknown-tool failure, unknown-provider deferred-to-runtime,
  agent_tool wins over llm_provider, AuthMethod/AuthSource convenience
  on non-class routes.
- `prelaunch_test.go` (3 new):
  - `_NonClass_UnknownAgentTool_FailsLoudly` — reviewer regression
  - `_NonClass_UnknownLLMProvider_DefersToRuntime` — legacy plugin seam
  - `_NonClass_VagueNode_FailsLoudly` — reviewer regression

cmd/kilroy:

- `run_handle_output_test.go` (4 cases): JSON-is-default, --pretty
  emits key=value, detached prints `detached=true`, empty fields
  omitted from JSON.

Updated:

- `prelaunch_test.go::TestValidatePreLaunch_PackageIntegrity_OK` —
  fixture needed a valid route to reach the package check (the new
  prelaunch correctly fails routes-less agent nodes).
- `run_e2e_test.go::TestE2E_FakeProvider_ImplementWorkflow` — parses
  the JSON run handle instead of grepping `run_id=` / `logs_root=`.
- `main_exit_codes_test.go::TestRun_PrintsCXDBUI*` — assertions match
  the JSON shape (`"cxdb_ui":"URL"`).

## Reviewer round-2 follow-up: opencode multi-provider routing

Discovered after the initial round-2 push by a worktree worker
(`worktree-coding-relay`) writing a workflow that exercised mixed
driver routing on purpose. Two paired bugs the reviewer insisted land
together (commit `87ae628`):

**F1 — `agent_tool="opencode"` had no provider mapping.** opencode is
multi-provider; the driver-implies-provider pattern (claude_cli →
anthropic, codex_cli → openai) breaks for it. ResolveAgentRoute now
treats opencode specially: requires explicit `llm_provider=`, uses it
as the route's Provider. Driver=opencode, Backend=BackendCLI.

**F1 corollary — fixed-provider tools fail loudly on mismatched
explicit providers.** `agent_tool="claude"` + `llm_provider="openai"`
used to silently win (agent_tool decides). Now produces a clear error
naming both providers and the word "conflicts" — the route metadata
can no longer disagree with what the binary uses.

**F2 — opencode template hardcoded anthropic in
OPENCODE_CONFIG_CONTENT.** Even after F1 routed kimi correctly, the
launched opencode subprocess only saw an anthropic provider block.
Template now reads `KILROY_AGENT_PROVIDER` from env (set by
tmux_handler from the resolved AgentRoute) and emits the matching
provider config, pulling DefaultBaseURL/DefaultAPIKeyEnv from
`providerspec.Builtin()`.

8 new tests cover the resolver path (opencode require/accept,
fixed-provider mismatch) and the template path
(buildOpencodeConfig/PrepareSession/BuildArgs for kimi/zai/anthropic/
unknown providers).

## What's still open from the two reviewer letters

1. **Async-default flip**: `--detach` becomes the default, `--sync`
   the explicit-block escape. Reviewer flagged this as a separate
   decision — a UX break, not a fixup. Punted to a discrete
   conversation.
2. **Stripping advanced run flags entirely** (`--graph`, `--config`,
   `--workspace`, etc.) — needs test-infrastructure migration to Go
   APIs first.

## Surface checks

- `go build ./...` clean.
- `go vet ./...` clean.
- `gofmt -l ./cmd/kilroy/ ./internal/attractor/engine/ ./internal/attractor/agents/`
  clean.
- Live smoke: `kilroy run --graph tiny.dot` prints JSON; `--pretty`
  prints key=value.
