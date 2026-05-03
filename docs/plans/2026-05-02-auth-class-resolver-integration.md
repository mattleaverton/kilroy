# Auth ↔ class-resolver integration

**Date:** 2026-05-02
**Branch:** `feat/v2-reframe`
**Status:** Approved scope; implementation plan.

Integrates auth resolution into the policy/class resolver. Removes the four-way duplication of credential names (policy.toml, provider specs, adapter `os.Getenv` calls, auth detector) by routing every credential decision through a single `AuthBindingResolver` consumed by prelaunch, execution, `kilroy auth check`, and the policy reachability check.

Replaces the parallel-detector framing in v2 plan §8 (`docs/plans/2026-05-01-kilroy-v2-final-plan.md`) with a credential-binding step inside class resolution. The three fallback layers — auth, policy, runtime — stay distinct. Block 6 Step 5 (AuthResolver injection) is pre-paid by this work.

No legacy support. No silent failures. No compiled-in active defaults.

---

## 1. Principles

1. **No compiled-in active runtime defaults.** Templates ship for *inspection* (`kilroy auth defaults`) and *init* (`kilroy auth init`). Runtime reads only project/user config. Missing config → typed error at prelaunch with `auth init` remediation.
2. **Policy stays generic.** Policy candidates declare `requires = { provider, method, tool? }` only. Personal chain names never appear in policy.toml. Indirection lives in user/project `[bindings]`.
3. **Bindings disambiguate; ambiguity is an error.** When a `requires` tuple matches multiple chains and `[bindings]` doesn't pick one, fail with `auth.ErrAmbiguousAuthChain` listing candidates and remediation.
4. **Named chains with `requires` metadata.** Chain identity is `(name, requires)`. Multiple chains can satisfy the same `(provider, method)` tuple — bindings or per-class `auth_ref` (future) selects between them.
5. **Snapshot identity, not values.** Prelaunch freezes `(chain_name, source_kind, source_name)` per agent node. Execution re-reads the named source; if the source has vanished, fail decisively (`auth.ErrSourceVanished`). Never re-walk the chain mid-run.
6. **Layered merge by chain name and binding key.** Project chain `X` *replaces* user chain `X` wholesale. Same for binding entries. No source-list merging across layers — that creates ordering surprises.
7. **Per-driver credential materialization is explicit.** Especially: `claude_cli` requires *scrubbing* `ANTHROPIC_API_KEY` from the child env, otherwise the CLI uses the env key instead of the subscription session. Silent wrong-billing is the failure mode being prevented.
8. **Single resolution path.** Prelaunch, execution, `auth check`, and policy reachability all consume one `AuthBindingResolver` returning one `AuthBindingSnapshot` shape. No four surfaces re-implementing similar logic.

---

## 2. Files and shape

```
internal/auth/data/default_chains.toml   — templates only (printed, sampled; never read at runtime)
~/.config/kilroy/auth.toml               — user config; the runtime source
<project>/.kilroy/auth.toml              — project config; replaces user entries by name
```

User/project config (the bindings indirection):

```toml
[bindings]
"anthropic/api_key"          = "anthropic_kilroy_api"
"anthropic/cli_oauth/claude" = "anthropic_claude_cli"
"openai/api_key"             = "openai_kilroy_api"
"openai/cli_oauth/codex"     = "openai_codex_cli"
"google/api_key"             = "google_kilroy_api"
"google/cli_oauth/gemini"    = "google_gemini_cli"

[chains.anthropic_kilroy_api]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "ANTHROPIC_API_KEY_KILROY" },
  # { kind = "env_var", name = "ANTHROPIC_API_KEY" },     # absent at init time; uncomment to enable
]

[chains.anthropic_claude_cli]
requires = { provider = "anthropic", method = "cli_oauth", tool = "claude" }
sources = [
  { kind = "cli_session", tool = "claude" },
]
```

Policy candidate (no personal naming, ever):

```toml
[[classes.hard_coding.chain]]
model_id = "claude-opus-4-7"
driver   = "claude_cli"
requires = { provider = "anthropic", method = "cli_oauth", tool = "claude" }
```

---

## 3. Resolution flow (the AuthBindingResolver)

For one policy candidate's `requires` tuple:

1. Look up `requires` tuple key (`"<provider>/<method>[/<tool>]"`) in `[bindings]`. If mapped → that's the chain name.
2. If not mapped → enumerate configured chains where `chain.requires == requires`.
   - 0 chains: `auth.ErrNoChainForRequirement`.
   - 1 chain: use it.
   - >1 chains: `auth.ErrAmbiguousAuthChain` (lists candidates, suggests `[bindings]` entry).
3. Walk the chosen chain's `sources` against current detection. Pick first usable.
4. None usable → `auth.ErrChainExhausted` (lists each source + skip reason).
5. Return `AuthBindingSnapshot{chain_name, source_kind, source_name, method, provider, skipped: [...]}`.

Same resolver called by prelaunch (snapshot per agent node), execution (re-reads source by name from snapshot), `auth check` (per binding/chain), and policy reachability (skip candidates whose binding errors).

---

## 4. `kilroy auth init` algorithm

Auth init is the discovery-and-population step. The resulting config is the matrix used to validate runs.

1. Read `internal/auth/data/default_chains.toml` (templates kilroy supports).
2. Run Block 5 detection (what's on this machine).
3. For each template binding entry: include verbatim in user config.
4. For each template chain:
   - Include all sources, but emit detected sources as active TOML entries and undetected sources as TOML comments.
   - If a chain has zero active sources, still emit it (with all sources commented) — surfaces what's available to enable.
5. Write `~/.config/kilroy/auth.toml`. Idempotent: refuses to overwrite without `--force`.
6. Print summary: chains with usable sources (count, names), chains without (count, names + remediation hint per source).

A future `kilroy auth init --rescan` updates the file in place: adds newly-detected sources as active entries (uncommented), preserves user reordering of source lists, doesn't remove anything the user added by hand. (Out of v1.)

---

## 5. Per-driver credential binder

The `internal/attractor/engine/credential_binder.go` produces `BindResult{ EnvSet, EnvScrub, FilesToWrite, SDKArg }` per driver. Critical entries:

| Driver | Binding action |
|---|---|
| `anthropic_sdk` | `SDKArg = <env value at execution time>` |
| `claude_cli` | **`EnvScrub = ["ANTHROPIC_API_KEY"]`** so the CLI uses session, not the env key. **The load-bearing materialization step.** |
| `openai_sdk` | `SDKArg = <env value>` |
| `codex_cli` | Write `<stageDir>/.codex/auth.json`; set `CODEX_HOME=<stageDir>/.codex` |
| `google_sdk` | `SDKArg = <env value>`; record source name in resolution.json |
| `gemini_cli` | Env updates per gemini's accepted names; OAuth left to CLI when `cli_session` source picked |

Adapter `NewFromEnv` helpers stay (used by tests / manual exploration). The engine's workflow path uses `WithCredential(Credential)` (or per-driver equivalent) only — `os.Getenv` deleted from the workflow code path.

---

## 6. Failure modes (decisive, no silence)

| Condition | Where caught | Error | User remediation |
|---|---|---|---|
| No `~/.config/kilroy/auth.toml` and no project config | Config loader | `auth.ErrNoConfig` | `kilroy auth init` |
| `requires` matches no configured chain | Resolver step 2 | `auth.ErrNoChainForRequirement` | Add a chain to user config; `auth defaults` shows the template |
| `requires` matches multiple chains, no binding | Resolver step 2 | `auth.ErrAmbiguousAuthChain` | Add a `[bindings]` entry |
| Chain's sources all absent | Resolver step 4 | `auth.ErrChainExhausted` | Add an env var or log into the relevant CLI; `auth check` shows skip reasons |
| Source vanished between prelaunch and execution | Execution materialization | `auth.ErrSourceVanished` | Restore the source; rerun |
| `claude_cli` route picked but `ANTHROPIC_API_KEY` set in env without scrub | Cannot happen — binder always scrubs | — | — |

---

## 7. Artifacts

`prelaunch_validation.json` — per agent node:
```json
"auth": {
  "chain_name": "anthropic_claude_cli",
  "source": { "kind": "cli_session", "name": "claude" },
  "status": "ok",
  "skipped": []
}
```

`<logs_root>/<node_id>/resolution.json` — authoritative; matches prelaunch under normal runs:
```json
"auth": {
  "chain_name": "anthropic_claude_cli",
  "source": { "kind": "cli_session", "name": "claude" },
  "method": "cli_oauth",
  "provider": "anthropic",
  "fallback_rank": 0,
  "skipped": []
}
```

`progress.ndjson` — new event:
```json
{"event":"auth_credential_selected","node_id":"agent","provider":"anthropic","method":"cli_oauth","chain_name":"anthropic_claude_cli","source_kind":"cli_session","source_name":"claude"}
```

---

## 8. Work units

Order respects dependencies. Parallelization markers indicate which can run as concurrent worker tasks against an isolated worktree.

### A1 — Foundational types + AuthBindingResolver (sequential, foundation)

- New package `internal/auth/binding/`.
- Types: `Requirement{Provider, Method, Tool}`, `Source{Kind: env_var|cli_session, Name|Tool}`, `Chain{Name, Requires, Sources}`, `Binding{Key, ChainName}`, `Snapshot`, `Resolver` interface.
- Resolver implements §3 algorithm.
- Typed errors: `ErrNoConfig`, `ErrNoChainForRequirement`, `ErrAmbiguousAuthChain`, `ErrChainExhausted`, `ErrSourceVanished`.
- Unit tests: binding hit, single-chain match, zero/multi-chain errors, source fallback within chain, exhausted chain, source-vanished, all error messages include candidates + remediation.

### A2 — Templates as data (parallel after A1)

- `internal/auth/data/default_chains.toml` shipped via `//go:embed`.
- Inventory: anthropic/openai/google × api_key/cli_oauth, with `_KILROY` env names *first* in source lists, plus full multi-name fallback for google.
- Bindings table maps each `(provider, method, tool?)` tuple to its conventional chain name.
- Used only by `auth defaults` (printed verbatim) and `auth init` (sampled). Never read at runtime.

### A3 — Config loading (parallel after A1)

- Reader for `~/.config/kilroy/auth.toml` and `<project>/.kilroy/auth.toml`.
- Merge by chain name and binding key — project replaces user wholesale.
- Returns `auth.ErrNoConfig` when neither file exists.
- Tests: user-only, project-only, both-with-overlap, malformed file, missing both.

### A4 — Per-driver credential binder (parallel after A1)

- New `internal/attractor/engine/credential_binder.go` with `Bind(driver, snapshot, exec) BindResult`.
- Per-driver materialization per §5 table.
- Engine call sites that build adapters / tmux env consume `BindResult`. Specifically: `internal/attractor/engine/agent_router.go` (API path adapter construction), `internal/attractor/agents/tmux_handler.go` (env construction).
- Tests per driver, especially: `claude_cli` scrubs `ANTHROPIC_API_KEY` from child env even when set in parent.

### A5 — Policy.toml refactor (sequential, after A1 + A3)

- Replace every `[chain.auth] env_var = "..."` and `cli = "..."` with `requires = { provider, method, tool? }`.
- `internal/policy/resolver.go` reachability check consults `auth.Resolver` instead of inspecting `env_var` / `cli` directly.
- Atomic; old `[chain.auth]` shape removed.
- Tests in `internal/policy/` updated.

### A6 — Prelaunch snapshot + execution consistency (sequential, after A1–A5)

- `internal/attractor/engine/prelaunch.go` resolves auth per agent node; records in `prelaunch_validation.json`.
- Snapshot frozen for the run (kept in run state).
- Execution path (`agent_router.go`, `tmux_handler.go`) re-materializes the named source at execution time; missing source → `ErrSourceVanished`.
- `engine/policy_class_persist.go` writes the auth block into per-node `resolution.json` (authoritative).

### A7 — Progress event (sequential, after A6)

- Emit `auth_credential_selected` from `engine.ResolveAgentClass` at the same point `policy_class_resolved` fires.
- Test in `internal/attractor/engine/policy_class_test.go`.

### A8 — CLI surfaces (parallel sub-units after A1, A2, A3)

- A8a: `kilroy auth defaults` — prints `default_chains.toml` verbatim.
- A8b: `kilroy auth init` — implements §4 algorithm.
- A8c: `kilroy auth list` enhancement — adds `referenced_by: [chain1, ...]` per detected entry; new `--chains` view shows configured chains and per-chain resolution.
- A8d: `kilroy auth check` — for each configured binding/chain, runs the resolver and reports per-chain status.
- All four are independent sub-files; can fan out to separate workers.

### A9 — Docs (parallel after A6)

- `AGENTS.md` Auth Resolution section.
- v2 plan §8 rewritten to integrated shape (delta in this branch).
- New `docs/auth.md` walks through `init → list → check → run` end-to-end.

### Deferred (not v1 foundation)

- `kilroy auth add / set / remove` programmatic editing — hand-edit + `auth init` is enough.
- `kilroy auth profile <name>` env switching — current layers cover most cases.
- Per-class auth differentiation in policy.toml (capability ships, use defers; bindings + multiple chains-per-tuple already enable it).
- `kilroy auth init --rescan` for incremental updates.

---

## 9. Parallelization strategy

Foundation (A1) lands sequentially first — everything depends on it. Then a single parallel campaign covers A2 + A3 + A4 + A8 sub-units. After campaign merges, sequential A5 → A6 → A7. Docs (A9) parallel with A6 after artifacts shape stabilizes.

Worker campaign (post-A1):

| Worker | Scope (touches) | Estimated diff size |
|---|---|---|
| W1 | A2 — `internal/auth/data/default_chains.toml`, `internal/auth/binding/templates.go` (embed loader) | small |
| W2 | A3 — `internal/auth/binding/config.go`, tests | small |
| W3 | A4 anthropic-axis — `credential_binder.go` `anthropic_sdk` + `claude_cli` cases + tests | medium |
| W4 | A4 openai-axis — `openai_sdk` + `codex_cli` cases + tests | medium |
| W5 | A4 google-axis — `google_sdk` + `gemini_cli` cases + tests | medium |
| W6 | A8a + A8b — `cmd/kilroy/auth_defaults.go`, `auth_init.go` | small-medium |
| W7 | A8c + A8d — `cmd/kilroy/auth_list.go` enhancement, `auth_check.go` | small-medium |

Each worker gets:
- A scope-narrowing prompt that names the only files it should touch.
- The shared types from A1 as starting context.
- A clear "definition of done" pointing at this plan's acceptance criteria.

Reviewer (me) merges in dependency order: A2 first (templates needed for A8b), A3 second, A4 sub-units third (independent of each other; merge in any order), A8 sub-units last. Each merge is non-fast-forward to preserve worker authorship.

After campaign: A5 (policy refactor) sequential, A6 (prelaunch+execution) sequential, A7 (event) sequential, A9 (docs) parallel sub-units if desired.

---

## 10. Acceptance criteria (definition of done)

1. `grep -r "ANTHROPIC_API_KEY\|OPENAI_API_KEY\|GOOGLE_API_KEY\|GEMINI_API_KEY" internal/policy internal/llm internal/attractor/engine` returns matches only in `internal/auth/data/`, `internal/auth/`, and tests. Zero hits in policy.toml, adapter call sites for the workflow path, or engine routing.
2. Fresh machine + no auth config → `kilroy run implement` fails decisively at prelaunch with `auth.ErrNoConfig` and the `auth init` remediation. **Verified with a real run.**
3. `kilroy auth init` produces an inspectable user file matching detected reality, with absent sources preserved as comments.
4. `kilroy auth check` correctly reports `ok` / `error` per configured binding+chain.
5. With both `ANTHROPIC_API_KEY_KILROY` and `ANTHROPIC_API_KEY` set, an SDK run uses `ANTHROPIC_API_KEY_KILROY` (because user config lists it first); the chain decision is recorded in `resolution.json`.
6. With `ANTHROPIC_API_KEY` set in env and a `claude_cli` route picked, the run uses the CLI subscription (not the env key) because the binder scrubbed the env. **Verified in run artifacts.**
7. Matrix runs (claude_cli, anthropic_sdk, openai_sdk, google_sdk) all complete; each `resolution.json` shows the right chain + source.
8. Two configured chains satisfying the same `requires` tuple, no `[bindings]` entry → prelaunch fails with `ErrAmbiguousAuthChain` listing candidates.
9. Existing E2E test (`TestRunImplement_E2E_FakeProvider`) passes — the fake-provider scaffold gets a minimal `auth init` run as part of test setup so the new prelaunch passes for the fake claude path.
10. Pre-existing test suite stays green; new tests cover the resolver, config layering, per-driver binder (with the env-scrub assertion as a load-bearing test).

---

## 11. References

- v2 final plan: `docs/plans/2026-05-01-kilroy-v2-final-plan.md` — §6 (policy resolver), §8 (auth as discovery), §9 (agent-conversation tuple)
- Block 5 (auth detection): `internal/auth/`
- Block 6 Step 5 (AuthResolver injection): pre-paid by this work
- Plan §13.7 (`class=` vs `agent_class=`): unrelated; mentioned because the same "split a tangled attribute" pattern applies to `[chain.auth]` here

*This plan is the source of truth for v1 of the auth/class integration. Drift from this plan in implementation should update this doc first, then code.*
