# Auth in Kilroy

Kilroy doesn't store credentials, doesn't rotate them, and doesn't own their lifecycle. It *discovers* what credentials live on your machine, *resolves* which one to use for each workflow run, and *materializes* the right form (env var, isolated config file, scrubbed env) for each driver.

This document walks the four user-facing surfaces: `kilroy auth defaults`, `kilroy auth init`, `kilroy auth list`, `kilroy auth check`.

## Mental model

Three distinct things, each with its own fallback layer:

1. **Auth chain** — for a given `(provider, method)` pair, an ordered list of credential sources kilroy will try. First usable wins.
2. **Policy** — for an abstract class like `hard_coding`, an ordered list of route candidates (model + driver + provider/method). First reachable wins.
3. **Runtime failover** — what to do when a call fails mid-run. Outside the scope of this doc.

The auth chain layer answers: *"of all the ways I could authenticate to provider X via method Y, which one should I actually use?"* Policy answers a separate question: *"of all the routes I could take to satisfy class Z, which one should I actually take?"*

## Files

| Path | Purpose |
|---|---|
| `~/.config/kilroy/auth.toml` (user) | The single source of truth for active runtime auth resolution on this machine. Created by `kilroy auth init`. |
| `<project>/.kilroy/auth.toml` (project, optional) | Project-level overrides. Project chains and bindings replace user-level entries by name (no source-list merging). |
| `internal/auth/data/default_chains.toml` (built-in templates) | Inspectable template printed by `kilroy auth defaults`. **Never read at runtime for active resolution.** |

There is no compiled-in fallback. If neither user nor project config exists, `kilroy run` fails decisively at prelaunch with `ErrNoConfig` and a remediation hint.

## Setup workflow

The expected one-time setup on a new machine:

```bash
kilroy auth defaults              # see what kilroy supports
kilroy auth init                  # generate ~/.config/kilroy/auth.toml from detection
kilroy auth check                 # verify every chain has a usable source
kilroy run <workflow> ...         # actually run a workflow
```

`kilroy auth init` is the magic-but-explicit step. It detects what's on your machine right now and writes a config that lists detected sources as **active TOML entries** and undetected sources as **commented TOML lines**. You can later uncomment a commented source after you set the env var or log into the relevant CLI.

## Anatomy of `auth.toml`

```toml
# Bindings disambiguate: when a Requirement matches multiple chains,
# the binding picks one by name.
[bindings]
"anthropic/api_key"          = "anthropic_api_key"
"anthropic/cli_oauth/claude" = "anthropic_claude_cli"
"openai/api_key"             = "openai_api_key"
"openai/cli_oauth/codex"     = "openai_codex_cli"
"google/api_key"             = "google_api_key"
"google/cli_oauth/gemini"    = "google_gemini_cli"

# Each chain says "for this Requirement, here are the sources to try, in order."
[chains.anthropic_api_key]
[chains.anthropic_api_key.requires]
provider = "anthropic"
method   = "api_key"

  [[chains.anthropic_api_key.sources]]
  kind = "env_var"
  name = "ANTHROPIC_API_KEY_KILROY"

  # [[chains.anthropic_api_key.sources]]
  # kind = "env_var"
  # name = "ANTHROPIC_API_KEY"
```

The `_KILROY` precedence is the convention for separating per-tool budgets — kilroy uses your `_KILROY`-suffixed key first, leaving your unsuffixed key for daily Claude/Codex/Gemini CLI use. You can flip the order or remove either source by editing the file.

## Resolution flow at runtime

For each agentic node in a workflow:

1. The policy resolver picks a route candidate based on the node's `agent_class` attribute. The candidate carries a `requires = { provider, method, tool? }` declaration.
2. The auth resolver consults `[bindings]` for the requirement key (`<provider>/<method>[/<tool>]`). If a binding entry exists, that's the chain.
3. If no binding entry exists, the resolver looks for chains whose `requires` matches. If exactly one matches, it's used; if zero match, it's `ErrNoChainForRequirement`; if multiple match, it's `ErrAmbiguousAuthChain`.
4. The chosen chain's sources are walked in order. The first usable source wins. None usable → `ErrChainExhausted`.
5. The chosen source identity (chain name + source kind + source name) is written into `prelaunch_validation.json` and `<stage_dir>/resolution.json`.
6. At execution time, the source is **re-read by name** to materialize the credential value. If the source has vanished between prelaunch and execution, kilroy fails with `ErrSourceVanished` — never re-walks the chain mid-run.

## Per-driver materialization

Different drivers receive credentials in different forms. The credential binder owns this:

| Driver | What gets materialized |
|---|---|
| `anthropic_sdk` | Pass the env var value to the SDK constructor |
| `claude_cli` | **Scrub `ANTHROPIC_API_KEY` from the child process env**, so the CLI uses the logged-in subscription session (not the env key) |
| `openai_sdk` | Pass to SDK |
| `codex_cli` | Write isolated `<stage>/.codex/auth.json`, set `CODEX_HOME` |
| `google_sdk` | Pass to SDK; record the env-var-name actually used (Google has multiple equivalents) |
| `gemini_cli` | Set canonical `GEMINI_API_KEY` env (or rely on OAuth when source is `cli_session`) |

The `claude_cli` env scrub is the load-bearing case: without it, setting `ANTHROPIC_API_KEY` in your shell would silently make `kilroy run` use the env key on a CLI route — billing the wrong account. The binder removes it from the child env before invoking `claude`.

## Failure modes

Each is decisive — kilroy never silently falls back to a different method or skips auth entirely.

| Error | When | Remediation |
|---|---|---|
| `ErrNoConfig` | No user/project auth.toml | `kilroy auth init` |
| `ErrNoChainForRequirement` | Policy candidate's `requires` matches no chain | Add a chain to your auth.toml; `kilroy auth defaults` shows the template |
| `ErrAmbiguousAuthChain` | Multiple chains match, no binding entry | Add `[bindings]` entry resolving the ambiguity |
| `ErrChainExhausted` | Chain selected but no source is usable | Add an env var, run the relevant CLI auth, or update the chain's source list |
| `ErrSourceVanished` | Source was OK at prelaunch but missing at execution | Restore the source; rerun |
| `ErrUnknownChain` | A binding names a chain that doesn't exist | Fix the chain name in `[bindings]` |

## Surfaces

### `kilroy auth defaults`

Prints `internal/auth/data/default_chains.toml` verbatim. Useful when you want to start from kilroy's recommended shape (e.g., piped into a fresh user file: `kilroy auth defaults > ~/.config/kilroy/auth.toml`, then edit), but `kilroy auth init` is the recommended path because it intersects templates with current detection.

### `kilroy auth init [--force] [--path <dir>]`

Discovers what's on the machine, intersects with templates, writes user config. Idempotent: refuses overwrite without `--force`. `--path` overrides destination directory (file is always named `auth.toml`).

Output summary lists chains with usable sources and chains without. JSON via `--json`.

### `kilroy auth list [--chains] [--pretty | --json]`

Detection-centric (default): shows every credential found on this machine, with `referenced_by: [chain1, chain2]` annotation per entry showing which configured chains use it.

Chain-centric (`--chains`): shows configured bindings + chains and what each resolves to.

### `kilroy auth check [--project <dir>] [--pretty | --json]`

Standalone diagnostic: runs the resolver against every binding+chain in your config and reports per-chain status. Independent of any specific run/workflow. Exit code 1 if any chain has no usable source.

## Per-class auth differentiation (capability ships, use defers)

Multiple chains can satisfy the same `(provider, method)` requirement. Today, the default config uses one chain per requirement, but the schema supports e.g. `anthropic_kilroy_api` (your daily key) vs `anthropic_org_api` (a shared org key) coexisting. A future feature would let policy candidates declare `auth_ref = "anthropic_org_api"` to override the binding default for specific classes — for example, `class hard_coding` uses a more expensive key, `class quick_easy` uses a cheap one. The schema is in place; the policy.toml side ships single chains for v1.

## What's NOT covered

The auth-chain resolver model fits one-driver-one-chain tools (claude, codex, gemini). It does NOT cover:

- **opencode**: a multi-provider tool with its own `~/.local/share/opencode/opencode.db` config DB. opencode runs through tmux honor whatever canonical env vars are set in the launcher; they do NOT follow the resolver's chain decision (so `_KILROY` precedence isn't honored, and claude/codex-style env-scrubs don't apply). Treat opencode as a separate auth surface for now.
- **Bare `kilroy attractor run` without `agent_class=`**: legacy stylesheet routing skips the resolver entirely. Class-routed nodes (`agent_class="..."`) flow through the resolver; non-class nodes don't.
- **Provider-specific env vars beyond the chain**: `ANTHROPIC_BASE_URL`, `OPENAI_BASE_URL`, etc. are still read directly from env by the LLM client constructors. These are operational overrides, not credentials, but they can affect routing in ways the chain doesn't see.
