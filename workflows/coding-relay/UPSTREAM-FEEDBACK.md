# Upstream feedback from coding-relay design + validation

Worktree branch: `worktree-coding-relay` (off `feat/v2-reframe`).
Date: 2026-05-04.

This is a running list of friction points and bugs found while
authoring `workflows/coding-relay/` and trying to validate it. The
workflow exercises mixed driver routing on purpose — the goal was to
hit the V2 surface in a way single-provider workflows don't. It worked.

## Findings

### F1 — BUG: `providerAndBackendForDriver` has no `opencode` case (P0 for any opencode workflow)

**Location:** `internal/attractor/engine/agent_router.go:147`

**Current behavior:**
```go
func providerAndBackendForDriver(driver string) (string, BackendKind) {
    switch driver {
    case "claude_cli":   return "anthropic", BackendCLI
    case "anthropic_sdk":return "anthropic", BackendAPI
    case "codex_cli":    return "openai",    BackendCLI
    case "openai_sdk":   return "openai",    BackendAPI
    case "gemini_cli":   return "google",    BackendCLI
    case "google_sdk":   return "google",    BackendAPI
    default:             return "", ""    // <-- opencode falls through
    }
}
```

**Symptom:** Any agent node with `agent_tool="opencode"` fails route
resolution at `agent_route.go:111` with:

```
agent_tool="opencode" maps to driver "opencode" which has no provider mapping
```

This breaks `workflows/multi-tool-exercise/` (the `opencode_write` node
fails today on `feat/v2-reframe`) as well as my new `coding-relay`
workflow. Both `kilroy workflows validate <name>` and any `kilroy run
<name>` invocation that hits an opencode node will fail.

**Reproduction:**
```bash
git checkout feat/v2-reframe
go build -o ./kilroy ./cmd/kilroy
./kilroy workflows validate multi-tool-exercise
# → opencode_write: status=fail, "no provider mapping"
./kilroy workflows validate coding-relay
# → coder: status=fail, same error
```

**Root cause analysis:**

opencode is *legitimately* multi-provider — the user picks a provider
via `--model anthropic/claude-...` or `kimi/kimi-k2`. The driver→provider
mapping assumption ("if I know the driver, I know the provider") that
holds for `claude_cli` (always anthropic) breaks for `opencode`.

Furthermore, `ResolveAgentRoute` in `agent_route.go:105-122` discards the
node's explicit `llm_provider="..."` attribute when `agent_tool=` is set
— the route's `Provider` is taken from `providerAndBackendForDriver`,
not from the user's explicit attribute. So even setting
`llm_provider="kimi"` on an opencode node won't help: the validator
errors out before reaching that code path.

**Fix options (suggested, not implemented):**

1. **Special-case opencode in ResolveAgentRoute.** When
   `agent_tool="opencode"`, *require* an explicit `llm_provider` on the
   node, use that as the route's `Provider`, and fix the backend at
   `BackendCLI`. The driver→provider mapping is bypassed for opencode.
   ```go
   if tool == "opencode" {
       provider := strings.TrimSpace(node.Attr("llm_provider", ""))
       if provider == "" {
           return AgentRoute{}, fmt.Errorf(
               "agent_tool=\"opencode\" requires explicit llm_provider= " +
               "(opencode is multi-provider; the node must say which one)")
       }
       return AgentRoute{
           NodeID:   node.ID,
           Source:   "agent_tool=opencode",
           Provider: provider,
           Model:    node.Attr("llm_model", ""),
           Driver:   "opencode",
           Backend:  BackendCLI,
       }, nil
   }
   ```

2. **Make `providerAndBackendForDriver` return a sentinel for opencode**
   (e.g. `"opencode_multi"` or `""` with a flag) and teach the
   ResolveAgentRoute caller to respect explicit `llm_provider=` when the
   driver is multi-provider. Slightly more invasive but generalizes if
   another multi-provider tool shows up later (`aider`, `cursor`, etc).

Both options preserve the intent of `multi-tool-exercise` (opencode
defaulting to whatever provider the stylesheet specifies) while
allowing coding-relay's explicit `llm_provider="kimi"` choice.

**Tests to add when fixed:**

- `TestResolveAgentRoute_OpencodeRequiresExplicitProvider` — empty
  `llm_provider` on opencode node fails with a clear message.
- `TestResolveAgentRoute_OpencodeAcceptsKimi` — `llm_provider="kimi"` +
  `llm_model="kimi-k2"` resolves to `Provider=kimi, Driver=opencode,
  Backend=BackendCLI`.
- `TestWorkflowsValidate_MultiToolExercise` — re-add a green run of the
  existing workflow (currently silently broken).

---

### F2 — KNOWN GAP: opencode is outside the auth binder (already documented)

**Location:** `internal/attractor/agents/templates/opencode.go:36-53`

**Current behavior:** Documented gap. The opencode template's
`PrepareSession` writes `OPENCODE_CONFIG_CONTENT` with a *hard-coded*
anthropic-only provider block:

```go
config := map[string]any{
    "provider": map[string]any{
        "anthropic": map[string]any{
            "options": map[string]any{
                "apiKey": "{env:ANTHROPIC_API_KEY}",
            },
        },
    },
}
```

So even if F1 is fixed and a node's route resolves to `Provider=kimi`,
`Driver=opencode`, the launched opencode subprocess would still only
have anthropic configured in its session — kimi would be unreachable.

**Suggested follow-up:** Make the opencode template produce
`OPENCODE_CONFIG_CONTENT` from the resolved route's `Provider` field
(plus a sane multi-provider default for graphs that don't specify).
Concretely, when `Provider="kimi"`, emit:

```json
{
  "provider": {
    "kimi": {
      "options": {
        "apiKey": "{env:KIMI_API_KEY}",
        "baseURL": "https://api.kimi.com/coding"
      }
    }
  }
}
```

Information needed already exists in `internal/providerspec/builtin.go`
(see the `kimi` builtin for `DefaultBaseURL` + `APIKeyEnv`). The template
just needs the resolved `AgentRoute.Provider` threaded through.

---

### F3 — UX: `kilroy workflows validate` exit code is success even when nodes fail

**Observed:**
```bash
$ ./kilroy workflows validate multi-tool-exercise; echo "rc=$?"
{ ... "status": "fail", "summary": { "ok": 2, "fail": 1 } }
rc=0
```

The JSON body says `"status": "fail"` but the process exits 0. CI
scripts running `kilroy workflows validate` in a loop won't notice the
failure unless they parse JSON. Suggest: exit non-zero when any node
status is `fail` (or when the top-level `status` field is `fail`).

(Did not test this exhaustively — could be that `--strict` or similar
already does this.)

---

### F4 — DESIGN: no class fits the "fast cheap CLI-coding" niche

The policy classes (`hard_coding`, `quick_easy`, `deep_investigation`,
`frontend_aesthetic`, `architectural_critique`) all chain
anthropic-first or openai-first. There's no class for "fast cheap coding
through opencode + kimi" or "fast cheap coding through any local
multi-provider tool". The coding-relay workflow had to use explicit
`agent_tool=opencode` + `llm_provider=kimi` instead of a clean class.

This isn't a bug — classes are intentionally curated and the right
ones can be added later. But it does mean class resolution can't be
exercised on the most interesting cost-conscious axis (opencode +
kimi-k2). Worth flagging if a "low-cost-coding" class is on the
roadmap.

---

### F5 — DOC: `kilroy workflows list` hides experimental workflows by default

Discovered when `coding-relay` (with `experimental=true`) didn't appear
in `kilroy workflows list`. The `--all` flag is documented in `--help`
but not surfaced in any obvious doc / skill. Suggest: tiny note in
`skills/using-kilroy/SKILL.md` under the workflows-list section.

This is a minor doc nit, not a bug. Filed for completeness.

---

## What's NOT a problem

The auth binder + class resolver correctly resolved 3 of 4 nodes:

- `planner` (no `agent_tool`, explicit `llm_provider=anthropic`,
  `llm_model=claude-sonnet-4.6`) → resolved to `anthropic_sdk` driver
  with backend=API. ✓
- `critic` (`agent_tool=codex`, explicit `llm_provider=openai`,
  `llm_model=gpt-5`) → resolved to `codex_cli` driver, binary found. ✓
- `status_pulse` (`agent_class=quick_easy`) → resolved through the
  policy chain to `claude_cli` + `claude-haiku-4-5` + `cli_oauth` with
  source `claude` (the user's logged-in CLI session). ✓

So the routing surface works correctly for all the well-supported paths
— the opencode case is the lone exception, and it has a clean fix.

---

## Recommended sequencing

1. **Fix F1** (small, contained — option 1 in the suggested fixes).
   Unblocks the coding-relay coder node and re-greens
   multi-tool-exercise. Add the three tests.
2. **Then F2** (template config generation from route.Provider). Needed
   to actually run kimi through opencode end-to-end.
3. **F3, F4, F5** are nice-to-haves; defer.

---

## Update — smoke-test round (2026-05-04, after F1+F2 landed upstream)

After pulling in the upstream F1+F2 fix (commit `87ae628`), validation
went 4/4 ok for coding-relay. Smoke test surfaced two more issues
worth documenting; first one is fixed in this worktree.

### F6 — BUG: F2 wiring gap — `KILROY_AGENT_PROVIDER` set in child env, read from parent process env (FIXED in this worktree)

**Symptom (from smoke run `01KQSPMPZDJV6NPDZB5DT1BHX1`):**

opencode launched with `--model anthropic/kimi-k2`, errored with:
```
Model not found: anthropic/kimi-k2.
```

**Root cause:** F2's `OpencodeBuildArgs` reads `KILROY_AGENT_PROVIDER`
via `os.Getenv` (from the kilroy parent process env). But
`tmux_handler.go:172-177` only sets that key in the *child env map*
that gets handed to tmux. The parent process never has it set, so
BuildArgs always defaulted to `"anthropic"` and produced
`anthropic/kimi-k2` regardless of route.Provider.

**Fix applied:** Thread `provider` through `Template.BuildArgs` /
`Template.BuildCommand` the same way `authMethod` is threaded — add a
5th positional arg. tmux_handler passes `route.Provider` at the call
site. opencode's BuildArgs reads it from the param, not from the
parent's env. The other three templates (claude, codex, gemini) take
provider as `_` since they're single-provider.

Commit in this branch: `e5097ef fix(agents): thread route.Provider
through Template.BuildArgs`. Tests:
`TestOpencodeBuildArgs_UsesProviderArg` (replaces the old
`_UsesProviderPrefixFromEnv`).

**Related cleanup**: the F2 commit's docstring on opencode template
still talks about "tmux_handler stashes route.Provider /
route.Model under KILROY_AGENT_PROVIDER / KILROY_AGENT_MODEL in env"
— with the fix, BuildArgs no longer reads env, only PrepareSession
does (which IS handed the env map). The doc was rewritten in the same
commit to match.

### F7 — UX: `--config` requires `repo.path`, ignores `--workspace` flag

**Symptom (from smoke run `01KQSPKZ4SN2RS37H1SH80F9Y1`):**

Setting `--workspace /tmp/coding-relay-smoke --config workflows/coding-relay/run.example.yaml`
produced `repo.path is required`, then with `repo.path: /tmp` in the
config, produced `not a git repo: /tmp` — `--workspace` did not override
the config's repo.path.

**Why it's friction:** A workflow that ships a per-workflow run config
(needed for non-auto-detect providers like kimi) becomes
not-relocatable: every consumer has to copy the config and edit
`repo.path` to point at their own target repo. The `--workspace` flag
exists exactly for "where do I run this against," but doesn't override
the config.

**Suggested fix:** When both are provided, `--workspace` should win
over `cfg.Repo.Path`. Or, define which has precedence (and document
it). Today, neither — config-required-and-not-overrideable forces
copy-and-edit ergonomics.

Workaround: hardcode a placeholder in run.example.yaml (currently
points at `/tmp/coding-relay-smoke`) and document that consumers must
edit it.

### F9 — BUG: F2's `buildOpencodeConfig` emits incomplete config for non-native opencode providers (FIXED in this worktree)

**Symptom:** After F6 was fixed and opencode launched with the correct
`--model kimi/kimi-k2`, opencode immediately errored:
```
ProviderModelNotFoundError: ProviderModelNotFoundError
 data: { providerID: "kimi", modelID: "kimi-k2", suggestions: [] }
```

**Root cause:** `buildOpencodeConfig` (the F2-introduced helper) emits
a minimal block:
```json
{ "provider": { "kimi": { "options": { "apiKey": "...", "baseURL": "..." } } } }
```

That works for opencode-native providers (anthropic / openai / google)
because opencode's own registry knows them. For kilroy's custom
providers (kimi, zai, cerebras, minimax, inception) opencode has zero
native knowledge — without `npm`, `name`, and `models` fields the
launch is rejected.

**Fix applied** (commit `2f252d4`):

`buildOpencodeConfig(provider, model)` now distinguishes "native
opencode provider" from "kilroy-custom" by `ProfileFamily != provider`
(native: anthropic→anthropic, openai→openai, google→google; custom:
kimi→openai, zai→openai, etc.). For custom providers it emits the
full declaration:

```json
{
  "provider": {
    "kimi": {
      "npm": "@ai-sdk/anthropic",
      "name": "Kimi",
      "options": {
        "apiKey": "{env:KIMI_API_KEY}",
        "baseURL": "https://api.kimi.com/coding/v1"
      },
      "models": { "kimi-k2": {} }
    }
  }
}
```

`npm` package is chosen from the API protocol:
| Protocol | npm package |
|---|---|
| `anthropic_messages` | `@ai-sdk/anthropic` |
| `openai_chat_completions` | `@ai-sdk/openai-compatible` |
| `openai_responses` | `@ai-sdk/openai` |
| `google_generate_content` | `@ai-sdk/google` |

For `anthropic_messages` the baseURL gets `/v1` appended because the
`@ai-sdk/anthropic` package adds `/messages` to whatever baseURL is
provided.

`PrepareSession` now reads `KILROY_AGENT_MODEL` from the env map (it
was already set by tmux_handler) and passes it to buildOpencodeConfig.

Tests added: `TestBuildOpencodeConfig_Anthropic_NativeMinimalShape`,
`TestBuildOpencodeConfig_Kimi_FullCustomDeclaration`,
`TestBuildOpencodeConfig_Zai_OpenAICompatibleNPM`,
`TestOpencodePrepareSession_HonorsKilroyAgentProviderAndModel`.

### F10 — DESIGN QUESTION: Kimi providerspec endpoint mismatch with the user's actual key

**Symptom:** With F1+F2+F6+F9 all fixed, opencode launches with the
right config and dials https://api.kimi.com/coding/v1/messages —
returns 401 "Invalid Authentication". The user's `KIMI_API_KEY_KILROY`
is valid against `https://api.moonshot.ai/v1/chat/completions`
(verified by direct curl) but invalid against the kimi.com endpoint.

**The wrinkle:** Moonshot ships at least two API products with
different keys:
- `api.moonshot.ai` — OpenAI-compatible chat completions, models
  `moonshot-v1-128k`, `moonshot-v1-8k`, `kimi-k2.5`
- `api.kimi.com/coding` — anthropic-messages compatible, models like
  `kimi-k2` (the "Kimi Coding" product)

Kilroy's `providerspec.Builtin("kimi")` hardcodes
`https://api.kimi.com/coding` + `anthropic_messages` protocol. A user
with a `api.moonshot.ai` key will always 401.

**Decisions needed (not really an upstream bug — a product question):**

1. **Should kilroy carry both as separate provider entries?** E.g.
   `kimi` (current — kimi.com, anthropic protocol, kimi-k2) and
   `moonshot` (new — moonshot.ai, openai protocol, kimi-k2.5)?
2. **Should the provider entry support endpoint overrides via run
   config?** E.g. `llm.providers.kimi.base_url = "..."` /
   `llm.providers.kimi.protocol = "..."` to swap between products
   without forking the spec.
3. **Should the workflow document which key the user needs?**
   Currently the README says "set KIMI_API_KEY" but doesn't say
   "specifically a Kimi Coding (kimi.com) key, not a generic Moonshot
   key."

This is the one item I can't make airtight in the worktree without
either changing the providerspec (decision #1 or #2) or asking the
user to source a different key.

### F11 — DESIGN: engine writes `.kilroy/TASK.md` per node, collides with workflow-author files on case-insensitive filesystems

**Symptom:** A workflow that wants its planner to write a per-iteration
file at `.kilroy/task.md` (lowercase) runs into a name collision: the
engine writes `.kilroy/TASK.md` (uppercase) on every node entry, with
the current node's prompt as content. On macOS HFS+ / APFS (the most
common dev environment) the filesystem is case-insensitive — so
`.kilroy/task.md` and `.kilroy/TASK.md` are the same inode. The
engine's per-node write happens last (just before each node runs), so
the planner's content is silently clobbered every time the engine
enters the next node.

**Concrete:** in run `01KQSR85JRD9GD9SMZB0M7HSJ8`, planner wrote its
scoped task description to `.kilroy/task.md`. When the engine
transitioned to the coder node, it wrote `# Task: coder\n\nYou are
the coder...` to `.kilroy/TASK.md` — overwriting the planner's
content. The coder then read `.kilroy/task.md` and saw its own prompt
instead of the planner's task. (The coder kept going on
INPUT.md alone, so the run wasn't fully blocked, but the
planner-coder discipline was lost.)

**Workaround in this workflow:** rename to `.kilroy/plan.md` (no
collision with engine names). Updated graph.dot, README.

**Suggested upstream fix:** any of:
1. Engine writes its prompt to a name workflow authors can't accidentally
   reach for: `.kilroy/_engine_prompt.md` or `.kilroy/.task.md` (dotfile)
   or `.kilroy/.engine/TASK.md`.
2. Document the reservation: "the `.kilroy/` directory has a reserved
   name list — TASK.md, INPUT.md, CONTEXT.md, decision.md, package/,
   data/. Don't write into those." Plus a launch-time linter that
   warns when a workflow's prompts reference these names.
3. Document the case-sensitivity issue specifically. Workflow authors
   on Linux won't see this; macOS authors will get bitten on first run.

This footgun is invisible until you actually run a workflow and inspect
the artifacts. Worth at least the lint or doc.

### F8 — UX: stale-build detection is not git-worktree-aware

**Symptom:**

Running `kilroy run` from inside a git worktree always trips the stale
build warning. `go version -m ./kilroy` reports
`vcs.revision=<parent-repo-HEAD>` because Go's `debug.ReadBuildInfo`
sees the parent repo's HEAD when building from a worktree, not the
worktree's HEAD.

**Concrete consequence:** Every dev session inside `.claude/worktrees/...`
needs `--confirm-stale-build`, which defeats the safety check (real
stale binaries get bypassed too). On a clean rebuild from worktree HEAD
e11d4977d8f5, the embedded vcs.revision was 93c92e57e7c8 (parent repo's
HEAD).

**Suggested fix:** In `cmd/kilroy/stale_build.go::binaryVCSRevision`,
detect git worktrees (e.g. by checking whether the binary's repo is a
linked worktree via `git rev-parse --git-common-dir` vs `--git-dir`)
and prefer the actual git HEAD of the working tree the binary was
built in over `debug.ReadBuildInfo`. Alternatively, bake the build's
git revision into a `-ldflags='-X main.embeddedBuildRevision=...'` at
go-build time so it's authoritative regardless of how Go reads VCS
info.

This is a minor ergonomic; not a P0. But it'll bite any developer
working in worktrees on the kilroy repo itself, so worth fixing.
