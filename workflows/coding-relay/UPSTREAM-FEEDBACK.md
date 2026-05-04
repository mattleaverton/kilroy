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

After F1 + F2, the coding-relay workflow can be smoke-tested and likely
exposes more findings.
