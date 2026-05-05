Generated from ./kilroy auth ... on 2026-05-05

# Kilroy Auth Subcommand Reference

This document describes the `kilroy auth` command surface. All output was captured by running commands against the local kilroy binary.

## Overview

The `kilroy auth` command group manages authentication configuration for LLM providers. It provides commands to inspect defaults, initialize configuration, list detected credentials, verify auth chain resolution, and get remediation suggestions.

```
$ ./kilroy auth --help
usage:
  kilroy auth defaults
  kilroy auth init [--force] [--path <dir>] [--json|--pretty]
  kilroy auth list [--pretty] [--json] [--chains]
  kilroy auth check [--pretty] [--json] [--project <dir>]
  kilroy auth suggest-fix [<provider>]

  defaults prints the default_chains.toml template verbatim.
  init     generates ~/.config/kilroy/auth.toml from the template.
  list     outputs JSON by default; pass --pretty for human-readable.
  list --chains pivots to a chain-centric view (one row per configured binding).
  check    runs the auth resolver for every configured binding and reports status.
```

---

## kilroy auth defaults

Prints the embedded default authentication chains template.

### Invocation

```
$ ./kilroy auth defaults
```

### Output

```
# default_chains.toml — Kilroy auth chain TEMPLATES
#
# PURPOSE
#   This file is shipped as a read-only template embedded in the kilroy binary.
#   It is NEVER read at runtime for active credential resolution.
#   It is used only by:
#     - `kilroy auth defaults`  — prints this file verbatim for user inspection
#     - `kilroy auth init`      — samples each chain, emitting detected sources
#                                 as active TOML and undetected sources as comments
#
# KILROY env precedence (_KILROY suffix)
#   Each api_key chain lists PROVIDER_KILROY env vars FIRST. This allows
#   separate per-tool budget management: set ANTHROPIC_API_KEY_KILROY to a
#   project-specific key, and kilroy will use it instead of the global key.
#   The unsuffixed fallbacks remain available for single-key setups.
#
# See docs/plans/2026-05-02-auth-class-resolver-integration.md for design.

# ---------------------------------------------------------------------------
# Bindings table
# Maps each (provider/method[/tool]) tuple to a conventional chain name.
# User config may override these to point to custom chain names.
# ---------------------------------------------------------------------------

[bindings]
"anthropic/api_key"          = "anthropic_api_key"
"anthropic/cli_oauth/claude" = "anthropic_claude_cli"
"openai/api_key"             = "openai_api_key"
"openai/cli_oauth/codex"     = "openai_codex_cli"
"google/api_key"             = "google_api_key"
"google/cli_oauth/gemini"    = "google_gemini_cli"

# ---------------------------------------------------------------------------
# Chains
# ---------------------------------------------------------------------------

# --- Anthropic: API key ---

[chains.anthropic_api_key]
[chains.anthropic_api_key.requires]
provider = "anthropic"
method   = "api_key"

[[chains.anthropic_api_key.sources]]
kind = "env_var"
name = "ANTHROPIC_API_KEY_KILROY"

[[chains.anthropic_api_key.sources]]
kind = "env_var"
name = "ANTHROPIC_API_KEY"

# --- Anthropic: Claude CLI OAuth ---

[chains.anthropic_claude_cli]
[chains.anthropic_claude_cli.requires]
provider = "anthropic"
method   = "cli_oauth"
tool     = "claude"

[[chains.anthropic_claude_cli.sources]]
kind = "cli_session"
tool = "claude"

# --- OpenAI: API key ---

[chains.openai_api_key]
[chains.openai_api_key.requires]
provider = "openai"
method   = "api_key"

[[chains.openai_api_key.sources]]
kind = "env_var"
name = "OPENAI_API_KEY_KILROY"

[[chains.openai_api_key.sources]]
kind = "env_var"
name = "OPENAI_API_KEY"

# --- OpenAI: Codex CLI OAuth ---

[chains.openai_codex_cli]
[chains.openai_codex_cli.requires]
provider = "openai"
method   = "cli_oauth"
tool     = "codex"

[[chains.openai_codex_cli.sources]]
kind = "cli_session"
tool = "codex"

# --- Google: API key ---
#
# Google's API key is accepted under several names across different tools:
#   GOOGLE_API_KEY              — used by google-cloud-go and some integrations
#   GEMINI_API_KEY              — used by the Gemini SDK and AI Studio
#   GOOGLE_GENERATIVE_AI_API_KEY — used by the Google Generative AI Node SDK
# All three are functionally equivalent; _KILROY variants go first for budget
# isolation, then the canonical names in a sensible fallback order.

[chains.google_api_key]
[chains.google_api_key.requires]
provider = "google"
method   = "api_key"

[[chains.google_api_key.sources]]
kind = "env_var"
name = "GOOGLE_API_KEY_KILROY"

[[chains.google_api_key.sources]]
kind = "env_var"
name = "GEMINI_API_KEY_KILROY"

[[chains.google_api_key.sources]]
kind = "env_var"
name = "GOOGLE_API_KEY"

[[chains.google_api_key.sources]]
kind = "env_var"
name = "GEMINI_API_KEY"

[[chains.google_api_key.sources]]
kind = "env_var"
name = "GOOGLE_GENERATIVE_AI_API_KEY"

# --- Google: Gemini CLI OAuth ---

[chains.google_gemini_cli]
[chains.google_gemini_cli.requires]
provider = "google"
method   = "cli_oauth"
tool     = "gemini"

[[chains.google_gemini_cli.sources]]
kind = "cli_session"
tool = "gemini"
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |

### Notes

- The template is embedded in the binary and never read at runtime
- Used by `kilroy auth init` to generate personalized `auth.toml`
- Supports `_KILROY` suffixed env vars for budget isolation

---

## kilroy auth init

Generates an `auth.toml` configuration file from the default template, sampling detected credentials.

### Invocation

```
$ ./kilroy auth init [--force] [--path <dir>] [--json|--pretty]
```

### Flags

| Flag | Description |
|------|-------------|
| `--force` | Overwrite an existing auth.toml |
| `--path <dir>` | Write to `<dir>/auth.toml` instead of default location (`~/.config/kilroy/`) |
| `--json` | Print machine-readable summary instead of human-friendly output |
| `--pretty` | Print human-readable output (default when not using `--json`) |

### Output (human-readable)

```
$ ./kilroy auth init --force --path /tmp/test_force
Wrote: /tmp/test_force/auth.toml

Chains with usable sources (6):
  • anthropic_api_key  [env:ANTHROPIC_API_KEY_KILROY, env:ANTHROPIC_API_KEY]
  • anthropic_claude_cli  [cli:claude]
  • google_api_key  [env:GEMINI_API_KEY_KILROY, env:GEMINI_API_KEY, env:GOOGLE_GENERATIVE_AI_API_KEY]
  • google_gemini_cli  [cli:gemini]
  • openai_api_key  [env:OPENAI_API_KEY_KILROY, env:OPENAI_API_KEY]
  • openai_codex_cli  [cli:codex]
```

### Output (JSON)

```json
{
  "destination": "/tmp/test_auth/auth.toml",
  "chains": [
    {
      "name": "anthropic_api_key",
      "usable_sources": [
        "env:ANTHROPIC_API_KEY_KILROY",
        "env:ANTHROPIC_API_KEY"
      ],
      "skipped_sources": []
    },
    {
      "name": "anthropic_claude_cli",
      "usable_sources": [
        "cli:claude"
      ],
      "skipped_sources": []
    },
    {
      "name": "google_api_key",
      "usable_sources": [
        "env:GEMINI_API_KEY_KILROY",
        "env:GEMINI_API_KEY",
        "env:GOOGLE_GENERATIVE_AI_API_KEY"
      ],
      "skipped_sources": [
        "env:GOOGLE_API_KEY_KILROY",
        "env:GOOGLE_API_KEY"
      ]
    },
    {
      "name": "google_gemini_cli",
      "usable_sources": [
        "cli:gemini"
      ],
      "skipped_sources": []
    },
    {
      "name": "openai_api_key",
      "usable_sources": [
        "env:OPENAI_API_KEY_KILROY",
        "env:OPENAI_API_KEY"
      ],
      "skipped_sources": []
    },
    {
      "name": "openai_codex_cli",
      "usable_sources": [
        "cli:codex"
      ],
      "skipped_sources": []
    }
  ],
  "ok": 6,
  "no_usable_source": 0
}
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (file written) |
| 1 | Error (e.g., file exists without `--force`) |

### Error Scenarios

**File already exists:**
```
$ ./kilroy auth init
auth config already exists at /Users/matt/.config/kilroy/auth.toml; use --force to overwrite
```

### Notes

- Active sources (detected on this machine) are emitted as live TOML entries
- Undetected sources are commented out; uncomment after adding the credential
- Default location is `~/.config/kilroy/auth.toml`

---

## kilroy auth list

Lists all discovered credentials and their status.

### Invocation

```
$ ./kilroy auth list [--pretty] [--json] [--chains]
```

### Flags

| Flag | Description |
|------|-------------|
| `--pretty` | Human-readable output (mutually exclusive with `--json`) |
| `--json` | Machine-readable JSON output (default) |
| `--chains` | Pivot to chain-centric view (one row per configured binding) |

### Output (default credential-centric JSON)

```json
{
  "kilroy_version": "0.1.0",
  "scanned_at": "2026-05-05T01:39:33Z",
  "platform": "darwin",
  "entries": [
    {
      "id": "anthropic.env.ANTHROPIC_API_KEY",
      "kind": "env_var",
      "provider": "anthropic",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "ANTHROPIC_API_KEY"
      },
      "shadows": [
        "anthropic.claude.cli_oauth"
      ],
      "notes": [
        "key prefix: sk-ant-"
      ],
      "referenced_by": [
        "anthropic_api_key"
      ]
    },
    {
      "id": "anthropic.env.ANTHROPIC_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "anthropic",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "ANTHROPIC_API_KEY_KILROY"
      },
      "shadows": [
        "anthropic.claude.cli_oauth"
      ],
      "notes": [
        "key prefix: sk-ant-"
      ],
      "referenced_by": [
        "anthropic_api_key"
      ]
    },
    {
      "id": "anthropic.claude.cli_oauth",
      "kind": "cli_oauth",
      "provider": "anthropic",
      "tool": "claude",
      "state": "ok",
      "identity": {},
      "source": {
        "file": "/Users/matt/.claude/settings.json",
        "keychain_service": "Claude Safe Storage",
        "keychain_account": "Claude Key"
      },
      "shadowed_by": [
        "anthropic.env.ANTHROPIC_API_KEY",
        "anthropic.env.ANTHROPIC_API_KEY_KILROY"
      ],
      "notes": [
        "env var overrides absent keychain session"
      ],
      "referenced_by": [
        "anthropic_claude_cli"
      ]
    },
    {
      "id": "cursor.cli_oauth",
      "kind": "cli_oauth",
      "provider": "cursor",
      "tool": "cursor",
      "state": "ok",
      "identity": {
        "email": "mleaverton@glowforge.com"
      },
      "expiry": {
        "refresh_token_present": true,
        "refreshable": true
      },
      "source": {
        "file": "/Users/matt/.cursor/cli-config.json",
        "keychain_service": "cursor-access-token",
        "keychain_account": "cursor-user"
      },
      "unreferenced": true
    },
    {
      "id": "github.gh.cli_oauth",
      "kind": "cli_oauth",
      "provider": "github",
      "tool": "gh",
      "state": "ok",
      "identity": {
        "user": "mleavertongf"
      },
      "source": {
        "file": "/Users/matt/.config/gh/hosts.yml",
        "keychain_service": "gh:github.com",
        "keychain_account": "mleavertongf"
      },
      "profiles": [
        {
          "name": "mleavertongf",
          "active": true,
          "identity": {},
          "state": "ok"
        },
        {
          "name": "mattleaverton",
          "active": false,
          "identity": {},
          "state": "ok"
        }
      ],
      "unreferenced": true
    },
    {
      "id": "google.env.GEMINI_API_KEY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GEMINI_API_KEY"
      },
      "shadows": [
        "google.gemini.oauth"
      ],
      "notes": [
        "key prefix: AIza"
      ],
      "unreferenced": true
    },
    {
      "id": "google.env.GEMINI_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GEMINI_API_KEY_KILROY"
      },
      "shadows": [
        "google.gemini.oauth"
      ],
      "notes": [
        "key prefix: AIza"
      ],
      "referenced_by": [
        "google_api_key"
      ]
    },
    {
      "id": "google.env.GOOGLE_GENERATIVE_AI_API_KEY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GOOGLE_GENERATIVE_AI_API_KEY"
      },
      "shadows": [
        "google.gemini.oauth"
      ],
      "notes": [
        "key prefix: AIza"
      ],
      "referenced_by": [
        "google_api_key"
      ]
    },
    {
      "id": "google.gemini.oauth",
      "kind": "cli_oauth",
      "provider": "google",
      "tool": "gemini",
      "state": "ok",
      "identity": {},
      "expiry": {
        "access_token_expires_at": "2026-03-02T13:07:21-06:00",
        "refresh_token_present": true,
        "refreshable": true
      },
      "source": {
        "file": "/Users/matt/.gemini/oauth_creds.json"
      },
      "shadowed_by": [
        "google.env.GEMINI_API_KEY_KILROY",
        "google.env.GEMINI_API_KEY",
        "google.env.GOOGLE_GENERATIVE_AI_API_KEY"
      ],
      "notes": [
        "expired; refreshable"
      ],
      "referenced_by": [
        "google_gemini_cli"
      ]
    },
    {
      "id": "openai.env.OPENAI_API_KEY",
      "kind": "env_var",
      "provider": "openai",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "OPENAI_API_KEY"
      },
      "shadows": [
        "openai.codex.cli"
      ],
      "notes": [
        "key prefix: sk-proj-"
      ],
      "referenced_by": [
        "openai_api_key"
      ]
    },
    {
      "id": "openai.env.OPENAI_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "openai",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "OPENAI_API_KEY_KILROY"
      },
      "shadows": [
        "openai.codex.cli"
      ],
      "notes": [
        "key prefix: sk-proj-"
      ],
      "referenced_by": [
        "openai_api_key"
      ]
    },
    {
      "id": "openai.codex.cli",
      "kind": "cli_oauth",
      "provider": "openai",
      "tool": "codex",
      "state": "ok",
      "identity": {},
      "expiry": {
        "access_token_expires_at": "2026-05-11T21:00:19-05:00",
        "refresh_token_present": true,
        "refreshable": true
      },
      "source": {
        "file": "/Users/matt/.codex/auth.json"
      },
      "shadowed_by": [
        "openai.env.OPENAI_API_KEY",
        "openai.env.OPENAI_API_KEY_KILROY"
      ],
      "referenced_by": [
        "openai_codex_cli"
      ]
    }
  ],
  "summary": {
    "total": 12,
    "ok": 12,
    "expired": 0,
    "missing": 0,
    "ambiguous": 0
  }
}
```

### Output (credential-centric pretty)

```
$ ./kilroy auth list --pretty
Kilroy auth scan — 2026-05-05T01:39:32Z on darwin

OK   anthropic    —                [env_var]
    env=ANTHROPIC_API_KEY
    shadows: anthropic.claude.cli_oauth (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: sk-ant-
    referenced by: anthropic_api_key

OK   anthropic    —                [env_var]
    env=ANTHROPIC_API_KEY_KILROY
    shadows: anthropic.claude.cli_oauth (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: sk-ant-
    referenced by: anthropic_api_key

OK   anthropic    claude           [cli_oauth]
    file=~/.claude/settings.json  keychain=Claude Safe Storage
    shadowed by: anthropic.env.ANTHROPIC_API_KEY, anthropic.env.ANTHROPIC_API_KEY_KILROY
    env var overrides absent keychain session
    referenced by: anthropic_claude_cli

OK   cursor       cursor           [cli_oauth] mleaverton@glowforge.com
    file=~/.cursor/cli-config.json  keychain=cursor-access-token
    (unreferenced in config)

OK   github       gh               [cli_oauth] @mleavertongf
    file=~/.config/gh/hosts.yml  keychain=gh:github.com
    profiles: mattleaverton, *mleavertongf  (* = active)
    (unreferenced in config)

OK   google       —                [env_var]
    env=GEMINI_API_KEY
    shadows: google.gemini.oauth (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: AIza
    (unreferenced in config)

OK   google       —                [env_var]
    env=GEMINI_API_KEY_KILROY
    shadows: google.gemini.oauth (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: AIza
    referenced by: google_api_key

OK   google       —                [env_var]
    env=GOOGLE_GENERATIVE_AI_API_KEY
    shadows: google.gemini.oauth (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: AIza
    referenced by: google_api_key

OK   google       gemini           [cli_oauth]
    file=~/.gemini/oauth_creds.json
    expires: 2026-03-02T13:07Z  refreshable: true
    shadowed by: google.env.GEMINI_API_KEY_KILROY, google.env.GEMINI_API_KEY, google.env.GOOGLE_GENERATIVE_AI_API_KEY
    expired; refreshable
    referenced by: google_gemini_cli

OK   openai       —                [env_var]
    env=OPENAI_API_KEY
    shadows: openai.codex.cli (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: sk-proj-
    referenced by: openai_api_key

OK   openai       —                [env_var]
    env=OPENAI_API_KEY_KILROY
    shadows: openai.codex.cli (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)
    key prefix: sk-proj-
    referenced by: openai_api_key

OK   openai       codex            [cli_oauth]
    file=~/.codex/auth.json
    expires: 2026-05-11T21:00Z  refreshable: true
    shadowed by: openai.env.OPENAI_API_KEY, openai.env.OPENAI_API_KEY_KILROY
    referenced by: openai_codex_cli

Summary: 12 entries — 12 ok, 0 expired, 0 missing, 0 ambiguous
```

### Output (chains view pretty)

```
$ ./kilroy auth list --chains --pretty
Auth chains — current detection

"anthropic/api_key"                  → chain anthropic_api_key          → env:ANTHROPIC_API_KEY_KILROY (ok)
"anthropic/cli_oauth/claude"         → chain anthropic_claude_cli       → cli:claude (ok)
"google/api_key"                     → chain google_api_key             → env:GEMINI_API_KEY_KILROY (ok)
"google/cli_oauth/gemini"            → chain google_gemini_cli          → cli:gemini (ok)
"openai/api_key"                     → chain openai_api_key             → env:OPENAI_API_KEY_KILROY (ok)
"openai/cli_oauth/codex"             → chain openai_codex_cli           → cli:codex (ok)

Summary: 6 ok, 0 error
```

### Output (chains view JSON)

```json
{
  "chains": [
    {
      "binding_key": "anthropic/api_key",
      "chain_name": "anthropic_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "ANTHROPIC_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "anthropic/cli_oauth/claude",
      "chain_name": "anthropic_claude_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "claude"
      },
      "skipped": []
    },
    {
      "binding_key": "google/api_key",
      "chain_name": "google_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "GEMINI_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "google/cli_oauth/gemini",
      "chain_name": "google_gemini_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "gemini"
      },
      "skipped": []
    },
    {
      "binding_key": "openai/api_key",
      "chain_name": "openai_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "OPENAI_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "openai/cli_oauth/codex",
      "chain_name": "openai_codex_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "codex"
      },
      "skipped": []
    }
  ],
  "summary": {
    "ok": 6,
    "error": 0,
    "total": 6
  }
}
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |

### JSON Schema

**Credential entry:**
- `id`: Unique identifier (e.g., `anthropic.env.ANTHROPIC_API_KEY`)
- `kind`: Source type (`env_var`, `cli_session`)
- `provider`: Provider name (`anthropic`, `openai`, `google`)
- `state`: Status (`ok`, `error`)
- `identity`: User identity info (empty object for env vars)
- `source`: Source details (env var name or CLI config file path)
- `shadows`: List of credentials this one takes precedence over
- `notes`: Additional information (key prefixes, etc.)
- `referenced_by`: Chains that reference this credential

**Chain entry:**
- `binding_key`: Provider/method/tool tuple
- `chain_name`: Name of the chain in auth.toml
- `status`: Resolution status (`ok`, `error`)
- `source`: Resolved source details
- `skipped`: Sources that were skipped in the chain

---

## kilroy auth check

Runs the auth resolver for every configured binding and reports status.

### Invocation

```
$ ./kilroy auth check [--pretty] [--json] [--project <dir>]
```

### Flags

| Flag | Description |
|------|-------------|
| `--pretty` | Human-readable output |
| `--json` | Machine-readable JSON output |
| `--project <dir>` | Project root containing `.kilroy/` (default: nearest `.kilroy/`) |

### Output (pretty)

```
$ ./kilroy auth check --pretty
Auth chains — resolution against current detection

OK   anthropic/api_key                → anthropic_api_key          → env:ANTHROPIC_API_KEY_KILROY
OK   anthropic/cli_oauth/claude       → anthropic_claude_cli       → cli:claude
OK   google/api_key                   → google_api_key             → env:GEMINI_API_KEY_KILROY
OK   google/cli_oauth/gemini          → google_gemini_cli          → cli:gemini
OK   openai/api_key                   → openai_api_key             → env:OPENAI_API_KEY_KILROY
OK   openai/cli_oauth/codex           → openai_codex_cli           → cli:codex

Summary: 6 ok, 0 error
```

### Output (JSON)

```json
{
  "checks": [
    {
      "binding_key": "anthropic/api_key",
      "chain_name": "anthropic_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "ANTHROPIC_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "anthropic/cli_oauth/claude",
      "chain_name": "anthropic_claude_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "claude"
      },
      "skipped": []
    },
    {
      "binding_key": "google/api_key",
      "chain_name": "google_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "GEMINI_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "google/cli_oauth/gemini",
      "chain_name": "google_gemini_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "gemini"
      },
      "skipped": []
    },
    {
      "binding_key": "openai/api_key",
      "chain_name": "openai_api_key",
      "status": "ok",
      "source": {
        "kind": "env_var",
        "name": "OPENAI_API_KEY_KILROY"
      },
      "skipped": []
    },
    {
      "binding_key": "openai/cli_oauth/codex",
      "chain_name": "openai_codex_cli",
      "status": "ok",
      "source": {
        "kind": "cli_session",
        "tool": "codex"
      },
      "skipped": []
    }
  ],
  "summary": {
    "ok": 6,
    "error": 0,
    "total": 6
  }
}
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All bindings resolved successfully |
| 1 | Auth config is absent or any binding cannot be resolved |

### Error Scenarios

When a binding cannot be resolved, the check will show an error status and exit with code 1.

---

## kilroy auth suggest-fix

Suggests fixes for authentication issues.

### Invocation

```
$ ./kilroy auth suggest-fix [<provider>]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<provider>` | Optional provider name to filter suggestions |

### Output (no issues found)

```
$ ./kilroy auth suggest-fix
All discovered credentials are healthy. Nothing to fix.
```

```
$ ./kilroy auth suggest-fix anthropic
No fixable issues found for provider "anthropic". (Run `kilroy auth list` to see all entries.)
```

### Output (invalid provider)

```
$ ./kilroy auth suggest-fix nonexistent
No fixable issues found for provider "nonexistent". (Run `kilroy auth list` to see all entries.)
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (may indicate nothing to fix) |

### Notes

- When no provider is specified, checks all discovered credentials
- When a provider is specified, filters to that provider only
- Currently returns success exit code even when no fixable issues exist

---

## Common Error Patterns

### Missing auth.toml

If `auth.toml` does not exist, `kilroy auth check` and `kilroy auth list` behave differently:

**`kilroy auth check --pretty` with missing config:**
```
$ HOME=/tmp/empty_home ./kilroy auth check --pretty
error: no auth config found (looked at /tmp/empty_home/.config/kilroy/auth.toml); run `kilroy auth init`
run `kilroy auth init` to create your auth config
```
Exit code: 1

**`kilroy auth list` with missing config:**
```json
$ HOME=/tmp/empty_home ./kilroy auth list
{
  "kilroy_version": "0.1.0",
  "scanned_at": "2026-05-05T01:53:00Z",
  "platform": "darwin",
  "entries": [
    {
      "id": "anthropic.env.ANTHROPIC_API_KEY",
      "kind": "env_var",
      "provider": "anthropic",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "ANTHROPIC_API_KEY"
      },
      "notes": [
        "key prefix: sk-ant-"
      ]
    },
    {
      "id": "anthropic.env.ANTHROPIC_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "anthropic",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "ANTHROPIC_API_KEY_KILROY"
      },
      "notes": [
        "key prefix: sk-ant-"
      ]
    },
    {
      "id": "cursor.cli_oauth",
      "kind": "cli_oauth",
      "provider": "cursor",
      "tool": "cursor",
      "state": "missing",
      "identity": {},
      "source": {
        "file": "/tmp/empty_home/.cursor/cli-config.json"
      }
    },
    {
      "id": "github.gh.cli_oauth",
      "kind": "cli_oauth",
      "provider": "github",
      "tool": "gh",
      "state": "missing",
      "identity": {},
      "source": {
        "file": "/tmp/empty_home/.config/gh/hosts.yml",
        "keychain_service": "gh:github.com"
      }
    },
    {
      "id": "google.env.GEMINI_API_KEY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GEMINI_API_KEY"
      },
      "notes": [
        "key prefix: AIza"
      ]
    },
    {
      "id": "google.env.GEMINI_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GEMINI_API_KEY_KILROY"
      },
      "notes": [
        "key prefix: AIza"
      ]
    },
    {
      "id": "google.env.GOOGLE_GENERATIVE_AI_API_KEY",
      "kind": "env_var",
      "provider": "google",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "GOOGLE_GENERATIVE_AI_API_KEY"
      },
      "notes": [
        "key prefix: AIza"
      ]
    },
    {
      "id": "openai.env.OPENAI_API_KEY",
      "kind": "env_var",
      "provider": "openai",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "OPENAI_API_KEY"
      },
      "shadows": [
        "openai.codex.cli"
      ],
      "notes": [
        "key prefix: sk-proj-"
      ]
    },
    {
      "id": "openai.env.OPENAI_API_KEY_KILROY",
      "kind": "env_var",
      "provider": "openai",
      "state": "ok",
      "identity": {},
      "source": {
        "env_var": "OPENAI_API_KEY_KILROY"
      },
      "shadows": [
        "openai.codex.cli"
      ],
      "notes": [
        "key prefix: sk-proj-"
      ]
    },
    {
      "id": "openai.codex.cli",
      "kind": "cli_oauth",
      "provider": "openai",
      "tool": "codex",
      "state": "missing",
      "identity": {},
      "source": {
        "file": "/tmp/empty_home/.codex/auth.json"
      },
      "shadowed_by": [
        "openai.env.OPENAI_API_KEY",
        "openai.env.OPENAI_API_KEY_KILROY"
      ]
    }
  ],
  "summary": {
    "total": 10,
    "ok": 7,
    "expired": 0,
    "missing": 3,
    "ambiguous": 0
  },
  "config_state": "uninitialized"
}
```
Exit code: 0

- `kilroy auth check` exits with code 1 when auth config is absent
- `kilroy auth list` exits with code 0 and shows `"config_state": "uninitialized"` when no config exists

### Shadowed Credentials

When multiple credential sources exist for the same provider, the env var typically wins for default invocations. Class-routed CLI runs scrub env vars to ensure the CLI session is used instead.

> **Note:** See the full `./kilroy auth list --pretty` output in the [kilroy auth list](#kilroy-auth-list) section above for examples of shadowed credentials (look for entries with `shadowed by:` and `shadows:` annotations).

### Unreferenced Credentials

Credentials not referenced by any chain are marked as `(unreferenced in config)` in the pretty output.

> **Note:** See the full `./kilroy auth list --pretty` output in the [kilroy auth list](#kilroy-auth-list) section above for examples of unreferenced credentials (look for entries marked `(unreferenced in config)`).

### Unknown Subcommand or Flag

Passing an unrecognized subcommand or flag produces a usage message and error:

```
$ ./kilroy auth --unknown-flag
usage:
  kilroy auth defaults
  kilroy auth init [--force] [--path <dir>] [--json|--pretty]
  kilroy auth list [--pretty] [--json] [--chains]
  kilroy auth check [--pretty] [--json] [--project <dir>]
  kilroy auth suggest-fix [<provider>]

  defaults prints the default_chains.toml template verbatim.
  init     generates ~/.config/kilroy/auth.toml from the template.
  list     outputs JSON by default; pass --pretty for human-readable.
  list --chains pivots to a chain-centric view (one row per configured binding).
  check    runs the auth resolver for every configured binding and reports status.
unknown auth subcommand: "--unknown-flag"
```

#### Per-Subcommand Unknown Flag Captures

All auth subcommands exit with code 1 when passed an unknown flag:

**`kilroy auth list --unknown-flag`:**
```
$ ./kilroy auth list --unknown-flag
unknown flag: "--unknown-flag"
```
Exit code: 1

**`kilroy auth check --unknown-flag`:**
```
$ ./kilroy auth check --unknown-flag
unknown flag: "--unknown-flag"
```
Exit code: 1

**`kilroy auth init --unknown-flag`:**
```
$ ./kilroy auth init --unknown-flag
unknown flag: "--unknown-flag"
```
Exit code: 1

**`kilroy auth defaults --unknown-flag`:**
```
$ ./kilroy auth defaults --unknown-flag
unknown flag: "--unknown-flag"
```
Exit code: 1
