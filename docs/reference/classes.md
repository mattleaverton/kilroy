# Policy Classes Reference

> Generated from `internal/policy/data/policy.toml` (schema_version=1, policy_version=2.0.0) on 2026-05-05

This document describes all policy classes available for routing agent nodes. Each class defines a fallback chain of candidates ordered by preference. The resolver walks the chain and picks the first candidate whose authentication requirements are satisfied.

## `architectural_critique`

System design review and architectural trade-off analysis.

### When to Use

System design review and architectural trade-off analysis.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `claude-opus-4-7` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:elite |
| 1 | `claude-opus-4-7` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:elite |
| 2 | `gpt-5` | `openai_sdk` | `openai` | api_key | api_key, tier:elite, cross_provider |
| 3 | `gemini-2.5-pro` | `google_sdk` | `google` | api_key | api_key, tier:elite, cross_provider |
| 4 | `claude-sonnet-4-6` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:mid |

## `coding_codex_apikey`

Codex CLI on the OpenAI api_key path with stage-isolated auth.json.

### When to Use

Codex CLI on the OpenAI api_key path with stage-isolated auth.json.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `gpt-5` | `codex_cli` | `openai` | api_key | api_key, tier:elite |

## `coding_codex_subscription`

Codex CLI via subscription (cli_oauth).

### When to Use

Codex CLI via subscription (cli_oauth).
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `gpt-5` | `codex_cli` | `openai` | cli_oauth (codex) | subscription, tier:elite |

## `coding_gemini_apikey`

Gemini CLI on the Google api_key path.

### When to Use

Gemini CLI on the Google api_key path.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `gemini-2.5-pro` | `gemini_cli` | `google` | api_key | api_key, tier:elite |

## `coding_gemini_subscription`

Gemini CLI via subscription (cli_oauth).

### When to Use

Gemini CLI via subscription (cli_oauth).
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `gemini-2.5-pro` | `gemini_cli` | `google` | cli_oauth (gemini) | subscription, tier:elite |

## `deep_investigation`

Long-context research and synthesis requiring 1M+ token windows.

**Aliases:** `research`

### When to Use

Long-context research and synthesis requiring 1M+ token windows.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `claude-opus-4-7` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:elite, long_context |
| 1 | `claude-opus-4-7` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:elite, long_context |
| 2 | `gemini-2.5-pro` | `google_sdk` | `google` | api_key | api_key, tier:elite, long_context, cross_provider |
| 3 | `gpt-5` | `openai_sdk` | `openai` | api_key | api_key, tier:elite, cross_provider |
| 4 | `claude-sonnet-4-6` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:mid |

## `frontend_aesthetic`

UI/UX design critique and frontend component generation.

### When to Use

UI/UX design critique and frontend component generation.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `gpt-5` | `openai_sdk` | `openai` | api_key | api_key, tier:elite, visual_reasoning |
| 1 | `claude-sonnet-4-6` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:mid |
| 2 | `claude-sonnet-4-6` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:mid |
| 3 | `gemini-2.5-pro` | `gemini_cli` | `google` | cli_oauth (gemini) | subscription, tier:elite, cross_provider |
| 4 | `gemini-2.5-pro` | `google_sdk` | `google` | api_key | api_key, tier:elite, cross_provider |
| 5 | `claude-haiku-4-5` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:cheap |

## `hard_coding`

Maximum reasoning depth for complex multi-file coding tasks.

**Aliases:** `coding`

### When to Use

Maximum reasoning depth for complex multi-file coding tasks.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `claude-opus-4-7` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:elite |
| 1 | `claude-opus-4-7` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:elite |
| 2 | `gpt-5` | `openai_sdk` | `openai` | api_key | api_key, tier:elite, cross_provider |
| 3 | `claude-sonnet-4-6` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:mid |
| 4 | `claude-sonnet-4-6` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:mid |

## `quick_easy`

Low-latency, low-cost tasks where speed beats depth.

**Aliases:** `fast`

### When to Use

Low-latency, low-cost tasks where speed beats depth.
### Fallback Chain

The resolver tries candidates in order until one passes authentication checks:

| Rank | Model | Driver | Provider | Auth Method | Tags |
|------|-------|--------|----------|-------------|------|
| 0 | `claude-haiku-4-5` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:cheap |
| 1 | `claude-haiku-4-5` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:cheap |
| 2 | `gemini-2.5-flash` | `google_sdk` | `google` | api_key | api_key, tier:cheap, cross_provider |
| 3 | `claude-sonnet-4-6` | `claude_cli` | `anthropic` | cli_oauth (claude) | subscription, tier:mid |
| 4 | `claude-sonnet-4-6` | `anthropic_sdk` | `anthropic` | api_key | api_key, tier:mid |

---

## Class Aliases

Aliases provide shorter or alternative names for classes:

| Alias | Resolves To |
|-------|-------------|
| `coding` | `hard_coding` |
| `fast` | `quick_easy` |
| `research` | `deep_investigation` |

