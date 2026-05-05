<!-- Generated from internal/attractor/workflows/manifest_v2.go on 2026-05-04 -->

# workflow.toml Schema Reference

This document describes the complete schema for `workflow.toml` files, which declare workflow packages for Kilroy. The loader auto-detects between **v2** (modern) and **legacy** (v1) formats.

## Schema Detection

- **v2**: `[workflow]` table has non-empty `name`, `version`, or `description`
- **Legacy**: Top-level `name`, `description`, `version` fields (v1 format)

## v2 Schema

### `[workflow]` table (required for v2)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Workflow identifier (kebab-case recommended) |
| `version` | string | Yes | — | Version string (e.g., "1", "1.0.0") |
| `description` | string | No | — | Human-readable description |
| `agent_description` | string | No | — | Description for agent-facing tools |
| `author` | string | No | — | **Decoded but NOT surfaced in Manifest** |
| `tags` | array of strings | No | — | **Decoded but NOT surfaced in Manifest** |
| `graph` | string | No | "graph.dot" | Path to DOT graph file (relative to package dir) |
| `default_class` | string | No | — | Policy class for agentic nodes without override |
| `experimental` | boolean | No | false | Hide from `kilroy workflows list` unless `--all` |

### `[inputs.<name>]` tables (optional)

Each input is defined as `[inputs.<name>]` where `<name>` is the input identifier.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | — | Input type: `"string"`, `"integer"`, `"float"`, `"boolean"`, `"path"`, `"enum"` |
| `required` | boolean | No | false | Whether input must be provided |
| `default` | string | No | — | Default value if not provided |
| `description` | string | No | — | Human-readable description |
| `enum_values` | array of strings | No | — | Allowed values when `type = "enum"` |
| `positional` | integer | No | 0 | Position for positional args (1-based, 0 = not positional) |
| `flag` | string | No | — | CLI flag form (e.g., `"--context"`) |

**Input Types:**
- `string`: Arbitrary text
- `integer`: Whole number
- `float`: Decimal number  
- `boolean`: true/false
- `path`: Filesystem path
- `enum`: One of `enum_values`

### `[outputs.<name>]` tables (optional)

Each output is defined as `[outputs.<name>]` where `<name>` is the output identifier.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | — | Output type: `"path"`, `"string"`, `"integer"`, `"float"`, `"boolean"` |
| `description` | string | No | — | Human-readable description |
| `optional` | boolean | No | false | Whether output may be absent |
| `path` | string | No | — | Filesystem path when `type = "path"` (relative to workspace) |

### `[side_effects]` table (optional)

Declarative planning signals. All fields are optional booleans.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mutates_git` | boolean | No | — | Workflow modifies git history/state |
| `writes_files` | boolean | No | — | Workflow writes to the filesystem |
| `network_egress` | boolean | No | — | Workflow makes network requests |
| `idempotent` | boolean | No | — | Running multiple times is safe |

**Note:** The `Set` field is internal — it becomes `true` when any side_effects field is authored.

### `[nodes.<id>]` tables (optional)

Per-node overrides for agentic nodes in the graph.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `class` | string | No | — | Policy class for this node (mutually exclusive with `model`) |
| `model` | string | No | — | Specific model for this node (mutually exclusive with `class`) |

### `[secrets]` table (optional)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `needs` | array of strings | No | — | Abstract credential names this workflow requires |

### `[defaults]` table (optional, legacy compatibility)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `labels` | table of strings | No | — | Label injection for run-config back-compat |

## Legacy Schema (v1)

The legacy format is still supported for backward compatibility.

### Top-level fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Workflow identifier |
| `description` | string | No | — | Human-readable description |
| `version` | string | Yes | — | Version string |
| `outputs` | array of strings | No | — | Output path strings |

### `[[inputs]]` array of tables

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Input identifier |
| `description` | string | No | — | Human-readable description |
| `required` | boolean | No | false | Whether input must be provided |
| `default` | string | No | — | Default value |

### `[defaults]` table

Same as v2: `labels` map for run-config compatibility.

### `[metadata]` table (optional)

Arbitrary string key-value pairs (parsed but not used by core loader).

## Internal/Loader Fields

These fields exist in the internal `Manifest` struct but are not part of the TOML wire format:

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `Schema` | string | Auto | "v2" or "legacy" based on detection |

## Minimal Complete Example (v2)

```toml
[workflow]
name = "review"
version = "1"
```

## Legacy Format Example

```toml
name = "implement"
description = "Implement a directed change with build+test verification."
version = "1"

outputs = ["result.md"]

[[inputs]]
name = "prompt"
description = "What to implement."
required = true

[[inputs]]
name = "context_files"
description = "Optional context paths."
required = false

[defaults]
labels = { workflow = "implement" }
```
