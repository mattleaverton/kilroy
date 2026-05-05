---
name: Platform Reframe
description: Kilroy reframed as a layered software operations platform — L0 graph runner, L1 agent capabilities, L2 workflow patterns. Feature branch feat/platform-reframe.
type: project
---

Kilroy identity reframed (2026-04-03): "software operations automation platform that uses DOT graphs to codify repeatable patterns of LLM-assisted work."

Key decisions:
- Three-layer architecture with enforced import boundaries (separate Go packages)
- L0: graph runner (DOT parsing, traversal, conditions, SQLite, hooks, HTTP API)
- L1: agent capabilities (CLI session management via tmux, provider detection, fidelity, failure classification)
- L2: workflow patterns (human-in-the-loop, git as hook, supervisor, workflow packages)
- "Codergen" renamed to "agent"
- DOT-as-executable-graph is novel — no other tool does this
- CLI tool interaction via tmux, not subprocess pipes
- SQLite run database is critical path, not deferred
- Supervisor: prototype, not full intervention policies
- Routing: keep all 5 steps — LLM-influenced routing is the differentiator
- Feature branch, big bang development, no backward compat constraints

**Why:** DOT as executable workflow format is genuinely novel. The platform model (workflow packages dispatched to workspaces) enables cross-repo software operations at scale.

**How to apply:** All stabilization work should be framed against this layered architecture. Plan is at `docs/plans/2026-04-03-platform-reframe.md`.
