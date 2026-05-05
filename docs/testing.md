# Testing

`go test ./...` is the default unit-test pass. It should stay fast enough for
local iteration and CI.

Integration tests must opt in explicitly:

```bash
KILROY_INTEGRATION=1 go test -timeout=300s ./...
```

Use integration tests for coverage that exercises full workflows, subprocess
CLIs, tmux sessions, HTTP server lifecycles, machine-local run corpora, or
intentional sleeps/timeouts. Keep pure parser, config, routing, and helper logic
as ordinary unit tests.

If a test is slow only because it waits for time to pass, first try to shorten or
inject the delay. If the test still needs process-level or workflow-level
execution to prove the behavior, call `requireIntegration(t)` at the start of the
test body.

Package-level wrappers delegate to `internal/testutil.RequireIntegration`; new
packages can either add the same wrapper or import `internal/testutil` directly.

Useful commands:

```bash
go test ./...
KILROY_INTEGRATION=1 go test ./internal/attractor/engine -run TestRunWithConfig -timeout=300s
KILROY_INTEGRATION=1 ./scripts/e2e.sh
```
