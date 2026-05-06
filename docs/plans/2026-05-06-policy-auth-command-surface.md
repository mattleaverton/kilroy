# Policy And Auth Command Surface Implementation Plan

**Goal:** Add targeted policy and auth commands so users express outcomes
without editing TOML.

**Architecture:** Keep embedded policy and auth templates as the base. Add small
override/config mutation helpers that write user/project state, then route
`policy resolve`, prelaunch, and check-style validation through the effective
state. Auth commands mutate only the global user auth file for now.

**Status:** Implemented in this branch. `workflows validate` and prelaunch now
load effective policy from the discovered project root, so project overrides
are visible before a run starts.

---

## Task 1: Policy Overrides

**Files:**
- Modify: `internal/policy/*`
- Modify: `internal/attractor/engine/policy_class.go`
- Modify: `cmd/kilroy/policy.go`
- Test: `internal/policy/*_test.go`, `cmd/kilroy/policy_test.go`

**Steps:**
1. Test project `policy prefer` writes an override and `policy resolve` reports
   the project source.
2. Test global `policy pin` resolves only the pinned model.
3. Implement override file loading/merging with built-in policy.
4. Add `policy prefer`, `policy pin`, `policy clear`, and `policy overrides`.

## Task 2: Auth Env Management

**Files:**
- Modify: `cmd/kilroy/auth.go`
- Modify: `cmd/kilroy/auth_init.go`
- Modify/Add: auth command tests

**Steps:**
1. Test `auth set <provider> --env <NAME>` creates/updates global
   `auth.toml`.
2. Test `auth prefer <requirement> <ENV>` moves a source to the front.
3. Test `auth remove-source <requirement> <ENV>` removes the source.
4. Test `auth init --rescan` adds newly detected sources without overwriting
   existing config.

## Task 3: Verification

Run targeted command tests first, then:

```bash
go test ./internal/policy ./cmd/kilroy
go test -timeout=300s ./...
```

Implemented verification:

```bash
KILROY_INTEGRATION=1 go test ./cmd/kilroy -run 'TestWorkflowsValidate_UsesProjectPolicyOverride|TestAuth(Set|Prefer|RemoveSource|InitRescan)|TestPolicy(Prefer|Pin)' -count=1 -v
go test -timeout=300s ./...
go vet ./...
go build ./cmd/kilroy/
```
