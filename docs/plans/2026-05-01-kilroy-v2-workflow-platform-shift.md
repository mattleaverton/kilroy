# Kilroy v2 Workflow Platform Shift

**Date:** 2026-05-01  
**Branch:** `feat/v2-reframe`  
**Status:** Draft design overview

## Summary

Kilroy is shifting from an engine-first tool with exposed LLM and runtime machinery into a workflow-first platform for running mixed script-and-agent work in a local repo or workspace.

The new center of gravity is:

- choose a workflow
- provide a workspace and input
- validate launchability
- start an async run
- capture everything that happened
- inspect the result later through stable run commands and data

The old center of gravity was:

- choose a graph
- choose a config
- choose providers, models, backends, flags, and auth shape
- run the engine directly

This change is not a small polish pass. It is a product reframe.

## Why Change

The current shape exposes too much infrastructure detail to both humans and agents:

- provider names
- model IDs
- backend selection
- auth and environment details
- graph and config plumbing
- engine-level flags that are useful but not part of the core user intent

That creates friction without adding corresponding value for most calls.

The desired outcome is a tool that is:

- simple to launch
- easy for agents to call repeatedly
- easy to inspect after the fact
- stable in its default behavior
- explicit in what happened
- flexible internally without forcing that flexibility onto workflow authors

## Product Identity

Kilroy is a small local-first workflow runner with strong audit capture.

It is not primarily:

- a graph authoring product
- a UI product
- a credential manager
- a provider console
- a hand-tuned runtime for model enthusiasts

It is primarily:

- a way to package a repeatable pattern of work
- a way to run that pattern in a real repo or workspace
- a way to capture the full record of how the run was launched and what happened

## Core Posture

### Agent-primary default

The primary consumer is an agent making many Kilroy calls, not a human watching one run.

That implies:

- async launch by default
- machine-readable output by default
- stable run handles
- polling-friendly follow-up commands
- typed, structured failure reporting

Human-friendly rendering still matters, but it is a secondary layer.

### Audit, not approval

Kilroy records what happened. It does not try to be a policy gate for cost, taste, or operator judgment.

If a route was selected, a fallback was used, auth was missing, or a launch failed, Kilroy should capture that cleanly and let callers or external tools decide what to do with it.

### Local-first and release-stable

Runtime behavior should not drift based on mutable local state unless explicitly intended.

Workflows, routing classes, and default resolution behavior should be stable for a given Kilroy build. If the routing data changes, that is a repo and release change.

## User Experience Target

The desired command shape is:

- `kilroy <workflow> [args]` for a small blessed set of built-in workflows
- `kilroy run <workflow> [args]` as the general workflow launch surface
- `kilroy workflows ...` to discover and inspect workflows
- `kilroy runs ...` to inspect, wait on, and query runs
- `kilroy auth ...` to inspect discovered auth state and remediation guidance
- one read-only surface for inspecting built-in routing classes and how they resolve

The default launch flow is:

1. Resolve the workflow package.
2. Validate the workflow package and graph.
3. Collect every referenced execution class.
4. Resolve those classes to concrete routes using built-in release data.
5. Validate that required tools, auth sources, providers, and models are usable enough for launch.
6. Persist the launch result, including failures.
7. If runnable, start the run asynchronously and return a handle.

## Workflow Package Model

The workflow package becomes the distributable unit.

A workflow package contains:

- `graph.dot`
- `workflow.toml`
- optional `scripts/`
- optional `prompts/`
- optional supporting assets

### `graph.dot`

`graph.dot` remains the workflow logic:

- nodes
- edges
- routing
- deterministic script steps
- agentic steps
- flow semantics

It is still the actual workflow.

### `workflow.toml`

`workflow.toml` is the manifest and invocation contract for the workflow package.

It should carry things like:

- workflow name
- summary and description
- inputs
- outputs
- workflow-level default execution class
- optional workflow metadata useful to callers
- launch defaults that belong to the package rather than the engine

It should not be the place where ordinary workflows hardcode:

- provider names
- model IDs
- backend types
- CLI executable paths
- auth env vars

Those are nearby infrastructure concerns, not workflow-author concerns.

### Discovery

Workflow discovery should be hierarchical and consistent:

- built-in workflows shipped with Kilroy
- user-level workflows
- project-local workflows

Local/project workflows should win on name collisions. Warning by default is the current recommended behavior.

## Execution Classes and Routing

Workflows should ask for known abstract classes rather than raw provider/model tuples.

Examples:

- `quick_easy`
- `hard_coding`
- `deep_investigation`
- `architectural_critique`

The workflow can set a default class at the workflow level, and individual agentic nodes can override that with another known class.

This gives simple workflows a clean default while still allowing mixed-intent workflows.

### What a class means

A class is an abstract request for capability and operating style.

The class does not directly encode:

- one fixed model
- one fixed provider
- one fixed transport

Instead, a class resolves to a concrete ordered route set carried by Kilroy’s built-in routing data.

### Built-in routing data

The routing knowledge should live in the Kilroy repo as release data and be built into the executable.

That means:

- no mutable runtime routing database is required
- no remote source is required to launch
- behavior is stable for a given release
- changes to routing defaults are reviewed like code and shipped like code

For now, workflows may only use known class names that the Kilroy release already understands.

## Agent Conversation Abstraction

Kilroy currently conflates several independent concerns when running agentic steps.

The new design should separate at least these axes:

- model
- driver
- transport
- auth source
- history sink

Those pieces should be cheap to compose in-tree through clear Go interfaces.

The important point is not “plugins” as a packaging story. The important point is making a new backend or route cheap to add without threading special cases through the whole runtime.

## Auth Position

Kilroy should help with auth visibility and resolution, not become a secret manager.

Kilroy should:

- discover auth state
- report what it found
- explain what is missing
- explain how to remediate common failures
- choose among available auth sources when a route requires it

Kilroy should not:

- store secrets
- rotate secrets
- own credential lifecycle

## Validation and Launch Contract

Launch validation is a separate concern from running the workflow.

The runner should hand the workflow to a validation layer that determines whether the workflow is runnable as presented under a chosen validation policy.

The validator should produce a structured report. Internally this report can include:

- whether the workflow is runnable
- which classes were requested
- how they resolved
- unknown classes
- unknown providers or models
- missing tools
- missing auth
- degraded but usable routes
- remediation notes

The runner only needs a launchable yes/no plus the report.

### Important launch rules

- validation happens before any workflow work begins
- failed launch still creates a persisted run record
- the resolution plan is snapshotted once at launch
- the run uses that frozen plan for its lifetime

Changes in the outside world during the run are accepted for now rather than causing mid-run re-resolution.

## Async Run Lifecycle

Async launch is the default.

The normal launch command returns immediately with machine-readable output containing enough information to find and inspect the run later.

That payload should include at least:

- `run_id`
- `status`
- `workflow`
- routing or release identity
- exact follow-up commands for inspection

Human-friendly output should be available through dedicated commands or a rendering flag, not by changing the default launch contract.

## Run Capture and Observability

Kilroy’s job is to capture the information well enough that other tools can present it well.

The run record should capture:

- workflow identity
- launch input
- validation report
- resolved class and route data
- actual concrete route used at each agentic step
- fallback activity
- tool and auth failures
- run lineage, including nested runs when Kilroy calls Kilroy

The DB and run artifacts are the product truth.

A built-in UI may exist only as a convenience. It should not be treated as the main product value.

## What Stops Being Core User Surface

The following should move out of ordinary workflow author and launcher thinking:

- raw model selection
- raw provider selection
- backend toggles as a primary user concern
- CLI transport flags
- auth path details
- graph-engine-first invocation patterns

These can still exist as expert escape hatches where necessary, but they should not define the default story.

## Project Impact

This shift changes the project in visible ways.

### Surface changes

- top-level `kilroy` becomes the primary surface
- `attractor` stops being the product identity
- async handle-oriented launch becomes the standard path
- built-in workflows become first-class commands

### Authoring changes

- workflows become packages, not just graphs
- `workflow.toml` becomes important
- known execution classes replace raw model/provider authoring for normal cases

### Runtime changes

- validation becomes a first-class launch stage
- route resolution becomes built-in release data
- agent backend concerns become more explicitly separated

### Data changes

- failed launch is a first-class persisted run outcome
- route resolution and launch validation become part of the run record
- nested run linkage should be recorded

## Non-Goals

The following are intentionally not core goals of this shift:

- multi-user shared mutable routing policy
- runtime editing of built-in classes
- secret storage
- perfect built-in visualization
- solving every cost-control concern in Kilroy itself
- preventing recursion

## Small Open Details

The major questions are mostly settled. Remaining details are important, but smaller:

- exact `workflow.toml` schema
- exact class catalog names
- exact structured validator report schema
- exact machine-readable launch payload schema
- exact warning behavior for workflow name collisions

## Prototype and Investigation Efforts

The following efforts are intentionally designed so they can be run in a largely empty context. They can be small code spikes, small research briefs, or narrow testing projects that clarify risk before major refactors land.

### 1. Workflow Package Spike

Create a minimal standalone workflow package format prototype with:

- one manifest
- one graph
- one script
- one prompt

Goals:

- confirm the package boundary feels right
- confirm the manifest can stay small
- confirm package discovery and packaging semantics are obvious

Success looks like a tiny runnable package with no surrounding Kilroy complexity required to understand it.

### 2. Class Resolver Prototype

Build a small isolated resolver that takes:

- a class name
- a baked-in routing table
- discovered local capabilities

and returns:

- the chosen route
- fallback options
- structured failure details

Goals:

- prove the class abstraction is sufficient
- prove the built-in data format is easy to maintain
- expose where the tuple model is still too tangled

### 3. Launch Validator Prototype

Build a narrow validator that accepts a workflow package and returns a structured launchability report.

Goals:

- confirm the separation between runner and validator
- define the shape of launch errors
- determine what “runnable” actually means under strict and permissive policies

This should be done without executing real workflow work.

### 4. Auth Discovery Survey and Stub

Create a small auth discovery stub plus a research table for:

- env-var auth
- local CLI login state
- common config file locations

Goals:

- determine what Kilroy can detect cheaply and reliably
- determine what is only inferable through weaker signals
- shape the initial `kilroy auth` output contract

### 5. Async Launch Contract Prototype

Build a tiny command prototype that:

- accepts a fake workflow name
- emits a machine-readable accepted handle
- persists a fake run record
- supports `show` and `wait`

Goals:

- make the agent-primary CLI contract concrete
- validate that the machine-readable output is sufficient
- refine what follow-up hints should be returned

### 6. Failed Launch Persistence Testbed

Build a launch-failure-only prototype where:

- validation intentionally fails
- a run record is still created
- `runs show` and `runs wait` can inspect that failed launch cleanly

Goals:

- confirm that “capture failures” is well supported
- confirm the data model for pre-execution failure is not awkward

### 7. Concurrency Isolation Soak Test

Run many concurrent synthetic workflows against empty or disposable workspaces.

Goals:

- verify log separation
- verify DB behavior
- verify workspace isolation
- verify nested and sibling runs do not trample each other

This should happen before leaning hard on agent-primary multi-run orchestration.

### 8. Nested Run Linkage Prototype

Build a tiny “Kilroy launches Kilroy” experiment using only env var parent linkage and persisted run metadata.

Goals:

- verify parent-child run recording
- decide what cancellation and lineage semantics should be visible
- keep recursion supported without over-designing it

### 9. Built-in Workflow Blessing Exercise

Select a very small candidate set of built-in workflows and test whether they truly deserve first-class top-level verbs.

Goals:

- prevent a cluttered top-level command surface
- force clarity about what is truly core
- verify that general workflow launch remains the main path

### 10. Human-Friendly Rendering Stub

Build a tiny renderer that takes machine launch output and run records and produces friendly terminal output.

Goals:

- keep the agent-primary default intact
- prove the human-friendly layer can stay separate
- avoid contaminating the core output contract with mixed-purpose formatting

## Scope Boundary

This document describes the product shift, operating principles, and architectural direction for Kilroy v2.

It does not yet define:

- the final command grammar in detail
- the final manifest schema in detail
- the final storage schema in detail
- the implementation sequence in task-level detail

Those belong in follow-on planning once this shape is accepted.
