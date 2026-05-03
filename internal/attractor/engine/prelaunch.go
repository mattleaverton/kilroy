// Pre-launch validation: a fast, no-LLM-cost check that runs before any
// preflight or engine dispatch. For every agentic node in the graph, it
// resolves the policy class against the current machine state, surfaces
// the resolved tuple, and verifies that the resolved candidate's auth +
// CLI binary (when applicable) are actually present.
//
// This is the validation layer the v2 reframe wants — auth and agent
// definitions checked at launch, no real-LLM-call probes, structured
// JSON report. Writes <logs_root>/prelaunch_validation.json.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// tomlDecodeBytes is a tiny indirection so unit tests can swap in a fake
// decoder if they ever need to. Today it's just a pass-through.
func tomlDecodeBytes(data []byte, dest any) (toml.MetaData, error) {
	return toml.Decode(string(data), dest)
}

// PreLaunchReport is the structured output of ValidatePreLaunch. Persisted
// alongside the run as prelaunch_validation.json for after-the-fact
// inspection (kilroy runs show, kilroy policy explain, etc.).
type PreLaunchReport struct {
	GeneratedAt string                 `json:"generated_at"`
	Package     *PreLaunchPackageCheck `json:"package,omitempty"`
	Nodes       []PreLaunchNodeCheck   `json:"nodes"`
	Secrets     []PreLaunchSecretCheck `json:"secrets,omitempty"`
	Summary     PreLaunchSummary       `json:"summary"`
}

// PreLaunchPackageCheck records workflow-package integrity findings —
// manifest required fields, tool_command scripts present, etc. Surfaces
// only when opts.PackageDir is set (i.e., the run came from a package
// rather than a raw --graph file).
type PreLaunchPackageCheck struct {
	Dir    string   `json:"dir,omitempty"`
	Status string   `json:"status"` // "ok" | "fail"
	Errors []string `json:"errors,omitempty"`
	// Notes are non-fatal observations (e.g., class= used as a
	// stylesheet selector rather than a policy class). They surface in
	// the report so users see them but don't block validation.
	Notes []string `json:"notes,omitempty"`
}

// PreLaunchNodeCheck records the resolution + auth-presence + binary-presence
// findings for a single agentic node.
type PreLaunchNodeCheck struct {
	NodeID         string `json:"node_id"`
	Class          string `json:"class,omitempty"`
	ResolvedModel  string `json:"resolved_model,omitempty"`
	ResolvedDriver string `json:"resolved_driver,omitempty"`
	AuthMethod     string `json:"auth_method,omitempty"`
	AuthSource     string `json:"auth_source,omitempty"`
	// BinaryFound is set only for CLI drivers (claude_cli, codex_cli, gemini_cli).
	// nil = not applicable; *true / *false = applicable.
	BinaryFound *bool    `json:"binary_found,omitempty"`
	Status      string   `json:"status"` // "ok" | "fail"
	Errors      []string `json:"errors,omitempty"`
}

// PreLaunchSecretCheck records whether a single required secret (a
// workflow.toml [secrets].needs entry, expressed as a provider name) has
// at least one healthy auth entry on this machine.
type PreLaunchSecretCheck struct {
	Name   string   `json:"name"`
	Status string   `json:"status"` // "ok" | "fail"
	Errors []string `json:"errors,omitempty"`
}

// PreLaunchSummary is a quick rollup for tooling.
type PreLaunchSummary struct {
	OK   int `json:"ok"`
	Fail int `json:"fail"`
}

// ValidatePreLaunch walks every agentic node in g, resolves any class=
// attribute through the policy resolver, and checks that the resolved
// candidate is actually launchable on this machine (auth present, CLI
// binary on PATH if the driver is a CLI). Returns a typed error when any
// node fails — callers should refuse to launch the run.
//
// Cost: a single auth.ListAll snapshot via policy.CollectMachineState,
// plus one os/exec.LookPath per CLI driver. No LLM calls.
func ValidatePreLaunch(g *model.Graph, opts RunOptions, deps PolicyDeps) (*PreLaunchReport, error) {
	report := &PreLaunchReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if g == nil {
		return report, nil
	}

	// Package-integrity check: only when PackageDir is set (i.e., the run
	// came from a workflow package, not a raw --graph file). Catches
	// authoring typos at runtime — same checks as shipped_packages_test.go,
	// but for the user's actual launch path.
	if opts.PackageDir != "" {
		pc := validatePackageIntegrity(opts.PackageDir, g)
		report.Package = pc
		if pc.Status == "fail" {
			report.Summary.Fail++
			_ = writePreLaunchReport(opts.LogsRoot, report)
			return report, &PreLaunchError{Report: report}
		}
	}

	reg := NewDefaultRegistry()
	// Snapshot policy data + machine state once so every node sees a
	// consistent view (especially for runs with many nodes).
	var policyData *policy.Data
	loadFn := deps.Load
	if loadFn == nil {
		loadFn = policy.Load
	}
	resolverFactory := deps.Resolver
	if resolverFactory == nil {
		resolverFactory = DefaultBindingResolver
	}
	// Snapshot the auth detector once for both the binding resolver and the
	// secrets validator below.
	var authList auth.ListOutput
	var authResolver *binding.Resolver
	var authLoaded bool

	// Per-node frozen snapshots. Plan §5: prelaunch is the authoritative
	// snapshot; execution reads from it via LoadPreLaunchSnapshot rather
	// than re-running policy.Resolve.
	frozenSnapshots := map[string]preLaunchNodeSnap{}

	// Stable iteration so the report is deterministic.
	nodeIDs := sortedNodeIDs(g)
	for _, id := range nodeIDs {
		n := g.Nodes[id]
		if n == nil {
			continue
		}
		if pr, ok := reg.Resolve(n).(ProviderRequiringHandler); !ok || !pr.RequiresProvider() {
			continue
		}
		check := PreLaunchNodeCheck{NodeID: id}

		className := strings.TrimSpace(n.Attr(PolicyClassAttr, ""))
		if className == "" {
			// No agent_class= attribute — node uses legacy stylesheet
			// routing (llm_provider/llm_model). Mark ok and move on;
			// validating that without an LLM probe isn't possible.
			check.Status = "ok"
			report.Nodes = append(report.Nodes, check)
			report.Summary.OK++
			continue
		}
		check.Class = className

		// Lazy-load policy + machine state on the first class-bearing node.
		if policyData == nil {
			d, err := loadFn()
			if err != nil {
				check.Status = "fail"
				check.Errors = append(check.Errors, fmt.Sprintf("policy load: %v", err))
				report.Nodes = append(report.Nodes, check)
				report.Summary.Fail++
				_ = writePreLaunchReport(opts.LogsRoot, report)
				return report, fmt.Errorf("prelaunch: policy load: %w", err)
			}
			policyData = d
		}
		if !authLoaded {
			authList = auth.ListAll("", auth.DefaultDetectors())
			// Use the source workspace (not the not-yet-created worktree)
			// as the project root for auth.toml lookup. The worktree is a
			// copy of the workspace, so .kilroy/auth.toml is identical;
			// using the workspace at prelaunch time means prelaunch and
			// execution agree about the project chain set.
			projectRoot := strings.TrimSpace(opts.Workspace)
			if projectRoot == "" {
				projectRoot = strings.TrimSpace(opts.RepoPath)
			}
			r, err := resolverFactory(projectRoot)
			if err != nil {
				check.Status = "fail"
				check.Errors = append(check.Errors, fmt.Sprintf("auth resolver: %v", err))
				report.Nodes = append(report.Nodes, check)
				report.Summary.Fail++
				_ = writePreLaunchReport(opts.LogsRoot, report)
				return report, fmt.Errorf("prelaunch: auth resolver: %w", err)
			}
			authResolver = r
			authLoaded = true
		}

		res, err := policy.Resolve(policy.ResolveRequest{
			ClassID:    className,
			NodeID:     id,
			WorkflowID: g.Name,
		}, policyData, authResolver)
		if err != nil {
			// Unknown agent_class= names are typos. They fail loudly —
			// the policy surface is non-overloaded (use plain `class=`
			// for stylesheet selectors). Other errors (no viable
			// candidate — class IS in policy but no auth on this
			// machine) are also hard fails.
			check.Status = "fail"
			check.Errors = append(check.Errors, fmt.Sprintf("agent_class %q: %v", className, err))
			report.Nodes = append(report.Nodes, check)
			report.Summary.Fail++
			continue
		}

		check.ResolvedModel = res.ModelID
		check.ResolvedDriver = res.Driver
		check.AuthMethod = res.AuthMethod()
		check.AuthSource = res.AuthSource()

		// Freeze the per-node resolution. Execution will read this rather
		// than re-running policy.Resolve, so config/env drift between
		// prelaunch and node execution cannot silently change the route.
		frozenSnapshots[id] = resolveResultToSnap(className, res)

		// CLI drivers need their binary on PATH AND need to be executable
		// (not a corrupt download, wrong arch, etc.). SDK drivers don't —
		// the HTTP client handles the request, no binary involved.
		if isCLIDriver(res.Driver) {
			binaryName := cliBinaryForDriver(res.Driver)
			binaryPath, lookErr := exec.LookPath(binaryName)
			found := lookErr == nil
			check.BinaryFound = &found
			if !found {
				check.Status = "fail"
				check.Errors = append(check.Errors, fmt.Sprintf("CLI binary %q not found on PATH (required by driver %s)", binaryName, res.Driver))
				report.Nodes = append(report.Nodes, check)
				report.Summary.Fail++
				continue
			}
			if probeErr := probeCLIBinary(binaryPath); probeErr != nil {
				check.Status = "fail"
				check.Errors = append(check.Errors, fmt.Sprintf("CLI binary %s does not respond to --help (required by driver %s): %v", binaryPath, res.Driver, probeErr))
				report.Nodes = append(report.Nodes, check)
				report.Summary.Fail++
				continue
			}
		}

		check.Status = "ok"
		report.Nodes = append(report.Nodes, check)
		report.Summary.OK++
	}

	// Per-secret checks against the workflow's [secrets].needs list. If we
	// haven't loaded the auth list yet (no class-bearing nodes triggered
	// it), do so now so secrets can be validated independently.
	if len(opts.RequiredSecrets) > 0 {
		if !authLoaded {
			authList = auth.ListAll("", auth.DefaultDetectors())
			authLoaded = true
		}
		secretChecks := validateSecrets(opts.RequiredSecrets, authList)
		report.Secrets = secretChecks
		for _, sc := range secretChecks {
			if sc.Status == "fail" {
				report.Summary.Fail++
			} else {
				report.Summary.OK++
			}
		}
	}

	if err := writePreLaunchReport(opts.LogsRoot, report); err != nil {
		// Persisting the report is best-effort; failure to write should
		// not mask a successful validation. Surface as a soft warning
		// via the sentinel logsRoot field rather than as an error.
		_ = err
	}

	// Write the frozen snapshots that execution will read. Plan §5
	// authoritativeness depends on this file existing — if the write
	// fails after a successful prelaunch, fail the run rather than let
	// execution silently re-resolve. Tests/legacy paths with no logs_root
	// are no-ops in writePreLaunchSnapshots and won't reach here.
	if err := writePreLaunchSnapshots(opts.LogsRoot, frozenSnapshots); err != nil {
		return report, fmt.Errorf("prelaunch: write snapshots: %w", err)
	}

	if report.Summary.Fail > 0 {
		return report, &PreLaunchError{Report: report}
	}
	return report, nil
}

// validateSecrets checks whether each required secret name (a provider name
// like "github" or "anthropic") has at least one auth entry in StateOK on
// this machine. Returns one PreLaunchSecretCheck per `needs` entry, in the
// order they were given.
func validateSecrets(needs []string, authList auth.ListOutput) []PreLaunchSecretCheck {
	if len(needs) == 0 {
		return nil
	}
	checks := make([]PreLaunchSecretCheck, 0, len(needs))
	for _, raw := range needs {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		check := PreLaunchSecretCheck{Name: name}
		satisfied := false
		for _, e := range authList.Entries {
			if e.Provider == name && e.State == auth.StateOK {
				satisfied = true
				break
			}
		}
		if satisfied {
			check.Status = "ok"
		} else {
			check.Status = "fail"
			check.Errors = append(check.Errors, fmt.Sprintf(
				"required secret %q not satisfied: no auth entry with provider=%s and state=ok found (hint: run `kilroy auth list` and configure the credential for %s)",
				name, name, name))
		}
		checks = append(checks, check)
	}
	return checks
}

// PreLaunchError is the typed error returned when one or more nodes fail
// pre-launch validation. Callers (cmd/kilroy/main.go) match on this to
// emit a consistent error code and avoid running anything more expensive.
type PreLaunchError struct {
	Report *PreLaunchReport
}

func (e *PreLaunchError) Error() string {
	if e.Report == nil {
		return "prelaunch validation failed"
	}
	failedNodes := []string{}
	for _, n := range e.Report.Nodes {
		if n.Status == "fail" {
			failedNodes = append(failedNodes, n.NodeID)
		}
	}
	failedSecrets := []string{}
	for _, s := range e.Report.Secrets {
		if s.Status == "fail" {
			failedSecrets = append(failedSecrets, s.Name)
		}
	}
	parts := []string{}
	if len(failedNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s): %s", len(failedNodes), strings.Join(failedNodes, ", ")))
	}
	if len(failedSecrets) > 0 {
		parts = append(parts, fmt.Sprintf("missing secret(s): %s", strings.Join(failedSecrets, ", ")))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("prelaunch validation failed (%d failure(s))", e.Report.Summary.Fail)
	}
	return "prelaunch validation failed for " + strings.Join(parts, "; ")
}

func writePreLaunchReport(logsRoot string, report *PreLaunchReport) error {
	logsRoot = strings.TrimSpace(logsRoot)
	if logsRoot == "" {
		return nil
	}
	if err := os.MkdirAll(logsRoot, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(logsRoot, "prelaunch_validation.json"), append(b, '\n'), 0o644)
}

// sortedNodeIDs returns the graph's node IDs in lexical order. Sorting
// makes the report stable across runs even though map iteration is random.
func sortedNodeIDs(g *model.Graph) []string {
	ids := make([]string, 0, len(g.Nodes))
	for id := range g.Nodes {
		ids = append(ids, id)
	}
	// Manual sort to avoid pulling in `sort` here just for this; lists are tiny.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	return ids
}

// isCLIDriver reports whether driver runs as a local CLI subprocess.
func isCLIDriver(driver string) bool {
	switch driver {
	case "claude_cli", "codex_cli", "gemini_cli":
		return true
	}
	return false
}

// probeCLIBinary runs `<binary> --help` with a short deadline and checks
// for exit 0. Catches broken binaries that LookPath can't see — wrong
// arch, missing dynamic deps, partial downloads. Migrated from the
// legacy provider_preflight; bounded to 5 seconds so a hung binary
// doesn't stall the launch.
func probeCLIBinary(binaryPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, "--help")
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// cliBinaryForDriver maps a CLI driver to the expected PATH binary name.
func cliBinaryForDriver(driver string) string {
	switch driver {
	case "claude_cli":
		return "claude"
	case "codex_cli":
		return "codex"
	case "gemini_cli":
		return "gemini"
	}
	return ""
}

// validatePackageIntegrity is the runtime version of the
// shipped_packages_test.go gate — workflow.toml parses with required
// fields, every node's tool_command bash <path> resolves to an existing
// regular file in the package, and every agent class= references a real
// policy class. Returns a PreLaunchPackageCheck describing what passed
// or failed; "fail" status aborts the run before any node is dispatched.
func validatePackageIntegrity(pkgDir string, g *model.Graph) *PreLaunchPackageCheck {
	pc := &PreLaunchPackageCheck{Dir: pkgDir}

	// Manifest: required fields. Use the v2-aware parser that
	// auto-detects legacy/v2 shape.
	manifestPath := filepath.Join(pkgDir, "workflow.toml")
	if info, err := os.Stat(manifestPath); err == nil && !info.IsDir() {
		raw, readErr := os.ReadFile(manifestPath)
		if readErr != nil {
			pc.Errors = append(pc.Errors, fmt.Sprintf("read workflow.toml: %v", readErr))
		} else {
			m, parseErr := parsePreLaunchManifest(raw)
			if parseErr != nil {
				pc.Errors = append(pc.Errors, fmt.Sprintf("parse workflow.toml: %v", parseErr))
			} else {
				if strings.TrimSpace(m.Name) == "" {
					pc.Errors = append(pc.Errors, "workflow.toml: name is required")
				}
				if strings.TrimSpace(m.Description) == "" {
					pc.Errors = append(pc.Errors, "workflow.toml: description is required")
				}
				if strings.TrimSpace(m.Version) == "" {
					pc.Errors = append(pc.Errors, "workflow.toml: version is required")
				}
			}
		}
	}

	// Walk every node; check tool_command scripts exist and class= names
	// match real policy classes.
	if g != nil {
		nodeIDs := sortedNodeIDs(g)
		var policyData *policy.Data
		for _, id := range nodeIDs {
			n := g.Nodes[id]
			if n == nil {
				continue
			}
			if cmd := strings.TrimSpace(n.Attr("tool_command", "")); cmd != "" {
				if rel := preLaunchScriptRelPath(cmd); rel != "" {
					abs := filepath.Join(pkgDir, rel)
					info, err := os.Stat(abs)
					if err != nil {
						pc.Errors = append(pc.Errors, fmt.Sprintf("node %q: tool_command references %q which does not exist at %s", id, cmd, abs))
					} else if !info.Mode().IsRegular() {
						pc.Errors = append(pc.Errors, fmt.Sprintf("node %q: script %s is not a regular file", id, abs))
					} else if info.Size() == 0 {
						pc.Errors = append(pc.Errors, fmt.Sprintf("node %q: script %s is empty", id, abs))
					}
				}
			}
			// agent_class= is the policy-resolution attribute. An unknown
			// name here is a typo and fails validation hard. (`class=` is
			// the unrelated CSS-style stylesheet selector — we don't
			// touch it here; the stylesheet engine handles its own
			// matching at parse time.)
			if cls := strings.TrimSpace(n.Attr(PolicyClassAttr, "")); cls != "" {
				if policyData == nil {
					d, err := policy.Load()
					if err != nil {
						pc.Errors = append(pc.Errors, fmt.Sprintf("policy load (for agent_class check): %v", err))
						break
					}
					policyData = d
				}
				if !preLaunchClassExists(policyData, cls) {
					pc.Errors = append(pc.Errors, fmt.Sprintf("node %q: agent_class=%q is not a real policy class or alias (run `kilroy policy list` to see available classes)", id, cls))
				}
			}
		}
	}

	if len(pc.Errors) > 0 {
		pc.Status = "fail"
	} else {
		pc.Status = "ok"
	}
	return pc
}

// preLaunchManifestHead is a minimal v2-vs-legacy discriminator. Mirrors
// the workflows package's parser to avoid the import cycle (workflows →
// engine).
type preLaunchManifestHead struct {
	Workflow struct {
		Name        string `toml:"name"`
		Version     string `toml:"version"`
		Description string `toml:"description"`
	} `toml:"workflow"`
	Name        string `toml:"name"`
	Description string `toml:"description"`
	Version     string `toml:"version"`
}

func parsePreLaunchManifest(raw []byte) (*preLaunchManifestHead, error) {
	var head preLaunchManifestHead
	if _, err := tomlDecodeBytes(raw, &head); err != nil {
		return nil, err
	}
	if head.Workflow.Name != "" || head.Workflow.Version != "" || head.Workflow.Description != "" {
		return &preLaunchManifestHead{
			Name:        head.Workflow.Name,
			Description: head.Workflow.Description,
			Version:     head.Workflow.Version,
		}, nil
	}
	return &head, nil
}

func preLaunchScriptRelPath(cmd string) string {
	fields := strings.Fields(cmd)
	if len(fields) < 2 {
		return ""
	}
	switch fields[0] {
	case "bash", "sh", "/bin/bash", "/bin/sh":
	default:
		return ""
	}
	const prefix = ".kilroy/package/"
	for _, f := range fields[1:] {
		if strings.HasPrefix(f, prefix) {
			return strings.TrimPrefix(f, prefix)
		}
	}
	return ""
}

func preLaunchClassExists(d *policy.Data, name string) bool {
	if _, ok := d.Classes[name]; ok {
		return true
	}
	for _, a := range d.Aliases {
		if a.From == name {
			if _, ok := d.Classes[a.To]; ok {
				return true
			}
		}
	}
	return false
}
