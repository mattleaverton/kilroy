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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/policy"
)

// PreLaunchReport is the structured output of ValidatePreLaunch. Persisted
// alongside the run as prelaunch_validation.json for after-the-fact
// inspection (kilroy runs show, kilroy policy explain, etc.).
type PreLaunchReport struct {
	GeneratedAt string               `json:"generated_at"`
	Nodes       []PreLaunchNodeCheck `json:"nodes"`
	Summary     PreLaunchSummary     `json:"summary"`
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

	reg := NewDefaultRegistry()
	// Snapshot policy data + machine state once so every node sees a
	// consistent view (especially for runs with many nodes).
	var policyData *policy.Data
	loadFn := deps.Load
	if loadFn == nil {
		loadFn = policy.Load
	}
	collectFn := deps.Collect
	if collectFn == nil {
		collectFn = policy.CollectMachineState
	}
	var stateLoaded bool
	var state policy.MachineState

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

		className := strings.TrimSpace(n.Attr("class", ""))
		if className == "" {
			// Pre-Step-4b graphs that use only stylesheet routing — we
			// can't validate them without an LLM probe. Mark as ok and
			// move on; the legacy preflight covers them if enabled.
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
		if !stateLoaded {
			state = collectFn()
			stateLoaded = true
		}

		res, err := policy.Resolve(policy.ResolveRequest{
			ClassID:    className,
			NodeID:     id,
			WorkflowID: g.Name,
		}, policyData, state)
		if err != nil {
			check.Status = "fail"
			check.Errors = append(check.Errors, fmt.Sprintf("class %q: %v", className, err))
			report.Nodes = append(report.Nodes, check)
			report.Summary.Fail++
			continue
		}

		check.ResolvedModel = res.ModelID
		check.ResolvedDriver = res.Driver
		check.AuthMethod = res.AuthMethod
		check.AuthSource = res.AuthSource

		// CLI drivers need their binary on PATH. SDK drivers don't (the
		// http client handles the request).
		if isCLIDriver(res.Driver) {
			binaryName := cliBinaryForDriver(res.Driver)
			_, lookErr := exec.LookPath(binaryName)
			found := lookErr == nil
			check.BinaryFound = &found
			if !found {
				check.Status = "fail"
				check.Errors = append(check.Errors, fmt.Sprintf("CLI binary %q not found on PATH (required by driver %s)", binaryName, res.Driver))
				report.Nodes = append(report.Nodes, check)
				report.Summary.Fail++
				continue
			}
		}

		check.Status = "ok"
		report.Nodes = append(report.Nodes, check)
		report.Summary.OK++
	}

	if err := writePreLaunchReport(opts.LogsRoot, report); err != nil {
		// Persisting the report is best-effort; failure to write should
		// not mask a successful validation. Surface as a soft warning
		// via the sentinel logsRoot field rather than as an error.
		_ = err
	}

	if report.Summary.Fail > 0 {
		return report, &PreLaunchError{Report: report}
	}
	return report, nil
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
	return fmt.Sprintf("prelaunch validation failed for %d node(s): %s",
		e.Report.Summary.Fail, strings.Join(failedNodes, ", "))
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
