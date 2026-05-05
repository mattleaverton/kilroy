// TmuxAgentHandler executes agent nodes by spawning CLI tools in tmux sessions.
// This replaces the subprocess-pipe model with observable, persistent sessions.
package agents

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/tmux"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
	"github.com/danshapiro/kilroy/internal/auth/binding"
)

const kilroySocket = "kilroy"

// TmuxAgentHandler invokes LLM CLI tools via tmux sessions.
type TmuxAgentHandler struct {
	Tmux      *tmux.Manager
	Templates *templates.Registry
	Timeout   time.Duration // default timeout per node (0 = 30 min)

	// PolicyDeps lets tests inject policy data and machine-state collection.
	// Zero value means production defaults (policy.Load / CollectMachineState).
	PolicyDeps engine.PolicyDeps
}

// NewTmuxAgentHandler creates a handler with default tmux manager and templates.
func NewTmuxAgentHandler() *TmuxAgentHandler {
	return &TmuxAgentHandler{
		Tmux:      tmux.NewManager(kilroySocket),
		Templates: templates.DefaultRegistry(),
		Timeout:   30 * time.Minute,
	}
}

// UsesFidelity implements engine.FidelityAwareHandler.
func (h *TmuxAgentHandler) UsesFidelity() bool { return true }

// RequiresProvider implements engine.ProviderRequiringHandler.
func (h *TmuxAgentHandler) RequiresProvider() bool { return true }

// Execute implements engine.Handler for legacy direct calls. Normal engine and
// dispatcher execution use ExecuteAgent with an explicit route.
func (h *TmuxAgentHandler) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	route, err := engine.ResolveAgentRoute(node, exec, h.PolicyDeps)
	if err != nil {
		return engine.AgentRouteFailureOutcome(err), nil
	}
	return h.ExecuteAgent(ctx, exec, node, route)
}

// ExecuteAgent implements route-aware agent execution. It spawns a CLI tool in
// a tmux session, waits for completion, captures output, and returns an
// outcome.
func (h *TmuxAgentHandler) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	// Map driver → tmux tool. CLI drivers have a 1:1 mapping; SDK
	// drivers (anthropic_sdk/openai_sdk/google_sdk) should not have
	// reached the tmux handler — that's a dispatch bug. Route-aware
	// execution uses the supplied route as the contract and does not
	// reinterpret agent_tool= or stylesheet attributes.
	if strings.TrimSpace(route.Driver) == "" {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("resolved route for node %q has no driver; tmux requires a CLI driver", node.ID),
			Meta:          map[string]any{"failure_class": "deterministic"},
		}, nil
	}
	toolName := toolNameForDriver(route.Driver)
	if toolName == "" {
		return runtime.Outcome{
			Status: runtime.StatusFail,
			FailureReason: fmt.Sprintf(
				"driver %q has no tmux tool mapping; this driver should not have reached the tmux handler — check dispatch routing",
				route.Driver),
			Meta: map[string]any{"failure_class": "deterministic"},
		}, nil
	}
	tmpl := h.Templates.Get(toolName)
	if tmpl == nil {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("no invocation template for tool %q", toolName),
		}, nil
	}

	// Build prompt from node attributes.
	prompt := strings.TrimSpace(node.Prompt())
	if prompt == "" {
		prompt = node.Label()
	}
	if wtPreamble := strings.TrimSpace(engine.BuildWorktreeContextPreamble(exec.WorktreeDir)); wtPreamble != "" {
		if strings.TrimSpace(prompt) == "" {
			prompt = wtPreamble
		} else {
			prompt = wtPreamble + "\n\n" + strings.TrimSpace(prompt)
		}
	}

	// Resolve model: class-resolved value wins; otherwise legacy llm_model.
	modelID := route.Model
	if modelID == "" {
		modelID = strings.TrimSpace(node.Attr("llm_model", ""))
	}

	// Build the session config using the transport layer.
	runID := ""
	if exec != nil && exec.Engine != nil {
		runID = exec.Engine.Options.RunID
	}
	runtimeEnv := engine.BuildStageRuntimeEnv(exec, node.ID)
	statusContractEnv := map[string]string{}
	if exec != nil {
		statusContractEnv = engine.BuildStageStatusContract(exec.WorktreeDir, runID).EnvVars
	}

	var authSnapshot *binding.Snapshot
	if route.ClassResult != nil {
		authSnapshot = &route.ClassResult.AuthSnapshot
	}

	// Build session config using the transport layer.
	transport := transport.NewTmuxTransport()
	cfg, err := transport.BuildSession(
		tmpl,
		node.ID,
		runID,
		exec.WorktreeDir,
		exec.LogsRoot,
		route.Driver,
		authSnapshot,
		runtimeEnv,
		statusContractEnv,
		engine.BindSnapshot,
		engineBindWrapper,
	)
	if err != nil {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("build session: %v", err),
		}, nil
	}

	// Execute using the pre-built session config.
	return h.ExecuteAgentWithSession(ctx, exec, node, route, tmpl, toolName, prompt, modelID, cfg)
}

// ExecuteAgentWithSession executes an agent using a pre-built session configuration.
// This allows the transport layer to build the session config separately from execution.
func (h *TmuxAgentHandler) ExecuteAgentWithSession(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute, tmpl *templates.Template, toolName, prompt, modelID string, cfg transport.SessionConfig) (runtime.Outcome, error) {
	// Extract values from session config.
	sessionName := cfg.SessionName
	env := cfg.Env
	envScrub := cfg.EnvScrub
	stageDir := transport.StageDir(exec.LogsRoot, node.ID)
	_ = os.MkdirAll(stageDir, 0o755)

	// Write credential files from bind result if present.
	if cfg.BindResult != nil {
		for path, content := range cfg.BindResult.FilesToWrite {
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return runtime.Outcome{
					Status:        runtime.StatusFail,
					FailureReason: fmt.Sprintf("write credential file dir: %v", err),
				}, nil
			}
			if err := os.WriteFile(path, content, 0o600); err != nil {
				return runtime.Outcome{
					Status:        runtime.StatusFail,
					FailureReason: fmt.Sprintf("write credential file: %v", err),
				}, nil
			}
		}
	}

	// Surface the resolved provider + model to the template's
	// PrepareSession via env. Multi-provider templates (opencode) read
	// these to construct OPENCODE_CONFIG_CONTENT for the right provider
	// rather than hardcoding anthropic. Single-provider templates
	// ignore them.
	if route.Provider != "" {
		env["KILROY_AGENT_PROVIDER"] = route.Provider
	}
	if modelID != "" {
		env["KILROY_AGENT_MODEL"] = modelID
	}

	// Emit provider_selected event so tmux runs match the API path's surface.
	runID := ""
	if exec != nil && exec.Engine != nil {
		runID = exec.Engine.Options.RunID
	}
	if exec != nil && exec.Engine != nil {
		source := route.Source
		if source == "" {
			source = "graph_attrs"
		}
		provider := route.Provider
		if provider == "" {
			provider = strings.TrimSpace(node.Attr("llm_provider", ""))
		}
		exec.Engine.AppendProgress(map[string]any{
			"event":    "provider_selected",
			"node_id":  node.ID,
			"provider": provider,
			"model":    modelID,
			"backend":  "cli",
			"source":   source,
		})
	}

	// Build and write the command. Pass auth_method so the template can
	// adjust args (e.g. claude omits --bare for cli_oauth, which is
	// incompatible with OAuth). Non-class routes have empty auth_method →
	// templates fall back to their default args.
	authMethod := route.AuthMethod()
	command := tmpl.BuildCommand(prompt, exec.WorktreeDir, modelID, authMethod, route.Provider)
	// When the template produces structured JSONL output, redirect it to a
	// known file so the log parser can find it without hunting through
	// tool-specific directories.
	agentOutputPath := filepath.Join(stageDir, "agent_output.jsonl")
	if tmpl.StructuredOutput {
		command = command + " > " + transport.ShellQuoteSimple(agentOutputPath) + " 2>&1"
	}

	// Wrap the command in `env -u VAR -u VAR2 ...` so the child process
	// literally does not see scrubbed env vars. tmux new-session inherits
	// the launcher's env, so deleting from our env map (above) is not
	// enough — `env -u` is what actually unsets them in the child. This is
	// the load-bearing wrong-billing prevention: claude_cli's binder
	// scrubs ANTHROPIC_API_KEY here so the CLI uses the logged-in
	// subscription session rather than silently using the env key.
	if len(envScrub) > 0 {
		var prefix strings.Builder
		prefix.WriteString("env")
		for _, name := range envScrub {
			prefix.WriteString(" -u ")
			prefix.WriteString(name)
		}
		prefix.WriteString(" ")
		command = prefix.String() + command
	}

	// Run per-tool session preparation (e.g. write isolated config files).
	if tmpl.PrepareSession != nil {
		if err := tmpl.PrepareSession(stageDir, env); err != nil {
			return runtime.Outcome{
				Status:        runtime.StatusFail,
				FailureReason: fmt.Sprintf("prepare session for %s: %v", toolName, err),
			}, nil
		}
	}
	_ = os.WriteFile(filepath.Join(stageDir, "tmux_command.txt"), []byte(command), 0o644)

	// Write prompt for debugging.
	_ = os.WriteFile(filepath.Join(stageDir, "prompt.md"), []byte(prompt), 0o644)

	// Emit progress event.
	if exec.Engine != nil {
		exec.Engine.AppendProgress(map[string]any{
			"event":   "tmux_session_start",
			"node_id": node.ID,
			"tool":    toolName,
			"session": sessionName,
		})
	}

	// Create tmux session.
	sessionStartTime := time.Now()
	session, err := h.Tmux.CreateSession(sessionName, exec.WorktreeDir, command, env)
	if err != nil {
		return runtime.Outcome{
			Status:         runtime.StatusFail,
			FailureReason:  fmt.Sprintf("create tmux session: %v", err),
			Meta:           map[string]any{"failure_class": "transient_infra"},
			ContextUpdates: map[string]any{"failure_class": "transient_infra"},
		}, nil
	}
	// Defensive cleanup: any return between here and the explicit
	// DestroySession below leaks the session on the kilroy tmux server.
	// Idempotent — DestroySession on an already-destroyed session is a
	// no-op error we ignore.
	defer func() { _ = h.Tmux.DestroySession(sessionName) }()

	// Store session metadata.
	_ = h.Tmux.SetEnvironment(sessionName, "KILROY_RUN_ID", runID)
	_ = h.Tmux.SetEnvironment(sessionName, "KILROY_NODE_ID", node.ID)

	// Handle startup dialogs.
	for _, dialog := range tmpl.StartupDialogs {
		h.handleStartupDialog(session.Name, dialog, tmpl.StartupTimeout)
	}

	// Start real-time log tailer if structured output is enabled.
	// Emits agent events to RunLog as they appear, rather than waiting
	// for completion. Works for any CLI tool that writes JSONL.
	var tailCancel context.CancelFunc
	if tmpl.StructuredOutput && exec.Engine != nil && exec.Engine.RunLog != nil {
		lineParser := agentlog.LineParserForTool(tmpl.Name)
		if lineParser != nil {
			tailCtx, cancel := context.WithCancel(ctx)
			tailCancel = cancel
			go agentlog.TailJSONL(tailCtx, agentOutputPath, lineParser, func(ev agentlog.AgentEvent) {
				exec.Engine.RunLog.Info("agent", node.ID, ev.Type, ev.Message, ev.Data)
				exec.Engine.TickStallWatchdog()
			}, agentlog.TailConfig{PollInterval: 500 * time.Millisecond})
		}
	}

	// Determine timeout.
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}

	// Wait for completion.
	var waitErr error
	if tmpl.ExitsOnComplete {
		waitErr = h.Tmux.WaitForExit(ctx, sessionName, timeout)
	} else {
		waitErr = h.Tmux.WaitForIdle(ctx, sessionName, tmux.WaitConfig{
			PromptPrefix:    tmpl.PromptPrefix,
			BusyIndicators:  tmpl.BusyIndicators,
			ConsecutiveIdle: 2,
			PollInterval:    200 * time.Millisecond,
		}, timeout)
	}

	// Stop the real-time log tailer — give it a moment to drain remaining lines.
	if tailCancel != nil {
		// Brief sleep to let the tailer pick up final lines written before exit.
		time.Sleep(600 * time.Millisecond)
		tailCancel()
	}

	// Capture output and exit status before destroying the session.
	output, _ := h.Tmux.CaptureOutput(sessionName, 0)
	exitCode := h.Tmux.PaneExitStatus(sessionName)

	// When structured output was redirected to a file, the pane is empty.
	// Extract the response text from the JSONL via the unified
	// agentbackend codec (Block 6 Step 3) — same TurnEvent surface every
	// codec produces, so response.md generation is no longer keyed on
	// per-tool extractor logic. Falls through to legacy agentlog
	// extractor for tools the new codec doesn't yet cover (opencode).
	if tmpl.StructuredOutput {
		if jsonlData, err := os.ReadFile(agentOutputPath); err == nil {
			responseText := agentbackend.ParseAndExtractText(tmpl.Name, jsonlData)
			if responseText == "" {
				// Codec returned empty (unknown tool, parse hiccup) —
				// preserve the legacy extractor as a safety net so a
				// regression here doesn't blank response.md.
				responseText = agentlog.ExtractResponseText(tmpl.Name, jsonlData)
			}
			if responseText != "" {
				output = responseText
			}
		}
	}
	if strings.TrimSpace(output) != "" {
		_ = os.WriteFile(filepath.Join(stageDir, "response.md"), []byte(output), 0o644)
	}

	// If no real-time tailer was running, do a batch parse of the log.
	if tailCancel == nil && exec.Engine != nil && exec.Engine.RunLog != nil {
		h.emitAgentLogEvents(exec, node.ID, tmpl, stageDir, sessionStartTime)
	}

	// Clean up session.
	_ = h.Tmux.DestroySession(sessionName)

	// Emit completion event.
	if exec.Engine != nil {
		exec.Engine.AppendProgress(map[string]any{
			"event":      "tmux_session_complete",
			"node_id":    node.ID,
			"tool":       toolName,
			"session":    sessionName,
			"exit_code":  exitCode,
			"output_len": len(output),
			"wait_error": fmt.Sprint(waitErr),
		})
	}

	if waitErr != nil {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("agent timeout: %v", waitErr),
			Meta:          map[string]any{"failure_class": "transient_infra"},
			ContextUpdates: map[string]any{
				"failure_class": "transient_infra",
				"last_stage":    node.ID,
				"last_response": engine.Truncate(output, 200),
			},
		}, nil
	}

	// Check exit code for failure detection.
	if exitCode > 0 {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("agent exited with code %d", exitCode),
			Meta:          map[string]any{"failure_class": "deterministic", "exit_code": exitCode},
			ContextUpdates: map[string]any{
				"failure_class": "deterministic",
				"last_stage":    node.ID,
				"last_response": engine.Truncate(output, 200),
			},
		}, nil
	}

	return runtime.Outcome{
		Status: runtime.StatusSuccess,
		Notes:  fmt.Sprintf("agent completed via tmux (%s)", toolName),
		ContextUpdates: map[string]any{
			"last_stage":    node.ID,
			"last_response": engine.Truncate(output, 200),
		},
	}, nil
}

// emitAgentLogEvents parses the agent's structured output and emits events to RunLog.
// Reads from the known agent_output.jsonl in the stage dir first, falls back to
// the template's LogLocator for non-structured-output modes.
func (h *TmuxAgentHandler) emitAgentLogEvents(exec *engine.Execution, nodeID string, tmpl *templates.Template, stageDir string, startedAfter time.Time) {
	// Primary: read from known output file.
	logPath := filepath.Join(stageDir, "agent_output.jsonl")
	if _, err := os.Stat(logPath); err != nil {
		// Fallback: use LogLocator to find tool-specific log files.
		if tmpl.LogLocator != nil {
			found, locErr := tmpl.LogLocator.FindLog(exec.WorktreeDir, startedAfter)
			if locErr != nil {
				exec.Engine.RunLog.Warn("agent", nodeID, "agent.log_not_found", fmt.Sprintf("Agent log not found: %v", locErr))
				return
			}
			logPath = found
		} else {
			return
		}
	}

	parser := agentlog.ParserForTool(tmpl.Name)
	if parser == nil {
		return
	}

	events, err := parser(logPath)
	if err != nil {
		exec.Engine.RunLog.Warn("agent", nodeID, "agent.log_parse_error", fmt.Sprintf("Parse agent log: %v", err))
		return
	}

	for _, ev := range events {
		exec.Engine.RunLog.Info("agent", nodeID, ev.Type, ev.Message, ev.Data)
	}
}

// handleStartupDialog polls for a startup dialog and dismisses it.
func (h *TmuxAgentHandler) handleStartupDialog(session string, dialog templates.StartupDialog, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lines, _ := h.Tmux.CaptureLines(session, 15)
		content := strings.Join(lines, "\n")
		detected := false
		for _, pattern := range dialog.DetectPatterns {
			if strings.Contains(content, pattern) {
				detected = true
				break
			}
		}
		if detected {
			for _, key := range dialog.Keys {
				h.Tmux.SendKeys(session, key)
				time.Sleep(200 * time.Millisecond)
			}
			if dialog.DelayAfter > 0 {
				time.Sleep(dialog.DelayAfter)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// toolNameForDriver maps a policy driver name to a tmux tool name. Only CLI
// drivers map to a tool; SDK drivers (anthropic_sdk, openai_sdk, google_sdk)
// have no tmux tool and return "".
func toolNameForDriver(driver string) string {
	switch driver {
	case "claude_cli":
		return "claude"
	case "codex_cli":
		return "codex"
	case "gemini_cli":
		return "gemini"
	case "opencode":
		return "opencode"
	default:
		return ""
	}
}

// engineBindWrapper wraps engine.Bind to return transport.BindResult.
// This adapter allows transport.MaterializeCredential to work with the
// engine's binding functions without creating an import cycle.
func engineBindWrapper(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (transport.BindResult, error) {
	engResult, err := engine.Bind(driver, snap, cred, stageDir)
	if err != nil {
		return transport.BindResult{}, err
	}
	return transport.BindResult{
		EnvSet:       engResult.EnvSet,
		EnvScrub:     engResult.EnvScrub,
		FilesToWrite: engResult.FilesToWrite,
		SDKArg:       engResult.SDKArg,
		SourceName:   engResult.SourceName,
		SourceKind:   engResult.SourceKind,
	}, nil
}
