// Package agents provides AgentBackend implementations used by the unified
// dispatcher.
package agents

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/auth"
	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
	"github.com/danshapiro/kilroy/internal/auth/binding"
)

var _ agentbackend.AgentBackend = (*SDKBackend)(nil)
var _ agentbackend.AgentBackend = (*TmuxBackend)(nil)

func extractProvider(extra map[string]any) (string, error) {
	if extra == nil {
		return "", errors.New("TurnOptions.Extra is nil: provider is required")
	}
	val, ok := extra["provider"]
	if !ok {
		return "", errors.New("TurnOptions.Extra[provider] is missing: provider is required")
	}
	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("TurnOptions.Extra[%q] has type %T, expected string", "provider", val)
	}
	if strings.TrimSpace(str) == "" {
		return "", errors.New("TurnOptions.Extra[provider] is empty: provider is required")
	}
	return str, nil
}

type SDKBackendOption func(*SDKBackend)

func WithSDKAuthResolver(resolver auth.AuthResolver) SDKBackendOption {
	return func(b *SDKBackend) {
		b.authResolver = resolver
	}
}

// SDKBackend executes API/SDK routes through the engine agent runner and
// exposes the result as a TurnStream. The runner is normally AgentRouter,
// which owns HTTP transport construction, class-routed snapshot binding, and
// the API agent loop.
type SDKBackend struct {
	runner       engine.AgentBackend
	authResolver auth.AuthResolver
	lastOutcome  *runtime.Outcome
}

func NewSDKBackend(runner engine.AgentBackend, opts ...SDKBackendOption) *SDKBackend {
	if runner == nil {
		runner = &engine.SimulatedAgentBackend{}
	}
	b := &SDKBackend{runner: runner}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *SDKBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	provider, err := extractProvider(opts.Extra)
	if err != nil {
		return nil, err
	}
	execCtx, node, route := turnExecutionContext(opts, engine.BackendAPI)
	if route.Provider == "" {
		route.Provider = provider
	}
	if route.Model == "" {
		route.Model = opts.Model
	}
	if route.Driver == "" {
		if driver, ok := opts.Extra["driver"].(string); ok && strings.TrimSpace(driver) != "" {
			route.Driver = strings.TrimSpace(driver)
		} else {
			route.Driver = "anthropic_sdk"
		}
	}
	if route.Backend == "" {
		route.Backend = engine.BackendAPI
	}
	if b.authResolver != nil {
		if err := resolveTurnCredential(ctx, b.authResolver, route); err != nil {
			return nil, err
		}
	}

	resp, out, err := b.runner.Run(ctx, execCtx, node, msg.Text, route)
	if err != nil {
		return nil, err
	}
	b.lastOutcome = normalizeTurnOutcome(out, node, resp)
	return newOutcomeTurnStream(resp, b.lastOutcome, agentbackend.ToolControlKilroy), nil
}

func (b *SDKBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

func (b *SDKBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,
		TokenStreaming: true,
		CostTracking:   false,
		ToolInjection:  true,
	}
}

func (b *SDKBackend) Close() error { return nil }

func (b *SDKBackend) LastOutcome() *runtime.Outcome {
	if b == nil || b.lastOutcome == nil {
		return nil
	}
	cp := *b.lastOutcome
	return &cp
}

type TmuxBackendOption func(*TmuxBackend)

func WithTmuxAuthResolver(resolver auth.AuthResolver) TmuxBackendOption {
	return func(b *TmuxBackend) {
		b.authResolver = resolver
	}
}

type tmuxSessionHandler interface {
	ExecuteAgentWithSession(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute, tmpl *templates.Template, toolName string, prompt string, modelID string, cfg transport.SessionConfig) (runtime.Outcome, error)
}

// TmuxBackend executes CLI driver routes through the tmux session transport and
// exposes the completed session as a TurnStream.
type TmuxBackend struct {
	handler      tmuxSessionHandler
	templates    *templates.Registry
	authResolver auth.AuthResolver
	lastOutcome  *runtime.Outcome
}

func NewTmuxBackend(handler tmuxSessionHandler, opts ...TmuxBackendOption) *TmuxBackend {
	if handler == nil {
		handler = NewTmuxAgentHandler()
	}
	b := &TmuxBackend{
		handler:   handler,
		templates: templates.DefaultRegistry(),
	}
	if h, ok := handler.(*TmuxAgentHandler); ok && h.Templates != nil {
		b.templates = h.Templates
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *TmuxBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	provider, err := extractProvider(opts.Extra)
	if err != nil {
		return nil, err
	}
	execCtx, node, route := turnExecutionContext(opts, engine.BackendCLI)
	if route.Provider == "" {
		route.Provider = provider
	}
	if route.Model == "" {
		route.Model = opts.Model
	}
	if route.Driver == "" {
		if driver, ok := opts.Extra["driver"].(string); ok && strings.TrimSpace(driver) != "" {
			route.Driver = strings.TrimSpace(driver)
		} else {
			route.Driver = "claude_cli"
		}
	}
	if route.Backend == "" {
		route.Backend = engine.BackendCLI
	}
	if b.authResolver != nil {
		if err := resolveTurnCredential(ctx, b.authResolver, route); err != nil {
			return nil, err
		}
	}

	toolName := toolNameForDriver(route.Driver)
	if toolName == "" {
		return nil, fmt.Errorf("driver %q has no tmux tool mapping", route.Driver)
	}
	tmpl := b.templates.Get(toolName)
	if tmpl == nil {
		return nil, fmt.Errorf("no invocation template for tool %q", toolName)
	}

	runID := ""
	if execCtx != nil && execCtx.Engine != nil {
		runID = execCtx.Engine.Options.RunID
	}
	runtimeEnv := engine.BuildStageRuntimeEnv(execCtx, node.ID)
	statusContractEnv := map[string]string{}
	if execCtx != nil {
		statusContractEnv = engine.BuildStageStatusContract(execCtx.WorktreeDir, runID).EnvVars
	}

	var authSnapshot *binding.Snapshot
	if route.ClassResult != nil {
		authSnapshot = &route.ClassResult.AuthSnapshot
	}

	tmuxTransport := transport.NewTmuxTransport()
	cfg, err := tmuxTransport.BuildSession(
		tmpl,
		node.ID,
		runID,
		execCtx.WorktreeDir,
		execCtx.LogsRoot,
		route.Driver,
		authSnapshot,
		runtimeEnv,
		statusContractEnv,
		engine.BindSnapshot,
		engineBindWrapper,
	)
	if err != nil {
		return nil, fmt.Errorf("build session: %w", err)
	}

	out, err := b.handler.ExecuteAgentWithSession(ctx, execCtx, node, route, tmpl, toolName, msg.Text, route.Model, cfg)
	if err != nil {
		return nil, err
	}
	b.lastOutcome = normalizeTurnOutcome(&out, node, out.Notes)

	if stream, ok := cliStreamForStage(ctx, execCtx, node, tmpl, msg, opts); ok {
		return stream, nil
	}
	return newOutcomeTurnStream(out.Notes, b.lastOutcome, agentbackend.ToolControlDriver), nil
}

func (b *TmuxBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlDriver
}

func (b *TmuxBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,
		TokenStreaming: false,
		CostTracking:   false,
		ToolInjection:  false,
	}
}

func (b *TmuxBackend) Close() error { return nil }

func (b *TmuxBackend) LastOutcome() *runtime.Outcome {
	if b == nil || b.lastOutcome == nil {
		return nil
	}
	cp := *b.lastOutcome
	return &cp
}

func resolveTurnCredential(ctx context.Context, resolver auth.AuthResolver, route engine.AgentRoute) error {
	if route.ClassResult == nil {
		return nil
	}
	_, err := resolver.ResolveCredential(ctx, auth.AgentRoute{
		Provider:         route.Provider,
		Driver:           route.Driver,
		SnapshotIdentity: route.ClassResult.AuthSnapshot,
	})
	return err
}

func turnExecutionContext(opts agentbackend.TurnOptions, defaultBackend engine.BackendKind) (*engine.Execution, *model.Node, engine.AgentRoute) {
	extra := opts.Extra
	var execCtx *engine.Execution
	if v, ok := extra["exec"].(*engine.Execution); ok && v != nil {
		execCtx = v
	}
	if execCtx == nil {
		execCtx = &engine.Execution{LogsRoot: ".", WorktreeDir: "."}
	}
	var node *model.Node
	if v, ok := extra["node"].(*model.Node); ok && v != nil {
		node = v
	}
	if node == nil {
		node = &model.Node{ID: "turn", Attrs: map[string]string{}}
	}
	route := engine.AgentRoute{}
	if v, ok := extra["route"].(engine.AgentRoute); ok {
		route = v
	}
	if route.Model == "" {
		route.Model = opts.Model
	}
	if route.Backend == "" {
		route.Backend = defaultBackend
	}
	return execCtx, node, route
}

func normalizeTurnOutcome(out *runtime.Outcome, node *model.Node, response string) *runtime.Outcome {
	if out == nil {
		out = &runtime.Outcome{Status: runtime.StatusSuccess, Notes: "agent turn completed"}
	}
	cp := *out
	if cp.Status == "" {
		cp.Status = runtime.StatusSuccess
	}
	if cp.ContextUpdates == nil {
		cp.ContextUpdates = map[string]any{}
	}
	if node != nil {
		if _, ok := cp.ContextUpdates["last_stage"]; !ok {
			cp.ContextUpdates["last_stage"] = node.ID
		}
	}
	if _, ok := cp.ContextUpdates["last_response"]; !ok {
		cp.ContextUpdates["last_response"] = engine.Truncate(response, 200)
	}
	return &cp
}

func cliStreamForStage(ctx context.Context, execCtx *engine.Execution, node *model.Node, tmpl *templates.Template, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, bool) {
	if execCtx == nil || node == nil || tmpl == nil || !tmpl.StructuredOutput {
		return nil, false
	}
	path := filepath.Join(transport.StageDir(execCtx.LogsRoot, node.ID), "agent_output.jsonl")
	if _, err := os.Stat(path); err != nil {
		return nil, false
	}
	stream, err := (&CLIBackend{Tool: tmpl.Name, AgentOutputPath: path}).StartTurn(ctx, msg, opts)
	if err != nil {
		return nil, false
	}
	return stream, true
}

type outcomeTurnStream struct {
	text     string
	outcome  *runtime.Outcome
	mode     agentbackend.ToolControlMode
	sentText bool
	sentEnd  bool
}

func newOutcomeTurnStream(text string, outcome *runtime.Outcome, mode agentbackend.ToolControlMode) *outcomeTurnStream {
	return &outcomeTurnStream{text: strings.TrimSpace(text), outcome: outcome, mode: mode}
}

func (s *outcomeTurnStream) Recv() (agentbackend.TurnEvent, error) {
	if !s.sentText && s.text != "" {
		s.sentText = true
		return agentbackend.TurnEvent{Type: agentbackend.TurnEventText, Text: s.text}, nil
	}
	if !s.sentEnd {
		s.sentEnd = true
		stopReason := "end_turn"
		if s.outcome != nil && s.outcome.Status == runtime.StatusFail {
			stopReason = "error"
		}
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventTurnEnd,
			End:  &agentbackend.TurnEndInfo{StopReason: stopReason},
		}, nil
	}
	return agentbackend.TurnEvent{}, io.EOF
}

func (s *outcomeTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	if s.mode == agentbackend.ToolControlDriver {
		return agentbackend.ErrToolControlDriver
	}
	return nil
}

func (s *outcomeTurnStream) Close() error { return nil }
