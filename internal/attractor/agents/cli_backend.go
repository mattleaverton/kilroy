// CLI-driver AgentBackend adapter. Wraps the tmux-based CLI execution
// path (claude/codex/gemini) in the agentbackend.AgentBackend interface
// so callers can drain a TurnStream of TurnEvents instead of poking at
// agent_output.jsonl directly.
//
// Block 6 Step 3: this is the materialized seam. The runtime tmux path
// already runs the codecs (extract.go); this adapter exposes the same
// stream as a typed interface for downstream consumers (orchestration
// layer, summary stages, future tool-injection loop).
//
// ToolControl returns ToolControlDriver — the CLI binary owns its own
// tool-call dispatch loop. SendToolResult on the returned TurnStream
// always returns ErrToolControlDriver. Cancellation is via the context
// passed to StartTurn.
//
// Today the adapter is batched-after-completion: StartTurn launches the
// session, waits for it to exit, parses the entire JSONL via the codec,
// then returns a TurnStream that drains the buffered events. A future
// extension can consume the live tailer to emit events incrementally.

package agents

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
)

// CLIBackend is an AgentBackend implementation backed by a completed
// tmux CLI session. The Tool field selects which codec parses the
// session's JSONL output (claude, codex; future: gemini).
//
// Lifecycle: caller invokes StartTurn after the tmux session has exited
// and agent_output.jsonl is on disk. StartTurn parses the JSONL via the
// matching codec and returns a TurnStream that emits the buffered
// events.
type CLIBackend struct {
	// Tool names the CLI driver — used for codec dispatch.
	// Recognized: "claude", "codex".
	Tool string

	// AgentOutputPath is the absolute path to the JSONL file the
	// session wrote on exit (e.g. <stage_dir>/agent_output.jsonl).
	AgentOutputPath string
}

// StartTurn parses the session's JSONL and returns a TurnStream that
// drains the buffered events. The msg/opts are unused in this batched
// adapter — the turn already happened; we're just surfacing what the
// CLI binary produced.
func (b *CLIBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	data, err := os.ReadFile(b.AgentOutputPath)
	if err != nil {
		return nil, err
	}
	events, err := agentbackend.ParseForTool(b.Tool, data)
	if err != nil {
		return nil, err
	}
	return &cliTurnStream{events: events}, nil
}

// ToolControl reports that the CLI binary owns the tool-call dispatch
// loop — kilroy observes events but does not inject tool results.
func (b *CLIBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlDriver
}

// Capabilities reports a conservative baseline. Token streaming is
// false because this is a batched-after-completion adapter; thinking
// is true because both claude and codex emit thinking blocks; cost
// tracking is true for claude (result event carries total_cost_usd).
func (b *CLIBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,
		TokenStreaming: false,
		CostTracking:   b.Tool == "claude",
		ToolInjection:  false,
	}
}

// Close is a no-op for batched adapters — the tmux session is the
// caller's lifecycle to manage.
func (b *CLIBackend) Close() error { return nil }

// cliTurnStream drains a buffered slice of events and returns io.EOF
// after the last one. SendToolResult always returns ErrToolControlDriver
// because this adapter operates in driver-owned tool-control mode.
type cliTurnStream struct {
	mu     sync.Mutex
	events []agentbackend.TurnEvent
	idx    int
	closed bool
}

func (s *cliTurnStream) Recv() (agentbackend.TurnEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return agentbackend.TurnEvent{}, errors.New("agentbackend: stream closed")
	}
	if s.idx >= len(s.events) {
		return agentbackend.TurnEvent{}, io.EOF
	}
	ev := s.events[s.idx]
	s.idx++
	return ev, nil
}

// SendToolResult on a CLIBackend stream is always ErrToolControlDriver —
// the driver binary owns the tool loop, kilroy cannot inject results.
func (s *cliTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	return agentbackend.ErrToolControlDriver
}

func (s *cliTurnStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
