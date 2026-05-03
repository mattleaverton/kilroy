// CLIBackend exercises the AgentBackend seam (Block 6 Step 3) for the
// claude/codex CLI paths. Asserts: ToolControl is driver-owned,
// SendToolResult fails with ErrToolControlDriver, Recv drains buffered
// events and returns io.EOF cleanly.

package agents

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
)

const claudeJSONL = `{"type":"system","subtype":"init","cwd":"/tmp"}
{"type":"assistant","message":{"content":[{"type":"text","text":"Hello"}]}}
{"type":"assistant","message":{"content":[{"type":"text","text":"World"}]}}
{"type":"result","subtype":"success","result":"Hello World","total_cost_usd":0.01,"usage":{"input_tokens":10,"output_tokens":3}}
`

func TestCLIBackend_Claude_DrainsEventsToEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent_output.jsonl")
	if err := os.WriteFile(path, []byte(claudeJSONL), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	b := &CLIBackend{Tool: "claude", AgentOutputPath: path}
	stream, err := b.StartTurn(context.Background(), agentbackend.UserMessage{}, agentbackend.TurnOptions{})
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	defer stream.Close()

	var texts []string
	var sawTurnEnd bool
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		switch ev.Type {
		case agentbackend.TurnEventText:
			texts = append(texts, ev.Text)
		case agentbackend.TurnEventTurnEnd:
			sawTurnEnd = true
			if ev.End == nil {
				t.Error("TurnEventTurnEnd has nil End")
			} else if ev.End.StopReason != "success" {
				t.Errorf("StopReason = %q, want success", ev.End.StopReason)
			}
		}
	}
	if len(texts) != 2 || texts[0] != "Hello" || texts[1] != "World" {
		t.Errorf("texts = %v, want [Hello World]", texts)
	}
	if !sawTurnEnd {
		t.Error("expected TurnEventTurnEnd")
	}
}

func TestCLIBackend_ToolControlIsDriver(t *testing.T) {
	b := &CLIBackend{Tool: "claude"}
	if b.ToolControl() != agentbackend.ToolControlDriver {
		t.Errorf("ToolControl = %v, want ToolControlDriver", b.ToolControl())
	}
}

func TestCLIBackend_SendToolResult_ReturnsErrToolControlDriver(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent_output.jsonl")
	// Minimal valid JSONL so StartTurn doesn't fail.
	if err := os.WriteFile(path, []byte(`{"type":"system","subtype":"init"}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	b := &CLIBackend{Tool: "claude", AgentOutputPath: path}
	stream, err := b.StartTurn(context.Background(), agentbackend.UserMessage{}, agentbackend.TurnOptions{})
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	defer stream.Close()
	err = stream.SendToolResult(context.Background(), agentbackend.ToolResult{ToolUseID: "x"})
	if !errors.Is(err, agentbackend.ErrToolControlDriver) {
		t.Errorf("SendToolResult err = %v, want ErrToolControlDriver", err)
	}
}

func TestCLIBackend_StartTurn_MissingFile_Errors(t *testing.T) {
	b := &CLIBackend{Tool: "claude", AgentOutputPath: "/does/not/exist"}
	_, err := b.StartTurn(context.Background(), agentbackend.UserMessage{}, agentbackend.TurnOptions{})
	if err == nil {
		t.Fatal("expected error for missing JSONL file")
	}
}

func TestCLIBackend_Capabilities_ClaudeReportsCostTracking(t *testing.T) {
	b := &CLIBackend{Tool: "claude"}
	caps := b.Capabilities()
	if !caps.Thinking {
		t.Error("expected Thinking=true")
	}
	if !caps.CostTracking {
		t.Error("expected CostTracking=true for claude")
	}
	if caps.ToolInjection {
		t.Error("expected ToolInjection=false for driver-owned loop")
	}
}

func TestCLIBackend_Capabilities_CodexNoCost(t *testing.T) {
	// Codex's turn.completed events don't carry cost info, so the
	// adapter reports CostTracking=false to set caller expectations.
	b := &CLIBackend{Tool: "codex"}
	if b.Capabilities().CostTracking {
		t.Error("expected CostTracking=false for codex")
	}
}

func TestCLIBackend_StreamRecvAfterClose_Errors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent_output.jsonl")
	if err := os.WriteFile(path, []byte(claudeJSONL), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	b := &CLIBackend{Tool: "claude", AgentOutputPath: path}
	stream, err := b.StartTurn(context.Background(), agentbackend.UserMessage{}, agentbackend.TurnOptions{})
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	stream.Close()
	if _, err := stream.Recv(); err == nil {
		t.Error("expected error from Recv after Close")
	}
}
