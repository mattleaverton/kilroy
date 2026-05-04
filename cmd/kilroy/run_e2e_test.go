// End-to-end test: full kilroy-run lifecycle against a workflow package
// with a fake `claude` binary substituting for the real LLM. The fake
// emits realistic stream-json (lifted from real run captures), creates
// the workflow's expected output files, and writes the stage status —
// everything else (engine, prelaunch, class resolution, tmux dispatch,
// agent_output.jsonl parsing, response.md extraction, summary stage,
// per-stage commits) is exercised against real code.
//
// This is the safety net for Block 6 step 2 (codec extraction) — the
// codec rewrite must keep this test green or document an intentional
// behavioral change.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeClaudeStreamJSON is a minimal but realistic claude --print
// --output-format=stream-json conversation. Three lines: an init event,
// a single assistant message with text content, and a result event
// with the final answer string. Mirrors the smallest captures we have
// in ~/.local/state/kilroy/attractor/runs/*/agent/agent_output.jsonl.
//
// This shape is what extractClaudeResponseText (in
// internal/attractor/agents/agentlog/extract.go) actually parses; the
// test would catch any regression that breaks that parsing path.
const fakeClaudeStreamJSON = `{"type":"system","subtype":"init","cwd":"/tmp","session_id":"e2e-test","tools":["Bash","Read","Edit","Write"],"model":"claude-opus-4-7","permissionMode":"bypassPermissions","apiKeySource":"none","claude_code_version":"e2e-test","output_style":"default","uuid":"00000000-0000-0000-0000-000000000001"}
{"type":"assistant","message":{"model":"claude-opus-4-7","id":"msg_e2e","type":"message","role":"assistant","content":[{"type":"text","text":"Created E2E_MARKER.md per the prompt. All done."}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":20}},"parent_tool_use_id":null,"session_id":"e2e-test","uuid":"00000000-0000-0000-0000-000000000002"}
{"type":"result","subtype":"success","is_error":false,"result":"Created E2E_MARKER.md per the prompt. All done.","session_id":"e2e-test","uuid":"00000000-0000-0000-0000-000000000003","total_cost_usd":0.001,"usage":{"input_tokens":100,"output_tokens":20}}
`

// stageFakeClaude writes a bash script in dir that pretends to be the
// claude CLI. It reads .kilroy/INPUT.md from cwd (the worktree) to
// prove the engine staged the input correctly, creates a deterministic
// output file (E2E_MARKER.md), writes a result.md the implement
// workflow's summary stage will read, emits stream-json on stdout
// (which the engine redirects to agent_output.jsonl), and writes
// {"status":"success"} to $KILROY_STAGE_STATUS_PATH.
func stageFakeClaude(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "claude")
	// The engine invokes claude with: --bare --dangerously-skip-permissions
	// --print --output-format stream-json --verbose --model <model> '<prompt>'
	// We don't parse the args; we just respond.
	//
	// One subtle gotcha: the engine creates a detached tmux session with
	// `new-session -d` and then replaces the placeholder shell via
	// `respawn-pane`. If the command exits faster than tmux's pane bring-up
	// settles, the session reports "Pane is dead (status 0)" before the
	// engine can attach for output capture, and the stage records a
	// transient_infra failure. A small sleep at the end keeps the pane
	// alive long enough for the engine to read pane state cleanly.
	script := `#!/bin/bash
set -uo pipefail

# Prove the engine staged INPUT.md by reading it (best-effort).
if [ -r ".kilroy/INPUT.md" ]; then
  : "$(cat .kilroy/INPUT.md > /dev/null)"
fi

# Create the deterministic file the test prompt asks for.
echo "e2e marker" > E2E_MARKER.md

# Write a result.md so the workflow's summary stage finds one and
# doesn't synthesize a BLOCKED stub.
cat > result.md <<'EOF'
# E2E test result

## Summary
Created E2E_MARKER.md per the prompt. All done.

## Files touched
- E2E_MARKER.md

## Diff outline
- Added a single-line marker file.

## Tests added
None — this is a marker file, not code.

## Open questions
None.
EOF

# Emit the canned stream-json on stdout. The engine redirects this
# to agent_output.jsonl and parses it via extractClaudeResponseText
# to populate response.md.
cat <<'STREAMJSON'
` + strings.TrimSpace(fakeClaudeStreamJSON) + `
STREAMJSON

# Signal success on the stage status contract path so the engine
# treats this stage as having succeeded with status="success".
if [ -n "${KILROY_STAGE_STATUS_PATH:-}" ]; then
  echo '{"status":"success"}' > "$KILROY_STAGE_STATUS_PATH"
fi

# Keep the pane alive briefly so tmux session creation settles before
# the binary exits. Without this the engine sees "Pane is dead" and
# treats the stage as transient_infra failure.
sleep 0.5
exit 0
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	return bin
}

// makeE2ERepo creates a tiny git-backed Go module with a passing test.
// The implement workflow's verify stage runs the user's verify_command
// (default `go build ./... && go test ./...`); the test repo must build
// + test cleanly so verify passes and the workflow reaches its summary
// + done stages.
func makeE2ERepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
	}
	run("git", "init", "-q", "-b", "main")
	run("git", "config", "user.email", "e2e@kilroy.test")
	run("git", "config", "user.name", "e2e")
	files := map[string]string{
		"go.mod": "module e2e\n\ngo 1.21\n",
		"hello.go": `package hello

// Hi returns a friendly string.
func Hi() string {
	return "hi"
}
`,
		"hello_test.go": `package hello

import "testing"

func TestHi(t *testing.T) {
	if Hi() != "hi" {
		t.Fatal("Hi() != hi")
	}
}
`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run("git", "add", "-A")
	run("git", "commit", "-q", "-m", "init")
	return repo
}

// TestRunImplement_E2E_FakeProvider is the reference end-to-end test for
// `kilroy run <workflow>`. It exercises the full lifecycle (CLI parse,
// workflow discovery, prelaunch validation, class resolution, tmux
// dispatch, JSONL parsing, response extraction, summary stage,
// per-stage commits, run record) against a fake claude binary that
// stands in for the real LLM. Successful completion plus all the
// expected artifacts and progress events constitute the "this is how
// the system is supposed to behave" baseline.
//
// Block 6's codec rewrite must keep this test green — any intentional
// behavioral change should land alongside a deliberate update here.
func TestRunImplement_E2E_FakeProvider(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not available; CLI-driver dispatch path requires tmux")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available; verify stage runs `go build && go test`")
	}

	bin := buildKilroyBinary(t)
	repo := makeE2ERepo(t)

	// Stage the fake claude on PATH ahead of any real claude install.
	fakePathDir := t.TempDir()
	stageFakeClaude(t, fakePathDir)

	// Inputs: a small prompt the fake claude will pretend to address by
	// creating E2E_MARKER.md.
	inputs := map[string]string{
		"prompt": "Create E2E_MARKER.md with the content 'e2e marker'. Then write {\"status\":\"success\"} to $KILROY_STAGE_STATUS_PATH and exit.",
		// Use a fast verify command — the default `go test ./... -timeout 60s`
		// is fine but takes a few seconds; this test repo's tiny test passes
		// in milliseconds so we leave the default.
	}
	inputsPath := filepath.Join(t.TempDir(), "inputs.json")
	inputsJSON, _ := json.Marshal(inputs)
	if err := os.WriteFile(inputsPath, inputsJSON, 0o644); err != nil {
		t.Fatal(err)
	}

	// Pull KILROY_WORKFLOW_PATHS from the repo's workflows dir (the
	// in-tree workflow packages we're exercising).
	workflowsDir := filepath.Join(findRepoRootForE2E(t), "workflows")

	// State dir isolated so we don't pollute the user's run history.
	stateHome := t.TempDir()

	// Auth config: the v2 resolver requires a user/project auth.toml. We
	// materialize a minimal one in an isolated XDG_CONFIG_HOME with the
	// claude_cli chain configured so the implement workflow's
	// agent_class="hard_coding" resolves to the fake-claude path.
	configHome := t.TempDir()
	authDir := filepath.Join(configHome, "kilroy")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("mkdir auth dir: %v", err)
	}
	authTOML := `
[bindings]
"anthropic/cli_oauth/claude" = "anthropic_claude_cli"

[chains.anthropic_claude_cli]
requires = { provider = "anthropic", method = "cli_oauth", tool = "claude" }

[[chains.anthropic_claude_cli.sources]]
kind = "cli_session"
tool = "claude"
`
	if err := os.WriteFile(filepath.Join(authDir, "auth.toml"), []byte(authTOML), 0o600); err != nil {
		t.Fatalf("write auth.toml: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin,
		"run", "implement",
		"--no-cxdb",
		"--workspace", repo,
		"--input", inputsPath,
		"--label", "task=e2e-fake-provider",
		"--confirm-stale-build",
	)
	// PATH: fake claude first, then keep the real path so tmux/git/go
	// are still findable. XDG_STATE_HOME isolates the run record.
	// XDG_CONFIG_HOME points at our test auth.toml. CLAUDE_TOKEN_OK is
	// a synthetic fake-tool marker; the real auth detector probes for a
	// claude CLI session via keychain/Files. For the test, the binding
	// resolver uses the chain we configured, and since the claude
	// detector returns state=ok on this dev machine (Matt's claude is
	// logged in), the cli_session source resolves cleanly.
	cmd.Env = append(os.Environ(),
		"PATH="+fakePathDir+":"+os.Getenv("PATH"),
		"KILROY_WORKFLOW_PATHS="+workflowsDir,
		"XDG_STATE_HOME="+stateHome,
		"XDG_CONFIG_HOME="+configHome,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kilroy run implement failed: %v\n%s", err, out)
	}

	// kilroy run emits a JSON run handle on stdout (v2 §4 contract).
	// Decode it to find logs_root + run_id. The run output may include
	// CombinedOutput stderr above stdout — locate the JSON line.
	var handle struct {
		RunID    string `json:"run_id"`
		LogsRoot string `json:"logs_root"`
	}
	if err := decodeRunHandleJSON(out, &handle); err != nil {
		t.Fatalf("could not decode run handle JSON: %v\noutput:\n%s", err, out)
	}
	logsRoot := handle.LogsRoot
	runID := handle.RunID
	if logsRoot == "" {
		t.Fatalf("missing logs_root in run handle:\n%s", out)
	}
	if runID == "" {
		t.Fatalf("missing run_id in run handle:\n%s", out)
	}
	t.Logf("e2e run %s at %s", runID, logsRoot)

	// 1. Prelaunch validation report exists and shows agent resolved.
	plPath := filepath.Join(logsRoot, "prelaunch_validation.json")
	plRaw, err := os.ReadFile(plPath)
	if err != nil {
		t.Fatalf("read prelaunch_validation.json: %v\nrun output:\n%s", err, out)
	}
	var pl struct {
		Package *struct {
			Status string `json:"status"`
		} `json:"package"`
		Nodes []struct {
			NodeID         string `json:"node_id"`
			Class          string `json:"class"`
			ResolvedModel  string `json:"resolved_model"`
			ResolvedDriver string `json:"resolved_driver"`
			Status         string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(plRaw, &pl); err != nil {
		t.Fatalf("decode prelaunch_validation.json: %v", err)
	}
	if pl.Package == nil || pl.Package.Status != "ok" {
		t.Errorf("prelaunch package status = %+v, want ok", pl.Package)
	}
	var agentNode *struct {
		NodeID         string `json:"node_id"`
		Class          string `json:"class"`
		ResolvedModel  string `json:"resolved_model"`
		ResolvedDriver string `json:"resolved_driver"`
		Status         string `json:"status"`
	}
	for i := range pl.Nodes {
		if pl.Nodes[i].NodeID == "agent" {
			agentNode = &pl.Nodes[i]
			break
		}
	}
	if agentNode == nil {
		t.Fatalf("prelaunch nodes missing agent: %+v", pl.Nodes)
	}
	if agentNode.Class != "hard_coding" {
		t.Errorf("agent class = %q, want hard_coding", agentNode.Class)
	}
	if agentNode.ResolvedDriver != "claude_cli" {
		t.Errorf("agent resolved_driver = %q, want claude_cli", agentNode.ResolvedDriver)
	}
	if agentNode.Status != "ok" {
		t.Errorf("agent status = %q, want ok", agentNode.Status)
	}

	// 2. progress.ndjson contains policy_class_resolved + provider_selected
	//    + tmux_session_start with the resolved tool=claude.
	progressRaw, err := os.ReadFile(filepath.Join(logsRoot, "progress.ndjson"))
	if err != nil {
		t.Fatalf("read progress.ndjson: %v", err)
	}
	expectEvents := map[string]bool{
		"policy_class_resolved": false,
		"provider_selected":     false,
		"tmux_session_start":    false,
		"run_completed":         false,
	}
	for _, line := range strings.Split(string(progressRaw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if name, _ := ev["event"].(string); expectEvents[name] {
			continue // already recorded
		} else if _, ok := expectEvents[name]; ok {
			expectEvents[name] = true
		}
	}
	for name, seen := range expectEvents {
		if !seen {
			t.Errorf("expected event %q in progress.ndjson, not seen", name)
		}
	}

	// 3. agent stage's resolution.json captures the resolved tuple per §6.4.
	resPath := filepath.Join(logsRoot, "agent", "resolution.json")
	resRaw, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read agent/resolution.json: %v", err)
	}
	var res struct {
		SchemaVersion string `json:"schema_version"`
		NodeID        string `json:"node_id"`
		Resolution    struct {
			Requested struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"requested"`
			Resolved struct {
				ModelID string `json:"model_id"`
				Driver  string `json:"driver"`
			} `json:"resolved"`
		} `json:"resolution"`
	}
	if err := json.Unmarshal(resRaw, &res); err != nil {
		t.Fatalf("decode agent/resolution.json: %v", err)
	}
	if res.NodeID != "agent" {
		t.Errorf("resolution node_id = %q, want agent", res.NodeID)
	}
	if res.Resolution.Requested.Value != "hard_coding" {
		t.Errorf("requested.value = %q, want hard_coding", res.Resolution.Requested.Value)
	}
	if res.Resolution.Resolved.Driver != "claude_cli" {
		t.Errorf("resolved.driver = %q, want claude_cli", res.Resolution.Resolved.Driver)
	}

	// 4. response.md contains the assistant's text from the canned
	//    stream-json (proves the JSONL parser ran).
	respPath := filepath.Join(logsRoot, "agent", "response.md")
	respRaw, err := os.ReadFile(respPath)
	if err != nil {
		// Surface the agent-stage state on failure so we can see what
		// the engine actually produced (jsonl, prompt, status.json).
		entries, _ := os.ReadDir(filepath.Join(logsRoot, "agent"))
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		jsonlPath := filepath.Join(logsRoot, "agent", "agent_output.jsonl")
		jsonlRaw, _ := os.ReadFile(jsonlPath)
		t.Logf("agent dir contents: %v", names)
		t.Logf("agent_output.jsonl (first 1KB):\n%s", string(jsonlRaw[:min(len(jsonlRaw), 1024)]))
		t.Fatalf("read agent/response.md: %v", err)
	}
	if !strings.Contains(string(respRaw), "Created E2E_MARKER.md per the prompt") {
		t.Errorf("response.md missing assistant text from canned stream-json:\n%s", respRaw)
	}

	// 5. The workflow produced result.md the agent wrote.
	resultPath := filepath.Join(logsRoot, "outputs", "result.md")
	resultRaw, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read outputs/result.md: %v", err)
	}
	if !strings.Contains(string(resultRaw), "E2E_MARKER.md") {
		t.Errorf("result.md should mention E2E_MARKER.md:\n%s", resultRaw)
	}

	// 6. final.json says success.
	finalRaw, err := os.ReadFile(filepath.Join(logsRoot, "final.json"))
	if err != nil {
		t.Fatalf("read final.json: %v", err)
	}
	var final struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(finalRaw, &final); err != nil {
		t.Fatalf("decode final.json: %v", err)
	}
	if final.Status != "success" {
		t.Errorf("final.status = %q, want success", final.Status)
	}
}

func extractKVValue(out, prefix string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

// decodeRunHandleJSON scans `out` for the first line that parses as a
// JSON object and decodes it into v. Tolerates stderr noise mixed in
// from CombinedOutput; the run handle is emitted on stdout as one line.
func decodeRunHandleJSON(out []byte, v any) error {
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
			continue
		}
		if err := json.Unmarshal([]byte(line), v); err == nil {
			return nil
		}
	}
	return fmt.Errorf("no JSON object found in output")
}

// findRepoRootForE2E walks up from cwd until it finds a go.mod. Used to
// locate workflows/ for the test, since tests don't know where the
// repo root is at runtime.
func findRepoRootForE2E(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root (no go.mod)")
		}
		dir = parent
	}
}
