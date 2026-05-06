package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

func TestServerReadsDefaultRunDBAndExposesRunDetail(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	logsRoot := t.TempDir()
	worktreeDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(logsRoot, "run.log"), []byte(`{"ts":"2026-05-06T00:00:00.000Z","source":"engine","event":"run.started","msg":"started"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write run.log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logsRoot, "prelaunch_validation.json"), []byte(`{"nodes":{"agent":{"class":"hard_coding","provider":"anthropic"}}}`), 0o644); err != nil {
		t.Fatalf("write prelaunch: %v", err)
	}
	if err := os.Mkdir(filepath.Join(logsRoot, "outputs"), 0o755); err != nil {
		t.Fatalf("mkdir outputs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logsRoot, "outputs.json"), []byte(`[{"name":"result.md","path":"result.md","size_bytes":7}]`), 0o644); err != nil {
		t.Fatalf("write outputs.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logsRoot, "outputs", "result.md"), []byte("done\n"), 0o644); err != nil {
		t.Fatalf("write output: %v", err)
	}
	if err := os.WriteFile(filepath.Join(worktreeDir, "README.md"), []byte("workspace\n"), 0o644); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	defer db.Close()

	started := time.Date(2026, 5, 6, 1, 2, 3, 0, time.UTC)
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       "01TESTPARENT",
		GraphName:   "parent-flow",
		Status:      "running",
		LogsRoot:    logsRoot,
		WorktreeDir: worktreeDir,
		RepoPath:    "/tmp/repo",
		RunBranch:   "attractor/run/01TESTPARENT",
		StartedAt:   started,
		DotSource:   "digraph { start -> agent }",
		Inputs:      map[string]any{"prompt": "inspect"},
		Labels:      map[string]string{"scope": "ui"},
		Invocation:  []string{"kilroy", "run", "investigate"},
		Config:      map[string]any{"llm": map[string]any{"cli_profile": "local"}},
	}); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	nodeID, err := db.InsertNodeStart("01TESTPARENT", "agent", 1, "agent")
	if err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if err := db.CompleteNode(nodeID, "success", "", "", "", "ok", map[string]any{"summary": "done"}); err != nil {
		t.Fatalf("complete node: %v", err)
	}
	if err := db.InsertEdgeDecision("01TESTPARENT", "start", "agent", "", "outcome=success", "condition_match"); err != nil {
		t.Fatalf("insert edge: %v", err)
	}
	if err := db.InsertProviderSelection("01TESTPARENT", "agent", 1, "anthropic", "claude-opus-4-7", "claude_cli"); err != nil {
		t.Fatalf("insert provider: %v", err)
	}
	if err := db.RecordNodeDiff("01TESTPARENT", "agent", 1, "abc", "def", 1, 2, 0); err != nil {
		t.Fatalf("insert diff: %v", err)
	}
	if err := db.InsertNodeArtifact(nodeID, rundb.NodeArtifact{
		Name:        "response.md",
		ContentType: "text/markdown; charset=utf-8",
		Content:     []byte("hello from artifact"),
	}); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       "01TESTCHILD",
		GraphName:   "child-flow",
		Status:      "success",
		LogsRoot:    t.TempDir(),
		StartedAt:   started.Add(time.Minute),
		ParentRunID: "01TESTPARENT",
	}); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	ts := httptest.NewServer(newHandler())
	defer ts.Close()

	getJSON := func(path string) map[string]any {
		t.Helper()
		res, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status=%d", path, res.StatusCode)
		}
		var out map[string]any
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		return out
	}

	health := getJSON("/api/health")
	if health["status"] != "ok" {
		t.Fatalf("health status=%v", health["status"])
	}
	list := getJSON("/api/runs")
	if got := int(list["count"].(float64)); got != 2 {
		t.Fatalf("run count=%d, want 2", got)
	}
	detail := getJSON("/api/runs/01TESTPARENT")
	for _, key := range []string{"nodes", "edges", "providers", "provider_summary", "diffs", "children", "inputs", "labels", "invocation", "config", "dot_source"} {
		if _, ok := detail[key]; !ok {
			t.Fatalf("detail missing %s: %#v", key, detail)
		}
	}
	if detail["run_id"] != "01TESTPARENT" {
		t.Fatalf("run_id=%v", detail["run_id"])
	}
	turns := getJSON("/api/runs/01TESTPARENT/nodes/agent/turns?attempt=1")
	raw, _ := json.Marshal(turns)
	if !strings.Contains(string(raw), "hello from artifact") {
		t.Fatalf("turns missing artifact content: %s", string(raw))
	}
	log := getJSON("/api/runs/01TESTPARENT/log")
	if got := int(log["count"].(float64)); got != 1 {
		t.Fatalf("log count=%d, want 1", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/runs/01TESTPARENT/events", nil)
	if err != nil {
		t.Fatalf("new event request: %v", err)
	}
	eventRes, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET events: %v", err)
	}
	line, err := bufio.NewReader(eventRes.Body).ReadString('\n')
	_ = eventRes.Body.Close()
	cancel()
	if err != nil {
		t.Fatalf("read SSE line: %v", err)
	}
	if !strings.HasPrefix(line, "data: ") || !strings.Contains(line, "run.started") {
		t.Fatalf("unexpected SSE line: %q", line)
	}
	policy := getJSON("/api/policy")
	if _, ok := policy["classes"]; !ok {
		t.Fatalf("policy missing classes: %#v", policy)
	}
	explain := getJSON("/api/policy/explain/01TESTPARENT")
	if _, ok := explain["prelaunch"]; !ok {
		t.Fatalf("policy explain missing prelaunch: %#v", explain)
	}
	auth := getJSON("/api/auth")
	if _, ok := auth["summary"]; !ok {
		t.Fatalf("auth missing summary: %#v", auth)
	}
	outputRes, err := http.Get(ts.URL + "/api/runs/01TESTPARENT/outputs/result.md")
	if err != nil {
		t.Fatalf("get output: %v", err)
	}
	defer outputRes.Body.Close()
	if outputRes.StatusCode != http.StatusOK {
		t.Fatalf("output status=%d", outputRes.StatusCode)
	}
	postRes, err := http.Post(ts.URL+"/api/runs", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST /api/runs: %v", err)
	}
	defer postRes.Body.Close()
	if postRes.StatusCode != http.StatusMethodNotAllowed && postRes.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /api/runs status=%d, want 404 or 405", postRes.StatusCode)
	}
}

func TestStaticUIIsReadOnlyAndSurfacesRoutingAuth(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "web", "index.html"))
	if err != nil {
		t.Fatalf("read UI: %v", err)
	}
	html := string(data)
	for _, stale := range []string{"Start Run", "/workflows", "cancelRun", "tmux: true", "showLaunchForm"} {
		if strings.Contains(html, stale) {
			t.Fatalf("UI still contains stale launch/cancel surface %q", stale)
		}
	}
	for _, want := range []string{"Routing", "Auth", "getPolicyExplain", "getAuth"} {
		if !strings.Contains(html, want) {
			t.Fatalf("UI missing %q", want)
		}
	}
}

func TestServerStartupDoesNotMutateRunDB(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	defer db.Close()

	if err := db.InsertRun(rundb.RunRecord{
		RunID:     "01TESTSTALE",
		GraphName: "stale-flow",
		Status:    "running",
		StartedAt: time.Now().Add(-3 * time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("insert stale run: %v", err)
	}

	srv := New(Config{Addr: "127.0.0.1:0"})
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("shutdown server: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not shut down")
	}

	run, err := db.GetRun("01TESTSTALE")
	if err != nil {
		t.Fatalf("get stale run: %v", err)
	}
	if run == nil {
		t.Fatalf("stale run disappeared")
	}
	if run.Status != "running" {
		t.Fatalf("server startup mutated stale run status to %q", run.Status)
	}
}
