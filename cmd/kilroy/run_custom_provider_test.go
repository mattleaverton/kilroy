// Reviewer regression: when cfg.LLM.Providers declares a non-canonical
// provider (kimi/zai/minimax/custom OpenAI-compat endpoint), `kilroy run`
// must route through the dispatcher → codergen → AgentRouter chain
// with an explicit API route. Package tests previously
// missed this because they instantiate engine.RunWithConfig directly,
// bypassing the layered registry the CLI installs. This test exec's the
// real kilroy binary.
package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRun_CustomProvider_DispatcherDelegatesToCodergen(t *testing.T) {
	bin := buildKilroyBinary(t)
	repo := initTestRepo(t)

	// Fake OpenAI-compat chat-completions server. minimal valid response
	// for one assistant turn — enough for the agent loop to terminate
	// after the first reply with no tool calls.
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","model":"minimax-m2.5","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer srv.Close()

	t.Setenv("MINIMAX_API_KEY", "test-key")

	cfgPath := filepath.Join(t.TempDir(), "run.yaml")
	cfg := "version: 1\n" +
		"repo:\n" +
		"  path: " + repo + "\n" +
		"llm:\n" +
		"  providers:\n" +
		"    minimax:\n" +
		"      backend: api\n" +
		"      api:\n" +
		"        protocol: openai_chat_completions\n" +
		"        api_key_env: MINIMAX_API_KEY\n" +
		"        base_url: " + srv.URL + "\n" +
		"        path: /v1/chat/completions\n" +
		"        profile_family: openai\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}

	graphPath := filepath.Join(t.TempDir(), "g.dot")
	graph := `digraph G {
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  failed [shape=Msquare, terminal_status="fail"]
  a [shape=box, llm_provider="minimax", llm_model="minimax-m2.5", agent_mode=one_shot, auto_status=true, prompt="say hi"]
  start -> a
  a -> exit   [condition="outcome=success"]
  a -> failed [condition="outcome!=success"]
}`
	if err := os.WriteFile(graphPath, []byte(graph), 0o644); err != nil {
		t.Fatalf("write graph: %v", err)
	}

	logsRoot := filepath.Join(t.TempDir(), "logs")
	code, out := runKilroy(t, bin,
		"run",
		"--sync",
		"--graph", graphPath,
		"--config", cfgPath,
		"--logs-root", logsRoot,
		"--no-cxdb",
		"--run-id", "custom-provider-regression",
		"--confirm-stale-build",
	)

	// The cardinal regression: dispatcher must not bail with "no
	// dispatch mapping" on a custom-provider node. That message would
	// indicate the route never reached agent_router.
	if strings.Contains(out, "no dispatch mapping") {
		t.Fatalf("dispatcher rejected custom provider before codergen could resolve it (regression!):\n%s", out)
	}

	// agent_router (codergen) must have made HTTP calls to our fake
	// server, proving the resolved OpenAI-compatible API route reached
	// execution rather than being rejected at dispatch.
	mu.Lock()
	gotHits := hits
	mu.Unlock()
	if gotHits == 0 {
		t.Fatalf("fake minimax server received no requests; codergen path not exercised:\n%s", out)
	}

	// Surface the run outcome for diagnosis when downstream behavior
	// regresses, but don't fail on it: the cardinal regression
	// (dispatch rejection) is what this test guards against, and the
	// agent loop's final exit depends on more variables (auto_status,
	// model output shape, etc.) than this test should police.
	t.Logf("run exit=%d (cardinal regression check passed; downstream agent loop outcome may vary)", code)
	t.Logf("output:\n%s", out)
}
