// Tests for opencode template's provider-aware OPENCODE_CONFIG_CONTENT
// generation. tmux_handler stashes route.Provider in
// KILROY_AGENT_PROVIDER before PrepareSession runs; the template reads
// it to emit the right provider block. Without this, opencode runs
// against (e.g.) kimi would silently launch with anthropic config.
package templates

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildOpencodeConfig_Anthropic(t *testing.T) {
	cfg := buildOpencodeConfig("anthropic")
	var got map[string]any
	if err := json.Unmarshal([]byte(cfg), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, cfg)
	}
	prov := got["provider"].(map[string]any)
	if _, ok := prov["anthropic"]; !ok {
		t.Fatalf("expected provider.anthropic block; got %s", cfg)
	}
	opts := prov["anthropic"].(map[string]any)["options"].(map[string]any)
	if opts["apiKey"] != "{env:ANTHROPIC_API_KEY}" {
		t.Errorf("apiKey: got %v want {env:ANTHROPIC_API_KEY}", opts["apiKey"])
	}
}

func TestBuildOpencodeConfig_Kimi(t *testing.T) {
	cfg := buildOpencodeConfig("kimi")
	if !strings.Contains(cfg, `"kimi"`) {
		t.Fatalf("expected kimi block in config; got %s", cfg)
	}
	if !strings.Contains(cfg, "{env:KIMI_API_KEY}") {
		t.Errorf("expected KIMI_API_KEY env reference; got %s", cfg)
	}
	if !strings.Contains(cfg, "api.kimi.com/coding") {
		t.Errorf("expected kimi base URL; got %s", cfg)
	}
}

func TestBuildOpencodeConfig_Zai(t *testing.T) {
	cfg := buildOpencodeConfig("zai")
	if !strings.Contains(cfg, `"zai"`) {
		t.Fatalf("expected zai block; got %s", cfg)
	}
	if !strings.Contains(cfg, "{env:ZAI_API_KEY}") {
		t.Errorf("expected ZAI_API_KEY env reference; got %s", cfg)
	}
}

// Unknown provider falls back to a canonical {env:<UPPER>_API_KEY}
// pattern. Catches typos at agent-launch time rather than silently
// launching against anthropic.
func TestBuildOpencodeConfig_UnknownProvider_UsesCanonicalEnvName(t *testing.T) {
	cfg := buildOpencodeConfig("madeup")
	if !strings.Contains(cfg, `"madeup"`) {
		t.Fatalf("provider block should preserve the input name; got %s", cfg)
	}
	if !strings.Contains(cfg, "{env:MADEUP_API_KEY}") {
		t.Errorf("unknown provider should use UPPER_API_KEY pattern; got %s", cfg)
	}
}

// PrepareSession reads KILROY_AGENT_PROVIDER from env and writes the
// matching OPENCODE_CONFIG_CONTENT. Empty env defaults to anthropic
// for back-compat.
func TestOpencodePrepareSession_HonorsKilroyAgentProvider(t *testing.T) {
	tmpl := OpenCode()
	stage := t.TempDir()

	envKimi := map[string]string{"KILROY_AGENT_PROVIDER": "kimi"}
	if err := tmpl.PrepareSession(stage, envKimi); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	cfg := envKimi["OPENCODE_CONFIG_CONTENT"]
	if !strings.Contains(cfg, "kimi") {
		t.Errorf("expected kimi config when KILROY_AGENT_PROVIDER=kimi; got %s", cfg)
	}
	if strings.Contains(cfg, "anthropic") {
		t.Errorf("kimi config should not mention anthropic; got %s", cfg)
	}

	envEmpty := map[string]string{}
	if err := tmpl.PrepareSession(stage, envEmpty); err != nil {
		t.Fatalf("PrepareSession default: %v", err)
	}
	if !strings.Contains(envEmpty["OPENCODE_CONFIG_CONTENT"], "anthropic") {
		t.Errorf("empty env should default to anthropic config; got %s", envEmpty["OPENCODE_CONFIG_CONTENT"])
	}
}

// BuildArgs uses KILROY_AGENT_PROVIDER as the model prefix when the
// model arg lacks a provider/ prefix, so opencode receives the right
// "kimi/kimi-k2" or "anthropic/claude-..." form regardless of what the
// node author wrote.
func TestOpencodeBuildArgs_UsesProviderPrefixFromEnv(t *testing.T) {
	tmpl := OpenCode()
	t.Setenv("KILROY_AGENT_PROVIDER", "kimi")
	args := tmpl.BuildArgs("hi", "/tmp/wt", "kimi-k2", "")
	found := false
	for i, a := range args {
		if a == "--model" && i+1 < len(args) {
			if args[i+1] != "kimi/kimi-k2" {
				t.Errorf("model arg: got %q want kimi/kimi-k2", args[i+1])
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("expected --model arg in %v", args)
	}
}

// Without KILROY_AGENT_PROVIDER, BuildArgs defaults to "anthropic/" —
// preserves back-compat for fixtures and direct-use callers that don't
// set the env key.
func TestOpencodeBuildArgs_DefaultsToAnthropicWhenNoProviderEnv(t *testing.T) {
	tmpl := OpenCode()
	t.Setenv("KILROY_AGENT_PROVIDER", "")
	args := tmpl.BuildArgs("hi", "/tmp/wt", "claude-sonnet-4-5", "")
	for i, a := range args {
		if a == "--model" && i+1 < len(args) {
			if args[i+1] != "anthropic/claude-sonnet-4-5" {
				t.Errorf("model arg: got %q want anthropic/claude-sonnet-4-5", args[i+1])
			}
		}
	}
}
