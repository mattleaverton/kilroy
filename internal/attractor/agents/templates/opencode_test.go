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

func TestBuildOpencodeConfig_Anthropic_NativeMinimalShape(t *testing.T) {
	// Anthropic is a native opencode provider — kilroy emits the minimal
	// config (options only) and lets opencode's own registry fill in the
	// rest. No npm / models / name in the block.
	cfg := buildOpencodeConfig("anthropic", "claude-sonnet-4-6")
	var got map[string]any
	if err := json.Unmarshal([]byte(cfg), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, cfg)
	}
	prov := got["provider"].(map[string]any)
	block, ok := prov["anthropic"].(map[string]any)
	if !ok {
		t.Fatalf("expected provider.anthropic block; got %s", cfg)
	}
	opts := block["options"].(map[string]any)
	if opts["apiKey"] != "{env:ANTHROPIC_API_KEY}" {
		t.Errorf("apiKey: got %v want {env:ANTHROPIC_API_KEY}", opts["apiKey"])
	}
	if _, hasNPM := block["npm"]; hasNPM {
		t.Errorf("native anthropic block should not declare npm; got %s", cfg)
	}
}

func TestBuildOpencodeConfig_Kimi_FullCustomDeclaration(t *testing.T) {
	// Kimi is anthropic_messages-protocol but not a native opencode
	// provider — kilroy must emit the FULL custom-provider declaration
	// (npm + name + options + models). Without this, opencode rejects
	// the launch with ProviderModelNotFoundError.
	cfg := buildOpencodeConfig("kimi", "kimi-k2")
	var got map[string]any
	if err := json.Unmarshal([]byte(cfg), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, cfg)
	}
	prov := got["provider"].(map[string]any)
	block, ok := prov["kimi"].(map[string]any)
	if !ok {
		t.Fatalf("expected provider.kimi block; got %s", cfg)
	}
	if block["npm"] != "@ai-sdk/anthropic" {
		t.Errorf("npm: got %v want @ai-sdk/anthropic (kimi speaks anthropic_messages)", block["npm"])
	}
	opts := block["options"].(map[string]any)
	if opts["apiKey"] != "{env:KIMI_API_KEY}" {
		t.Errorf("apiKey: got %v", opts["apiKey"])
	}
	// baseURL must end at /v1 because @ai-sdk/anthropic appends /messages itself.
	wantBase := "https://api.kimi.com/coding/v1"
	if opts["baseURL"] != wantBase {
		t.Errorf("baseURL: got %v want %s (must include /v1 suffix)", opts["baseURL"], wantBase)
	}
	models, ok := block["models"].(map[string]any)
	if !ok {
		t.Fatalf("expected models map for custom provider; got %s", cfg)
	}
	if _, ok := models["kimi-k2"]; !ok {
		t.Errorf("models map should declare kimi-k2; got %s", cfg)
	}
}

func TestBuildOpencodeConfig_Zai_OpenAICompatibleNPM(t *testing.T) {
	// Z.ai speaks openai_chat_completions — npm package differs.
	cfg := buildOpencodeConfig("zai", "glm-4.6")
	var got map[string]any
	if err := json.Unmarshal([]byte(cfg), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, cfg)
	}
	block := got["provider"].(map[string]any)["zai"].(map[string]any)
	if block["npm"] != "@ai-sdk/openai-compatible" {
		t.Errorf("npm: got %v want @ai-sdk/openai-compatible", block["npm"])
	}
	opts := block["options"].(map[string]any)
	if opts["apiKey"] != "{env:ZAI_API_KEY}" {
		t.Errorf("apiKey: got %v", opts["apiKey"])
	}
}

// Unknown provider falls back to a canonical {env:<UPPER>_API_KEY}
// pattern. Catches typos at agent-launch time rather than silently
// launching against anthropic. No npm/baseURL emitted (we have no spec
// to look up).
func TestBuildOpencodeConfig_UnknownProvider_UsesCanonicalEnvName(t *testing.T) {
	cfg := buildOpencodeConfig("madeup", "")
	if !strings.Contains(cfg, `"madeup"`) {
		t.Fatalf("provider block should preserve the input name; got %s", cfg)
	}
	if !strings.Contains(cfg, "{env:MADEUP_API_KEY}") {
		t.Errorf("unknown provider should use UPPER_API_KEY pattern; got %s", cfg)
	}
}

// PrepareSession reads KILROY_AGENT_PROVIDER + KILROY_AGENT_MODEL from
// the env map and writes the matching OPENCODE_CONFIG_CONTENT. The
// model is needed because custom providers (kimi, zai, etc.) require a
// `models` block declaring exactly which model is being launched.
func TestOpencodePrepareSession_HonorsKilroyAgentProviderAndModel(t *testing.T) {
	tmpl := OpenCode()
	stage := t.TempDir()

	envKimi := map[string]string{
		"KILROY_AGENT_PROVIDER": "kimi",
		"KILROY_AGENT_MODEL":    "kimi-k2",
	}
	if err := tmpl.PrepareSession(stage, envKimi); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	cfg := envKimi["OPENCODE_CONFIG_CONTENT"]
	if !strings.Contains(cfg, `"kimi"`) {
		t.Errorf("expected kimi block; got %s", cfg)
	}
	if !strings.Contains(cfg, `"kimi-k2"`) {
		t.Errorf("expected kimi-k2 in models block; got %s", cfg)
	}
	if !strings.Contains(cfg, "@ai-sdk/anthropic") {
		t.Errorf("expected @ai-sdk/anthropic npm package for kimi; got %s", cfg)
	}

	envEmpty := map[string]string{}
	if err := tmpl.PrepareSession(stage, envEmpty); err != nil {
		t.Fatalf("PrepareSession default: %v", err)
	}
	if !strings.Contains(envEmpty["OPENCODE_CONFIG_CONTENT"], "anthropic") {
		t.Errorf("empty env should default to anthropic config; got %s", envEmpty["OPENCODE_CONFIG_CONTENT"])
	}
}

// BuildArgs uses the provider parameter as the model prefix when the
// model arg lacks a provider/ prefix, so opencode receives the right
// "kimi/kimi-k2" or "anthropic/claude-..." form regardless of what the
// node author wrote. tmux_handler passes route.Provider as the 5th arg.
func TestOpencodeBuildArgs_UsesProviderArg(t *testing.T) {
	tmpl := OpenCode()
	args := tmpl.BuildArgs("hi", "/tmp/wt", "kimi-k2", "", "kimi")
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

// BuildEnv honors the kilroy budget-isolation convention: _KILROY-suffixed
// env vars are preferred over canonical ones, but the exported map key is
// always the canonical name (opencode's config block references canonical
// names via {env:NAME}).
func TestOpencodeBuildEnv_PrefersKilroySuffixedKeys(t *testing.T) {
	tmpl := OpenCode()

	// Both set: _KILROY value wins, exported under canonical name.
	t.Setenv("KIMI_API_KEY", "canonical-value")
	t.Setenv("KIMI_API_KEY_KILROY", "kilroy-value")
	env := tmpl.BuildEnv()
	if got := env["KIMI_API_KEY"]; got != "kilroy-value" {
		t.Errorf("KIMI_API_KEY: got %q want %q (should prefer _KILROY)", got, "kilroy-value")
	}
	if _, leaked := env["KIMI_API_KEY_KILROY"]; leaked {
		t.Errorf("env should expose canonical name only, not _KILROY suffix")
	}
}

func TestOpencodeBuildEnv_FallsBackToCanonicalWhenKilroyAbsent(t *testing.T) {
	tmpl := OpenCode()
	t.Setenv("KIMI_API_KEY", "canonical-value")
	t.Setenv("KIMI_API_KEY_KILROY", "")
	env := tmpl.BuildEnv()
	if got := env["KIMI_API_KEY"]; got != "canonical-value" {
		t.Errorf("KIMI_API_KEY: got %q want canonical-value (no _KILROY set)", got)
	}
}

// Without an explicit provider arg, BuildArgs defaults to "anthropic/" —
// preserves back-compat for fixtures and direct-use callers.
func TestOpencodeBuildArgs_DefaultsToAnthropicWhenNoProvider(t *testing.T) {
	tmpl := OpenCode()
	args := tmpl.BuildArgs("hi", "/tmp/wt", "claude-sonnet-4-5", "", "")
	for i, a := range args {
		if a == "--model" && i+1 < len(args) {
			if args[i+1] != "anthropic/claude-sonnet-4-5" {
				t.Errorf("model arg: got %q want anthropic/claude-sonnet-4-5", args[i+1])
			}
		}
	}
}
