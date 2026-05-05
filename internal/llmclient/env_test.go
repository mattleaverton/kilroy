package llmclient

import "testing"

func clearProviderEnv(t *testing.T) {
	t.Helper()
	t.Setenv("OPENAI_API_KEY_KILROY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY_KILROY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("GEMINI_API_KEY_KILROY", "")
	t.Setenv("GEMINI_API_KEY", "")
	t.Setenv("GOOGLE_API_KEY_KILROY", "")
	t.Setenv("GOOGLE_API_KEY", "")
	t.Setenv("GOOGLE_GENERATIVE_AI_API_KEY", "")
	t.Setenv("CODEX_APP_SERVER_COMMAND", "")
	t.Setenv("CODEX_APP_SERVER_ARGS", "")
	t.Setenv("CODEX_APP_SERVER_COMMAND_ARGS", "")
	t.Setenv("CODEX_APP_SERVER_AUTO_DISCOVER", "")
}

func TestNewFromEnv_ErrorsWhenNoProvidersConfigured(t *testing.T) {
	clearProviderEnv(t)

	_, err := NewFromEnv()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNewFromEnv_RegistersCodexAppServerWhenCommandOverrideIsSet(t *testing.T) {
	clearProviderEnv(t)

	t.Setenv("CODEX_APP_SERVER_COMMAND", "codex")
	c, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}
	names := c.ProviderNames()
	if len(names) != 1 || names[0] != "codex-app-server" {
		t.Fatalf("provider names: got %v want [codex-app-server]", names)
	}
}
