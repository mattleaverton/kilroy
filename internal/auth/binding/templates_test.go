// Tests for the template embed loader (A2). Verifies that the embedded
// default_chains.toml round-trips through LoadDefaultTemplates cleanly and
// that all six required bindings and chains are present with correct shape.

package binding

import (
	"testing"
)

// expectedBindingKeys are the six canonical (provider/method[/tool]) binding
// keys that the default_chains.toml must declare.
var expectedBindingKeys = []string{
	"anthropic/api_key",
	"anthropic/cli_oauth/claude",
	"openai/api_key",
	"openai/cli_oauth/codex",
	"google/api_key",
	"google/cli_oauth/gemini",
}

// expectedChainNames are the six chain names that the default_chains.toml
// must declare.
var expectedChainNames = []string{
	"anthropic_api_key",
	"anthropic_claude_cli",
	"openai_api_key",
	"openai_codex_cli",
	"google_api_key",
	"google_gemini_cli",
}

func TestDefaultChainsTOML_NonEmpty(t *testing.T) {
	b := DefaultChainsTOML()
	if len(b) == 0 {
		t.Fatal("DefaultChainsTOML() returned empty bytes")
	}
}

func TestLoadDefaultTemplates_ParsesCleanly(t *testing.T) {
	_, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
}

func TestLoadDefaultTemplates_AllSixBindings(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	for _, key := range expectedBindingKeys {
		if _, ok := cfg.Bindings[key]; !ok {
			t.Errorf("missing binding key %q", key)
		}
	}
	if got := len(cfg.Bindings); got != len(expectedBindingKeys) {
		t.Errorf("bindings count = %d, want %d", got, len(expectedBindingKeys))
	}
}

func TestLoadDefaultTemplates_AllSixChains(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	for _, name := range expectedChainNames {
		if _, ok := cfg.Chains[name]; !ok {
			t.Errorf("missing chain %q", name)
		}
	}
	if got := len(cfg.Chains); got != len(expectedChainNames) {
		t.Errorf("chains count = %d, want %d", got, len(expectedChainNames))
	}
}

func TestLoadDefaultTemplates_ChainNamePopulated(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	for name, chain := range cfg.Chains {
		if chain.Name == "" {
			t.Errorf("chain %q has empty Name field (not populated from map key)", name)
		}
		if chain.Name != name {
			t.Errorf("chain %q has Name = %q, want key name", name, chain.Name)
		}
	}
}

func TestLoadDefaultTemplates_AnthropicAPIKey_SourceOrder(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	chain, ok := cfg.Chains["anthropic_api_key"]
	if !ok {
		t.Fatal("chain anthropic_api_key not found")
	}
	if len(chain.Sources) < 2 {
		t.Fatalf("anthropic_api_key sources len = %d, want >= 2", len(chain.Sources))
	}
	if chain.Sources[0].Name != "ANTHROPIC_API_KEY_KILROY" {
		t.Errorf("sources[0] = %q, want ANTHROPIC_API_KEY_KILROY", chain.Sources[0].Name)
	}
	if chain.Sources[1].Name != "ANTHROPIC_API_KEY" {
		t.Errorf("sources[1] = %q, want ANTHROPIC_API_KEY", chain.Sources[1].Name)
	}
}

func TestLoadDefaultTemplates_GoogleAPIKey_AllNames(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	chain, ok := cfg.Chains["google_api_key"]
	if !ok {
		t.Fatal("chain google_api_key not found")
	}

	// Collect all source names for easier lookup.
	nameAt := make(map[string]int, len(chain.Sources))
	for i, src := range chain.Sources {
		nameAt[src.Name] = i
	}

	required := []string{
		"GOOGLE_API_KEY_KILROY",
		"GEMINI_API_KEY_KILROY",
		"GOOGLE_API_KEY",
		"GEMINI_API_KEY",
		"GOOGLE_GENERATIVE_AI_API_KEY",
	}
	for _, name := range required {
		if _, ok := nameAt[name]; !ok {
			t.Errorf("google_api_key missing source %q", name)
		}
	}

	// _KILROY variants must appear before their plain counterparts.
	kilroyGoogle, hasKG := nameAt["GOOGLE_API_KEY_KILROY"]
	plainGoogle, hasPG := nameAt["GOOGLE_API_KEY"]
	if hasKG && hasPG && kilroyGoogle >= plainGoogle {
		t.Errorf("GOOGLE_API_KEY_KILROY (rank %d) must come before GOOGLE_API_KEY (rank %d)", kilroyGoogle, plainGoogle)
	}

	kilroyGemini, hasKGem := nameAt["GEMINI_API_KEY_KILROY"]
	plainGemini, hasPGem := nameAt["GEMINI_API_KEY"]
	if hasKGem && hasPGem && kilroyGemini >= plainGemini {
		t.Errorf("GEMINI_API_KEY_KILROY (rank %d) must come before GEMINI_API_KEY (rank %d)", kilroyGemini, plainGemini)
	}
}

func TestLoadDefaultTemplates_CLIChains_HaveCLISession(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	cliChains := map[string]string{
		"anthropic_claude_cli": "claude",
		"openai_codex_cli":     "codex",
		"google_gemini_cli":    "gemini",
	}
	for chainName, expectedTool := range cliChains {
		chain, ok := cfg.Chains[chainName]
		if !ok {
			t.Errorf("chain %q not found", chainName)
			continue
		}
		if len(chain.Sources) != 1 {
			t.Errorf("%s sources len = %d, want 1", chainName, len(chain.Sources))
			continue
		}
		src := chain.Sources[0]
		if src.Kind != SourceCLISession {
			t.Errorf("%s source kind = %q, want cli_session", chainName, src.Kind)
		}
		if src.Tool != expectedTool {
			t.Errorf("%s source tool = %q, want %q", chainName, src.Tool, expectedTool)
		}
	}
}

func TestLoadDefaultTemplates_BindingKeys_MatchRequirementKey(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates() error: %v", err)
	}
	// Each binding key must be the canonical Requirement.Key() form.
	// Verify by constructing the Requirement from the chain's requires field
	// and checking Key() matches the binding.
	for key, chainName := range cfg.Bindings {
		chain, ok := cfg.Chains[chainName]
		if !ok {
			t.Errorf("binding %q → chain %q not found", key, chainName)
			continue
		}
		if got := chain.Requires.Key(); got != key {
			t.Errorf("binding key %q != chain.Requires.Key() %q for chain %q", key, got, chainName)
		}
	}
}
