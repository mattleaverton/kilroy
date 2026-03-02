package engine

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/dot"
)

func TestUsedAPIProviders_OmittedFailoverDoesNotIncludeImplicitTargets(t *testing.T) {
	g, err := dot.Parse([]byte(`digraph G {
  start [shape=Mdiamond]
  a [shape=box, llm_provider="openai", llm_model="gpt-5.2", prompt="x"]
  exit [shape=Msquare]
  start -> a -> exit
}`))
	if err != nil {
		t.Fatalf("parse dot: %v", err)
	}

	cfg := &RunConfigFile{}
	t.Setenv("TEST_GOOGLE_API_KEY", "x")
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendAPI},
		"google": {
			Backend: BackendAPI,
			API: ProviderAPIConfig{
				APIKeyEnv: "TEST_GOOGLE_API_KEY",
			},
		},
	}

	runtimes, err := resolveProviderRuntimes(cfg)
	if err != nil {
		t.Fatalf("resolveProviderRuntimes: %v", err)
	}
	got := usedAPIProviders(g, runtimes)
	if strings.Join(got, ",") != "openai" {
		t.Fatalf("providers=%v want [openai]", got)
	}
}
