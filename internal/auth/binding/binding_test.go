// Tests for the auth binding resolver. Cover every path in the algorithm:
// binding hit, single-chain match, no chain, ambiguous, exhausted chain,
// source vanished at bind time. The fakeView lets tests control detection
// state precisely without touching real env or auth detectors.

package binding

import (
	"errors"
	"os"
	"testing"
)

// fakeView is a DetectionView for tests that lets us declare exactly which
// sources are present.
type fakeView struct {
	envs map[string]bool
	clis map[string]bool
}

func (f fakeView) EnvVarPresent(name string) bool { return f.envs[name] }
func (f fakeView) CLISessionOK(tool string) bool  { return f.clis[tool] }

func newFakeView() *fakeView {
	return &fakeView{envs: map[string]bool{}, clis: map[string]bool{}}
}

func TestResolve_BindingHit_PicksNamedChain(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{
			"anthropic/api_key": "anthropic_kilroy_api",
		},
		Chains: map[string]Chain{
			"anthropic_kilroy_api": {
				Name:     "anthropic_kilroy_api",
				Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey},
				Sources: []Source{
					{Kind: SourceEnvVar, Name: "ANTHROPIC_API_KEY_KILROY"},
					{Kind: SourceEnvVar, Name: "ANTHROPIC_API_KEY"},
				},
			},
			// A second chain that also satisfies the requirement, to prove
			// the binding wins over an ambiguous match.
			"anthropic_other": {
				Name:     "anthropic_other",
				Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey},
				Sources:  []Source{{Kind: SourceEnvVar, Name: "ANTHROPIC_API_KEY_OTHER"}},
			},
		},
	}
	view := newFakeView()
	view.envs["ANTHROPIC_API_KEY_KILROY"] = true

	r := NewResolver(cfg, view)
	snap, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.ChainName != "anthropic_kilroy_api" {
		t.Errorf("chain = %q, want anthropic_kilroy_api", snap.ChainName)
	}
	if snap.Source.Name != "ANTHROPIC_API_KEY_KILROY" {
		t.Errorf("source = %q, want ANTHROPIC_API_KEY_KILROY", snap.Source.Name)
	}
	if snap.FallbackRank != 0 {
		t.Errorf("fallback_rank = %d, want 0", snap.FallbackRank)
	}
}

func TestResolve_NoBinding_SingleMatch_Picks(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{},
		Chains: map[string]Chain{
			"only": {
				Name:     "only",
				Requires: Requirement{Provider: "openai", Method: MethodAPIKey},
				Sources:  []Source{{Kind: SourceEnvVar, Name: "OPENAI_API_KEY"}},
			},
		},
	}
	view := newFakeView()
	view.envs["OPENAI_API_KEY"] = true

	r := NewResolver(cfg, view)
	snap, err := r.Resolve(Requirement{Provider: "openai", Method: MethodAPIKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.ChainName != "only" {
		t.Errorf("chain = %q, want only", snap.ChainName)
	}
}

func TestResolve_NoBinding_NoMatch_Errors(t *testing.T) {
	cfg := &Config{Chains: map[string]Chain{}}
	r := NewResolver(cfg, newFakeView())
	_, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	var noChain *ErrNoChainForRequirement
	if !errors.As(err, &noChain) {
		t.Fatalf("err = %v, want *ErrNoChainForRequirement", err)
	}
	if noChain.Requirement.Provider != "anthropic" {
		t.Errorf("requirement.provider = %q", noChain.Requirement.Provider)
	}
}

func TestResolve_NoBinding_Ambiguous_Errors(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{},
		Chains: map[string]Chain{
			"a_one": {Name: "a_one", Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey}, Sources: []Source{{Kind: SourceEnvVar, Name: "X"}}},
			"a_two": {Name: "a_two", Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey}, Sources: []Source{{Kind: SourceEnvVar, Name: "Y"}}},
		},
	}
	r := NewResolver(cfg, newFakeView())
	_, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	var amb *ErrAmbiguousAuthChain
	if !errors.As(err, &amb) {
		t.Fatalf("err = %v, want *ErrAmbiguousAuthChain", err)
	}
	if len(amb.Candidates) != 2 || amb.Candidates[0] != "a_one" || amb.Candidates[1] != "a_two" {
		t.Errorf("candidates = %v, want [a_one a_two]", amb.Candidates)
	}
}

func TestResolve_BindingPointsToUnknownChain_Errors(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{"anthropic/api_key": "missing"},
		Chains:   map[string]Chain{},
	}
	r := NewResolver(cfg, newFakeView())
	_, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	var unk *ErrUnknownChain
	if !errors.As(err, &unk) {
		t.Fatalf("err = %v, want *ErrUnknownChain", err)
	}
	if unk.ChainName != "missing" {
		t.Errorf("chain name = %q, want missing", unk.ChainName)
	}
}

func TestResolve_FallbackWithinChain_PicksFirstUsable(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{},
		Chains: map[string]Chain{
			"only": {
				Name:     "only",
				Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey},
				Sources: []Source{
					{Kind: SourceEnvVar, Name: "FIRST"},
					{Kind: SourceEnvVar, Name: "SECOND"},
					{Kind: SourceEnvVar, Name: "THIRD"},
				},
			},
		},
	}
	view := newFakeView()
	view.envs["THIRD"] = true // only the third is set

	r := NewResolver(cfg, view)
	snap, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.Source.Name != "THIRD" {
		t.Errorf("source = %q, want THIRD", snap.Source.Name)
	}
	if snap.FallbackRank != 2 {
		t.Errorf("fallback_rank = %d, want 2", snap.FallbackRank)
	}
	if len(snap.Skipped) != 2 {
		t.Errorf("skipped len = %d, want 2", len(snap.Skipped))
	}
	if snap.Skipped[0].Reason != "env_var_missing:FIRST" {
		t.Errorf("skipped[0].reason = %q", snap.Skipped[0].Reason)
	}
}

func TestResolve_ChainExhausted_Errors(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{},
		Chains: map[string]Chain{
			"only": {
				Name:     "only",
				Requires: Requirement{Provider: "anthropic", Method: MethodAPIKey},
				Sources:  []Source{{Kind: SourceEnvVar, Name: "ABSENT_A"}, {Kind: SourceEnvVar, Name: "ABSENT_B"}},
			},
		},
	}
	r := NewResolver(cfg, newFakeView())
	_, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodAPIKey})
	var ex *ErrChainExhausted
	if !errors.As(err, &ex) {
		t.Fatalf("err = %v, want *ErrChainExhausted", err)
	}
	if ex.ChainName != "only" {
		t.Errorf("chain name = %q", ex.ChainName)
	}
	if len(ex.Skipped) != 2 {
		t.Errorf("skipped len = %d, want 2", len(ex.Skipped))
	}
}

func TestResolve_CLISession_Source(t *testing.T) {
	cfg := &Config{
		Bindings: map[string]string{},
		Chains: map[string]Chain{
			"claude_cli": {
				Name:     "claude_cli",
				Requires: Requirement{Provider: "anthropic", Method: MethodCLIOAuth, Tool: "claude"},
				Sources:  []Source{{Kind: SourceCLISession, Tool: "claude"}},
			},
		},
	}
	view := newFakeView()
	view.clis["claude"] = true

	r := NewResolver(cfg, view)
	snap, err := r.Resolve(Requirement{Provider: "anthropic", Method: MethodCLIOAuth, Tool: "claude"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.Source.Kind != SourceCLISession {
		t.Errorf("kind = %q", snap.Source.Kind)
	}
	if snap.Source.Tool != "claude" {
		t.Errorf("tool = %q", snap.Source.Tool)
	}
}

func TestBind_EnvVar_Present(t *testing.T) {
	t.Setenv("KILROY_TEST_BIND", "test-value")
	r := NewResolver(&Config{}, newFakeView())
	snap := Snapshot{Source: Source{Kind: SourceEnvVar, Name: "KILROY_TEST_BIND"}}
	cred, err := r.Bind(snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.Value != "test-value" {
		t.Errorf("value = %q, want test-value", cred.Value)
	}
}

func TestBind_EnvVar_Vanished(t *testing.T) {
	const name = "KILROY_TEST_BIND_VANISH"
	os.Unsetenv(name)
	r := NewResolver(&Config{}, newFakeView())
	snap := Snapshot{Source: Source{Kind: SourceEnvVar, Name: name}}
	_, err := r.Bind(snap)
	var van *ErrSourceVanished
	if !errors.As(err, &van) {
		t.Fatalf("err = %v, want *ErrSourceVanished", err)
	}
}

func TestBind_CLISession_OK(t *testing.T) {
	view := newFakeView()
	view.clis["claude"] = true
	r := NewResolver(&Config{}, view)
	snap := Snapshot{Source: Source{Kind: SourceCLISession, Tool: "claude"}}
	cred, err := r.Bind(snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.CLITool != "claude" {
		t.Errorf("cli tool = %q", cred.CLITool)
	}
}

func TestBind_CLISession_Vanished(t *testing.T) {
	view := newFakeView()
	// claude session not OK
	r := NewResolver(&Config{}, view)
	snap := Snapshot{Source: Source{Kind: SourceCLISession, Tool: "claude"}}
	_, err := r.Bind(snap)
	var van *ErrSourceVanished
	if !errors.As(err, &van) {
		t.Fatalf("err = %v, want *ErrSourceVanished", err)
	}
}

func TestRequirement_Key(t *testing.T) {
	cases := []struct {
		req  Requirement
		want string
	}{
		{Requirement{Provider: "anthropic", Method: MethodAPIKey}, "anthropic/api_key"},
		{Requirement{Provider: "anthropic", Method: MethodCLIOAuth, Tool: "claude"}, "anthropic/cli_oauth/claude"},
		{Requirement{Provider: "openai", Method: MethodCLIOAuth, Tool: "codex"}, "openai/cli_oauth/codex"},
	}
	for _, c := range cases {
		if got := c.req.Key(); got != c.want {
			t.Errorf("Key() = %q, want %q", got, c.want)
		}
	}
}

func TestSource_ID(t *testing.T) {
	cases := []struct {
		src  Source
		want string
	}{
		{Source{Kind: SourceEnvVar, Name: "X"}, "env:X"},
		{Source{Kind: SourceCLISession, Tool: "claude"}, "cli:claude"},
	}
	for _, c := range cases {
		if got := c.src.ID(); got != c.want {
			t.Errorf("ID() = %q, want %q", got, c.want)
		}
	}
}
