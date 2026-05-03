package policy

import (
	"errors"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// fakeView is a binding.DetectionView for tests.
type fakeView struct {
	envs map[string]bool
	clis map[string]bool
}

func (f fakeView) EnvVarPresent(name string) bool { return f.envs[name] }
func (f fakeView) CLISessionOK(tool string) bool  { return f.clis[tool] }

// testAuthConfig builds a binding.Config matching the policy candidates
// minimalPolicy declares — anthropic api_key + cli_oauth/claude, openai
// api_key.
func testAuthConfig() *binding.Config {
	return &binding.Config{
		Bindings: map[string]string{
			"anthropic/api_key":          "anthropic_api_key",
			"anthropic/cli_oauth/claude": "anthropic_claude_cli",
			"openai/api_key":             "openai_api_key",
		},
		Chains: map[string]binding.Chain{
			"anthropic_api_key": {
				Name:     "anthropic_api_key",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY"}},
			},
			"anthropic_claude_cli": {
				Name:     "anthropic_claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
				Sources:  []binding.Source{{Kind: binding.SourceCLISession, Tool: "claude"}},
			},
			"openai_api_key": {
				Name:     "openai_api_key",
				Requires: binding.Requirement{Provider: "openai", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "OPENAI_API_KEY"}},
			},
		},
	}
}

// minimalPolicy returns a minimal two-class policy suitable for most tests.
func minimalPolicy() *Data {
	return &Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]Class{
			"hard_coding": {
				Description: "test hard coding",
				Chain: []Candidate{
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "claude_cli",
						Transport:   "cli_subprocess",
						HistorySink: "jsonl_local",
						Requires:    binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
					},
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "anthropic_sdk",
						Transport:   "http",
						HistorySink: "api_stream",
						Requires:    binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
					},
				},
			},
			"quick_easy": {
				Description: "test quick easy",
				Chain: []Candidate{
					{
						ModelID:     "gpt-4o-mini",
						Driver:      "openai_sdk",
						Transport:   "http",
						HistorySink: "api_stream",
						Requires:    binding.Requirement{Provider: "openai", Method: binding.MethodAPIKey},
					},
				},
			},
		},
	}
}

// makeResolver constructs a binding.Resolver from a fakeView + the test config.
func makeResolver(view fakeView) *binding.Resolver {
	return binding.NewResolver(testAuthConfig(), view)
}

func TestResolver(t *testing.T) {
	t.Run("class match, first candidate reachable", func(t *testing.T) {
		policy := minimalPolicy()
		view := fakeView{envs: map[string]bool{}, clis: map[string]bool{"claude": true}}
		result, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, makeResolver(view))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != 0 {
			t.Errorf("expected FallbackRank=0, got %d", result.FallbackRank)
		}
		if len(result.Skipped) != 0 {
			t.Errorf("expected no skips, got %d: %v", len(result.Skipped), result.Skipped)
		}
		if result.RequestType != "class" {
			t.Errorf("expected RequestType=class, got %s", result.RequestType)
		}
		if result.ModelID != "claude-opus-4-7" {
			t.Errorf("unexpected ModelID: %s", result.ModelID)
		}
		if result.AuthMethod() != string(binding.MethodCLIOAuth) {
			t.Errorf("expected AuthMethod=cli_oauth, got %s", result.AuthMethod())
		}
		if result.AuthSource() != "claude" {
			t.Errorf("expected AuthSource=claude, got %s", result.AuthSource())
		}
	})

	t.Run("class match, first candidate unreachable second reachable", func(t *testing.T) {
		policy := minimalPolicy()
		view := fakeView{envs: map[string]bool{"ANTHROPIC_API_KEY": true}, clis: map[string]bool{}}
		result, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, makeResolver(view))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != 1 {
			t.Errorf("expected FallbackRank=1, got %d", result.FallbackRank)
		}
		if len(result.Skipped) != 1 {
			t.Errorf("expected 1 skip, got %d", len(result.Skipped))
		}
		if result.Driver != "anthropic_sdk" {
			t.Errorf("expected driver=anthropic_sdk, got %s", result.Driver)
		}
		if result.AuthSource() != "ANTHROPIC_API_KEY" {
			t.Errorf("expected AuthSource=ANTHROPIC_API_KEY, got %s", result.AuthSource())
		}
	})

	t.Run("class with all candidates unreachable", func(t *testing.T) {
		policy := minimalPolicy()
		view := fakeView{envs: map[string]bool{}, clis: map[string]bool{}}
		_, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, makeResolver(view))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var nv ErrNoViableCandidate
		if !errors.As(err, &nv) {
			t.Fatalf("expected ErrNoViableCandidate, got %T: %v", err, err)
		}
		wantSkips := len(policy.Classes["hard_coding"].Chain)
		if len(nv.Skipped) != wantSkips {
			t.Errorf("expected %d skip records, got %d", wantSkips, len(nv.Skipped))
		}
	})

	t.Run("unknown class", func(t *testing.T) {
		policy := minimalPolicy()
		view := fakeView{}
		_, err := Resolve(ResolveRequest{ClassID: "does_not_exist"}, policy, makeResolver(view))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var unk ErrUnknownClass
		if !errors.As(err, &unk) {
			t.Fatalf("expected ErrUnknownClass, got %T: %v", err, err)
		}
		if unk.Name != "does_not_exist" {
			t.Errorf("expected Name=does_not_exist, got %s", unk.Name)
		}
		if len(unk.Available) < 2 {
			t.Errorf("expected Available list, got %v", unk.Available)
		}
		for i := 1; i < len(unk.Available); i++ {
			if unk.Available[i-1] > unk.Available[i] {
				t.Errorf("Available not sorted: %v", unk.Available)
				break
			}
		}
	})

	t.Run("alias resolves and preserves original RequestValue", func(t *testing.T) {
		policy := minimalPolicy()
		policy.Aliases = []ClassAlias{{From: "coding", To: "hard_coding"}}
		view := fakeView{clis: map[string]bool{"claude": true}, envs: map[string]bool{}}
		result, err := Resolve(ResolveRequest{ClassID: "coding"}, policy, makeResolver(view))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequestValue != "coding" {
			t.Errorf("expected RequestValue=coding, got %s", result.RequestValue)
		}
	})

	t.Run("strict mode match reachable", func(t *testing.T) {
		policy := minimalPolicy()
		view := fakeView{envs: map[string]bool{"ANTHROPIC_API_KEY": true}, clis: map[string]bool{}}
		result, err := Resolve(ResolveRequest{ModelID: "claude-opus-4-7"}, policy, makeResolver(view))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != -1 {
			t.Errorf("expected FallbackRank=-1, got %d", result.FallbackRank)
		}
		if result.RequestType != "strict" {
			t.Errorf("expected RequestType=strict, got %s", result.RequestType)
		}
	})

	t.Run("strict mode model not in any class", func(t *testing.T) {
		policy := minimalPolicy()
		_, err := Resolve(ResolveRequest{ModelID: "nonexistent-model-xyz"}, policy, makeResolver(fakeView{}))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var um ErrUnknownModel
		if !errors.As(err, &um) {
			t.Fatalf("expected ErrUnknownModel, got %T: %v", err, err)
		}
	})

	t.Run("strict mode model exists but unreachable", func(t *testing.T) {
		policy := minimalPolicy()
		_, err := Resolve(ResolveRequest{ModelID: "claude-opus-4-7"}, policy, makeResolver(fakeView{}))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var su ErrStrictModelUnreachable
		if !errors.As(err, &su) {
			t.Fatalf("expected ErrStrictModelUnreachable, got %T: %v", err, err)
		}
		if su.Reason == "" {
			t.Error("expected non-empty Reason")
		}
	})

	t.Run("both class and model set", func(t *testing.T) {
		policy := minimalPolicy()
		_, err := Resolve(ResolveRequest{ClassID: "hard_coding", ModelID: "claude-opus-4-7"}, policy, makeResolver(fakeView{}))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var bcm ErrBothClassAndModel
		if !errors.As(err, &bcm) {
			t.Fatalf("expected ErrBothClassAndModel, got %T: %v", err, err)
		}
	})

	t.Run("nil resolver errors", func(t *testing.T) {
		policy := minimalPolicy()
		_, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, nil)
		if err == nil {
			t.Fatal("expected error for nil resolver, got nil")
		}
	})
}
