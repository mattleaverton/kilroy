package policy

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/auth"
)

// envEntry builds a test auth.Entry representing an OK environment variable.
func envEntry(provider, varName string) auth.Entry {
	return auth.Entry{
		Kind:     auth.KindEnvVar,
		Provider: provider,
		State:    auth.StateOK,
		Source:   auth.Source{EnvVar: varName},
	}
}

// cliEntry builds a test auth.Entry representing an authenticated CLI tool.
func cliEntry(provider, tool string) auth.Entry {
	return auth.Entry{
		Kind:     auth.KindCLIOAuth,
		Provider: provider,
		Tool:     tool,
		State:    auth.StateOK,
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
						Auth:        AuthReq{Kind: "cli_session", CLI: "claude"},
					},
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "anthropic_sdk",
						Transport:   "http",
						HistorySink: "api_stream",
						Auth:        AuthReq{Kind: "env_var", EnvVar: "ANTHROPIC_API_KEY"},
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
						Auth:        AuthReq{Kind: "env_var", EnvVar: "OPENAI_API_KEY"},
					},
				},
			},
		},
	}
}

func TestResolver(t *testing.T) {
	// Case 1: class match, first candidate reachable → rank 0, no skips.
	t.Run("class match, first candidate reachable", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{Auth: auth.ListOutput{
			Entries: []auth.Entry{cliEntry("anthropic", "claude")},
		}}
		result, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, state)
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
	})

	// Case 2: class match, first candidate unreachable, second reachable → rank 1, one skip.
	t.Run("class match, first candidate unreachable second reachable", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{Auth: auth.ListOutput{
			Entries: []auth.Entry{envEntry("anthropic", "ANTHROPIC_API_KEY")},
		}}
		result, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, state)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != 1 {
			t.Errorf("expected FallbackRank=1, got %d", result.FallbackRank)
		}
		if len(result.Skipped) != 1 {
			t.Errorf("expected 1 skip, got %d", len(result.Skipped))
		}
		if len(result.Skipped) > 0 {
			want := "cli_not_installed:claude"
			if result.Skipped[0].Reason != want {
				t.Errorf("expected skip reason %q, got %q", want, result.Skipped[0].Reason)
			}
		}
		if result.Driver != "anthropic_sdk" {
			t.Errorf("expected driver=anthropic_sdk, got %s", result.Driver)
		}
	})

	// Case 3: class with all candidates unreachable → ErrNoViableCandidate covering all.
	t.Run("class with all candidates unreachable", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{Auth: auth.ListOutput{Entries: []auth.Entry{}}}
		_, err := Resolve(ResolveRequest{ClassID: "hard_coding"}, policy, state)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		e, ok := err.(ErrNoViableCandidate)
		if !ok {
			t.Fatalf("expected ErrNoViableCandidate, got %T: %v", err, err)
		}
		wantSkips := len(policy.Classes["hard_coding"].Chain) // 2
		if len(e.Skipped) != wantSkips {
			t.Errorf("expected %d skip records (one per candidate), got %d", wantSkips, len(e.Skipped))
		}
		if e.ClassID != "hard_coding" {
			t.Errorf("expected ClassID=hard_coding, got %s", e.ClassID)
		}
	})

	// Case 4: unknown class → ErrUnknownClass with sorted Available.
	t.Run("unknown class", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{}
		_, err := Resolve(ResolveRequest{ClassID: "does_not_exist"}, policy, state)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		e, ok := err.(ErrUnknownClass)
		if !ok {
			t.Fatalf("expected ErrUnknownClass, got %T: %v", err, err)
		}
		if e.Name != "does_not_exist" {
			t.Errorf("expected Name=does_not_exist, got %s", e.Name)
		}
		if len(e.Available) < 2 {
			t.Errorf("expected Available to list class names, got %v", e.Available)
		}
		// Verify Available is sorted.
		for i := 1; i < len(e.Available); i++ {
			if e.Available[i-1] > e.Available[i] {
				t.Errorf("Available not sorted: %v", e.Available)
				break
			}
		}
	})

	// Case 5: alias → resolving aliased name succeeds; RequestValue preserves original.
	t.Run("alias resolves and preserves original RequestValue", func(t *testing.T) {
		policy := minimalPolicy()
		policy.Aliases = []ClassAlias{
			{From: "coding", To: "hard_coding"},
		}
		state := MachineState{Auth: auth.ListOutput{
			Entries: []auth.Entry{cliEntry("anthropic", "claude")},
		}}
		result, err := Resolve(ResolveRequest{ClassID: "coding"}, policy, state)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequestValue != "coding" {
			t.Errorf("expected RequestValue=coding (original), got %s", result.RequestValue)
		}
		if result.ModelID == "" {
			t.Error("expected a resolved ModelID")
		}
	})

	// Case 6: strict-mode match, reachable → FallbackRank == -1.
	t.Run("strict mode match reachable", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{Auth: auth.ListOutput{
			Entries: []auth.Entry{envEntry("anthropic", "ANTHROPIC_API_KEY")},
		}}
		result, err := Resolve(ResolveRequest{ModelID: "claude-opus-4-7"}, policy, state)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != -1 {
			t.Errorf("expected FallbackRank=-1, got %d", result.FallbackRank)
		}
		if result.RequestType != "strict" {
			t.Errorf("expected RequestType=strict, got %s", result.RequestType)
		}
		if result.RequestValue != "claude-opus-4-7" {
			t.Errorf("expected RequestValue=claude-opus-4-7, got %s", result.RequestValue)
		}
	})

	// Case 7: strict-mode, model not in any class → ErrUnknownModel.
	t.Run("strict mode model not in any class", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{}
		_, err := Resolve(ResolveRequest{ModelID: "nonexistent-model-xyz"}, policy, state)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		e, ok := err.(ErrUnknownModel)
		if !ok {
			t.Fatalf("expected ErrUnknownModel, got %T: %v", err, err)
		}
		if e.ModelID != "nonexistent-model-xyz" {
			t.Errorf("expected ModelID=nonexistent-model-xyz, got %s", e.ModelID)
		}
	})

	// Case 8: strict-mode, model exists but unreachable → ErrStrictModelUnreachable.
	t.Run("strict mode model exists but unreachable", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{Auth: auth.ListOutput{Entries: []auth.Entry{}}}
		_, err := Resolve(ResolveRequest{ModelID: "claude-opus-4-7"}, policy, state)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		e, ok := err.(ErrStrictModelUnreachable)
		if !ok {
			t.Fatalf("expected ErrStrictModelUnreachable, got %T: %v", err, err)
		}
		if e.ModelID != "claude-opus-4-7" {
			t.Errorf("expected ModelID=claude-opus-4-7, got %s", e.ModelID)
		}
		if e.Reason == "" {
			t.Error("expected non-empty Reason")
		}
	})

	// Case 9: both ClassID and ModelID set → ErrBothClassAndModel.
	t.Run("both class and model set", func(t *testing.T) {
		policy := minimalPolicy()
		state := MachineState{}
		_, err := Resolve(ResolveRequest{ClassID: "hard_coding", ModelID: "claude-opus-4-7"}, policy, state)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if _, ok := err.(ErrBothClassAndModel); !ok {
			t.Fatalf("expected ErrBothClassAndModel, got %T: %v", err, err)
		}
	})

	// Case 10: none-auth candidate → always reachable regardless of state.
	t.Run("none-auth candidate always reachable", func(t *testing.T) {
		policy := &Data{
			SchemaVersion: "1",
			PolicyVersion: "test",
			Classes: map[string]Class{
				"local": {
					Description: "local model, no auth",
					Chain: []Candidate{
						{
							ModelID:     "local-llm",
							Driver:      "local_sdk",
							Transport:   "http",
							HistorySink: "jsonl_local",
							Auth:        AuthReq{Kind: "none"},
						},
					},
				},
			},
		}
		state := MachineState{Auth: auth.ListOutput{Entries: []auth.Entry{}}}
		result, err := Resolve(ResolveRequest{ClassID: "local"}, policy, state)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.FallbackRank != 0 {
			t.Errorf("expected FallbackRank=0, got %d", result.FallbackRank)
		}
		if result.ModelID != "local-llm" {
			t.Errorf("expected ModelID=local-llm, got %s", result.ModelID)
		}
		if result.AuthMethod != "none" {
			t.Errorf("expected AuthMethod=none, got %s", result.AuthMethod)
		}
	})
}
