package llm

import "testing"

func TestExecutionPolicy_Kimi(t *testing.T) {
	p := ExecutionPolicy("moonshot", "")
	if !p.ForceStream {
		t.Fatalf("ForceStream=false want true")
	}
	if got, want := p.MinMaxTokens, 16000; got != want {
		t.Fatalf("MinMaxTokens=%d want %d", got, want)
	}
	if p.Reason == "" {
		t.Fatalf("Reason should be non-empty")
	}
}

func TestExecutionPolicy_NonKimi(t *testing.T) {
	for _, provider := range []string{"openai", "anthropic", "google", "zai", "minimax"} {
		t.Run(provider, func(t *testing.T) {
			p := ExecutionPolicy(provider, "")
			if p.ForceStream {
				t.Fatalf("ForceStream=true want false")
			}
			if p.MinMaxTokens != 0 {
				t.Fatalf("MinMaxTokens=%d want 0", p.MinMaxTokens)
			}
			if p.MaxMaxTokens != 0 {
				t.Fatalf("MaxMaxTokens=%d want 0 for unknown model", p.MaxMaxTokens)
			}
			if p.Reason != "" {
				t.Fatalf("Reason=%q want empty", p.Reason)
			}
		})
	}
}

func TestExecutionPolicy_ModelCap(t *testing.T) {
	cases := []struct {
		provider    string
		model       string
		wantMaxCap  int
	}{
		// Models below Anthropic default (65,536)
		{"anthropic", "claude-3.5-sonnet", 8192},
		{"anthropic", "anthropic/claude-3.5-sonnet", 8192},
		{"anthropic", "claude-3.5-haiku", 8192},
		{"anthropic", "claude-3-haiku", 4096},
		{"anthropic", "claude-opus-4.1", 32000},
		{"anthropic", "claude-opus-4", 32000},
		{"anthropic", "claude-opus-4.5", 64000},
		{"anthropic", "claude-sonnet-4.5", 64000},
		{"anthropic", "claude-3.7-sonnet", 64000},
		{"anthropic", "claude-3.7-sonnet:thinking", 64000},
		// Models at/above Anthropic default — no cap
		{"anthropic", "claude-opus-4.6", 0},
		// Google models below default (32,768)
		{"google", "gemini-2.0-flash-001", 8192},
		{"google", "google/gemini-2.0-flash-001", 8192},
		{"google", "gemini-2.0-flash-lite-001", 8192},
		// Google models at/above default — no cap
		{"google", "gemini-2.5-pro", 0},
		{"google", "gemini-2.5-flash", 0},
	}

	for _, tc := range cases {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			p := ExecutionPolicy(tc.provider, tc.model)
			if p.MaxMaxTokens != tc.wantMaxCap {
				t.Fatalf("MaxMaxTokens=%d want %d", p.MaxMaxTokens, tc.wantMaxCap)
			}
		})
	}
}

func TestApplyExecutionPolicy_MinMaxTokens(t *testing.T) {
	cases := []struct {
		name     string
		start    *int
		policy   ProviderExecutionPolicy
		want     int
		wantSame bool
	}{
		{
			name:     "no max tokens set",
			start:    nil,
			policy:   ProviderExecutionPolicy{MinMaxTokens: 16000},
			want:     16000,
			wantSame: false,
		},
		{
			name:     "below floor",
			start:    intRef(16),
			policy:   ProviderExecutionPolicy{MinMaxTokens: 16000},
			want:     16000,
			wantSame: false,
		},
		{
			name:     "already at floor",
			start:    intRef(16000),
			policy:   ProviderExecutionPolicy{MinMaxTokens: 16000},
			want:     16000,
			wantSame: true,
		},
		{
			name:     "above floor",
			start:    intRef(32000),
			policy:   ProviderExecutionPolicy{MinMaxTokens: 16000},
			want:     32000,
			wantSame: true,
		},
		{
			name:     "policy disabled",
			start:    intRef(42),
			policy:   ProviderExecutionPolicy{MinMaxTokens: 0},
			want:     42,
			wantSame: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := Request{
				Model:     "m",
				Provider:  "kimi",
				Messages:  []Message{User("hi")},
				MaxTokens: tc.start,
			}
			got := ApplyExecutionPolicy(req, tc.policy)
			if got.MaxTokens == nil {
				t.Fatalf("MaxTokens=nil")
			}
			if *got.MaxTokens != tc.want {
				t.Fatalf("MaxTokens=%d want %d", *got.MaxTokens, tc.want)
			}
			if tc.wantSame && tc.start != nil && got.MaxTokens != tc.start {
				t.Fatalf("expected MaxTokens pointer to be unchanged")
			}
			if !tc.wantSame && tc.start != nil && got.MaxTokens == tc.start {
				t.Fatalf("expected MaxTokens pointer to be replaced")
			}
		})
	}
}

func TestApplyExecutionPolicy_MaxMaxTokens(t *testing.T) {
	cases := []struct {
		name  string
		start *int
		cap   int
		want  int
	}{
		{"nil input capped", nil, 8192, 8192},
		{"above cap clamped", intRef(65536), 8192, 8192},
		{"at cap unchanged", intRef(8192), 8192, 8192},
		{"below cap unchanged", intRef(4096), 8192, 4096},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := Request{
				Model:     "claude-3.5-sonnet",
				Provider:  "anthropic",
				Messages:  []Message{User("hi")},
				MaxTokens: tc.start,
			}
			policy := ProviderExecutionPolicy{MaxMaxTokens: tc.cap}
			got := ApplyExecutionPolicy(req, policy)
			if got.MaxTokens == nil {
				t.Fatalf("MaxTokens=nil")
			}
			if *got.MaxTokens != tc.want {
				t.Fatalf("MaxTokens=%d want %d", *got.MaxTokens, tc.want)
			}
		})
	}
}

func intRef(v int) *int { return &v }
