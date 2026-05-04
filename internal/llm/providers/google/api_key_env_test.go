// Verifies the google adapter honors the _KILROY-first env precedence
// across the GOOGLE_API_KEY / GEMINI_API_KEY / GOOGLE_GENERATIVE_AI_API_KEY
// alias group.

package google

import (
	"strings"
	"testing"
)

func TestAPIKeyEnvPrecedence_KILROY(t *testing.T) {
	cases := []struct {
		name    string
		set     map[string]string
		wantKey string
		wantErr string
	}{
		{
			name: "GOOGLE_KILROY beats everything else",
			set: map[string]string{
				"GOOGLE_API_KEY_KILROY":        "g-kilroy",
				"GEMINI_API_KEY_KILROY":        "gem-kilroy",
				"GOOGLE_API_KEY":               "g-canonical",
				"GEMINI_API_KEY":               "gem-canonical",
				"GOOGLE_GENERATIVE_AI_API_KEY": "gen-ai",
			},
			wantKey: "g-kilroy",
		},
		{
			name: "GEMINI_KILROY beats canonical pair",
			set: map[string]string{
				"GEMINI_API_KEY_KILROY": "gem-kilroy",
				"GOOGLE_API_KEY":        "g-canonical",
				"GEMINI_API_KEY":        "gem-canonical",
			},
			wantKey: "gem-kilroy",
		},
		{
			name:    "falls back to GOOGLE_API_KEY before GEMINI_API_KEY",
			set:     map[string]string{"GOOGLE_API_KEY": "g-canonical", "GEMINI_API_KEY": "gem-canonical"},
			wantKey: "g-canonical",
		},
		{
			name:    "GENERATIVE_AI is last resort",
			set:     map[string]string{"GOOGLE_GENERATIVE_AI_API_KEY": "gen-ai"},
			wantKey: "gen-ai",
		},
		{
			name:    "neither set is an error",
			set:     map[string]string{},
			wantErr: "GEMINI_API_KEY",
		},
	}

	envVars := []string{
		"GOOGLE_API_KEY_KILROY",
		"GEMINI_API_KEY_KILROY",
		"GOOGLE_API_KEY",
		"GEMINI_API_KEY",
		"GOOGLE_GENERATIVE_AI_API_KEY",
		"GEMINI_BASE_URL",
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range envVars {
				t.Setenv(k, "")
			}
			for k, v := range tc.set {
				t.Setenv(k, v)
			}
			a, err := NewFromEnv()
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewFromEnv: %v", err)
			}
			if a.APIKey != tc.wantKey {
				t.Fatalf("APIKey = %q, want %q", a.APIKey, tc.wantKey)
			}
		})
	}
}
