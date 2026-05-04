// Verifies the openai adapter honors the _KILROY-first env precedence
// (OPENAI_API_KEY_KILROY before OPENAI_API_KEY).

package openai

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
			name:    "kilroy beats canonical",
			set:     map[string]string{"OPENAI_API_KEY_KILROY": "kilroy-secret", "OPENAI_API_KEY": "canonical-secret"},
			wantKey: "kilroy-secret",
		},
		{
			name:    "falls back to canonical",
			set:     map[string]string{"OPENAI_API_KEY": "canonical-secret"},
			wantKey: "canonical-secret",
		},
		{
			name:    "kilroy alone is sufficient",
			set:     map[string]string{"OPENAI_API_KEY_KILROY": "only-kilroy"},
			wantKey: "only-kilroy",
		},
		{
			name:    "neither set is an error",
			set:     map[string]string{},
			wantErr: "OPENAI_API_KEY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPENAI_API_KEY_KILROY", "")
			t.Setenv("OPENAI_API_KEY", "")
			t.Setenv("OPENAI_BASE_URL", "")
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
