package auth

import (
	"fmt"
	"os"
	"strings"
)

// envVarSpec describes one env var we scan for credentials.
type envVarSpec struct {
	name     string
	provider string
	// prefixes lists valid recognized prefixes in priority order.
	// Any match → StateOK; no match → StateAmbiguous.
	prefixes []string
}

// envVarTable is ordered; aliases (GEMINI_API_KEY, GITHUB_TOKEN) follow their
// primary counterparts so the emitted slice is naturally ordered.
var envVarTable = []envVarSpec{
	{name: "ANTHROPIC_API_KEY", provider: "anthropic", prefixes: []string{"sk-ant-"}},
	{name: "OPENAI_API_KEY", provider: "openai", prefixes: []string{"sk-proj-", "sk-"}},
	{name: "GOOGLE_API_KEY", provider: "google", prefixes: []string{"AIza"}},
	{name: "GEMINI_API_KEY", provider: "google", prefixes: []string{"AIza"}},
	{name: "OPENROUTER_API_KEY", provider: "openrouter", prefixes: []string{"sk-or-"}},
	{name: "GH_TOKEN", provider: "github", prefixes: []string{"ghp_", "github_pat_"}},
	{name: "GITHUB_TOKEN", provider: "github", prefixes: []string{"ghp_", "github_pat_"}},
}

// EnvVarDetector scans well-known API-key environment variables.
type EnvVarDetector struct{}

// NewEnvVarDetector constructs an EnvVarDetector.
func NewEnvVarDetector() *EnvVarDetector { return &EnvVarDetector{} }

// Name returns the stable detector name.
func (d *EnvVarDetector) Name() string { return "env_vars" }

// Detect scans envVarTable and emits one Entry per set, non-empty variable.
// Credential values are never stored or returned; only their prefix is noted.
func (d *EnvVarDetector) Detect() ([]Entry, error) {
	var entries []Entry

	for _, spec := range envVarTable {
		val, ok := os.LookupEnv(spec.name)
		if !ok || val == "" {
			continue
		}

		var state State
		var notes []string

		// Find the first matching recognized prefix.
		matched := ""
		for _, p := range spec.prefixes {
			if strings.HasPrefix(val, p) {
				matched = p
				break
			}
		}

		if matched != "" {
			state = StateOK
			notes = []string{fmt.Sprintf("key prefix: %s", matched)}
		} else {
			state = StateAmbiguous
			// Show only the first 8 chars as a non-sensitive hint.
			preview := val
			if len(preview) > 8 {
				preview = preview[:8]
			}
			// Build human-readable list of expected prefixes.
			quoted := make([]string, len(spec.prefixes))
			for i, p := range spec.prefixes {
				quoted[i] = "`" + p + "`"
			}
			expected := strings.Join(quoted, " or ")
			notes = []string{
				fmt.Sprintf("key prefix: %s", preview),
				fmt.Sprintf("value does not match expected %s prefix", expected),
			}
		}

		var remediation string
		if state == StateAmbiguous {
			remediation = fmt.Sprintf("Verify the value of %s; expected prefix not recognized. Re-export the var with a valid key.", spec.name)
		}
		entries = append(entries, Entry{
			ID:          fmt.Sprintf("%s.env.%s", spec.provider, spec.name),
			Kind:        KindEnvVar,
			Provider:    spec.provider,
			Tool:        "",
			State:       state,
			Source:      Source{EnvVar: spec.name},
			Notes:       notes,
			Remediation: remediation,
		})
	}

	return entries, nil
}
