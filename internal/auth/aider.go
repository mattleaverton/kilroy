package auth

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// aiderGlobalPath and aiderProjectPath are package-level vars so tests can override them.
var (
	aiderGlobalPath  string
	aiderProjectPath = ".aider.conf.yml"
)

func init() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.Getenv("HOME")
	}
	aiderGlobalPath = filepath.Join(home, ".aider.conf.yml")
}

// aiderProviderKeys maps aider YAML config keys to canonical provider names.
var aiderProviderKeys = map[string]string{
	"anthropic-api-key": "anthropic",
	"openai-api-key":    "openai",
	"gemini-api-key":    "google",
}

// AiderDetector discovers auth credentials configured for the Aider CLI tool.
// It reads global (~/.aider.conf.yml) and project-local (.aider.conf.yml) config
// files and reports found API keys without exposing their values.
type AiderDetector struct{}

// NewAiderDetector returns a new AiderDetector.
func NewAiderDetector() *AiderDetector { return &AiderDetector{} }

// Name returns the stable detector name.
func (d *AiderDetector) Name() string { return "aider" }

// Detect scans Aider config files for provider API keys.
// It emits one entry per discovered key (KindCLIAPIKey), with project config
// taking precedence over global config when both define the same key.
// Env-var credentials are handled by a separate env-var detector, not here.
func (d *AiderDetector) Detect() ([]Entry, error) {
	globalKeys, globalErr, globalExists := readAiderConfig(aiderGlobalPath)
	projectKeys, projectErr, projectExists := readAiderConfig(aiderProjectPath)

	var entries []Entry

	// Emit ambiguous entries for any malformed config files.
	if globalExists && globalErr != nil {
		entries = append(entries, Entry{
			ID:          "aider.ambiguous.global",
			Kind:        KindCLIAPIKey,
			Provider:    "",
			Tool:        "aider",
			State:       StateAmbiguous,
			Source:      Source{File: aiderGlobalPath},
			Remediation: fmt.Sprintf("Aider config malformed: %s", aiderGlobalPath),
		})
	}
	if projectExists && projectErr != nil {
		entries = append(entries, Entry{
			ID:          "aider.ambiguous.project",
			Kind:        KindCLIAPIKey,
			Provider:    "",
			Tool:        "aider",
			State:       StateAmbiguous,
			Source:      Source{File: aiderProjectPath},
			Remediation: fmt.Sprintf("Aider config malformed: %s", aiderProjectPath),
		})
	}

	// Emit one entry per recognized provider key found in either (non-malformed) file.
	// Project config takes precedence over global config for the same key.
	for yamlKey, provider := range aiderProviderKeys {
		inGlobal := globalErr == nil && globalKeys[yamlKey]
		inProject := projectErr == nil && projectKeys[yamlKey]

		if !inGlobal && !inProject {
			continue
		}

		if inProject {
			e := Entry{
				ID:       fmt.Sprintf("aider.project.cli_api_key.%s", provider),
				Kind:     KindCLIAPIKey,
				Provider: provider,
				Tool:     "aider",
				State:    StateOK,
				Source:   Source{File: aiderProjectPath},
			}
			if inGlobal {
				globalID := fmt.Sprintf("aider.global.cli_api_key.%s", provider)
				e.Notes = []string{"shadows ~/.aider.conf.yml entry"}
				e.Shadows = []string{globalID}
			}
			entries = append(entries, e)
		} else {
			entries = append(entries, Entry{
				ID:       fmt.Sprintf("aider.global.cli_api_key.%s", provider),
				Kind:     KindCLIAPIKey,
				Provider: provider,
				Tool:     "aider",
				State:    StateOK,
				Source:   Source{File: aiderGlobalPath},
			})
		}
	}

	return entries, nil
}

// readAiderConfig reads and minimally parses an aider YAML config file.
// Returns:
//   - keys: set of recognized provider YAML keys present with non-empty values
//   - err: non-nil if the file exists but is malformed
//   - exists: true if the file was found (readable)
func readAiderConfig(path string) (keys map[string]bool, err error, exists bool) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return nil, nil, false
		}
		// Unreadable file — treat as malformed (exists but broken).
		return nil, readErr, true
	}

	var raw map[string]interface{}
	if parseErr := yaml.Unmarshal(data, &raw); parseErr != nil {
		return nil, parseErr, true
	}

	keys = make(map[string]bool)
	for yamlKey := range aiderProviderKeys {
		if val, ok := raw[yamlKey]; ok && val != nil {
			if str, ok := val.(string); ok && str != "" {
				keys[yamlKey] = true
			}
		}
	}
	return keys, nil, true
}
