package auth

import (
	"os"
	"path/filepath"
	"strings"
)

// Package-level vars for testability.
var (
	claudeSettingsPath = filepath.Join(os.Getenv("HOME"), ".claude", "settings.json")

	// keychainProbeClaude probes for a Claude session token.
	//
	// Default: conservative stub that always returns false.
	//
	// Production wiring (follow-up): replace body with:
	//   err := exec.Command("security", "find-generic-password",
	//     "-s", service, "-a", account).Run()
	//   return err == nil
	// That call does not print the secret; it exits 0 iff the item exists.
	keychainProbeClaude = func(service, account string) bool {
		return false
	}
)

const (
	claudeProvider        = "anthropic"
	claudeTool            = "claude"
	claudeEnvVarName      = "ANTHROPIC_API_KEY"
	claudeEnvVarPrefix    = "sk-ant-"
	claudeKeychainService = "Claude Safe Storage"
	claudeKeychainAccount = "Claude Key"
	claudeEnvEntryID      = "anthropic.env.ANTHROPIC_API_KEY"
	claudeCLIOAuthEntryID = "anthropic.claude.cli_oauth"
)

// ClaudeDetector discovers authentication state for the Anthropic Claude CLI.
type ClaudeDetector struct{}

// NewClaudeDetector returns a new ClaudeDetector.
func NewClaudeDetector() *ClaudeDetector { return &ClaudeDetector{} }

// Name implements Detector.
func (d *ClaudeDetector) Name() string { return "claude" }

// Detect implements Detector.
//
// State matrix:
//
//	settings absent + env absent  → single missing cli_oauth entry
//	settings present + keychain ok                → cli_oauth: ok
//	settings present + no keychain + env set      → cli_oauth: ok (note) + env: ok
//	settings present + no keychain + env absent   → cli_oauth: ambiguous
//	settings absent  + env set                    → env: ok (no cli_oauth entry)
func (d *ClaudeDetector) Detect() ([]Entry, error) {
	apiKey := os.Getenv(claudeEnvVarName)
	envSet := apiKey != ""
	envValid := strings.HasPrefix(apiKey, claudeEnvVarPrefix)

	_, statErr := os.Stat(claudeSettingsPath)
	settingsPresent := statErr == nil

	// Fast path: nothing installed, nothing set.
	if !settingsPresent && !envSet {
		return []Entry{
			{
				ID:       claudeCLIOAuthEntryID,
				Kind:     KindCLIOAuth,
				Provider: claudeProvider,
				Tool:     claudeTool,
				State:    StateMissing,
				Source:   Source{File: claudeSettingsPath},
			},
		}, nil
	}

	var entries []Entry
	var cliEntry *Entry

	if settingsPresent {
		keychainOK := keychainProbeClaude(claudeKeychainService, claudeKeychainAccount)
		e := Entry{
			ID:       claudeCLIOAuthEntryID,
			Kind:     KindCLIOAuth,
			Provider: claudeProvider,
			Tool:     claudeTool,
			Source: Source{
				File:            claudeSettingsPath,
				KeychainService: claudeKeychainService,
				KeychainAccount: claudeKeychainAccount,
			},
		}
		switch {
		case keychainOK:
			e.State = StateOK
		case envSet:
			e.State = StateOK
			e.Notes = []string{"env var overrides absent keychain session"}
		default:
			e.State = StateAmbiguous
			e.Remediation = "Re-run: claude /logout && claude"
		}
		cliEntry = &e
	}

	if envSet {
		envState := StateOK
		if !envValid {
			envState = StateAmbiguous
		}
		envEntry := Entry{
			ID:       claudeEnvEntryID,
			Kind:     KindEnvVar,
			Provider: claudeProvider,
			Tool:     claudeTool,
			State:    envState,
			Source:   Source{EnvVar: claudeEnvVarName},
		}
		if cliEntry != nil {
			envEntry.Shadows = []string{claudeCLIOAuthEntryID}
			cliEntry.ShadowedBy = []string{claudeEnvEntryID}
		}
		entries = append(entries, envEntry)
	}

	if cliEntry != nil {
		entries = append(entries, *cliEntry)
	}

	return entries, nil
}
