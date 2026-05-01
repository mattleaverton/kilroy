package auth

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ghHostsPath is the default path to the gh CLI hosts config file.
// Overridden in tests.
var ghHostsPath = os.ExpandEnv("$HOME/.config/gh/hosts.yml")

// keychainProbeGH checks whether a keychain entry exists for the given
// gh service/account pair. The default implementation always returns false on
// non-Darwin platforms; on macOS a real implementation would use
// security(1). Overridden in tests.
var keychainProbeGH func(service, account string) bool = func(service, account string) bool {
	return false
}

// ghHostsFile mirrors the relevant fields from ~/.config/gh/hosts.yml.
type ghHostsFile map[string]ghHostEntry

type ghHostEntry struct {
	User  string              `yaml:"user"`
	Users map[string]struct{} `yaml:"users"`
}

// GHDetector detects credentials for the GitHub gh CLI.
type GHDetector struct{}

// NewGHDetector returns a new GHDetector.
func NewGHDetector() *GHDetector { return &GHDetector{} }

// Name returns the detector name.
func (d *GHDetector) Name() string { return "gh" }

const (
	ghProvider         = "github"
	ghTool             = "gh"
	ghKeychainService  = "gh:github.com"
	ghHost             = "github.com"
	ghCLIEntryID       = "github.gh.cli_oauth"
	ghEnvGHToken       = "GH_TOKEN"
	ghEnvGithubToken   = "GITHUB_TOKEN"
	ghRemediation      = "Run: gh auth login or gh auth switch"
)

// Detect scans for gh CLI credentials.
func (d *GHDetector) Detect() ([]Entry, error) {
	var entries []Entry

	// Determine active env var (if any).
	envVarName := ""
	if v := os.Getenv(ghEnvGHToken); v != "" {
		envVarName = ghEnvGHToken
	} else if v := os.Getenv(ghEnvGithubToken); v != "" {
		envVarName = ghEnvGithubToken
	}

	// Parse hosts.yml.
	data, err := os.ReadFile(ghHostsPath)
	fileAbsent := os.IsNotExist(err)
	if err != nil && !fileAbsent {
		return nil, fmt.Errorf("gh: reading hosts file: %w", err)
	}

	var cliEntry Entry
	cliEntry.ID = ghCLIEntryID
	cliEntry.Kind = KindCLIOAuth
	cliEntry.Provider = ghProvider
	cliEntry.Tool = ghTool
	cliEntry.Source = Source{
		File:            ghHostsPath,
		KeychainService: ghKeychainService,
	}

	if fileAbsent {
		if envVarName == "" {
			// No file, no env var → missing.
			cliEntry.State = StateMissing
			entries = append(entries, cliEntry)
			return entries, nil
		}
		// File absent but env var present: still emit cli entry as missing.
		cliEntry.State = StateMissing
	} else {
		// Parse the YAML.
		var hosts ghHostsFile
		if err := yaml.Unmarshal(data, &hosts); err != nil {
			return nil, fmt.Errorf("gh: parsing hosts file: %w", err)
		}

		hostEntry, ok := hosts[ghHost]
		if !ok {
			// github.com section entirely absent.
			cliEntry.State = StateAmbiguous
			cliEntry.Remediation = ghRemediation
			entries = append(entries, cliEntry)
			if envVarName != "" {
				entries = append([]Entry{buildGHEnvEntry(envVarName, ghCLIEntryID)}, entries...)
			}
			return entries, nil
		}

		activeUser := hostEntry.User
		users := hostEntry.Users

		// Ambiguous cases.
		if activeUser == "" || len(users) == 0 {
			cliEntry.State = StateAmbiguous
			cliEntry.Remediation = ghRemediation
			entries = append(entries, cliEntry)
			if envVarName != "" {
				entries = append([]Entry{buildGHEnvEntry(envVarName, ghCLIEntryID)}, entries...)
			}
			return entries, nil
		}
		if _, inUsers := users[activeUser]; !inUsers {
			cliEntry.State = StateAmbiguous
			cliEntry.Remediation = ghRemediation
			entries = append(entries, cliEntry)
			if envVarName != "" {
				entries = append([]Entry{buildGHEnvEntry(envVarName, ghCLIEntryID)}, entries...)
			}
			return entries, nil
		}

		// Build profiles.
		profiles := make([]Profile, 0, len(users))
		for uname := range users {
			p := Profile{
				Name:   uname,
				Active: uname == activeUser,
				State:  StateOK,
			}
			profiles = append(profiles, p)
		}

		cliEntry.Identity = Identity{User: activeUser}
		cliEntry.Source.KeychainAccount = activeUser
		cliEntry.Profiles = profiles

		// Keychain probe.
		if keychainProbeGH(ghKeychainService, activeUser) {
			cliEntry.State = StateOK
		} else {
			cliEntry.State = StateExpired
		}
	}

	entries = append(entries, cliEntry)

	// Prepend env entry if present, and record shadowing.
	if envVarName != "" {
		envEntry := buildGHEnvEntry(envVarName, ghCLIEntryID)
		cliEntry2 := entries[len(entries)-1]
		cliEntry2.ShadowedBy = []string{envEntry.ID}
		entries[len(entries)-1] = cliEntry2
		entries = append([]Entry{envEntry}, entries...)
	}

	return entries, nil
}

func buildGHEnvEntry(envVarName, cliEntryID string) Entry {
	return Entry{
		ID:       fmt.Sprintf("github.gh.env.%s", envVarName),
		Kind:     KindEnvVar,
		Provider: ghProvider,
		Tool:     ghTool,
		State:    StateOK,
		Source:   Source{EnvVar: envVarName},
		Shadows:  []string{cliEntryID},
	}
}
