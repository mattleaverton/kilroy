// Loads and merges user/project auth.toml files into a single Config.
// See docs/plans/2026-05-02-auth-class-resolver-integration.md §2 + §6.

package binding

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// defaultUserConfigPath returns the expected location of the user-level
// auth.toml, respecting $XDG_CONFIG_HOME with fallback to ~/.config.
func defaultUserConfigPath() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "kilroy", "auth.toml")
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "kilroy", "auth.toml")
}

// LoadConfig finds and loads ~/.config/kilroy/auth.toml and an optional
// project-level <projectRoot>/.kilroy/auth.toml, merging them. Project
// chain entries replace user chain entries of the same name; same for
// binding entries.
//
// projectRoot may be empty (no project override). When neither file
// exists, returns *ErrNoConfig.
func LoadConfig(projectRoot string) (Config, error) {
	userPath := defaultUserConfigPath()

	var projectPath string
	if projectRoot != "" {
		projectPath = filepath.Join(projectRoot, ".kilroy", "auth.toml")
	}

	userExists := fileExists(userPath)
	projectExists := projectPath != "" && fileExists(projectPath)

	if !userExists && !projectExists {
		return Config{}, &ErrNoConfig{UserPath: userPath, ProjectPath: projectPath}
	}

	// Pass only the paths that actually exist; empty string = skip in
	// LoadConfigFromPaths (avoids a redundant stat / re-parse error).
	var effectiveUser, effectiveProject string
	if userExists {
		effectiveUser = userPath
	}
	if projectExists {
		effectiveProject = projectPath
	}

	return LoadConfigFromPaths(effectiveUser, effectiveProject)
}

// LoadConfigFromPaths is the testable variant. userPath and projectPath
// may each be empty strings to signal "skip this layer." Otherwise, the
// path must exist (file-not-found is an error here, not a missing-config).
// When both are empty, returns *ErrNoConfig.
func LoadConfigFromPaths(userPath, projectPath string) (Config, error) {
	if userPath == "" && projectPath == "" {
		return Config{}, &ErrNoConfig{UserPath: userPath, ProjectPath: projectPath}
	}

	merged := Config{
		Bindings: make(map[string]string),
		Chains:   make(map[string]Chain),
	}

	// User layer: lowest priority.
	if userPath != "" {
		cfg, err := loadTOMLFile(userPath)
		if err != nil {
			return Config{}, err
		}
		mergeConfig(&merged, cfg)
	}

	// Project layer: overwrites user entries of the same key (plan principle 6).
	if projectPath != "" {
		cfg, err := loadTOMLFile(projectPath)
		if err != nil {
			return Config{}, err
		}
		mergeConfig(&merged, cfg)
	}

	return merged, nil
}

// loadTOMLFile parses a single auth.toml file and returns a Config.
// An empty file is valid and produces an empty Config. A malformed file
// returns a wrapped error that names the path so the user can locate it.
func loadTOMLFile(path string) (Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("auth config: parsing %s: %w", path, err)
	}

	// Ensure maps are non-nil when the file had no [bindings] or [chains].
	if cfg.Bindings == nil {
		cfg.Bindings = make(map[string]string)
	}
	if cfg.Chains == nil {
		cfg.Chains = make(map[string]Chain)
	}

	// Chain.Name is tagged toml:"-" so the decoder leaves it empty;
	// populate it from the map key here.
	for name, chain := range cfg.Chains {
		chain.Name = name
		cfg.Chains[name] = chain
	}

	return cfg, nil
}

// mergeConfig merges src into dst. src entries overwrite dst entries of the
// same key — project-replaces-user semantics with no source-list merging
// across layers (plan principle 6).
func mergeConfig(dst *Config, src Config) {
	for k, v := range src.Bindings {
		dst.Bindings[k] = v
	}
	for name, chain := range src.Chains {
		dst.Chains[name] = chain
	}
}

// fileExists reports whether the given path names a file or directory that
// can be stat'd without error.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
