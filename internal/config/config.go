// Loads and merges user/project config.toml files into a single Config.
// Mirrors the structure and semantics of internal/auth/binding/config.go.

package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config is the merged configuration from user and project layers.
// Uses pointer fields to distinguish "field omitted" from "field explicitly
// set to zero/empty value" during TOML decode and merge.
type Config struct {
	SchemaVersion *string       `toml:"schema_version"`
	Runtime       RuntimeConfig `toml:"runtime"`
	CxDB          CxDBConfig    `toml:"cxdb"`
	Tools         ToolsConfig   `toml:"tools"`
}

// RuntimeConfig holds timeout and retry settings for the v0 schema.
type RuntimeConfig struct {
	CodexIdleTimeoutMS          *int `toml:"codex_idle_timeout_ms"`
	CodexTotalTimeoutMS         *int `toml:"codex_total_timeout_ms"`
	CodexKillGraceMS            *int `toml:"codex_kill_grace_ms"`
	CodexTimeoutMaxRetries      *int `toml:"codex_timeout_max_retries"`
	CodergenHeartbeatIntervalMS *int `toml:"codergen_heartbeat_interval_ms"`
	CodexStateDBMaxRetries      *int `toml:"codex_state_db_max_retries"`
}

// CxDBUIConfig holds the CXDB UI command and URL settings.
type CxDBUIConfig struct {
	Command *[]string `toml:"command"`
	URL     *string   `toml:"url"`
}

// CxDBConfig holds CXDB-related configuration with nested UI section.
type CxDBConfig struct {
	UI CxDBUIConfig `toml:"ui"`
}

// ToolsConfig holds tool path configurations.
type ToolsConfig struct {
	ClaudePath *string `toml:"claude_path"`
}

// ErrNoConfig is returned when neither user nor project config files exist.
type ErrNoConfig struct {
	UserPath    string
	ProjectPath string
}

func (e *ErrNoConfig) Error() string {
	return fmt.Sprintf("config: no config found at user=%q project=%q", e.UserPath, e.ProjectPath)
}

// defaultUserConfigPath returns the expected location of the user-level
// config.toml, respecting $XDG_CONFIG_HOME with fallback to ~/.config.
func defaultUserConfigPath() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "kilroy", "config.toml")
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "kilroy", "config.toml")
}

// LoadConfig finds and loads ~/.config/kilroy/config.toml and an optional
// project-level <projectRoot>/.kilroy/config.toml, merging them. Project
// scalar values replace user values.
//
// projectRoot may be empty (no project override). When neither file
// exists, returns *ErrNoConfig.
func LoadConfig(projectRoot string) (Config, error) {
	userPath := defaultUserConfigPath()

	var projectPath string
	if projectRoot != "" {
		projectPath = filepath.Join(projectRoot, ".kilroy", "config.toml")
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

	merged := Config{}

	// User layer: lowest priority.
	if userPath != "" {
		cfg, err := loadTOMLFile(userPath)
		if err != nil {
			return Config{}, err
		}
		merged = cfg
	}

	// Project layer: overwrites user entries of the same key (project-wins semantics).
	if projectPath != "" {
		cfg, err := loadTOMLFile(projectPath)
		if err != nil {
			return Config{}, err
		}
		mergeConfig(&merged, cfg)
	}

	return merged, nil
}

// loadTOMLFile parses a single config.toml file and returns a Config.
// An empty file is valid and produces an empty Config. A malformed file
// or one with unknown fields returns a wrapped error that names the path
// so the user can locate it.
func loadTOMLFile(path string) (Config, error) {
	var cfg Config
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	// Strict decode: reject unknown fields.
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		return Config{}, fmt.Errorf("config: %s: unknown field(s): %v", path, undecoded)
	}

	return cfg, nil
}

// mergeConfig merges src into dst. src entries that are non-nil overwrite
// dst entries. This allows explicit zero/empty values in project config to
// override non-zero values from user config.
func mergeConfig(dst *Config, src Config) {
	// Schema version: project wins if set
	if src.SchemaVersion != nil {
		dst.SchemaVersion = src.SchemaVersion
	}

	// Runtime: project scalar values win if explicitly set (non-nil)
	if src.Runtime.CodexIdleTimeoutMS != nil {
		dst.Runtime.CodexIdleTimeoutMS = src.Runtime.CodexIdleTimeoutMS
	}
	if src.Runtime.CodexTotalTimeoutMS != nil {
		dst.Runtime.CodexTotalTimeoutMS = src.Runtime.CodexTotalTimeoutMS
	}
	if src.Runtime.CodexKillGraceMS != nil {
		dst.Runtime.CodexKillGraceMS = src.Runtime.CodexKillGraceMS
	}
	if src.Runtime.CodexTimeoutMaxRetries != nil {
		dst.Runtime.CodexTimeoutMaxRetries = src.Runtime.CodexTimeoutMaxRetries
	}
	if src.Runtime.CodergenHeartbeatIntervalMS != nil {
		dst.Runtime.CodergenHeartbeatIntervalMS = src.Runtime.CodergenHeartbeatIntervalMS
	}
	if src.Runtime.CodexStateDBMaxRetries != nil {
		dst.Runtime.CodexStateDBMaxRetries = src.Runtime.CodexStateDBMaxRetries
	}

	// CxDB.UI: project values win if explicitly set (scalars and slices)
	if src.CxDB.UI.Command != nil {
		dst.CxDB.UI.Command = src.CxDB.UI.Command
	}
	if src.CxDB.UI.URL != nil {
		dst.CxDB.UI.URL = src.CxDB.UI.URL
	}

	// Tools: project values win if explicitly set
	if src.Tools.ClaudePath != nil {
		dst.Tools.ClaudePath = src.Tools.ClaudePath
	}
}

// fileExists reports whether the given path names a file or directory that
// can be stat'd without error.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
