// Tests for the config loader. Covers user-only, project-only, merged,
// overlapping (project-wins), missing, malformed, empty, schema_version,
// strict unknown-field rejection, and explicit zero/empty override edge cases.

package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Helper functions for creating pointers to literals in tests.
func intPtr(i int) *int       { return &i }
func strPtr(s string) *string { return &s }

// writeTestFile creates parent directories and writes content to path.
func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// Sample TOML fixtures used across multiple tests.
const userConfigTOML = `
schema_version = "1.0.0"

[runtime]
codex_idle_timeout_ms = 1000
codex_total_timeout_ms = 2000

[cxdb.ui]
url = "http://localhost:9000"
`

const projectConfigTOML = `
schema_version = "1.1.0"

[runtime]
codex_idle_timeout_ms = 3000
codex_kill_grace_ms = 5000

[tools]
claude_path = "/usr/local/bin/claude"
`

// Test 1: user file only — project path skipped (empty string).
func TestLoadConfigFromPaths_UserOnly(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, userConfigTOML)

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := derefString(cfg.SchemaVersion); got != "1.0.0" {
		t.Errorf("SchemaVersion = %q, want 1.0.0", got)
	}
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 1000 {
		t.Errorf("Runtime.CodexIdleTimeoutMS = %d, want 1000", got)
	}
	if got := derefInt(cfg.Runtime.CodexTotalTimeoutMS); got != 2000 {
		t.Errorf("Runtime.CodexTotalTimeoutMS = %d, want 2000", got)
	}
	if got := derefString(cfg.CxDB.UI.URL); got != "http://localhost:9000" {
		t.Errorf("CxDB.UI.URL = %q, want http://localhost:9000", got)
	}
}

// Test 2: project file only — user path skipped.
func TestLoadConfigFromPaths_ProjectOnly(t *testing.T) {
	dir := t.TempDir()
	projectPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, projectPath, projectConfigTOML)

	cfg, err := LoadConfigFromPaths("", projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := derefString(cfg.SchemaVersion); got != "1.1.0" {
		t.Errorf("SchemaVersion = %q, want 1.1.0", got)
	}
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 3000 {
		t.Errorf("Runtime.CodexIdleTimeoutMS = %d, want 3000", got)
	}
	if got := derefInt(cfg.Runtime.CodexKillGraceMS); got != 5000 {
		t.Errorf("Runtime.CodexKillGraceMS = %d, want 5000", got)
	}
	if got := derefString(cfg.Tools.ClaudePath); got != "/usr/local/bin/claude" {
		t.Errorf("Tools.ClaudePath = %q, want /usr/local/bin/claude", got)
	}
}

// Test 3: both present, no overlapping scalar values — all entries appear.
func TestLoadConfigFromPaths_BothNoOverlap(t *testing.T) {
	userContent := `
schema_version = "1.0.0"

[runtime]
codex_idle_timeout_ms = 1000

[cxdb.ui]
url = "http://user-url:9000"
`
	projectContent := `
[tools]
claude_path = "/project/claude"
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := derefString(cfg.SchemaVersion); got != "1.0.0" {
		t.Errorf("SchemaVersion = %q, want 1.0.0 (from user)", got)
	}
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 1000 {
		t.Errorf("Runtime.CodexIdleTimeoutMS = %d, want 1000 (from user)", got)
	}
	if got := derefString(cfg.CxDB.UI.URL); got != "http://user-url:9000" {
		t.Errorf("CxDB.UI.URL = %q, want http://user-url:9000 (from user)", got)
	}
	if got := derefString(cfg.Tools.ClaudePath); got != "/project/claude" {
		t.Errorf("Tools.ClaudePath = %q, want /project/claude (from project)", got)
	}
}

// Test 4: overlapping scalar values — project wins.
func TestLoadConfigFromPaths_OverlappingScalars_ProjectWins(t *testing.T) {
	userContent := `
schema_version = "1.0.0"

[runtime]
codex_idle_timeout_ms = 1000
codex_total_timeout_ms = 2000

[cxdb.ui]
url = "http://user-url:9000"

[tools]
claude_path = "/user/claude"
`
	projectContent := `
schema_version = "2.0.0"

[runtime]
codex_idle_timeout_ms = 9999
codex_kill_grace_ms = 5000

[cxdb.ui]
url = "http://project-url:9001"

[tools]
claude_path = "/project/claude"
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Project should win on all overlapping fields
	if got := derefString(cfg.SchemaVersion); got != "2.0.0" {
		t.Errorf("SchemaVersion = %q, want 2.0.0 (project wins)", got)
	}
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 9999 {
		t.Errorf("Runtime.CodexIdleTimeoutMS = %d, want 9999 (project wins)", got)
	}
	// Non-overlapping user fields should be preserved
	if got := derefInt(cfg.Runtime.CodexTotalTimeoutMS); got != 2000 {
		t.Errorf("Runtime.CodexTotalTimeoutMS = %d, want 2000 (from user)", got)
	}
	// Project-only fields should be present
	if got := derefInt(cfg.Runtime.CodexKillGraceMS); got != 5000 {
		t.Errorf("Runtime.CodexKillGraceMS = %d, want 5000 (from project)", got)
	}
	if got := derefString(cfg.CxDB.UI.URL); got != "http://project-url:9001" {
		t.Errorf("CxDB.UI.URL = %q, want http://project-url:9001 (project wins)", got)
	}
	if got := derefString(cfg.Tools.ClaudePath); got != "/project/claude" {
		t.Errorf("Tools.ClaudePath = %q, want /project/claude (project wins)", got)
	}
}

// Test 5: neither file present — LoadConfig returns *ErrNoConfig with both paths.
func TestLoadConfig_NeitherPresent(t *testing.T) {
	xdgDir := t.TempDir() // valid dir, but no config.toml inside
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	projectRoot := t.TempDir() // valid dir, but no .kilroy/config.toml inside

	_, err := LoadConfig(projectRoot)
	if err == nil {
		t.Fatal("expected *ErrNoConfig, got nil")
	}

	var noConf *ErrNoConfig
	if !errors.As(err, &noConf) {
		t.Fatalf("expected *ErrNoConfig, got %T: %v", err, err)
	}

	wantUser := filepath.Join(xdgDir, "kilroy", "config.toml")
	wantProject := filepath.Join(projectRoot, ".kilroy", "config.toml")

	if noConf.UserPath != wantUser {
		t.Errorf("UserPath = %q, want %q", noConf.UserPath, wantUser)
	}
	if noConf.ProjectPath != wantProject {
		t.Errorf("ProjectPath = %q, want %q", noConf.ProjectPath, wantProject)
	}
}

// Test 5b: LoadConfigFromPaths("","") also returns *ErrNoConfig.
func TestLoadConfigFromPaths_BothEmpty(t *testing.T) {
	_, err := LoadConfigFromPaths("", "")
	var noConf *ErrNoConfig
	if !errors.As(err, &noConf) {
		t.Fatalf("expected *ErrNoConfig, got %T: %v", err, err)
	}
}

// Test 6: malformed file — error wraps the file path.
func TestLoadConfigFromPaths_MalformedFile(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, "key = @@@invalid_value\n")

	_, err := LoadConfigFromPaths(userPath, "")
	if err == nil {
		t.Fatal("expected error for malformed TOML, got nil")
	}
	if !strings.Contains(err.Error(), userPath) {
		t.Errorf("error should mention the file path %q; got: %v", userPath, err)
	}
}

// Test 7: unknown field — strict decode rejects it.
func TestLoadConfigFromPaths_UnknownField(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, `
schema_version = "1.0.0"
unknown_field = true
`)

	_, err := LoadConfigFromPaths(userPath, "")
	if err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error should mention 'unknown'; got: %v", err)
	}
}

// Test 8: unknown nested field — strict decode rejects it.
func TestLoadConfigFromPaths_UnknownNestedField(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, `
[runtime]
codex_idle_timeout_ms = 1000
unknown_runtime_field = "test"
`)

	_, err := LoadConfigFromPaths(userPath, "")
	if err == nil {
		t.Fatal("expected error for unknown nested field, got nil")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error should mention 'unknown'; got: %v", err)
	}
}

// Test 9: empty file — yields empty Config without error.
func TestLoadConfigFromPaths_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, "")

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error for empty file: %v", err)
	}
	if cfg.SchemaVersion != nil {
		t.Errorf("expected nil SchemaVersion, got %q", *cfg.SchemaVersion)
	}
	if cfg.Runtime.CodexIdleTimeoutMS != nil {
		t.Errorf("expected nil CodexIdleTimeoutMS, got %d", *cfg.Runtime.CodexIdleTimeoutMS)
	}
}

// Test 10: default user path respects XDG_CONFIG_HOME vs HOME.
func TestDefaultUserConfigPath(t *testing.T) {
	t.Run("XDG_CONFIG_HOME_set", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/custom/xdg")
		got := defaultUserConfigPath()
		want := "/custom/xdg/kilroy/config.toml"
		if got != want {
			t.Errorf("defaultUserConfigPath() = %q, want %q", got, want)
		}
	})

	t.Run("XDG_CONFIG_HOME_empty_falls_back_to_HOME", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		home := os.Getenv("HOME")
		got := defaultUserConfigPath()
		want := filepath.Join(home, ".config", "kilroy", "config.toml")
		if got != want {
			t.Errorf("defaultUserConfigPath() = %q, want %q", got, want)
		}
	})
}

// Test 11: all RuntimeConfig fields can be loaded.
func TestLoadConfigFromPaths_AllRuntimeFields(t *testing.T) {
	content := `
[runtime]
codex_idle_timeout_ms = 1001
codex_total_timeout_ms = 2002
codex_kill_grace_ms = 3003
codex_timeout_max_retries = 4004
codergen_heartbeat_interval_ms = 5005
codex_state_db_max_retries = 6006
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, content)

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 1001 {
		t.Errorf("CodexIdleTimeoutMS = %d, want 1001", got)
	}
	if got := derefInt(cfg.Runtime.CodexTotalTimeoutMS); got != 2002 {
		t.Errorf("CodexTotalTimeoutMS = %d, want 2002", got)
	}
	if got := derefInt(cfg.Runtime.CodexKillGraceMS); got != 3003 {
		t.Errorf("CodexKillGraceMS = %d, want 3003", got)
	}
	if got := derefInt(cfg.Runtime.CodexTimeoutMaxRetries); got != 4004 {
		t.Errorf("CodexTimeoutMaxRetries = %d, want 4004", got)
	}
	if got := derefInt(cfg.Runtime.CodergenHeartbeatIntervalMS); got != 5005 {
		t.Errorf("CodergenHeartbeatIntervalMS = %d, want 5005", got)
	}
	if got := derefInt(cfg.Runtime.CodexStateDBMaxRetries); got != 6006 {
		t.Errorf("CodexStateDBMaxRetries = %d, want 6006", got)
	}
}

// Test 12: CxDB.UI.Command slice is loaded correctly.
func TestLoadConfigFromPaths_CxDBUICommand(t *testing.T) {
	content := `
[cxdb.ui]
command = ["open", "-a", "Safari"]
url = "http://localhost:8080"
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "config.toml")
	writeTestFile(t, userPath, content)

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"open", "-a", "Safari"}
	if cfg.CxDB.UI.Command == nil {
		t.Fatal("CxDB.UI.Command is nil, want non-nil")
	}
	if !reflect.DeepEqual(*cfg.CxDB.UI.Command, want) {
		t.Errorf("CxDB.UI.Command = %v, want %v", *cfg.CxDB.UI.Command, want)
	}
	if got := derefString(cfg.CxDB.UI.URL); got != "http://localhost:8080" {
		t.Errorf("CxDB.UI.URL = %q, want http://localhost:8080", got)
	}
}

// Test 13: CxDB.UI.Command is replaced (not merged) on project override.
func TestLoadConfigFromPaths_CxDBUICommand_ProjectReplaces(t *testing.T) {
	userContent := `
[cxdb.ui]
command = ["firefox"]
url = "http://user-url:9000"
`
	projectContent := `
[cxdb.ui]
command = ["chrome", "--app"]
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Project command should replace user command entirely
	want := []string{"chrome", "--app"}
	if cfg.CxDB.UI.Command == nil {
		t.Fatal("CxDB.UI.Command is nil, want non-nil")
	}
	if !reflect.DeepEqual(*cfg.CxDB.UI.Command, want) {
		t.Errorf("CxDB.UI.Command = %v, want %v", *cfg.CxDB.UI.Command, want)
	}
	// URL from user should remain since project didn't set it
	if got := derefString(cfg.CxDB.UI.URL); got != "http://user-url:9000" {
		t.Errorf("CxDB.UI.URL = %q, want http://user-url:9000", got)
	}
}

// Test 14: Project can override user int with explicit 0.
func TestLoadConfigFromPaths_ExplicitZeroInt(t *testing.T) {
	userContent := `
[runtime]
codex_idle_timeout_ms = 1000
`
	projectContent := `
[runtime]
codex_idle_timeout_ms = 0
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Project's explicit 0 should override user's 1000
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 0 {
		t.Errorf("CodexIdleTimeoutMS = %d, want 0 (project override)", got)
	}
}

// Test 15: Project can override user string with explicit empty string.
func TestLoadConfigFromPaths_ExplicitEmptyString(t *testing.T) {
	userContent := `
[cxdb.ui]
url = "http://user-url:9000"
`
	projectContent := `
[cxdb.ui]
url = ""
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Project's explicit empty string should override user's URL
	if got := derefString(cfg.CxDB.UI.URL); got != "" {
		t.Errorf("CxDB.UI.URL = %q, want empty string (project override)", got)
	}
}

// Test 16: Project can replace user command list with explicit empty list.
func TestLoadConfigFromPaths_ExplicitEmptyList(t *testing.T) {
	userContent := `
[cxdb.ui]
command = ["firefox", "--private"]
url = "http://user-url:9000"
`
	projectContent := `
[cxdb.ui]
command = []
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Project's explicit empty list should replace user's command
	if cfg.CxDB.UI.Command == nil {
		t.Fatal("CxDB.UI.Command is nil, want non-nil empty slice")
	}
	if len(*cfg.CxDB.UI.Command) != 0 {
		t.Errorf("CxDB.UI.Command = %v, want empty slice (project override)", *cfg.CxDB.UI.Command)
	}
	// URL from user should remain since project didn't set it
	if got := derefString(cfg.CxDB.UI.URL); got != "http://user-url:9000" {
		t.Errorf("CxDB.UI.URL = %q, want http://user-url:9000", got)
	}
}

// Test 17: Omitted fields don't override user values (nil check behavior).
func TestLoadConfigFromPaths_OmittedFieldsPreserveUserValues(t *testing.T) {
	userContent := `
schema_version = "1.0.0"

[runtime]
codex_idle_timeout_ms = 1000
codex_total_timeout_ms = 2000

[cxdb.ui]
url = "http://user-url:9000"
command = ["firefox"]
`
	// Project config that only sets one field
	projectContent := `
[runtime]
codex_kill_grace_ms = 5000
`
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// User values should be preserved where project didn't specify
	if got := derefString(cfg.SchemaVersion); got != "1.0.0" {
		t.Errorf("SchemaVersion = %q, want 1.0.0", got)
	}
	if got := derefInt(cfg.Runtime.CodexIdleTimeoutMS); got != 1000 {
		t.Errorf("CodexIdleTimeoutMS = %d, want 1000", got)
	}
	if got := derefInt(cfg.Runtime.CodexTotalTimeoutMS); got != 2000 {
		t.Errorf("CodexTotalTimeoutMS = %d, want 2000", got)
	}
	// Project value should be set
	if got := derefInt(cfg.Runtime.CodexKillGraceMS); got != 5000 {
		t.Errorf("CodexKillGraceMS = %d, want 5000", got)
	}
	// User values preserved
	if got := derefString(cfg.CxDB.UI.URL); got != "http://user-url:9000" {
		t.Errorf("CxDB.UI.URL = %q, want http://user-url:9000", got)
	}
}

// derefInt returns the value of an *int or 0 if nil.
func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// derefString returns the value of a *string or "" if nil.
func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
