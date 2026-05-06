package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/projectroot"
)

type RuntimeCLIFlags struct {
	CodexIdleTimeoutMS          *int
	CodexTotalTimeoutMS         *int
	CodexKillGraceMS            *int
	CodexTimeoutMaxRetries      *int
	CodergenHeartbeatIntervalMS *int
	CodexStateDBMaxRetries      *int
}

type CxDBUICLIFlags struct {
	URL *string
}

type CxDBCLIFlags struct {
	UI CxDBUICLIFlags
}

type ToolsCLIFlags struct {
	ClaudePath *string
}

type CLIFlags struct {
	Runtime RuntimeCLIFlags
	CxDB    CxDBCLIFlags
	Tools   ToolsCLIFlags
}

type LayeredConfig struct {
	Config            Config
	ProjectRoot       string
	ProjectRootSource string
	Sources           map[string]string
}

func ResolveProjectRoot(workDir string) (root string, source string, err error) {
	return projectroot.Find(workDir)
}

func LoadLayered(workDir string, flags CLIFlags) (*LayeredConfig, error) {
	root, rootSource, err := ResolveProjectRoot(workDir)
	if err != nil {
		return nil, err
	}

	cfg := defaultConfig()
	sources := map[string]string{
		"runtime.codergen_heartbeat_interval_ms": "default",
		"runtime.codex_idle_timeout_ms":          "default",
		"runtime.codex_total_timeout_ms":         "default",
		"runtime.codex_kill_grace_ms":            "default",
		"runtime.codex_timeout_max_retries":      "default",
	}

	userPath := defaultUserConfigPath()
	if fileExists(userPath) {
		userCfg, err := loadTOMLFile(userPath)
		if err != nil {
			return nil, err
		}
		mergeConfig(&cfg, userCfg)
		markConfigSources(sources, userCfg, "user:"+userPath)
	}

	if strings.TrimSpace(root) != "" {
		projectPath := filepath.Join(root, ".kilroy", "config.toml")
		if fileExists(projectPath) {
			projectCfg, err := loadTOMLFile(projectPath)
			if err != nil {
				return nil, err
			}
			mergeConfig(&cfg, projectCfg)
			markConfigSources(sources, projectCfg, "project:"+projectPath)
		}
	}

	if err := applyEnv(&cfg, sources); err != nil {
		return nil, err
	}
	ApplyCLIFlags(&cfg, flags)
	markCLISources(sources, flags)

	return &LayeredConfig{
		Config:            cfg,
		ProjectRoot:       root,
		ProjectRootSource: rootSource,
		Sources:           sources,
	}, nil
}

func defaultConfig() Config {
	return Config{
		Runtime: RuntimeConfig{
			CodergenHeartbeatIntervalMS: intPtrValue(5000),
			CodexIdleTimeoutMS:          intPtrValue(300000),
			CodexTotalTimeoutMS:         intPtrValue(3600000),
			CodexKillGraceMS:            intPtrValue(5000),
			CodexTimeoutMaxRetries:      intPtrValue(3),
		},
	}
}

func ApplyCLIFlags(cfg *Config, flags CLIFlags) {
	if cfg == nil {
		return
	}
	if flags.Runtime.CodexIdleTimeoutMS != nil {
		cfg.Runtime.CodexIdleTimeoutMS = flags.Runtime.CodexIdleTimeoutMS
	}
	if flags.Runtime.CodexTotalTimeoutMS != nil {
		cfg.Runtime.CodexTotalTimeoutMS = flags.Runtime.CodexTotalTimeoutMS
	}
	if flags.Runtime.CodexKillGraceMS != nil {
		cfg.Runtime.CodexKillGraceMS = flags.Runtime.CodexKillGraceMS
	}
	if flags.Runtime.CodexTimeoutMaxRetries != nil {
		cfg.Runtime.CodexTimeoutMaxRetries = flags.Runtime.CodexTimeoutMaxRetries
	}
	if flags.Runtime.CodergenHeartbeatIntervalMS != nil {
		cfg.Runtime.CodergenHeartbeatIntervalMS = flags.Runtime.CodergenHeartbeatIntervalMS
	}
	if flags.Runtime.CodexStateDBMaxRetries != nil {
		cfg.Runtime.CodexStateDBMaxRetries = flags.Runtime.CodexStateDBMaxRetries
	}
	if flags.CxDB.UI.URL != nil {
		cfg.CxDB.UI.URL = flags.CxDB.UI.URL
	}
	if flags.Tools.ClaudePath != nil {
		cfg.Tools.ClaudePath = flags.Tools.ClaudePath
	}
}

func applyEnv(cfg *Config, sources map[string]string) error {
	intEnv := []struct {
		env   string
		key   string
		field **int
	}{
		{"KILROY_CODEX_IDLE_TIMEOUT_MS", "runtime.codex_idle_timeout_ms", &cfg.Runtime.CodexIdleTimeoutMS},
		{"KILROY_CODEX_TOTAL_TIMEOUT_MS", "runtime.codex_total_timeout_ms", &cfg.Runtime.CodexTotalTimeoutMS},
		{"KILROY_CODEX_KILL_GRACE_MS", "runtime.codex_kill_grace_ms", &cfg.Runtime.CodexKillGraceMS},
		{"KILROY_CODEX_TIMEOUT_MAX_RETRIES", "runtime.codex_timeout_max_retries", &cfg.Runtime.CodexTimeoutMaxRetries},
		{"KILROY_CODERGEN_HEARTBEAT_INTERVAL_MS", "runtime.codergen_heartbeat_interval_ms", &cfg.Runtime.CodergenHeartbeatIntervalMS},
		{"KILROY_CODEX_STATE_DB_MAX_RETRIES", "runtime.codex_state_db_max_retries", &cfg.Runtime.CodexStateDBMaxRetries},
	}
	for _, item := range intEnv {
		raw, ok := os.LookupEnv(item.env)
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		v, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("config: env %s: expected integer: %w", item.env, err)
		}
		*item.field = intPtrValue(v)
		sources[item.key] = "env:" + item.env
	}

	if raw, ok := os.LookupEnv("KILROY_CXDB_UI_URL"); ok && strings.TrimSpace(raw) != "" {
		cfg.CxDB.UI.URL = strPtrValue(strings.TrimSpace(raw))
		sources["cxdb.ui.url"] = "env:KILROY_CXDB_UI_URL"
	}
	if raw, ok := os.LookupEnv("KILROY_TOOLS_CLAUDE_PATH"); ok && strings.TrimSpace(raw) != "" {
		cfg.Tools.ClaudePath = strPtrValue(strings.TrimSpace(raw))
		sources["tools.claude_path"] = "env:KILROY_TOOLS_CLAUDE_PATH"
	}
	return nil
}

func markConfigSources(sources map[string]string, cfg Config, source string) {
	if cfg.SchemaVersion != nil {
		sources["schema_version"] = source
	}
	if cfg.Runtime.CodexIdleTimeoutMS != nil {
		sources["runtime.codex_idle_timeout_ms"] = source
	}
	if cfg.Runtime.CodexTotalTimeoutMS != nil {
		sources["runtime.codex_total_timeout_ms"] = source
	}
	if cfg.Runtime.CodexKillGraceMS != nil {
		sources["runtime.codex_kill_grace_ms"] = source
	}
	if cfg.Runtime.CodexTimeoutMaxRetries != nil {
		sources["runtime.codex_timeout_max_retries"] = source
	}
	if cfg.Runtime.CodergenHeartbeatIntervalMS != nil {
		sources["runtime.codergen_heartbeat_interval_ms"] = source
	}
	if cfg.Runtime.CodexStateDBMaxRetries != nil {
		sources["runtime.codex_state_db_max_retries"] = source
	}
	if cfg.CxDB.UI.URL != nil {
		sources["cxdb.ui.url"] = source
	}
	if cfg.CxDB.UI.Command != nil {
		sources["cxdb.ui.command"] = source
	}
	if cfg.Tools.ClaudePath != nil {
		sources["tools.claude_path"] = source
	}
}

func markCLISources(sources map[string]string, flags CLIFlags) {
	if flags.Runtime.CodexIdleTimeoutMS != nil {
		sources["runtime.codex_idle_timeout_ms"] = "cli"
	}
	if flags.Runtime.CodexTotalTimeoutMS != nil {
		sources["runtime.codex_total_timeout_ms"] = "cli"
	}
	if flags.Runtime.CodexKillGraceMS != nil {
		sources["runtime.codex_kill_grace_ms"] = "cli"
	}
	if flags.Runtime.CodexTimeoutMaxRetries != nil {
		sources["runtime.codex_timeout_max_retries"] = "cli"
	}
	if flags.Runtime.CodergenHeartbeatIntervalMS != nil {
		sources["runtime.codergen_heartbeat_interval_ms"] = "cli"
	}
	if flags.Runtime.CodexStateDBMaxRetries != nil {
		sources["runtime.codex_state_db_max_retries"] = "cli"
	}
	if flags.CxDB.UI.URL != nil {
		sources["cxdb.ui.url"] = "cli"
	}
	if flags.Tools.ClaudePath != nil {
		sources["tools.claude_path"] = "cli"
	}
}

func intPtrValue(v int) *int {
	return &v
}

func strPtrValue(v string) *string {
	return &v
}
