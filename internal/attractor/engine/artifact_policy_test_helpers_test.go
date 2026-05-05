package engine

import "strings"

func validMinimalRunConfigForTest() *RunConfigFile {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)
	cfg.Version = 1
	cfg.Repo.Path = "/tmp/repo"
	cfg.CXDB.BinaryAddr = "127.0.0.1:9009"
	cfg.CXDB.HTTPBaseURL = "http://127.0.0.1:9010"
	return cfg
}

func containsEnv(env []string, item string) bool {
	for _, v := range env {
		if v == item {
			return true
		}
	}
	return false
}

func findEnvPrefix(env []string, prefix string) string {
	for _, v := range env {
		if strings.HasPrefix(v, prefix) {
			return v
		}
	}
	return ""
}
