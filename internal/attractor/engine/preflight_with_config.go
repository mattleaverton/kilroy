package engine

import (
	"context"
	"path/filepath"
	"strings"
)

// PreflightResult contains metadata emitted by validate-only startup checks.
// (`PreflightReportPath` keeps the legacy field name for cmd/kilroy back-compat
// but now points at prelaunch_validation.json — the only artifact written
// since the old runProviderCLIPreflight machinery was removed.)
type PreflightResult struct {
	RunID               string
	LogsRoot            string
	PreflightReportPath string
	Warnings            []string
	CXDBUIURL           string
}

// PreflightWithConfig runs all RunWithConfig prechecks and exits before
// pipeline startup. The check is now ValidatePreLaunch — package integrity,
// class resolution, auth presence, CLI binary capability, and required
// secrets. No LLM calls.
func PreflightWithConfig(ctx context.Context, dotSource []byte, cfg *RunConfigFile, overrides RunOptions) (*PreflightResult, error) {
	boot, err := bootstrapRunWithConfig(ctx, dotSource, cfg, overrides)
	if err != nil {
		return nil, err
	}
	defer closeRunBootstrapResources(boot)

	cxdbUI := ""
	if boot.Startup != nil {
		cxdbUI = strings.TrimSpace(boot.Startup.UIURL)
	}

	return &PreflightResult{
		RunID:               boot.Options.RunID,
		LogsRoot:            boot.Options.LogsRoot,
		PreflightReportPath: filepath.Join(boot.Options.LogsRoot, "prelaunch_validation.json"),
		Warnings:            append([]string{}, boot.Warnings...),
		CXDBUIURL:           cxdbUI,
	}, nil
}
