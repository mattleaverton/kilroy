// Per-driver credential binder for OpenAI-backed drivers.
// Contributes binders for codex_cli (isolated auth.json pattern) and
// openai_sdk (SDK argument passthrough). Both register at init().
// See docs/plans/2026-05-02-auth-class-resolver-integration.md §5.

package engine

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// BindCodexCLI materialises an OpenAI credential for the Codex CLI driver.
//
// For SourceEnvVar: writes an isolated auth.json under <stageDir>/.codex/ so
// the run never mutates ~/.codex/, and sets CODEX_HOME to that directory.
// This mirrors the PrepareSession logic in templates/codex.go, shifted into
// the binder layer (A4 cleanup; A6 will rewire callers).
//
// For SourceCLISession: the user is logged in to Codex; no file materialization
// or env override is needed — Codex uses its own session from ~/.codex/.
func BindCodexCLI(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	switch cred.Snapshot.Source.Kind {
	case binding.SourceEnvVar:
		if cred.Value == "" {
			return BindResult{}, fmt.Errorf("codex_cli: env_var credential has empty value (source %q)", cred.Snapshot.Source.Name)
		}
		codexHome := filepath.Join(stageDir, ".codex")
		authPath := filepath.Join(codexHome, "auth.json")
		authPayload := map[string]string{
			"OPENAI_API_KEY": cred.Value,
			"auth_mode":      "apikey",
		}
		data, err := json.Marshal(authPayload)
		if err != nil {
			return BindResult{}, fmt.Errorf("codex_cli: marshal auth.json: %w", err)
		}
		return BindResult{
			FilesToWrite: map[string][]byte{authPath: data},
			EnvSet:       map[string]string{"CODEX_HOME": codexHome},
			SourceKind:   string(binding.SourceEnvVar),
			SourceName:   cred.Snapshot.Source.Name,
		}, nil

	case binding.SourceCLISession:
		// User is logged in to Codex; rely on the session at ~/.codex/.
		// No file writes, no env overrides needed.
		return BindResult{
			SourceKind: "cli_session",
			SourceName: cred.CLITool,
		}, nil

	default:
		return BindResult{}, fmt.Errorf("codex_cli: unsupported source kind %q", cred.Snapshot.Source.Kind)
	}
}

// BindOpenAISDK materialises an OpenAI credential for the openai_sdk driver.
// Only SourceEnvVar is supported; the value is passed directly as the SDK
// constructor argument (SDKArg).
func BindOpenAISDK(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	if cred.Snapshot.Source.Kind != binding.SourceEnvVar {
		return BindResult{}, fmt.Errorf("openai_sdk: unsupported source kind %q; only env_var is supported", cred.Snapshot.Source.Kind)
	}
	if cred.Value == "" {
		return BindResult{}, fmt.Errorf("openai_sdk: credential value is empty (source %q)", cred.Snapshot.Source.Name)
	}
	return BindResult{
		SDKArg:     cred.Value,
		SourceKind: "env_var",
		SourceName: cred.Snapshot.Source.Name,
	}, nil
}

func init() {
	RegisterBinder("codex_cli", BindCodexCLI)
	RegisterBinder("openai_sdk", BindOpenAISDK)
}
