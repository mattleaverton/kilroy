// Per-driver credential binder for Google drivers (google_sdk, gemini_cli).
//
// Google supports three equivalent API key env var names:
//   - GOOGLE_API_KEY
//   - GEMINI_API_KEY
//   - GOOGLE_GENERATIVE_AI_API_KEY
//
// The resolver picks whichever name the user has configured first in their
// chain's source list. Both binders record the original source name in
// SourceName so resolution.json captures *which* of the three names was
// actually used — multi-name observability is load-bearing for debugging.
//
// gemini_cli additionally supports an OAuth login path: when the chain
// selects a cli_session source (tool=gemini), the CLI reads
// ~/.gemini/oauth_creds.json; no env writes are made and no files are
// written by the binder.
package engine

import (
	"fmt"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// BindGoogleSDK materializes a credential for the google_sdk driver.
// Only env_var sources are valid; the SDK path has no OAuth equivalent.
// SourceName records which env var name was resolved (GOOGLE_API_KEY,
// GEMINI_API_KEY, or GOOGLE_GENERATIVE_AI_API_KEY) so resolution.json
// captures the exact name used.
func BindGoogleSDK(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	if snap.Source.Kind != binding.SourceEnvVar {
		return BindResult{}, fmt.Errorf(
			"google_sdk: unsupported source kind %q; expected %q",
			snap.Source.Kind, binding.SourceEnvVar,
		)
	}
	if cred.Value == "" {
		return BindResult{}, fmt.Errorf(
			"google_sdk: credential value is empty for source %q",
			snap.Source.Name,
		)
	}
	return BindResult{
		SDKArg:     cred.Value,
		SourceKind: string(binding.SourceEnvVar),
		SourceName: snap.Source.Name,
	}, nil
}

// BindGeminiCLI materializes a credential for the gemini_cli driver.
//
// For env_var sources: writes GEMINI_API_KEY (canonical name accepted by the
// gemini CLI) regardless of which equivalent env var name the user has set.
// SourceName records the original name so observability captures the real
// source.
//
// For cli_session sources (tool=gemini): no env writes, no files. The CLI
// reads ~/.gemini/oauth_creds.json on its own; the binder's job is simply
// to record the source identity.
func BindGeminiCLI(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	switch snap.Source.Kind {
	case binding.SourceEnvVar:
		return BindResult{
			EnvSet:     map[string]string{"GEMINI_API_KEY": cred.Value},
			SourceKind: string(binding.SourceEnvVar),
			SourceName: snap.Source.Name,
		}, nil

	case binding.SourceCLISession:
		return BindResult{
			SourceKind: string(binding.SourceCLISession),
			SourceName: "gemini",
		}, nil

	default:
		return BindResult{}, fmt.Errorf(
			"gemini_cli: unsupported source kind %q; expected %q or %q",
			snap.Source.Kind, binding.SourceEnvVar, binding.SourceCLISession,
		)
	}
}

func init() {
	RegisterBinder("gemini_cli", BindGeminiCLI)
	RegisterBinder("google_sdk", BindGoogleSDK)
}
