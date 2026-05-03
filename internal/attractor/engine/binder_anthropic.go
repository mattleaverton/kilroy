// Credential binders for the anthropic axis: claude_cli and anthropic_sdk.
// Registered at package init so the dispatch table in credential_binder.go
// picks them up automatically.
//
// The load-bearing case is BindClaudeCLI: it MUST scrub ANTHROPIC_API_KEY
// from the child process environment so that the Claude CLI honours the
// logged-in subscription session (via ~/.claude/) rather than silently
// falling back to the API key — which would cause silent wrong-billing.
//
// See docs/plans/2026-05-02-auth-class-resolver-integration.md §5 and §6.
package engine

import (
	"fmt"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

func init() {
	RegisterBinder("claude_cli", BindClaudeCLI)
	RegisterBinder("anthropic_sdk", BindAnthropicSDK)
}

// BindClaudeCLI materialises a cli_session credential for the claude_cli
// driver. The critical action is scrubbing ANTHROPIC_API_KEY from the child
// environment — without this the Claude CLI sees the env key and uses it
// instead of the logged-in session, causing silent wrong-billing.
//
// Accepts only a Credential whose source kind is SourceCLISession and whose
// CLITool is "claude". Any other combination is a resolver bug; this function
// fails loudly so the problem surfaces immediately rather than silently
// misbilling.
func BindClaudeCLI(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	if cred.Snapshot.Source.Kind != binding.SourceCLISession {
		return BindResult{}, fmt.Errorf(
			"BindClaudeCLI: expected cli_session credential, got %q — "+
				"claude_cli driver only accepts a cli_session source; "+
				"check resolver configuration",
			cred.Snapshot.Source.Kind,
		)
	}
	if cred.CLITool != "claude" {
		return BindResult{}, fmt.Errorf(
			"BindClaudeCLI: expected CLITool %q, got %q — "+
				"claude_cli driver only accepts a credential bound to the claude tool",
			"claude", cred.CLITool,
		)
	}

	// Scrub ANTHROPIC_API_KEY so the CLI uses ~/.claude/ session, not the key.
	// This is the load-bearing materialization step (plan §5, §7 acceptance criterion 6).
	return BindResult{
		EnvScrub:   []string{"ANTHROPIC_API_KEY"},
		SourceKind: string(binding.SourceCLISession),
		SourceName: "claude",
	}, nil
}

// BindAnthropicSDK materialises an env_var credential for the anthropic_sdk
// driver. The credential value is passed through as SDKArg so the SDK
// constructor can use it directly without further os.Getenv calls.
func BindAnthropicSDK(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	if cred.Snapshot.Source.Kind != binding.SourceEnvVar {
		return BindResult{}, fmt.Errorf(
			"BindAnthropicSDK: expected env_var credential, got %q — "+
				"anthropic_sdk driver only accepts an env_var source; "+
				"check resolver configuration",
			cred.Snapshot.Source.Kind,
		)
	}
	if cred.Value == "" {
		return BindResult{}, fmt.Errorf(
			"BindAnthropicSDK: credential value is empty for env_var %q — "+
				"the environment variable must be set to a non-empty value",
			cred.Snapshot.Source.Name,
		)
	}

	return BindResult{
		SDKArg:     cred.Value,
		SourceKind: string(binding.SourceEnvVar),
		SourceName: cred.Snapshot.Source.Name,
	}, nil
}
