//go:build darwin

// macOS keychain probe via `security find-generic-password`. We only check
// presence (exit code), never read the value. The detection layer must
// never expose credential bytes — agents and external callers receive
// only state and identity.

package auth

import (
	"os/exec"
)

// macOSKeychainProbe returns true if a generic-password entry exists in the
// user's login keychain for the given service+account. False on absence or
// any error (including security tool unavailable). Never logs the value.
func macOSKeychainProbe(service, account string) bool {
	if service == "" || account == "" {
		return false
	}
	cmd := exec.Command("security", "find-generic-password",
		"-s", service,
		"-a", account)
	// Discard all output; only the exit status matters.
	_ = cmd.Run()
	return cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == 0
}

func init() {
	// Wire the real probe into per-tool stubs that detectors left swappable.
	keychainProbeClaude = macOSKeychainProbe
	keychainProbeGH = macOSKeychainProbe
	keychainProbeCursor = macOSKeychainProbe
}
