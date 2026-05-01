//go:build !darwin

// Non-darwin builds keep the per-detector stubs (returning false). Real
// keychain integration on Linux/Windows is a follow-up: Linux libsecret
// via `secret-tool`, Windows Credential Manager via `cmdkey`/Win32 API.

package auth
