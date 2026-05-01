package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func writeHostsYAML(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "hosts.yml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write hosts.yml: %v", err)
	}
	return path
}

func TestGHDetector_MissingFile_NoEnv(t *testing.T) {
	tmp := t.TempDir()
	old := ghHostsPath
	ghHostsPath = filepath.Join(tmp, "hosts.yml") // doesn't exist
	t.Cleanup(func() { ghHostsPath = old })

	os.Unsetenv("GH_TOKEN")
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].State != StateMissing {
		t.Errorf("expected state=missing, got %s", entries[0].State)
	}
}

func TestGHDetector_ValidUser_KeychainTrue(t *testing.T) {
	tmp := t.TempDir()
	p := writeHostsYAML(t, tmp, `
github.com:
  user: mleaverton
  users:
    mleaverton:
    mattleaverton:
`)
	old := ghHostsPath
	ghHostsPath = p
	t.Cleanup(func() { ghHostsPath = old })

	oldProbe := keychainProbeGH
	keychainProbeGH = func(service, account string) bool { return true }
	t.Cleanup(func() { keychainProbeGH = oldProbe })

	os.Unsetenv("GH_TOKEN")
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.State != StateOK {
		t.Errorf("expected state=ok, got %s", e.State)
	}
	if e.Identity.User != "mleaverton" {
		t.Errorf("expected user=mleaverton, got %s", e.Identity.User)
	}
}

func TestGHDetector_NoUserField_Ambiguous(t *testing.T) {
	tmp := t.TempDir()
	p := writeHostsYAML(t, tmp, `
github.com:
  users:
    mleaverton:
`)
	old := ghHostsPath
	ghHostsPath = p
	t.Cleanup(func() { ghHostsPath = old })

	os.Unsetenv("GH_TOKEN")
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].State != StateAmbiguous {
		t.Errorf("expected state=ambiguous, got %s", entries[0].State)
	}
	if entries[0].Remediation == "" {
		t.Error("expected non-empty remediation")
	}
}

func TestGHDetector_MultiUser_ProfilesEnumerated(t *testing.T) {
	tmp := t.TempDir()
	p := writeHostsYAML(t, tmp, `
github.com:
  user: mleaverton
  users:
    mleaverton:
    mattleaverton:
    botuser:
`)
	old := ghHostsPath
	ghHostsPath = p
	t.Cleanup(func() { ghHostsPath = old })

	oldProbe := keychainProbeGH
	keychainProbeGH = func(service, account string) bool { return true }
	t.Cleanup(func() { keychainProbeGH = oldProbe })

	os.Unsetenv("GH_TOKEN")
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if len(e.Profiles) != 3 {
		t.Errorf("expected 3 profiles, got %d", len(e.Profiles))
	}

	activeCount := 0
	activeFound := false
	for _, p := range e.Profiles {
		if p.Active {
			activeCount++
			if p.Name == "mleaverton" {
				activeFound = true
			}
		}
	}
	if activeCount != 1 {
		t.Errorf("expected exactly 1 active profile, got %d", activeCount)
	}
	if !activeFound {
		t.Error("expected mleaverton to be active profile")
	}
}

func TestGHDetector_GHTokenEnv_TwoEntries(t *testing.T) {
	tmp := t.TempDir()
	p := writeHostsYAML(t, tmp, `
github.com:
  user: mleaverton
  users:
    mleaverton:
`)
	old := ghHostsPath
	ghHostsPath = p
	t.Cleanup(func() { ghHostsPath = old })

	oldProbe := keychainProbeGH
	keychainProbeGH = func(service, account string) bool { return true }
	t.Cleanup(func() { keychainProbeGH = oldProbe })

	os.Setenv("GH_TOKEN", "ghp_fakefakefake")
	t.Cleanup(func() { os.Unsetenv("GH_TOKEN") })
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	envEntry := entries[0]
	cliEntry := entries[1]

	if envEntry.Kind != KindEnvVar {
		t.Errorf("first entry should be env_var, got %s", envEntry.Kind)
	}
	if envEntry.State != StateOK {
		t.Errorf("env entry should be state=ok, got %s", envEntry.State)
	}
	if len(envEntry.Shadows) == 0 || envEntry.Shadows[0] != ghCLIEntryID {
		t.Errorf("env entry should shadow cli entry, got %v", envEntry.Shadows)
	}

	if cliEntry.Kind != KindCLIOAuth {
		t.Errorf("second entry should be cli_oauth, got %s", cliEntry.Kind)
	}
	if len(cliEntry.ShadowedBy) == 0 {
		t.Error("cli entry should have ShadowedBy set")
	}
}

func TestGHDetector_ValidUser_KeychainFalse_Expired(t *testing.T) {
	tmp := t.TempDir()
	p := writeHostsYAML(t, tmp, `
github.com:
  user: mleaverton
  users:
    mleaverton:
`)
	old := ghHostsPath
	ghHostsPath = p
	t.Cleanup(func() { ghHostsPath = old })

	oldProbe := keychainProbeGH
	keychainProbeGH = func(service, account string) bool { return false }
	t.Cleanup(func() { keychainProbeGH = oldProbe })

	os.Unsetenv("GH_TOKEN")
	os.Unsetenv("GITHUB_TOKEN")

	d := NewGHDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].State != StateExpired {
		t.Errorf("expected state=expired, got %s", entries[0].State)
	}
}
