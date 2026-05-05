package testutil

import "testing"

func TestIntegrationEnabledDefaultsOff(t *testing.T) {
	t.Setenv(IntegrationEnv, "")

	if IntegrationEnabled() {
		t.Fatal("integration tests should be opt-in")
	}
}

func TestIntegrationEnabledAcceptsExplicitTruthValues(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", "yes", "on"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(IntegrationEnv, value)

			if !IntegrationEnabled() {
				t.Fatalf("integration tests should be enabled for %q", value)
			}
		})
	}
}

func TestIntegrationEnabledRejectsOtherValues(t *testing.T) {
	for _, value := range []string{"0", "false", "no", "unit"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(IntegrationEnv, value)

			if IntegrationEnabled() {
				t.Fatalf("integration tests should stay disabled for %q", value)
			}
		})
	}
}
