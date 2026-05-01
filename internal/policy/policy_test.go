package policy

import "testing"

// TestLoad_EmbeddedPolicyParsesAndValidates is the foundation smoke test:
// the embedded policy.toml shipped with kilroy must parse cleanly and pass
// validation on every build. If this test fails, the binary ships broken.
func TestLoad_EmbeddedPolicyParsesAndValidates(t *testing.T) {
	d, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	if d.SchemaVersion == "" {
		t.Errorf("schema_version is empty")
	}
	if d.PolicyVersion == "" {
		t.Errorf("policy_version is empty")
	}
	if len(d.Classes) == 0 {
		t.Errorf("no classes defined")
	}
	// hard_coding is the v2 baseline; it must always exist.
	hc, ok := d.Classes["hard_coding"]
	if !ok {
		t.Fatalf("class 'hard_coding' not defined; v2 baseline broken")
	}
	if len(hc.Chain) == 0 {
		t.Errorf("hard_coding has empty chain")
	}
}
