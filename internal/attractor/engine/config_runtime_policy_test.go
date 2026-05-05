package engine

import "testing"

func TestRuntimePolicy_DefaultsAndValidation(t *testing.T) {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)

	if cfg.RuntimePolicy.StallTimeoutMS == nil || *cfg.RuntimePolicy.StallTimeoutMS != 600000 {
		t.Fatalf("expected default stall_timeout_ms=600000")
	}
	if cfg.RuntimePolicy.StallCheckIntervalMS == nil || *cfg.RuntimePolicy.StallCheckIntervalMS != 5000 {
		t.Fatalf("expected default stall_check_interval_ms=5000")
	}
	if cfg.RuntimePolicy.MaxLLMRetries == nil || *cfg.RuntimePolicy.MaxLLMRetries != 6 {
		t.Fatalf("expected default max_llm_retries=6")
	}

	cfg.Version = 1
	cfg.Repo.Path = "/tmp/repo"
	cfg.CXDB.BinaryAddr = "127.0.0.1:1"
	cfg.CXDB.HTTPBaseURL = "http://127.0.0.1:1"

	zero := 0
	cfg.RuntimePolicy.MaxLLMRetries = &zero
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("max_llm_retries=0 should be valid: %v", err)
	}

	neg := -1
	cfg.RuntimePolicy.MaxLLMRetries = &neg
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected validation error for negative max_llm_retries")
	}
}

func TestApplyConfigDefaults_ArtifactPolicyCheckpointExcludeGlobs(t *testing.T) {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)

	if len(cfg.ArtifactPolicy.Checkpoint.ExcludeGlobs) == 0 {
		t.Fatal("expected non-empty artifact_policy.checkpoint.exclude_globs defaults")
	}
	if len(cfg.Git.CheckpointExcludeGlobs) != 0 {
		t.Fatal("legacy git.checkpoint_exclude_globs must remain empty")
	}
}

func TestApplyConfigDefaults_ArtifactPolicyInitialized(t *testing.T) {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)

	if cfg.ArtifactPolicy.Env.ManagedRoots == nil {
		t.Fatal("artifact_policy.env.managed_roots must be initialized")
	}
	if cfg.ArtifactPolicy.Env.Overrides == nil {
		t.Fatal("artifact_policy.env.overrides must be initialized")
	}
}

func TestApplyConfigDefaults_ArtifactPolicyProfilesDefault(t *testing.T) {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)

	if len(cfg.ArtifactPolicy.Profiles) == 0 {
		t.Fatal("artifact_policy.profiles must default to a non-empty explicit list")
	}
}

func TestValidateConfig_ArtifactPolicyRejectsUnknownProfile(t *testing.T) {
	cfg := validMinimalRunConfigForTest()
	cfg.ArtifactPolicy.Profiles = []string{"fortran77"}

	err := validateConfig(cfg)
	if err == nil {
		t.Fatal("expected validation error for unknown artifact_policy profile")
	}
}

func TestValidateConfig_ArtifactPolicyRejectsLegacyGitCheckpointExcludes(t *testing.T) {
	cfg := validMinimalRunConfigForTest()
	cfg.Git.CheckpointExcludeGlobs = []string{"**/tmp-build/**"}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected legacy git.checkpoint_exclude_globs validation error")
	}
}

func TestApplyConfigDefaults_LegacyGitCheckpointExcludesNotAutoPopulated(t *testing.T) {
	cfg := &RunConfigFile{}
	applyConfigDefaults(cfg)
	if len(cfg.Git.CheckpointExcludeGlobs) != 0 {
		t.Fatal("git.checkpoint_exclude_globs must remain empty after migration to artifact_policy")
	}
}
