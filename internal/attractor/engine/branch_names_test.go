package engine

import "testing"

func TestBuildRunBranch_UsesNormalizedPrefix(t *testing.T) {
	got := buildRunBranch("attractor/run/", "r1")
	if got != "attractor/run/r1" {
		t.Fatalf("buildRunBranch = %q, want %q", got, "attractor/run/r1")
	}
}

func TestBuildParallelBranch_UsesSiblingParallelNamespace(t *testing.T) {
	got := buildParallelBranch("attractor/run/", "run-123", "Par Node", 1, "child/a")
	if got != "attractor/run/parallel/run-123/par-node/pass1/child-a" {
		t.Fatalf("buildParallelBranch = %q", got)
	}
}

func TestBuildParallelBranch_PassNumIncrementsInName(t *testing.T) {
	got1 := buildParallelBranch("attractor/run/", "run-123", "fanout", 1, "branch-a")
	got2 := buildParallelBranch("attractor/run/", "run-123", "fanout", 2, "branch-a")
	if got1 != "attractor/run/parallel/run-123/fanout/pass1/branch-a" {
		t.Fatalf("pass1 = %q", got1)
	}
	if got2 != "attractor/run/parallel/run-123/fanout/pass2/branch-a" {
		t.Fatalf("pass2 = %q", got2)
	}
}
