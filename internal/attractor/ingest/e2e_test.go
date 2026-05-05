package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestRunWithMockClaude tests the full Run pipeline using a mock claude script
// that writes a known .dot file to pipeline.dot.
func TestRunWithMockClaude(t *testing.T) {
	requireIntegration(t)
	repoRoot := findRepoRoot(t)

	// Read a known-good .dot file to use as mock output.
	dotPath := filepath.Join(repoRoot, "research", "refactor-test-vague.dot")
	if _, err := os.Stat(dotPath); err != nil {
		t.Skipf("research dot file not found: %v", err)
	}

	// Create a mock claude script that copies the .dot file to pipeline.dot in CWD.
	tmpDir := t.TempDir()
	mockScript := filepath.Join(tmpDir, "claude")
	err := os.WriteFile(mockScript, []byte("#!/bin/sh\ncp '"+dotPath+"' ./pipeline.dot\n"), 0o755)
	if err != nil {
		t.Fatal(err)
	}

	// Create a minimal skill file.
	skillPath := filepath.Join(tmpDir, "SKILL.md")
	err = os.WriteFile(skillPath, []byte("# Test Skill\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	// Point KILROY_CLAUDE_PATH at the mock.
	t.Setenv("KILROY_CLAUDE_PATH", mockScript)

	result, err := Run(context.Background(), Options{
		Requirements: "solitaire plz",
		SkillPath:    skillPath,
		Model:        "claude-sonnet-4-5",
		RepoPath:     repoRoot,
		Validate:     true,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.DotContent == "" {
		t.Fatal("DotContent is empty")
	}
	if result.DotContent[:7] != "digraph" {
		t.Errorf("DotContent should start with 'digraph', got %q", result.DotContent[:20])
	}

	// Check that the extracted content matches what we'd get from direct extraction.
	dotContent, err := os.ReadFile(dotPath)
	if err != nil {
		t.Fatalf("reading source dot file: %v", err)
	}
	directExtract, err := ExtractDigraph(string(dotContent))
	if err != nil {
		t.Fatalf("direct ExtractDigraph failed: %v", err)
	}
	if result.DotContent != directExtract {
		t.Error("Run extraction differs from direct ExtractDigraph")
	}

	t.Logf("DotContent length: %d bytes", len(result.DotContent))
	t.Logf("Warnings: %v", result.Warnings)
}

// TestRunWithMockClaudeWrappedOutput tests that Run reads the pipeline.dot file
// even when the file contains extra content around the digraph.
func TestRunWithMockClaudeWrappedOutput(t *testing.T) {
	requireIntegration(t)
	repoRoot := findRepoRoot(t)

	dotPath := filepath.Join(repoRoot, "research", "refactor-test-moderate.dot")
	dotContent, err := os.ReadFile(dotPath)
	if err != nil {
		t.Skipf("research dot file not found: %v", err)
	}

	// Create a mock that writes the dot content to pipeline.dot.
	tmpDir := t.TempDir()
	mockScript := filepath.Join(tmpDir, "claude")
	err = os.WriteFile(mockScript, []byte("#!/bin/sh\ncp '"+dotPath+"' ./pipeline.dot\n"), 0o755)
	if err != nil {
		t.Fatal(err)
	}

	skillPath := filepath.Join(tmpDir, "SKILL.md")
	err = os.WriteFile(skillPath, []byte("# Test Skill\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("KILROY_CLAUDE_PATH", mockScript)

	result, err := Run(context.Background(), Options{
		Requirements: "Build a link checker CLI",
		SkillPath:    skillPath,
		Model:        "claude-sonnet-4-5",
		RepoPath:     repoRoot,
		Validate:     true,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.DotContent[:7] != "digraph" {
		t.Errorf("should start with 'digraph', got %q", result.DotContent[:20])
	}
	if result.DotContent[len(result.DotContent)-1] != '}' {
		t.Errorf("should end with '}', got %q", result.DotContent[len(result.DotContent)-5:])
	}

	t.Logf("Read %d bytes from pipeline.dot (source file %d bytes)", len(result.DotContent), len(dotContent))
}

// TestRunWithMockClaudeFailure tests that Run returns an error when claude fails.
func TestRunWithMockClaudeFailure(t *testing.T) {
	requireIntegration(t)
	tmpDir := t.TempDir()

	mockScript := filepath.Join(tmpDir, "claude")
	err := os.WriteFile(mockScript, []byte("#!/bin/sh\necho 'API error' >&2\nexit 1\n"), 0o755)
	if err != nil {
		t.Fatal(err)
	}

	skillPath := filepath.Join(tmpDir, "SKILL.md")
	err = os.WriteFile(skillPath, []byte("# Test Skill\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("KILROY_CLAUDE_PATH", mockScript)

	_, err = Run(context.Background(), Options{
		Requirements: "Build something",
		SkillPath:    skillPath,
		Model:        "claude-sonnet-4-5",
	})
	if err == nil {
		t.Fatal("expected error when claude fails")
	}
	t.Logf("Got expected error: %v", err)
}

// TestRunWithMockClaudeNoPipelineDot tests that Run returns an error when
// claude exits successfully but doesn't write pipeline.dot.
func TestRunWithMockClaudeNoPipelineDot(t *testing.T) {
	requireIntegration(t)
	tmpDir := t.TempDir()

	mockScript := filepath.Join(tmpDir, "claude")
	err := os.WriteFile(mockScript, []byte("#!/bin/sh\nexit 0\n"), 0o755)
	if err != nil {
		t.Fatal(err)
	}

	skillPath := filepath.Join(tmpDir, "SKILL.md")
	err = os.WriteFile(skillPath, []byte("# Test Skill\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("KILROY_CLAUDE_PATH", mockScript)

	_, err = Run(context.Background(), Options{
		Requirements: "Build something",
		SkillPath:    skillPath,
		Model:        "claude-sonnet-4-5",
	})
	if err == nil {
		t.Fatal("expected error when pipeline.dot is missing")
	}
	t.Logf("Got expected error: %v", err)
}
