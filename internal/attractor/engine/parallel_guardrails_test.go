package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestParallelHandler_FailsFastOnEmptyRunBranchPrefix(t *testing.T) {
	exec := &Execution{
		LogsRoot: t.TempDir(),
		Engine: &Engine{
			Options: RunOptions{
				RepoPath:        t.TempDir(),
				RunID:           "run-1",
				RunBranchPrefix: "",
			},
		},
	}
	parallelNode := model.NewNode("par")
	edge := model.NewEdge("par", "a")

	res := (&ParallelHandler{}).runBranch(context.Background(), exec, parallelNode, "deadbeef", "join", 0, edge, 1, nil)
	if res.Outcome.Status != runtime.StatusFail {
		t.Fatalf("status = %q, want %q", res.Outcome.Status, runtime.StatusFail)
	}
	if !strings.Contains(res.Outcome.FailureReason, "run_branch_prefix") {
		t.Fatalf("failure_reason = %q, want run_branch_prefix guardrail", res.Outcome.FailureReason)
	}
	if !strings.Contains(res.Error, "run_branch_prefix") {
		t.Fatalf("error = %q, want run_branch_prefix guardrail", res.Error)
	}
}

func TestRunSubgraphUntil_ContextCanceled_StopsBeforeNextNode(t *testing.T) {
	got := runCanceledSubgraphFixture(t)
	if got.scheduledAfterCancel {
		t.Fatalf("scheduled node %q after cancellation", got.nextNode)
	}
}

func TestParallelCancelPrecedence_DoesNotScheduleNewWork(t *testing.T) {
	got := runParallelCancelFixture(t)
	if got.startedNodesAfterCancel > 0 {
		t.Fatalf("started %d nodes after cancel", got.startedNodesAfterCancel)
	}
}

func TestFanIn_RunScopedPromotion_ConflictFailsWithInputSnapshotConflict(t *testing.T) {
	out := runFanInPromotionHandlerFixture(t, []string{"postmortem_latest.md"}, []fanInPromotionBranchSeed{
		{
			Key: "b",
			Files: map[string]string{
				"postmortem_latest.md": "from-branch-b",
			},
		},
		{
			Key: "a",
			Files: map[string]string{
				"postmortem_latest.md": "from-branch-a",
			},
		},
	})

	if out.Status != runtime.StatusFail {
		t.Fatalf("status: got %q want %q", out.Status, runtime.StatusFail)
	}
	if out.FailureReason != "input_snapshot_conflict" {
		t.Fatalf("failure_reason: got %q want input_snapshot_conflict", out.FailureReason)
	}
	conflicts := metaConflictList(t, out.Meta["conflicts"])
	if len(conflicts) == 0 {
		t.Fatalf("conflicts payload missing: %+v", out.Meta)
	}
	if strings.TrimSpace(anyToString(conflicts[0]["path"])) == "" {
		t.Fatalf("conflict payload missing path: %+v", conflicts[0])
	}
}

func TestFanIn_RunScopedPromotion_ConflictPayloadSortedByPathThenBranch(t *testing.T) {
	out := runFanInPromotionHandlerFixture(t, []string{"z.md", "a.md"}, []fanInPromotionBranchSeed{
		{
			Key: "b",
			Files: map[string]string{
				"z.md": "branch-b-z",
				"a.md": "branch-b-a",
			},
		},
		{
			Key: "a",
			Files: map[string]string{
				"z.md": "branch-a-z",
				"a.md": "branch-a-a",
			},
		},
	})

	if out.Status != runtime.StatusFail {
		t.Fatalf("status: got %q want %q", out.Status, runtime.StatusFail)
	}
	conflicts := metaConflictList(t, out.Meta["conflicts"])
	if len(conflicts) != 2 {
		t.Fatalf("expected 2 conflicts, got %d (%+v)", len(conflicts), conflicts)
	}
	if strings.TrimSpace(anyToString(conflicts[0]["path"])) != "a.md" ||
		strings.TrimSpace(anyToString(conflicts[1]["path"])) != "z.md" {
		t.Fatalf("conflicts must be sorted by path: %+v", conflicts)
	}
	branches := anyToStringSlice(t, conflicts[0]["branches"])
	if len(branches) != 2 || branches[0] != "a" || branches[1] != "b" {
		t.Fatalf("conflict branches must be sorted by branch key: %+v", conflicts[0]["branches"])
	}
}

type fanInPromotionBranchSeed struct {
	Key   string
	Files map[string]string
}

func runFanInPromotionHandlerFixture(t *testing.T, promote []string, branches []fanInPromotionBranchSeed) runtime.Outcome {
	t.Helper()

	runID := "fanin-promotion-unit"
	mainLogsRoot := t.TempDir()
	mainWorktree := t.TempDir()

	mainLineage := newInputSnapshotLineage(runID)
	baseRev := mainLineage.CreateRunRevision("", map[string]string{})
	mainLineage.RunHead = baseRev
	if err := mainLineage.SaveAtomic(mainLogsRoot); err != nil {
		t.Fatalf("save main lineage: %v", err)
	}

	results := make([]parallelBranchResult, 0, len(branches))
	for _, branch := range branches {
		branchKey := strings.TrimSpace(branch.Key)
		if branchKey == "" {
			t.Fatalf("branch key is required")
		}
		branchLogsRoot := t.TempDir()

		lineage := newInputSnapshotLineage(runID)
		base := lineage.CreateRunRevision("", map[string]string{})
		lineage.RunHead = base
		forkRev, err := lineage.ForkBranch("worktree", base)
		if err != nil {
			t.Fatalf("ForkBranch(%s): %v", branchKey, err)
		}
		nextRev := lineage.AdvanceBranch("worktree", forkRev, digestMapForFiles(branch.Files))
		if strings.TrimSpace(nextRev) == "" {
			t.Fatalf("advance branch lineage produced empty revision for %s", branchKey)
		}
		if err := lineage.SaveAtomic(branchLogsRoot); err != nil {
			t.Fatalf("save branch lineage %s: %v", branchKey, err)
		}
		if err := writeJSON(inputRunManifestPath(branchLogsRoot), &InputManifest{
			BaseRunRevision:    base,
			BranchHeadRevision: nextRev,
		}); err != nil {
			t.Fatalf("write branch manifest %s: %v", branchKey, err)
		}
		for rel, content := range branch.Files {
			path := filepath.Join(inputRevisionRoot(branchLogsRoot, nextRev), filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", path, err)
			}
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatalf("write %s: %v", path, err)
			}
		}

		results = append(results, parallelBranchResult{
			BranchKey: branchKey,
			LogsRoot:  branchLogsRoot,
			Outcome: runtime.Outcome{
				Status: runtime.StatusSuccess,
			},
		})
	}

	ctx := runtime.NewContext()
	ctx.Set("parallel.results", results)

	cfg := &RunConfigFile{}
	cfg.Inputs.Materialize.FanIn.PromoteRunScoped = append([]string{}, promote...)

	eng := &Engine{
		Options:                    RunOptions{RunID: runID},
		RunConfig:                  cfg,
		LogsRoot:                   mainLogsRoot,
		WorktreeDir:                mainWorktree,
		InputMaterializationPolicy: InputMaterializationPolicy{Enabled: true},
		InputInferenceCache:        map[string][]InferredReference{},
		InputSourceTargetMap:       map[string]string{},
		loopFailureSignatures:      map[string]int{},
		restartFailureSignatures:   map[string]int{},
	}

	handler := &FanInHandler{}
	out, err := handler.Execute(context.Background(), &Execution{
		Context:     ctx,
		LogsRoot:    mainLogsRoot,
		WorktreeDir: mainWorktree,
		Engine:      eng,
	}, &model.Node{ID: "join"})
	if err != nil {
		t.Fatalf("FanInHandler.Execute: %v", err)
	}
	return out
}

func digestMapForFiles(files map[string]string) map[string]string {
	digest := map[string]string{}
	for rel, content := range files {
		normalized := filepath.ToSlash(filepath.Clean(strings.TrimSpace(rel)))
		if normalized == "" || normalized == "." {
			continue
		}
		sum := sha256.Sum256([]byte(content))
		digest[normalized] = "sha256:" + hex.EncodeToString(sum[:])
	}
	return digest
}

func metaConflictList(t *testing.T, raw any) []map[string]any {
	t.Helper()
	switch v := raw.(type) {
	case []map[string]any:
		return append([]map[string]any{}, v...)
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				t.Fatalf("conflict item must decode as map[string]any, got %T", item)
			}
			out = append(out, m)
		}
		return out
	default:
		t.Fatalf("conflicts payload must decode as []any, got %T", raw)
	}
	return nil
}

func anyToStringSlice(t *testing.T, raw any) []string {
	t.Helper()
	switch v := raw.(type) {
	case []string:
		return append([]string{}, v...)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, strings.TrimSpace(anyToString(item)))
		}
		return out
	default:
		t.Fatalf("expected []string/[]any, got %T", raw)
	}
	return nil
}
