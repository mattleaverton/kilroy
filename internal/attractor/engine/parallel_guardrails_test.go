package engine

import (
	"context"
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
