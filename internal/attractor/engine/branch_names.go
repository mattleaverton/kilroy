package engine

import (
	"fmt"
	"strings"
)

func buildRunBranch(prefix, runID string) string {
	trimmedPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	trimmedRunID := strings.Trim(strings.TrimSpace(runID), "/")
	switch {
	case trimmedPrefix == "":
		return trimmedRunID
	case trimmedRunID == "":
		return trimmedPrefix
	default:
		return trimmedPrefix + "/" + trimmedRunID
	}
}

// buildParallelBranch constructs the git branch name for a parallel branch.
// passNum is the 1-based count of how many times this fan-out node has been
// dispatched in the current run, producing names like:
//
//	attractor/run/{runID}/parallel/{fanNodeID}/pass1/{childNodeID}
//	attractor/run/{runID}/parallel/{fanNodeID}/pass2/{childNodeID}  (re-visit)
func buildParallelBranch(prefix, runID, fanNodeID string, passNum int, childNodeID string) string {
	runID = strings.Trim(strings.TrimSpace(runID), "/")
	fanNodeID = sanitizeRefComponent(fanNodeID)
	childNodeID = sanitizeRefComponent(childNodeID)
	pass := fmt.Sprintf("pass%d", passNum)
	parts := []string{"parallel"}
	for _, p := range []string{runID, fanNodeID, pass, childNodeID} {
		if strings.TrimSpace(p) == "" {
			continue
		}
		parts = append(parts, p)
	}
	return buildRunBranch(prefix, strings.Join(parts, "/"))
}
