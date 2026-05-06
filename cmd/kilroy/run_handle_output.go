// Output formatting for the kilroy run / resume run handle. v2 §4: JSON
// to stdout by default for any command that produces structured data;
// --pretty emits human-friendly key=value text. Agents juggling N runs
// parse the JSON; humans skimming a single run hit --pretty.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
)

// runHandle is the canonical run-handle shape — the same fields kilroy
// run, kilroy resume, and (with detached=true + a couple extras)
// kilroy run --detach print. Stable JSON contract.
type runHandle struct {
	Detached       bool                    `json:"detached,omitempty"`
	RunID          string                  `json:"run_id,omitempty"`
	LogsRoot       string                  `json:"logs_root,omitempty"`
	WorktreeDir    string                  `json:"worktree,omitempty"`
	RunBranch      string                  `json:"run_branch,omitempty"`
	FinalCommitSHA string                  `json:"final_commit,omitempty"`
	CXDBUIURL      string                  `json:"cxdb_ui,omitempty"`
	PIDFile        string                  `json:"pid_file,omitempty"`
	PreLaunch      *engine.PreLaunchReport `json:"prelaunch,omitempty"`
	FinalStatus    string                  `json:"final_status,omitempty"`
}

// runHandleFromResult builds a runHandle from an engine.Result. Empty
// fields are omitted (omitempty on every field).
func runHandleFromResult(res *engine.Result) runHandle {
	if res == nil {
		return runHandle{}
	}
	return runHandle{
		RunID:          res.RunID,
		LogsRoot:       res.LogsRoot,
		WorktreeDir:    res.WorktreeDir,
		RunBranch:      res.RunBranch,
		FinalCommitSHA: res.FinalCommitSHA,
		CXDBUIURL:      res.CXDBUIURL,
		FinalStatus:    string(res.FinalStatus),
	}
}

// emitRunHandle writes the run handle to w. JSON is the default;
// pretty=true switches to the legacy key=value format. Newline always
// terminates the last line, matching the prior text format.
func emitRunHandle(w io.Writer, h runHandle, pretty bool) {
	if pretty {
		emitRunHandlePretty(w, h)
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(h)
}

func emitRunHandlePretty(w io.Writer, h runHandle) {
	if h.Detached {
		fmt.Fprintln(w, "detached=true")
	}
	if h.RunID != "" {
		fmt.Fprintf(w, "run_id=%s\n", h.RunID)
	}
	if h.LogsRoot != "" {
		fmt.Fprintf(w, "logs_root=%s\n", h.LogsRoot)
	}
	if h.WorktreeDir != "" {
		fmt.Fprintf(w, "worktree=%s\n", h.WorktreeDir)
	}
	if h.RunBranch != "" {
		fmt.Fprintf(w, "run_branch=%s\n", h.RunBranch)
	}
	if h.FinalCommitSHA != "" {
		fmt.Fprintf(w, "final_commit=%s\n", h.FinalCommitSHA)
	}
	if h.CXDBUIURL != "" {
		fmt.Fprintf(w, "cxdb_ui=%s\n", h.CXDBUIURL)
	}
	if h.PIDFile != "" {
		fmt.Fprintf(w, "pid_file=%s\n", h.PIDFile)
	}
	if h.PreLaunch != nil {
		status := "ok"
		if h.PreLaunch.Summary.Fail > 0 {
			status = "fail"
		}
		fmt.Fprintf(w, "prelaunch_status=%s\n", status)
	}
	if h.FinalStatus != "" {
		fmt.Fprintf(w, "final_status=%s\n", h.FinalStatus)
	}
}

// printRunHandle is the production helper: writes the handle to stdout
// and ensures the trailing newline that callers historically relied on.
// pretty=true => legacy key=value; default => JSON.
func printRunHandle(h runHandle, pretty bool) {
	emitRunHandle(os.Stdout, h, pretty)
}
