package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
)

type staleBuildStatus struct {
	Stale         bool
	BinaryPath    string
	RepoRoot      string
	BuiltRevision string
	HeadRevision  string
}

// embeddedBuildRevision can be set at build time:
//
//	go build -ldflags "-X main.embeddedBuildRevision=<sha>"
//
// When unset, runtime build info is used.
var embeddedBuildRevision string

// isReleaseBuild is set to "true" by goreleaser at release time:
//
//	go build -ldflags "-X main.isReleaseBuild=true"
//
// When set, stale-build detection is suppressed entirely. The embedded SHA
// in a released binary is a build-time fingerprint, not a runtime invariant —
// users running a released kilroy from anywhere should never be told their
// binary is "stale" relative to whatever git repo happens to contain it.
//
// Dev builds via plain `go build ./cmd/kilroy` leave this empty; the dev-mode
// CWD-scoped check in detectStaleKilroyBuildFor still applies.
var isReleaseBuild string

func ensureFreshKilroyBuild(confirmStaleBuild bool) error {
	status, ok := detectStaleKilroyBuild()
	if !ok || !status.Stale {
		return nil
	}
	warning := staleBuildWarning(status)
	if !confirmStaleBuild {
		return fmt.Errorf("%s\nrefusing to run a stale build without --confirm-stale-build", warning)
	}
	fmt.Fprintln(os.Stderr, warning)
	fmt.Fprintln(os.Stderr, "proceeding because --confirm-stale-build was provided")
	return nil
}

func detectStaleKilroyBuild() (staleBuildStatus, bool) {
	// Released binaries don't perform stale-build detection: their embedded
	// SHA is a build-time fingerprint, not a runtime invariant.
	if isReleaseBuild != "" {
		return staleBuildStatus{}, false
	}
	exePath, err := os.Executable()
	if err != nil {
		return staleBuildStatus{}, false
	}
	if evalPath, evalErr := filepath.EvalSymlinks(exePath); evalErr == nil {
		exePath = evalPath
	}
	exePath = strings.TrimSpace(exePath)
	if exePath == "" {
		return staleBuildStatus{}, false
	}
	cwd, err := os.Getwd()
	if err != nil {
		return staleBuildStatus{}, false
	}
	return detectStaleKilroyBuildFor(exePath, cwd)
}

// detectStaleKilroyBuildFor is the testable core of stale-build detection.
// exePath must already be symlink-resolved. cwd is the invocation working directory.
//
// The check is scoped to dev-mode invocations: it only fires when cwd is inside
// the same git repository that the binary was built from. When the user invokes
// kilroy from an unrelated directory (including a completely different git repo),
// the function returns (staleBuildStatus{}, false) — a silent no-op.
func detectStaleKilroyBuildFor(exePath, cwd string) (staleBuildStatus, bool) {
	rev, ok := binaryVCSRevision()
	if !ok {
		return staleBuildStatus{}, false
	}
	exePath = strings.TrimSpace(exePath)
	if exePath == "" {
		return staleBuildStatus{}, false
	}

	// Locate the git repo that the binary lives in (the "source repo").
	repoRoot, ok := gitTopLevel(filepath.Dir(exePath))
	if !ok {
		// Binary is not inside any git repo — no stale check possible.
		return staleBuildStatus{}, false
	}

	// Scope guard: only fire when cwd is inside the same repo as the binary.
	// This limits stale-build detection to dev-mode (user is working inside the
	// kilroy source tree). Invocations from any other directory are silently
	// no-oped, including invocations from a completely different git repo.
	cwdRepoRoot, ok := gitTopLevel(cwd)
	if !ok {
		// CWD has no git repo — not a dev invocation.
		return staleBuildStatus{}, false
	}

	// Use realpath-resolved roots for comparison so that symlinked checkouts
	// (e.g. ~/.local/bin/kilroy → ~/sw/kilroy/kilroy) compare correctly.
	realRepoRoot, err := filepath.EvalSymlinks(repoRoot)
	if err != nil {
		realRepoRoot = repoRoot
	}
	realCWDRepoRoot, err := filepath.EvalSymlinks(cwdRepoRoot)
	if err != nil {
		realCWDRepoRoot = cwdRepoRoot
	}
	if realCWDRepoRoot != realRepoRoot {
		// CWD is in a different git repo — not a dev invocation for this binary.
		return staleBuildStatus{}, false
	}

	head, ok := gitHEADRevision(repoRoot)
	if !ok {
		return staleBuildStatus{}, false
	}
	status := staleBuildStatus{
		BinaryPath:    exePath,
		RepoRoot:      repoRoot,
		BuiltRevision: rev,
		HeadRevision:  head,
	}
	status.Stale = !sameRevision(status.BuiltRevision, status.HeadRevision)
	return status, true
}

func binaryVCSRevision() (string, bool) {
	if rev := strings.TrimSpace(embeddedBuildRevision); rev != "" {
		return rev, true
	}
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil {
		return "", false
	}
	for _, s := range info.Settings {
		if s.Key != "vcs.revision" {
			continue
		}
		rev := strings.TrimSpace(s.Value)
		if rev == "" {
			return "", false
		}
		return rev, true
	}
	return "", false
}

func gitTopLevel(dir string) (string, bool) {
	out, err := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel").CombinedOutput()
	if err != nil {
		return "", false
	}
	root := strings.TrimSpace(string(out))
	if root == "" {
		return "", false
	}
	return root, true
}

func gitHEADRevision(repoRoot string) (string, bool) {
	out, err := exec.Command("git", "-C", repoRoot, "rev-parse", "HEAD").CombinedOutput()
	if err != nil {
		return "", false
	}
	head := strings.TrimSpace(string(out))
	if head == "" {
		return "", false
	}
	return head, true
}

func sameRevision(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

func staleBuildWarning(status staleBuildStatus) string {
	return strings.Join([]string{
		"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
		"WARNING: STALE KILROY BUILD DETECTED",
		fmt.Sprintf("binary: %s", status.BinaryPath),
		fmt.Sprintf("repo_root: %s", status.RepoRoot),
		fmt.Sprintf("built_revision: %s", shortRevision(status.BuiltRevision)),
		fmt.Sprintf("repo_head: %s", shortRevision(status.HeadRevision)),
		"rebuild with: go build -o ./kilroy ./cmd/kilroy",
		"then rerun; or pass --confirm-stale-build to continue anyway",
		"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
	}, "\n")
}

func shortRevision(rev string) string {
	rev = strings.TrimSpace(rev)
	if len(rev) <= 12 {
		return rev
	}
	return rev[:12]
}
