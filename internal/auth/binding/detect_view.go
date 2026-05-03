// AuthListView adapts internal/auth.ListOutput into a DetectionView so the
// resolver can ask "is env var X present?" and "is CLI session Y ok?"
// without coupling the binding package to detector internals.

package binding

import (
	"os"

	"github.com/danshapiro/kilroy/internal/auth"
)

// AuthListView wraps an auth.ListOutput and answers DetectionView queries
// against it. EnvVarPresent reads the live process environment (same source
// the env var detector uses). CLISessionOK consults the report.
type AuthListView struct {
	List auth.ListOutput
}

// EnvVarPresent reports whether the named env var is set to a non-empty
// value in the live process environment.
func (v AuthListView) EnvVarPresent(name string) bool {
	return os.Getenv(name) != ""
}

// CLISessionOK reports whether the given CLI tool has a usable logged-in
// session per the auth report (kind=cli_oauth, state=ok).
func (v AuthListView) CLISessionOK(tool string) bool {
	for _, e := range v.List.Entries {
		if e.Tool != tool {
			continue
		}
		if e.Kind != auth.KindCLIOAuth {
			continue
		}
		if e.State == auth.StateOK {
			return true
		}
	}
	return false
}
