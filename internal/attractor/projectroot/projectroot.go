// Locates the Kilroy project root by upward search for a .kilroy/
// directory marker, with KILROY_PROJECT_ROOT as an explicit override.
// Implements the marker-discovery rules from
// docs/plans/2026-05-01-kilroy-v2-final-plan.md §7.2 and §7.4.

package projectroot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// EnvVar is the environment variable that overrides upward search when
// set to a non-empty value. The named directory must contain a .kilroy/
// subdirectory or Find returns an error — silent fallback after an
// explicit override is the failure mode §7.4 calls out.
const EnvVar = "KILROY_PROJECT_ROOT"

// Source values returned by Find indicating how the project root was
// resolved. SourceNone is returned alongside an empty root when no
// .kilroy/ marker was found before reaching $HOME or filesystem root.
const (
	SourceEnv    = "env"
	SourceUpward = "upward"
	SourceNone   = "none"
)

// ErrEnvRootMissingMarker is returned when KILROY_PROJECT_ROOT is set
// but the named directory has no .kilroy/ subdirectory.
var ErrEnvRootMissingMarker = errors.New("KILROY_PROJECT_ROOT does not contain a .kilroy/ directory")

// Find resolves the Kilroy project root.
//
// If KILROY_PROJECT_ROOT is set to a non-empty value, the named
// directory is used iff it contains a .kilroy/ subdirectory; otherwise
// an error wrapping ErrEnvRootMissingMarker is returned.
//
// Otherwise Find walks upward from start (or os.Getwd() when start is
// "") and returns the first directory containing a .kilroy/
// subdirectory. The walk terminates at $HOME and at the filesystem
// root; if neither contains the marker, Find returns ("", "none", nil).
func Find(start string) (string, string, error) {
	if env := os.Getenv(EnvVar); env != "" {
		abs, err := filepath.Abs(env)
		if err != nil {
			abs = env
		}
		if hasMarker(abs) {
			return abs, SourceEnv, nil
		}
		return "", SourceEnv, fmt.Errorf("%w: %s", ErrEnvRootMissingMarker, abs)
	}

	if start == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", SourceNone, nil
		}
		start = cwd
	}
	abs, err := filepath.Abs(start)
	if err != nil {
		abs = start
	}
	home, _ := os.UserHomeDir()

	dir := abs
	for {
		if hasMarker(dir) {
			return dir, SourceUpward, nil
		}
		if dir == home {
			return "", SourceNone, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", SourceNone, nil
		}
		dir = parent
	}
}

// hasMarker reports whether dir contains a .kilroy/ subdirectory.
func hasMarker(dir string) bool {
	info, err := os.Stat(filepath.Join(dir, ".kilroy"))
	return err == nil && info.IsDir()
}
