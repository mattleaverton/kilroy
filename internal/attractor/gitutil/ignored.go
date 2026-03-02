package gitutil

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ListIgnoredFiles returns repo-relative paths of all gitignored files under
// dir. It runs: git ls-files --others --ignored --exclude-standard
func ListIgnoredFiles(dir string) ([]string, error) {
	stdout, _, err := runGit(dir, "ls-files", "--others", "--ignored", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range strings.Split(stdout, "\n") {
		f = strings.TrimSpace(f)
		if f != "" {
			files = append(files, f)
		}
	}
	return files, nil
}

// CopyIgnoredFiles copies all gitignored files from srcDir into dstDir,
// preserving relative paths and file permissions. Intermediate directories are
// created as needed. Symlinks and non-regular files are skipped. Individual
// file errors are silently skipped; the function returns an error only if the
// ignored-file listing itself fails.
func CopyIgnoredFiles(srcDir, dstDir string) error {
	files, err := ListIgnoredFiles(srcDir)
	if err != nil {
		return err
	}
	for _, rel := range files {
		src := filepath.Join(srcDir, rel)
		dst := filepath.Join(dstDir, rel)
		info, err := os.Lstat(src)
		if err != nil {
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			continue
		}
		_ = copyFileMode(src, dst, info.Mode())
	}
	return nil
}

func copyFileMode(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
