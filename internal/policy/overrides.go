package policy

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	OverrideModePrefer = "prefer"
	OverrideModePin    = "pin"

	PolicySourceBuiltIn         = "built_in"
	PolicySourceGlobalOverride  = "global_override"
	PolicySourceProjectOverride = "project_override"
)

// OverrideConfig is the persisted TOML shape for user/project policy
// overrides. The CLI owns this file; users should not need to edit it by
// hand for normal preference/pin workflows.
type OverrideConfig struct {
	Classes map[string]ClassOverride `toml:"classes"`
}

// ClassOverride describes one class-level model preference.
type ClassOverride struct {
	Mode    string `toml:"mode"`
	ModelID string `toml:"model_id"`
	Driver  string `toml:"driver,omitempty"`
}

// DefaultUserOverridePath returns the XDG-aware path for global policy
// overrides.
func DefaultUserOverridePath() string {
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "kilroy", "policy-overrides.toml")
	}
	home := strings.TrimSpace(os.Getenv("HOME"))
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, ".config", "kilroy", "policy-overrides.toml")
}

// ProjectOverridePath returns the project-local override path.
func ProjectOverridePath(projectRoot string) string {
	if strings.TrimSpace(projectRoot) == "" {
		return ""
	}
	return filepath.Join(projectRoot, ".kilroy", "policy-overrides.toml")
}

// LoadEffective returns the embedded policy with global and then project
// overrides applied. Project overrides have the highest precedence.
func LoadEffective(projectRoot string) (*Data, error) {
	data, err := Load()
	if err != nil {
		return nil, err
	}
	lookup := cloneData(data)
	if err := applyOverridePath(data, lookup, DefaultUserOverridePath(), PolicySourceGlobalOverride); err != nil {
		return nil, err
	}
	if projectPath := ProjectOverridePath(projectRoot); projectPath != "" {
		if err := applyOverridePath(data, lookup, projectPath, PolicySourceProjectOverride); err != nil {
			return nil, err
		}
	}
	if err := validateData(data); err != nil {
		return nil, fmt.Errorf("policy overrides produced invalid policy: %w", err)
	}
	return data, nil
}

func applyOverridePath(data, lookup *Data, path, source string) error {
	cfg, exists, err := LoadOverrideFile(path)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return ApplyOverrides(data, lookup, cfg, source, path)
}

// LoadOverrideFile loads a policy override file. The returned exists value is
// false for a missing file.
func LoadOverrideFile(path string) (OverrideConfig, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return OverrideConfig{}, false, nil
	}
	var cfg OverrideConfig
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return OverrideConfig{}, false, nil
		}
		return OverrideConfig{}, false, fmt.Errorf("stat policy overrides %s: %w", path, err)
	}
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return OverrideConfig{}, true, fmt.Errorf("parse policy overrides %s: %w", path, err)
	}
	if cfg.Classes == nil {
		cfg.Classes = map[string]ClassOverride{}
	}
	return cfg, true, nil
}

// SaveOverrideFile writes a policy override file.
func SaveOverrideFile(path string, cfg OverrideConfig) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("policy override path is empty")
	}
	if cfg.Classes == nil {
		cfg.Classes = map[string]ClassOverride{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create policy override dir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open policy overrides %s: %w", path, err)
	}
	defer f.Close()
	enc := toml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		return fmt.Errorf("write policy overrides %s: %w", path, err)
	}
	return nil
}

// SetOverride updates or creates one class override in path.
func SetOverride(path, className, mode, modelID, driver string) error {
	mode = strings.TrimSpace(mode)
	if mode != OverrideModePrefer && mode != OverrideModePin {
		return fmt.Errorf("unknown override mode %q", mode)
	}
	className = strings.TrimSpace(className)
	modelID = strings.TrimSpace(modelID)
	driver = strings.TrimSpace(driver)
	if className == "" {
		return fmt.Errorf("class is required")
	}
	if modelID == "" {
		return fmt.Errorf("model is required")
	}

	base, err := Load()
	if err != nil {
		return err
	}
	if _, ok := base.Classes[className]; !ok {
		return ErrUnknownClass{Name: className, Available: sortedClassNames(base.Classes)}
	}
	if matches := findCandidateMatches(base, modelID, driver); len(matches) == 0 {
		if driver != "" {
			return fmt.Errorf("policy: no candidate found for model %q with driver %q", modelID, driver)
		}
		return ErrUnknownModel{ModelID: modelID}
	}

	cfg, exists, err := LoadOverrideFile(path)
	if err != nil {
		return err
	}
	if !exists || cfg.Classes == nil {
		cfg.Classes = map[string]ClassOverride{}
	}
	cfg.Classes[className] = ClassOverride{
		Mode:    mode,
		ModelID: modelID,
		Driver:  driver,
	}
	return SaveOverrideFile(path, cfg)
}

// ClearOverride removes one class override from path.
func ClearOverride(path, className string) (bool, error) {
	cfg, exists, err := LoadOverrideFile(path)
	if err != nil {
		return false, err
	}
	if !exists || cfg.Classes == nil {
		return false, nil
	}
	if _, ok := cfg.Classes[className]; !ok {
		return false, nil
	}
	delete(cfg.Classes, className)
	return true, SaveOverrideFile(path, cfg)
}

// ApplyOverrides mutates data by applying cfg. lookup should usually be a
// copy of the original embedded policy so project overrides can still refer
// to built-in candidates even after a global pin narrowed the effective chain.
func ApplyOverrides(data, lookup *Data, cfg OverrideConfig, source, path string) error {
	if data == nil || lookup == nil {
		return fmt.Errorf("policy override: nil policy data")
	}
	if len(cfg.Classes) == 0 {
		return nil
	}
	names := make([]string, 0, len(cfg.Classes))
	for name := range cfg.Classes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, className := range names {
		ov := cfg.Classes[className]
		if err := applyClassOverride(data, lookup, className, ov, source, path); err != nil {
			return err
		}
	}
	return nil
}

func applyClassOverride(data, lookup *Data, className string, ov ClassOverride, source, path string) error {
	className = strings.TrimSpace(className)
	mode := strings.TrimSpace(ov.Mode)
	modelID := strings.TrimSpace(ov.ModelID)
	driver := strings.TrimSpace(ov.Driver)

	if mode != OverrideModePrefer && mode != OverrideModePin {
		return fmt.Errorf("policy override %s class %q: mode must be %q or %q", path, className, OverrideModePrefer, OverrideModePin)
	}
	if modelID == "" {
		return fmt.Errorf("policy override %s class %q: model_id is required", path, className)
	}
	current, ok := data.Classes[className]
	if !ok {
		return ErrUnknownClass{Name: className, Available: sortedClassNames(data.Classes)}
	}
	matches := findCandidateMatches(lookup, modelID, driver)
	if len(matches) == 0 {
		if driver != "" {
			return fmt.Errorf("policy override %s class %q: no candidate found for model %q with driver %q", path, className, modelID, driver)
		}
		return ErrUnknownModel{ModelID: modelID}
	}

	switch mode {
	case OverrideModePin:
		current.Chain = append([]Candidate{}, matches...)
	case OverrideModePrefer:
		current.Chain = prependCandidates(matches, current.Chain)
	}
	data.Classes[className] = current
	if data.AppliedOverrides == nil {
		data.AppliedOverrides = map[string]AppliedOverride{}
	}
	data.AppliedOverrides[className] = AppliedOverride{
		Source:  source,
		Path:    path,
		Mode:    mode,
		ModelID: modelID,
		Driver:  driver,
	}
	return nil
}

func findCandidateMatches(data *Data, modelID, driver string) []Candidate {
	if data == nil {
		return nil
	}
	var matches []Candidate
	seen := map[string]bool{}
	names := sortedClassNames(data.Classes)
	for _, className := range names {
		for _, c := range data.Classes[className].Chain {
			if c.ModelID != modelID {
				continue
			}
			if driver != "" && c.Driver != driver {
				continue
			}
			key := candidateKey(c)
			if seen[key] {
				continue
			}
			seen[key] = true
			matches = append(matches, c)
		}
	}
	return matches
}

func prependCandidates(preferred, chain []Candidate) []Candidate {
	out := make([]Candidate, 0, len(preferred)+len(chain))
	seen := map[string]bool{}
	for _, c := range preferred {
		key := candidateKey(c)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, c)
	}
	for _, c := range chain {
		key := candidateKey(c)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, c)
	}
	return out
}

func candidateKey(c Candidate) string {
	return strings.Join([]string{
		c.ModelID,
		c.Driver,
		c.Transport,
		c.HistorySink,
		c.Requires.Provider,
		string(c.Requires.Method),
		c.Requires.Tool,
	}, "\x00")
}

func cloneData(in *Data) *Data {
	if in == nil {
		return nil
	}
	out := *in
	out.Classes = make(map[string]Class, len(in.Classes))
	for name, cls := range in.Classes {
		copied := cls
		copied.Chain = append([]Candidate{}, cls.Chain...)
		out.Classes[name] = copied
	}
	out.Aliases = append([]ClassAlias{}, in.Aliases...)
	out.Deprecated = append([]Deprecation{}, in.Deprecated...)
	if len(in.AppliedOverrides) > 0 {
		out.AppliedOverrides = make(map[string]AppliedOverride, len(in.AppliedOverrides))
		for k, v := range in.AppliedOverrides {
			out.AppliedOverrides[k] = v
		}
	} else {
		out.AppliedOverrides = nil
	}
	return &out
}
