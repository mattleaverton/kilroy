package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/providerspec"
)

func authSet(args []string) {
	provider, envName, err := parseAuthSetArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg, err := loadGlobalAuthOrTemplate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "auth set: %v\n", err)
		os.Exit(1)
	}
	req := apiKeyRequirement(provider)
	chainName := ensureChainForRequirement(&cfg, req)
	chain := cfg.Chains[chainName]
	chain.Sources = moveEnvSourceToFront(chain.Sources, envName)
	cfg.Chains[chainName] = chain

	path := globalAuthPath()
	if err := saveGlobalAuthConfig(path, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "auth set: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote global auth: %s\n", path)
	fmt.Printf("%s uses env %s\n", req.Key(), envName)
}

func parseAuthSetArgs(args []string) (provider, envName string, err error) {
	usage := "usage: kilroy auth set <provider> --env <ENV_VAR>"
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "--env":
			i++
			if i >= len(args) {
				return "", "", fmt.Errorf("--env requires an env var name\n%s", usage)
			}
			envName = args[i]
		case "-h", "--help":
			return "", "", fmt.Errorf("%s", usage)
		default:
			if strings.HasPrefix(a, "--") {
				return "", "", fmt.Errorf("unknown flag %q\n%s", a, usage)
			}
			if provider != "" {
				return "", "", fmt.Errorf("unexpected extra argument %q\n%s", a, usage)
			}
			provider = a
		}
	}
	provider = canonicalProvider(provider)
	envName = strings.TrimSpace(envName)
	if provider == "" || envName == "" {
		return "", "", fmt.Errorf("provider and --env are required\n%s", usage)
	}
	return provider, envName, nil
}

func authPrefer(args []string) {
	req, envName, err := parseAuthRequirementEnvArgs("prefer", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg, err := loadGlobalAuthOrTemplate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "auth prefer: %v\n", err)
		os.Exit(1)
	}
	chainName := ensureChainForRequirement(&cfg, req)
	chain := cfg.Chains[chainName]
	chain.Sources = moveEnvSourceToFront(chain.Sources, envName)
	cfg.Chains[chainName] = chain

	path := globalAuthPath()
	if err := saveGlobalAuthConfig(path, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "auth prefer: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote global auth: %s\n", path)
	fmt.Printf("%s now prefers env %s\n", req.Key(), envName)
}

func authRemoveSource(args []string) {
	req, envName, err := parseAuthRequirementEnvArgs("remove-source", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg, exists, err := loadGlobalAuthIfExists()
	if err != nil {
		fmt.Fprintf(os.Stderr, "auth remove-source: %v\n", err)
		os.Exit(1)
	}
	if !exists {
		fmt.Fprintf(os.Stderr, "auth remove-source: no global auth config at %s\n", globalAuthPath())
		os.Exit(1)
	}
	chainName, ok := cfg.Bindings[req.Key()]
	if !ok || strings.TrimSpace(chainName) == "" {
		fmt.Fprintf(os.Stderr, "auth remove-source: no chain bound for %s\n", req.Key())
		os.Exit(1)
	}
	chain, ok := cfg.Chains[chainName]
	if !ok {
		fmt.Fprintf(os.Stderr, "auth remove-source: bound chain %q is not defined\n", chainName)
		os.Exit(1)
	}
	chain.Sources = removeEnvSource(chain.Sources, envName)
	cfg.Chains[chainName] = chain

	path := globalAuthPath()
	if err := saveGlobalAuthConfig(path, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "auth remove-source: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote global auth: %s\n", path)
	fmt.Printf("removed env %s from %s\n", envName, req.Key())
}

func parseAuthRequirementEnvArgs(verb string, args []string) (binding.Requirement, string, error) {
	usage := fmt.Sprintf("usage: kilroy auth %s <provider/method[/tool]> <ENV_VAR>", verb)
	var reqKey, envName string
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return binding.Requirement{}, "", fmt.Errorf("%s", usage)
		}
		if strings.HasPrefix(a, "--") {
			return binding.Requirement{}, "", fmt.Errorf("unknown flag %q\n%s", a, usage)
		}
		switch {
		case reqKey == "":
			reqKey = a
		case envName == "":
			envName = a
		default:
			return binding.Requirement{}, "", fmt.Errorf("unexpected extra argument %q\n%s", a, usage)
		}
	}
	if reqKey == "" || envName == "" {
		return binding.Requirement{}, "", fmt.Errorf("requirement and env var are required\n%s", usage)
	}
	req, err := parseRequirementKey(reqKey)
	if err != nil {
		return binding.Requirement{}, "", err
	}
	req.Provider = canonicalProvider(req.Provider)
	if req.Provider == "" {
		return binding.Requirement{}, "", fmt.Errorf("provider is required in %q", reqKey)
	}
	return req, strings.TrimSpace(envName), nil
}

func loadGlobalAuthOrTemplate() (binding.Config, error) {
	cfg, exists, err := loadGlobalAuthIfExists()
	if err != nil {
		return binding.Config{}, err
	}
	if exists {
		return cfg, nil
	}
	cfg, err = binding.LoadDefaultTemplates()
	if err != nil {
		return binding.Config{}, err
	}
	ensureConfigMaps(&cfg)
	return cfg, nil
}

func loadGlobalAuthIfExists() (binding.Config, bool, error) {
	path := globalAuthPath()
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return binding.Config{}, false, nil
		}
		return binding.Config{}, false, fmt.Errorf("stat %s: %w", path, err)
	}
	cfg, err := binding.LoadConfigFromPaths(path, "")
	if err != nil {
		return binding.Config{}, true, err
	}
	ensureConfigMaps(&cfg)
	return cfg, true, nil
}

func globalAuthPath() string {
	return filepath.Join(authDefaultConfigDir(), "auth.toml")
}

func saveGlobalAuthConfig(path string, cfg binding.Config) error {
	ensureConfigMaps(&cfg)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create auth config dir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	enc := toml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func ensureConfigMaps(cfg *binding.Config) {
	if cfg.Bindings == nil {
		cfg.Bindings = map[string]string{}
	}
	if cfg.Chains == nil {
		cfg.Chains = map[string]binding.Chain{}
	}
	for name, chain := range cfg.Chains {
		chain.Name = name
		cfg.Chains[name] = chain
	}
}

func ensureChainForRequirement(cfg *binding.Config, req binding.Requirement) string {
	ensureConfigMaps(cfg)
	key := req.Key()
	if chainName := strings.TrimSpace(cfg.Bindings[key]); chainName != "" {
		chain := cfg.Chains[chainName]
		chain.Name = chainName
		if chain.Requires.Provider == "" {
			chain.Requires = req
		}
		cfg.Chains[chainName] = chain
		return chainName
	}
	chainName := defaultChainName(req)
	cfg.Bindings[key] = chainName
	chain := cfg.Chains[chainName]
	chain.Name = chainName
	chain.Requires = req
	cfg.Chains[chainName] = chain
	return chainName
}

func defaultChainName(req binding.Requirement) string {
	parts := []string{
		safeName(req.Provider),
		safeName(string(req.Method)),
	}
	if req.Tool != "" {
		parts = append(parts, safeName(req.Tool))
	}
	return strings.Join(parts, "_")
}

func safeName(in string) string {
	in = strings.ToLower(strings.TrimSpace(in))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range in {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

func moveEnvSourceToFront(sources []binding.Source, envName string) []binding.Source {
	envName = strings.TrimSpace(envName)
	out := []binding.Source{{Kind: binding.SourceEnvVar, Name: envName}}
	for _, src := range sources {
		if src.Kind == binding.SourceEnvVar && src.Name == envName {
			continue
		}
		out = append(out, src)
	}
	return out
}

func removeEnvSource(sources []binding.Source, envName string) []binding.Source {
	envName = strings.TrimSpace(envName)
	out := make([]binding.Source, 0, len(sources))
	for _, src := range sources {
		if src.Kind == binding.SourceEnvVar && src.Name == envName {
			continue
		}
		out = append(out, src)
	}
	return out
}

func sourceInChain(sources []binding.Source, src binding.Source) bool {
	for _, existing := range sources {
		if existing.Kind != src.Kind {
			continue
		}
		switch src.Kind {
		case binding.SourceEnvVar:
			if existing.Name == src.Name {
				return true
			}
		case binding.SourceCLISession:
			if existing.Tool == src.Tool {
				return true
			}
		}
	}
	return false
}

func apiKeyRequirement(provider string) binding.Requirement {
	return binding.Requirement{
		Provider: canonicalProvider(provider),
		Method:   binding.MethodAPIKey,
	}
}

func canonicalProvider(provider string) string {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return ""
	}
	if canon := providerspec.CanonicalProviderKey(provider); canon != "" {
		return canon
	}
	return strings.ToLower(provider)
}

func sortedChainNames(cfg binding.Config) []string {
	names := make([]string, 0, len(cfg.Chains))
	for name := range cfg.Chains {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
