// templates.go embeds the default_chains.toml template and exposes helpers
// for `kilroy auth defaults` (verbatim print) and `kilroy auth init` (sampling).
// The embedded bytes are NEVER used for active runtime resolution — they exist
// only as a starting-point template that users copy and trim into their own
// ~/.config/kilroy/auth.toml.

package binding

import (
	_ "embed"
	"fmt"

	"github.com/BurntSushi/toml"
)

//go:embed data/default_chains.toml
var defaultChainsTOML []byte

// DefaultChainsTOML returns the embedded default_chains.toml bytes verbatim.
// Callers use this to print the file for `kilroy auth defaults` or to sample
// it for `kilroy auth init`. The returned slice must not be modified.
func DefaultChainsTOML() []byte {
	return defaultChainsTOML
}

// LoadDefaultTemplates parses the embedded TOML into a Config. The returned
// Config is ready for inspection or sampling by `kilroy auth init`. Chain.Name
// is populated from the map key for every chain in the result.
//
// Errors only on TOML parse failure, which indicates a build-time defect in
// the embedded file rather than a user error.
func LoadDefaultTemplates() (Config, error) {
	var cfg Config
	if _, err := toml.Decode(string(defaultChainsTOML), &cfg); err != nil {
		return Config{}, fmt.Errorf("parse default_chains.toml: %w", err)
	}
	// Chain.Name is toml:"-" so the decoder skips it; populate from map key.
	for name, chain := range cfg.Chains {
		chain.Name = name
		cfg.Chains[name] = chain
	}
	return cfg, nil
}
