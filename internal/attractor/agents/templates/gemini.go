// Gemini CLI invocation template.
package templates

import (
	"time"
)

// Gemini returns an invocation template for Google Gemini CLI.
func Gemini() Template {
	return Template{
		Name:   "gemini",
		Binary: "gemini",
		BuildArgs: func(prompt, workDir, model string) []string {
			args := []string{"--auto-accept-all"}
			if model != "" {
				args = append(args, "--model", model)
			}
			args = append(args, prompt)
			return args
		},
		BuildEnv: func() map[string]string {
			// Credential delivery is the binder's job (see
			// internal/attractor/engine/binder_google.go). The template
			// must NOT pass through env keys here; otherwise a CLI session
			// route can be silently overridden by a stray env var the
			// binder told us not to use.
			return map[string]string{}
		},
		PromptPrefix:    ">",
		BusyIndicators:  []string{},
		ProcessNames:    []string{"gemini"},
		ExitsOnComplete: true,
		StartupTimeout:  15 * time.Second,
	}
}
