// Tests for SweepStaleTmuxSessions and runIDFromSessionName parsing.
// SweepStaleTmuxSessions is best-effort; we only need to verify the
// parser and the lookup callback contract.
package agents

import "testing"

func TestRunIDFromSessionName(t *testing.T) {
	cases := map[string]string{
		// Standard shape: kilroy-<runid>-<nodeid>.
		"kilroy-01KQR6N9XZYSQQV96D1GEXDMRP-agent":       "01KQR6N9XZYSQQV96D1GEXDMRP",
		"kilroy-01KQQQCDD61C6DTV98CPF3ANQ3-test":        "01KQQQCDD61C6DTV98CPF3ANQ3",
		"kilroy-01KQR2EWE6WN2EQFVFF5494SKY-implementer": "01KQR2EWE6WN2EQFVFF5494SKY",
		// Multi-segment node id (rare but valid).
		"kilroy-01KQR-some-node-here": "01KQR",
		// Non-kilroy session.
		"unrelated":   "",
		"my-session":  "",
		"kilroy":      "",
		"kilroy-":     "",
		"kilroy-only": "",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			got := runIDFromSessionName(name)
			if got != want {
				t.Fatalf("runIDFromSessionName(%q): got %q want %q", name, got, want)
			}
		})
	}
}

func TestSweepStaleTmuxSessions_NilArgs_NoOp(t *testing.T) {
	// Both nil — early return.
	if killed, err := SweepStaleTmuxSessions(nil, nil); killed != 0 || err != nil {
		t.Fatalf("nil mgr+lookup: got killed=%d err=%v", killed, err)
	}
	// Just lookup nil — early return.
	if killed, err := SweepStaleTmuxSessions(nil, func(string) (bool, error) { return false, nil }); killed != 0 || err != nil {
		t.Fatalf("nil mgr: got killed=%d err=%v", killed, err)
	}
}
