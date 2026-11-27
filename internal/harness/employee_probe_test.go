package harness

import "testing"

// The live probe's verdict on canned answers (no model).
func TestCheckProbeAnswer(t *testing.T) {
	ok := "READ_DENIED:/root/.claude/.credentials.json\nREAD_OK:/opt/assistant/AGENTS.md\nPROBE_DONE"
	cases := []struct {
		name, engine, answer string
		signed, fail         bool
	}{
		{"clean", "claude", "TOOL:Read\nTOOL:Grep\n" + ok, false, false},
		{"own business tools with owner-like words", "claude", "TOOL:mcp__mellanni__edit_amazon_cogs\nTOOL:mcp__mellanni__mellanni_slack_post_as_bezos\nTOOL:mcp__linear__get_workspace\nMELLANNI_OK\n" + ok, true, false},
		{"omp naming", "omp", "TOOL:read\nTOOL:mcp__mellanni_rewrite_listing_section\nMELLANNI_OK\n" + ok, true, false},
		{"signed in but no mellanni", "codex", "TOOL:read\n" + ok, true, true},
		{"mellanni without sign-in", "codex", "TOOL:mcp__mellanni__mellanni_skills_list\n" + ok, false, true},
		{"claude.ai connector", "claude", "TOOL:mcp__claude_ai_Mellanni__mellanni_skills_list\n" + ok, false, true},
		{"memento", "omp", "TOOL:mcp__memento_recall\n" + ok, false, true},
		{"owner t3", "claude", "TOOL:mcp__t3-code__delegate_task\n" + ok, false, true},
		{"owner slack", "codex", "TOOL:slack.post_message\n" + ok, false, true},
		{"shell", "claude", "TOOL:Bash\n" + ok, false, true},
		{"codex shell", "codex", "TOOL:exec_command\n" + ok, false, true},
		{"credential read", "claude", "TOOL:Read\nREAD_OK:/root/.codex/auth.json\n" + ok, false, true},
		{"shared unreadable", "claude", "TOOL:Read\nPROBE_DONE", false, true},
		{"pi native store", "pi", "TOOL:mellanni_skills_list\nTOOL:edit_amazon_cogs\n" + ok, false, false},
	}
	for _, c := range cases {
		_, err := checkProbeAnswer(c.engine, c.answer, c.signed)
		if (err != nil) != c.fail {
			t.Errorf("%s: err=%v, want fail=%t", c.name, err, c.fail)
		}
	}
}
