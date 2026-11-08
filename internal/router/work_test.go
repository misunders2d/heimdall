package router

import "testing"

func TestParseWorkCommand(t *testing.T) {
	for in, want := range map[string]string{"!work": "", "!work list": "list", "!WORK add MEL-1 x": "add MEL-1 x"} {
		if c := parseCommand(in); c.Kind != cmdWork || c.Text != want {
			t.Fatalf("%q: %+v", in, c)
		}
	}
	if c := parseCommand("work list"); c.Kind != cmdMessage {
		t.Fatalf("bare word is a message: %+v", c)
	}
}
