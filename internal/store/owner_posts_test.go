package store

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestPlatformBefore(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"41", "42", true}, {"42", "42", false}, {"43", "42", false}, {"9", "10", true},
		{"1700000000.000100", "1700000000.000101", true},
		{"1700000000.9", "1700000000.10", false}, // .900000 vs .100000, never string order
		{"1699999999.999999", "1700000000.000000", true},
		{"1700000000.000100", "1700000000.000100", false},
		{"", "42", false}, {"42", "", false}, {"41", "1700000000.1", false}, {"x", "y", false}, {"-1", "2", false},
		{"0", "1", false}, {"0.000001", "1.000000", false}, // zero is never a platform ID
	} {
		if got := PlatformBefore(c.a, c.b); got != c.want {
			t.Errorf("PlatformBefore(%q, %q) = %t", c.a, c.b, got)
		}
	}
}

// Invariant I1: a card has a receipt only when every part has a valid
// platform ID, each strictly after the one before, in one format.
func TestCardReceipt(t *testing.T) {
	for _, c := range []struct {
		parts []string
		want  string
	}{
		{nil, ""}, {[]string{}, ""}, {[]string{""}, ""}, {[]string{"0"}, ""}, {[]string{"-3"}, ""},
		{[]string{"5"}, "5"}, {[]string{"5", "6", "9"}, "9"},
		{[]string{"0", "7"}, ""}, {[]string{"", "7"}, ""}, {[]string{"5", "", "7"}, ""}, {[]string{"5", "0"}, ""},
		{[]string{"7", "5"}, ""}, {[]string{"5", "5"}, ""}, {[]string{"5", "9", "8"}, ""},
		{[]string{"5", "1700000000.000100"}, ""},
		{[]string{"1700000000.000100", "1700000000.000200"}, "1700000000.000200"},
		{[]string{"1700000000.000200", "1700000000.000100"}, ""},
	} {
		if got := CardReceipt(c.parts); got != c.want {
			t.Errorf("CardReceipt(%q) = %q, want %q", c.parts, got, c.want)
		}
	}
}

// A sent card's receipt follows CardReceipt, and a Yes answers it only
// when stored later and after the receipt on the platform.
func TestSentRefRecordsOnlyAWholeReceipt(t *testing.T) {
	for _, c := range []struct {
		name    string
		parts   []string
		yes     string
		approve bool
	}{
		{"ordered, Yes after", []string{"41", "42"}, "43", true},
		{"ordered, Yes between parts", []string{"41", "45"}, "43", false},
		{"first zero", []string{"0", "42"}, "43", false},
		{"last missing", []string{"41", ""}, "43", false},
		{"unordered", []string{"42", "41"}, "43", false},
		{"nothing reported", nil, "43", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := open(t)
			card := core.ConvKey{Interface: "telegram", Chat: "42"}
			p := OwnerPost{ID: "p1", Creator: "telegram:1", Card: card, Target: core.ConvKey{Interface: "slack", Chat: "C1"}, Label: "#c", Text: "x", Origin: "t",
				State: OwnerPostPending, Expires: time.Now().Add(time.Hour), CardMessage: &core.Outbound{Conv: card, Text: "card"}}
			if err := s.AnswerCommand(0, core.ConvKey{}, CommandEnd{Detail: "job", Job: &JobChange{NewOwnerPost: &p}}); err != nil {
				t.Fatal(err)
			}
			row, ok, err := s.NextOutbound("telegram")
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			if err := s.SentRef(row.ID, c.parts); err != nil {
				t.Fatal(err)
			}
			got, _, _ := s.OwnerPostByID("p1")
			if seen := got.CardSeenBefore(1<<40, c.yes); seen != c.approve {
				t.Fatalf("approvable %t, want %t (ref %q)", seen, c.approve, got.CardRef)
			}
			if !c.approve && got.CardRef == "" && !got.CardUnconfirmed() {
				t.Fatal("a sent card without a receipt is not marked unconfirmed")
			}
		})
	}
}

// Invariant I2: each delivery step is recorded before the next; a step
// out of order is refused, and recorded uploads require the text done.
func TestOwnerPostStagesKeepOrder(t *testing.T) {
	approved := func(t *testing.T) *Store {
		s := open(t)
		p := OwnerPost{ID: "p1", Creator: "telegram:1", Card: core.ConvKey{Interface: "telegram", Chat: "42"}, Target: core.ConvKey{Interface: "slack", Chat: "C1"}, Label: "#c", Text: "x", Origin: "t",
			State: OwnerPostApproved, Expires: time.Now().Add(time.Hour)}
		if err := s.AnswerCommand(0, core.ConvKey{}, CommandEnd{Detail: "job", Job: &JobChange{NewOwnerPost: &p}}); err != nil {
			t.Fatal(err)
		}
		return s
	}
	type step struct {
		name string
		do   func(*Store) error
		ok   bool
	}
	start := func(parts, i int) func(*Store) error {
		return func(s *Store) error { return s.StartOwnerPostPart("p1", parts, i) }
	}
	done := func(i int, ts string) func(*Store) error {
		return func(s *Store) error { return s.OwnerPostPartDone("p1", i, ts) }
	}
	uploads := func(parts int) func(*Store) error {
		return func(s *Store) error { return s.SetOwnerPostUploads("p1", parts, []string{"F1"}) }
	}
	files := func(s *Store) error { return s.OwnerPostFilesDone("p1") }
	notSent := func(i int) func(*Store) error {
		return func(s *Store) error { return s.OwnerPostPartNotSent("p1", i) }
	}
	for _, c := range []struct {
		name  string
		steps []step
		want  OwnerPostStages
	}{
		{"in order", []step{{"start 0", start(2, 0), true}, {"done 0", done(0, "1.1"), true}, {"start 1", start(2, 1), true}, {"done 1", done(1, "1.2"), true}, {"uploads", uploads(2), true}, {"files", files, true}},
			OwnerPostStages{Parts: 2, Started: 2, Done: []string{"1.1", "1.2"}, Files: true, Uploads: []string{"F1"}}},
		{"part 1 before part 0 done", []step{{"start 0", start(2, 0), true}, {"start 1", start(2, 1), false}}, OwnerPostStages{Parts: 2, Started: 1}},
		{"done without start", []step{{"done 0", done(0, "1.1"), false}}, OwnerPostStages{}},
		{"done without ts", []step{{"start 0", start(1, 0), true}, {"done 0", done(0, ""), false}}, OwnerPostStages{Parts: 1, Started: 1}},
		{"split changed", []step{{"start 0", start(2, 0), true}, {"done 0", done(0, "1.1"), true}, {"start 1 of 3", start(3, 1), false}}, OwnerPostStages{Parts: 2, Started: 1, Done: []string{"1.1"}}},
		{"uploads before text done", []step{{"start 0", start(2, 0), true}, {"done 0", done(0, "1.1"), true}, {"uploads", uploads(2), false}}, OwnerPostStages{Parts: 2, Started: 1, Done: []string{"1.1"}}},
		{"files without uploads", []step{{"files", files, false}}, OwnerPostStages{}},
		{"uploads twice", []step{{"uploads", uploads(0), true}, {"again", uploads(0), false}}, OwnerPostStages{Uploads: []string{"F1"}}},
		{"text after uploads", []step{{"uploads", uploads(0), true}, {"start 0", start(1, 0), false}}, OwnerPostStages{Uploads: []string{"F1"}}},
		{"not sent rewinds", []step{{"start 0", start(1, 0), true}, {"not sent", notSent(0), true}, {"start again", start(1, 0), true}}, OwnerPostStages{Parts: 1, Started: 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := approved(t)
			for _, st := range c.steps {
				err := st.do(s)
				if (err == nil) != st.ok {
					t.Fatalf("%s: err %v, want ok %t", st.name, err, st.ok)
				}
				if err != nil && !errors.Is(err, ErrOwnerPostStage) {
					t.Fatalf("%s: refused with %v, not ErrOwnerPostStage", st.name, err)
				}
			}
			g, err := s.OwnerPostStage("p1")
			if err != nil {
				t.Fatal(err)
			}
			if g.Parts != c.want.Parts || g.Started != c.want.Started || !slices.Equal(g.Done, c.want.Done) || g.Files != c.want.Files || !slices.Equal(g.Uploads, c.want.Uploads) {
				t.Fatalf("stages %+v, want %+v", g, c.want)
			}
		})
	}
	// A declined or unknown post has no delivery steps.
	s := open(t)
	if _, err := s.OwnerPostStage("nope"); !errors.Is(err, ErrOwnerPostNotApproved) {
		t.Fatal(err)
	}
}
