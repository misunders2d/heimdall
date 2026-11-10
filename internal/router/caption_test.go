package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

type fitIface struct{ max int }

func (fitIface) Name() string                                                         { return "fit" }
func (fitIface) Run(context.Context, func(context.Context, core.Inbound) error) error { return nil }
func (fitIface) Send(context.Context, core.Outbound) error                            { return nil }
func (f fitIface) CaptionFits(c string) bool                                          { return len(c) <= f.max }

// A caption its interface cannot fit is never cut: the file row goes bare
// and the whole caption follows it as a formatted reply row; captions that
// fit, bare files and interfaces without a limit are left alone.
func TestSplitCaptions(t *testing.T) {
	r := &Router{ifaces: map[string]Interface{"fit": fitIface{max: 5}, "any": nil}}
	fit, other := core.ConvKey{Interface: "fit", Chat: "1"}, core.ConvKey{Interface: "any", Chat: "1"}
	rows := []core.Outbound{
		{Conv: fit, ReplyTo: "9", File: &core.File{Name: "a.gif", Caption: "short"}},
		{Conv: fit, ReplyTo: "9", File: &core.File{Name: "b.gif", Caption: "too long"}},
		{Conv: fit, File: &core.File{Name: "c.txt"}},
		{Conv: other, File: &core.File{Name: "d.gif", Caption: "too long"}},
	}
	var got []string
	for _, o := range r.splitCaptions(rows) {
		if o.File != nil {
			got = append(got, fmt.Sprintf("%s file %s %q", o.Conv.Interface, o.File.Name, o.File.Caption))
		} else {
			got = append(got, fmt.Sprintf("%s text %q formatted=%v reply=%s", o.Conv.Interface, o.Text, o.Formatted, o.ReplyTo))
		}
	}
	want := fmt.Sprint([]string{`fit file a.gif "short"`, `fit file b.gif ""`, `fit text "too long" formatted=true reply=9`, `fit file c.txt ""`, `any file d.gif "too long"`})
	if fmt.Sprint(got) != want {
		t.Fatalf("rows:\n%v\nwant\n%v", got, want)
	}
	if rows[1].File.Caption != "too long" {
		t.Fatal("input row changed")
	}
}
