package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/misunders2d/heimdall/internal/config"
	"github.com/misunders2d/heimdall/internal/core"
)

var (
	slackWatchChannel = regexp.MustCompile(`^[CDG][A-Z0-9]+$`)
	slackWatchThread  = regexp.MustCompile(`^[0-9]+\.[0-9]{6}$`)
)

// slackWatch is a host-operator command, like drain. Authority comes from
// write access to the existing private data directory, not process env IDs.
func slackWatch(args []string) error {
	fs := flag.NewFlagSet("slack-watch", flag.ContinueOnError)
	path := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" || fs.NArg() < 2 {
		return errors.New("usage: heimdall slack-watch -config FILE show|off|answer|work CHANNEL[/THREAD] [RULE]")
	}
	mode, target := fs.Arg(0), fs.Arg(1)
	chat, thread, scoped := strings.Cut(target, "/")
	if !slackWatchChannel.MatchString(chat) || scoped && !slackWatchThread.MatchString(thread) {
		return errors.New("expected a Slack channel ID or CHANNEL/ROOT_TIMESTAMP")
	}
	w := core.SlackWatch{Mode: mode, Thread: thread}
	switch mode {
	case "show", core.SlackWatchOff, core.SlackWatchAnswer:
		if fs.NArg() != 2 {
			return errors.New("show, answer and off only take the Slack target")
		}
	case core.SlackWatchWork:
		w.Rule = strings.TrimSpace(fs.Arg(2))
		if chat[0] == 'D' || fs.NArg() != 3 || w.Rule == "" || len(w.Rule) > 4000 {
			return errors.New("work takes one quoted rule of up to 4000 bytes and a channel or group target")
		}
	default:
		return errors.New("watch mode must be show, off, answer or work")
	}
	raw, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	cfg, err := config.Parse(raw)
	if err != nil {
		return err
	}
	if cfg.Slack == nil || cfg.Slack.WorkOwnerID == "" || !cfg.Durable.On() || !cfg.Durable.Assistant {
		return errors.New("Slack watches require a configured Slack work_owner_id and durable assistant work")
	}
	st, err := openAdminStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	if mode != "show" {
		if err := st.SetSlackWatch(chat, w); err != nil {
			return err
		}
		fmt.Printf("Slack watch %s: %s\n", target, mode)
		return nil
	}
	w, err = st.SlackWatch(chat, thread)
	if err != nil {
		return err
	}
	if w.Mode == "" {
		if rule := cfg.Slack.ConversationWork[chat]; rule != "" {
			w.Mode, w.Rule = core.SlackWatchWork, rule
		} else {
			w.Mode = core.SlackWatchOff
		}
	}
	return json.NewEncoder(os.Stdout).Encode(w)
}
