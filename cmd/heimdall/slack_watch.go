package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/config"
	"github.com/misunders2d/heimdall/internal/core"
)

// slackWatchCommand uses the running service's own binary and configuration.
func slackWatchCommand(configPath string) string {
	bin, binErr := os.Executable()
	path, pathErr := filepath.Abs(configPath)
	if binErr != nil || pathErr != nil {
		return ""
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	return quote(bin) + " slack-watch -config " + quote(path)
}

var (
	slackWatchChannel = regexp.MustCompile(`^[CDG][A-Z0-9]+$`)
	slackWatchThread  = regexp.MustCompile(`^[0-9]+\.[0-9]{6}$`)
)

func parseSlackWatchDelay(input string) (int64, error) {
	delay, err := time.ParseDuration(input)
	if err != nil || delay <= 0 || delay%time.Second != 0 {
		return 0, errors.New("watch delay must be a positive duration in whole seconds, such as 5m")
	}
	return int64(delay / time.Second), nil
}

// slackWatch is a host-operator command, like drain. Authority comes from
// write access to the existing private data directory, not process env IDs.
func slackWatch(args []string) error {
	fs := flag.NewFlagSet("slack-watch", flag.ContinueOnError)
	path := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" || fs.NArg() == 0 {
		return errors.New("usage: heimdall slack-watch -config FILE default-delay [DELAY] | show|off CHANNEL[/THREAD] | answer CHANNEL[/THREAD] [DELAY] | work CHANNEL[/THREAD] RULE [DELAY]")
	}
	mode, target := fs.Arg(0), fs.Arg(1)
	global := mode == "default-delay"
	chat, thread, scoped := strings.Cut(target, "/")
	if global {
		chat, thread = "", ""
	} else if !slackWatchChannel.MatchString(chat) || scoped && !slackWatchThread.MatchString(thread) {
		return errors.New("expected a Slack channel ID or CHANNEL/ROOT_TIMESTAMP")
	}
	w := core.SlackWatch{Mode: mode, Thread: thread}
	switch mode {
	case "default-delay":
		if fs.NArg() > 2 {
			return errors.New("default-delay takes one optional duration, such as 5m")
		}
		if fs.NArg() == 2 {
			delay, err := parseSlackWatchDelay(target)
			if err != nil {
				return err
			}
			w.Mode, w.DelaySeconds = core.SlackWatchAnswer, delay
		}
	case "show", core.SlackWatchOff:
		if fs.NArg() != 2 {
			return errors.New("show and off only take the Slack target")
		}
	case core.SlackWatchAnswer:
		if fs.NArg() > 3 {
			return errors.New("answer takes a Slack target and an optional delay, such as 5m")
		}
		if fs.NArg() == 3 {
			delay, err := parseSlackWatchDelay(fs.Arg(2))
			if err != nil {
				return err
			}
			w.DelaySeconds = delay
		}
	case core.SlackWatchWork:
		w.Rule = strings.TrimSpace(fs.Arg(2))
		if chat[0] == 'D' || fs.NArg() < 3 || fs.NArg() > 4 || w.Rule == "" || len(w.Rule) > 4000 {
			return errors.New("work takes a channel or group target, one quoted rule of up to 4000 bytes, and an optional delay such as 5m")
		}
		if fs.NArg() == 4 {
			delay, err := parseSlackWatchDelay(fs.Arg(3))
			if err != nil {
				return err
			}
			w.DelaySeconds = delay
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
	if global {
		if fs.NArg() == 2 {
			if err := st.SetSlackWatch(chat, w); err != nil {
				return err
			}
		}
		w, err := st.SlackWatch(chat, thread)
		if err != nil {
			return err
		}
		w.Mode = core.SlackWatchAnswer
		fmt.Printf("Slack watch default delay: %s\n", w.Delay())
		return nil
	}
	if mode != "show" {
		if err := st.SetSlackWatch(chat, w); err != nil {
			return err
		}
		if mode == core.SlackWatchAnswer || w.DelaySeconds > 0 {
			w, err = st.SlackWatch(chat, thread)
			if err != nil {
				return err
			}
			fmt.Printf("Slack watch %s: %s (delay %s)\n", target, w.Mode, w.Delay())
		} else {
			fmt.Printf("Slack watch %s: %s\n", target, mode)
		}
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
	if w.Mode == core.SlackWatchAnswer {
		w.DelaySeconds = int64(w.Delay() / time.Second)
	}
	return json.NewEncoder(os.Stdout).Encode(w)
}
