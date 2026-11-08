// Command heimdall routes chat messages to agent harnesses.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/config"
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/googleoauth"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/iface/local"
	"github.com/misunders2d/heimdall/internal/iface/slack"
	"github.com/misunders2d/heimdall/internal/iface/telegram"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

var version = "dev"

const usage = `usage:
  heimdall run -config FILE
      route messages from the configured interfaces (Telegram and Slack) until stopped
      by SIGINT/SIGTERM. Tokens come from HEIMDALL_TELEGRAM_TOKEN and HEIMDALL_SLACK_{BOT,APP}_TOKEN.
      Logs go to stderr.
  heimdall local -config FILE [-as USER] [-chat CHAT]
      route messages typed on stdin (one per line; optional "@user #chat/thread"
      prefix) and print replies; exits after stdin ends, all work is delivered
      and every harness process has exited.
      The local interface trusts its operator: for testing only.
  heimdall harnesses -config FILE
      show where each harness is found for each role's OS user
  heimdall check-config -config FILE
      offline validation; missing employee account is a warning
  heimdall sched list|show ID|health|export -config FILE
      scheduled jobs: list, one job, health JSON (exit 1 on any issue), definitions as JSON
  heimdall sched control -socket PATH
      one owner-authenticated JSON operation from stdin; no direct store access
  heimdall sched mcp -socket PATH
      stdio MCP adapter for the shared owner tool catalog
  heimdall drain -config FILE [-timeout 30m]
      hold new scheduled runs and wait until no turn runs and every reply is
      delivered: exit 0 when idle, 75 on timeout (run before a restart)
  heimdall version`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "hook":
		if len(os.Args) != 3 || os.Args[2] != "slack-guard" && os.Args[2] != "employee-guard" && os.Args[2] != "input-context" {
			err = errors.New("unknown hook")
		} else if os.Args[2] == "input-context" {
			err = harness.ContextHook(os.Stdin, os.Stdout)
		} else if os.Args[2] == "employee-guard" {
			err = harness.EmployeeGuard(os.Stdin, os.Stdout)
		} else {
			err = harness.SlackGuard(os.Stdin, os.Stdout)
		}
	case "run":
		err = run(os.Args[2:])
	case "local":
		err = runLocal(os.Args[2:])
	case "harnesses":
		err = listHarnesses(os.Args[2:])
	case "check-config":
		err = checkConfig(os.Args[2:])
	case "sched":
		err = sched(os.Args[2:])
	case "drain":
		err = drain(os.Args[2:])
	case "version":
		fmt.Println("heimdall", version)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "heimdall:", err)
		if len(os.Args) == 3 && os.Args[1] == "hook" && os.Args[2] == "input-context" {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func loadConfig(fs *flag.FlagSet, args []string) (*config.Config, error) {
	path := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *path == "" {
		return nil, errors.New("-config is required")
	}
	return config.Load(*path)
}

func openStore(cfg *config.Config) (*store.Store, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	return store.Open(filepath.Join(cfg.DataDir, "heimdall.db"))
}

// run routes the configured chat interfaces until a signal or an interface
// failure (such as a token that belongs to another bot) stops it.
func run(args []string) error {
	cfg, err := loadConfig(flag.NewFlagSet("run", flag.ExitOnError), args)
	if err != nil {
		return err
	}
	if cfg.Telegram == nil && cfg.Slack == nil {
		return errors.New("no interface configured: add telegram or slack")
	}
	logger := log.New(os.Stderr, "heimdall: ", log.LstdFlags)
	st, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	access := cfg.Access()
	var interfaces []router.Interface
	if cfg.Telegram != nil {
		apiBase, fileBase, maxBytes := cfg.Telegram.APIBase, cfg.Telegram.FileBase, cfg.Telegram.MaxFileBytes
		if v := os.Getenv("HEIMDALL_TELEGRAM_API_BASE"); v != "" {
			apiBase = v
		}
		if v := os.Getenv("HEIMDALL_TELEGRAM_FILE_BASE"); v != "" {
			fileBase = v
		}
		if v := os.Getenv("HEIMDALL_TELEGRAM_MAX_FILE_BYTES"); v != "" {
			var err error
			maxBytes, err = strconv.ParseInt(v, 10, 64)
			if err != nil || maxBytes <= 0 {
				return errors.New("invalid HEIMDALL_TELEGRAM_MAX_FILE_BYTES")
			}
		}
		localRoot := cfg.Telegram.LocalFileRoot
		if v := os.Getenv("HEIMDALL_TELEGRAM_LOCAL_FILE_ROOT"); v != "" {
			localRoot = v
		}
		tg, err := telegram.New(telegram.Options{LocalFileRoot: localRoot, Access: &access, UnmentionedCommands: cfg.UnmentionedCommands(), Token: os.Getenv(telegram.TokenEnv), API: apiBase, FileBase: fileBase, MaxFileBytes: maxBytes, BotUsername: cfg.Telegram.BotUsername, Log: logger, UploadDir: filepath.Join(cfg.DataDir, "uploads", "telegram")})
		if err != nil {
			return err
		}
		interfaces = append(interfaces, tg)
	}
	if c := cfg.Slack; c != nil {
		sl, err := slack.New(slack.Options{Access: &access, OwnerSlackEnabled: cfg.OwnerSlack != nil && cfg.OwnerSlack.Enabled, Owners: cfg.Owners, BotToken: os.Getenv(slack.BotTokenEnv), AppToken: os.Getenv(slack.AppTokenEnv), TeamID: c.TeamID, DataDir: cfg.DataDir, Store: st, UnmentionedCommands: cfg.UnmentionedCommands(), MaxFileBytes: c.MaxFileBytes, EventDedupeHours: c.EventDedupeHours, DispatchConcurrency: c.DispatchConcurrency, IngressRetryWindow: time.Duration(c.IngressRetrySeconds) * time.Second, StatusName: c.StatusName, StatusUpdates: c.StatusUpdates, Log: logger})
		if err != nil {
			return err
		}
		interfaces = append(interfaces, sl)
	}
	jobs := newJobs(cfg, st, logger, interfaces)
	for _, i := range interfaces {
		if sl, ok := i.(*slack.Slack); ok && jobs != nil {
			jobs.Reverify = sl.Reverify // Slack creators are verified live at every fire
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	google, err := googleoauth.New(cfg.EmployeeGoogle, cfg.DataDir)
	if err != nil {
		// Employee Google is optional: owner and other turns keep running.
		logger.Printf("employee Google disabled: %v", err)
		google = nil
	}
	work, err := openDurable(cfg, st, logger)
	if err != nil {
		return err
	}
	if work != nil {
		defer work.Close()
	}
	r := router.New(router.Options{Work: work, Google: google, EmployeeHarnesses: cfg.EmployeeHarnesses, Harness: cfg.Harness, OwnerSlack: cfg.OwnerSlack, SlackTeam: slackTeam(cfg), Access: cfg.Access(), Order: cfg.HarnessOrder, MaxParallel: cfg.MaxParallel, Accounts: harness.Accounts{RunAs: cfg.RunAs, Log: logger}, Log: logger, OutboxDir: filepath.Join(cfg.DataDir, "outbox"), GuardLog: guardLog(cfg), GuardLogMax: cfg.GuardLogMax(), Jobs: jobs}, st, interfaces...)
	if cfg.Scheduler.On() && cfg.Scheduler.Control != nil {
		listener, err := func() (*net.UnixListener, error) {
			u, err := user.Lookup(cfg.RunAs[core.RoleOwner].User)
			if err != nil {
				return nil, err
			}
			uid, err := strconv.ParseUint(u.Uid, 10, 32)
			if err != nil {
				return nil, err
			}
			return jobs.ListenControl(ctx, cfg.Scheduler.Control.Owner, uint32(uid), filepath.Join(cfg.DataDir, "scheduler-control"), cfg.Scheduler.Control.Source)
		}()
		if err != nil {
			logger.Printf("scheduler control unavailable (router continues): %v", err)
		} else {
			defer listener.Close()
		}
	}
	if work != nil {
		work.SetWake(r.Wake)
		if cfg.Durable.Review {
			work.SetReviewer(r)
		}
		if cfg.Durable.Dispatch {
			work.SetDispatcher(r, r.OwnsRoute)
		}
		if err := work.Launch(); err != nil {
			return fmt.Errorf("durable commitments: %w", err)
		}
	}
	logger.Printf("started %s: mode %s, harness order %s, data in %s", version, cfg.Mode, strings.Join(cfg.HarnessOrder, " → "), cfg.DataDir)
	err = r.Run(ctx)
	logger.Printf("stopped")
	return err
}

// openDurable opens durable commitments when configured (docs/durable.md):
// <data_dir>/durable.db beside heimdall.db. Disabled by default.
func openDurable(cfg *config.Config, st *store.Store, logger *log.Logger) (*durable.Runtime, error) {
	d := cfg.Durable
	if !d.On() {
		return nil, nil
	}
	res := map[string]durable.Resource{}
	for name, r := range d.Resources {
		res[name] = durable.Resource{Concurrency: r.Concurrency, PerWorker: r.PerWorker, RateLimit: r.RateLimit, RatePeriod: time.Duration(r.RatePeriodSeconds) * time.Second}
	}
	var obs durable.Observer
	if len(d.T3Command) > 0 {
		if !filepath.IsAbs(d.T3Command[0]) {
			return nil, errors.New("durable.t3_command must start with an absolute path")
		}
		obs = durable.T3{Command: d.T3Command}
	}
	return durable.Open(durable.Options{Path: filepath.Join(cfg.DataDir, "durable.db"), Outbox: st, Observer: obs, Observe: time.Duration(d.ObserveSeconds) * time.Second, Resources: res, Silence: time.Duration(d.SilenceSeconds) * time.Second, Linear: d.Linear, Log: logger})
}

// guardLog is the prompt-injection log: metadata of flagged messages, never
// their text.
func guardLog(cfg *config.Config) string { return filepath.Join(cfg.DataDir, "prompt-guard.log") }

func runLocal(args []string) error {
	fs := flag.NewFlagSet("local", flag.ExitOnError)
	as := fs.String("as", "owner", "default sender ID (principal local:ID)")
	chat := fs.String("chat", "local", "default chat")
	cfg, err := loadConfig(fs, args)
	if err != nil {
		return err
	}
	st, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	l := local.New(os.Stdin, os.Stdout, *as, *chat)
	logger := log.New(os.Stderr, "heimdall: ", log.LstdFlags)
	r := router.New(router.Options{EmployeeHarnesses: cfg.EmployeeHarnesses, Harness: cfg.Harness, OwnerSlack: cfg.OwnerSlack, SlackTeam: slackTeam(cfg), Access: cfg.Access(), Order: cfg.HarnessOrder, MaxParallel: cfg.MaxParallel, Accounts: harness.Accounts{RunAs: cfg.RunAs, Log: logger}, Log: logger, GuardLog: guardLog(cfg), GuardLogMax: cfg.GuardLogMax(), Jobs: newJobs(cfg, st, logger, []router.Interface{l})}, st, l)
	go func() {
		select {
		case <-l.EOF():
		case <-ctx.Done():
			return
		}
		for !r.Idle() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		cancel()
	}()
	return r.Run(ctx)
}

func listHarnesses(args []string) error {
	cfg, err := loadConfig(flag.NewFlagSet("harnesses", flag.ExitOnError), args)
	if err != nil {
		return err
	}
	accounts := harness.Accounts{RunAs: cfg.RunAs}
	for _, role := range []core.Role{core.RoleOwner, core.RoleEmployee} {
		runAs, ok := cfg.RunAs[role]
		if !ok {
			continue
		}
		fmt.Printf("%s (OS user %s):\n", role, runAs.User)
		for _, h := range harness.Names() {
			l, err := accounts.Prepare(role, role == core.RoleOwner, harness.Binary(h))
			if err != nil {
				fmt.Printf("  %-7s unavailable: %v\n", h, err)
				continue
			}
			fmt.Printf("  %-7s %s\n", h, l.Path)
		}
	}
	return nil
}

// newJobs is the scheduler when the config enables it, else nil.
func newJobs(cfg *config.Config, st *store.Store, logger *log.Logger, ifaces []router.Interface) *scheduler.Scheduler {
	if !cfg.Scheduler.On() {
		return nil
	}
	var names []string
	for _, i := range ifaces {
		names = append(names, i.Name())
	}
	return scheduler.New(schedulerConfig(cfg, names), st, cfg.Access(), logger)
}

// schedulerConfig is the scheduler's part of cfg. Without a scheduler
// section (heimdall sched on a config that never enabled it), the grace
// defaults apply.
func schedulerConfig(cfg *config.Config, interfaces []string) scheduler.Config {
	c := cfg.Scheduler
	if c == nil {
		c = &config.Scheduler{GraceRecurringSeconds: 7200, GraceOneOffSeconds: 86400}
	}
	return scheduler.Config{DefaultTZ: c.DefaultTZ, MaxConcurrent: c.MaxConcurrent, PerCreator: c.PerCreator,
		GraceRecurring: time.Duration(c.GraceRecurringSeconds) * time.Second, GraceOneOff: time.Duration(c.GraceOneOffSeconds) * time.Second,
		PauseAfter: c.PauseAfter, EmployeeHarnesses: cfg.EmployeeHarnesses, Interfaces: interfaces}
}

// sched is the operator's view of scheduled jobs. It works whether the
// scheduler is enabled or not: jobs are kept either way.
func sched(args []string) error {
	if len(args) > 0 && (args[0] == "control" || args[0] == "mcp") {
		return schedControl(args)
	}
	if len(args) == 0 {
		return errors.New("usage: heimdall sched list|show ID|health|export -config FILE")
	}
	sub, id := args[0], ""
	args = args[1:]
	if sub == "show" {
		if len(args) == 0 || strings.HasPrefix(args[0], "-") {
			return errors.New("usage: heimdall sched show ID -config FILE")
		}
		id, args = args[0], args[1:]
	}
	cfg, err := loadConfig(flag.NewFlagSet("sched", flag.ExitOnError), args)
	if err != nil {
		return err
	}
	st, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	s := scheduler.New(schedulerConfig(cfg, nil), st, cfg.Access(), nil)
	switch sub {
	case "list", "show":
		fmt.Println(s.Admin(sub, id))
	case "health":
		h := s.Health(time.Duration(cfg.Harness.RunTimeoutSeconds) * time.Second)
		out, _ := json.MarshalIndent(h, "", "  ")
		fmt.Println(string(out))
		if !h.OK {
			os.Exit(1)
		}
	case "export":
		out, err := s.Export()
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	default:
		return fmt.Errorf("unknown sched command %q", sub)
	}
	return nil
}

// drain holds new scheduled runs and waits until Heimdall is idle: exit 0,
// or 75 (EX_TEMPFAIL) on timeout.
func drain(args []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	timeout := fs.Duration("timeout", 30*time.Minute, "how long to wait")
	cfg, err := loadConfig(fs, args)
	if err != nil {
		return err
	}
	st, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	idle, err := scheduler.Drain(ctx, st, *timeout, time.Second)
	if err != nil {
		return err
	}
	if !idle {
		fmt.Fprintln(os.Stderr, "heimdall: drain timed out: turns still running or replies undelivered")
		st.Close()
		os.Exit(75)
	}
	fmt.Println("idle")
	return nil
}

func slackTeam(cfg *config.Config) string {
	if cfg.Slack != nil {
		return cfg.Slack.TeamID
	}
	return ""
}

func checkConfig(args []string) error {
	fs := flag.NewFlagSet("check-config", flag.ContinueOnError)
	path := fs.String("config", "", "config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" || fs.NArg() != 0 {
		return errors.New("-config FILE is required, with no positional arguments")
	}
	warnings, err := config.CheckFile(*path)
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		fmt.Fprintln(os.Stderr, "warning:", warning)
	}
	fmt.Println("configuration valid (offline; credentials and interfaces not checked)")
	return nil
}
