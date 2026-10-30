// Command heimdall routes chat messages to agent harnesses.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/config"
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/iface/local"
	"github.com/misunders2d/heimdall/internal/iface/telegram"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

var version = "dev"

const usage = `usage:
  heimdall run -config FILE
      route messages from the configured interfaces (Telegram) until stopped
      by SIGINT/SIGTERM. The bot token is read from HEIMDALL_TELEGRAM_TOKEN.
      Logs go to stderr.
  heimdall local -config FILE [-as USER] [-chat CHAT]
      route messages typed on stdin (one per line; optional "@user #chat/thread"
      prefix) and print replies; exits after stdin ends, all work is delivered
      and every harness process has exited.
      The local interface trusts its operator: for testing only.
  heimdall harnesses -config FILE
      show where each harness is found for each role's OS user
  heimdall version`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "local":
		err = runLocal(os.Args[2:])
	case "harnesses":
		err = listHarnesses(os.Args[2:])
	case "version":
		fmt.Println("heimdall", version)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "heimdall:", err)
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
	if cfg.Telegram == nil {
		return errors.New("no interface configured: add a \"telegram\" block with bot_username")
	}
	logger := log.New(os.Stderr, "heimdall: ", log.LstdFlags)
	tg, err := telegram.New(telegram.Options{Token: os.Getenv(telegram.TokenEnv), BotUsername: cfg.Telegram.BotUsername, Log: logger, UploadDir: filepath.Join(cfg.DataDir, "uploads", "telegram")})
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
	r := router.New(router.Options{Access: cfg.Access(), Order: cfg.HarnessOrder, MaxParallel: cfg.MaxParallel, Accounts: harness.Accounts{RunAs: cfg.RunAs, Log: logger}, Log: logger, OutboxDir: filepath.Join(cfg.DataDir, "outbox"), GuardLog: guardLog(cfg), GuardLogMax: cfg.GuardLogMax()}, st, tg)
	logger.Printf("started %s: mode %s, harness order %s, data in %s", version, cfg.Mode, strings.Join(cfg.HarnessOrder, " → "), cfg.DataDir)
	err = r.Run(ctx)
	logger.Printf("stopped")
	return err
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
	r := router.New(router.Options{Access: cfg.Access(), Order: cfg.HarnessOrder, MaxParallel: cfg.MaxParallel, Accounts: harness.Accounts{RunAs: cfg.RunAs, Log: logger}, Log: logger, GuardLog: guardLog(cfg), GuardLogMax: cfg.GuardLogMax()}, st, l)
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
