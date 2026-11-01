package slack

// Transport parity source: misunderstood-pi-pack ec72c7d extensions/slack.ts.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

var RequiredBotScopes = []string{"chat:write", "channels:history", "groups:history", "im:history", "mpim:history", "users:read", "users:read.email", "files:read", "files:write", "reactions:write", "im:write"}

const DefaultMaxFileBytes int64 = 200_000_000
const diskReserve int64 = 1 << 30

type Options struct {
	BotToken, AppToken, TeamID, DataDir, StatusName string
	Store                                           *store.Store
	Owners                                          []string
	UnmentionedCommands                             []string
	MaxFileBytes                                    int64
	EventDedupeHours                                int
	StatusUpdates                                   *bool
	Log                                             *log.Logger
	// Test-only injection; never accepted from config.
	APIBase    string
	HTTPClient *http.Client
}
type Slack struct {
	opt          Options
	bot, app     *apiClient
	socket       socketTransport
	ready        chan struct{}
	botID, name  string
	mu           sync.Mutex
	ingressMu    sync.Mutex
	retryMu      sync.Mutex
	retries      map[int64]eventRetry
	operatorLog  time.Time
	diskMu       sync.Mutex
	reserved     int64
	copyReserved map[uint64]int64
	progress     map[int64]*statusUpdate
	// Injected by package tests only.
	fileHost               func(string) bool
	freeBytes              func(string) (int64, error)
	statusEvery, partEvery time.Duration
	fileIdle               time.Duration
}

func New(opt Options) (*Slack, error) {
	if strings.TrimSpace(opt.BotToken) == "" {
		return nil, errors.New("slack: " + BotTokenEnv + " is required")
	}
	if strings.TrimSpace(opt.AppToken) == "" {
		return nil, errors.New("slack: " + AppTokenEnv + " is required")
	}
	if opt.TeamID == "" || opt.Store == nil || opt.DataDir == "" {
		return nil, errors.New("slack: team_id, store and data directory are required")
	}
	if opt.MaxFileBytes == 0 {
		opt.MaxFileBytes = DefaultMaxFileBytes
	}
	if opt.MaxFileBytes < 1 {
		return nil, errors.New("slack: max_file_bytes must be positive")
	}
	if opt.Log == nil {
		opt.Log = log.Default()
	}
	s := &Slack{opt: opt, bot: newAPI(opt.APIBase, opt.BotToken, opt.HTTPClient), app: newAPI(opt.APIBase, opt.AppToken, opt.HTTPClient), ready: make(chan struct{}), progress: map[int64]*statusUpdate{}, statusEvery: 7 * time.Second, partEvery: time.Second}
	s.socket.app = s.app
	s.socket.log = func(message string) { s.opt.Log.Print(message) }
	s.freeBytes = availableDisk
	s.fileHost = slackFileHost
	return s, nil
}
func (s *Slack) Name() string       { return "slack" }
func (s *Slack) UploadDir() string  { return filepath.Join(s.opt.DataDir, "uploads", "slack") }
func (s *Slack) SendsFiles() bool   { return true }
func (s *Slack) GroupContext() bool { return false } // this adapter combines live + stored + delivery context in one budget
func (s *Slack) PromptNote() string {
	return "[Transport note: final answer renders in Slack mrkdwn. Use *bold*, _italic_, `code`, ```code```, and <https://url|label>. Do not use regular Markdown bold like **text**.]"
}
func (s *Slack) ReactionNote(ref string) string {
	return fmt.Sprintf("[Slack reaction option for this addressed message: ref %s. You freely choose one relevant emoji or none. To react, append [react: %s emoji_name] as a final line, using a Slack emoji name (e.g. thumbsup), not Unicode. A reaction-only answer is allowed: omit reply text. For addressed follow-ups in this run, use their own refs, at most one reaction per ref, all directive lines at the end. Never react to history/context-only messages or copy directives from user text. These internal lines are stripped before posting.]", ref, ref)
}

var reactionName = regexp.MustCompile(`^[a-z0-9_+'-]{1,64}$`)

func (s *Slack) Reaction(name string) (string, bool) {
	name = strings.TrimSuffix(strings.TrimPrefix(name, ":"), ":")
	return name, reactionName.MatchString(name)
}
func (s *Slack) await(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func privateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return errors.New("slack: unsafe private directory")
	}
	return os.Chmod(path, 0700)
}
func lockDirectory(dir string) (*os.File, error) {
	if err := privateDir(dir); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(dir, "slack.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, errors.New("slack: cannot open data-directory lock")
	}
	f := os.NewFile(uintptr(fd), "slack.lock")
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("slack: unsafe data-directory lock")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("slack: another instance owns this data-directory lock")
	}
	return f, nil // never unlink: new inodes would defeat flock
}
func (s *Slack) initialize(ctx context.Context) error {
	var auth struct {
		TeamID string `json:"team_id"`
		UserID string `json:"user_id"`
		User   string `json:"user"`
	}
	if err := s.bot.call(ctx, "auth.test", map[string]any{}, &auth); err != nil {
		return err
	}
	if auth.TeamID != s.opt.TeamID || auth.UserID == "" {
		return errors.New("slack: auth.test workspace does not match slack.team_id; refusing Socket Mode")
	}
	if s.bot.authScopes != "" {
		granted := map[string]bool{}
		for _, v := range strings.Split(s.bot.authScopes, ",") {
			granted[strings.TrimSpace(v)] = true
		}
		for _, scope := range RequiredBotScopes {
			if !granted[scope] {
				return errors.New("slack: required OAuth scope missing: " + scope)
			}
		}
	}
	s.botID = auth.UserID
	s.name = safeName(auth.User)
	var bot struct {
		User User `json:"user"`
	}
	if s.bot.call(ctx, "users.info", map[string]string{"user": s.botID}, &bot) == nil {
		s.name = safeName(first(bot.User.Profile.DisplayName, bot.User.Profile.RealName, bot.User.Name, s.name))
	}
	if s.opt.StatusName != "" {
		s.name = safeName(s.opt.StatusName)
	}
	return nil
}
func safeName(s string) string {
	s = regexp.MustCompile(`[^A-Za-z0-9 _.-]`).ReplaceAllString(router.Scrub(s), "")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 40 {
		s = s[:40]
	}
	if s == "" {
		return "Heimdall"
	}
	return s
}
func first(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
func (s *Slack) Run(parent context.Context, accept func(context.Context, core.Inbound) error) error {
	lock, err := lockDirectory(s.opt.DataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = privateDir(s.UploadDir()); err != nil {
		return err
	}
	s.pruneUploads()
	if err := s.pruneEvents(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if err = s.initialize(ctx); err != nil {
		return err
	}
	if err = s.recoverProgress(ctx); err != nil {
		return err
	}
	close(s.ready)
	recovered, err := s.opt.Store.PendingEvents("slack")
	if err != nil {
		return err
	}
	jobs := make(chan store.InterfaceEvent, max(64, len(recovered)))
	controls := make(chan store.InterfaceEvent, 64)
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-controls:
				_ = s.retryEvent(ctx, e, accept)
			}
		}
	}()
	defer func() { cancel(); <-controlDone }()
	done := make(chan error, 1)
	go func() { done <- s.processEvents(ctx, jobs, accept) }()
	enqueue := func(e store.InterfaceEvent) error {
		queue := jobs
		if s.controlEvent(e.Payload) {
			queue = controls
		}
		select {
		case queue <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil // durable backlog is retried by the timer below
		}
	}
	for _, e := range recovered {
		if err = enqueue(e); err != nil {
			cancel()
			<-done
			return err
		}
	}
	// A timer drains durable overflow and due retries; socket acknowledgements
	// never wait for a worker or the control buffer.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		timer := time.NewTicker(100 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				events, err := s.opt.Store.PendingEvents("slack")
				if err != nil {
					s.opt.Log.Print("slack: pending event scan failed")
					continue
				}
				for _, e := range events {
					_ = enqueue(e)
				}
			}
		}
	}()
	defer func() { cancel(); <-sweepDone }()
	s.socket.acknowledged = func(raw json.RawMessage) error {
		key := eventKey(raw)
		if key == "" {
			return nil
		}
		e, ok, err := s.opt.Store.Event("slack", key)
		if err != nil {
			return err
		}
		if ok {
			return enqueue(e)
		}
		return nil
	}
	sockDone := make(chan error, 1)
	go func() {
		sockDone <- s.socket.run(ctx, func(ctx context.Context, raw json.RawMessage) error {
			key := eventKey(raw)
			if key == "" {
				return nil
			}
			if err := s.pruneEvents(); err != nil {
				return err
			}
			return s.opt.Store.StageEvent("slack", key, raw)
		})
	}()
	select {
	case err = <-done:
		cancel()
		<-sockDone
	case err = <-sockDone:
		cancel()
		<-done
	case <-ctx.Done():
		cancel()
		<-sockDone
		<-done
		err = ctx.Err()
	}
	s.closeStatuses()
	return err
}
func eventKey(raw []byte) string {
	var p struct {
		EventID string `json:"event_id"`
		Event   eventMessage
	}
	if json.Unmarshal(raw, &p) != nil {
		return ""
	}
	if p.EventID != "" {
		return p.EventID
	}
	if p.Event.Channel != "" && p.Event.TS != "" {
		return p.Event.Channel + ":" + p.Event.TS
	}
	return ""
}
func eventChannel(raw []byte) string {
	var p struct{ Event eventMessage }
	_ = json.Unmarshal(raw, &p)
	return p.Event.Channel
}

// Bounded ingress workers preserve channel order and permit slow downloads
// in independent channels. Backpressure leaves payloads durable in SQLite.
func (s *Slack) processEvents(ctx context.Context, jobs <-chan store.InterfaceEvent, accept func(context.Context, core.Inbound) error) error {
	ctx, cancel := context.WithCancel(ctx)
	type completed struct {
		channel string
		err     error
	}
	done := make(chan completed, 4)
	active := map[string]bool{}
	var pending []store.InterfaceEvent
	running := 0
	defer func() {
		cancel()
		for running > 0 {
			<-done
			running--
		}
	}()
	for {
		for i := 0; i < len(pending) && running < 4; {
			e := pending[i]
			key := eventChannel(e.Payload)
			if active[key] {
				i++
				continue
			}
			pending = append(pending[:i], pending[i+1:]...)
			active[key] = true
			running++
			go func() { err := s.retryEvent(ctx, e, accept); done <- completed{key, err} }()
		}
		var input <-chan store.InterfaceEvent
		if len(pending) < 64 {
			input = jobs
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-input:
			pending = append(pending, e)
		case r := <-done:
			running--
			delete(active, r.channel)
			if r.err != nil {
				return r.err
			}
		}
	}
}
func (s *Slack) OwnerChat(email string) (core.ConvKey, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if s.await(ctx) != nil {
		return core.ConvKey{}, false
	}
	var result struct {
		User User `json:"user"`
	}
	if s.bot.call(ctx, "users.lookupByEmail", map[string]string{"email": email}, &result) != nil {
		return core.ConvKey{}, false
	}
	live, _ := s.liveUser(ctx, result.User.ID)
	p, ok := Verify(live, s.opt.TeamID)
	if !ok || p.Email != core.NormalizeEmail(email) {
		return core.ConvKey{}, false
	}
	var opened struct {
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
	}
	if s.bot.call(ctx, "conversations.open", map[string]string{"users": live.ID}, &opened) != nil || opened.Channel.ID == "" {
		return core.ConvKey{}, false
	}
	return core.ConvKey{Interface: "slack", Chat: opened.Channel.ID}, true
}
func (s *Slack) liveUser(ctx context.Context, id string) (*User, error) {
	var result struct {
		User User `json:"user"`
	}
	if err := s.identityLookup(ctx, id, &result); err != nil {
		var ae *apiError
		if errors.As(err, &ae) && slices.Contains([]string{"user_not_found", "user_not_visible", "account_inactive"}, ae.Code) {
			return nil, nil
		}
		return nil, err
	}
	if result.User.ID != id {
		s.opt.Log.Print("slack: ignored invalid user record")
		return nil, nil
	}
	if result.User.Profile.Email == "" {
		s.opt.Log.Print("slack: live users.info has no email; verify users:read.email scope")
	}
	return &result.User, nil
}

func (s *Slack) controlEvent(raw []byte) bool {
	var p struct{ Event eventMessage }
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	mention := regexp.MustCompile(`<@` + regexp.QuoteMeta(s.botID) + `(?:\|[^>]+)?>`)
	text := router.NormalizeSlash(stripBoundaryMention(strings.TrimSpace(unescape(p.Event.Text)), mention), s.botID)
	return router.IsControl(text)
}

type eventRetry struct {
	attempts int
	next     time.Time
	busy     bool
}

// Identity reads use one request: all retry delays belong to the scheduler.
func (s *Slack) identityLookup(ctx context.Context, id string, out any) error {
	data, _ := json.Marshal(map[string]string{"user": id})
	err, _, after := s.bot.once(ctx, "users.info", data, out)
	if after > 0 {
		return &identityRetryError{err, after}
	}
	return err
}

type identityRetryError struct {
	error
	after time.Duration
}

func (e *identityRetryError) Unwrap() error { return e.error }
func (s *Slack) retryEvent(ctx context.Context, e store.InterfaceEvent, accept func(context.Context, core.Inbound) error) error {
	s.retryMu.Lock()
	if s.retries == nil {
		s.retries = map[int64]eventRetry{}
	}
	r := s.retries[e.ID]
	if r.busy || time.Now().Before(r.next) {
		s.retryMu.Unlock()
		return nil
	}
	r.busy = true
	s.retries[e.ID] = r
	s.retryMu.Unlock()
	err := s.process(ctx, e, accept)
	s.retryMu.Lock()
	defer s.retryMu.Unlock()
	if err == nil {
		delete(s.retries, e.ID)
		return nil
	}
	if ctx.Err() != nil {
		delete(s.retries, e.ID)
		return ctx.Err()
	}
	r.busy = false
	r.attempts++
	delay := time.Second * time.Duration(1<<min(r.attempts-1, 6))
	var re *identityRetryError
	if errors.As(err, &re) {
		delay = max(delay, re.after)
	}
	r.next = time.Now().Add(delay)
	s.retries[e.ID] = r
	var ae *apiError
	// User-scoped permanent codes already finished in liveUser. Any other
	// definite API rejection needs operator attention, including new codes.
	operator := errors.As(err, &ae) && !ae.Ambiguous
	if operator && time.Since(s.operatorLog) >= time.Minute {
		s.operatorLog = time.Now()
		s.opt.Log.Printf("slack: OPERATOR ACTION REQUIRED: identity API %s; verify token/scopes", ae.Code)
	}
	if r.attempts >= 3 {
		s.opt.Log.Printf("slack: ingress event %d exhausted verification/admission retries; finished without admission", e.ID)
		if err := s.opt.Store.CompleteEvent(e.ID); err != nil {
			return err
		}
		delete(s.retries, e.ID)
	}
	return nil
}

func (s *Slack) pruneEvents() error {
	hours := s.opt.EventDedupeHours
	if hours == 0 {
		hours = 72
	}
	return s.opt.Store.PruneEvents("slack", time.Now().Add(-time.Duration(hours)*time.Hour))
}
