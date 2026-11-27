package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// Owner post states (docs/owner-post.md).
const (
	OwnerPostPending  = "pending"  // its card waits for the owner's Yes or No
	OwnerPostApproved = "approved" // its delivery row is queued (or sent)
	OwnerPostDeclined = "declined"
)

// OwnerPost is one request to post to Slack as the owner: the exact text
// and Heimdall's own checked copies of its files, the card conversation
// (the owner's private chat with the bot) and the decision.
type OwnerPost struct {
	ID      string
	Creator string // the requesting owner's principal key
	Card    core.ConvKey
	Target  core.ConvKey // slack chat and thread
	Label   string       // destination as shown on the card
	Text    string
	Files   []core.File
	Origin  string // who asked, for the card: this chat, an external launcher run, trusted T3
	// SlackUser and SlackTeam: the owner's Slack account verified live at
	// the request; only that account's token ever sends the post.
	SlackUser, SlackTeam string
	State                string
	Expires              time.Time
	Created              time.Time
	// CardMessage is the card, queued with the post (not stored here); its
	// outbound row is linked so its confirmed delivery can be told.
	CardMessage *core.Outbound
	// CardMark is the newest inbound row stored when the card was confirmed
	// sent; -1 while it was not. CardRef is the card's (last part's)
	// platform message ID as sent. Only a reply stored later and sent after
	// CardRef on the platform answers it.
	CardMark int64
	CardRef  string
}

// CardSeenBefore reports the post's card confirmed delivered before the
// reply: stored before the reply's inbound row, and earlier on the platform
// than its message ID msg (same chat). A card without a receipt (CardRef
// empty: a part's ID missing, zero or out of order) never is.
func (p OwnerPost) CardSeenBefore(reply int64, msg string) bool {
	return p.CardMark >= 0 && p.CardMark < reply && p.CardRef != "" && PlatformBefore(p.CardRef, msg)
}

// CardUnconfirmed reports a card that was sent but has no receipt: it can
// never be approved (a No still cancels it).
func (p OwnerPost) CardUnconfirmed() bool { return p.CardMark >= 0 && p.CardRef == "" }

// CardReceipt is a sent message's receipt: the last part's platform ID,
// only when the interface reported one valid ID for every part, each
// strictly after the one before (same format). Anything else: none.
func CardReceipt(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	for i, id := range parts {
		if _, _, ok := platformID(id); !ok {
			return ""
		}
		if i > 0 && !PlatformBefore(parts[i-1], id) {
			return ""
		}
	}
	return parts[len(parts)-1]
}

// platformID parses a Telegram message ID (a positive integer, frac -1) or
// a Slack timestamp ("<seconds>.<micro>", positive seconds).
func platformID(s string) (sec, frac int64, ok bool) {
	whole, part, dotted := strings.Cut(s, ".")
	if whole == "" || len(whole) > 18 || strings.Trim(whole, "0123456789") != "" {
		return 0, 0, false
	}
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || sec <= 0 {
		return 0, 0, false
	}
	if !dotted {
		return sec, -1, true
	}
	if part == "" || len(part) > 9 || strings.Trim(part, "0123456789") != "" {
		return 0, 0, false
	}
	frac, _ = strconv.ParseInt(part+strings.Repeat("0", 9-len(part)), 10, 64)
	return sec, frac, true
}

// PlatformBefore reports message ID a sent before b in one chat: Telegram
// message IDs (positive integers) or Slack timestamps ("<seconds>.<micro>",
// compared by their integer parts, never as floats). Anything else,
// zero included: false.
func PlatformBefore(a, b string) bool {
	as, af, aok := platformID(a)
	bs, bf, bok := platformID(b)
	if !aok || !bok || (af < 0) != (bf < 0) {
		return false
	}
	return as < bs || (as == bs && af < bf)
}

// OwnerPostDecision approves or declines a pending post. Approving queues
// Out in the same transaction; a post no longer pending (answered, expired)
// is left as it is and nothing is queued.
type OwnerPostDecision struct {
	ID, Creator string
	Approve     bool
	Out         *core.Outbound
	At          time.Time // the answer's time (the scheduler's clock); zero: now
}

func insertOwnerPost(tx *sql.Tx, p OwnerPost) error {
	files, err := json.Marshal(p.Files)
	if err != nil {
		return err
	}
	created := unix(p.Created)
	if created == 0 {
		created = now()
	}
	decided := int64(0)
	if p.State != OwnerPostPending {
		decided = created
	}
	_, err = tx.Exec(`INSERT INTO owner_posts(id,creator,card_iface,card_chat,card_thread,target_chat,target_thread,label,text,files,origin,state,expires_at,created_at,decided_at,slack_user,slack_team) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		p.ID, p.Creator, p.Card.Interface, p.Card.Chat, p.Card.Thread, p.Target.Chat, p.Target.Thread, p.Label, p.Text, string(files), p.Origin, p.State, unix(p.Expires), created, decided, p.SlackUser, p.SlackTeam)
	if err != nil || p.CardMessage == nil {
		return err
	}
	card, err := enqueue(tx, *p.CardMessage)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE owner_posts SET card_out=? WHERE id=?`, card, p.ID)
	return err
}

func decideOwnerPost(tx *sql.Tx, d OwnerPostDecision) error {
	state := OwnerPostDeclined
	if d.Approve {
		state = OwnerPostApproved
	}
	at := unix(d.At)
	if at == 0 {
		at = now()
	}
	res, err := tx.Exec(`UPDATE owner_posts SET state=?,decided_at=? WHERE id=? AND creator=? AND state=? AND expires_at>?`, state, at, d.ID, d.Creator, OwnerPostPending, at)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 && d.Approve && d.Out != nil {
		_, err = enqueue(tx, *d.Out)
	}
	return err
}

const ownerPostCols = `id,creator,card_iface,card_chat,card_thread,target_chat,target_thread,label,text,files,origin,state,expires_at,created_at,card_mark,slack_user,slack_team,card_ref`

func scanOwnerPost(row interface{ Scan(...any) error }) (OwnerPost, error) {
	var p OwnerPost
	var files string
	var expires, created int64
	if err := row.Scan(&p.ID, &p.Creator, &p.Card.Interface, &p.Card.Chat, &p.Card.Thread, &p.Target.Chat, &p.Target.Thread, &p.Label, &p.Text, &files, &p.Origin, &p.State, &expires, &created, &p.CardMark, &p.SlackUser, &p.SlackTeam, &p.CardRef); err != nil {
		return p, err
	}
	p.Target.Interface = core.PlatformSlack
	if err := json.Unmarshal([]byte(files), &p.Files); err != nil {
		return p, err
	}
	p.Expires, p.Created = ms(expires), ms(created)
	return p, nil
}

// PendingOwnerPosts are creator's posts waiting for an answer in card,
// oldest first; expired ones are left out. Whether a reply may answer one
// is CardSeenBefore.
func (s *Store) PendingOwnerPosts(creator string, card core.ConvKey, at time.Time) ([]OwnerPost, error) {
	rows, err := s.db.Query(`SELECT `+ownerPostCols+` FROM owner_posts WHERE creator=? AND card_iface=? AND card_chat=? AND card_thread=? AND state=? AND expires_at>? ORDER BY created_at, id`,
		creator, card.Interface, card.Chat, card.Thread, OwnerPostPending, at.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OwnerPost
	for rows.Next() {
		p, err := scanOwnerPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// OwnerPostByID is one stored post.
func (s *Store) OwnerPostByID(id string) (OwnerPost, bool, error) {
	p, err := scanOwnerPost(s.db.QueryRow(`SELECT `+ownerPostCols+` FROM owner_posts WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	return p, err == nil, err
}

// OwnerPostStages is how far an approved post's delivery got, recorded in
// the store before each next step (docs/owner-post.md, "Delivery"): a
// retry never writes a step recorded done, and checks Slack before writing
// one that may have started.
type OwnerPostStages struct {
	Parts   int      `json:"parts"`   // text parts, fixed by the first write
	Started int      `json:"started"` // parts whose write may have begun
	Done    []string `json:"done"`    // each confirmed part's Slack ts, in order
	Files   bool     `json:"files"`   // the files' upload was completed (confirmed)
	// Uploads are the Slack file IDs recorded before the upload was
	// completed: recorded only once every text part is done.
	Uploads []string `json:"-"`
}

// TextDone reports every one of parts text parts confirmed; recorded
// uploads prove it too.
func (g OwnerPostStages) TextDone(parts int) bool {
	return len(g.Uploads) > 0 || (g.Parts == parts || g.Parts == 0 && parts == 0) && len(g.Done) == parts
}

// ErrOwnerPostStage refuses a delivery step out of order or for a post
// whose text no longer splits as it did: nothing more of it is sent.
// ErrOwnerPostNotApproved: the post is missing or not approved.
var (
	ErrOwnerPostStage       = errors.New("owner post delivery out of order")
	ErrOwnerPostNotApproved = errors.New("owner post is not approved")
)

// OwnerPostStage is an approved post's delivery progress.
func (s *Store) OwnerPostStage(id string) (OwnerPostStages, error) {
	return ownerPostStage(s.db.QueryRow(`SELECT stages,uploads,state FROM owner_posts WHERE id=?`, id))
}

func ownerPostStage(row interface{ Scan(...any) error }) (OwnerPostStages, error) {
	var g OwnerPostStages
	var raw, uploads, state string
	if err := row.Scan(&raw, &uploads, &state); errors.Is(err, sql.ErrNoRows) {
		return g, ErrOwnerPostNotApproved
	} else if err != nil {
		return g, err
	}
	if state != OwnerPostApproved {
		return g, ErrOwnerPostNotApproved
	}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &g); err != nil {
			return g, err
		}
	}
	if uploads != "" {
		if err := json.Unmarshal([]byte(uploads), &g.Uploads); err != nil {
			return g, err
		}
	}
	return g, nil
}

// ownerPostStep changes an approved post's progress in one transaction;
// change refuses a step out of order with ErrOwnerPostStage.
func (s *Store) ownerPostStep(id string, change func(*OwnerPostStages) error) error {
	return s.tx(func(tx *sql.Tx) error {
		g, err := ownerPostStage(tx.QueryRow(`SELECT stages,uploads,state FROM owner_posts WHERE id=?`, id))
		if err != nil {
			return err
		}
		if err := change(&g); err != nil {
			return err
		}
		raw, err := json.Marshal(g)
		if err != nil {
			return err
		}
		uploads := ""
		if len(g.Uploads) > 0 {
			b, err := json.Marshal(g.Uploads)
			if err != nil {
				return err
			}
			uploads = string(b)
		}
		_, err = tx.Exec(`UPDATE owner_posts SET stages=?,uploads=? WHERE id=? AND state=?`, string(raw), uploads, id, OwnerPostApproved)
		return err
	})
}

// StartOwnerPostPart records that part index of parts may be written now.
// Earlier parts must be done, no upload recorded, and the split unchanged.
func (s *Store) StartOwnerPostPart(id string, parts, index int) error {
	return s.ownerPostStep(id, func(g *OwnerPostStages) error {
		if g.Parts == 0 {
			g.Parts = parts
		}
		if g.Parts != parts || index < 0 || index >= parts || len(g.Done) != index || len(g.Uploads) > 0 || g.Started > index+1 {
			return ErrOwnerPostStage
		}
		g.Started = index + 1
		return nil
	})
}

// OwnerPostPartDone records part index confirmed in Slack at ts.
func (s *Store) OwnerPostPartDone(id string, index int, ts string) error {
	return s.ownerPostStep(id, func(g *OwnerPostStages) error {
		if ts == "" || len(g.Done) != index || g.Started != index+1 || len(g.Uploads) > 0 {
			return ErrOwnerPostStage
		}
		g.Done = append(g.Done, ts)
		return nil
	})
}

// OwnerPostPartNotSent records that Slack clearly refused part index: it
// was not written, so a retry writes it without a check.
func (s *Store) OwnerPostPartNotSent(id string, index int) error {
	return s.ownerPostStep(id, func(g *OwnerPostStages) error {
		if len(g.Done) != index || g.Started != index+1 {
			return ErrOwnerPostStage
		}
		g.Started = index
		return nil
	})
}

// SetOwnerPostUploads records the uploaded Slack file IDs of an approved
// post before its upload is completed (shared), only once all its parts
// (of parts) are done; recorded once.
func (s *Store) SetOwnerPostUploads(id string, parts int, ids []string) error {
	return s.ownerPostStep(id, func(g *OwnerPostStages) error {
		if len(ids) == 0 || len(g.Uploads) > 0 || !g.TextDone(parts) {
			return ErrOwnerPostStage
		}
		for _, f := range ids {
			if f == "" {
				return ErrOwnerPostStage
			}
		}
		g.Uploads = append([]string(nil), ids...)
		return nil
	})
}

// OwnerPostFilesDone records the files' upload completed in Slack.
func (s *Store) OwnerPostFilesDone(id string) error {
	return s.ownerPostStep(id, func(g *OwnerPostStages) error {
		if len(g.Uploads) == 0 {
			return ErrOwnerPostStage
		}
		g.Files = true
		return nil
	})
}

// OwnerPostDelivery reports outbound row out as the delivery row of one
// approved post as the owner to Slack channel chat: a text event tagged
// with that row's delivery ID there is Heimdall's own post.
func (s *Store) OwnerPostDelivery(out int64, chat string) (bool, error) {
	if out <= 0 || chat == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM outbound o JOIN owner_posts p
		ON p.id = CASE WHEN json_valid(o.extra) THEN json_extract(o.extra,'$.owner_post.id') END
		WHERE o.id=? AND o.iface=? AND o.chat=? AND p.state=? AND p.target_chat=?`, out, core.PlatformSlack, chat, OwnerPostApproved, chat).Scan(&n)
	return n > 0, err
}

// OwnerPostFiles reports whether every given Slack file ID was recorded as
// an upload of one approved post as the owner to dest (its channel and
// thread), so a file-share echo there is Heimdall's.
func (s *Store) OwnerPostFiles(dest core.ConvKey, ids []string) (bool, error) {
	want := map[string]bool{}
	for _, id := range ids {
		if id == "" {
			return false, nil
		}
		want[id] = true
	}
	if len(want) == 0 {
		return false, nil
	}
	if dest.Chat == "" {
		return false, nil
	}
	args := []any{OwnerPostApproved, dest.Chat, dest.Thread}
	for id := range want {
		args = append(args, id)
	}
	// One post holds them all: the echo is one completed upload.
	var n int
	err := s.db.QueryRow(`SELECT COALESCE(MAX(c),0) FROM (SELECT COUNT(DISTINCT j.value) c FROM owner_posts p, json_each(p.uploads) j
		WHERE p.uploads != '' AND p.state=? AND p.target_chat=? AND p.target_thread=? AND j.value IN (?`+strings.Repeat(",?", len(want)-1)+`) GROUP BY p.id)`, args...).Scan(&n)
	return err == nil && n == len(want), err
}
