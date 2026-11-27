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
// than its message ID msg (same chat). An unknown or unordered ID never is.
func (p OwnerPost) CardSeenBefore(reply int64, msg string) bool {
	return p.CardMark >= 0 && p.CardMark < reply && PlatformBefore(p.CardRef, msg)
}

// PlatformBefore reports message ID a sent before b in one chat: Telegram
// message IDs (integers) or Slack timestamps ("<seconds>.<micro>", compared
// by their integer parts, never as floats). Anything else: false.
func PlatformBefore(a, b string) bool {
	parse := func(s string) (sec, frac int64, ok bool) {
		whole, part, dotted := strings.Cut(s, ".")
		if whole == "" || len(whole) > 18 || strings.Trim(whole, "0123456789") != "" {
			return 0, 0, false
		}
		sec, err := strconv.ParseInt(whole, 10, 64)
		if err != nil {
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
	as, af, aok := parse(a)
	bs, bf, bok := parse(b)
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

// OwnerPostUploads are the Slack file IDs recorded for a post's upload
// before files.completeUploadExternal was called; nil when none was.
func (s *Store) OwnerPostUploads(id string) ([]string, error) {
	var raw string
	if err := s.db.QueryRow(`SELECT uploads FROM owner_posts WHERE id=?`, id).Scan(&raw); err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var ids []string
	return ids, json.Unmarshal([]byte(raw), &ids)
}

// SetOwnerPostUploads records the uploaded Slack file IDs of an approved
// post, before its upload is completed (shared).
func (s *Store) SetOwnerPostUploads(id string, ids []string) error {
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	res, err := s.db.Exec(`UPDATE owner_posts SET uploads=? WHERE id=? AND state=?`, string(raw), id, OwnerPostApproved)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("owner post is not approved")
	}
	return nil
}

// OwnerPostFiles reports whether every given Slack file ID was recorded as
// an upload of a post as the owner, so its file-share echo is Heimdall's.
func (s *Store) OwnerPostFiles(ids []string) (bool, error) {
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
	args := make([]any, 0, len(want))
	for id := range want {
		args = append(args, id)
	}
	var n int
	err := s.db.QueryRow(`SELECT COUNT(DISTINCT j.value) FROM owner_posts p, json_each(p.uploads) j
		WHERE p.uploads != '' AND j.value IN (?`+strings.Repeat(",?", len(args)-1)+`)`, args...).Scan(&n)
	return err == nil && n == len(want), err
}
