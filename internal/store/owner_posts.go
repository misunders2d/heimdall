package store

import (
	"database/sql"
	"encoding/json"
	"errors"
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
	State   string
	Expires time.Time
	Created time.Time
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
	_, err = tx.Exec(`INSERT INTO owner_posts(id,creator,card_iface,card_chat,card_thread,target_chat,target_thread,label,text,files,origin,state,expires_at,created_at,decided_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		p.ID, p.Creator, p.Card.Interface, p.Card.Chat, p.Card.Thread, p.Target.Chat, p.Target.Thread, p.Label, p.Text, string(files), p.Origin, p.State, unix(p.Expires), created, decided)
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

const ownerPostCols = `id,creator,card_iface,card_chat,card_thread,target_chat,target_thread,label,text,files,origin,state,expires_at,created_at`

func scanOwnerPost(row interface{ Scan(...any) error }) (OwnerPost, error) {
	var p OwnerPost
	var files string
	var expires, created int64
	if err := row.Scan(&p.ID, &p.Creator, &p.Card.Interface, &p.Card.Chat, &p.Card.Thread, &p.Target.Chat, &p.Target.Thread, &p.Label, &p.Text, &files, &p.Origin, &p.State, &expires, &created); err != nil {
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
// oldest first; expired ones are left out.
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
