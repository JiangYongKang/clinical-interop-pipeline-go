package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Event is one tamper-evident audit trail entry. Only identifiers (never raw
// clinical payloads or masked identity values) are recorded.
type Event struct {
	Seq       int64     `json:"seq"`
	Time      time.Time `json:"time"`
	Actor     string    `json:"actor"`
	Action    string    `json:"action"`
	Ref       string    `json:"ref"`
	Decision  string    `json:"decision"` // allow | deny
	Basis     string    `json:"basis"`
	Resources []string  `json:"resources"`
	PrevHash  string    `json:"prev_hash"`
	Hash      string    `json:"hash"`
}

// Log is an append-only, hash-chained in-memory audit log. Every entry's hash
// binds it to the previous entry, so a verifier can detect any modification.
type Log struct {
	mu      sync.Mutex
	events  []Event
	lastHex string
}

// New builds an empty audit log.
func New() *Log { return &Log{} }

// Append records one decision and returns the stored event. Resources must be
// stable identifiers only (MRNs, visit ids, observation ids).
func (l *Log) Append(actor, action, ref, decision, basis string, resources []string, at time.Time) Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	res := append([]string(nil), resources...)
	if len(res) > 1 {
		sortStrings(res)
	}
	seq := int64(len(l.events)) + 1
	e := Event{
		Seq:       seq,
		Time:      at.UTC(),
		Actor:     actor,
		Action:    action,
		Ref:       ref,
		Decision:  decision,
		Basis:     basis,
		Resources: res,
		PrevHash:  l.lastHex,
	}
	e.Hash = hashEvent(e)
	l.events = append(l.events, e)
	l.lastHex = e.Hash
	return e
}

// Entries returns a deep copy of all audit events in append order.
func (l *Log) Entries() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	for i := range out {
		out[i].Resources = append([]string(nil), l.events[i].Resources...)
	}
	return out
}

// Verify walks the hash chain and reports the first broken entry, if any.
func (l *Log) Verify() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := ""
	for _, e := range l.events {
		if e.PrevHash != prev {
			return fmt.Errorf("audit chain broken at seq %d: prev hash mismatch", e.Seq)
		}
		if e.Hash != hashEvent(e) {
			return fmt.Errorf("audit chain broken at seq %d: content hash mismatch", e.Seq)
		}
		// NOTE: event timestamps are intentionally NOT required to be
		// monotonic. Ingestion events are stamped with the wire message's
		// business time (which may arrive out of order) while export events
		// use the serving clock. Tamper evidence comes from the hash chain,
		// not from timestamp ordering.
		prev = e.Hash
	}
	return nil
}

func hashEvent(e Event) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%d|%s|%s|%s|%s|%s|%s|",
		e.PrevHash, e.Seq, e.Time.Format(time.RFC3339Nano), e.Actor, e.Action, e.Ref, e.Decision, e.Basis)
	b.WriteString(strings.Join(e.Resources, ","))
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
