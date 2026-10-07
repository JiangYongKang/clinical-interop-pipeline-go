package export

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// DefaultPageSize is used when a paged request does not specify a size.
const DefaultPageSize = 100

// PageRequest is one page of a paginated, permission-checked export. A
// request without Cursor opens a new consistent read; the response carries
// an opaque Cursor that every later page of the same read must present.
type PageRequest struct {
	Actor          string
	Purpose        Purpose
	UnmaskedFields []string
	PatientMRNs    []string // explicit selection; every id must exist AND be allowed
	Ref            string
	At             time.Time // serve/audit time
	AsOf           time.Time // when non-zero, read from the historical view
	PageSize       int
	Cursor         string
}

// Page is one slice of a stable, repeatable export read. Ordering within and
// across pages is (resource kind, stable id): patient, then encounter, then
// observation.
type Page struct {
	Patients     []*domain.Patient
	Encounters   []*domain.Encounter
	Observations []*domain.Observation
	NextCursor   string
	LastPage     bool
	AuditSeq     int64
}

// cursorToken is the opaque, self-describing continuation marker.
type cursorToken struct {
	Sid    string `json:"sid"`
	Offset int    `json:"off"`
	Total  int    `json:"tot"`
}

func (c cursorToken) encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursorToken, bool) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return cursorToken{}, false
	}
	var c cursorToken
	if err := json.Unmarshal(b, &c); err != nil || c.Sid == "" {
		return cursorToken{}, false
	}
	return c, true
}

// ExportPage returns one page of a permission-checked export pinned to one
// consistent snapshot. Policy failures (unknown actor, illegal mask, an
// explicit patient selector that does not exist or is outside the actor's
// allow-list) reject the ENTIRE request — no partial page is ever returned
// and the denial never echoes the offending raw identifier.
func (svc *Service) ExportPage(req PageRequest) (*Page, error) {
	at := req.At
	if at.IsZero() {
		at = time.Now()
	}
	deny := func(st domain.Status, reason string) (*Page, error) {
		ev := svc.audit.Append(req.Actor, "export-page", req.Ref, "deny", reason, nil, at)
		return nil, &domain.RejectError{Status: st, Ref: req.Ref, Reason: reason + "; audit_seq=" + itoa(ev.Seq)}
	}

	role, ok := svc.roles[req.Actor]
	if !ok {
		return deny(domain.StatusRejectedPermission, "unknown actor")
	}
	if req.Purpose != PurposeNormal && req.Purpose != PurposeAudit {
		return deny(domain.StatusRejectedPermission, "unknown export purpose")
	}
	unmask := map[string]bool{}
	if len(req.UnmaskedFields) > 0 {
		if req.Purpose != PurposeAudit {
			return deny(domain.StatusRejectedInvalidMask, "identity fields may only be retained for audit purpose")
		}
		if !role.Scopes[ScopeAuditUnmask] {
			return deny(domain.StatusRejectedPermission, "actor lacks audit:unmask scope")
		}
		for _, f := range req.UnmaskedFields {
			f = strings.TrimSpace(f)
			if !unmaskableFields[f] {
				return deny(domain.StatusRejectedInvalidMask, "field is not unmaskable")
			}
			unmask[f] = true
		}
	}
	if req.Purpose == PurposeAudit && !role.Scopes[ScopeAuditUnmask] {
		return deny(domain.StatusRejectedPermission, "actor lacks audit:unmask scope")
	}
	hasRead := role.Scopes[ScopePatientRead] || role.Scopes[ScopeEncounterRead] || role.Scopes[ScopeObservationRead]
	if !hasRead {
		return deny(domain.StatusRejectedPermission, "actor has no readable resource scope")
	}

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}

	// Open or resume one pinned read.
	var sess *pageSession
	var offset int
	if req.Cursor == "" {
		snap := svc.viewSnapshot(req.AsOf)

		// Build the selector set. An explicit selector that names a patient
		// the store does not know OR that is outside the allow-list rejects
		// the whole request. The same generic wording is used for both so the
		// denial never reveals which (existence is itself sensitive).
		known := map[string]bool{}
		for _, p := range snap.Patients {
			known[p.MRN] = true
		}
		wildcard := role.AllowedMRNs["*"]
		allowed := map[string]bool{}
		if len(req.PatientMRNs) > 0 {
			for _, m := range req.PatientMRNs {
				m = strings.TrimSpace(m)
				if m == "" {
					return deny(domain.StatusRejectedPermission, "blank patient selector")
				}
				if !known[m] || (!wildcard && !role.AllowedMRNs[m]) {
					return deny(domain.StatusRejectedPermission, "patient selector unknown or outside actor allow-list")
				}
				allowed[m] = true
			}
		} else {
			for _, p := range snap.Patients {
				if wildcard || role.AllowedMRNs[p.MRN] {
					allowed[p.MRN] = true
				}
			}
		}

		sess = &pageSession{
			actor:    req.Actor,
			purpose:  req.Purpose,
			unmask:   strings.Join(sortedKeys(unmask), "|"),
			snapshot: svc.filterByScopeAndMRN(snap, role, allowed),
			viewTime: at,
			size:     pageSize,
		}
		sid := newSessionID()
		svc.pageMu.Lock()
		svc.pages[sid] = sess
		svc.pageMu.Unlock()
		offset = 0
	} else {
		cur, ok := decodeCursor(req.Cursor)
		if !ok {
			return deny(domain.StatusRejectedMalformed, "invalid page cursor")
		}
		svc.pageMu.Lock()
		sess = svc.pages[cur.Sid]
		svc.pageMu.Unlock()
		if sess == nil {
			return deny(domain.StatusRejectedMalformed, "unknown or expired page cursor")
		}
		if sess.size != pageSize {
			return deny(domain.StatusRejectedMalformed, "page size must stay constant within one read")
		}
		// A continuation is the same read: the same actor must continue with
		// the same purpose and the same retained-field set. Scope is looked up
		// live, so revoking an actor mid-read takes effect immediately.
		if sess.actor != req.Actor || sess.purpose != req.Purpose ||
			sess.unmask != strings.Join(sortedKeys(unmask), "|") {
			return deny(domain.StatusRejectedPermission, "cursor does not match this read's actor or masking")
		}
		if cur.Offset < 0 || cur.Offset > cur.Total {
			return deny(domain.StatusRejectedMalformed, "cursor offset out of range")
		}
		offset = cur.Offset
	}

	rows := flatten(sess.snapshot, role)
	total := len(rows)
	end := offset + pageSize
	if end > total {
		end = total
	}
	page := &Page{}
	for _, r := range rows[offset:end] {
		switch r.kind {
		case "patient":
			page.Patients = append(page.Patients, svc.maskPatient(r.patient, unmask))
		case "encounter":
			page.Encounters = append(page.Encounters, svc.maskEncounter(r.encounter, unmask))
		case "observation":
			page.Observations = append(page.Observations, svc.maskObservation(r.observation))
		}
	}
	if end < total {
		page.NextCursor = cursorToken{Sid: sessionOf(sess, svc), Offset: end, Total: total}.encode()
	} else {
		page.LastPage = true
	}

	ev := svc.audit.Append(req.Actor, "export-page", req.Ref, "allow",
		"purpose="+string(req.Purpose)+",offset="+itoaInt(offset)+",count="+itoaInt(end-offset)+",total="+itoaInt(total),
		nil, at)
	page.AuditSeq = ev.Seq
	return page, nil
}

func (svc *Service) viewSnapshot(asOf time.Time) store.Snapshot {
	if asOf.IsZero() {
		return svc.store.Snapshot()
	}
	return svc.store.SnapshotAt(asOf, store.HistoryOptions{})
}

// filterByScopeAndMRN returns a snapshot restricted to the actor's scopes
// and the pre-validated MRN allow-set.
func (svc *Service) filterByScopeAndMRN(snap store.Snapshot, role Role, allowed map[string]bool) store.Snapshot {
	out := store.Snapshot{}
	if role.Scopes[ScopePatientRead] {
		for _, p := range snap.Patients {
			if allowed[p.MRN] {
				out.Patients = append(out.Patients, p)
			}
		}
	}
	if role.Scopes[ScopeEncounterRead] {
		for _, e := range snap.Encounters {
			if allowed[e.MRN] {
				out.Encounters = append(out.Encounters, e)
			}
		}
	}
	if role.Scopes[ScopeObservationRead] {
		for _, o := range snap.Observations {
			if allowed[o.MRN] {
				out.Observations = append(out.Observations, o)
			}
		}
	}
	return out
}

// resultRow is one export row in the global stable order.
type resultRow struct {
	kind        string
	id          string
	patient     *domain.Patient
	encounter   *domain.Encounter
	observation *domain.Observation
}

func flatten(snap store.Snapshot, role Role) []resultRow {
	rows := make([]resultRow, 0, len(snap.Patients)+len(snap.Encounters)+len(snap.Observations))
	if role.Scopes[ScopePatientRead] {
		for _, p := range snap.Patients {
			rows = append(rows, resultRow{kind: "patient", id: p.MRN, patient: p})
		}
	}
	if role.Scopes[ScopeEncounterRead] {
		for _, e := range snap.Encounters {
			rows = append(rows, resultRow{kind: "encounter", id: e.VisitID, encounter: e})
		}
	}
	if role.Scopes[ScopeObservationRead] {
		for _, o := range snap.Observations {
			rows = append(rows, resultRow{kind: "observation", id: o.ObservationID, observation: o})
		}
	}
	kindRank := map[string]int{"patient": 0, "encounter": 1, "observation": 2}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].kind != rows[j].kind {
			return kindRank[rows[i].kind] < kindRank[rows[j].kind]
		}
		return rows[i].id < rows[j].id
	})
	return rows
}

func newSessionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// sessionOf recovers the session id by reverse lookup (sessions are pinned
// for the lifetime of the read; ids are unique so the lookup is unambiguous).
func sessionOf(sess *pageSession, svc *Service) string {
	svc.pageMu.Lock()
	defer svc.pageMu.Unlock()
	for id, s := range svc.pages {
		if s == sess {
			return id
		}
	}
	return ""
}

func itoaInt(n int) string {
	if n == 0 {
		return "0"
	}
	return strconv.FormatInt(int64(n), 10)
}
