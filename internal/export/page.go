package export

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

const defaultPageSize = 16

// PageRequest opens one paginated, snapshot-consistent export session.
type PageRequest struct {
	Actor          string
	Purpose        Purpose
	UnmaskedFields []string
	PatientMRNs    []string // explicit roster; every MRN must exist and be allowed
	PageSize       int
	Ref            string
	At             time.Time // zero means "now": pins one consistent snapshot
}

// Page is one stable slice of a frozen export session. Pages are partitioned
// by patient (a patient and all of its encounters/observations move together);
// ordering is the deterministic MRN order, so any page is reproducible.
type Page struct {
	Token      string
	Index      int // 1-based page index within the session
	TotalPages int
	Done       bool
	ResultSet
}

// Session is a frozen, permission-checked, masked export view. Only the
// opaque token is exposed to callers; all state stays server-side.
type Session struct {
	token string
}

// Token returns the opaque session handle used to fetch subsequent pages.
func (s *Session) Token() string { return s.token }

// frozenView is the permission-checked, masked, immutable export material.
type frozenView struct {
	groups []patientGroup
}

type patientGroup struct {
	mrn         string // raw MRN, kept only for grouping; payloads are masked
	patient     *domain.Patient
	encounters  []*domain.Encounter
	observation []*domain.Observation
}

// session is a frozen view plus the cursor state of one paginated export.
type session struct {
	token      string
	actor      string
	ref        string
	pageSize   int
	view       frozenView
	nextOffset int // number of patients already served
	mu         sync.Mutex
}

// OpenSession validates the whole request up front (role, masking, allow-list
// and roster existence — every check is all-or-nothing), appends exactly one
// audit event and freezes one store snapshot. Subsequent pages are served from
// that frozen snapshot, so concurrent writers can never make pages drift.
//
// At pins a business-time view: pages then reflect only conclusions that had
// arrived and taken effect by At, using the same rule as store.AsOf.
func (svc *Service) OpenSession(req PageRequest) (*Session, *domain.RejectError) {
	at := req.At.UTC()
	if at.IsZero() {
		at = time.Now().UTC()
	}
	deny := func(status domain.Status, reason string) (*Session, *domain.RejectError) {
		ev := svc.audit.Append(req.Actor, "export-page", req.Ref, "deny", reason, nil, at)
		return nil, &domain.RejectError{
			Status: status, Ref: req.Ref,
			Reason: reason + "; audit_seq=" + itoa(ev.Seq),
		}
	}

	role, ok := svc.roles[req.Actor]
	if !ok {
		return deny(domain.StatusRejectedPermission, "unknown actor")
	}
	if req.Purpose != PurposeNormal && req.Purpose != PurposeAudit {
		return deny(domain.StatusRejectedPermission, "unknown export purpose")
	}

	// Masking combination is validated before any data is selected.
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

	// Freeze the snapshot ONCE at open; pages never re-read the live store.
	snap := svc.store.AsOf(at)
	wildcard := role.AllowedMRNs["*"]

	selected := map[string]bool{}
	if len(req.PatientMRNs) > 0 {
		// Roster mode: every selector must be non-blank, within the allow-list
		// AND exist in the frozen snapshot. One failure rejects the request.
		existing := map[string]bool{}
		for _, p := range snap.Patients {
			existing[p.MRN] = true
		}
		for _, m := range req.PatientMRNs {
			m = strings.TrimSpace(m)
			if m == "" {
				return deny(domain.StatusRejectedPermission, "blank patient selector")
			}
			if !wildcard && !role.AllowedMRNs[m] {
				return deny(domain.StatusRejectedPermission, "patient outside actor allow-list")
			}
			if !existing[m] {
				// No raw selector is echoed back; the audit event is also
				// resource-free, so neither error nor trail leaks input fields.
				return deny(domain.StatusRejectedPermission, "requested patient does not exist")
			}
			selected[m] = true
		}
	} else {
		for _, p := range snap.Patients {
			if wildcard || role.AllowedMRNs[p.MRN] {
				selected[p.MRN] = true
			}
		}
	}

	hasScope := false
	view := frozenView{}
	byMRN := map[string]*patientGroup{}
	order := []string{}
	group := func(mrn string) *patientGroup {
		g := byMRN[mrn]
		if g == nil {
			g = &patientGroup{mrn: mrn}
			byMRN[mrn] = g
			order = append(order, mrn)
		}
		return g
	}
	basis := []string{"purpose=" + string(req.Purpose)}
	resourceIDs := []string{}
	if role.Scopes[ScopePatientRead] {
		hasScope = true
		basis = append(basis, "scope:patient:read")
		for _, p := range snap.Patients {
			if !selected[p.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Patient/"+p.MRN)
			group(p.MRN).patient = svc.maskPatient(p, unmask)
		}
	}
	if role.Scopes[ScopeEncounterRead] {
		hasScope = true
		basis = append(basis, "scope:encounter:read")
		for _, e := range snap.Encounters {
			if !selected[e.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Encounter/"+e.VisitID)
			g := group(e.MRN)
			g.encounters = append(g.encounters, svc.maskEncounter(e, unmask))
		}
	}
	if role.Scopes[ScopeObservationRead] {
		hasScope = true
		basis = append(basis, "scope:observation:read")
		for _, o := range snap.Observations {
			if !selected[o.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Observation/"+o.ObservationID)
			g := group(o.MRN)
			g.observation = append(g.observation, svc.maskObservation(o))
		}
	}
	if !hasScope {
		return deny(domain.StatusRejectedPermission, "actor has no readable resource scope")
	}

	// Groups are ordered by the pseudonymized MRN — the stable key actually
	// present in masked output and the same key the non-paged Export sorts by —
	// so page order is deterministic and identical across repeat sessions.
	view.groups = make([]patientGroup, 0, len(order))
	for _, mrn := range order {
		g := byMRN[mrn]
		sort.Slice(g.encounters, func(i, j int) bool { return g.encounters[i].VisitID < g.encounters[j].VisitID })
		sort.Slice(g.observation, func(i, j int) bool {
			return g.observation[i].ObservationID < g.observation[j].ObservationID
		})
		view.groups = append(view.groups, *g)
	}
	sort.Slice(view.groups, func(i, j int) bool {
		pi, pj := view.groups[i].patient, view.groups[j].patient
		switch {
		case pi != nil && pj != nil:
			return pi.MRN < pj.MRN
		case pi != nil:
			return true
		case pj != nil:
			return false
		default:
			return view.groups[i].mrn < view.groups[j].mrn
		}
	})
	sort.Strings(resourceIDs)

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	token := newToken()
	sess := &session{
		token: token, actor: req.Actor, ref: req.Ref,
		pageSize: pageSize, view: view,
	}
	svc.sessionsMu.Lock()
	svc.sessions[token] = sess
	svc.sessionsMu.Unlock()

	totalPages := 1
	if n := len(view.groups); n > 0 {
		totalPages = (n + pageSize - 1) / pageSize
	}
	ev := svc.audit.Append(req.Actor, "export-page", req.Ref, "allow",
		strings.Join(basis, ",")+",unmasked="+strings.Join(sortedKeys(unmask), "|")+
			",patients="+itoa(int64(len(view.groups)))+",pages="+itoa(int64(totalPages)),
		resourceIDs, at)
	_ = ev
	return &Session{token: token}, nil
}

// NextPage returns the next stable page of the session. The cursor advances
// only over the frozen view, so pages never overlap, skip or drift while
// writers concurrently commit new data. A session with zero patients yields a
// single empty terminal page (Done == true).
func (svc *Service) NextPage(token string) (*Page, *domain.RejectError) {
	sess := svc.lookup(token)
	if sess == nil {
		return nil, &domain.RejectError{
			Status: domain.StatusRejectedPermission,
			Reason: "unknown or closed export session",
		}
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()

	n := len(sess.view.groups)
	totalPages := 1
	if n > 0 {
		totalPages = (n + sess.pageSize - 1) / sess.pageSize
	}
	idx := sess.nextOffset / sess.pageSize // 0-based
	page := &Page{
		Token: sess.token, Index: idx + 1, TotalPages: totalPages,
	}
	if sess.nextOffset >= n {
		page.Done = true
		return page, nil
	}
	end := sess.nextOffset + sess.pageSize
	if end > n {
		end = n
	}
	for _, g := range sess.view.groups[sess.nextOffset:end] {
		if g.patient != nil {
			page.Patients = append(page.Patients, g.patient)
		}
		page.Encounters = append(page.Encounters, g.encounters...)
		page.Observations = append(page.Observations, g.observation...)
	}
	sess.nextOffset = end
	if end >= n {
		page.Done = true
	}
	return page, nil
}

// CloseSession releases a frozen session. Unknown/closed tokens are ignored.
func (svc *Service) CloseSession(token string) {
	svc.sessionsMu.Lock()
	delete(svc.sessions, token)
	svc.sessionsMu.Unlock()
}

func (svc *Service) lookup(token string) *session {
	svc.sessionsMu.RLock()
	defer svc.sessionsMu.RUnlock()
	return svc.sessions[token]
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
