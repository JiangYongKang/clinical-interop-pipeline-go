package export

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// Purpose controls the masking boundary.
type Purpose string

const (
	PurposeNormal Purpose = "normal" // always masked
	PurposeAudit  Purpose = "audit"  // may retain identity, fully audited
)

// Scope constants. audit:unmask is mandatory for ANY identity-retaining export.
const (
	ScopePatientRead     = "patient:read"
	ScopeEncounterRead   = "encounter:read"
	ScopeObservationRead = "observation:read"
	ScopeAuditUnmask     = "audit:unmask"
)

// unmaskableFields is the exact allow-list of fields an audit export may keep.
var unmaskableFields = map[string]bool{
	"family_name": true,
	"given_name":  true,
	"birth_date":  true,
	"gender":      true,
}

// Role is one caller principal: granted scopes and explicit patient allow-list.
// An empty AllowedMRNs slice means the role may read no patients; the wildcard
// "*" means every patient.
type Role struct {
	Name        string
	Scopes      map[string]bool
	AllowedMRNs map[string]bool
}

// Request is one controlled export request.
type Request struct {
	Actor          string
	Purpose        Purpose
	UnmaskedFields []string // only meaningful with PurposeAudit
	PatientMRNs    []string // explicit selection; empty means all allowed
	Ref            string
	At             time.Time
}

// ResultSet is a deterministic, ordered export payload.
type ResultSet struct {
	Patients     []*domain.Patient     `json:"patients"`
	Encounters   []*domain.Encounter   `json:"encounters"`
	Observations []*domain.Observation `json:"observations"`
	AuditSeq     int64                 `json:"audit_seq"`
}

// Service enforces scope, masking and audit-purpose rules.
type Service struct {
	store  *store.Store
	audit  *audit.Log
	roles  map[string]Role
	macKey []byte

	sessionsMu sync.RWMutex
	sessions   map[string]*session
}

// New builds an export service. macKey salts the pseudonymization HMAC.
func New(st *store.Store, al *audit.Log, roles []Role, macKey []byte) *Service {
	rm := make(map[string]Role, len(roles))
	for _, r := range roles {
		rm[r.Name] = r
	}
	return &Service{
		store:    st,
		audit:    al,
		roles:    rm,
		macKey:   append([]byte(nil), macKey...),
		sessions: map[string]*session{},
	}
}

// Export returns data the actor is allowed to see, masked per purpose, and
// writes exactly one audit event. Every policy failure rejects the whole
// request — there are never partial results — and denial errors carry only
// the caller-supplied ref, never raw identifiers or payload fields.
func (svc *Service) Export(req Request) (*ResultSet, error) {
	at := req.At
	if at.IsZero() {
		at = time.Now()
	}
	deny := func(reason string) (*ResultSet, error) {
		ev := svc.audit.Append(req.Actor, "export", req.Ref, "deny", reason, nil, at)
		return nil, &domain.RejectError{
			Status: domain.StatusRejectedPermission,
			Ref:    req.Ref,
			Reason: reason + "; audit_seq=" + itoa(ev.Seq),
		}
	}

	role, ok := svc.roles[req.Actor]
	if !ok {
		return deny("unknown actor")
	}
	if req.Purpose != PurposeNormal && req.Purpose != PurposeAudit {
		return deny("unknown export purpose")
	}

	// invalidMask denies an illegal masking combination while still writing
	// the mandatory audit trail.
	invalidMask := func(reason string) (*ResultSet, error) {
		ev := svc.audit.Append(req.Actor, "export", req.Ref, "deny", reason, nil, at)
		return nil, &domain.RejectError{
			Status: domain.StatusRejectedInvalidMask,
			Ref:    req.Ref,
			Reason: reason + "; audit_seq=" + itoa(ev.Seq),
		}
	}
	// Validate the masking combination up front.
	unmask := map[string]bool{}
	if len(req.UnmaskedFields) > 0 {
		if req.Purpose != PurposeAudit {
			return invalidMask("identity fields may only be retained for audit purpose")
		}
		if !role.Scopes[ScopeAuditUnmask] {
			return deny("actor lacks audit:unmask scope")
		}
		for _, f := range req.UnmaskedFields {
			f = strings.TrimSpace(f)
			if !unmaskableFields[f] {
				return invalidMask("field is not unmaskable")
			}
			unmask[f] = true
		}
	}
	if req.Purpose == PurposeAudit && !role.Scopes[ScopeAuditUnmask] {
		// Audit-purpose exports always need the unmask scope even if this
		// particular response happens to be fully masked.
		return deny("actor lacks audit:unmask scope")
	}

	// Resolve the patient selection and enforce the allow-list atomically:
	// one out-of-scope MRN rejects the entire request. The view is pinned to
	// the request instant (AsOf): only conclusions that had arrived and taken
	// effect by at are visible, so repeated requests at the same at agree.
	snap := svc.store.AsOf(at)
	allowedMRNs := map[string]bool{}
	wildcard := role.AllowedMRNs["*"]
	if len(req.PatientMRNs) > 0 {
		for _, m := range req.PatientMRNs {
			m = strings.TrimSpace(m)
			if m == "" {
				return deny("blank patient selector")
			}
			if !wildcard && !role.AllowedMRNs[m] {
				return deny("patient outside actor allow-list")
			}
			allowedMRNs[m] = true
		}
	} else {
		for _, p := range snap.Patients {
			if wildcard || role.AllowedMRNs[p.MRN] {
				allowedMRNs[p.MRN] = true
			}
		}
	}

	res := &ResultSet{}
	basis := []string{"purpose=" + string(req.Purpose)}
	resourceIDs := []string{}

	if role.Scopes[ScopePatientRead] {
		basis = append(basis, "scope:patient:read")
		for _, p := range snap.Patients {
			if !allowedMRNs[p.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Patient/"+p.MRN)
			res.Patients = append(res.Patients, svc.maskPatient(p, unmask))
		}
	}
	if role.Scopes[ScopeEncounterRead] {
		basis = append(basis, "scope:encounter:read")
		for _, e := range snap.Encounters {
			if !allowedMRNs[e.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Encounter/"+e.VisitID)
			res.Encounters = append(res.Encounters, svc.maskEncounter(e, unmask))
		}
	}
	if role.Scopes[ScopeObservationRead] {
		basis = append(basis, "scope:observation:read")
		for _, o := range snap.Observations {
			if !allowedMRNs[o.MRN] {
				continue
			}
			resourceIDs = append(resourceIDs, "Observation/"+o.ObservationID)
			res.Observations = append(res.Observations, svc.maskObservation(o))
		}
	}
	if len(basis) == 1 {
		// no data scope at all
		return deny("actor has no readable resource scope")
	}

	sort.Slice(res.Patients, func(i, j int) bool { return res.Patients[i].MRN < res.Patients[j].MRN })
	sort.Slice(res.Encounters, func(i, j int) bool { return res.Encounters[i].VisitID < res.Encounters[j].VisitID })
	sort.Slice(res.Observations, func(i, j int) bool {
		return res.Observations[i].ObservationID < res.Observations[j].ObservationID
	})
	sort.Strings(resourceIDs)

	ev := svc.audit.Append(req.Actor, "export", req.Ref, "allow",
		strings.Join(basis, ",")+",unmasked="+strings.Join(sortedKeys(unmask), "|"),
		resourceIDs, at)
	res.AuditSeq = ev.Seq
	return res, nil
}

// maskPatient retains a stable pseudonym (HMAC of the MRN) but removes every
// direct identifier unless this is an authorized audit export.
func (svc *Service) maskPatient(p *domain.Patient, unmask map[string]bool) *domain.Patient {
	c := *p
	c.MRN = svc.pseudonym("mrn:" + p.MRN)
	if !unmask["family_name"] {
		c.FamilyName = "***"
	}
	if !unmask["given_name"] {
		c.GivenName = "***"
	}
	if !unmask["birth_date"] {
		c.BirthDate = "****-**-**"
	}
	if !unmask["gender"] {
		c.Gender = "*"
	}
	return &c
}

func (svc *Service) maskEncounter(e *domain.Encounter, unmask map[string]bool) *domain.Encounter {
	c := *e
	c.MRN = svc.pseudonym("mrn:" + e.MRN)
	c.VisitID = svc.pseudonym("visit:" + e.VisitID)
	return &c
}

func (svc *Service) maskObservation(o *domain.Observation) *domain.Observation {
	c := *o
	c.MRN = svc.pseudonym("mrn:" + o.MRN)
	if c.VisitID != "" {
		c.VisitID = svc.pseudonym("visit:" + c.VisitID)
	}
	return &c
}

// pseudonym is deterministic per (kind+key, service MAC key): the same entity
// maps to the same token across exports, enabling joins without revealing
// identity, while distinct identifier spaces (MRN vs visit id) never collide.
func (svc *Service) pseudonym(v string) string {
	mac := hmac.New(sha256.New, svc.macKey)
	_, _ = mac.Write([]byte(v))
	return "pid-" + hex.EncodeToString(mac.Sum(nil))[:16]
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
