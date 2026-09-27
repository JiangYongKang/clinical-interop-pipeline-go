package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

const shardCount = 64

// Outcome is the deterministic result of applying one message.
type Outcome struct {
	Status      domain.Status
	MessageID   string
	Reason      string
	PatientMRN  string
	VisitID     string
	Observation string
	Digest      string
}

// Snapshot is a stable, ordered view of the whole store (for export/testing).
type Snapshot struct {
	Patients     []*domain.Patient
	Encounters   []*domain.Encounter
	Observations []*domain.Observation
}

// provenance records how an entity reached its current conclusion, so stale
// and cross-version updates can be judged deterministically.
type provenance struct {
	UpdatedAt time.Time
	Rank      domain.VersionRank
	Writer    string // message id of the winning write
}

// idemRec is one idempotency-table entry.
type idemRec struct {
	Digest string
	MRN    string
}

type shard struct {
	mu           sync.Mutex
	patients     map[string]*domain.Patient
	encounters   map[string]*domain.Encounter
	observations map[string]*domain.Observation
	patientProv  map[string]provenance
	encProv      map[string]provenance
	obsProv      map[string]provenance
}

// Store is a sharded, in-memory, concurrency-safe clinical data store.
// All entities of one message share one patient MRN, therefore one shard lock
// serializes every message touching a patient; the global idempotency lock is
// always taken AFTER the shard lock, which makes deadlocks impossible.
type Store struct {
	shards [shardCount]*shard
	idemMu sync.Mutex
	idem   map[string]idemRec
}

// New builds an empty store.
func New() *Store {
	s := &Store{idem: make(map[string]idemRec)}
	for i := range s.shards {
		s.shards[i] = &shard{
			patients:     map[string]*domain.Patient{},
			encounters:   map[string]*domain.Encounter{},
			observations: map[string]*domain.Observation{},
			patientProv:  map[string]provenance{},
			encProv:      map[string]provenance{},
			obsProv:      map[string]provenance{},
		}
	}
	return s
}

// Apply validates idempotency, performs stale/type-conflict checks and merges
// field-level updates atomically per message.
func (s *Store) Apply(cs *normalize.ChangeSet, now time.Time) Outcome {
	mrn := messageMRN(cs)
	digest := ContentDigest(cs)
	out := Outcome{MessageID: cs.MessageID, PatientMRN: mrn, Digest: digest}
	if mrn == "" {
		out.Status = domain.StatusRejectedMissingField
		out.Reason = "message contains no attributable patient"
		return out
	}
	if cs.Encounter != nil {
		out.VisitID = cs.Encounter.VisitID
	}

	sh := s.shards[shardIndex(mrn)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	s.idemMu.Lock()
	if rec, ok := s.idem[cs.MessageID]; ok {
		s.idemMu.Unlock()
		if rec.Digest == digest {
			out.Status = domain.StatusDeduplicated
			out.Reason = "identical re-delivery of already applied message"
			return out
		}
		out.Status = domain.StatusConflict
		out.Reason = "same message id re-used with different content"
		return out
	}
	s.idemMu.Unlock()

	// All checks below happen under the shard lock; on rejection nothing is
	// written, so a corrected message can be re-sent later.
	var basis []string
	created, updated := 0, 0

	if cs.Patient != nil {
		if rec, ok := sh.patientProv[mrn]; ok {
			if !isNewer(cs.Patient.UpdatedAt, cs.Rank, cs.MessageID, rec) {
				out.Status = domain.StatusStale
				out.Reason = "patient conclusion is not newer than stored version"
				return out
			}
			mergePatient(sh.patients[mrn], cs.Patient)
			updated++
			basis = append(basis, "patient:updated")
		} else {
			sh.patients[mrn] = clonePatient(cs.Patient)
			created++
			basis = append(basis, "patient:new")
		}
		sh.patientProv[mrn] = provenance{cs.Patient.UpdatedAt, cs.Rank, cs.MessageID}
	}

	if cs.Encounter != nil {
		e := cs.Encounter
		if prov, ok := sh.encProv[e.VisitID]; ok {
			if !isNewer(e.UpdatedAt, cs.Rank, cs.MessageID, prov) {
				out.Status = domain.StatusStale
				out.Reason = "encounter conclusion is not newer than stored version"
				return out
			}
			mergeEncounter(sh.encounters[e.VisitID], e)
			updated++
			basis = append(basis, "encounter:updated")
		} else {
			sh.encounters[e.VisitID] = cloneEncounter(e)
			created++
			basis = append(basis, "encounter:new")
		}
		sh.encProv[e.VisitID] = provenance{e.UpdatedAt, cs.Rank, cs.MessageID}
	}

	for _, o := range cs.Observations {
		if ex, ok := sh.observations[o.ObservationID]; ok {
			if (ex.ValueNumber == nil) != (o.ValueNumber == nil) {
				out.Status = domain.StatusRejectedTypeConflict
				out.Reason = "observation value type conflicts with stored value"
				out.Observation = o.ObservationID
				return out
			}
			if prov, ok := sh.obsProv[o.ObservationID]; ok && !isNewer(o.UpdatedAt, cs.Rank, cs.MessageID, prov) {
				out.Status = domain.StatusStale
				out.Reason = "observation conclusion is not newer than stored version"
				out.Observation = o.ObservationID
				return out
			}
			mergeObservation(sh.observations[o.ObservationID], o)
			updated++
			basis = append(basis, "observation:"+o.ObservationID+":updated")
		} else {
			sh.observations[o.ObservationID] = cloneObservation(o)
			created++
			basis = append(basis, "observation:"+o.ObservationID+":new")
		}
		sh.obsProv[o.ObservationID] = provenance{o.UpdatedAt, cs.Rank, cs.MessageID}
	}

	// Commit idempotency only after the whole message merged successfully.
	s.idemMu.Lock()
	s.idem[cs.MessageID] = idemRec{Digest: digest, MRN: mrn}
	s.idemMu.Unlock()

	if updated > 0 {
		out.Status = domain.StatusUpdated
	} else {
		out.Status = domain.StatusAccepted
	}
	sort.Strings(basis)
	out.Reason = strings.Join(basis, "; ")
	_ = now
	return out
}

// isNewer decides whether a candidate conclusion may replace the stored one.
// Ordering: business timestamp, then wire-version rank, then the lexicographic
// message id as a final tiebreak. The lex tiebreak makes the result independent
// of delivery order (re-runnable batches converge to one identical state).
func isNewer(candAt time.Time, candRank domain.VersionRank, candMsg string, rec provenance) bool {
	if candAt.After(rec.UpdatedAt) {
		return true
	}
	if candAt.Before(rec.UpdatedAt) {
		return false
	}
	if candRank != rec.Rank {
		return candRank > rec.Rank
	}
	return candMsg < rec.Writer
}

func mergePatient(dst *domain.Patient, src *domain.Patient) {
	// Required identity/value fields are overwritten; optional fields are
	// merged only when the newer message actually carries them.
	dst.FamilyName = src.FamilyName
	dst.BirthDate = src.BirthDate
	dst.Gender = src.Gender
	if src.GivenName != "" {
		dst.GivenName = src.GivenName
	}
	dst.SourceSystem = src.SourceSystem
	dst.Version = src.Version
	dst.UpdatedAt = src.UpdatedAt
}

func mergeEncounter(dst *domain.Encounter, src *domain.Encounter) {
	dst.Class = src.Class
	if src.StartedAt != "" {
		dst.StartedAt = src.StartedAt
	}
	if src.EndedAt != "" {
		dst.EndedAt = src.EndedAt
	}
	dst.SourceSystem = src.SourceSystem
	dst.Version = src.Version
	dst.UpdatedAt = src.UpdatedAt
}

func mergeObservation(dst *domain.Observation, src *domain.Observation) {
	dst.TestCode = src.TestCode
	if src.TestName != "" {
		dst.TestName = src.TestName
	}
	dst.ValueString = src.ValueString
	dst.ValueNumber = src.ValueNumber
	if src.Unit != "" {
		dst.Unit = src.Unit
	}
	dst.Status = src.Status
	if src.EffectiveTime != "" {
		dst.EffectiveTime = src.EffectiveTime
	}
	if src.VisitID != "" {
		dst.VisitID = src.VisitID
	}
	dst.SourceSystem = src.SourceSystem
	dst.Version = src.Version
	dst.UpdatedAt = src.UpdatedAt
}

// Snapshot returns a deterministic deep copy of current data.
func (s *Store) Snapshot() Snapshot {
	snap := Snapshot{}
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for _, p := range sh.patients {
			snap.Patients = append(snap.Patients, clonePatient(p))
		}
		for _, e := range sh.encounters {
			snap.Encounters = append(snap.Encounters, cloneEncounter(e))
		}
		for _, o := range sh.observations {
			snap.Observations = append(snap.Observations, cloneObservation(o))
		}
		sh.mu.Unlock()
	}
	sort.Slice(snap.Patients, func(i, j int) bool { return snap.Patients[i].MRN < snap.Patients[j].MRN })
	sort.Slice(snap.Encounters, func(i, j int) bool { return snap.Encounters[i].VisitID < snap.Encounters[j].VisitID })
	sort.Slice(snap.Observations, func(i, j int) bool {
		return snap.Observations[i].ObservationID < snap.Observations[j].ObservationID
	})
	return snap
}

func messageMRN(cs *normalize.ChangeSet) string {
	if cs.Patient != nil {
		return cs.Patient.MRN
	}
	if cs.Encounter != nil {
		return cs.Encounter.MRN
	}
	if len(cs.Observations) > 0 {
		return cs.Observations[0].MRN
	}
	return ""
}

func shardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % shardCount
}

// ContentDigest is the SHA-256 of the canonical message content. Timestamps
// originate from the wire message, so identical bytes always hash identically
// regardless of when they are delivered.
func ContentDigest(cs *normalize.ChangeSet) string {
	var b strings.Builder
	fmt.Fprintf(&b, "msg|%s|%s|%s|%d|%s\n", cs.MessageID, cs.Format, cs.Version, cs.Rank, cs.SourceSystem)
	if cs.Patient != nil {
		p := cs.Patient
		fmt.Fprintf(&b, "P|%s|%s|%s|%s|%s|%s|%d\n", p.MRN, p.FamilyName, p.GivenName, p.BirthDate, p.Gender, p.SourceSystem, p.UpdatedAt.UnixNano())
	}
	if cs.Encounter != nil {
		e := cs.Encounter
		fmt.Fprintf(&b, "E|%s|%s|%s|%s|%s|%s|%d\n", e.VisitID, e.MRN, e.Class, e.StartedAt, e.EndedAt, e.SourceSystem, e.UpdatedAt.UnixNano())
	}
	obs := append([]*domain.Observation(nil), cs.Observations...)
	sort.Slice(obs, func(i, j int) bool { return obs[i].ObservationID < obs[j].ObservationID })
	for _, o := range obs {
		num := ""
		if o.ValueNumber != nil {
			num = fmt.Sprintf("%v", *o.ValueNumber)
		}
		fmt.Fprintf(&b, "O|%s|%s|%s|%s|%s|s=%s|n=%s|%s|%s|%s|%s|%d\n",
			o.ObservationID, o.MRN, o.VisitID, o.TestCode, o.TestName,
			o.ValueString, num, o.Unit, o.Status, o.EffectiveTime, o.SourceSystem, o.UpdatedAt.UnixNano())
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func clonePatient(p *domain.Patient) *domain.Patient {
	c := *p
	return &c
}

func cloneEncounter(e *domain.Encounter) *domain.Encounter { c := *e; return &c }

func cloneObservation(o *domain.Observation) *domain.Observation {
	c := *o
	if o.ValueNumber != nil {
		n := *o.ValueNumber
		c.ValueNumber = &n
	}
	return &c
}
