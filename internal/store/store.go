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

	// Append-only conclusion journals. Every successful create/update appends
	// exactly one revision; rejections never append. The stored entity is the
	// post-merge conclusion, so an as-of view is the last eligible revision.
	patientRevs map[string][]PatientRevision
	encRevs     map[string][]EncounterRevision
	obsRevs     map[string][]ObservationRevision
}

// Store is a sharded, in-memory, concurrency-safe clinical data store.
// All entities of one message share one patient MRN; a batch may span several
// MRNs, so batch transactions lock every involved shard in ascending index
// order and always take the global idempotency lock AFTER the shard locks —
// one global lock order makes deadlocks impossible.
type Store struct {
	shards [shardCount]*shard
	idemMu sync.Mutex
	idem   map[string]idemRec
	seq    int64 // global, monotonically increasing revision number
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
			patientRevs:  map[string][]PatientRevision{},
			encRevs:      map[string][]EncounterRevision{},
			obsRevs:      map[string][]ObservationRevision{},
		}
	}
	return s
}

// Apply validates idempotency, performs stale/type-conflict checks and merges
// field-level updates atomically per message.
func (s *Store) Apply(cs *normalize.ChangeSet, now time.Time) Outcome {
	outs, _, out := s.applyTx([]*normalize.ChangeSet{cs}, now)
	if out != nil {
		return *out
	}
	return outs[0]
}

// txReject describes why a transaction must abort before committing anything.
type txReject struct {
	index  int
	status domain.Status
	reason string
	obsID  string
}

// shardView is a private, copy-on-write working copy of one shard. Until a
// candidate actually writes, it aliases the committed maps; the first write
// clones every map, so an aborted transaction leaves committed bytes untouched.
type shardView struct {
	sh           *shard
	patients     map[string]*domain.Patient
	encounters   map[string]*domain.Encounter
	observations map[string]*domain.Observation
	patientProv  map[string]provenance
	encProv      map[string]provenance
	obsProv      map[string]provenance
	patientRevs  map[string][]PatientRevision
	encRevs      map[string][]EncounterRevision
	obsRevs      map[string][]ObservationRevision
	dirty        bool
}

func newView(sh *shard) *shardView {
	return &shardView{
		sh:           sh,
		patients:     sh.patients,
		encounters:   sh.encounters,
		observations: sh.observations,
		patientProv:  sh.patientProv,
		encProv:      sh.encProv,
		obsProv:      sh.obsProv,
		patientRevs:  sh.patientRevs,
		encRevs:      sh.encRevs,
		obsRevs:      sh.obsRevs,
	}
}

// touch clones the shard maps once for private mutation.
func (v *shardView) touch() {
	if v.dirty {
		return
	}
	v.patients = clonePatientMap(v.sh.patients)
	v.encounters = cloneEncounterMap(v.sh.encounters)
	v.observations = cloneObservationMap(v.sh.observations)
	v.patientProv = cloneProvP(v.sh.patientProv)
	v.encProv = cloneProvE(v.sh.encProv)
	v.obsProv = cloneProvO(v.sh.obsProv)
	v.patientRevs = clonePatientRevMap(v.sh.patientRevs)
	v.encRevs = cloneEncounterRevMap(v.sh.encRevs)
	v.obsRevs = cloneObservationRevMap(v.sh.obsRevs)
	v.dirty = true
}

// commit installs the working copies back onto the shard.
func (v *shardView) commit() {
	if !v.dirty {
		return
	}
	v.sh.patients = v.patients
	v.sh.encounters = v.encounters
	v.sh.observations = v.observations
	v.sh.patientProv = v.patientProv
	v.sh.encProv = v.encProv
	v.sh.obsProv = v.obsProv
	v.sh.patientRevs = v.patientRevs
	v.sh.encRevs = v.encRevs
	v.sh.obsRevs = v.obsRevs
}

// applyTx runs the validate-then-commit engine shared by single Apply and
// ApplyBatch. On hard failure it returns a txReject and installs nothing.
func (s *Store) applyTx(css []*normalize.ChangeSet, now time.Time) ([]Outcome, int, *Outcome) {
	mrns := make([]string, len(css))
	for i, cs := range css {
		mrns[i] = messageMRN(cs)
		if mrns[i] == "" {
			o := Outcome{MessageID: safeMsgID(cs), Status: domain.StatusRejectedMissingField,
				Reason: "message contains no attributable patient"}
			return nil, i, &o
		}
	}

	// Lock every involved shard in one global (ascending) order.
	idxSet := map[int]bool{}
	for _, mrn := range mrns {
		idxSet[int(shardIndex(mrn))] = true
	}
	idxs := make([]int, 0, len(idxSet))
	for i := range idxSet {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	for _, i := range idxs {
		s.shards[i].mu.Lock()
		defer s.shards[i].mu.Unlock()
	}
	s.idemMu.Lock()
	defer s.idemMu.Unlock()

	views := map[int]*shardView{}
	view := func(mrn string) *shardView {
		i := int(shardIndex(mrn))
		v := views[i]
		if v == nil {
			v = newView(s.shards[i])
			views[i] = v
		}
		return v
	}
	// Working idempotency table: committed entries plus entries staged by
	// earlier messages of THIS transaction, so in-batch replay is recognized.
	workIdem := make(map[string]idemRec, len(s.idem))
	for k, v := range s.idem {
		workIdem[k] = v
	}

	outs := make([]Outcome, len(css))
	created, updated, deduped := 0, 0, 0

	for i, cs := range css {
		mrn := mrns[i]
		digest := ContentDigest(cs)
		out := Outcome{MessageID: cs.MessageID, PatientMRN: mrn, Digest: digest}
		if cs.Encounter != nil {
			out.VisitID = cs.Encounter.VisitID
		}
		v := view(mrn)

		if rec, ok := workIdem[cs.MessageID]; ok {
			if rec.Digest == digest {
				out.Status = domain.StatusDeduplicated
				out.Reason = "identical re-delivery of already applied message"
				outs[i] = out
				deduped++
				continue
			}
			rj := Outcome{MessageID: cs.MessageID, PatientMRN: mrn, Digest: digest,
				Status: domain.StatusConflict, Reason: "same message id re-used with different content"}
			return nil, i, &rj
		}

		// All checks below mutate only private working copies; on rejection
		// nothing is committed, so a corrected message can be re-sent later.
		var basis []string
		itemCreated, itemUpdated := stage(s, v, cs, now, &out, &basis)
		if out.Status != "" {
			// hard rejection staged -> abort the whole transaction
			return nil, i, &out
		}

		workIdem[cs.MessageID] = idemRec{Digest: digest, MRN: mrn}
		if itemUpdated > 0 {
			out.Status = domain.StatusUpdated
		} else {
			out.Status = domain.StatusAccepted
		}
		created += itemCreated
		updated += itemUpdated
		sort.Strings(basis)
		out.Reason = strings.Join(basis, "; ")
		outs[i] = out
	}

	// All messages passed: install working copies and idempotency entries.
	for _, v := range views {
		v.commit()
	}
	for i, cs := range css {
		if outs[i].Status == domain.StatusDeduplicated {
			continue
		}
		s.idem[cs.MessageID] = idemRec{Digest: outs[i].Digest, MRN: mrns[i]}
	}
	_ = created
	_ = updated
	_ = deduped
	return outs, 0, nil
}

// stage validates and buffers one message's merges into the shard view. On a
// hard rejection it sets out.Status/out.Reason and returns. Revisions are
// appended to the view's private journal slices.
func stage(s *Store, v *shardView, cs *normalize.ChangeSet, now time.Time,
	out *Outcome, basis *[]string) (created, updates int) {

	if cs.Patient != nil {
		mrn := cs.Patient.MRN
		if rec, ok := v.patientProv[mrn]; ok {
			if !isNewer(cs.Patient.UpdatedAt, cs.Rank, cs.MessageID, rec) {
				out.Status = domain.StatusStale
				out.Reason = "patient conclusion is not newer than stored version"
				return 0, 0
			}
			merged := clonePatient(v.patients[mrn])
			mergePatient(merged, cs.Patient)
			v.touch()
			v.patients[mrn] = merged
			updates++
			*basis = append(*basis, "patient:updated")
		} else {
			v.touch()
			v.patients[mrn] = clonePatient(cs.Patient)
			created++
			*basis = append(*basis, "patient:new")
		}
		v.patientProv[mrn] = provenance{cs.Patient.UpdatedAt, cs.Rank, cs.MessageID}
		v.patientRevs[mrn] = append(v.patientRevs[mrn], PatientRevision{
			Seq: s.nextSeq(), ReceivedAt: now.UTC(), BusinessAt: cs.Patient.UpdatedAt,
			Rank: cs.Rank, WriterMsgID: cs.MessageID, Patient: clonePatient(v.patients[mrn]),
		})
	}

	if cs.Encounter != nil {
		e := cs.Encounter
		if prov, ok := v.encProv[e.VisitID]; ok {
			if !isNewer(e.UpdatedAt, cs.Rank, cs.MessageID, prov) {
				out.Status = domain.StatusStale
				out.Reason = "encounter conclusion is not newer than stored version"
				return created, updates
			}
			merged := cloneEncounter(v.encounters[e.VisitID])
			mergeEncounter(merged, e)
			v.touch()
			v.encounters[e.VisitID] = merged
			updates++
			*basis = append(*basis, "encounter:updated")
		} else {
			v.touch()
			v.encounters[e.VisitID] = cloneEncounter(e)
			created++
			*basis = append(*basis, "encounter:new")
		}
		v.encProv[e.VisitID] = provenance{e.UpdatedAt, cs.Rank, cs.MessageID}
		v.encRevs[e.VisitID] = append(v.encRevs[e.VisitID], EncounterRevision{
			Seq: s.nextSeq(), ReceivedAt: now.UTC(), BusinessAt: e.UpdatedAt,
			Rank: cs.Rank, WriterMsgID: cs.MessageID, Encounter: cloneEncounter(v.encounters[e.VisitID]),
		})
	}

	for _, o := range cs.Observations {
		if ex, ok := v.observations[o.ObservationID]; ok {
			if (ex.ValueNumber == nil) != (o.ValueNumber == nil) {
				out.Status = domain.StatusRejectedTypeConflict
				out.Reason = "observation value type conflicts with stored value"
				out.Observation = o.ObservationID
				return created, updates
			}
			if prov, ok := v.obsProv[o.ObservationID]; ok && !isNewer(o.UpdatedAt, cs.Rank, cs.MessageID, prov) {
				out.Status = domain.StatusStale
				out.Reason = "observation conclusion is not newer than stored version"
				out.Observation = o.ObservationID
				return created, updates
			}
			merged := cloneObservation(v.observations[o.ObservationID])
			mergeObservation(merged, o)
			v.touch()
			v.observations[o.ObservationID] = merged
			updates++
			*basis = append(*basis, "observation:"+o.ObservationID+":updated")
		} else {
			v.touch()
			v.observations[o.ObservationID] = cloneObservation(o)
			created++
			*basis = append(*basis, "observation:"+o.ObservationID+":new")
		}
		v.obsProv[o.ObservationID] = provenance{o.UpdatedAt, cs.Rank, cs.MessageID}
		v.obsRevs[o.ObservationID] = append(v.obsRevs[o.ObservationID], ObservationRevision{
			Seq: s.nextSeq(), ReceivedAt: now.UTC(), BusinessAt: o.UpdatedAt,
			Rank: cs.Rank, WriterMsgID: cs.MessageID,
			Observation: cloneObservation(v.observations[o.ObservationID]),
		})
	}
	return created, updates
}

func (s *Store) nextSeq() int64 {
	s.seq++
	return s.seq
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
	sortSnap(&snap)
	return snap
}

func sortSnap(snap *Snapshot) {
	sort.Slice(snap.Patients, func(i, j int) bool { return snap.Patients[i].MRN < snap.Patients[j].MRN })
	sort.Slice(snap.Encounters, func(i, j int) bool { return snap.Encounters[i].VisitID < snap.Encounters[j].VisitID })
	sort.Slice(snap.Observations, func(i, j int) bool {
		return snap.Observations[i].ObservationID < snap.Observations[j].ObservationID
	})
}

func messageMRN(cs *normalize.ChangeSet) string {
	if cs == nil {
		return ""
	}
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

func safeMsgID(cs *normalize.ChangeSet) string {
	if cs == nil {
		return ""
	}
	return cs.MessageID
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

func clonePatientMap(m map[string]*domain.Patient) map[string]*domain.Patient {
	out := make(map[string]*domain.Patient, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneEncounterMap(m map[string]*domain.Encounter) map[string]*domain.Encounter {
	out := make(map[string]*domain.Encounter, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneObservationMap(m map[string]*domain.Observation) map[string]*domain.Observation {
	out := make(map[string]*domain.Observation, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneProvP(m map[string]provenance) map[string]provenance {
	out := make(map[string]provenance, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneProvE(m map[string]provenance) map[string]provenance { return cloneProvP(m) }
func cloneProvO(m map[string]provenance) map[string]provenance { return cloneProvP(m) }

func clonePatientRevMap(m map[string][]PatientRevision) map[string][]PatientRevision {
	out := make(map[string][]PatientRevision, len(m))
	for k, v := range m {
		out[k] = append([]PatientRevision(nil), v...)
	}
	return out
}

func cloneEncounterRevMap(m map[string][]EncounterRevision) map[string][]EncounterRevision {
	out := make(map[string][]EncounterRevision, len(m))
	for k, v := range m {
		out[k] = append([]EncounterRevision(nil), v...)
	}
	return out
}

func cloneObservationRevMap(m map[string][]ObservationRevision) map[string][]ObservationRevision {
	out := make(map[string][]ObservationRevision, len(m))
	for k, v := range m {
		out[k] = append([]ObservationRevision(nil), v...)
	}
	return out
}
