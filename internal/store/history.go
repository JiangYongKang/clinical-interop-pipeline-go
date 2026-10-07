package store

import (
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

// PatientRevision is one recorded conclusion version for one patient.
type PatientRevision struct {
	Seq         int64
	ReceivedAt  time.Time // arrival instant (serving clock); never rewritten
	BusinessAt  time.Time // the conclusion's own business time (wire time)
	Rank        domain.VersionRank
	WriterMsgID string
	Patient     *domain.Patient
}

// EncounterRevision is one recorded conclusion version for one encounter.
type EncounterRevision struct {
	Seq         int64
	ReceivedAt  time.Time
	BusinessAt  time.Time
	Rank        domain.VersionRank
	WriterMsgID string
	Encounter   *domain.Encounter
}

// ObservationRevision is one recorded conclusion version for one observation.
type ObservationRevision struct {
	Seq         int64
	ReceivedAt  time.Time
	BusinessAt  time.Time
	Rank        domain.VersionRank
	WriterMsgID string
	Observation *domain.Observation
}

// AsOf returns the view of every entity as it was known at instant at.
//
// Visibility rule (identical for HL7 2.3.1 / 2.5.1 and FHIR R4): a revision is
// visible iff it had BOTH arrived (ReceivedAt <= at) and taken business effect
// (BusinessAt <= at) by at; among visible revisions of one entity the one with
// the greatest Seq — i.e. the last conclusion already known at at — wins, using
// the same (business time, version rank, message id) ordering as live ingestion.
// Consequently:
//   - a late-arriving old message (ReceivedAt > at) cannot rewrite a settled
//     historical view, even if its business time precedes at;
//   - a future-dated conclusion already delivered (BusinessAt > at) stays
//     invisible until its business time;
//   - repeated queries at the same at over an ever-growing journal return the
//     identical view.
func (s *Store) AsOf(at time.Time) Snapshot {
	at = at.UTC()
	snap := Snapshot{}
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for _, revs := range sh.patientRevs {
			if r, ok := visiblePatient(revs, at); ok {
				snap.Patients = append(snap.Patients, clonePatient(r.Patient))
			}
		}
		for _, revs := range sh.encRevs {
			if r, ok := visibleEncounter(revs, at); ok {
				snap.Encounters = append(snap.Encounters, cloneEncounter(r.Encounter))
			}
		}
		for _, revs := range sh.obsRevs {
			if r, ok := visibleObservation(revs, at); ok {
				snap.Observations = append(snap.Observations, cloneObservation(r.Observation))
			}
		}
		sh.mu.Unlock()
	}
	sortSnap(&snap)
	return snap
}

// PatientAt returns the patient conclusion visible at at (nil if none yet).
func (s *Store) PatientAt(mrn string, at time.Time) *domain.Patient {
	sh := s.shards[shardIndex(mrn)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if r, ok := visiblePatient(sh.patientRevs[mrn], at.UTC()); ok {
		return clonePatient(r.Patient)
	}
	return nil
}

// PatientHistory returns every recorded patient revision in arrival order.
// Corrections and withdrawals stay visible on the timeline as separate
// revisions (an observation's "cancelled"/"entered-in-error" conclusion is a
// revision, never a deletion).
func (s *Store) PatientHistory(mrn string) []PatientRevision {
	sh := s.shards[shardIndex(mrn)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	revs := sh.patientRevs[mrn]
	out := make([]PatientRevision, len(revs))
	for i, r := range revs {
		r.Patient = clonePatient(r.Patient)
		out[i] = r
	}
	return out
}

// EncounterHistory returns every recorded encounter revision in arrival order.
// Encounters are stored in the shard of their owning patient MRN, so the
// lookup scans shards rather than hashing the visit id.
func (s *Store) EncounterHistory(visitID string) []EncounterRevision {
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		revs, ok := sh.encRevs[visitID]
		sh.mu.Unlock()
		if !ok {
			continue
		}
		out := make([]EncounterRevision, len(revs))
		for j, r := range revs {
			r.Encounter = cloneEncounter(r.Encounter)
			out[j] = r
		}
		return out
	}
	return nil
}

// ObservationHistory returns every recorded observation revision in arrival
// order, so corrections and withdrawals are inspectable on a timeline.
// Observations are stored in the shard of their owning patient MRN.
func (s *Store) ObservationHistory(obsID string) []ObservationRevision {
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		revs, ok := sh.obsRevs[obsID]
		sh.mu.Unlock()
		if !ok {
			continue
		}
		out := make([]ObservationRevision, len(revs))
		for j, r := range revs {
			r.Observation = cloneObservation(r.Observation)
			out[j] = r
		}
		return out
	}
	return nil
}

func visiblePatient(revs []PatientRevision, at time.Time) (PatientRevision, bool) {
	winner := -1
	for i := range revs {
		if !revs[i].ReceivedAt.After(at) && !revs[i].BusinessAt.After(at) {
			winner = i
		}
	}
	if winner < 0 {
		return PatientRevision{}, false
	}
	return revs[winner], true
}

func visibleEncounter(revs []EncounterRevision, at time.Time) (EncounterRevision, bool) {
	winner := -1
	for i := range revs {
		if !revs[i].ReceivedAt.After(at) && !revs[i].BusinessAt.After(at) {
			winner = i
		}
	}
	if winner < 0 {
		return EncounterRevision{}, false
	}
	return revs[winner], true
}

func visibleObservation(revs []ObservationRevision, at time.Time) (ObservationRevision, bool) {
	winner := -1
	for i := range revs {
		if !revs[i].ReceivedAt.After(at) && !revs[i].BusinessAt.After(at) {
			winner = i
		}
	}
	if winner < 0 {
		return ObservationRevision{}, false
	}
	return revs[winner], true
}
