package store

import (
	"sort"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

// EntityRevision is one winning conclusion for one entity at one point in
// its timeline. Revisions are append-only; corrections and withdrawals
// (observation status cancelled / entered-in-error) both remain visible.
type EntityRevision struct {
	Kind       string // "patient" | "encounter" | "observation"
	ID         string
	MRN        string
	UpdatedAt  time.Time
	Rank       domain.VersionRank
	Writer     string
	RecordedAt time.Time

	Patient     *domain.Patient
	Encounter   *domain.Encounter
	Observation *domain.Observation
}

// HistoryOptions narrows an as-of / timeline view.
type HistoryOptions struct {
	MRN  string // when set, only this patient's entities are returned
	Kind string // when set, only this entity kind ("patient"|"encounter"|"observation")
	ID   string // when set, only this entity id
}

// SnapshotAt reconstructs the conclusion as it stood at business time at.
//
// One revision participates only when BOTH hold:
//
//	r.UpdatedAt  <= at   (the conclusion pertains to that time or earlier)
//	r.RecordedAt <= at   (the conclusion had actually arrived by then)
//
// The second condition is what stops a late-delivered old message from
// rewriting a history that was already fixed at `at`. Among the eligible
// revisions of one entity the winner is picked by the exact same total
// order used live: business UpdatedAt, then wire-version rank, then the
// lexicographic writer message id. HL7 2.3.1 / 2.5.1 and FHIR R4 therefore
// follow one rule and repeated reads at the same `at` always return the
// identical snapshot.
func (s *Store) SnapshotAt(at time.Time, opts HistoryOptions) Snapshot {
	at = at.UTC()
	wantKind := map[string]bool{}
	switch opts.Kind {
	case "":
		wantKind = map[string]bool{"patient": true, "encounter": true, "observation": true}
	case "patient", "encounter", "observation":
		wantKind[opts.Kind] = true
	default:
		return Snapshot{}
	}

	snap := Snapshot{}
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		if wantKind["patient"] {
			for id, revs := range sh.patRevs {
				if opts.ID != "" && opts.Kind != "" && id != opts.ID {
					continue
				}
				if r, ok := winnerAt(revs, at); ok && (opts.MRN == "" || r.MRN == opts.MRN) {
					snap.Patients = append(snap.Patients, clonePatient(r.Patient))
				}
			}
		}
		if wantKind["encounter"] {
			for id, revs := range sh.encRevs {
				if opts.ID != "" && opts.Kind != "" && id != opts.ID {
					continue
				}
				if r, ok := winnerAt(revs, at); ok && (opts.MRN == "" || r.MRN == opts.MRN) {
					snap.Encounters = append(snap.Encounters, cloneEncounter(r.Encounter))
				}
			}
		}
		if wantKind["observation"] {
			for id, revs := range sh.obsRevs {
				if opts.ID != "" && opts.Kind != "" && id != opts.ID {
					continue
				}
				if r, ok := winnerAt(revs, at); ok && (opts.MRN == "" || r.MRN == opts.MRN) {
					snap.Observations = append(snap.Observations, cloneObservation(r.Observation))
				}
			}
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

// winnerAt returns the winning eligible revision at time at, or ok=false
// when no eligible revision exists yet.
func winnerAt(revs []EntityRevision, at time.Time) (EntityRevision, bool) {
	var best EntityRevision
	found := false
	for _, r := range revs {
		if r.UpdatedAt.After(at) || r.RecordedAt.After(at) {
			continue
		}
		if !found || revisionBefore(best, r) {
			best, found = r, true
		}
	}
	return best, found
}

// revisionBefore reports whether a ranks strictly before b in the winning
// total order (UpdatedAt, rank, writer). It mirrors store.isNewer.
func revisionBefore(a, b EntityRevision) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.Before(b.UpdatedAt)
	}
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	return a.Writer < b.Writer
}

// Timeline returns every winning revision of one entity in ascending
// business-time order (UpdatedAt, rank, writer). Corrections and withdrawals
// remain distinct entries instead of being collapsed away.
func (s *Store) Timeline(kind, id string) ([]EntityRevision, error) {
	var revs []EntityRevision
	switch kind {
	case "patient", "encounter", "observation":
	default:
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unknown entity kind"}
	}
	// Entities are physically stored in the owning patient's MRN shard,
	// which is unrelated to the encounter/observation id, so every shard is
	// scanned (one shard lock at a time).
	for i := 0; i < shardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		var src []EntityRevision
		switch kind {
		case "patient":
			src = sh.patRevs[id]
		case "encounter":
			src = sh.encRevs[id]
		case "observation":
			src = sh.obsRevs[id]
		}
		for _, r := range src {
			revs = append(revs, cloneRevision(r))
		}
		sh.mu.Unlock()
	}

	sort.SliceStable(revs, func(i, j int) bool { return revisionBefore(revs[i], revs[j]) })
	return revs, nil
}

func cloneRevision(r EntityRevision) EntityRevision {
	c := r
	if r.Patient != nil {
		c.Patient = clonePatient(r.Patient)
	}
	if r.Encounter != nil {
		c.Encounter = cloneEncounter(r.Encounter)
	}
	if r.Observation != nil {
		c.Observation = cloneObservation(r.Observation)
	}
	return c
}
