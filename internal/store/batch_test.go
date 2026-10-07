package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

// encCS builds a minimal encounter message for a patient.
func encCS(msg, mrn, visit, version string, updated time.Time) *normalize.ChangeSet {
	return &normalize.ChangeSet{
		MessageID: msg, Format: "x", Version: version, Rank: normalize.VersionRanks[version],
		SourceSystem: "S", ReceivedAt: baseTime,
		Encounter: &domain.Encounter{
			VisitID: visit, MRN: mrn, Class: "IMP",
			SourceSystem: "S", Version: version, UpdatedAt: updated,
		},
	}
}

func items(cs ...*normalize.ChangeSet) []BatchItem {
	out := make([]BatchItem, len(cs))
	for i, c := range cs {
		out[i] = BatchItem{Ref: "ref-" + c.MessageID, CS: c}
	}
	return out
}

func TestBatch_AllOrNothing_OneBadRejectsEntireBatch(t *testing.T) {
	s := New()
	// Pre-existing newer patient conclusion.
	s.Apply(patientCS("EXISTING", "m1", "CURRENT", "R4", baseTime.Add(10*time.Hour)), baseTime)

	good := patientCS("B1", "m2", "NEWPAT", "R4", baseTime)
	// stale relative to the stored m1 conclusion -> whole batch must abort
	bad := patientCS("B2", "m1", "OLDER", "2.5.1", baseTime.Add(1*time.Hour))

	out := s.ApplyBatch("BATCH-1", items(good, bad), baseTime.Add(time.Minute))
	if out.Status != domain.StatusStale {
		t.Fatalf("want stale rejection, got %s (%s)", out.Status, out.Reason)
	}
	snap := s.Snapshot()
	// m2 from the "good" message must NOT exist: no half-committed state.
	for _, p := range snap.Patients {
		if p.MRN == "m2" {
			t.Fatal("batch partially committed: m2 landed despite rejection")
		}
	}
	if len(snap.Patients) != 1 || snap.Patients[0].MRN != "m1" {
		t.Fatalf("unexpected store state: %+v", snap.Patients)
	}
	t.Log("business=atomic-batch case=one-stale-message-aborts-whole-batch result=pass (no half-committed state)")
	if _, poisoned := s.batchIdem["BATCH-1"]; poisoned {
		t.Fatal("failed batch must not be recorded")
	}
}

func TestBatch_FixAndResubmitCommitsFully(t *testing.T) {
	s := New()
	s.Apply(patientCS("EXISTING", "m1", "CURRENT", "R4", baseTime.Add(10*time.Hour)), baseTime)

	bad := patientCS("B2", "m1", "OLDER", "2.5.1", baseTime.Add(1*time.Hour))
	good := patientCS("B1", "m2", "NEWPAT", "R4", baseTime)

	out := s.ApplyBatch("BATCH-1", items(good, bad), baseTime.Add(time.Minute))
	if out.Status != domain.StatusStale {
		t.Fatalf("first submit should fail stale, got %s", out.Status)
	}
	// Fix the offending message so it is now newer; same batch id re-submitted
	// after a FAILURE must commit fully — the failure leaves no poison.
	fixed := patientCS("B2", "m1", "NEWER", "R4", baseTime.Add(20*time.Hour))
	out = s.ApplyBatch("BATCH-1", items(good, fixed), baseTime.Add(2*time.Minute))
	if out.Status != domain.StatusAccepted && out.Status != domain.StatusUpdated {
		t.Fatalf("fixed batch should commit, got %s (%s)", out.Status, out.Reason)
	}
	snap := s.Snapshot()
	if len(snap.Patients) != 2 {
		t.Fatalf("want 2 patients after fixed batch, got %d", len(snap.Patients))
	}
	m1 := snap.Patients[0].FamilyName
	if snap.Patients[0].MRN != "m1" {
		m1 = snap.Patients[1].FamilyName
	}
	if m1 != "NEWER" {
		t.Fatalf("m1 should be NEWER, got %s", m1)
	}
	t.Log("business=atomic-batch case=fix-offender-and-resubmit result=pass (prior failure not poisoned)")
}

func TestBatch_IdenticalReplayIsDeduplicated(t *testing.T) {
	s := New()
	a := patientCS("B1", "m1", "PAT1", "R4", baseTime)
	b := encCS("B2", "m1", "v1", "2.5.1", baseTime.Add(time.Hour))
	its := items(a, b)

	out1 := s.ApplyBatch("BATCH-X", its, baseTime)
	if out1.Status != domain.StatusAccepted {
		t.Fatalf("first batch=%s", out1.Status)
	}
	// Resubmit same batch id + same content, shuffled order.
	out2 := s.ApplyBatch("BATCH-X", items(b, a), baseTime.Add(5*time.Minute))
	if out2.Status != domain.StatusDeduplicated {
		t.Fatalf("identical replay must be deduplicated, got %s", out2.Status)
	}
	if out2.Digest != out1.Digest {
		t.Fatal("shuffled-order replay must hash to the same batch digest")
	}
	snap := s.Snapshot()
	if len(snap.Patients) != 1 || len(snap.Encounters) != 1 {
		t.Fatalf("replay created data: %+v", snap)
	}
	t.Log("business=batch-idempotency case=same-batch-shuffled-replay result=pass (deduplicated, order independent)")
}

func TestBatch_SameIDDifferentContentRejected(t *testing.T) {
	s := New()
	s.ApplyBatch("BATCH-X", items(patientCS("B1", "m1", "A", "R4", baseTime)), baseTime)
	out := s.ApplyBatch("BATCH-X", items(patientCS("B1", "m1", "DIFFERENT", "R4", baseTime)), baseTime)
	if out.Status != domain.StatusRejectedBatch {
		t.Fatalf("want rejected_batch, got %s", out.Status)
	}
	if got := s.Snapshot().Patients[0].FamilyName; got != "A" {
		t.Fatalf("rejected batch must not mutate data, got %s", got)
	}
}

func TestBatch_OverlapWithCommittedMessageRejected(t *testing.T) {
	s := New()
	// Message B1 already landed via a single-message commit.
	s.Apply(patientCS("B1", "m1", "A", "R4", baseTime), baseTime)
	out := s.ApplyBatch("BATCH-NEW", items(
		patientCS("B1", "m1", "A", "R4", baseTime),
		patientCS("B2", "m2", "B", "R4", baseTime),
	), baseTime.Add(time.Minute))
	if out.Status != domain.StatusRejectedBatch {
		t.Fatalf("overlapping batch must be rejected_batch, got %s", out.Status)
	}
	if len(s.Snapshot().Patients) != 1 {
		t.Fatal("overlapping batch must not add m2")
	}
}

func TestBatch_TypeConflictAbortsWholeBatch(t *testing.T) {
	s := New()
	num := 6.0
	s.Apply(obsCS("OBS-OLD", "m1", "o1", num, baseTime), baseTime)
	conflict := &normalize.ChangeSet{
		MessageID: "C1", Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: baseTime,
		Observations: []*domain.Observation{{
			ObservationID: "o1", MRN: "m1", TestCode: "C", Status: "amended",
			ValueString: "high", UpdatedAt: baseTime.Add(2 * time.Hour),
		}},
	}
	innocent := patientCS("P1", "m9", "BYSTANDER", "R4", baseTime)
	out := s.ApplyBatch("BATCH-C", items(innocent, conflict), baseTime)
	if out.Status != domain.StatusRejectedTypeConflict {
		t.Fatalf("want type conflict, got %s", out.Status)
	}
	for _, p := range s.Snapshot().Patients {
		if p.MRN == "m9" {
			t.Fatal("innocent message must not commit when batch aborts")
		}
	}
}

func TestBatch_DuplicateMessageIDWithinBatch(t *testing.T) {
	s := New()
	a := patientCS("DUP", "m1", "A", "R4", baseTime)
	b := patientCS("DUP", "m2", "B", "R4", baseTime)
	out := s.ApplyBatch("BATCH-D", items(a, b), baseTime)
	if out.Status != domain.StatusRejectedDuplicateID {
		t.Fatalf("want duplicate_id, got %s", out.Status)
	}
}

func TestBatch_ConcurrentDistinctBatchesNoDeadlockNoLoss(t *testing.T) {
	s := New()
	const n = 40
	var wg sync.WaitGroup
	statuses := make(chan domain.Status, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mrn := fmt.Sprintf("bmrn-%02d", i)
			// two messages in the batch, possibly spanning two shards
			its := items(
				patientCS(fmt.Sprintf("BP-%02d", i), mrn, "X", "R4", baseTime),
				encCS(fmt.Sprintf("BE-%02d", i), mrn, fmt.Sprintf("bv-%02d", i), "2.5.1", baseTime.Add(time.Hour)),
			)
			out := s.ApplyBatch(fmt.Sprintf("BATCH-%02d", i), its, baseTime)
			statuses <- out.Status
		}(i)
	}
	wg.Wait()
	close(statuses)
	for st := range statuses {
		if st != domain.StatusAccepted && st != domain.StatusUpdated {
			t.Fatalf("unexpected concurrent batch status %s", st)
		}
	}
	snap := s.Snapshot()
	if len(snap.Patients) != n || len(snap.Encounters) != n {
		t.Fatalf("lost batch writes: patients=%d encounters=%d want %d", len(snap.Patients), len(snap.Encounters), n)
	}
	t.Logf("business=batch-concurrency case=%d-distinct-batches result=pass (no deadlock, no lost writes)", n)
}
