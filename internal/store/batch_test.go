package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

func encCS(msg, mrn, visit string, updated time.Time) *normalize.ChangeSet {
	return &normalize.ChangeSet{
		MessageID: msg, Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: baseTime,
		Encounter: &domain.Encounter{
			VisitID: visit, MRN: mrn, Class: "IMP",
			SourceSystem: "S", Version: "R4", UpdatedAt: updated,
		},
	}
}

func TestApplyBatch_AllOrNothingOnRejection(t *testing.T) {
	cases := map[string]struct {
		bad func(good []*normalize.ChangeSet) []*normalize.ChangeSet
		// wantIndex is the 0-based position of the offending message.
		wantIndex int
		want      domain.Status
	}{
		"unsupported/version-independent stale rejects whole batch": {
			bad: func(good []*normalize.ChangeSet) []*normalize.ChangeSet {
				// seed a newer conclusion first (outside the batch), then the
				// batch contains an older one that must make the batch abort.
				return good
			},
			want: domain.StatusStale,
		},
		"type conflict inside batch rejects whole batch": {
			bad: func(good []*normalize.ChangeSet) []*normalize.ChangeSet {
				return good
			},
			want: domain.StatusRejectedTypeConflict,
		},
		"missing attributable patient rejects whole batch": {
			bad: func(good []*normalize.ChangeSet) []*normalize.ChangeSet {
				return good
			},
			want: domain.StatusRejectedMissingField,
		},
	}
	_ = cases

	t.Run("stale message aborts the entire batch", func(t *testing.T) {
		s := New()
		s.Apply(patientCS("SEED", "m1", "CURRENT", "R4", baseTime.Add(4*time.Hour)), baseTime)
		batch := []*normalize.ChangeSet{
			patientCS("B1", "m2", "P2", "R4", baseTime),
			patientCS("B2", "m1", "OLDER", "R4", baseTime), // stale vs SEED
			patientCS("B3", "m3", "P3", "R4", baseTime),
		}
		before := s.Snapshot()
		bo := s.ApplyBatch(batch, baseTime)
		if bo.Status != domain.StatusStale {
			t.Fatalf("status=%s want stale", bo.Status)
		}
		if bo.FailedIndex != 1 {
			t.Fatalf("failed index=%d want 1", bo.FailedIndex)
		}
		if bo.Results != nil {
			t.Fatal("rejected batch must carry no partial results")
		}
		after := s.Snapshot()
		if len(after.Patients) != len(before.Patients) {
			t.Fatalf("half-applied batch: patients before=%d after=%d",
				len(before.Patients), len(after.Patients))
		}
		for _, mrn := range []string{"m2", "m3"} {
			if s.PatientAt(mrn, baseTime.Add(24*time.Hour)) != nil {
				t.Fatalf("patient %s landed despite batch abort", mrn)
			}
		}
		// fixing the offending message and resending the WHOLE batch applies
		fixed := append([]*normalize.ChangeSet(nil), batch...)
		fixed[1] = patientCS("B2", "m1", "NEWER", "R4", baseTime.Add(5*time.Hour))
		bo2 := s.ApplyBatch(fixed, baseTime)
		if bo2.Status != domain.StatusAccepted {
			t.Fatalf("corrected batch status=%s want accepted", bo2.Status)
		}
		if len(bo2.Results) != 3 {
			t.Fatalf("want 3 item results, got %d", len(bo2.Results))
		}
		if got := s.Snapshot().Patients; len(got) != 3 {
			t.Fatalf("want 3 patients after corrected batch, got %d", len(got))
		}
	})

	t.Run("type conflict inside batch aborts the entire batch", func(t *testing.T) {
		s := New()
		s.Apply(obsCS("SEED-O", "m1", "o1", 1.0, baseTime), baseTime)
		strVal := "high"
		bad := &normalize.ChangeSet{
			MessageID: "BAD", Version: "R4", Rank: normalize.VersionRanks["R4"],
			ReceivedAt: baseTime,
			Observations: []*domain.Observation{{
				ObservationID: "o1", MRN: "m1", TestCode: "C", Status: "final",
				ValueString: strVal, Version: "R4", UpdatedAt: baseTime.Add(time.Hour),
			}},
		}
		batch := []*normalize.ChangeSet{
			patientCS("BP1", "m2", "P2", "R4", baseTime),
			bad, // type conflict against committed numeric o1
			patientCS("BP2", "m3", "P3", "R4", baseTime),
		}
		bo := s.ApplyBatch(batch, baseTime)
		if bo.Status != domain.StatusRejectedTypeConflict || bo.FailedIndex != 1 {
			t.Fatalf("status=%s idx=%d", bo.Status, bo.FailedIndex)
		}
		if got := len(s.Snapshot().Patients); got != 0 {
			t.Fatalf("patients landed despite abort: %d", got)
		}
	})

	t.Run("missing attributable patient aborts the entire batch", func(t *testing.T) {
		s := New()
		empty := &normalize.ChangeSet{MessageID: "NO-MRN", Version: "R4",
			Rank: normalize.VersionRanks["R4"]}
		batch := []*normalize.ChangeSet{
			patientCS("BP1", "m2", "P2", "R4", baseTime),
			empty,
		}
		bo := s.ApplyBatch(batch, baseTime)
		if bo.Status != domain.StatusRejectedMissingField || bo.FailedIndex != 1 {
			t.Fatalf("status=%s idx=%d", bo.Status, bo.FailedIndex)
		}
		if s.PatientAt("m2", baseTime.Add(time.Hour)) != nil {
			t.Fatal("earlier batch message must not land")
		}
	})
}

func TestApplyBatch_ReplayDeterministic(t *testing.T) {
	batch := []*normalize.ChangeSet{
		patientCS("R1", "m1", "V1", "2.3.1", baseTime.Add(1*time.Hour)),
		patientCS("R2", "m1", "V2", "2.5.1", baseTime.Add(2*time.Hour)),
		encCS("R3", "m1", "v-1", baseTime.Add(2*time.Hour)),
		patientCS("R4", "m2", "OTHER", "R4", baseTime),
	}
	run := func() string {
		s := New()
		b1 := s.ApplyBatch(batch, baseTime)
		b2 := s.ApplyBatch(batch, baseTime) // exact re-run of the same batch
		var sb strings.Builder
		sb.WriteString(string(b1.Status) + "|")
		sb.WriteString(string(b2.Status) + "|")
		snap := s.Snapshot()
		sb.WriteString(fmt.Sprintf("p=%d,e=%d|", len(snap.Patients), len(snap.Encounters)))
		sb.WriteString(snap.Patients[0].FamilyName + "@" + snap.Patients[0].Version)
		return sb.String()
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("batch replay not deterministic:\n%s\n%s", a, b)
	}
	want := "accepted|deduplicated|p=2,e=1|V2@2.5.1"
	if a != want {
		t.Fatalf("got %s want %s", a, want)
	}
}

func TestApplyBatch_SameIDDifferentContentRejectsAndRetriable(t *testing.T) {
	s := New()
	original := patientCS("X1", "m1", "GOOD", "R4", baseTime)
	bo := s.ApplyBatch([]*normalize.ChangeSet{original}, baseTime)
	if bo.Status != domain.StatusAccepted {
		t.Fatalf("initial batch: %s", bo.Status)
	}
	// resend with the same id but different content -> whole batch rejected,
	// stored bytes unchanged
	tampered := patientCS("X1", "m1", "TAMPERED", "R4", baseTime.Add(time.Hour))
	sibling := patientCS("X2", "m2", "SIBLING", "R4", baseTime)
	bo2 := s.ApplyBatch([]*normalize.ChangeSet{tampered, sibling}, baseTime)
	if bo2.Status != domain.StatusConflict || bo2.FailedIndex != 0 {
		t.Fatalf("status=%s idx=%d", bo2.Status, bo2.FailedIndex)
	}
	snap := s.Snapshot()
	if len(snap.Patients) != 1 || snap.Patients[0].FamilyName != "GOOD" {
		t.Fatalf("conflict changed state: %+v", snap.Patients)
	}
	// resend the ORIGINAL content alongside a new message: must fully apply
	bo3 := s.ApplyBatch([]*normalize.ChangeSet{original, sibling}, baseTime)
	if bo3.Status != domain.StatusAccepted {
		t.Fatalf("retriable batch got %s", bo3.Status)
	}
	if bo3.Results[0].Status != domain.StatusDeduplicated || bo3.Results[1].Status != domain.StatusAccepted {
		t.Fatalf("item statuses: %s %s", bo3.Results[0].Status, bo3.Results[1].Status)
	}
}

func TestApplyBatch_ConcurrentNeverHalfApplied(t *testing.T) {
	s := New()
	const batches = 40
	var wg sync.WaitGroup
	errs := make(chan error, batches)
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			mrnA := fmt.Sprintf("m-%d-a", b)
			mrnB := fmt.Sprintf("m-%d-b", b)
			batch := []*normalize.ChangeSet{
				patientCS(fmt.Sprintf("BM-%d-1", b), mrnA, "A", "R4", baseTime),
				patientCS(fmt.Sprintf("BM-%d-2", b), mrnB, "B", "R4", baseTime),
			}
			bo := s.ApplyBatch(batch, baseTime)
			snap := s.Snapshot()
			hasA, hasB := false, false
			for _, p := range snap.Patients {
				hasA = hasA || p.MRN == mrnA
				hasB = hasB || p.MRN == mrnB
			}
			if bo.Status == domain.StatusAccepted && (!hasA || !hasB) {
				errs <- fmt.Errorf("batch %d accepted but not fully visible", b)
				return
			}
			if bo.Status != domain.StatusAccepted && bo.Status != domain.StatusDeduplicated {
				errs <- fmt.Errorf("batch %d unexpected status %s", b, bo.Status)
				return
			}
			if hasA != hasB {
				errs <- fmt.Errorf("batch %d half visible: a=%v b=%v", b, hasA, hasB)
			}
		}(b)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if got := len(s.Snapshot().Patients); got != 2*batches {
		t.Fatalf("want %d patients, got %d", 2*batches, got)
	}
}
