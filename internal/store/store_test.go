package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

var baseTime = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func patientCS(msg, mrn, family, version string, updated time.Time) *normalize.ChangeSet {
	rank := normalize.VersionRanks[version]
	return &normalize.ChangeSet{
		MessageID: msg, Format: "x", Version: version, Rank: rank,
		SourceSystem: "S", ReceivedAt: baseTime,
		Patient: &domain.Patient{
			MRN: mrn, FamilyName: family, GivenName: "G",
			BirthDate: "1970-01-01", Gender: "female",
			SourceSystem: "S", Version: version, UpdatedAt: updated,
		},
	}
}

func obsCS(msg, mrn, oid string, num float64, updated time.Time) *normalize.ChangeSet {
	return &normalize.ChangeSet{
		MessageID: msg, Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: baseTime,
		Observations: []*domain.Observation{{
			ObservationID: oid, MRN: mrn, TestCode: "C", Status: "final",
			ValueNumber: &num, SourceSystem: "S", Version: "R4", UpdatedAt: updated,
		}},
	}
}

func TestApply_IdempotentRedelivery(t *testing.T) {
	s := New()
	cs := patientCS("MSG-1", "m1", "SMITH", "2.5.1", baseTime)
	o1 := s.Apply(cs, baseTime)
	o2 := s.Apply(cs, baseTime)
	if o1.Status != domain.StatusAccepted {
		t.Fatalf("first apply=%s", o1.Status)
	}
	if o2.Status != domain.StatusDeduplicated {
		t.Fatalf("redelivery apply=%s want deduplicated", o2.Status)
	}
	if o1.Digest != o2.Digest {
		t.Fatal("digest must be identical for identical content")
	}
}

func TestApply_SameIDDifferentContentConflict(t *testing.T) {
	s := New()
	s.Apply(patientCS("MSG-1", "m1", "SMITH", "2.5.1", baseTime), baseTime)
	o := s.Apply(patientCS("MSG-1", "m1", "JONES", "2.5.1", baseTime), baseTime)
	if o.Status != domain.StatusConflict {
		t.Fatalf("want conflict, got %s", o.Status)
	}
	if got := s.Snapshot().Patients[0].FamilyName; got != "SMITH" {
		t.Fatalf("conflict must not mutate stored data, family=%s", got)
	}
}

func TestApply_OutOfOrderOlderCannotOverwrite(t *testing.T) {
	s := New()
	older := baseTime.Add(-24 * time.Hour)
	// deliver NEWER first, then OLDER
	s.Apply(patientCS("MSG-NEW", "m1", "NEWNAME", "2.5.1", baseTime), baseTime)
	o := s.Apply(patientCS("MSG-OLD", "m1", "OLDNAME", "2.5.1", older), older)
	if o.Status != domain.StatusStale {
		t.Fatalf("want stale, got %s", o.Status)
	}
	if got := s.Snapshot().Patients[0].FamilyName; got != "NEWNAME" {
		t.Fatalf("older data overwrote newer: %s", got)
	}
}

func TestApply_OrderIndependentConvergence(t *testing.T) {
	// Two delivery orders of the same messages must converge to one state.
	msgs := []*normalize.ChangeSet{
		patientCS("MSG-A", "m1", "V1", "2.3.1", baseTime.Add(1*time.Hour)),
		patientCS("MSG-B", "m1", "V2", "2.5.1", baseTime.Add(2*time.Hour)),
		patientCS("MSG-C", "m1", "V3", "R4", baseTime.Add(3*time.Hour)),
	}
	run := func(order []int) string {
		s := New()
		for _, i := range order {
			s.Apply(msgs[i], baseTime)
		}
		return s.Snapshot().Patients[0].FamilyName + "@" + s.Snapshot().Patients[0].Version
	}
	a := run([]int{0, 1, 2})
	b := run([]int{2, 1, 0})
	c := run([]int{1, 0, 2})
	if a != b || b != c || a != "V3@R4" {
		t.Fatalf("non-convergent: %s %s %s", a, b, c)
	}
}

func TestApply_CrossVersionSameTimestampRankWins(t *testing.T) {
	s := New()
	s.Apply(patientCS("HL7-MSG", "m1", "HL7NAME", "2.3.1", baseTime), baseTime)
	o := s.Apply(patientCS("FHIR-MSG", "m1", "FHIRNAME", "R4", baseTime), baseTime)
	if o.Status != domain.StatusUpdated {
		t.Fatalf("want updated, got %s (%s)", o.Status, o.Reason)
	}
	if got := s.Snapshot().Patients[0].FamilyName; got != "FHIRNAME" {
		t.Fatalf("higher rank at same timestamp must win, got %s", got)
	}
}

func TestApply_FieldMergeKeepsOptionalValues(t *testing.T) {
	s := New()
	cs1 := patientCS("MSG-1", "m1", "SMITH", "2.5.1", baseTime)
	cs1.Patient.GivenName = "JANE"
	s.Apply(cs1, baseTime)
	// newer message omits given name: existing optional value must survive
	cs2 := patientCS("MSG-2", "m1", "SMITHE", "2.5.1", baseTime.Add(time.Hour))
	cs2.Patient.GivenName = ""
	s.Apply(cs2, baseTime)
	p := s.Snapshot().Patients[0]
	if p.FamilyName != "SMITHE" || p.GivenName != "JANE" {
		t.Fatalf("field merge wrong: %+v", p)
	}
}

func TestApply_TypeConflictAcrossMessages(t *testing.T) {
	s := New()
	s.Apply(obsCS("MSG-1", "m1", "o1", 5.6, baseTime), baseTime)
	cs2 := &normalize.ChangeSet{
		MessageID: "MSG-2", Version: "R4", Rank: normalize.VersionRanks["R4"],
		Observations: []*domain.Observation{{
			ObservationID: "o1", MRN: "m1", TestCode: "C", Status: "final",
			ValueString: "high", SourceSystem: "S", Version: "R4",
			UpdatedAt: baseTime.Add(time.Hour),
		}},
	}
	o := s.Apply(cs2, baseTime)
	if o.Status != domain.StatusRejectedTypeConflict {
		t.Fatalf("want type conflict, got %s", o.Status)
	}
}

func TestApply_ConcurrentSamePatientNoDuplicatesNoLoss(t *testing.T) {
	s := New()
	const goroutines = 60
	// 20 distinct immutable messages, each redelivered 3x concurrently.
	// Identical bytes => identical digest => stable deduplication even under
	// a data race on delivery order.
	css := make([]*normalize.ChangeSet, goroutines)
	for g := 0; g < goroutines; g++ {
		k := g % 20
		css[g] = patientCS(fmt.Sprintf("MSG-%02d", k), "m1", fmt.Sprintf("NAME-%02d", k), "R4",
			baseTime.Add(time.Duration(k)*time.Hour))
	}
	var wg sync.WaitGroup
	statuses := make([]domain.Status, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			statuses[g] = s.Apply(css[g], baseTime).Status
		}(g)
	}
	wg.Wait()

	acc, dup, stale, conflict := 0, 0, 0, 0
	for _, st := range statuses {
		switch st {
		case domain.StatusAccepted, domain.StatusUpdated:
			acc++
		case domain.StatusDeduplicated:
			dup++
		case domain.StatusStale:
			stale++
		case domain.StatusConflict:
			conflict++
		default:
			t.Fatalf("unexpected status %s", st)
		}
	}
	snap := s.Snapshot()
	if len(snap.Patients) != 1 {
		t.Fatalf("want exactly 1 patient, got %d", len(snap.Patients))
	}
	// 60 outcomes must be one of the deterministic dispositions and cover
	// every redelivery: none may be lost, none may conflict.
	if acc+dup+stale != goroutines {
		t.Fatalf("outcomes don't cover all deliveries: %d+%d+%d != %d", acc, dup, stale, goroutines)
	}
	if acc < 1 {
		t.Fatal("at least one delivery must create the patient")
	}
	if conflict != 0 {
		t.Fatalf("identical redelivery must never conflict, got %d conflicts", conflict)
	}
	// the newest message's redelivery is recorded exactly once: its other two
	// copies must have been deduplicated (they can only be stale if a newer
	// message existed, but MSG-19 IS the newest)
	if dup < 2 {
		t.Fatalf("newest message redeliveries must deduplicate, got only %d", dup)
	}
	// message MSG-19 has the newest business timestamp and must win no matter
	// which goroutine ran last
	if snap.Patients[0].FamilyName != "NAME-19" {
		t.Fatalf("lost update: final family=%s, want NAME-19", snap.Patients[0].FamilyName)
	}
	t.Logf("applied=%d deduplicated=%d stale=%d", acc, dup, stale)
}

func TestApply_ConcurrentDistinctPatients(t *testing.T) {
	s := New()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mrn := fmt.Sprintf("mrn-%04d", i)
			o := s.Apply(patientCS(fmt.Sprintf("M-%04d", i), mrn, "X", "R4", baseTime), baseTime)
			if o.Status != domain.StatusAccepted {
				t.Errorf("patient %s status=%s", mrn, o.Status)
			}
		}(i)
	}
	wg.Wait()
	if got := len(s.Snapshot().Patients); got != n {
		t.Fatalf("want %d patients, got %d (lost inserts)", n, got)
	}
}

func TestContentDigest_StableAndSensitive(t *testing.T) {
	cs := patientCS("MSG-1", "m1", "SMITH", "R4", baseTime)
	d1 := ContentDigest(cs)
	d2 := ContentDigest(cs)
	if d1 != d2 || len(d1) != 64 {
		t.Fatalf("digest unstable or wrong length: %s", d1)
	}
	cs3 := patientCS("MSG-1", "m1", "JONES", "R4", baseTime)
	if ContentDigest(cs3) == d1 {
		t.Fatal("different content must hash differently")
	}
}
