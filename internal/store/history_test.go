package store

import (
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

func TestHistory_AsOfLateArrivingOldMessageCannotRewriteHistory(t *testing.T) {
	s := New()
	// Day 1: patient name V1 arrives on time (business 10:00, arrival 10:00).
	d1 := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	s.Apply(patientCS("M1", "m1", "V1", "R4", d1), d1)

	// Query at day 2: V1 is the settled conclusion.
	d2 := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	view := s.AsOf(d2)
	if view.Patients[0].FamilyName != "V1" {
		t.Fatalf("day2 view=%s want V1", view.Patients[0].FamilyName)
	}

	// Day 3: an OLD correction (business day 1 09:00) arrives LATE. It loses to
	// the stored newer conclusion anyway (stale), and even if it had won it
	// would arrive after d2 and never alter the day-2 view.
	d3 := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	o := s.Apply(patientCS("M-OLD", "m1", "ANCIENT", "2.5.1", d1.Add(-time.Hour)), d3)
	if o.Status != domain.StatusStale {
		t.Fatalf("late old message status=%s want stale", o.Status)
	}
	again := s.AsOf(d2)
	if again.Patients[0].FamilyName != "V1" {
		t.Fatalf("late arrival rewrote settled history: %s", again.Patients[0].FamilyName)
	}

	// Day 4: a genuine newer correction lands; day-2 view stays V1, day-4 is V2.
	d4 := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	s.Apply(patientCS("M2", "m1", "V2", "R4", d4), d4)
	if got := s.AsOf(d2).Patients[0].FamilyName; got != "V1" {
		t.Fatalf("day2 view drifted to %s", got)
	}
	if got := s.AsOf(d4).Patients[0].FamilyName; got != "V2" {
		t.Fatalf("day4 view=%s want V2", got)
	}
}

func TestHistory_FutureBusinessTimeInvisibleUntilEffective(t *testing.T) {
	s := New()
	arrival := time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)
	future := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	// a message already delivered but effective only in the future
	cs := patientCS("FUT", "m1", "FUTURE", "R4", future)
	o := s.Apply(cs, arrival)
	if o.Status != domain.StatusAccepted {
		t.Fatalf("future-dated ingest=%s", o.Status)
	}
	if p := s.PatientAt("m1", time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)); p != nil {
		t.Fatal("future-dated conclusion must be invisible before its business time")
	}
	if p := s.PatientAt("m1", future); p == nil || p.FamilyName != "FUTURE" {
		t.Fatal("future-dated conclusion must become visible at/after its business time")
	}
}

func TestHistory_SamePointRepeatedlyIdentical(t *testing.T) {
	s := New()
	t1 := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	at := t1.Add(36 * time.Hour) // pinned historical point
	s.Apply(patientCS("H1", "m1", "A", "2.3.1", t1), t1)
	first := s.AsOf(at)
	// new conclusions keep arriving strictly AFTER the pinned point, as in
	// production where the serving clock is monotonic; none may alter it
	after1 := at.Add(6 * time.Hour)
	after2 := at.Add(30 * time.Hour)
	s.Apply(patientCS("H2", "m1", "B", "2.5.1", after1), after1)
	s.Apply(patientCS("H3", "m1", "C", "R4", after2), after2)
	second := s.AsOf(at)
	third := s.AsOf(at)
	if !patientsEqual(first.Patients, second.Patients) || !patientsEqual(second.Patients, third.Patients) {
		t.Fatal("repeated as-of queries at the same point must be identical")
	}
	// sanity: the live view does advance, proving the pin is what holds history
	if got := s.AsOf(after2).Patients[0].FamilyName; got != "C" {
		t.Fatalf("live view=%s want C", got)
	}
}

func TestHistory_CorrectionsAndWithdrawalsRemainOnTimeline(t *testing.T) {
	s := New()
	t1 := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	mkObs := func(msg, status string, at time.Time) *normalize.ChangeSet {
		num := 5.6
		return &normalize.ChangeSet{
			MessageID: msg, Version: "R4", Rank: normalize.VersionRanks["R4"],
			ReceivedAt: at,
			Observations: []*domain.Observation{{
				ObservationID: "obs-9", MRN: "m1", TestCode: "GLU", Status: status,
				ValueNumber: &num, Unit: "mmol/L", Version: "R4", UpdatedAt: at,
			}},
		}
	}
	s.Apply(mkObs("O1", "preliminary", t1), t1)
	s.Apply(mkObs("O2", "final", t2), t2)
	s.Apply(mkObs("O3", "entered-in-error", t3), t3)

	revs := s.ObservationHistory("obs-9")
	if len(revs) != 3 {
		t.Fatalf("want 3 timeline revisions, got %d", len(revs))
	}
	wantStatus := []string{"preliminary", "final", "entered-in-error"}
	for i, want := range wantStatus {
		if revs[i].Observation.Status != want {
			t.Fatalf("rev %d status=%s want %s", i, revs[i].Observation.Status, want)
		}
		if !revs[i].ReceivedAt.Equal(revs[i].BusinessAt) {
			t.Fatal("revision time axes not retained")
		}
	}
	// at t1 only preliminary exists; withdrawal never erases earlier history
	v1 := s.AsOf(t1)
	if len(v1.Observations) != 1 || v1.Observations[0].Status != "preliminary" {
		t.Fatalf("t1 view wrong: %+v", v1.Observations)
	}
	v3 := s.AsOf(t3)
	if v3.Observations[0].Status != "entered-in-error" {
		t.Fatalf("t3 view=%s want entered-in-error (still present, not deleted)",
			v3.Observations[0].Status)
	}
}

func TestHistory_CrossVersionSamePointRuleIsUniform(t *testing.T) {
	s := New()
	// HL7 2.3.1 and FHIR R4 conclusions with the SAME business timestamp: the
	// version rank (R4 > 2.3.1) decides at that point for both formats alike.
	at := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	s.Apply(patientCS("HL7", "m1", "HL7NAME", "2.3.1", at), at)
	s.Apply(patientCS("FHIR", "m1", "FHIRNAME", "R4", at), at)
	p := s.PatientAt("m1", at)
	if p.FamilyName != "FHIRNAME" {
		t.Fatalf("as-of rank tiebreak wrong: %s", p.FamilyName)
	}
	revs := s.PatientHistory("m1")
	if len(revs) != 2 {
		t.Fatalf("want both format revisions retained, got %d", len(revs))
	}
	// 2.5.1 later correction at a later business time wins at later points
	later := at.Add(time.Hour)
	s.Apply(patientCS("HL7-2", "m1", "HL7251NAME", "2.5.1", later), later)
	if got := s.PatientAt("m1", later).FamilyName; got != "HL7251NAME" {
		t.Fatalf("newer business time must win across versions: %s", got)
	}
	if got := s.PatientAt("m1", at).FamilyName; got != "FHIRNAME" {
		t.Fatalf("earlier point must remain FHIRNAME, got %s", got)
	}
}

func patientsEqual(a, b []*domain.Patient) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if *a[i] != *b[i] {
			return false
		}
	}
	return true
}
