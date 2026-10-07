package store

import (
	"strconv"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

func TestSnapshotAt_BusinessTimeline(t *testing.T) {
	s := New()
	t1 := baseTime
	t2 := baseTime.Add(24 * time.Hour)
	t3 := baseTime.Add(48 * time.Hour)

	// recorded times follow delivery, later than each business time.
	s.Apply(patientCS("M1", "m1", "V1", "2.3.1", t1), t1.Add(time.Hour))
	s.Apply(patientCS("M2", "m1", "V2", "2.5.1", t2), t2.Add(time.Hour))
	s.Apply(patientCS("M3", "m1", "V3", "R4", t3), t3.Add(time.Hour))

	cases := []struct {
		at   time.Time
		want string
	}{
		{t1.Add(-time.Hour), ""}, // nothing yet
		{t1.Add(2 * time.Hour), "V1"},
		{t2.Add(2 * time.Hour), "V2"},
		{t3.Add(2 * time.Hour), "V3"},
		{t3.Add(30 * 24 * time.Hour), "V3"},
	}
	for _, c := range cases {
		snap := s.SnapshotAt(c.at, HistoryOptions{MRN: "m1"})
		got := ""
		if len(snap.Patients) == 1 {
			got = snap.Patients[0].FamilyName
		} else if len(snap.Patients) > 1 {
			t.Fatalf("at %v: expected <=1 patient, got %d", c.at, len(snap.Patients))
		}
		if got != c.want {
			t.Errorf("as-of %s: family=%q want %q", c.at.Format("15:04"), got, c.want)
		}
	}
	t.Log("business=as-of-timeline case=reconstruct-patient-at-each-business-point result=pass")
}

func TestSnapshotAt_Repeatable(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		s.Apply(patientCS("M"+strconv.Itoa(i), "m1", "V"+strconv.Itoa(i), "R4",
			baseTime.Add(time.Duration(i)*time.Hour)), baseTime.Add(time.Duration(i+1)*time.Hour))
	}
	at := baseTime.Add(10 * time.Hour)
	a := s.SnapshotAt(at, HistoryOptions{})
	b := s.SnapshotAt(at, HistoryOptions{})
	if len(a.Patients) != len(b.Patients) {
		t.Fatal("repeat read length differs")
	}
	for i := range a.Patients {
		if a.Patients[i].FamilyName != b.Patients[i].FamilyName {
			t.Fatal("repeat read content differs")
		}
	}
}

func TestSnapshotAt_LateRecordedCorrectionCannotRewritePast(t *testing.T) {
	s := New()
	// V1 pertains to the 1st, recorded on the 2nd.
	s.Apply(patientCS("M1", "m1", "V1", "R4",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)),
		time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	// V2 corrects a past day (the 5th) but is only RECORDED on the 15th,
	// i.e. after an observer queries on the 10th.
	s.Apply(patientCS("M2", "m1", "V2", "R4",
		time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)),
		time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC))

	view10 := s.SnapshotAt(time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), HistoryOptions{MRN: "m1"})
	if len(view10.Patients) != 1 || view10.Patients[0].FamilyName != "V1" {
		t.Fatalf("past view must stay V1, got %+v", view10.Patients)
	}
	view20 := s.SnapshotAt(time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC), HistoryOptions{MRN: "m1"})
	if len(view20.Patients) != 1 || view20.Patients[0].FamilyName != "V2" {
		t.Fatalf("view after recording must show V2, got %+v", view20.Patients)
	}
	t.Log("business=as-of-timeline case=late-recorded-correction-cannot-rewrite-past result=pass")
}

func TestSnapshotAt_CrossVersionSameTimestampRankWins(t *testing.T) {
	s := New()
	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	rec := at.Add(time.Hour)
	s.Apply(patientCS("HL7", "m1", "HL7NAME", "2.3.1", at), rec)
	s.Apply(patientCS("FHIR", "m1", "FHIRNAME", "R4", at), rec.Add(time.Minute))
	view := s.SnapshotAt(rec.Add(time.Hour), HistoryOptions{MRN: "m1"})
	if len(view.Patients) != 1 || view.Patients[0].FamilyName != "FHIRNAME" {
		t.Fatalf("higher version rank must win at same business time, got %+v", view.Patients)
	}
	t.Log("business=cross-version-history case=HL7-vs-FHIR-same-time-rank-unified result=pass")
}

func obsStatusCS(msg, mrn, oid, status string, updated, recorded time.Time) *normalize.ChangeSet {
	return &normalize.ChangeSet{
		MessageID: msg, Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: baseTime,
		Observations: []*domain.Observation{{
			ObservationID: oid, MRN: mrn, TestCode: "C", Status: status,
			ValueString: "x", Version: "R4", UpdatedAt: updated,
		}},
	}
}

func TestTimeline_CorrectionsAndWithdrawalsRemainVisible(t *testing.T) {
	s := New()
	t1 := baseTime
	t2 := baseTime.Add(time.Hour)
	t3 := baseTime.Add(2 * time.Hour)
	s.Apply(obsStatusCS("O1", "m1", "o1", "final", t1, t1), t1)
	s.Apply(obsStatusCS("O2", "m1", "o1", "amended", t2, t2), t2)
	s.Apply(obsStatusCS("O3", "m1", "o1", "cancelled", t3, t3), t3)

	revs, err := s.Timeline("observation", "o1")
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 3 {
		t.Fatalf("want 3 timeline revisions, got %d", len(revs))
	}
	want := []string{"final", "amended", "cancelled"}
	for i, w := range want {
		if revs[i].Observation.Status != w {
			t.Fatalf("revision %d status=%s want %s", i, revs[i].Observation.Status, w)
		}
	}
	// Historical view at t2 still shows the amended result; current view is
	// cancelled — withdrawal is visible on the timeline, not erased.
	mid := s.SnapshotAt(t2.Add(time.Minute), HistoryOptions{MRN: "m1"})
	if len(mid.Observations) != 1 || mid.Observations[0].Status != "amended" {
		t.Fatalf("mid view=%+v", mid.Observations)
	}
	now := s.Snapshot()
	if now.Observations[0].Status != "cancelled" {
		t.Fatalf("current view status=%s want cancelled", now.Observations[0].Status)
	}
	t.Log("business=as-of-timeline case=correction-then-withdrawal-visible-not-erased result=pass")
}

func TestSnapshotAt_MRNFilter(t *testing.T) {
	s := New()
	s.Apply(patientCS("A", "m1", "A", "R4", baseTime), baseTime)
	s.Apply(patientCS("B", "m2", "B", "R4", baseTime), baseTime)
	view := s.SnapshotAt(baseTime.Add(time.Hour), HistoryOptions{MRN: "m2"})
	if len(view.Patients) != 1 || view.Patients[0].MRN != "m2" {
		t.Fatalf("MRN filter wrong: %+v", view.Patients)
	}
}
