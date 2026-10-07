package pipeline

import (
	"testing"
	"time"
)

// TestPipeline_AsOfAcrossVersionsAndOutOfOrder proves the history rules on the
// full wire path: HL7 2.3.1/2.5.1 and FHIR R4 share one visibility/ordering
// rule, and late arrivals cannot rewrite a pinned historical view.
func TestPipeline_AsOfAcrossVersionsAndOutOfOrder(t *testing.T) {
	pl, _, st, _ := newTestPipeline()

	// t1: HL7 2.3.1 preliminary patient conclusion
	t1 := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	hl7231 := "MSH|^~\\&|H|C|L|C|20260901080000||ADT^A01|HL7-231|P|2.3.1\r" +
		"PID|1||MRN-H||HISTSECRET^J||19840312|F\rPV1|1|I|W||||||||||||||||V-H1"
	ingestAt(pl, t1, "hl7v2", hl7231)

	// t2 arrives ON TIME: FHIR R4 correction with the SAME business timestamp
	// as t1's conclusion -> higher rank (R4) wins at that point.
	t2 := time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)
	fhirSame := `{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"FHIR-1"},
	  "entry":[{"resource":{"resourceType":"Patient","identifier":[{"value":"MRN-H"}],
	  "name":[{"family":"FHIRNAME","given":["J"]}],"birthDate":"1984-03-12","gender":"female",
	  "meta":{"lastUpdated":"2026-09-01T08:00:00Z"}}}]}`
	ingestAt(pl, t2, "fhir", fhirSame)

	// Pin the query point AFTER t2 but BEFORE the late message below.
	pin := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	view1 := st.AsOf(pin)
	if len(view1.Patients) != 1 || view1.Patients[0].FamilyName != "FHIRNAME" {
		t.Fatalf("pinned view=%+v want FHIRNAME (R4 rank wins)", view1.Patients)
	}

	// t3: a genuinely newer HL7 2.5.1 conclusion arrives late.
	t3 := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	hl7251 := "MSH|^~\\&|H|C|L|C|20260903080000||ADT^A01|HL7-251|P|2.5.1\r" +
		"PID|1||MRN-H||LATESTNAME^J||19840312|F\rPV1|1|I|W||||||||||||||||V-H1"
	ingestAt(pl, t3, "hl7v2", hl7251)

	// The pinned view must be unchanged even though newer data now exists.
	view2 := st.AsOf(pin)
	if view2.Patients[0].FamilyName != "FHIRNAME" {
		t.Fatalf("late arrival rewrote pinned history: %s", view2.Patients[0].FamilyName)
	}
	// Live view and later points see the 2.5.1 newer-business-time conclusion.
	latest := st.PatientAt("MRN-H", t3)
	if latest == nil || latest.FamilyName != "LATESTNAME" {
		t.Fatalf("later point=%v want LATESTNAME", latest)
	}

	// The timeline keeps all three conclusions, with rank and times.
	revs := st.PatientHistory("MRN-H")
	if len(revs) != 3 {
		t.Fatalf("want 3 revisions, got %d", len(revs))
	}
	// revision writers in arrival order
	wantWriters := []string{"HL7-231", "FHIR-1", "HL7-251"}
	for i, w := range wantWriters {
		if revs[i].WriterMsgID != w {
			t.Fatalf("rev %d writer=%s want %s", i, revs[i].WriterMsgID, w)
		}
	}
}

func ingestAt(pl *Pipeline, at time.Time, format, body string) {
	prev := pl.now
	pl.now = func() time.Time { return at }
	pl.Ingest(format, "ref", []byte(body))
	pl.now = prev
}
