package normalize

import (
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
)

func strptr(s string) *string { return &s }
func fptr(v float64) *float64 { return &v }

func TestNormalize_VocabularyMapping(t *testing.T) {
	doc := &ingest.RawDocument{
		Format: "hl7v2", Version: "2.3.1", MessageID: "M1",
		SourceSystem: "H", ReceivedAt: time.Now(),
		Patient: &ingest.RawPatient{
			MRN: strptr("m1"), FamilyName: strptr("SMITH"), GivenName: strptr("J"),
			BirthDate: strptr("1984-03-12"), Gender: strptr("F"),
		},
		Encounter: &ingest.RawEncounter{
			VisitID: strptr("v1"), MRN: strptr("m1"), Class: strptr("I"),
		},
		Observations: []*ingest.RawObservation{{
			ObservationID: strptr("o1"), MRN: strptr("m1"), VisitID: strptr("v1"),
			TestCode: strptr("GLU"), TestName: strptr("Glucose"),
			ValueType: "number", ValueNumber: fptr(5.6), Unit: strptr("mmol/L"),
			Status: strptr("F"),
		}},
	}
	cs, err := Normalize(doc)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Rank != VersionRanks["2.3.1"] {
		t.Fatalf("rank wrong: %d", cs.Rank)
	}
	if cs.Patient.Gender != "female" {
		t.Fatalf("gender mapping: %s", cs.Patient.Gender)
	}
	if cs.Encounter.Class != "IMP" {
		t.Fatalf("class mapping: %s", cs.Encounter.Class)
	}
	if cs.Observations[0].Status != "final" {
		t.Fatalf("status mapping: %s", cs.Observations[0].Status)
	}
}

func TestNormalize_MissingAndTypeRejections(t *testing.T) {
	base := func() *ingest.RawDocument {
		return &ingest.RawDocument{
			Format: "fhir", Version: "R4", MessageID: "M",
			ReceivedAt: time.Now(),
			Patient: &ingest.RawPatient{
				MRN: strptr("m"), FamilyName: strptr("X"),
				BirthDate: strptr("1970-01-01"), Gender: strptr("male"),
			},
		}
	}
	t.Run("missing patient mrn", func(t *testing.T) {
		d := base()
		d.Patient.MRN = nil
		_, err := Normalize(d)
		if rj, ok := err.(*domain.RejectError); !ok || rj.Status != domain.StatusRejectedMissingField {
			t.Fatalf("want missing-field reject, got %v", err)
		}
	})
	t.Run("observation missing value", func(t *testing.T) {
		d := base()
		d.Observations = []*ingest.RawObservation{{
			ObservationID: strptr("o"), MRN: strptr("m"), TestCode: strptr("C"), Status: strptr("final"),
		}}
		_, err := Normalize(d)
		if rj, ok := err.(*domain.RejectError); !ok || rj.Status != domain.StatusRejectedMissingField {
			t.Fatalf("want missing value reject, got %v", err)
		}
	})
	t.Run("both value kinds", func(t *testing.T) {
		d := base()
		d.Observations = []*ingest.RawObservation{{
			ObservationID: strptr("o"), MRN: strptr("m"), TestCode: strptr("C"), Status: strptr("final"),
			ValueType: "number", ValueNumber: fptr(1), ValueString: strptr("x"),
		}}
		_, err := Normalize(d)
		if rj, ok := err.(*domain.RejectError); !ok || rj.Status != domain.StatusRejectedTypeConflict {
			t.Fatalf("want type-conflict reject, got %v", err)
		}
	})
	t.Run("duplicate observation ids", func(t *testing.T) {
		d := base()
		mk := func() *ingest.RawObservation {
			return &ingest.RawObservation{
				ObservationID: strptr("dup"), MRN: strptr("m"), TestCode: strptr("C"),
				Status: strptr("final"), ValueType: "string", ValueString: strptr("x"),
			}
		}
		d.Observations = []*ingest.RawObservation{mk(), mk()}
		_, err := Normalize(d)
		if rj, ok := err.(*domain.RejectError); !ok || rj.Status != domain.StatusRejectedDuplicateID {
			t.Fatalf("want duplicate-id reject, got %v", err)
		}
	})
	t.Run("unknown version rank", func(t *testing.T) {
		d := base()
		d.Version = "R9"
		_, err := Normalize(d)
		if rj, ok := err.(*domain.RejectError); !ok || rj.Status != domain.StatusRejectedVersion {
			t.Fatalf("want version reject, got %v", err)
		}
	})
}
