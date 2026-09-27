package ingest

import (
	"errors"
	"strings"
	"testing"
)

func hl7Message(version, msgID, seq, obxValue string) string {
	var b strings.Builder
	b.WriteString("MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|" + msgID + "|P|" + version + "\r")
	b.WriteString("ZVR|" + seq + "\r")
	b.WriteString("PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r")
	b.WriteString("PV1|1|I|WARD1||||DOC1" + strings.Repeat("|", 12) + "VN1001" + strings.Repeat("|", 25) + "20260101100000\r")
	if obxValue != "" {
		b.WriteString("OBR|1|ORD1|FIL1|GLU^Glucose\r")
		b.WriteString("OBX|1|NM|GLU^Glucose||" + obxValue + "|mmol/L|3.9-6.1|H|||F|||20260102090000\r")
	}
	return b.String()
}

func fhirBundle(id, version, seq, mrn string) string {
	return `{"id":"` + id + `","fhirVersion":"` + version + `","x-source":"PORTAL","x-sequence":` + seq + `,
	  "entry":[{"resource":{"resourceType":"Patient","id":"p1","identifier":[{"value":"` + mrn + `"}],
	    "name":[{"family":"Zhang","given":["San"]}],"gender":"male","birthDate":"1980-01-01"}}]}`
}

func mustReason(t *testing.T, err error, want Reason) {
	t.Helper()
	var ie *Error
	if !errors.As(err, &ie) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if ie.Reason != want {
		t.Fatalf("reason = %s, want %s (err=%v)", ie.Reason, want, err)
	}
}

func TestParseHL7OK(t *testing.T) {
	u, err := Parse("hl7v2", []byte(hl7Message("2.3", "M1", "10", "6.2")))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.MessageID != "M1" || u.Sequence != 10 || u.SourceVersion != "2.3" {
		t.Fatalf("unexpected envelope: %+v", u)
	}
	if len(u.Patients) != 1 || len(u.Encounters) != 1 || len(u.Observations) != 1 {
		t.Fatalf("entities = %d/%d/%d", len(u.Patients), len(u.Encounters), len(u.Observations))
	}
	p := u.Patients[0]
	if p.ID != "PAT:MRN1001" || p.Gender != "male" || p.BirthDate != "1980-01-01" {
		t.Fatalf("patient mapping wrong: %+v", p)
	}
	if u.Observations[0].PatientID != p.ID || u.Observations[0].EncounterID != u.Encounters[0].ID {
		t.Fatalf("observation linkage wrong: %+v", u.Observations[0])
	}
}

func TestParseHL7v25OK(t *testing.T) {
	if _, err := Parse("hl7v2", []byte(hl7Message("2.5", "M2", "1", "5.0"))); err != nil {
		t.Fatalf("v2.5 should be supported: %v", err)
	}
}

func TestParseHL7UnsupportedVersion(t *testing.T) {
	_, err := Parse("hl7v2", []byte(hl7Message("2.1", "M3", "1", "")))
	mustReason(t, err, ReasonUnsupportedVersion)
}

func TestParseHL7MissingSequence(t *testing.T) {
	raw := "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|M4|P|2.3\r" +
		"PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r"
	_, err := Parse("hl7v2", []byte(raw))
	mustReason(t, err, ReasonMissingField)
}

func TestParseHL7MissingMRN(t *testing.T) {
	raw := "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|M5|P|2.3\r" +
		"ZVR|1\r" +
		"PID|1|||Zhang^San||19800101|M\r"
	_, err := Parse("hl7v2", []byte(raw))
	mustReason(t, err, ReasonMissingField)
}

func TestParseHL7TypeConflictNumeric(t *testing.T) {
	_, err := Parse("hl7v2", []byte(hl7Message("2.5", "M6", "1", "not-a-number")))
	mustReason(t, err, ReasonTypeConflict)
}

func TestParseHL7BadBirthDate(t *testing.T) {
	raw := "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|M7|P|2.3\r" +
		"ZVR|1\r" +
		"PID|1||MRN1001^^^HIS||Zhang^San||19XX|M\r"
	_, err := Parse("hl7v2", []byte(raw))
	mustReason(t, err, ReasonTypeConflict)
}

func TestParseFHIROK(t *testing.T) {
	u, err := Parse("fhir-json", []byte(fhirBundle("B1", "R4", "7", "MRN1001")))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.MessageID != "B1" || u.Sequence != 7 {
		t.Fatalf("unexpected envelope: %+v", u)
	}
	if u.Patients[0].ID != "PAT:MRN1001" {
		t.Fatalf("cross-source dedup key mismatch: %s", u.Patients[0].ID)
	}
}

func TestParseFHIRUnsupportedVersion(t *testing.T) {
	_, err := Parse("fhir-json", []byte(fhirBundle("B2", "STU3", "1", "MRN1001")))
	mustReason(t, err, ReasonUnsupportedVersion)
}

func TestParseFHIRMissingMRN(t *testing.T) {
	raw := `{"id":"B3","fhirVersion":"R4","x-source":"PORTAL","x-sequence":1,
	  "entry":[{"resource":{"resourceType":"Patient","id":"p1","gender":"male","birthDate":"1980-01-01"}}]}`
	_, err := Parse("fhir-json", []byte(raw))
	mustReason(t, err, ReasonMissingField)
}

func TestParseFHIRTypeConflictQuantity(t *testing.T) {
	raw := `{"id":"B4","fhirVersion":"R4","x-source":"PORTAL","x-sequence":1,
	  "entry":[
	    {"resource":{"resourceType":"Patient","id":"p1","identifier":[{"value":"MRN1001"}],"gender":"male","birthDate":"1980-01-01"}},
	    {"resource":{"resourceType":"Observation","id":"o1","status":"final",
	      "code":{"coding":[{"code":"GLU"}]},"subject":{"reference":"Patient/p1"},
	      "effectiveDateTime":"2026-01-02T09:00:00Z","valueQuantity":{"value":"high","unit":"mmol/L"}}}
	  ]}`
	_, err := Parse("fhir-json", []byte(raw))
	mustReason(t, err, ReasonTypeConflict)
}

func TestParseFHIRDanglingReference(t *testing.T) {
	raw := `{"id":"B5","fhirVersion":"R4","x-source":"PORTAL","x-sequence":1,
	  "entry":[
	    {"resource":{"resourceType":"Patient","id":"p1","identifier":[{"value":"MRN1001"}],"gender":"male","birthDate":"1980-01-01"}},
	    {"resource":{"resourceType":"Observation","id":"o1","status":"final",
	      "code":{"coding":[{"code":"GLU"}]},"subject":{"reference":"Patient/ghost"},
	      "effectiveDateTime":"2026-01-02T09:00:00Z","valueString":"ok"}}
	  ]}`
	_, err := Parse("fhir-json", []byte(raw))
	mustReason(t, err, ReasonMissingField)
}

func TestParseUnsupportedContentType(t *testing.T) {
	_, err := Parse("dicom", []byte("{}"))
	mustReason(t, err, ReasonUnsupportedVersion)
}

// 同一输入重复解析必须得到相同的稳定主键（可重复验证）。
func TestStableIDsDeterministic(t *testing.T) {
	raw := hl7Message("2.3", "M8", "3", "6.2")
	u1, err1 := Parse("hl7v2", []byte(raw))
	u2, err2 := Parse("hl7v2", []byte(raw))
	if err1 != nil || err2 != nil {
		t.Fatalf("parse: %v %v", err1, err2)
	}
	if u1.Patients[0].ID != u2.Patients[0].ID || u1.Encounters[0].ID != u2.Encounters[0].ID || u1.Observations[0].ID != u2.Observations[0].ID {
		t.Fatal("stable ids differ between runs")
	}
}
