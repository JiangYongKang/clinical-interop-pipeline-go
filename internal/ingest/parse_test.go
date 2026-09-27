package ingest

import (
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

func rejectStatus(t *testing.T, err error, want domain.Status) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected reject %s, got nil", want)
	}
	rj, ok := err.(*domain.RejectError)
	if !ok {
		t.Fatalf("expected *RejectError, got %T", err)
	}
	if rj.Status != want {
		t.Fatalf("status=%s want %s (%s)", rj.Status, want, rj.Reason)
	}
}

func TestParseHL7_ADT_A01(t *testing.T) {
	msg := "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|20260901101500||ADT^A01^ADT_A01|MSG-1|P|2.5.1\r" +
		"PID|1||MRN-1||SMITH^JANE||19840312|F\r" +
		"PV1|1|I|WARD-7||||||||||||||||VISIT-9"
	doc, err := ParseHL7([]byte(msg), time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Version != "2.5.1" || doc.MessageID != "MSG-1" || doc.SourceSystem != "HOSP-A" {
		t.Fatalf("MSH mapping wrong: %+v", doc)
	}
	if *doc.Patient.MRN != "MRN-1" || *doc.Patient.FamilyName != "SMITH" || *doc.Patient.GivenName != "JANE" {
		t.Fatalf("PID mapping wrong: %+v", doc.Patient)
	}
	if *doc.Encounter.VisitID != "VISIT-9" || *doc.Encounter.Class != "I" {
		t.Fatalf("PV1 mapping wrong: %+v", doc.Encounter)
	}
}
func TestParseHL7_ORU_Observations(t *testing.T) {
	msg := "MSH|^~\\&|LAB|CITY|HOSP-A|CITY|20260902093000||ORU^R01|MSG-2|P|2.3.1\r" +
		"PID|1||MRN-1||SMITH^JANE||19840312|F\r" +
		"OBR|1||ORD-7\r" +
		"OBX|1|NM|GLU^Glucose||5.6|mmol/L|||||F|||20260902091500\r" +
		"OBX|2|ST|NOTE^Lab note||fasting|||||F|||20260902091500"
	doc, err := ParseHL7([]byte(msg), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Observations) != 2 {
		t.Fatalf("want 2 observations, got %d", len(doc.Observations))
	}
	num := doc.Observations[0]
	if num.ValueType != "number" || num.ValueNumber == nil || *num.ValueNumber != 5.6 {
		t.Fatalf("numeric value mapping wrong: %+v", num)
	}
	if *num.ObservationID != "ORD-7-1-GLU" {
		t.Fatalf("synthesized id wrong: %v", *num.ObservationID)
	}
	if *num.Status != "F" || *num.Unit != "mmol/L" {
		t.Fatalf("status/unit wrong: %+v", num)
	}
	str := doc.Observations[1]
	if str.ValueType != "string" || str.ValueString == nil || *str.ValueString != "fasting" {
		t.Fatalf("string value mapping wrong: %+v", str)
	}
	// repeated delivery yields identical synthesized ids
	doc2, _ := ParseHL7([]byte(msg), time.Now())
	if *doc2.Observations[0].ObservationID != *num.ObservationID {
		t.Fatal("observation ids not stable across parses")
	}
}

func TestParseHL7_Rejections(t *testing.T) {
	base := func(version string) []byte {
		return []byte("MSH|^~\\&|H|C|L|C|20260101000000||ADT^A01|MSG-X|P|" + version +
			"\rPID|1||MRN-9||DOE^JOHN||19700101|M\rPV1|1|I|W||||||||||||||||V9")
	}
	if _, err := ParseHL7(base("2.2"), time.Now()); err == nil {
		t.Fatal("2.2 must be rejected")
	} else {
		rejectStatus(t, err, domain.StatusRejectedVersion)
	}
	if _, err := ParseHL7([]byte("PID|1||M||A"), time.Now()); err == nil {
		t.Fatal("missing MSH must be rejected")
	} else {
		rejectStatus(t, err, domain.StatusRejectedMalformed)
	}
	if _, err := ParseHL7(base("3.0"), time.Now()); err == nil {
		t.Fatal("3.0 must be rejected")
	} else {
		rejectStatus(t, err, domain.StatusRejectedVersion)
	}
	// NM segment carrying a non-numeric value is a type conflict, not silence
	bad := "MSH|^~\\&|L|C|H|C|20260101000000||ORU^R01|MSG-Y|P|2.5.1\r" +
		"PID|1||MRN-9||DOE^JOHN||19700101|M\r" +
		"OBR|1||ORD-9\rOBX|1|NM|GLU||not-a-number|mmol/L|||||F"
	_, err := ParseHL7([]byte(bad), time.Now())
	rejectStatus(t, err, domain.StatusRejectedTypeConflict)
}

func TestParseFHIR_R4_Bundle(t *testing.T) {
	payload := []byte(`{
      "resourceType":"Bundle","fhirVersion":"R4","source":"HOSP-B",
      "identifier":{"value":"MSG-3"},
      "meta":{"lastUpdated":"2026-09-03T08:00:00Z"},
      "entry":[
        {"resource":{"resourceType":"Patient","id":"p1","identifier":[{"value":"MRN-7"}],
          "name":[{"family":"GARCIA","given":["CARL"]}],"birthDate":"1979-11-30","gender":"male"}},
        {"resource":{"resourceType":"Encounter","id":"e1","identifier":[{"value":"VISIT-8"}],
          "class":{"code":"EMER"},"subject":{"reference":"Patient/p1"},
          "period":{"start":"2026-09-03T07:00:00Z"}}},
        {"resource":{"resourceType":"Observation","id":"o1","identifier":[{"value":"OBS-9"}],
          "status":"final","code":{"coding":[{"code":"CREA","display":"Creatinine"}]},
          "subject":{"reference":"Patient/p1"},"encounter":{"reference":"Encounter/e1"},
          "effectiveDateTime":"2026-09-03T07:20:00Z",
          "valueQuantity":{"value":88.4,"unit":"umol/L"}}}
      ]}`)
	doc, err := ParseFHIR(payload, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if doc.MessageID != "MSG-3" || *doc.Patient.MRN != "MRN-7" {
		t.Fatalf("patient mapping wrong: %+v", doc.Patient)
	}
	if *doc.Encounter.VisitID != "VISIT-8" || *doc.Encounter.MRN != "MRN-7" {
		t.Fatalf("encounter reference resolution wrong: %+v", doc.Encounter)
	}
	o := doc.Observations[0]
	if o.ValueType != "number" || o.ValueNumber == nil || *o.ValueNumber != 88.4 {
		t.Fatalf("observation value wrong: %+v", o)
	}
	if *o.MRN != "MRN-7" || *o.VisitID != "VISIT-8" {
		t.Fatalf("observation refs wrong: %+v", o)
	}
}

func TestParseFHIR_Rejections(t *testing.T) {
	cases := map[string]struct {
		payload string
		status  domain.Status
	}{
		"not json":          {`{bad`, domain.StatusRejectedMalformed},
		"not bundle":        {`{"resourceType":"Patient"}`, domain.StatusRejectedMalformed},
		"missing version":   {`{"resourceType":"Bundle","identifier":{"value":"x"}}`, domain.StatusRejectedVersion},
		"unsupported STU3":  {`{"resourceType":"Bundle","fhirVersion":"STU3","identifier":{"value":"x"}}`, domain.StatusRejectedVersion},
		"missing messageid": {`{"resourceType":"Bundle","fhirVersion":"R4"}`, domain.StatusRejectedMalformed},
		"bad value[x]": {`{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"m"},
		   "entry":[{"resource":{"resourceType":"Observation","id":"o","identifier":[{"value":"O"}],
		   "status":"final","code":{"coding":[{"code":"C"}]},"valueBoolean":true}}]}`,
			domain.StatusRejectedTypeConflict},
		"duplicate obs ids": {`{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"m"},
		   "entry":[{"resource":{"resourceType":"Observation","id":"a","identifier":[{"value":"DUP"}],
		   "status":"final","code":{"coding":[{"code":"C"}]},"valueString":"x"}},
		   {"resource":{"resourceType":"Observation","id":"b","identifier":[{"value":"DUP"}],
		   "status":"final","code":{"coding":[{"code":"C2"}]},"valueString":"y"}}]}`,
			domain.StatusRejectedDuplicateID},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseFHIR([]byte(tc.payload), time.Now())
			rejectStatus(t, err, tc.status)
		})
	}
}

func TestParse_DispatchUnknownFormat(t *testing.T) {
	if _, err := Parse("csv", []byte("a,b"), time.Now()); err == nil {
		t.Fatal("unknown format must reject")
	} else {
		rejectStatus(t, err, domain.StatusRejectedMalformed)
	}
}
