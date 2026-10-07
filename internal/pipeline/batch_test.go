package pipeline

import (
	"strings"
	"testing"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

const hl7A = "MSH|^~\\&|H|C|L|C|20260901101500||ADT^A01|PB-1001|P|2.5.1\r" +
	"PID|1||BMRN-1||BATCHSUR^JANE||19840312|F\r" +
	"PV1|1|I|W||||||||||||||||BV-1"

const hl7B = "MSH|^~\\&|H|C|L|C|20260902101500||ORU^R01|PB-1002|P|2.3.1\r" +
	"PID|1||BMRN-1||BATCHSUR^JANE||19840312|F\r" +
	"OBR|1||ORD-9\rOBX|1|NM|GLU^Glucose||4.2|mmol/L|||||F|||20260902091500"

const fhirC = `{
 "resourceType":"Bundle","fhirVersion":"R4","source":"HOSP-B",
 "identifier":{"value":"PB-2001"},"meta":{"lastUpdated":"2026-09-03T08:00:00Z"},
 "entry":[{"resource":{
   "resourceType":"Patient","id":"p","identifier":[{"value":"BMRN-2"}],
   "name":[{"family":"BATCHGARCIA","given":["CARL"]}],
   "birthDate":"1979-01-01","gender":"male"}}]}`

func TestIngestBatch_AtomicCommit(t *testing.T) {
	pl, _, st, al := newTestPipeline()
	res, rj := pl.IngestBatch("WB-1", []BatchMessage{
		{Ref: "a", Format: "hl7v2", Payload: []byte(hl7A)},
		{Ref: "b", Format: "hl7v2", Payload: []byte(hl7B)},
		{Ref: "c", Format: "fhir", Payload: []byte(fhirC)},
	})
	if rj != nil {
		t.Fatal(rj)
	}
	if res.Status != domain.StatusAccepted && res.Status != domain.StatusUpdated {
		t.Fatalf("status=%s", res.Status)
	}
	if len(res.Items) != 3 {
		t.Fatalf("want 3 item results, got %d", len(res.Items))
	}
	snap := st.Snapshot()
	if len(snap.Patients) != 2 || len(snap.Encounters) != 1 || len(snap.Observations) != 1 {
		t.Fatalf("whole batch should land: %+v", snap)
	}
	// exactly one allow audit entry for the batch
	var allows, denies int
	for _, e := range al.Entries() {
		if e.Action != "batch" {
			continue
		}
		if e.Decision == "allow" {
			allows++
		} else {
			denies++
		}
	}
	if allows != 1 || denies != 0 {
		t.Fatalf("want 1 allow / 0 deny batch audit, got %d/%d", allows, denies)
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
	t.Log("business=atomic-batch case=hl7+hl7+fhir-commit-and-audit result=pass")
}

func TestIngestBatch_OneBadMessageRejectsEverything(t *testing.T) {
	badVersion := "MSH|^~\\&|H|C|L|C|20200101000000||ADT^A01|PB-9001|P|2.2\r" +
		"PID|1||BMRN-9||BATCHSECRET^JOHN||19700101|M\rPV1|1|I|W||||||||||||||||BV-9"
	cases := map[string][]BatchMessage{
		"bad version first": {
			{Ref: "bad", Format: "hl7v2", Payload: []byte(badVersion)},
			{Ref: "good", Format: "hl7v2", Payload: []byte(hl7A)},
		},
		"bad version last": {
			{Ref: "good", Format: "hl7v2", Payload: []byte(hl7A)},
			{Ref: "bad", Format: "hl7v2", Payload: []byte(badVersion)},
		},
		"missing field": {
			{Ref: "good", Format: "hl7v2", Payload: []byte(hl7A)},
			{Ref: "bad", Format: "fhir", Payload: []byte(
				`{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"PB-9"},
				  "entry":[{"resource":{"resourceType":"Patient","gender":"female"}}]}`)},
		},
		"bad json": {
			{Ref: "bad", Format: "fhir", Payload: []byte("{not-json-BATCHSECRET")},
			{Ref: "good", Format: "hl7v2", Payload: []byte(hl7B)},
		},
	}
	for name, msgs := range cases {
		t.Run(name, func(t *testing.T) {
			pl, logBuf, st, al := newTestPipeline()
			res, rj := pl.IngestBatch("WB-"+name, msgs)
			if rj == nil {
				t.Fatal("expected batch rejection")
			}
			if res != nil && res.Status == domain.StatusAccepted {
				t.Fatal("rejected batch cannot report accepted")
			}
			if len(st.Snapshot().Patients) != 0 {
				t.Fatalf("%s: no data may land on rejected batch, got %d patients", name, len(st.Snapshot().Patients))
			}
			// sensitive raw fields must not leak into error, log or audit
			for _, leak := range []string{"BATCHSECRET", "BATCHSUR", "BATCHGARCIA"} {
				if strings.Contains(rj.Error(), leak) {
					t.Fatalf("%s: error leaked %s: %v", name, leak, rj)
				}
				if strings.Contains(logBuf.String(), leak) {
					t.Fatalf("%s: log leaked %s", name, leak)
				}
			}
			for _, e := range al.Entries() {
				for _, r := range e.Resources {
					if strings.Contains(r, "BATCHSECRET") || strings.Contains(r, "BATCHSUR") {
						t.Fatalf("%s: audit leaked sensitive resource %s", name, r)
					}
				}
			}
			if err := al.Verify(); err != nil {
				t.Fatal(err)
			}
			t.Logf("business=atomic-batch case=%s result=pass (zero landed, no sensitive leak)", name)
		})
	}
}

func TestIngestBatch_ReplayAndFixResubmit(t *testing.T) {
	pl, _, st, _ := newTestPipeline()
	msgs := []BatchMessage{
		{Ref: "a", Format: "hl7v2", Payload: []byte(hl7A)},
		{Ref: "c", Format: "fhir", Payload: []byte(fhirC)},
	}
	first, rj := pl.IngestBatch("WB-R", msgs)
	if rj != nil {
		t.Fatal(rj)
	}
	// identical batch replay -> whole batch deduplicated
	replay, rj := pl.IngestBatch("WB-R", msgs)
	if rj != nil {
		t.Fatal("identical replay should not be an error")
	}
	if replay.Status != domain.StatusDeduplicated {
		t.Fatalf("replay=%s want deduplicated", replay.Status)
	}
	if len(st.Snapshot().Patients) != 2 {
		t.Fatal("replay must not add data")
	}

	// a failing batch with the same batch id must not block; then fixing it
	// under a NEW batch id commits. Note first batch already consumed msg ids
	// PB-1001/PB-2001, so the fixed batch uses fresh messages.
	hl7A2 := strings.Replace(hl7A, "PB-1001", "PB-1003", 1)
	hl7A2 = strings.Replace(hl7A2, "BMRN-1", "BMRN-3", 1)
	hl7A2 = strings.Replace(hl7A2, "BV-1", "BV-3", 1)
	badHL7 := "MSH|^~\\&|H|C|L|C|20200101000000||ADT^A01|PB-1004|P|2.2\r" +
		"PID|1||BMRN-3||X^Y||19700101|M\rPV1|1|I|W||||||||||||||||BV-3b"
	_, rj = pl.IngestBatch("WB-F", []BatchMessage{
		{Ref: "a", Format: "hl7v2", Payload: []byte(hl7A2)},
		{Ref: "bad", Format: "hl7v2", Payload: []byte(badHL7)},
	})
	if rj == nil {
		t.Fatal("expected WB-F to fail on version")
	}
	// the good message of the failed batch must not be committed
	for _, p := range st.Snapshot().Patients {
		if p.MRN == "BMRN-3" {
			t.Fatal("failed batch must leave no data")
		}
	}
	// fix bad message (same msg id PB-1004, valid version) and resubmit:
	// since the failed attempt never registered anything, it must now commit.
	fixedHL7 := "MSH|^~\\&|H|C|L|C|20260905101500||ADT^A01|PB-1004|P|2.5.1\r" +
		"PID|1||BMRN-3||FIXEDSUR^Y||19700101|M\rPV1|1|I|W||||||||||||||||BV-3b"
	if _, rj = pl.IngestBatch("WB-F", []BatchMessage{
		{Ref: "a", Format: "hl7v2", Payload: []byte(hl7A2)},
		{Ref: "fixed", Format: "hl7v2", Payload: []byte(fixedHL7)},
	}); rj != nil {
		t.Fatalf("fixed batch should commit: %v", rj)
	}
	found := false
	for _, p := range st.Snapshot().Patients {
		if p.MRN == "BMRN-3" {
			found = true
		}
	}
	if !found {
		t.Fatal("fixed batch did not commit BMRN-3")
	}
	t.Log("business=atomic-batch case=failed-then-fixed-resubmit-with-same-batch-id result=pass")
	_ = first
}
