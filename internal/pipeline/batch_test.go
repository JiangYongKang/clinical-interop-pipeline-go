package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

func adtMsg(msgID, mrn, surname string, ts string) BatchItem {
	body := "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|" + ts + "||ADT^A01^ADT_A01|" + msgID + "|P|2.5.1\r" +
		"PID|1||" + mrn + "||" + surname + "^JANE||19840312|F\r" +
		"PV1|1|I|WARD-7||||||||||||||||VISIT-" + mrn
	return BatchItem{Format: "hl7v2", Ref: "ref-" + msgID, Payload: []byte(body)}
}

func oruMsg(msgID, mrn, mshTS, effTS string) BatchItem {
	body := "MSH|^~\\&|LAB|CITY|H|C|" + mshTS + "||ORU^R01|" + msgID + "|P|2.3.1\r" +
		"PID|1||" + mrn + "||BATCHSECRET^JANE||19840312|F\r" +
		"OBR|1||ORD-7\rOBX|1|NM|GLU^Glucose||5.6|mmol/L|||||F|||" + effTS
	return BatchItem{Format: "hl7v2", Ref: "ref-" + msgID, Payload: []byte(body)}
}

// goodBatch has a strictly ascending per-patient business timeline:
// 09-03 ADT -> 09-04 ORU (same patient), 09-05 ADT (second patient).
func goodBatch() []BatchItem {
	return []BatchItem{
		adtMsg("BM-1", "MRN-B1", "BATCHSECRET1", "20260903101500"),
		oruMsg("BM-2", "MRN-B1", "20260904093000", "20260904091500"),
		adtMsg("BM-3", "MRN-B2", "BATCHSECRET2", "20260905101500"),
	}
}

func TestIngestBatch_HappyPathAllLands(t *testing.T) {
	pl, _, st, al := newTestPipeline()
	res, rj := pl.IngestBatch(goodBatch())
	if rj != nil {
		t.Fatal(rj)
	}
	if res.Status != domain.StatusAccepted || len(res.Items) != 3 {
		t.Fatalf("status=%s items=%d", res.Status, len(res.Items))
	}
	snap := st.Snapshot()
	if len(snap.Patients) != 2 || len(snap.Observations) != 1 || len(snap.Encounters) != 2 {
		t.Fatalf("batch not fully landed: p=%d e=%d o=%d",
			len(snap.Patients), len(snap.Encounters), len(snap.Observations))
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
	// 3 item events + 1 batch summary
	if n := len(al.Entries()); n != 4 {
		t.Fatalf("want 4 audit entries, got %d", n)
	}
}

func TestIngestBatch_OneBadMessageRejectsEverything(t *testing.T) {
	cases := map[string]struct {
		mutate func(b []BatchItem) []BatchItem
		want   domain.Status
	}{
		"unsupported version": {
			mutate: func(b []BatchItem) []BatchItem {
				bad := adtMsg("BAD-VER", "MRN-X1", "BATCHSECRET9", "20200101000000")
				bad.Payload = []byte(strings.ReplaceAll(string(bad.Payload), "2.5.1", "2.2"))
				b[0] = bad
				return b
			},
			want: domain.StatusRejectedVersion,
		},
		"missing required field": {
			mutate: func(b []BatchItem) []BatchItem {
				bad := `{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"BF1"},
				  "entry":[{"resource":{"resourceType":"Patient","familyName":"BATCHSECRET9"}}]}`
				b[1] = BatchItem{Format: "fhir", Ref: "ref-BF1", Payload: []byte(bad)}
				return b
			},
			want: domain.StatusRejectedMissingField,
		},
		"malformed json": {
			mutate: func(b []BatchItem) []BatchItem {
				b[2] = BatchItem{Format: "fhir", Ref: "ref-BADJSON", Payload: []byte("{not-json")}
				return b
			},
			want: domain.StatusRejectedMalformed,
		},
		"unknown format": {
			mutate: func(b []BatchItem) []BatchItem {
				b[0] = BatchItem{Format: "csv", Ref: "ref-CSV", Payload: []byte("BATCHSECRET9,x")}
				return b
			},
			want: domain.StatusRejectedMalformed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pl, logBuf, st, al := newTestPipeline()
			_, rj := pl.IngestBatch(tc.mutate(goodBatch()))
			if rj == nil {
				t.Fatal("expected batch rejection")
			}
			if rj.Status != tc.want {
				t.Fatalf("status=%s want %s", rj.Status, tc.want)
			}
			if n := len(st.Snapshot().Patients); n != 0 {
				t.Fatalf("rejected batch left %d patients behind", n)
			}
			// no raw sensitive fields anywhere
			if strings.Contains(rj.Error(), "BATCHSECRET") || strings.Contains(logBuf.String(), "BATCHSECRET") {
				t.Fatalf("rejection leaked raw field: err=%v", rj)
			}
			entries := al.Entries()
			if len(entries) != 1 || entries[0].Decision != "deny" || len(entries[0].Resources) != 0 {
				t.Fatalf("rejected batch must leave one resource-free deny event, got %+v", entries)
			}
			if err := al.Verify(); err != nil {
				t.Fatal(err)
			}

			// fix the offending item and resend the WHOLE batch: must apply
			res2, rj2 := pl.IngestBatch(goodBatch())
			if rj2 != nil {
				t.Fatalf("corrected batch rejected: %v", rj2)
			}
			if res2.Status != domain.StatusAccepted {
				t.Fatalf("corrected batch status=%s", res2.Status)
			}
			if n := len(st.Snapshot().Patients); n != 2 {
				t.Fatalf("corrected batch landed %d patients, want 2", n)
			}
		})
	}
}

func TestIngestBatch_ExactReplayIsDeduplicatedAndRepeats(t *testing.T) {
	run := func() string {
		pl, _, st, al := newTestPipeline()
		r1, _ := pl.IngestBatch(goodBatch())
		r2, _ := pl.IngestBatch(goodBatch())
		snap := st.Snapshot()
		return fmt.Sprintf("%s|%s|p=%d,e=%d,o=%d,audit=%d",
			r1.Status, r2.Status, len(snap.Patients), len(snap.Encounters),
			len(snap.Observations), len(al.Entries()))
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("batch replay non-deterministic:\n%s\n%s", a, b)
	}
	if want := "accepted|deduplicated|p=2,e=2,o=1,audit=8"; a != want {
		t.Fatalf("got %s want %s", a, want)
	}
}

func TestIngestBatch_StaleAgainstCommittedStateAbortsBatch(t *testing.T) {
	pl, _, st, _ := newTestPipeline()
	// commit a newer patient conclusion first
	newer := adtMsg("NEWER", "MRN-B1", "CURRENTNAME", "20260920101500")
	if _, rj := pl.Ingest(newer.Format, newer.Ref, newer.Payload); rj != nil {
		t.Fatal(rj)
	}
	// the batch carries the same patient with OLDER business times -> abort
	if res, rj := pl.IngestBatch(goodBatch()); rj == nil || res.Status != domain.StatusStale {
		t.Fatalf("want stale batch rejection, got %v %v", res, rj)
	}
	if n := len(st.Snapshot().Patients); n != 1 {
		t.Fatalf("stale batch must land nothing, patients=%d", n)
	}
	// bring every batch item for that patient forward past the committed
	// conclusion (ADT 09-25 then ORU 09-26); the second patient is unaffected.
	fixed := goodBatch()
	fixed[0] = adtMsg("BM-1", "MRN-B1", "BATCHSECRET1", "20260925101500")
	fixed[1] = oruMsg("BM-2", "MRN-B1", "20260926093000", "20260926091500")
	res, rj := pl.IngestBatch(fixed)
	if rj != nil {
		t.Fatal(rj)
	}
	if res.Status != domain.StatusAccepted {
		t.Fatalf("corrected batch status=%s", res.Status)
	}
}

func TestIngestBatch_EmptyBatchRejected(t *testing.T) {
	pl, _, _, al := newTestPipeline()
	res, rj := pl.IngestBatch(nil)
	if rj == nil || res.Status != domain.StatusRejectedMissingField {
		t.Fatalf("empty batch: %v %v", res, rj)
	}
	if entries := al.Entries(); len(entries) != 1 || entries[0].Decision != "deny" {
		t.Fatal("empty batch not audited as deny")
	}
}
