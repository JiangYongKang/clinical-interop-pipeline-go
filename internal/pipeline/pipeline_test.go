package pipeline

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

var fixedAt = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

const adt = "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|20260903101500||ADT^A01^ADT_A01|MSG-1001|P|2.5.1\r" +
	"PID|1||MRN-1||SECRETSURNAME^JANE||19840312|F\r" +
	"PV1|1|I|WARD-7||||||||||||||||VISIT-9"

func newTestPipeline() (*Pipeline, *bytes.Buffer, *store.Store, *audit.Log) {
	buf := &bytes.Buffer{}
	logger := log.New(buf, "", 0)
	st := store.New()
	al := audit.New()
	pl := New(st, al, logger, func() time.Time { return fixedAt })
	return pl, buf, st, al
}

func TestIngest_HappyPathAndAudit(t *testing.T) {
	pl, _, _, al := newTestPipeline()
	o, rj := pl.Ingest("hl7v2", "ref-1", []byte(adt))
	if rj != nil {
		t.Fatal(rj)
	}
	if o.Status != domain.StatusAccepted {
		t.Fatalf("status=%s", o.Status)
	}
	entries := al.Entries()
	if len(entries) != 1 || entries[0].Decision != "allow" {
		t.Fatalf("audit not written: %+v", entries)
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestIngest_RejectionsNeverLeakRawFields(t *testing.T) {
	cases := map[string]struct {
		format string
		body   string
		want   domain.Status
	}{
		"bad version": {"hl7v2",
			"MSH|^~\\&|H|C|L|C|20200101000000||ADT^A01|MSG-X|P|2.2\r" +
				"PID|1||MRN-9||SECRETSURNAME^JOHN||19700101|M\r" +
				"PV1|1|I|W||||||||||||||||V9",
			domain.StatusRejectedVersion},
		"bad json": {"fhir", `{"resourceType":"Patient","secret":"SECRETSURNAME"}`,
			domain.StatusRejectedMalformed},
		"unknown format": {"csv", "SECRETSURNAME,123", domain.StatusRejectedMalformed},
		"missing mrn": {"fhir", `{"resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"m"},
		  "entry":[{"resource":{"resourceType":"Patient","familyName":"SECRETSURNAME"}}]}`,
			domain.StatusRejectedMissingField},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pl, logBuf, _, al := newTestPipeline()
			_, rj := pl.Ingest(tc.format, "ref-"+name, []byte(tc.body))
			if rj == nil {
				t.Fatal("expected rejection")
			}
			if rj.Status != tc.want {
				t.Fatalf("status=%s want %s", rj.Status, tc.want)
			}
			if strings.Contains(rj.Error(), "SECRETSURNAME") {
				t.Fatalf("error response leaked raw sensitive field: %v", rj)
			}
			if strings.Contains(logBuf.String(), "SECRETSURNAME") {
				t.Fatal("logger printed raw sensitive field")
			}
			entries := al.Entries()
			if len(entries) != 1 || entries[0].Decision != "deny" {
				t.Fatalf("rejection not fully audited: %+v", entries)
			}
			for _, res := range entries[0].Resources {
				if strings.Contains(res, "SECRETSURNAME") {
					t.Fatal("audit resource leaked sensitive field")
				}
			}
			if err := al.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIngest_BatchReplayDeterministic(t *testing.T) {
	oru := "MSH|^~\\&|LAB|CITY|H|C|20260902093000||ORU^R01|MSG-1002|P|2.3.1\r" +
		"PID|1||MRN-1||SECRETSURNAME^JANE||19840312|F\r" +
		"OBR|1||ORD-7\rOBX|1|NM|GLU^Glucose||5.6|mmol/L|||||F|||20260902091500"
	// old-name message (2026-08) must lose to the newer conclusion
	stale := "MSH|^~\\&|H|C|L|C|20260801000000||ADT^A01|MSG-OLD|P|2.5.1\r" +
		"PID|1||MRN-1||ANCIENT^JANE||19840312|F\r" +
		"PV1|1|I|W||||||||||||||||VISIT-9"
	// Delivery order: ORU (newer patient data, no visit) arrives FIRST, then
	// the ADT that creates the visit (out-of-order by business time but still
	// a newer patient conclusion), then an old-name message that must lose,
	// then an exact redelivery.
	batch := [][2]string{
		{"hl7v2", oru},
		{"hl7v2", adt},
		{"hl7v2", stale},
		{"hl7v2", adt},
	}
	run := func() []string {
		pl, _, st, al := newTestPipeline()
		var statuses []string
		for i, m := range batch {
			o, _ := pl.Ingest(m[0], "b", []byte(m[1]))
			statuses = append(statuses, string(o.Status))
			_ = i
		}
		snap := st.Snapshot()
		statuses = append(statuses,
			"patients="+itoa(len(snap.Patients)),
			"encounters="+itoa(len(snap.Encounters)),
			"observations="+itoa(len(snap.Observations)),
			"family="+snap.Patients[0].FamilyName,
			"audit="+itoa(len(al.Entries())))
		return statuses
	}
	a := run()
	b := run()
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatalf("replay not deterministic:\n%v\n%v", a, b)
	}
	want := []string{"accepted", "updated", "stale", "deduplicated",
		"patients=1", "encounters=1", "observations=1", "family=SECRETSURNAME", "audit=4"}
	if strings.Join(a, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected statuses:\ngot  %v\nwant %v", a, want)
	}
}

func TestIngest_ConcurrentOneStore(t *testing.T) {
	pl, _, st, _ := newTestPipeline()
	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, rj := pl.Ingest("hl7v2", "concurrent", []byte(adt))
			if rj != nil {
				errs <- rj
				return
			}
			switch o.Status {
			case domain.StatusAccepted, domain.StatusDeduplicated:
			default:
				errs <- &domain.RejectError{Status: o.Status, Reason: "unexpected concurrent status"}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if got := len(st.Snapshot().Patients); got != 1 {
		t.Fatalf("want 1 patient after concurrent ingestion, got %d", got)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
