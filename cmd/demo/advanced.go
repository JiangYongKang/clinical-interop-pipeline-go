package main

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/export"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// runBatchIngestDemo exercises atomic whole-batch submission: a first batch
// containing one unsupported message must leave no trace; after the message
// is fixed, the same batch id commits fully; an identical replay is reported
// as deduplicated.
func runBatchIngestDemo(st *store.Store, pl *pipeline.Pipeline, logger *log.Logger) {
	fmt.Println("\n=== atomic batch ingestion ===")

	good := sampleMessage{
		Ref: "b.good", Format: "hl7v2",
		Payload: "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|20260910101500||ADT^A01|BATCH-1001|P|2.5.1\r" +
			"PID|1||MRN-B1||BATCHSURNAME^ANA||19900101|F\r" +
			"PV1|1|I|WARD-1||||||||||||||||VISIT-B1",
	}
	bad := sampleMessage{
		Ref: "b.bad", Format: "hl7v2",
		Payload: "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|20260910101600||ADT^A01|BATCH-1002|P|2.2\r" +
			"PID|1||MRN-B2||SHOULDNOTLAND^BOB||19910101|M\r" +
			"PV1|1|I|WARD-2||||||||||||||||VISIT-B2",
	}

	if _, rj := pl.IngestBatch("BATCH-A", []pipeline.BatchMessage{
		{Ref: good.Ref, Format: good.Format, Payload: []byte(good.Payload)},
		{Ref: bad.Ref, Format: bad.Format, Payload: []byte(bad.Payload)},
	}); rj != nil {
		fmt.Printf("[reject] %s -> %s (whole batch rejected; nothing landed)\n", "BATCH-A", rj.Error())
	}
	snap := st.Snapshot()
	fmt.Printf("after failed batch: patients=%d (expect 0 batch patients)\n", countMRNs(snap, "MRN-B"))

	fixed := sampleMessage{
		Ref: "b.fixed", Format: "hl7v2",
		Payload: "MSH|^~\\&|HOSP-A|CITY|LAB|CITY|20260910101600||ADT^A01|BATCH-1002|P|2.5.1\r" +
			"PID|1||MRN-B2||FIXEDSURNAME^BOB||19910101|M\r" +
			"PV1|1|I|WARD-2||||||||||||||||VISIT-B2",
	}
	res, rj := pl.IngestBatch("BATCH-A", []pipeline.BatchMessage{
		{Ref: good.Ref, Format: good.Format, Payload: []byte(good.Payload)},
		{Ref: fixed.Ref, Format: fixed.Format, Payload: []byte(fixed.Payload)},
	})
	if rj != nil {
		fmt.Println("[fatal] fixed batch rejected:", rj)
		return
	}
	fmt.Printf("fixed batch committed: status=%s items=%d\n", res.Status, len(res.Items))

	replay, _ := pl.IngestBatch("BATCH-A", []pipeline.BatchMessage{
		{Ref: fixed.Ref, Format: fixed.Format, Payload: []byte(fixed.Payload)},
		{Ref: good.Ref, Format: good.Format, Payload: []byte(good.Payload)},
	})
	fmt.Printf("identical batch replay (shuffled): status=%s\n", replay.Status)
}

// runHistoryDemo writes a patient conclusion, an amendment and a withdrawal
// at increasing business/recording times and then reads several as-of views.
// It uses its own pipeline with an advancing serving clock so that the
// recording time of each result is realistic and distinct from its business
// time.
func runHistoryDemo() {
	fmt.Println("\n=== history & point-in-time reconstruction ===")
	hst := store.New()
	hal := audit.New()
	step := 0
	clock := func() time.Time {
		base, _ := time.Parse("2006-01-02T15:04:05Z", "2026-09-01T08:00:00Z")
		return base.Add(time.Duration(step) * 4 * 24 * time.Hour)
	}
	pl := pipeline.New(hst, hal, log.New(io.Discard, "", 0), clock)
	st := hst
	_ = hal
	obs := func(msg, ts, status, val string) sampleMessage {
		return sampleMessage{
			Ref: "h." + msg, Format: "hl7v2",
			Payload: "MSH|^~\\&|LAB|CITY|H|CITY|" + ts + "||ORU^R01|" + msg + "|P|2.5.1\r" +
				"PID|1||MRN-H1||HISTSURNAME^LEA||19850505|F\r" +
				"OBR|1||ORD-H\rOBX|1|NM|K^Potassium||" + val + "|mmol/L|||||" + status + "|||" + ts,
		}
	}
	// Business time (OBX-14/MSH-7) and recording (clock) both advance.
	msgs := []sampleMessage{
		obs("HIST-1", "20260901090000", "F", "4.1"),
		obs("HIST-2", "20260905090000", "C", "4.9"), // amended/corrected
		obs("HIST-3", "20260910090000", "X", "4.9"), // cancelled/withdrawn
	}
	for _, m := range msgs {
		// serving clock starts at the business day of the first delivery and
		// advances with each delivery (recording time != business time)
		step++
		if _, rj := pl.Ingest(m.Format, m.Ref, []byte(m.Payload)); rj != nil {
			fmt.Println("history ingest reject:", rj)
		}
	}
	query := func(label, day string) {
		at, _ := time.Parse("2006-01-02", day)
		view := st.SnapshotAt(at, store.HistoryOptions{MRN: "MRN-H1"})
		if len(view.Observations) == 0 {
			fmt.Printf("as-of %s (%s): no observation yet\n", day, label)
			return
		}
		o := view.Observations[0]
		num := 0.0
		if o.ValueNumber != nil {
			num = *o.ValueNumber
		}
		fmt.Printf("as-of %s (%s): status=%s value=%.1f\n", day, label, o.Status, num)
	}
	query("before everything", "2026-08-31")
	query("after first result (recorded 09-05)", "2026-09-06")
	query("after correction (recorded 09-09)", "2026-09-10")
	query("after withdrawal (recorded 09-13)", "2026-09-14")

	revs, err := st.Timeline("observation", "ORD-H-1-K")
	if err != nil {
		fmt.Println("timeline error:", err)
		return
	}
	fmt.Print("timeline (corrections/withdrawals never erased):")
	for _, r := range revs {
		fmt.Printf(" [%s %s@%s]", r.Observation.Status, r.Writer, r.UpdatedAt.Format("2006-01-02"))
	}
	fmt.Println()
}

// runPagedExportDemo pages through the export with a small page size and
// proves every page comes from one pinned, stable view.
func runPagedExportDemo(svc *export.Service) {
	fmt.Println("\n=== paginated consistent export ===")
	const pageSize = 2
	page := 1
	var cursor string
	totalPatients, totalEnc, totalObs := 0, 0, 0
	for {
		req := export.PageRequest{
			Actor: "analytics", Purpose: export.PurposeNormal,
			Ref: fmt.Sprintf("page-%d", page), At: fixedClock(), PageSize: pageSize, Cursor: cursor,
		}
		res, err := svc.ExportPage(req)
		if err != nil {
			fmt.Println("paged export refused:", err)
			return
		}
		totalPatients += len(res.Patients)
		totalEnc += len(res.Encounters)
		totalObs += len(res.Observations)
		fmt.Printf("page %d: patients=%d encounters=%d observations=%d last=%v\n",
			page, len(res.Patients), len(res.Encounters), len(res.Observations), res.LastPage)
		if res.LastPage {
			break
		}
		cursor = res.NextCursor
		page++
	}
	fmt.Printf("paged totals: patients=%d encounters=%d observations=%d\n", totalPatients, totalEnc, totalObs)

	// A request naming a patient that does not exist is rejected wholesale.
	if _, err := svc.ExportPage(export.PageRequest{
		Actor: "analytics", Purpose: export.PurposeNormal,
		Ref: "page-ghost", At: fixedClock(), PageSize: pageSize, PatientMRNs: []string{"MRN-DOES-NOT-EXIST"},
	}); err != nil {
		fmt.Println("unknown-patient export refused (no raw id echoed):", err)
	}
}

func countMRNs(snap store.Snapshot, prefix string) int {
	n := 0
	for _, p := range snap.Patients {
		if len(p.MRN) >= len(prefix) && p.MRN[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}
