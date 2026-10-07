package main

import (
	"fmt"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/export"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

func toItems(msgs []sampleMessage) []pipeline.BatchItem {
	items := make([]pipeline.BatchItem, len(msgs))
	for i, m := range msgs {
		items[i] = pipeline.BatchItem{Format: m.Format, Ref: m.Ref, Payload: []byte(m.Payload)}
	}
	return items
}

// demoBatch shows all-or-nothing batch commit: a poisoned batch lands nothing,
// its identical re-run semantics, then the corrected batch fully applies.
func demoBatch(pl *pipeline.Pipeline, st *store.Store) {
	fmt.Println("\n=== atomic batch ingest ===")

	before := len(st.Snapshot().Patients)
	_, rj := pl.IngestBatch(toItems(badBatch))
	if rj == nil {
		fmt.Println("unexpected: poisoned batch accepted")
	} else {
		fmt.Printf("[reject] poisoned batch -> %s\n", rj.Error())
	}
	if after := len(st.Snapshot().Patients); after != before {
		fmt.Printf("!! half-applied batch detected: patients %d -> %d\n", before, after)
	} else {
		fmt.Printf("no partial state: patients stayed at %d\n", before)
	}

	res, rj := pl.IngestBatch(toItems(goodBatch))
	if rj != nil {
		fmt.Printf("[reject] good batch -> %v\n", rj)
		return
	}
	fmt.Printf("[commit] good batch status=%s items=%d\n", res.Status, len(res.Items))

	res2, _ := pl.IngestBatch(toItems(goodBatch))
	fmt.Printf("[replay] identical batch re-run status=%s (wholly deduplicated)\n", res2.Status)
}

// demoHistory shows point-in-time reconstruction over corrections.
func demoHistory(pl *pipeline.Pipeline, st *store.Store) {
	fmt.Println("\n=== point-in-time history ===")
	point1 := time.Date(2026, 9, 15, 23, 59, 59, 0, time.UTC)
	// Each message "arrives" at its own business time so the as-of semantics
	// are visible; the production serving clock is monotonic the same way.
	arrivals := map[string]time.Time{
		"hist-v1":            time.Date(2026, 9, 15, 8, 0, 0, 0, time.UTC),
		"hist-v2-correction": time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC),
	}
	for _, m := range historyMessages {
		at := arrivals[m.Ref]
		pl.SetClock(func() time.Time { return at })
		if _, rj := pl.Ingest(m.Format, m.Ref, []byte(m.Payload)); rj != nil {
			fmt.Printf("[reject] %s -> %v\n", m.Ref, rj)
		}
	}
	pl.SetClock(fixedClock)
	vAt := func(at time.Time) string {
		p := st.PatientAt("MRN-H001", at)
		if p == nil {
			return "<none>"
		}
		return p.FamilyName
	}
	fmt.Printf("view at %s -> %s (before correction)\n",
		point1.Format(time.RFC3339), vAt(point1))
	after := time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)
	fmt.Printf("view at %s -> %s (after correction)\n",
		after.Format(time.RFC3339), vAt(after))
	// re-query the old point after the correction landed: must be identical
	fmt.Printf("re-query old point -> %s (history immutable)\n", vAt(point1))
	revs := st.PatientHistory("MRN-H001")
	fmt.Printf("timeline revisions=%d:", len(revs))
	for _, r := range revs {
		fmt.Printf(" [%s@%s by %s]", r.Patient.FamilyName,
			r.BusinessAt.Format("2006-01-02"), r.WriterMsgID)
	}
	fmt.Println()
}

// demoPagination shows a frozen, consistent, paginated export and the
// all-or-nothing roster checks.
func demoPagination(svc *export.Service) {
	fmt.Println("\n=== paginated consistent export ===")
	at := fixedClock()
	sess, rj := svc.OpenSession(export.PageRequest{
		Actor: "analytics", Purpose: export.PurposeNormal, PageSize: 2, At: at,
	})
	if rj != nil {
		fmt.Printf("[reject] open session -> %v\n", rj)
		return
	}
	for {
		pg, err := svc.NextPage(sess.Token())
		if err != nil {
			fmt.Printf("[reject] next page -> %v\n", err)
			break
		}
		fmt.Printf("page %d/%d patients=%d encounters=%d observations=%d done=%v\n",
			pg.Index, pg.TotalPages, len(pg.Patients), len(pg.Encounters),
			len(pg.Observations), pg.Done)
		if pg.Done {
			break
		}
	}
	svc.CloseSession(sess.Token())

	// a roster naming a nonexistent patient rejects the whole request
	if _, err := svc.OpenSession(export.PageRequest{
		Actor: "analytics", Purpose: export.PurposeNormal,
		PatientMRNs: []string{"MRN-0001", "MRN-DOES-NOT-EXIST"}, PageSize: 10, At: at,
	}); err != nil {
		fmt.Println("[reject] roster with nonexistent patient ->", err)
	}
}
