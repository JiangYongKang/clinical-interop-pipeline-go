package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/export"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

type sampleMessage struct {
	Ref     string
	Format  string
	Payload string
}

// fixedClock makes demo runs deterministic and repeatable.
func fixedClock() time.Time { return time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC) }

func demoRoles() []export.Role {
	return []export.Role{
		{Name: "analytics", Scopes: map[string]bool{
			export.ScopePatientRead: true, export.ScopeEncounterRead: true, export.ScopeObservationRead: true,
		}, AllowedMRNs: map[string]bool{"*": true}},
		{Name: "auditor", Scopes: map[string]bool{
			export.ScopePatientRead: true, export.ScopeEncounterRead: true,
			export.ScopeObservationRead: true, export.ScopeAuditUnmask: true,
		}, AllowedMRNs: map[string]bool{"*": true}},
		{Name: "lab-tech", Scopes: map[string]bool{
			export.ScopeObservationRead: true,
		}, AllowedMRNs: map[string]bool{"MRN-0002": true}},
	}
}

func main() {
	runTwice := flag.Bool("repeat", false, "ingest the same batch twice and show deterministic convergence")
	flag.Parse()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	runBatch(logger, 1)
	if *runTwice {
		fmt.Println("\n=== second run (same batch, fresh store) ===")
		runBatch(logger, 2)
	}
}

func runBatch(logger *log.Logger, round int) {
	st := store.New()
	al := audit.New()
	pl := pipeline.New(st, al, logger, fixedClock)

	fmt.Printf("\n=== round %d: ingestion ===\n", round)
	for _, m := range append(append(append([]sampleMessage{}, sampleMessages...), staleMessages...), badMessages...) {
		_, rj := pl.Ingest(m.Format, m.Ref, []byte(m.Payload))
		if rj != nil {
			fmt.Printf("[reject] %-22s -> %s\n", m.Ref, rj.Error())
		}
	}
	if _, rj := pl.Ingest(typeConflictFHIR.Format, typeConflictFHIR.Ref, []byte(typeConflictFHIR.Payload)); rj != nil {
		fmt.Printf("[reject] %-22s -> %s\n", typeConflictFHIR.Ref, rj.Error())
	}
	// exact redelivery of the first HL7 message -> deduplicated
	if _, rj := pl.Ingest(sampleMessages[0].Format, sampleMessages[0].Ref+"-redelivery", []byte(sampleMessages[0].Payload)); rj != nil {
		fmt.Printf("[reject] redelivery error: %v\n", rj)
	}

	// new capabilities: atomic batch, historical reconstruction, paged export
	runBatchIngestDemo(st, pl, logger)
	runHistoryDemo()

	svc := export.New(st, al, demoRoles(), []byte("local-demo-mac-key"))
	runPagedExportDemo(svc)

	fmt.Println("\n--- analytics (normal purpose, masked) ---")
	showExport(svc, export.Request{Actor: "analytics", Purpose: export.PurposeNormal})

	fmt.Println("--- auditor (audit purpose, family_name unmasked) ---")
	showExport(svc, export.Request{Actor: "auditor", Purpose: export.PurposeAudit, UnmaskedFields: []string{"family_name"}})

	fmt.Println("--- analytics attempting unmask (must be refused) ---")
	if _, err := svc.Export(export.Request{Actor: "analytics", Purpose: export.PurposeNormal, UnmaskedFields: []string{"family_name"}}); err != nil {
		fmt.Println("refused:", err)
	}

	fmt.Println("--- lab-tech requesting patient data (must be refused) ---")
	if _, err := svc.Export(export.Request{Actor: "lab-tech", Purpose: export.PurposeNormal, PatientMRNs: []string{"MRN-0001"}}); err != nil {
		fmt.Println("refused:", err)
	}

	if err := al.Verify(); err != nil {
		fmt.Println("AUDIT CHAIN INVALID:", err)
	} else {
		fmt.Printf("\naudit chain verified: %d entries, chain intact\n", len(al.Entries()))
	}

	snap := st.Snapshot()
	fmt.Printf("final state: patients=%d encounters=%d observations=%d\n",
		len(snap.Patients), len(snap.Encounters), len(snap.Observations))
	for _, p := range snap.Patients {
		fmt.Printf("  patient mrn=%s family=%s gender=%s version=%s updated=%s\n",
			p.MRN, p.FamilyName, p.Gender, p.Version, p.UpdatedAt.Format(time.RFC3339))
	}
}

func showExport(svc *export.Service, req export.Request) {
	res, err := svc.Export(req)
	if err != nil {
		fmt.Println("refused:", err)
		return
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
}
