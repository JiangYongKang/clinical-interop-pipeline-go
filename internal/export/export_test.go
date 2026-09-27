package export

import (
	"strings"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

var at = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

func seedStore(t *testing.T) (*store.Store, *audit.Log) {
	t.Helper()
	s := store.New()
	cs := &normalize.ChangeSet{
		MessageID: "M1", Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: at,
		Patient: &domain.Patient{
			MRN: "mrn-1", FamilyName: "SMITH", GivenName: "JANE",
			BirthDate: "1984-03-12", Gender: "female", Version: "R4", UpdatedAt: at,
		},
		Encounter: &domain.Encounter{
			VisitID: "visit-1", MRN: "mrn-1", Class: "IMP",
			StartedAt: "2026-09-01T10:00:00Z", Version: "R4", UpdatedAt: at,
		},
		Observations: []*domain.Observation{{
			ObservationID: "obs-1", MRN: "mrn-1", VisitID: "visit-1",
			TestCode: "GLU", Status: "final", ValueString: "5.6",
			Version: "R4", UpdatedAt: at,
		}},
	}
	if o := s.Apply(cs, at); o.Status != domain.StatusAccepted {
		t.Fatalf("seed failed: %s %s", o.Status, o.Reason)
	}
	// a second patient the lab-tech is allowed to see
	cs2 := &normalize.ChangeSet{
		MessageID: "M2", Version: "R4", Rank: normalize.VersionRanks["R4"],
		ReceivedAt: at,
		Patient: &domain.Patient{
			MRN: "mrn-2", FamilyName: "JONES", GivenName: "A",
			BirthDate: "1970-01-01", Gender: "male", Version: "R4", UpdatedAt: at,
		},
	}
	s.Apply(cs2, at)
	return s, audit.New()
}

func testRoles() []Role {
	return []Role{
		{Name: "analytics", Scopes: map[string]bool{
			ScopePatientRead: true, ScopeEncounterRead: true, ScopeObservationRead: true,
		}, AllowedMRNs: map[string]bool{"*": true}},
		{Name: "auditor", Scopes: map[string]bool{
			ScopePatientRead: true, ScopeEncounterRead: true,
			ScopeObservationRead: true, ScopeAuditUnmask: true,
		}, AllowedMRNs: map[string]bool{"*": true}},
		{Name: "lab-tech", Scopes: map[string]bool{
			ScopeObservationRead: true,
		}, AllowedMRNs: map[string]bool{"mrn-2": true}},
	}
}

func TestExport_NormalPurposeIsMasked(t *testing.T) {
	s, al := seedStore(t)
	svc := New(s, al, testRoles(), []byte("k"))
	res, err := svc.Export(Request{Actor: "analytics", Purpose: PurposeNormal, At: at})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Patients) != 2 {
		t.Fatalf("want 2 patients, got %d", len(res.Patients))
	}
	for _, p := range res.Patients {
		if p.FamilyName != "***" || p.GivenName != "***" || p.BirthDate != "****-**-**" || p.Gender != "*" {
			t.Fatalf("identity not masked: %+v", p)
		}
		if strings.HasPrefix(p.MRN, "mrn-") {
			t.Fatalf("raw MRN leaked: %s", p.MRN)
		}
	}
	// pseudonyms must be stable joins across resource kinds and exports
	p1 := res.Patients[0].MRN
	var linked bool
	for _, e := range res.Encounters {
		if e.MRN == p1 {
			linked = true
		}
	}
	if !linked {
		t.Fatal("masked encounter MRN does not join to masked patient MRN")
	}
	res2, _ := svc.Export(Request{Actor: "analytics", Purpose: PurposeNormal, At: at})
	if res2.Patients[0].MRN != res.Patients[0].MRN {
		t.Fatal("pseudonym is not deterministic across exports")
	}
}

func TestExport_AuditPurposeSelectiveUnmask(t *testing.T) {
	s, al := seedStore(t)
	svc := New(s, al, testRoles(), []byte("k"))
	res, err := svc.Export(Request{
		Actor: "auditor", Purpose: PurposeAudit, UnmaskedFields: []string{"family_name"}, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	var foundRaw bool
	for _, p := range res.Patients {
		if p.FamilyName == "SMITH" {
			foundRaw = true
			if p.GivenName != "***" {
				t.Fatal("non-selected field must stay masked")
			}
		}
	}
	if !foundRaw {
		t.Fatal("audit export did not retain authorized family name")
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestExport_Denials(t *testing.T) {
	s, al := seedStore(t)
	svc := New(s, al, testRoles(), []byte("k"))

	deny := func(name string, req Request, want domain.Status) {
		t.Helper()
		_, err := svc.Export(req)
		if err == nil {
			t.Fatalf("%s: expected denial", name)
		}
		rj := err.(*domain.RejectError)
		if rj.Status != want {
			t.Fatalf("%s: status=%s want %s", name, rj.Status, want)
		}
		// errors must never carry raw identifiers
		if strings.Contains(err.Error(), "SMITH") || strings.Contains(err.Error(), "mrn-1") {
			t.Fatalf("%s: error leaked sensitive data: %v", name, err)
		}
	}
	deny("unknown actor", Request{Actor: "ghost", Purpose: PurposeNormal, At: at}, domain.StatusRejectedPermission)
	deny("normal purpose unmask", Request{
		Actor: "analytics", Purpose: PurposeNormal, UnmaskedFields: []string{"family_name"}, At: at,
	}, domain.StatusRejectedInvalidMask)
	deny("illegal field", Request{
		Actor: "auditor", Purpose: PurposeAudit, UnmaskedFields: []string{"ssn"}, At: at,
	}, domain.StatusRejectedInvalidMask)
	deny("missing unmask scope", Request{
		Actor: "analytics", Purpose: PurposeAudit, At: at,
	}, domain.StatusRejectedPermission)
	deny("patient outside allow-list", Request{
		Actor: "lab-tech", Purpose: PurposeNormal, PatientMRNs: []string{"mrn-1"}, At: at,
	}, domain.StatusRejectedPermission)

	// every denial must leave an audit trail; no partial results may exist
	entries := al.Entries()
	if len(entries) != 5 {
		t.Fatalf("want 5 denial audit entries, got %d", len(entries))
	}
	for _, e := range entries {
		if e.Decision != "deny" {
			t.Fatalf("expected deny entry, got %+v", e)
		}
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestExport_ScopeLimitsResources(t *testing.T) {
	s, al := seedStore(t)
	svc := New(s, al, testRoles(), []byte("k"))
	// lab-tech only has observation:read and only mrn-2, which has no obs
	res, err := svc.Export(Request{Actor: "lab-tech", Purpose: PurposeNormal, At: at})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Patients) != 0 || len(res.Encounters) != 0 || len(res.Observations) != 0 {
		t.Fatalf("scoped actor must see no data, got %+v", res)
	}
}
