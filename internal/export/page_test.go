package export

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

func TestExportPage_PaginationStableAcrossConcurrentWrites(t *testing.T) {
	s, al := seedManyTyped(t, 6)
	svc := New(s, al, testRoles(), []byte("k"))

	// First page opens a pinned read with a tiny page size.
	p1, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if p1.LastPage || p1.NextCursor == "" {
		t.Fatal("expected more pages")
	}
	firstMRNs := append([]string{}, collectPageMRNs(p1)...)

	// While paging, a writer keeps adding brand new patients. It writes one
	// guaranteed late patient before the read could finish, then keeps going
	// until told to stop.
	stop := make(chan struct{})
	committed := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstDone := false
		i := 100
		for {
			select {
			case <-stop:
				return
			default:
				mrn := "late-" + itoaPkg(i)
				cs := &normalize.ChangeSet{
					MessageID: "LATE" + itoaPkg(i), Version: "R4", Rank: normalize.VersionRanks["R4"],
					ReceivedAt: at,
					Patient: &domain.Patient{
						MRN: mrn, FamilyName: "LATE", BirthDate: "1970-01-01",
						Gender: "male", Version: "R4", UpdatedAt: at,
					},
				}
				s.Apply(cs, at)
				if !firstDone {
					firstDone = true
					committed <- struct{}{}
				}
				i++
				if i > 200 {
					return // write each id once so fresh read grows unboundedly
				}
			}
		}
	}()
	<-committed

	var seen []string
	seen = append(seen, firstMRNs...)
	cur := p1.NextCursor
	pages := 1
	for {
		pg, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 2, Cursor: cur})
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, collectPageMRNs(pg)...)
		pages++
		if pg.LastPage {
			break
		}
		cur = pg.NextCursor
		if pages > 100 {
			t.Fatal("paging did not terminate")
		}
	}
	close(stop)
	wg.Wait()

	// The pinned read must show exactly the original 6 patients x 3 rows
	// (patient+encounter+observation) = 18 rows, each once, with none of the
	// concurrently written late-* patients drifting in.
	if len(seen) != 18 {
		t.Fatalf("want 18 rows from pinned read, got %d: %v", len(seen), seen)
	}
	seenSet := map[string]int{}
	for _, m := range seen {
		seenSet[m]++
	}
	for i := 0; i < 6; i++ {
		// rows are pseudonymized but stable; verify uniqueness instead
	}
	for m, c := range seenSet {
		if c != 1 {
			t.Fatalf("row duplicated/missing across pages: %s x%d", m, c)
		}
		if strings.Contains(m, "late-") {
			t.Fatal("concurrent write leaked into a pinned read")
		}
	}

	// A fresh read opened afterwards DOES see the late writers.
	fresh, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if !fresh.LastPage {
		t.Fatal("fresh read should fit on one page")
	}
	if len(fresh.Patients) <= 6 {
		t.Fatalf("fresh read should include concurrent writes, got %d", len(fresh.Patients))
	}
	t.Log("business=paged-consistency case=pages-stable-while-writer-runs result=pass (pinned view, no drift)")
}

func TestExportPage_NonexistentOrUnauthorizedRejectsWholeRequest(t *testing.T) {
	s, al := seedManyTyped(t, 3)
	svc := New(s, al, testRoles(), []byte("k"))

	cases := map[string]PageRequest{
		"nonexistent patient": {
			Actor: "analytics", Purpose: PurposeNormal, At: at,
			PatientMRNs: []string{"mrn-ghost", "mrn-1"},
		},
		"outside allow-list": {
			Actor: "lab-tech", Purpose: PurposeNormal, At: at,
			PatientMRNs: []string{"mrn-1"},
		},
		"mix allowed and not": {
			Actor: "lab-tech", Purpose: PurposeNormal, At: at,
			PatientMRNs: []string{"mrn-2", "mrn-1"},
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			pg, err := svc.ExportPage(req)
			if err == nil {
				t.Fatal("expected denial")
			}
			if pg != nil {
				t.Fatal("denial must not return a partial page")
			}
			rj := err.(*domain.RejectError)
			if rj.Status != domain.StatusRejectedPermission {
				t.Fatalf("status=%s want rejected_permission", rj.Status)
			}
			// denial text must not echo raw MRNs
			for _, bad := range []string{"mrn-ghost", "mrn-1"} {
				if strings.Contains(rj.Error(), bad) {
					t.Fatalf("denial leaked raw selector %s: %v", bad, rj)
				}
			}
			t.Logf("business=export-whole-denial case=%s result=pass (no partial page, no raw id)", name)
		})
	}
}

func TestExportPage_MaskAndScope(t *testing.T) {
	s, al := seedManyTyped(t, 2)
	svc := New(s, al, testRoles(), []byte("k"))
	pg, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pg.Patients {
		if p.FamilyName != "***" {
			t.Fatalf("unmasked patient: %+v", p)
		}
	}
	// lab-tech scope is observation-only: page must contain zero patients/encounters
	lt, err := svc.ExportPage(PageRequest{Actor: "lab-tech", Purpose: PurposeNormal, At: at, PageSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(lt.Patients) != 0 || len(lt.Encounters) != 0 {
		t.Fatalf("lab-tech must not page patients/encounters: %+v", lt)
	}
}

func TestExportPage_BadCursorAndChangingSize(t *testing.T) {
	s, al := seedManyTyped(t, 4)
	svc := New(s, al, testRoles(), []byte("k"))
	p1, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 1, Cursor: "%%%not-base64"}); err == nil {
		t.Fatal("garbage cursor must be rejected")
	}
	if _, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 999, Cursor: p1.NextCursor}); err == nil {
		t.Fatal("changing page size mid-read must be rejected")
	}
	// unknown actor presenting a cursor is also denied wholesale
	if _, err := svc.ExportPage(PageRequest{Actor: "ghost", Purpose: PurposeNormal, At: at, PageSize: 1, Cursor: p1.NextCursor}); err == nil {
		t.Fatal("unknown actor must be denied even with a valid cursor")
	}
}

func TestExportPage_AsOfHistoricalView(t *testing.T) {
	s, al := seedManyTyped(t, 2)
	svc := New(s, al, testRoles(), []byte("k"))
	// ask for a view before anything was recorded -> empty read, but still paged
	pg, err := svc.ExportPage(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, At: at,
		AsOf: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pg.Patients) != 0 {
		t.Fatalf("as-of year 2000 must be empty, got %d", len(pg.Patients))
	}
	if !pg.LastPage {
		t.Fatal("empty read should be the last page")
	}
}

func collectPageMRNs(p *Page) []string {
	out := []string{}
	for _, pat := range p.Patients {
		out = append(out, pat.MRN)
	}
	for _, e := range p.Encounters {
		out = append(out, "enc:"+e.VisitID)
	}
	for _, o := range p.Observations {
		out = append(out, "obs:"+o.ObservationID)
	}
	return out
}

func TestExportPage_CursorBoundToActorAndMask(t *testing.T) {
	s, al := seedManyTyped(t, 3)
	svc := New(s, al, testRoles(), []byte("k"))
	p1, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	// another actor presenting analytics' cursor must be denied, not receive
	// analytics' pinned (masked) page.
	if _, err := svc.ExportPage(PageRequest{Actor: "auditor", Purpose: PurposeNormal, At: at, PageSize: 1, Cursor: p1.NextCursor}); err == nil {
		t.Fatal("cursor must not be reusable by another actor")
	}
	// same actor but switching to audit purpose with unmasking mid-read denied
	if _, err := svc.ExportPage(PageRequest{
		Actor: "auditor", Purpose: PurposeAudit, UnmaskedFields: []string{"family_name"},
		At: at, PageSize: 1, Cursor: p1.NextCursor,
	}); err == nil {
		t.Fatal("cursor must not allow switching purpose/masking")
	}
	// the legitimate continuation still works
	p2, err := svc.ExportPage(PageRequest{Actor: "analytics", Purpose: PurposeNormal, At: at, PageSize: 1, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if p2.Patients[0].FamilyName != "***" {
		t.Fatal("legitimate continuation must stay masked")
	}
}
