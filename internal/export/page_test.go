package export

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// seedMany seeds n patients (each with one encounter and one observation),
// every revision arriving at t0 with business time t0.
func seedMany(t *testing.T, n int, t0 time.Time) (*store.Store, *audit.Log) {
	t.Helper()
	s := store.New()
	for i := 0; i < n; i++ {
		mrn := mrnOf(i)
		num := float64(i)
		cs := &normalize.ChangeSet{
			MessageID: "M-" + mrn, Version: "R4", Rank: normalize.VersionRanks["R4"],
			ReceivedAt: t0,
			Patient: &domain.Patient{
				MRN: mrn, FamilyName: "SECRET" + mrn, GivenName: "G",
				BirthDate: "1970-01-01", Gender: "female", Version: "R4", UpdatedAt: t0,
			},
			Encounter: &domain.Encounter{
				VisitID: "visit-" + mrn, MRN: mrn, Class: "IMP", Version: "R4", UpdatedAt: t0,
			},
			Observations: []*domain.Observation{{
				ObservationID: "obs-" + mrn, MRN: mrn, VisitID: "visit-" + mrn,
				TestCode: "GLU", Status: "final", ValueNumber: &num,
				Version: "R4", UpdatedAt: t0,
			}},
		}
		if o := s.Apply(cs, t0); o.Status != domain.StatusAccepted {
			t.Fatalf("seed %s: %s %s", mrn, o.Status, o.Reason)
		}
	}
	return s, audit.New()
}

func mrnOf(i int) string { return "mrn-" + pad3(i) }

func pad3(i int) string {
	switch {
	case i < 10:
		return "00" + itoaTest(int64(i))
	case i < 100:
		return "0" + itoaTest(int64(i))
	default:
		return itoaTest(int64(i))
	}
}

func itoaTest(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestSession_PagesAreStableCompleteAndOrdered(t *testing.T) {
	t0 := at
	s, al := seedMany(t, 7, t0)
	svc := New(s, al, testRoles(), []byte("k"))

	sess, rj := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 3, At: t0,
	})
	if rj != nil {
		t.Fatal(rj)
	}
	var totalP, totalE, totalO int
	pageIdx := 0
	// ordering is by the raw MRN; pseudonyms are not lexicographically ordered,
	// so stability is verified by comparing the pagewise pseudonym sequence to a
	// full, independently produced export (which is ordered by raw MRN).
	full, err := svc.Export(Request{Actor: "analytics", Purpose: PurposeNormal, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	var expected []string
	for _, p := range full.Patients {
		expected = append(expected, p.MRN)
	}
	var paged []string
	for {
		pg, err := svc.NextPage(sess.token)
		if err != nil {
			t.Fatal(err)
		}
		pageIdx++
		if pg.Index != pageIdx || pg.TotalPages != 3 {
			t.Fatalf("page meta idx=%d want %d total=%d want 3", pg.Index, pageIdx, pg.TotalPages)
		}
		if len(pg.Patients) > 3 {
			t.Fatalf("page overflow: %d patients", len(pg.Patients))
		}
		for _, p := range pg.Patients {
			if strings.HasPrefix(p.MRN, "mrn-") {
				t.Fatal("raw MRN leaked in page")
			}
			if p.FamilyName != "***" {
				t.Fatal("page not masked")
			}
		}
		for _, p := range pg.Patients {
			paged = append(paged, p.MRN)
		}
		totalP += len(pg.Patients)
		totalE += len(pg.Encounters)
		totalO += len(pg.Observations)
		if pg.Done {
			break
		}
	}
	if strings.Join(paged, ",") != strings.Join(expected, ",") {
		t.Fatalf("pagewise order/content != full export:\npaged %v\nfull  %v", paged, expected)
	}
	if totalP != 7 || totalE != 7 || totalO != 7 {
		t.Fatalf("page totals p=%d e=%d o=%d want 7 each", totalP, totalE, totalO)
	}
	if pageIdx != 3 {
		t.Fatalf("want 3 pages, got %d", pageIdx)
	}
	// two allow events: the paged session and the non-paged Export used above
	// as the ordering oracle
	if n := len(al.Entries()); n != 2 || al.Entries()[0].Decision != "allow" {
		t.Fatalf("want 2 allow audit entries, got %+v", al.Entries())
	}
}

func TestSession_PagesDoNotDriftUnderConcurrentWrites(t *testing.T) {
	t0 := at
	s, _ := seedMany(t, 10, t0)
	al := audit.New()
	svc := New(s, al, testRoles(), []byte("k"))

	// Freeze the session at t0.
	sess, rj := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 2, At: t0,
	})
	if rj != nil {
		t.Fatal(rj)
	}

	// Baseline: drain an identical second session opened at t0.
	sessBase, _ := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 2, At: t0,
	})
	base := collectAll(t, svc, sessBase.token)

	// Concurrent writers keep committing NEW patients and UPDATING existing
	// ones with strictly later arrival+business times while we page.
	stop := make(chan struct{})
	var wrote atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 100
		base := t0.Add(time.Hour)
		for {
			select {
			case <-stop:
				return
			default:
			}
			tick := base.Add(time.Duration(i) * time.Minute)
			mrn := "mrn-new-" + itoaTest(int64(i))
			cs := &normalize.ChangeSet{
				MessageID: "N-" + itoaTest(int64(i)), Format: "x", Version: "R4",
				Rank: normalize.VersionRanks["R4"], ReceivedAt: tick,
				Patient: &domain.Patient{
					MRN: mrn, FamilyName: "NEW", BirthDate: "1970-01-01",
					Gender: "male", Version: "R4", UpdatedAt: tick,
				},
			}
			s.Apply(cs, tick)
			wrote.Add(1)
			i++
			time.Sleep(time.Microsecond * 500)
		}
	}()

	// Wait until at least one concurrent commit has landed, THEN page: pages
	// must still reflect the frozen t0 view rather than the growing live store.
	deadline := time.Now().Add(2 * time.Second)
	for wrote.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if wrote.Load() == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("writer goroutine never committed before paging")
	}

	got := collectAll(t, svc, sess.token)
	close(stop)
	wg.Wait()

	if got != base {
		t.Fatalf("pages drifted during concurrent writes:\ngot  %s\nwant %s", got, base)
	}
	// the writer demonstrably committed concurrently before paging finished:
	// it records its own count
	if wrote.Load() < 1 {
		t.Fatal("writer goroutine never committed")
	}
	// the live store has grown while the frozen session had not
	if n := len(s.Snapshot().Patients); n <= 10 {
		t.Fatalf("expected concurrent growth, got %d patients", n)
	}
}

func collectAll(t *testing.T, svc *Service, token string) string {
	t.Helper()
	var sb strings.Builder
	pages := 0
	for {
		pg, err := svc.NextPage(token)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		sb.WriteString("P" + itoaTest(int64(pg.Index)) + ":")
		for _, p := range pg.Patients {
			sb.WriteString(p.MRN + ",")
		}
		sb.WriteString(";")
		if pg.Done {
			break
		}
	}
	return sb.String()
}

func TestSession_RosterRejectsUnknownAndOutOfScopePatients(t *testing.T) {
	t0 := at
	s, al := seedMany(t, 3, t0)
	svc := New(s, al,
		append(testRoles(), Role{
			Name:        "limited",
			Scopes:      map[string]bool{ScopePatientRead: true},
			AllowedMRNs: map[string]bool{mrnOf(0): true},
		}), []byte("k"))

	cases := map[string]struct {
		actor string
		mrns  []string
		want  domain.Status
	}{
		"nonexistent patient rejects whole request": {
			"analytics", []string{mrnOf(0), "mrn-ghost"}, domain.StatusRejectedPermission,
		},
		"out-of-scope patient rejects whole request": {
			"limited", []string{mrnOf(0), mrnOf(1)}, domain.StatusRejectedPermission,
		},
		"unknown actor": {
			"ghost", []string{mrnOf(0)}, domain.StatusRejectedPermission,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			logBuf := strings.Builder{}
			_ = logBuf
			sess, rj := svc.OpenSession(PageRequest{
				Actor: tc.actor, Purpose: PurposeNormal,
				PatientMRNs: tc.mrns, PageSize: 10, At: t0, Ref: "ref-" + name,
			})
			if sess != nil || rj == nil {
				t.Fatal("expected rejection")
			}
			if rj.Status != tc.want {
				t.Fatalf("status=%s want %s", rj.Status, tc.want)
			}
			// raw selectors must never appear in the error reason
			if strings.Contains(rj.Error(), "mrn-ghost") {
				t.Fatalf("denial echoed raw nonexistent selector: %v", rj)
			}
			// no session may be usable after refusal
			if p, _ := svc.NextPage("anything"); p != nil {
				t.Fatal("rejected open must not yield pages")
			}
		})
	}
	entries := al.Entries()
	for _, e := range entries {
		if e.Decision != "deny" || len(e.Resources) != 0 {
			t.Fatalf("denial must be resource-free, got %+v", e)
		}
	}
	if err := al.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestSession_ExplicitRosterAcceptsOnlyExactExisting(t *testing.T) {
	t0 := at
	s, _ := seedMany(t, 5, t0)
	al := audit.New()
	svc := New(s, al, testRoles(), []byte("k"))
	sess, rj := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal,
		PatientMRNs: []string{mrnOf(1), mrnOf(3)}, PageSize: 10, At: t0,
	})
	if rj != nil {
		t.Fatal(rj)
	}
	pg, err := svc.NextPage(sess.token)
	if err != nil {
		t.Fatal(err)
	}
	if !pg.Done || len(pg.Patients) != 2 {
		t.Fatalf("want exactly 2 selected patients on one page, got %d done=%v",
			len(pg.Patients), pg.Done)
	}
}

func TestSession_AsOfExcludesLaterConclusions(t *testing.T) {
	t0 := at
	s, _ := seedMany(t, 3, t0)
	al := audit.New()
	svc := New(s, al, testRoles(), []byte("k"))
	// open at t0: 3 patients
	sess, _ := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 10, At: t0,
	})
	pg, _ := svc.NextPage(sess.token)
	if len(pg.Patients) != 3 {
		t.Fatalf("want 3 at t0, got %d", len(pg.Patients))
	}
	// a patient arriving after t0 must not appear in the frozen session
	later := t0.Add(2 * time.Hour)
	s.Apply(&normalize.ChangeSet{
		MessageID: "LATE", Version: "R4", Rank: normalize.VersionRanks["R4"], ReceivedAt: later,
		Patient: &domain.Patient{
			MRN: "mrn-late", FamilyName: "LATE", BirthDate: "1970-01-01",
			Gender: "male", Version: "R4", UpdatedAt: later,
		},
	}, later)
	pg2, _ := svc.NextPage(sess.token)
	if !pg2.Done {
		t.Fatal("terminal page expected")
	}
	// a NEW session at t0 still sees 3; a session later sees 4
	sess2, _ := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 10, At: t0,
	})
	pg3, _ := svc.NextPage(sess2.token)
	if len(pg3.Patients) != 3 {
		t.Fatalf("pinned t0 session must still see 3, got %d", len(pg3.Patients))
	}
	sess3, _ := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 10, At: later,
	})
	pg4, _ := svc.NextPage(sess3.token)
	if len(pg4.Patients) != 4 {
		t.Fatalf("later session must see 4, got %d", len(pg4.Patients))
	}
}

func TestSession_ClosedAndUnknownTokens(t *testing.T) {
	t0 := at
	s, _ := seedMany(t, 1, t0)
	al := audit.New()
	svc := New(s, al, testRoles(), []byte("k"))
	sess, rj := svc.OpenSession(PageRequest{
		Actor: "analytics", Purpose: PurposeNormal, PageSize: 10, At: t0,
	})
	if rj != nil {
		t.Fatal(rj)
	}
	if _, err := svc.NextPage(sess.token); err != nil {
		t.Fatal(err)
	}
	svc.CloseSession(sess.token)
	if _, err := svc.NextPage(sess.token); err == nil {
		t.Fatal("closed token must be rejected")
	}
	if _, err := svc.NextPage("deadbeef"); err == nil {
		t.Fatal("unknown token must be rejected")
	}
}

func TestSession_EmptySelectionTerminalPage(t *testing.T) {
	t0 := at
	s, _ := seedMany(t, 1, t0)
	al := audit.New()
	svc := New(s, al, testRoles(), []byte("k"))
	sess, rj := svc.OpenSession(PageRequest{
		Actor: "lab-tech", Purpose: PurposeNormal, PageSize: 10, At: t0,
	})
	if rj != nil {
		t.Fatal(rj)
	}
	pg, err := svc.NextPage(sess.token)
	if err != nil {
		t.Fatal(err)
	}
	if !pg.Done || len(pg.Patients) != 0 {
		t.Fatalf("want one empty terminal page, got done=%v patients=%d", pg.Done, len(pg.Patients))
	}
}
