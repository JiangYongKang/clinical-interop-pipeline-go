package export

import (
	"strconv"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

func itoaPkg(i int) string { return strconv.Itoa(i) }

// seedManyTyped creates n patients with an encounter and an observation each,
// committed at staggered recorded times.
func seedManyTyped(t *testing.T, n int) (*store.Store, *audit.Log) {
	t.Helper()
	s := store.New()
	for i := 0; i < n; i++ {
		mrn := "mrn-" + strconv.Itoa(i)
		rec := at.Add(time.Duration(i) * time.Minute)
		cs := &normalize.ChangeSet{
			MessageID: "M" + strconv.Itoa(i), Version: "R4",
			Rank: normalize.VersionRanks["R4"], ReceivedAt: rec,
			Patient: &domain.Patient{
				MRN: mrn, FamilyName: "FAMILY" + strconv.Itoa(i), GivenName: "G",
				BirthDate: "1980-01-01", Gender: "female", Version: "R4", UpdatedAt: rec,
			},
			Encounter: &domain.Encounter{
				VisitID: "visit-" + strconv.Itoa(i), MRN: mrn, Class: "IMP",
				Version: "R4", UpdatedAt: rec,
			},
			Observations: []*domain.Observation{{
				ObservationID: "obs-" + strconv.Itoa(i), MRN: mrn,
				VisitID: "visit-" + strconv.Itoa(i), TestCode: "GLU", Status: "final",
				ValueString: "5", Version: "R4", UpdatedAt: rec,
			}},
		}
		if o := s.Apply(cs, rec); o.Status != domain.StatusAccepted {
			t.Fatalf("seed %d failed: %s %s", i, o.Status, o.Reason)
		}
	}
	return s, audit.New()
}
