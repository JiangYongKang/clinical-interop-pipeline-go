package normalize

import (
	"fmt"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
)

// VersionRanks orders every supported wire version on one scale. A higher
// rank means a newer wire format; it never makes an older conclusion win.
var VersionRanks = map[string]domain.VersionRank{
	"2.3.1": 10, // HL7 v2
	"2.5.1": 20,
	"R4":    30, // FHIR
}

// ChangeSet is one normalized message: identities + canonical entities.
type ChangeSet struct {
	MessageID    string
	Format       string
	Version      string
	Rank         domain.VersionRank
	SourceSystem string
	ReceivedAt   time.Time

	Patient      *domain.Patient
	Encounter    *domain.Encounter
	Observations []*domain.Observation
}

// Normalize converts a raw parsed document into canonical entities and
// performs field-level validation. It rejects (never silently ignores):
//   - missing required identity/value fields  -> rejected_missing_field
//   - duplicated stable ids inside the message -> rejected_duplicate_id
//
// Cross-message value-type conflicts are detected later in the store, where
// the prior value is available.
func Normalize(doc *ingest.RawDocument) (*ChangeSet, error) {
	if doc == nil {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "empty document"}
	}
	rank, ok := VersionRanks[doc.Version]
	if !ok {
		return nil, &domain.RejectError{Status: domain.StatusRejectedVersion, Reason: "unknown version rank: " + doc.Version}
	}
	cs := &ChangeSet{
		MessageID:    doc.MessageID,
		Format:       doc.Format,
		Version:      doc.Version,
		Rank:         rank,
		SourceSystem: doc.SourceSystem,
		ReceivedAt:   doc.ReceivedAt,
	}

	if doc.Patient != nil {
		p, err := normalizePatient(doc, doc.Patient)
		if err != nil {
			return nil, err
		}
		cs.Patient = p
	}
	if doc.Encounter != nil {
		e, err := normalizeEncounter(doc, doc.Encounter)
		if err != nil {
			return nil, err
		}
		cs.Encounter = e
	}

	seenObs := map[string]bool{}
	seenVisit := ""
	for _, ro := range doc.Observations {
		o, err := normalizeObservation(doc, ro)
		if err != nil {
			return nil, err
		}
		if seenObs[o.ObservationID] {
			return nil, &domain.RejectError{
				Status: domain.StatusRejectedDuplicateID,
				Reason: "duplicated observation id within message",
			}
		}
		seenObs[o.ObservationID] = true
		if cs.Encounter == nil && o.VisitID != "" {
			if seenVisit != "" && seenVisit != o.VisitID {
				return nil, &domain.RejectError{
					Status: domain.StatusRejectedDuplicateID,
					Reason: "observations reference multiple visits without PV1",
				}
			}
			seenVisit = o.VisitID
		}
		cs.Observations = append(cs.Observations, o)
	}
	return cs, nil
}

// requireString returns the pointed-to non-blank value or a reject error.
func requireString(v *string, field string) (string, error) {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "", &domain.RejectError{
			Status: domain.StatusRejectedMissingField,
			Reason: "required field missing: " + field,
		}
	}
	return strings.TrimSpace(*v), nil
}

func normalizePatient(doc *ingest.RawDocument, rp *ingest.RawPatient) (*domain.Patient, error) {
	mrn, err := requireString(rp.MRN, "patient.mrn")
	if err != nil {
		return nil, err
	}
	family, err := requireString(rp.FamilyName, "patient.family_name")
	if err != nil {
		return nil, err
	}
	birth, err := requireString(rp.BirthDate, "patient.birth_date")
	if err != nil {
		return nil, err
	}
	gender, err := requireString(rp.Gender, "patient.gender")
	if err != nil {
		return nil, err
	}
	p := &domain.Patient{
		MRN:          mrn,
		FamilyName:   family,
		GivenName:    deref(rp.GivenName),
		BirthDate:    birth,
		Gender:       canonicalGender(gender),
		SourceSystem: doc.SourceSystem,
		Version:      doc.Version,
		UpdatedAt:    nonZero(rp.UpdatedAt, doc.ReceivedAt),
	}
	return p, nil
}

func normalizeEncounter(doc *ingest.RawDocument, re *ingest.RawEncounter) (*domain.Encounter, error) {
	visit, err := requireString(re.VisitID, "encounter.visit_id")
	if err != nil {
		return nil, err
	}
	mrn, err := requireString(re.MRN, "encounter.mrn")
	if err != nil {
		return nil, err
	}
	class, err := requireString(re.Class, "encounter.class")
	if err != nil {
		return nil, err
	}
	e := &domain.Encounter{
		VisitID:      visit,
		MRN:          mrn,
		Class:        canonicalClass(class),
		StartedAt:    deref(re.StartedAt),
		EndedAt:      deref(re.EndedAt),
		SourceSystem: doc.SourceSystem,
		Version:      doc.Version,
		UpdatedAt:    nonZero(re.UpdatedAt, doc.ReceivedAt),
	}
	return e, nil
}

func normalizeObservation(doc *ingest.RawDocument, ro *ingest.RawObservation) (*domain.Observation, error) {
	id, err := requireString(ro.ObservationID, "observation.id")
	if err != nil {
		return nil, err
	}
	mrn, err := requireString(ro.MRN, "observation.mrn")
	if err != nil {
		return nil, err
	}
	code, err := requireString(ro.TestCode, "observation.test_code")
	if err != nil {
		return nil, err
	}
	status, err := requireString(ro.Status, "observation.status")
	if err != nil {
		return nil, err
	}
	// Exactly one value must be present and must agree with its declared type.
	hasNum := ro.ValueNumber != nil
	hasStr := ro.ValueString != nil && strings.TrimSpace(*ro.ValueString) != ""
	if hasNum && hasStr {
		return nil, &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "observation carries both string and numeric value"}
	}
	if !hasNum && !hasStr {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMissingField, Reason: "required field missing: observation.value"}
	}
	if hasNum && ro.ValueType == "string" {
		return nil, &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "numeric value declared as string type"}
	}
	if hasStr && ro.ValueType == "number" {
		return nil, &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "string value declared as numeric type"}
	}

	o := &domain.Observation{
		ObservationID: id,
		MRN:           mrn,
		VisitID:       deref(ro.VisitID),
		TestCode:      code,
		TestName:      deref(ro.TestName),
		ValueString:   deref(ro.ValueString),
		ValueNumber:   ro.ValueNumber,
		Unit:          deref(ro.Unit),
		Status:        canonicalObsStatus(status),
		EffectiveTime: deref(ro.EffectiveTime),
		SourceSystem:  doc.SourceSystem,
		Version:       doc.Version,
		UpdatedAt:     nonZero(ro.UpdatedAt, doc.ReceivedAt),
	}
	return o, nil
}

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func nonZero(t, fallback time.Time) time.Time {
	if !t.IsZero() {
		return t.UTC()
	}
	return fallback.UTC()
}

// canonicalGender maps HL7 v2 administrative-sex and FHIR administrative-gender
// onto one vocabulary: male | female | other | unknown.
func canonicalGender(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "m", "male":
		return "male"
	case "f", "female":
		return "female"
	case "o", "a", "other":
		return "other"
	case "u", "unk", "unknown":
		return "unknown"
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

// canonicalClass maps HL7 v2 patient-class and FHIR ActEncounterCode onto:
// IMP (inpatient) | AMB (ambulatory) | EMER (emergency) | OBS (observation).
func canonicalClass(v string) string {
	c := strings.ToUpper(strings.TrimSpace(v))
	switch c {
	case "I":
		return "IMP"
	case "O", "P":
		return "AMB"
	case "E":
		return "EMER"
	case "R":
		return "OBS"
	default:
		return c
	}
}

// canonicalObsStatus maps HL7 OBX-11 result status and FHIR Observation.status
// onto: registered | preliminary | final | amended | cancelled | entered-in-error.
func canonicalObsStatus(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "f":
		return "final"
	case "p", "r", "preliminary":
		return "preliminary"
	case "c", "amended", "corrected":
		return "amended"
	case "x", "cancelled":
		return "cancelled"
	case "d", "entered-in-error":
		return "entered-in-error"
	case "o", "registered":
		return "registered"
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

// Digest renders a stable identity for log/basis messages.
func (cs *ChangeSet) String() string {
	return fmt.Sprintf("%s/%s@%s", cs.Format, cs.MessageID, cs.Version)
}
