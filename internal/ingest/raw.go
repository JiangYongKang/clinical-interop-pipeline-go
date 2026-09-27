package ingest

import (
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

// RawDocument is the protocol-neutral intermediate shape produced by parsers.
// Pointers distinguish "field absent" from "field present but empty".
type RawDocument struct {
	MessageID    string
	Format       string // "hl7v2" | "fhir"
	Version      string
	SourceSystem string
	ReceivedAt   time.Time

	Patient      *RawPatient
	Encounter    *RawEncounter
	Observations []*RawObservation

	// updatedAtHint is MSH-7 (HL7) / Bundle.meta.lastUpdated (FHIR), used as
	// the entity UpdatedAt when the wire record carries no own timestamp.
	updatedAtHint time.Time
}

type RawPatient struct {
	MRN        *string
	FamilyName *string
	GivenName  *string
	BirthDate  *string
	Gender     *string
	UpdatedAt  time.Time
}

type RawEncounter struct {
	VisitID   *string
	MRN       *string
	Class     *string
	StartedAt *string
	EndedAt   *string
	UpdatedAt time.Time
}

type RawObservation struct {
	ObservationID *string
	MRN           *string
	VisitID       *string
	TestCode      *string
	TestName      *string
	ValueString   *string
	ValueNumber   *float64
	ValueType     string // "string" | "number"
	Unit          *string
	Status        *string
	EffectiveTime *string
	UpdatedAt     time.Time
}

// Parse parses one wire message into a RawDocument.
func Parse(format string, payload []byte, receivedAt time.Time) (*RawDocument, error) {
	switch format {
	case "hl7v2":
		return ParseHL7(payload, receivedAt)
	case "fhir":
		return ParseFHIR(payload, receivedAt)
	default:
		return nil, &domain.RejectError{
			Status: domain.StatusRejectedMalformed,
			Reason: "unsupported wire format",
		}
	}
}
