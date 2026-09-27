package domain

import "time"

// Status is the final, machine-readable disposition of one ingested message.
type Status string

const (
	StatusAccepted             Status = "accepted"               // new entities written
	StatusUpdated              Status = "updated"                // existing entities updated
	StatusDeduplicated         Status = "deduplicated"           // same message id + identical content
	StatusConflict             Status = "conflict"               // same message id, different content
	StatusStale                Status = "stale"                  // older conclusion, cannot overwrite
	StatusRejectedMalformed    Status = "rejected_malformed"     // unparsable wire message
	StatusRejectedMessageType  Status = "rejected_message_type"  // unsupported message/event type
	StatusRejectedMissingField Status = "rejected_missing_field" // required field absent/empty
	StatusRejectedTypeConflict Status = "rejected_type_conflict" // value type disagreement for same id
	StatusRejectedVersion      Status = "rejected_version"       // unsupported wire version
	StatusRejectedDuplicateID  Status = "rejected_duplicate_id"  // duplicated stable id inside one batch
	StatusRejectedPermission   Status = "rejected_permission"    // caller not allowed
	StatusRejectedInvalidMask  Status = "rejected_invalid_mask"  // illegal masking combination
)

// RejectError is the ONLY error shape returned to callers. It never carries
// raw payload fields; Ref is a caller-supplied correlation identifier.
type RejectError struct {
	Status Status
	Ref    string
	Reason string
}

func (e *RejectError) Error() string {
	if e.Ref != "" {
		return string(e.Status) + ": ref=" + e.Ref + ": " + e.Reason
	}
	return string(e.Status) + ": " + e.Reason
}

// VersionRank orders supported wire versions; higher rank never overwrites
// a conclusion already reached at a lower rank with a newer timestamp.
type VersionRank int

// Patient is the canonical patient model.
type Patient struct {
	MRN          string    `json:"mrn"`
	FamilyName   string    `json:"family_name"`
	GivenName    string    `json:"given_name"`
	BirthDate    string    `json:"birth_date"` // YYYY-MM-DD
	Gender       string    `json:"gender"`
	SourceSystem string    `json:"source_system,omitempty"`
	Version      string    `json:"version,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Encounter is the canonical encounter/visit model.
type Encounter struct {
	VisitID      string    `json:"visit_id"`
	MRN          string    `json:"mrn"`
	Class        string    `json:"class"`
	StartedAt    string    `json:"started_at"` // RFC3339 or YYYY-MM-DD
	EndedAt      string    `json:"ended_at"`
	SourceSystem string    `json:"source_system,omitempty"`
	Version      string    `json:"version,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Observation is the canonical lab result model.
type Observation struct {
	ObservationID string    `json:"observation_id"`
	MRN           string    `json:"mrn"`
	VisitID       string    `json:"visit_id"`
	TestCode      string    `json:"test_code"`
	TestName      string    `json:"test_name"`
	ValueString   string    `json:"value_string"`
	ValueNumber   *float64  `json:"value_number"`
	Unit          string    `json:"unit"`
	Status        string    `json:"status"`
	EffectiveTime string    `json:"effective_time"` // RFC3339 or YYYY-MM-DD
	SourceSystem  string    `json:"source_system,omitempty"`
	Version       string    `json:"version,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}
