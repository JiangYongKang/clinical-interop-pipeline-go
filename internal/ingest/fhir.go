package ingest

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

// SupportedFHIRVersions is the allow-list of FHIR releases.
var SupportedFHIRVersions = map[string]bool{"R4": true}

// fhirBundle is the accepted FHIR-style, versioned envelope.
type fhirBundle struct {
	ResourceType string `json:"resourceType"`
	FHIRVersion  string `json:"fhirVersion"`
	Source       string `json:"source"`
	Identifier   *struct {
		Value string `json:"value"`
	} `json:"identifier"`
	Meta *struct {
		LastUpdated string `json:"lastUpdated"`
	} `json:"meta"`
	Entry []struct {
		Resource json.RawMessage `json:"resource"`
	} `json:"entry"`
}

type fhirMeta struct {
	VersionID   string `json:"versionId"`
	LastUpdated string `json:"lastUpdated"`
}

type fhirIdent struct {
	Value  string `json:"value"`
	System string `json:"system"`
}

type fhirRef struct {
	Reference  string     `json:"reference"`
	Identifier *fhirIdent `json:"identifier"`
}

// ParseFHIR parses a FHIR-style versioned JSON bundle (R4 supported).
func ParseFHIR(payload []byte, receivedAt time.Time) (*RawDocument, error) {
	var b fhirBundle
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	if err := dec.Decode(&b); err != nil {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid json bundle"}
	}
	if b.ResourceType != "Bundle" {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "top-level resource is not a Bundle"}
	}
	if b.FHIRVersion == "" {
		return nil, &domain.RejectError{Status: domain.StatusRejectedVersion, Reason: "missing fhirVersion"}
	}
	if !SupportedFHIRVersions[b.FHIRVersion] {
		return nil, &domain.RejectError{Status: domain.StatusRejectedVersion, Reason: "unsupported fhir version"}
	}
	if b.Identifier == nil || strings.TrimSpace(b.Identifier.Value) == "" {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "missing bundle message identifier"}
	}

	doc := &RawDocument{
		Format:       "fhir",
		Version:      b.FHIRVersion,
		MessageID:    strings.TrimSpace(b.Identifier.Value),
		SourceSystem: strings.TrimSpace(b.Source),
		ReceivedAt:   receivedAt,
	}
	if b.Meta != nil {
		if t, ok := parseFHIRInstant(b.Meta.LastUpdated); ok {
			doc.updatedAtHint = t
		}
	}

	// Index resources so Observation.encounter / subject references resolve.
	resByKey := map[string]map[string]*jsonRef{}
	for i, e := range b.Entry {
		if len(e.Resource) == 0 {
			return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "bundle entry without resource"}
		}
		var head struct {
			ResourceType string `json:"resourceType"`
			ID           string `json:"id"`
		}
		if err := json.Unmarshal(e.Resource, &head); err != nil {
			return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid entry resource json"}
		}
		rt := strings.TrimSpace(head.ResourceType)
		if rt != "Patient" && rt != "Encounter" && rt != "Observation" {
			return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unsupported entry resource type"}
		}
		if resByKey[rt] == nil {
			resByKey[rt] = map[string]*jsonRef{}
		}
		if head.ID != "" {
			resByKey[rt][head.ID] = &jsonRef{index: i, raw: e.Resource}
		}
	}

	seenPatient, seenEncounter := false, false
	seenObs := map[string]bool{}
	for _, e := range b.Entry {
		var head struct {
			ResourceType string `json:"resourceType"`
			ID           string `json:"id"`
		}
		_ = json.Unmarshal(e.Resource, &head)
		var err error
		switch head.ResourceType {
		case "Patient":
			if seenPatient {
				return nil, &domain.RejectError{Status: domain.StatusRejectedDuplicateID, Reason: "multiple Patient resources in bundle"}
			}
			seenPatient = true
			err = parseFHIRPatient(e.Resource, doc)
		case "Encounter":
			if seenEncounter {
				return nil, &domain.RejectError{Status: domain.StatusRejectedDuplicateID, Reason: "multiple Encounter resources in bundle"}
			}
			seenEncounter = true
			err = parseFHIREncounter(e.Resource, doc, resByKey)
		case "Observation":
			err = parseFHIRObservation(e.Resource, doc, resByKey, seenObs)
		}
		if err != nil {
			return nil, err
		}
	}
	return doc, nil
}

type jsonRef struct {
	index int
	raw   json.RawMessage
}

func resolveMRN(resByKey map[string]map[string]*jsonRef, ref fhirRef) string {
	// Cross-bundle references carry the business identifier directly.
	if ref.Identifier != nil && strings.TrimSpace(ref.Identifier.Value) != "" {
		return strings.TrimSpace(ref.Identifier.Value)
	}
	id := strings.TrimPrefix(ref.Reference, "Patient/")
	if r, ok := resByKey["Patient"][id]; ok {
		var p struct {
			Identifier []fhirIdent `json:"identifier"`
		}
		if err := json.Unmarshal(r.raw, &p); err == nil && len(p.Identifier) > 0 {
			return strings.TrimSpace(p.Identifier[0].Value)
		}
	}
	return ""
}

func resolveVisit(resByKey map[string]map[string]*jsonRef, ref fhirRef) string {
	id := strings.TrimPrefix(ref.Reference, "Encounter/")
	if r, ok := resByKey["Encounter"][id]; ok {
		var e struct {
			Identifier []fhirIdent `json:"identifier"`
		}
		if err := json.Unmarshal(r.raw, &e); err == nil && len(e.Identifier) > 0 {
			return strings.TrimSpace(e.Identifier[0].Value)
		}
		return id
	}
	return ""
}

func resourceUpdatedAt(meta *fhirMeta, doc *RawDocument) time.Time {
	if meta != nil {
		if t, ok := parseFHIRInstant(meta.LastUpdated); ok {
			return t
		}
	}
	if !doc.updatedAtHint.IsZero() {
		return doc.updatedAtHint
	}
	return doc.ReceivedAt
}

func parseFHIRPatient(raw json.RawMessage, doc *RawDocument) error {
	var p struct {
		ID         string      `json:"id"`
		Meta       *fhirMeta   `json:"meta"`
		Identifier []fhirIdent `json:"identifier"`
		Name       []struct {
			Family string   `json:"family"`
			Given  []string `json:"given"`
		} `json:"name"`
		BirthDate string `json:"birthDate"`
		Gender    string `json:"gender"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Patient resource"}
	}
	rp := &RawPatient{UpdatedAt: resourceUpdatedAt(p.Meta, doc)}
	if len(p.Identifier) > 0 && strings.TrimSpace(p.Identifier[0].Value) != "" {
		mrn := strings.TrimSpace(p.Identifier[0].Value)
		rp.MRN = &mrn
	}
	if len(p.Name) > 0 {
		fam := strings.TrimSpace(p.Name[0].Family)
		if fam != "" {
			rp.FamilyName = &fam
		}
		if len(p.Name[0].Given) > 0 {
			given := strings.TrimSpace(p.Name[0].Given[0])
			if given != "" {
				rp.GivenName = &given
			}
		}
	}
	if p.BirthDate != "" {
		if _, err := time.Parse("2006-01-02", p.BirthDate); err != nil {
			return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Patient.birthDate"}
		}
		d := p.BirthDate
		rp.BirthDate = &d
	}
	if p.Gender != "" {
		g := p.Gender
		rp.Gender = &g
	}
	doc.Patient = rp
	return nil
}

func parseFHIREncounter(raw json.RawMessage, doc *RawDocument, resByKey map[string]map[string]*jsonRef) error {
	var e struct {
		ID         string      `json:"id"`
		Meta       *fhirMeta   `json:"meta"`
		Identifier []fhirIdent `json:"identifier"`
		Status     string      `json:"status"`
		Class      *struct {
			Code string `json:"code"`
		} `json:"class"`
		Subject *fhirRef `json:"subject"`
		Period  *struct {
			Start string `json:"start"`
			End   string `json:"end"`
		} `json:"period"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Encounter resource"}
	}
	re := &RawEncounter{UpdatedAt: resourceUpdatedAt(e.Meta, doc)}
	vid := e.ID
	if len(e.Identifier) > 0 && strings.TrimSpace(e.Identifier[0].Value) != "" {
		vid = strings.TrimSpace(e.Identifier[0].Value)
	}
	if vid != "" {
		re.VisitID = &vid
	}
	mrn := ""
	if e.Subject != nil {
		mrn = resolveMRN(resByKey, *e.Subject)
	}
	if mrn == "" && doc.Patient != nil && doc.Patient.MRN != nil {
		mrn = *doc.Patient.MRN
	}
	if mrn != "" {
		re.MRN = &mrn
	}
	if e.Class != nil && strings.TrimSpace(e.Class.Code) != "" {
		c := strings.TrimSpace(e.Class.Code)
		re.Class = &c
	}
	if e.Period != nil {
		if e.Period.Start != "" {
			s, err := normalizeInstantOrDate(e.Period.Start)
			if err != nil {
				return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Encounter.period.start"}
			}
			re.StartedAt = &s
		}
		if e.Period.End != "" {
			s, err := normalizeInstantOrDate(e.Period.End)
			if err != nil {
				return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Encounter.period.end"}
			}
			re.EndedAt = &s
		}
	}
	doc.Encounter = re
	return nil
}

func parseFHIRObservation(raw json.RawMessage, doc *RawDocument, resByKey map[string]map[string]*jsonRef, seen map[string]bool) error {
	var o struct {
		ID         string      `json:"id"`
		Meta       *fhirMeta   `json:"meta"`
		Identifier []fhirIdent `json:"identifier"`
		Status     string      `json:"status"`
		Code       *struct {
			Coding []struct {
				Code    string `json:"code"`
				Display string `json:"display"`
			} `json:"coding"`
			Text string `json:"text"`
		} `json:"code"`
		Subject           *fhirRef `json:"subject"`
		Encounter         *fhirRef `json:"encounter"`
		EffectiveDateTime string   `json:"effectiveDateTime"`
		ValueString       *string  `json:"valueString"`
		ValueQuantity     *struct {
			Value *float64 `json:"value"`
			Unit  string   `json:"unit"`
		} `json:"valueQuantity"`
		ValueCodeableConcept *struct {
			Text   string `json:"text"`
			Coding []struct {
				Code string `json:"code"`
			} `json:"coding"`
		} `json:"valueCodeableConcept"`
	}
	// Detect unsupported value[x] variants as real type conflicts instead of
	// silently dropping them.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Observation resource"}
	}
	for k := range probe {
		if strings.HasPrefix(k, "value") && k != "valueString" && k != "valueQuantity" && k != "valueCodeableConcept" {
			return &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "unsupported Observation value type"}
		}
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Observation resource"}
	}

	ro := &RawObservation{UpdatedAt: resourceUpdatedAt(o.Meta, doc)}
	id := o.ID
	if len(o.Identifier) > 0 && strings.TrimSpace(o.Identifier[0].Value) != "" {
		id = strings.TrimSpace(o.Identifier[0].Value)
	}
	if id == "" {
		return &domain.RejectError{Status: domain.StatusRejectedMissingField, Reason: "observation stable id missing"}
	}
	if seen[id] {
		return &domain.RejectError{Status: domain.StatusRejectedDuplicateID, Reason: "duplicated observation id in bundle"}
	}
	seen[id] = true
	oid := id
	ro.ObservationID = &oid

	if o.Code != nil {
		code, name := "", o.Code.Text
		if len(o.Code.Coding) > 0 {
			code = strings.TrimSpace(o.Code.Coding[0].Code)
			if name == "" {
				name = strings.TrimSpace(o.Code.Coding[0].Display)
			}
		}
		if code != "" {
			ro.TestCode = &code
		}
		if name != "" {
			ro.TestName = &name
		}
	}

	valueKinds := 0
	if o.ValueString != nil {
		valueKinds++
		s := *o.ValueString
		ro.ValueType, ro.ValueString = "string", &s
	}
	if o.ValueQuantity != nil {
		valueKinds++
		ro.ValueType = "number"
		if o.ValueQuantity.Value != nil {
			n := *o.ValueQuantity.Value
			ro.ValueNumber = &n
		}
		if u := strings.TrimSpace(o.ValueQuantity.Unit); u != "" {
			ro.Unit = &u
		}
	}
	if o.ValueCodeableConcept != nil {
		valueKinds++
		s := o.ValueCodeableConcept.Text
		if s == "" && len(o.ValueCodeableConcept.Coding) > 0 {
			s = strings.TrimSpace(o.ValueCodeableConcept.Coding[0].Code)
		}
		ro.ValueType = "string"
		ro.ValueString = &s
	}
	if valueKinds > 1 {
		return &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "multiple value[x] present"}
	}

	if o.Status != "" {
		s := o.Status
		ro.Status = &s
	}
	if o.EffectiveDateTime != "" {
		s, err := normalizeInstantOrDate(o.EffectiveDateTime)
		if err != nil {
			return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "invalid Observation.effectiveDateTime"}
		}
		ro.EffectiveTime = &s
	}
	if o.Subject != nil {
		if mrn := resolveMRN(resByKey, *o.Subject); mrn != "" {
			ro.MRN = &mrn
		}
	}
	if ro.MRN == nil && doc.Patient != nil && doc.Patient.MRN != nil {
		mrn := *doc.Patient.MRN
		ro.MRN = &mrn
	}
	if o.Encounter != nil {
		if vid := resolveVisit(resByKey, *o.Encounter); vid != "" {
			ro.VisitID = &vid
		}
	}
	doc.Observations = append(doc.Observations, ro)
	return nil
}

// parseFHIRInstant parses an RFC3339 instant and reports it in UTC.
func parseFHIRInstant(v string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(v))
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// normalizeInstantOrDate accepts an RFC3339 instant or a YYYY-MM-DD date and
// returns a canonical string (instant -> UTC RFC3339, date -> YYYY-MM-DD).
func normalizeInstantOrDate(v string) (string, error) {
	v = strings.TrimSpace(v)
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC().Format(time.RFC3339), nil
	}
	if d, err := time.Parse("2006-01-02", v); err == nil {
		return d.Format("2006-01-02"), nil
	}
	return "", &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unparsable date or instant"}
}
