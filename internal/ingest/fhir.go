package ingest

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

// 支持的 FHIR 版本：R4(4.0.x)。
func supportedFHIRVersion(v string) bool {
	return v == "R4" || strings.HasPrefix(v, "4.0")
}

// bundle 为 FHIR 风格 Bundle 的最小子集（含本地扩展头字段）。
type bundle struct {
	ID           string          `json:"id"`
	FHIRVersion  string          `json:"fhirVersion"`
	Source       string          `json:"x-source"`
	Sequence     json.RawMessage `json:"x-sequence"`
	Entry        []bundleEntry   `json:"entry"`
}

type bundleEntry struct {
	Resource json.RawMessage `json:"resource"`
}

type resourceHeader struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id"`
}

type fhirPatient struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id"`
	Identifier   []struct {
		Value string `json:"value"`
	} `json:"identifier"`
	Name []struct {
		Family string   `json:"family"`
		Given  []string `json:"given"`
	} `json:"name"`
	Gender    string `json:"gender"`
	BirthDate string `json:"birthDate"`
}

type fhirEncounter struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id"`
	Status       string `json:"status"`
	Class        struct {
		Code string `json:"code"`
	} `json:"class"`
	Subject struct {
		Reference string `json:"reference"`
	} `json:"subject"`
	Period struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"period"`
}

type fhirObservation struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id"`
	Status       string `json:"status"`
	Code         struct {
		Coding []struct {
			Code string `json:"code"`
			Name string `json:"display"`
		} `json:"coding"`
	} `json:"code"`
	Subject struct {
		Reference string `json:"reference"`
	} `json:"subject"`
	Encounter struct {
		Reference string `json:"reference"`
	} `json:"encounter"`
	EffectiveDateTime string `json:"effectiveDateTime"`
	ValueQuantity     *struct {
		Value    json.RawMessage `json:"value"`
		Unit     string          `json:"unit"`
		Code     string          `json:"code"`
	} `json:"valueQuantity"`
	ValueString string `json:"valueString"`
}

func parseFHIR(raw []byte) (*model.Update, error) {
	var b bundle
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&b); err != nil {
		return nil, newError(ReasonInvalidEncoding, "body", "invalid JSON")
	}
	if b.ID == "" {
		return nil, newError(ReasonMissingField, "id", "bundle id required for idempotency")
	}
	if !supportedFHIRVersion(b.FHIRVersion) {
		log.Printf("ingest fhir message_id=%s rejected reason=%s field=fhirVersion", b.ID, ReasonUnsupportedVersion)
		return nil, newError(ReasonUnsupportedVersion, "fhirVersion", "only R4/4.0.x is supported")
	}
	if b.Source == "" {
		return nil, newError(ReasonMissingField, "x-source", "source code required")
	}
	if len(b.Sequence) == 0 {
		return nil, newError(ReasonMissingField, "x-sequence", "monotonic sequence required")
	}
	seq, err := parseJSONInt64(b.Sequence)
	if err != nil {
		return nil, newError(ReasonTypeConflict, "x-sequence", "sequence must be an integer")
	}

	upd := &model.Update{
		MessageID:     b.ID,
		SourceKind:    model.SourceFHIR,
		SourceVersion: b.FHIRVersion,
		Sequence:      seq,
	}

	// 先建索引：bundle 内资源 id -> 稳定实体 id，供引用解析。
	patientIndex := map[string]string{}
	encounterIndex := map[string]string{}

	for _, e := range b.Entry {
		var h resourceHeader
		if err := json.Unmarshal(e.Resource, &h); err != nil {
			return nil, newError(ReasonInvalidEncoding, "entry.resource", "invalid resource")
		}
		switch h.ResourceType {
		case "Patient":
			p, err := parseFHIRPatient(e.Resource, b.FHIRVersion, seq)
			if err != nil {
				return nil, err
			}
			if _, dup := patientIndex[h.ID]; dup {
				return nil, newError(ReasonDuplicate, "entry", "duplicate patient resource id in bundle")
			}
			patientIndex[h.ID] = p.ID
			upd.Patients = append(upd.Patients, p)
		case "Encounter":
			ec, err := parseFHIREncounter(e.Resource, b.Source, b.FHIRVersion, seq)
			if err != nil {
				return nil, err
			}
			encounterIndex[h.ID] = ec.ID
			upd.Encounters = append(upd.Encounters, ec)
		case "Observation":
			ob, err := parseFHIRObservation(e.Resource, b.Source, b.FHIRVersion, seq)
			if err != nil {
				return nil, err
			}
			upd.Observations = append(upd.Observations, ob)
		default:
			return nil, newError(ReasonTypeConflict, "entry.resourceType", "unsupported resource type: "+h.ResourceType)
		}
	}

	// 解析引用。
	for i := range upd.Encounters {
		enc := &upd.Encounters[i]
		ref := ""
		for _, e := range b.Entry {
			var h resourceHeader
			_ = json.Unmarshal(e.Resource, &h)
			if h.ResourceType == "Encounter" && "ENC:"+b.Source+":"+h.ID == enc.ID {
				var fe fhirEncounter
				_ = json.Unmarshal(e.Resource, &fe)
				ref = fe.Subject.Reference
			}
		}
		pid, ok := patientIndex[strings.TrimPrefix(ref, "Patient/")]
		if !ok {
			return nil, newError(ReasonMissingField, "Encounter.subject", "subject reference must resolve to a Patient in the bundle")
		}
		enc.PatientID = pid
	}

	for i := range upd.Observations {
		ob := &upd.Observations[i]
		var fo fhirObservation
		for _, e := range b.Entry {
			var h resourceHeader
			_ = json.Unmarshal(e.Resource, &h)
			if h.ResourceType == "Observation" && ob.ID == "OBS:"+b.Source+":"+h.ID {
				_ = json.Unmarshal(e.Resource, &fo)
			}
		}
		pid, ok := patientIndex[strings.TrimPrefix(fo.Subject.Reference, "Patient/")]
		if !ok {
			return nil, newError(ReasonMissingField, "Observation.subject", "subject reference must resolve to a Patient in the bundle")
		}
		ob.PatientID = pid
		if fo.Encounter.Reference != "" {
			eid, ok := encounterIndex[strings.TrimPrefix(fo.Encounter.Reference, "Encounter/")]
			if !ok {
				return nil, newError(ReasonMissingField, "Observation.encounter", "encounter reference must resolve in the bundle")
			}
			ob.EncounterID = eid
		}
	}

	if len(upd.Patients) == 0 {
		return nil, newError(ReasonMissingField, "entry", "at least one Patient required")
	}

	log.Printf("ingest fhir message_id=%s version=%s seq=%d entities=%d status=parsed",
		b.ID, b.FHIRVersion, seq, len(upd.Patients)+len(upd.Encounters)+len(upd.Observations))
	return upd, nil
}

func parseJSONInt64(raw json.RawMessage) (int64, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0, errMissing
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// 拒绝浮点形式，保证序号语义明确。
		return 0, errMissing
	}
	return n, nil
}

var errMissing = &simpleError{"integer required"}

type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }

func parseFHIRPatient(raw json.RawMessage, version string, seq int64) (model.Patient, error) {
	var p fhirPatient
	if err := json.Unmarshal(raw, &p); err != nil {
		return model.Patient{}, newError(ReasonInvalidEncoding, "Patient", "invalid patient resource")
	}
	if len(p.Identifier) == 0 || p.Identifier[0].Value == "" {
		return model.Patient{}, newError(ReasonMissingField, "Patient.identifier[0].value", "MRN required")
	}
	if p.BirthDate == "" {
		return model.Patient{}, newError(ReasonMissingField, "Patient.birthDate", "birthDate required")
	}
	if _, err := time.Parse("2006-01-02", p.BirthDate); err != nil {
		return model.Patient{}, newError(ReasonTypeConflict, "Patient.birthDate", "must be YYYY-MM-DD")
	}
	switch p.Gender {
	case "male", "female", "other", "unknown":
	default:
		return model.Patient{}, newError(ReasonTypeConflict, "Patient.gender", "must be male/female/other/unknown")
	}
	name := ""
	if len(p.Name) > 0 {
		name = strings.TrimSpace(strings.Join(p.Name[0].Given, " ") + " " + p.Name[0].Family)
	}
	mrn := p.Identifier[0].Value
	return model.Patient{
		ID:            stablePatientID(mrn),
		MRN:           mrn,
		Name:          name,
		BirthDate:     p.BirthDate,
		Gender:        p.Gender,
		SourceKind:    model.SourceFHIR,
		SourceVersion: version,
		Sequence:      seq,
		UpdatedAt:     time.Now().UTC(),
	}, nil
}

func parseFHIREncounter(raw json.RawMessage, source, version string, seq int64) (model.Encounter, error) {
	var fe fhirEncounter
	if err := json.Unmarshal(raw, &fe); err != nil {
		return model.Encounter{}, newError(ReasonInvalidEncoding, "Encounter", "invalid encounter resource")
	}
	if fe.ID == "" {
		return model.Encounter{}, newError(ReasonMissingField, "Encounter.id", "id required")
	}
	switch fe.Status {
	case "planned", "in-progress", "finished", "cancelled":
	default:
		return model.Encounter{}, newError(ReasonTypeConflict, "Encounter.status", "invalid status code")
	}
	start, err := time.Parse(time.RFC3339, fe.Period.Start)
	if err != nil {
		return model.Encounter{}, newError(ReasonTypeConflict, "Encounter.period.start", "must be RFC3339")
	}
	ec := model.Encounter{
		ID:         "ENC:" + source + ":" + fe.ID,
		Status:     fe.Status,
		Class:      normalizeClassCode(fe.Class.Code),
		Start:      start.UTC(),
		SourceKind: model.SourceFHIR,
		Sequence:   seq,
		UpdatedAt:  time.Now().UTC(),
	}
	if fe.Period.End != "" {
		end, err := time.Parse(time.RFC3339, fe.Period.End)
		if err != nil {
			return model.Encounter{}, newError(ReasonTypeConflict, "Encounter.period.end", "must be RFC3339")
		}
		ec.End = end.UTC()
	}
	return ec, nil
}

func parseFHIRObservation(raw json.RawMessage, source, version string, seq int64) (model.Observation, error) {
	var fo fhirObservation
	if err := json.Unmarshal(raw, &fo); err != nil {
		return model.Observation{}, newError(ReasonInvalidEncoding, "Observation", "invalid observation resource")
	}
	if fo.ID == "" {
		return model.Observation{}, newError(ReasonMissingField, "Observation.id", "id required")
	}
	switch fo.Status {
	case "final", "preliminary", "corrected", "amended":
	case "registered":
	default:
		return model.Observation{}, newError(ReasonTypeConflict, "Observation.status", "invalid status code")
	}
	if len(fo.Code.Coding) == 0 || fo.Code.Coding[0].Code == "" {
		return model.Observation{}, newError(ReasonMissingField, "Observation.code.coding[0].code", "code required")
	}
	if fo.EffectiveDateTime == "" {
		return model.Observation{}, newError(ReasonMissingField, "Observation.effectiveDateTime", "effective time required")
	}
	obsTime, err := time.Parse(time.RFC3339, fo.EffectiveDateTime)
	if err != nil {
		return model.Observation{}, newError(ReasonTypeConflict, "Observation.effectiveDateTime", "must be RFC3339")
	}

	value, unit := "", ""
	switch {
	case fo.ValueQuantity != nil:
		s := strings.TrimSpace(string(fo.ValueQuantity.Value))
		if s == "" || s == "null" {
			return model.Observation{}, newError(ReasonMissingField, "Observation.valueQuantity.value", "value required")
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return model.Observation{}, newError(ReasonTypeConflict, "Observation.valueQuantity.value", "must be numeric")
		}
		value, unit = s, fo.ValueQuantity.Unit
	case fo.ValueString != "":
		value = fo.ValueString
	default:
		return model.Observation{}, newError(ReasonMissingField, "Observation.value[x]", "valueQuantity or valueString required")
	}

	status := fo.Status
	if status == "amended" || status == "registered" {
		if status == "amended" {
			status = "corrected"
		} else {
			status = "preliminary"
		}
	}

	return model.Observation{
		ID:         "OBS:" + source + ":" + fo.ID,
		Code:       fo.Code.Coding[0].Code,
		Name:       fo.Code.Coding[0].Name,
		Value:      value,
		Unit:       unit,
		Status:     status,
		ObservedAt: obsTime.UTC(),
		SourceKind: model.SourceFHIR,
		Sequence:   seq,
		UpdatedAt:  time.Now().UTC(),
	}, nil
}

func normalizeClassCode(c string) string {
	switch strings.ToUpper(strings.TrimSpace(c)) {
	case "IMP", "INPATIENT":
		return "inpatient"
	case "AMB", "OUTPATIENT":
		return "outpatient"
	case "EMER", "EMERGENCY":
		return "emergency"
	default:
		return "outpatient"
	}
}
