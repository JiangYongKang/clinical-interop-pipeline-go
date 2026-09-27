package ingest

import (
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
)

// SupportedHL7Versions is the allow-list of HL7 v2 versions.
var SupportedHL7Versions = map[string]bool{
	"2.3.1": true,
	"2.5.1": true,
}

// supportedHL7MessageTypes is the allow-list of MSH-9 event types.
var supportedHL7MessageTypes = map[string]bool{
	"ADT": true, // patient/visit administration
	"ORU": true, // observation result (lab)
}

// ParseHL7 parses an HL7 v2 ER7 message (2.3.1 / 2.5.1 supported).
//
// Segment grammar: MSH|... uses '|' as field separator where the leading
// delimiter is itself field 1, so MSH-3 starts at token index 2.
// All failures are reported with typed *domain.RejectError values and never
// include raw segment contents.
func ParseHL7(payload []byte, receivedAt time.Time) (*RawDocument, error) {
	text := strings.ReplaceAll(string(payload), "\r\n", "\r")
	text = strings.ReplaceAll(text, "\n", "\r")
	segments := strings.Split(text, "\r")

	doc := &RawDocument{Format: "hl7v2", ReceivedAt: receivedAt}
	var pid, pv1, obr []string
	var obx [][]string

	foundMSH := false
	for _, seg := range segments {
		if strings.TrimSpace(seg) == "" {
			continue
		}
		fields := strings.Split(seg, "|")
		switch fields[0] {
		case "MSH":
			if foundMSH {
				return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "multiple MSH segments"}
			}
			foundMSH = true
			if err := parseMSH(fields, doc); err != nil {
				return nil, err
			}
		case "PID":
			pid = fields
		case "PV1":
			pv1 = fields
		case "OBR":
			obr = fields
		case "OBX":
			obx = append(obx, fields)
		default:
			// unknown segments are ignored, per HL7 convention
		}
	}
	if !foundMSH {
		return nil, &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "missing MSH segment"}
	}

	if pid != nil {
		if err := parsePID(pid, doc); err != nil {
			return nil, err
		}
	}
	if pv1 != nil {
		if err := parsePV1(pv1, doc); err != nil {
			return nil, err
		}
	}
	visitID := ""
	if doc.Encounter != nil && doc.Encounter.VisitID != nil {
		visitID = *doc.Encounter.VisitID
	}
	for _, seg := range obx {
		if err := parseOBX(seg, obr, visitID, doc); err != nil {
			return nil, err
		}
	}
	return doc, nil
}

// mshField returns the nth MSH field using the standard's special indexing
// (MSH itself counts as field 1, so MSH-n lives at token index n-1).
func mshField(fields []string, n int) string {
	idx := n - 1
	if idx < 0 || idx >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[idx])
}

// segField returns the nth field of any non-MSH segment, where the segment tag
// occupies index 0 and field n therefore lives at token index n.
func segField(fields []string, n int) string {
	if n < 0 || n >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[n])
}

// firstComponent returns the component before '^' and after '~' repeats.
func firstComponent(v string) string {
	if i := strings.IndexByte(v, '~'); i >= 0 {
		v = v[:i]
	}
	if i := strings.IndexByte(v, '^'); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

func parseMSH(fields []string, doc *RawDocument) error {
	// fields[0]="MSH", fields[1]="|", fields[2]=MSH-3 ...
	if len(fields) < 12 { // must reach MSH-12 version id (token index n-1 = 11)
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "MSH segment too short"}
	}
	doc.SourceSystem = mshField(fields, 3) // MSH-3 sending application
	version := mshField(fields, 12)        // MSH-12 version id
	if !SupportedHL7Versions[version] {
		return &domain.RejectError{Status: domain.StatusRejectedVersion, Reason: "unsupported hl7 version"}
	}
	doc.Version = version
	typeField := mshField(fields, 9) // MSH-9 message type, e.g. "ADT^A01" or "ADT^A01^ADT_A01"
	eventType := firstComponent(typeField)
	if !supportedHL7MessageTypes[eventType] {
		return &domain.RejectError{Status: domain.StatusRejectedMessageType, Reason: "unsupported message type"}
	}
	doc.MessageID = mshField(fields, 10) // MSH-10 message control id
	if doc.MessageID == "" {
		return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "missing MSH-10 control id"}
	}
	if t, ok := parseHL7Time(mshField(fields, 7)); ok { // MSH-7 date/time of message
		doc.updatedAtHint = t
	}
	return nil
}

func parsePID(fields []string, doc *RawDocument) error {
	p := &RawPatient{UpdatedAt: doc.updatedAtHint}
	if v := firstComponent(segField(fields, 3)); v != "" { // PID-3 patient identifier list
		s := v
		p.MRN = &s
	}
	if name := segField(fields, 5); name != "" { // PID-5 patient name
		parts := strings.Split(name, "^")
		fam := strings.TrimSpace(parts[0])
		given := ""
		if len(parts) > 1 {
			given = strings.TrimSpace(parts[1])
		}
		p.FamilyName = &fam
		if given != "" {
			p.GivenName = &given
		}
	}
	if v := segField(fields, 7); v != "" { // PID-7 date/time of birth -> date
		d, err := hl7Date(v)
		if err != nil {
			return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unparsable birth date"}
		}
		p.BirthDate = &d
	}
	if v := segField(fields, 8); v != "" { // PID-8 administrative sex
		p.Gender = &v
	}
	doc.Patient = p
	return nil
}

func parsePV1(fields []string, doc *RawDocument) error {
	e := &RawEncounter{UpdatedAt: doc.updatedAtHint}
	if v := firstComponent(segField(fields, 19)); v != "" { // PV1-19 visit number
		s := v
		e.VisitID = &s
	}
	if doc.Patient != nil && doc.Patient.MRN != nil {
		mrn := *doc.Patient.MRN
		e.MRN = &mrn
	}
	if v := segField(fields, 2); v != "" { // PV1-2 patient class
		s := v
		e.Class = &s
	}
	doc.Encounter = e
	return nil
}

func parseOBX(fields, obr []string, visitID string, doc *RawDocument) error {
	o := &RawObservation{UpdatedAt: doc.updatedAtHint}

	// OBX-3 observation identifier: code^name
	ident := segField(fields, 3)
	parts := strings.Split(ident, "^")
	if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
		code := strings.TrimSpace(parts[0])
		o.TestCode = &code
	}
	if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
		name := strings.TrimSpace(parts[1])
		o.TestName = &name
	}

	// OBX-2 value type: NM -> number, ST/TX/CWE -> string
	vt := strings.ToUpper(segField(fields, 2))
	rawVal := segField(fields, 5) // OBX-5 observation value
	switch vt {
	case "NM":
		o.ValueType = "number"
		if rawVal != "" {
			n, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "NM value is not numeric"}
			}
			o.ValueNumber = &n
		}
	case "ST", "TX", "CWE", "":
		o.ValueType = "string"
		if rawVal != "" {
			s := rawVal
			o.ValueString = &s
		}
	default:
		return &domain.RejectError{Status: domain.StatusRejectedTypeConflict, Reason: "unsupported OBX value type"}
	}

	// OBX-6 units: take first component
	if v := firstComponent(segField(fields, 6)); v != "" {
		s := v
		o.Unit = &s
	}
	// OBX-11 observation result status
	if v := segField(fields, 11); v != "" {
		s := v
		o.Status = &s
	}
	// OBX-14 date/time of observation (effective)
	if v := segField(fields, 14); v != "" {
		if d, err := hl7Date(v); err == nil {
			o.EffectiveTime = &d
		} else if t, ok := parseHL7Time(v); ok {
			s := t.UTC().Format(time.RFC3339)
			o.EffectiveTime = &s
		}
	}

	// OBX-21 observation unique id (2.5.1+). When absent (2.3.1), synthesize a
	// stable id from the filler order number (OBR-3), the segment set id
	// (OBX-1) and the test code, so multiple results under one order stay
	// distinct while repeated deliveries map to the same id.
	id := firstComponent(segField(fields, 21))
	if id == "" {
		setID := segField(fields, 1)
		order := ""
		if len(obr) > 0 {
			order = firstComponent(segField(obr, 3))
		}
		if order != "" {
			id = order + "-" + setID + "-" + o.derefCode()
		} else {
			id = "OBX-" + setID + "-" + o.derefCode()
		}
	}
	if id != "" {
		s := id
		o.ObservationID = &s
	}

	if doc.Patient != nil && doc.Patient.MRN != nil {
		mrn := *doc.Patient.MRN
		o.MRN = &mrn
	}
	if visitID != "" {
		v := visitID
		o.VisitID = &v
	}
	doc.Observations = append(doc.Observations, o)
	return nil
}

func (o *RawObservation) derefCode() string {
	if o.TestCode != nil {
		return *o.TestCode
	}
	return ""
}

// parseHL7Time parses HL7 DTM/TS values: YYYYMMDDHHMMSS[.S][+ZZZZ].
func parseHL7Time(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	layouts := []string{
		"20060102150405.0000-0700",
		"20060102150405-0700",
		"20060102150405.0000",
		"20060102150405",
		"200601021504",
		"20060102",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, v, time.UTC); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// hl7Date normalizes an HL7 date portion to YYYY-MM-DD.
func hl7Date(v string) (string, error) {
	t, ok := parseHL7Time(v)
	if !ok {
		return "", &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unparsable date"}
	}
	return t.UTC().Format("2006-01-02"), nil
}
