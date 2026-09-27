package ingest

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

// 支持的 HL7 v2 版本与消息类型。
var supportedHL7Versions = map[string]bool{"2.3": true, "2.5": true}

// hl7Field 按 "|" 切分后的一个段；MSH 段特殊：下标 1 即 MSH-2。
type hl7Segment struct {
	name   string
	fields []string
}

// field 取 MSH-n 风格字段（1-based，对所有段统一：f(3) 为该段第 3 个字段）。
func (s hl7Segment) field(n int) string {
	if s.name == "MSH" {
		// 切分结果：[MSH, MSH-2, MSH-3, ...]
		idx := n - 1
		if idx >= 0 && idx < len(s.fields) {
			return s.fields[idx]
		}
		return ""
	}
	if n >= 0 && n < len(s.fields) {
		return s.fields[n]
	}
	return ""
}

func component(v string, n int) string {
	parts := strings.Split(v, "^")
	if n >= 0 && n < len(parts) {
		return parts[n]
	}
	return ""
}

// parseHL7 解析 HL7 v2 消息（MSH/PID/PV1/OBR/OBX，自定义 ZVR 携带单调序号）。
func parseHL7(raw []byte) (*model.Update, error) {
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	lines := strings.Split(text, "\n")

	var segs []hl7Segment
	for _, line := range lines {
		line = strings.TrimRight(line, "\n")
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "|")
		segs = append(segs, hl7Segment{name: parts[0], fields: parts})
	}
	if len(segs) == 0 || segs[0].name != "MSH" {
		return nil, newError(ReasonInvalidEncoding, "MSH", "missing MSH header segment")
	}
	msh := segs[0]

	messageID := msh.field(10)
	if messageID == "" {
		return nil, newError(ReasonMissingField, "MSH-10", "message control id required for idempotency")
	}
	version := msh.field(12)
	if !supportedHL7Versions[version] {
		log.Printf("ingest hl7 message_id=%s rejected reason=%s field=MSH-12", messageID, ReasonUnsupportedVersion)
		return nil, newError(ReasonUnsupportedVersion, "MSH-12", "only 2.3 and 2.5 are supported")
	}
	msgType := msh.field(9)
	kind := component(msgType, 0)
	trigger := component(msgType, 1)
	sourceCode := msh.field(3) // sending application，参与稳定主键
	if sourceCode == "" {
		return nil, newError(ReasonMissingField, "MSH-3", "sending application required")
	}

	// ZVR（自定义段）携带跨消息单调版本序号：ZVR|<sequence>
	var sequence int64
	foundSeq := false
	for _, s := range segs {
		if s.name == "ZVR" {
			v := s.field(1)
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, newError(ReasonTypeConflict, "ZVR-1", "sequence must be an integer")
			}
			sequence = n
			foundSeq = true
		}
	}
	if !foundSeq {
		return nil, newError(ReasonMissingField, "ZVR-1", "monotonic sequence required for cross-version ordering")
	}

	upd := &model.Update{
		MessageID:     messageID,
		SourceKind:    model.SourceHL7v2,
		SourceVersion: version,
		Sequence:      sequence,
	}

	var pid, pv1 *hl7Segment
	var obr *hl7Segment
	for i := range segs {
		switch segs[i].name {
		case "PID":
			pid = &segs[i]
		case "PV1":
			pv1 = &segs[i]
		case "OBR":
			obr = &segs[i]
		case "OBX":
			obs, err := parseOBX(segs[i], sourceCode, obr, version, sequence)
			if err != nil {
				return nil, err
			}
			upd.Observations = append(upd.Observations, obs)
		}
	}
	if pid == nil {
		return nil, newError(ReasonMissingField, "PID", "patient segment required")
	}
	patient, err := parsePID(*pid, version, sequence)
	if err != nil {
		return nil, err
	}
	upd.Patients = []model.Patient{patient}

	if pv1 != nil {
		enc, err := parsePV1(*pv1, sourceCode, patient.ID, version, sequence, kind, trigger)
		if err != nil {
			return nil, err
		}
		upd.Encounters = []model.Encounter{enc}
		for i := range upd.Observations {
			upd.Observations[i].PatientID = patient.ID
			upd.Observations[i].EncounterID = enc.ID
		}
	} else {
		for i := range upd.Observations {
			upd.Observations[i].PatientID = patient.ID
		}
	}

	log.Printf("ingest hl7 message_id=%s type=%s^%s version=%s seq=%d entities=%d status=parsed",
		messageID, kind, trigger, version, sequence, len(upd.Patients)+len(upd.Encounters)+len(upd.Observations))
	return upd, nil
}

func parsePID(pid hl7Segment, version string, seq int64) (model.Patient, error) {
	mrn := component(pid.field(3), 0) // PID-3 重复标识取首个组件
	if mrn == "" {
		return model.Patient{}, newError(ReasonMissingField, "PID-3", "medical record number required")
	}
	name := strings.TrimSpace(component(pid.field(5), 0) + " " + component(pid.field(5), 1))
	birth, err := parseHL7Date(pid.field(7))
	if err != nil {
		return model.Patient{}, newError(ReasonTypeConflict, "PID-7", err.Error())
	}
	return model.Patient{
		ID:            stablePatientID(mrn),
		MRN:           mrn,
		Name:          name,
		BirthDate:     birth,
		Gender:        normalizeGender(pid.field(8)),
		SourceKind:    model.SourceHL7v2,
		SourceVersion: version,
		Sequence:      seq,
		UpdatedAt:     time.Now().UTC(),
	}, nil
}

func parsePV1(pv1 hl7Segment, sourceCode, patientID, version string, seq int64, kind, trigger string) (model.Encounter, error) {
	visit := pv1.field(19)
	if visit == "" {
		return model.Encounter{}, newError(ReasonMissingField, "PV1-19", "visit number required")
	}
	start, err := parseHL7DateTime(pv1.field(44))
	if err != nil {
		return model.Encounter{}, newError(ReasonTypeConflict, "PV1-44", err.Error())
	}
	status := "in-progress"
	if kind == "ADT" && trigger == "A03" {
		status = "finished"
	}
	class := normalizeClass(pv1.field(2))
	return model.Encounter{
		ID:         "ENC:" + sourceCode + ":" + visit,
		PatientID:  patientID,
		Status:     status,
		Class:      class,
		Start:      start,
		SourceKind: model.SourceHL7v2,
		Sequence:   seq,
		UpdatedAt:  time.Now().UTC(),
	}, nil
}

func parseOBX(obx hl7Segment, sourceCode string, obr *hl7Segment, version string, seq int64) (model.Observation, error) {
	code := component(obx.field(3), 0)
	if code == "" {
		return model.Observation{}, newError(ReasonMissingField, "OBX-3", "observation code required")
	}
	name := component(obx.field(3), 1)
	valueType := obx.field(2)
	value := obx.field(5)
	if value == "" {
		return model.Observation{}, newError(ReasonMissingField, "OBX-5", "observation value required")
	}
	// 类型冲突：声明数值型(NM)却给出非数值，必须显式拒绝。
	if valueType == "NM" {
		if _, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err != nil {
			return model.Observation{}, newError(ReasonTypeConflict, "OBX-5", "declared NM but value is not numeric")
		}
	}
	obsTime, err := parseHL7DateTime(obx.field(14))
	if err != nil {
		return model.Observation{}, newError(ReasonTypeConflict, "OBX-14", err.Error())
	}
	setID := obx.field(1)
	orderID := ""
	if obr != nil {
		orderID = obr.field(3)
	}
	if orderID == "" {
		return model.Observation{}, newError(ReasonMissingField, "OBR-3", "order id required for observations")
	}
	return model.Observation{
		ID:         "OBS:" + sourceCode + ":" + orderID + ":" + setID,
		Code:       code,
		Name:       name,
		Value:      value,
		Unit:       component(obx.field(6), 0),
		Status:     normalizeObsStatus(obx.field(11)),
		ObservedAt: obsTime,
		SourceKind: model.SourceHL7v2,
		Sequence:   seq,
		UpdatedAt:  time.Now().UTC(),
	}, nil
}

func parseHL7Date(v string) (string, error) {
	v = strings.TrimSpace(v)
	if len(v) < 8 {
		return "", fmt.Errorf("expected YYYYMMDD")
	}
	d, err := time.Parse("20060102", v[:8])
	if err != nil {
		return "", err
	}
	return d.Format("2006-01-02"), nil
}

func parseHL7DateTime(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	layouts := []string{"20060102150405", "200601021504", "20060102"}
	for _, l := range layouts {
		if len(v) == len(l) {
			if t, err := time.Parse(l, v); err == nil {
				return t.UTC(), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("expected YYYYMMDD[HHMMSS]")
}

func normalizeGender(g string) string {
	switch strings.ToUpper(strings.TrimSpace(g)) {
	case "M":
		return "male"
	case "F":
		return "female"
	case "O", "A":
		return "other"
	default:
		return "unknown"
	}
}

func normalizeClass(c string) string {
	switch strings.ToUpper(strings.TrimSpace(c)) {
	case "I":
		return "inpatient"
	case "O":
		return "outpatient"
	case "E":
		return "emergency"
	default:
		return "outpatient"
	}
}

func normalizeObsStatus(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "F":
		return "final"
	case "P":
		return "preliminary"
	case "C":
		return "corrected"
	default:
		return "preliminary"
	}
}

func stablePatientID(mrn string) string {
	return "PAT:" + strings.ToUpper(strings.TrimSpace(mrn))
}
