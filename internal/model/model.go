// Package model 定义跨 HL7 v2 / FHIR 统一后的临床数据模型。
package model

import "time"

// SourceKind 标识消息来源类型。
type SourceKind string

const (
	SourceHL7v2 SourceKind = "hl7v2"
	SourceFHIR  SourceKind = "fhir"
)

// Patient 为统一后的患者模型。
type Patient struct {
	ID            string // 稳定去重主键（按 MRN 归一化得到）
	MRN           string // 病历号
	Name          string
	BirthDate     string // YYYY-MM-DD
	Gender        string // male/female/other/unknown
	SourceKind    SourceKind
	SourceVersion string // 如 2.3 / 2.5 / R4
	Sequence      int64  // 来源消息内的版本序号，越大越新
	UpdatedAt     time.Time
}

// Encounter 为统一后的就诊模型。
type Encounter struct {
	ID         string // 稳定主键（来源编码+来源就诊号）
	PatientID  string
	Status     string // planned/in-progress/finished/cancelled
	Class      string // inpatient/outpatient/emergency
	Start      time.Time
	End        time.Time
	SourceKind SourceKind
	Sequence   int64
	UpdatedAt  time.Time
}

// Observation 为统一后的检验结果模型。
type Observation struct {
	ID         string // 稳定主键（来源编码+检验实例号）
	PatientID  string
	EncounterID string
	Code       string // LOINC 风格检验编码
	Name       string
	Value      string
	Unit       string
	Status     string // final/preliminary/corrected
	ObservedAt time.Time
	SourceKind SourceKind
	Sequence   int64
	UpdatedAt  time.Time
}

// Update 是一次消息归一化后的完整写入包。
type Update struct {
	MessageID   string // 来源消息唯一标识，用于幂等去重
	SourceKind  SourceKind
	SourceVersion string
	Sequence    int64
	Patients    []Patient
	Encounters  []Encounter
	Observations []Observation
}
