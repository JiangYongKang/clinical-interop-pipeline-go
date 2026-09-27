package ingest

import (
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

// ParseResult 是单条消息解析结果，ApplyStatus 描述其最终判定。
type ParseResult struct {
	Update     *model.Update
	Status     Status
	Reason     Reason
	Entities   int // 归一化出的实体数
}

// Status 是解析/应用阶段的稳定状态码。
type Status string

const (
	StatusApplied   Status = "applied"
	StatusDeduped   Status = "deduped"   // 重复消息标识，稳定去重
	StatusStale     Status = "stale"     // 旧版本，不覆盖新版本
	StatusRejected  Status = "rejected"  // 字段缺失/类型冲突/版本不支持
)

// Parse 将 HL7 v2 文本或 FHIR 风格 JSON 归一化为 Update。
// raw 的格式由 contentType 区分："hl7v2" / "fhir-json"。
func Parse(contentType string, raw []byte) (*model.Update, error) {
	switch contentType {
	case "hl7v2":
		return parseHL7(raw)
	case "fhir-json":
		return parseFHIR(raw)
	default:
		return nil, newError(ReasonUnsupportedVersion, "contentType", "unsupported content type: "+contentType)
	}
}
