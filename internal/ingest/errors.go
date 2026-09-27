package ingest

// Reason 是输入被拒绝或被特殊处理的可区分原因码。
type Reason string

const (
	ReasonUnsupportedVersion Reason = "unsupported_version"
	ReasonMissingField       Reason = "missing_field"
	ReasonTypeConflict       Reason = "type_conflict"
	ReasonDuplicate          Reason = "duplicate"
	ReasonStaleVersion       Reason = "stale_version"
	ReasonInvalidEncoding    Reason = "invalid_encoding"
)

// Error 携带稳定原因码，且不包含原始敏感字段内容。
type Error struct {
	Reason  Reason
	Field   string // 仅记录字段路径，不记录字段值
	Message string
}

func (e *Error) Error() string {
	if e.Field != "" {
		return string(e.Reason) + ": " + e.Field + ": " + e.Message
	}
	return string(e.Reason) + ": " + e.Message
}

func newError(reason Reason, field, msg string) *Error {
	return &Error{Reason: reason, Field: field, Message: msg}
}
