// Package export 实现按调用方权限的受控导出与审计留痕。
//
// 边界规则：
//   - 普通用途（general）必须脱敏；请求未脱敏数据属于非法脱敏组合，直接拒绝。
//   - 审计用途（audit）可保留身份信息，但每一次导出（含被拒绝的）都完整留痕。
//   - 请求的部分患者不存在时整体拒绝（不允许部分成功）。
//   - 所有错误响应只携带原因码与计数，不回显任何原始敏感字段。
package export

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sync"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

// Role 为调用方用途。
type Role string

const (
	RoleGeneral Role = "general" // 普通用途：强制脱敏
	RoleAudit   Role = "audit"   // 审计用途：可保留身份信息，必须留痕
)

// Reason 为导出拒绝原因码。
type Reason string

const (
	ReasonUnauthorized   Reason = "unauthorized"
	ReasonIllegalMasking Reason = "illegal_masking_combination"
	ReasonPartialFailure Reason = "partial_failure"
)

// Error 为导出错误；只含原因码与计数，绝不含原始敏感字段。
type Error struct {
	Reason Reason
	Detail string
}

func (e *Error) Error() string { return string(e.Reason) + ": " + e.Detail }

// Request 描述一次导出请求。
type Request struct {
	Caller     string
	Role       Role
	Unmasked   bool // 仅审计用途可置 true
	PatientIDs []string
}

// AuditEntry 为一次导出的完整留痕。
type AuditEntry struct {
	Caller     string
	Role       Role
	Unmasked   bool
	PatientIDs []string
	Count      int
	Result     string // allowed/denied
	Reason     string
}

// Exporter 受控导出器。
type Exporter struct {
	mu           sync.Mutex
	patients     []model.Patient
	encounters   []model.Encounter
	observations []model.Observation
	trail        []AuditEntry
	// salt 用于脱敏导出中的稳定假名化（本地固定，进程内一致）。
	salt string
}

// New 创建导出器。
func New() *Exporter {
	return &Exporter{salt: "clinical-interop-local-salt"}
}

// Load 注入当前数据快照（由调用方从 store 取得）。
func (e *Exporter) Load(patients []model.Patient, encounters []model.Encounter, observations []model.Observation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.patients = append([]model.Patient(nil), patients...)
	e.encounters = append([]model.Encounter(nil), encounters...)
	e.observations = append([]model.Observation(nil), observations...)
}

// Export 执行一次受控导出；任何拒绝都以 error 返回，且错误不包含原始敏感字段。
func (e *Exporter) Export(req Request) (patients []model.Patient, encounters []model.Encounter, observations []model.Observation, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deny := func(reason Reason, detail string) error {
		e.record(req, 0, "denied", string(reason))
		log.Printf("export caller=%s role=%s unmasked=%v result=denied reason=%s", req.Caller, req.Role, req.Unmasked, reason)
		return &Error{Reason: reason, Detail: detail}
	}

	if req.Caller == "" {
		return nil, nil, nil, deny(ReasonUnauthorized, "caller identity required")
	}
	switch req.Role {
	case RoleGeneral:
		if req.Unmasked {
			return nil, nil, nil, deny(ReasonIllegalMasking, "general role must not request unmasked data")
		}
	case RoleAudit:
		// 审计用途允许 masked 或 unmasked，两者均留痕。
	default:
		return nil, nil, nil, deny(ReasonUnauthorized, "unknown role")
	}

	// 选定患者集合；指定了清单则必须全部存在，否则整体拒绝。
	byID := map[string]model.Patient{}
	for _, p := range e.patients {
		byID[p.ID] = p
	}
	var selected []model.Patient
	if len(req.PatientIDs) == 0 {
		selected = append(selected, e.patients...)
	} else {
		missing := 0
		for _, id := range req.PatientIDs {
			p, ok := byID[id]
			if !ok {
				missing++
				continue
			}
			selected = append(selected, p)
		}
		if missing > 0 {
			// 不回显任何请求中的标识，只报告数量。
			return nil, nil, nil, deny(ReasonPartialFailure, "some requested patients were not found; export rejected as a whole")
		}
	}

	idSet := map[string]bool{}
	for _, p := range selected {
		idSet[p.ID] = true
	}
	for _, enc := range e.encounters {
		if idSet[enc.PatientID] {
			encounters = append(encounters, enc)
		}
	}
	for _, ob := range e.observations {
		if idSet[ob.PatientID] {
			observations = append(observations, ob)
		}
	}

	masked := !req.Unmasked
	if masked {
		patients, encounters, observations = e.maskAll(selected, encounters, observations)
	} else {
		patients = append(patients, selected...)
	}

	e.record(req, len(patients), "allowed", "")
	log.Printf("export caller=%s role=%s unmasked=%v patients=%d encounters=%d observations=%d result=allowed",
		req.Caller, req.Role, req.Unmasked, len(patients), len(encounters), len(observations))
	return patients, encounters, observations, nil
}

// maskAll 脱敏：姓名/病历号置空，生日仅保留年份，患者主键假名化并保持一致性连接。
func (e *Exporter) maskAll(patients []model.Patient, encounters []model.Encounter, observations []model.Observation) ([]model.Patient, []model.Encounter, []model.Observation) {
	alias := map[string]string{}
	out := make([]model.Patient, 0, len(patients))
	for _, p := range patients {
		a := e.pseudonym(p.ID)
		alias[p.ID] = a
		p.ID = a
		p.MRN = ""
		p.Name = ""
		if len(p.BirthDate) >= 4 {
			p.BirthDate = p.BirthDate[:4] + "-**-**"
		} else {
			p.BirthDate = ""
		}
		out = append(out, p)
	}
	encOut := make([]model.Encounter, 0, len(encounters))
	for _, enc := range encounters {
		if a, ok := alias[enc.PatientID]; ok {
			enc.PatientID = a
		}
		encOut = append(encOut, enc)
	}
	obsOut := make([]model.Observation, 0, len(observations))
	for _, ob := range observations {
		if a, ok := alias[ob.PatientID]; ok {
			ob.PatientID = a
		}
		obsOut = append(obsOut, ob)
	}
	return out, encOut, obsOut
}

func (e *Exporter) pseudonym(id string) string {
	sum := sha256.Sum256([]byte(e.salt + ":" + id))
	return "PSEUDO:" + hex.EncodeToString(sum[:8])
}

func (e *Exporter) record(req Request, count int, result, reason string) {
	e.trail = append(e.trail, AuditEntry{
		Caller:     req.Caller,
		Role:       req.Role,
		Unmasked:   req.Unmasked,
		PatientIDs: append([]string(nil), req.PatientIDs...),
		Count:      count,
		Result:     result,
		Reason:     reason,
	})
}

// AuditTrail 返回全部导出留痕副本。
func (e *Exporter) AuditTrail() []AuditEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]AuditEntry(nil), e.trail...)
}
