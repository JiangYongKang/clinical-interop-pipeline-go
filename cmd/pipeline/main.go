// 本地端到端演示：接收 -> 归一化 -> 幂等更新 -> 受控导出。
package main

import (
	"fmt"
	"log"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/export"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

func main() {
	st := store.New()

	messages := []struct {
		name        string
		contentType string
		raw         string
	}{
		{"hl7-v23-admit", "hl7v2", "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|MSG001|P|2.3\r" +
			"ZVR|10\r" +
			"PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r" +
			"PV1|1|I|WARD1||||DOC1||||||||||||VN1001|||||||||||||||||||||||||20260101100000\r"},
		{"hl7-v25-lab-newer", "hl7v2", "MSH|^~\\&|LIS|HOSP|PIPE|LAB|20260102120000||ORU^R01|MSG002|P|2.5\r" +
			"ZVR|20\r" +
			"PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r" +
			"PV1|1|I|WARD1||||DOC1||||||||||||VN1001|||||||||||||||||||||||||20260101100000\r" +
			"OBR|1|ORD1|FIL1|GLU^Glucose\r" +
			"OBX|1|NM|GLU^Glucose||6.2|mmol/L|3.9-6.1|H|||F|||20260102090000\r"},
		{"hl7-v23-stale", "hl7v2", "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101110000||ADT^A01|MSG003|P|2.3\r" +
			"ZVR|5\r" +
			"PID|1||MRN1001^^^HIS||Zhang^S||19800101|M\r" +
			"PV1|1|I|WARD1||||DOC1||||||||||||VN1001|||||||||||||||||||||||||20260101100000\r"},
		{"hl7-duplicate", "hl7v2", "MSH|^~\\&|LIS|HOSP|PIPE|LAB|20260102120000||ORU^R01|MSG002|P|2.5\r" +
			"ZVR|20\r" +
			"PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r" +
			"PV1|1|I|WARD1||||DOC1||||||||||||VN1001|||||||||||||||||||||||||20260101100000\r" +
			"OBR|1|ORD1|FIL1|GLU^Glucose\r" +
			"OBX|1|NM|GLU^Glucose||6.2|mmol/L|3.9-6.1|H|||F|||20260102090000\r"},
		{"hl7-unsupported-version", "hl7v2", "MSH|^~\\&|HIS|HOSP|PIPE|LAB|20260101120000||ADT^A01|MSG004|P|2.1\r" +
			"ZVR|30\r" +
			"PID|1||MRN1002^^^HIS||Li^Si||19900202|F\r"},
		{"hl7-type-conflict", "hl7v2", "MSH|^~\\&|LIS|HOSP|PIPE|LAB|20260103120000||ORU^R01|MSG005|P|2.5\r" +
			"ZVR|40\r" +
			"PID|1||MRN1001^^^HIS||Zhang^San||19800101|M\r" +
			"OBR|1|ORD2|FIL2|GLU^Glucose\r" +
			"OBX|1|NM|GLU^Glucose||not-a-number|mmol/L|||||F|||20260103090000\r"},
		{"fhir-r4-encounter", "fhir-json", `{
		  "id":"BND001","fhirVersion":"R4","x-source":"PORTAL","x-sequence":50,
		  "entry":[
		    {"resource":{"resourceType":"Patient","id":"p1","identifier":[{"value":"MRN1001"}],"name":[{"family":"Zhang","given":["San"]}],"gender":"male","birthDate":"1980-01-01"}},
		    {"resource":{"resourceType":"Encounter","id":"e1","status":"finished","class":{"code":"IMP"},"subject":{"reference":"Patient/p1"},"period":{"start":"2026-01-01T10:00:00Z","end":"2026-01-05T10:00:00Z"}}},
		    {"resource":{"resourceType":"Observation","id":"o1","status":"final","code":{"coding":[{"code":"GLU","display":"Glucose"}]},"subject":{"reference":"Patient/p1"},"encounter":{"reference":"Encounter/e1"},"effectiveDateTime":"2026-01-02T09:00:00Z","valueQuantity":{"value":6.1,"unit":"mmol/L"}}}
		  ]}`},
		{"fhir-missing-field", "fhir-json", `{
		  "id":"BND002","fhirVersion":"R4","x-source":"PORTAL","x-sequence":60,
		  "entry":[{"resource":{"resourceType":"Patient","id":"p2","gender":"female","birthDate":"1990-02-02"}}]}`},
	}

	for _, m := range messages {
		upd, err := ingest.Parse(m.contentType, []byte(m.raw))
		if err != nil {
			log.Printf("pipeline input=%s status=rejected error=%v", m.name, err)
			continue
		}
		res := st.Apply(upd)
		fmt.Printf("pipeline input=%s message_id=%s status=%s applied=%d stale=%d dup=%d\n",
			m.name, res.MessageID, res.Status, res.Applied, res.SkippedStale, res.SkippedDup)
	}

	patients, encounters, observations := st.Snapshot()
	fmt.Printf("snapshot patients=%d encounters=%d observations=%d\n", len(patients), len(encounters), len(observations))

	exp := export.New()
	exp.Load(patients, encounters, observations)

	// 普通用途：脱敏导出。
	gp, ge, gob, err := exp.Export(export.Request{Caller: "researcher-1", Role: export.RoleGeneral})
	fmt.Printf("export general err=%v patients=%d encounters=%d observations=%d\n", err, len(gp), len(ge), len(gob))
	for _, p := range gp {
		fmt.Printf("  masked patient id=%s mrn=%q name=%q birth=%q\n", p.ID, p.MRN, p.Name, p.BirthDate)
	}

	// 非法脱敏组合：普通用途请求未脱敏，必须拒绝。
	_, _, _, err = exp.Export(export.Request{Caller: "researcher-1", Role: export.RoleGeneral, Unmasked: true})
	fmt.Printf("export general+unmasked err=%v\n", err)

	// 审计用途：保留身份信息并留痕。
	ap, _, _, err := exp.Export(export.Request{Caller: "auditor-1", Role: export.RoleAudit, Unmasked: true})
	fmt.Printf("export audit err=%v patients=%d\n", err, len(ap))
	for _, p := range ap {
		fmt.Printf("  audit patient id=%s mrn=%q name=%q\n", p.ID, p.MRN, p.Name)
	}

	// 部分失败：请求不存在的患者，整体拒绝。
	_, _, _, err = exp.Export(export.Request{Caller: "auditor-1", Role: export.RoleAudit, Unmasked: true, PatientIDs: []string{"PAT:MRN1001", "PAT:NOPE"}})
	fmt.Printf("export partial-failure err=%v\n", err)

	fmt.Printf("audit trail entries=%d\n", len(exp.AuditTrail()))
	for _, a := range exp.AuditTrail() {
		fmt.Printf("  audit caller=%s role=%s unmasked=%v result=%s reason=%s count=%d\n",
			a.Caller, a.Role, a.Unmasked, a.Result, a.Reason, a.Count)
	}
}
