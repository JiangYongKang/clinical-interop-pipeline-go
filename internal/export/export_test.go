package export

import (
	"errors"
	"strings"
	"testing"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

func newLoadedExporter() *Exporter {
	e := New()
	e.Load(
		[]model.Patient{
			{ID: "PAT:MRN1001", MRN: "MRN1001", Name: "San Zhang", BirthDate: "1980-01-01", Gender: "male"},
			{ID: "PAT:MRN1002", MRN: "MRN1002", Name: "Si Li", BirthDate: "1990-02-02", Gender: "female"},
		},
		[]model.Encounter{{ID: "ENC:HIS:VN1", PatientID: "PAT:MRN1001", Status: "finished"}},
		[]model.Observation{{ID: "OBS:LIS:F1:1", PatientID: "PAT:MRN1001", Code: "GLU", Value: "6.2"}},
	)
	return e
}

func mustExportReason(t *testing.T, err error, want Reason) {
	t.Helper()
	var ee *Error
	if !errors.As(err, &ee) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if ee.Reason != want {
		t.Fatalf("reason = %s, want %s", ee.Reason, want)
	}
}

func TestGeneralExportIsMasked(t *testing.T) {
	e := newLoadedExporter()
	p, enc, obs, err := e.Export(Request{Caller: "r1", Role: RoleGeneral})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(p) != 2 || len(enc) != 1 || len(obs) != 1 {
		t.Fatalf("counts: %d/%d/%d", len(p), len(enc), len(obs))
	}
	for _, pt := range p {
		if pt.MRN != "" || pt.Name != "" {
			t.Fatalf("general identity leaked: %+v", pt)
		}
		if strings.Contains(pt.ID, "MRN") {
			t.Fatalf("masked id leaks MRN: %s", pt.ID)
		}
		if !strings.HasSuffix(pt.BirthDate, "-**-**") {
			t.Fatalf("birthdate not masked: %q", pt.BirthDate)
		}
	}
	// 假名化后关联必须保持一致。
	if enc[0].PatientID != p[0].ID && enc[0].PatientID != p[1].ID {
		t.Fatalf("encounter linkage broken after masking: %+v", enc[0])
	}
}

func TestGeneralUnmaskedRejected(t *testing.T) {
	e := newLoadedExporter()
	_, _, _, err := e.Export(Request{Caller: "r1", Role: RoleGeneral, Unmasked: true})
	mustExportReason(t, err, ReasonIllegalMasking)
}

func TestUnknownRoleRejected(t *testing.T) {
	e := newLoadedExporter()
	_, _, _, err := e.Export(Request{Caller: "r1", Role: "admin"})
	mustExportReason(t, err, ReasonUnauthorized)
}

func TestEmptyCallerRejected(t *testing.T) {
	e := newLoadedExporter()
	_, _, _, err := e.Export(Request{Role: RoleGeneral})
	mustExportReason(t, err, ReasonUnauthorized)
}

func TestAuditUnmaskedWithTrail(t *testing.T) {
	e := newLoadedExporter()
	p, _, _, err := e.Export(Request{Caller: "aud1", Role: RoleAudit, Unmasked: true})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if p[0].MRN == "" || p[0].Name == "" {
		t.Fatalf("audit export must retain identity: %+v", p[0])
	}
	trail := e.AuditTrail()
	if len(trail) != 1 || trail[0].Result != "allowed" || trail[0].Caller != "aud1" || !trail[0].Unmasked {
		t.Fatalf("audit trail incomplete: %+v", trail)
	}
}

func TestPartialFailureRejectedAsWhole(t *testing.T) {
	e := newLoadedExporter()
	p, enc, obs, err := e.Export(Request{
		Caller: "aud1", Role: RoleAudit, Unmasked: true,
		PatientIDs: []string{"PAT:MRN1001", "PAT:DOES-NOT-EXIST"},
	})
	mustExportReason(t, err, ReasonPartialFailure)
	if p != nil || enc != nil || obs != nil {
		t.Fatal("partial export must not return any data")
	}
}

// 所有拒绝路径都必须留痕。
func TestDenylistAlsoAudited(t *testing.T) {
	e := newLoadedExporter()
	_, _, _, _ = e.Export(Request{Caller: "r1", Role: RoleGeneral, Unmasked: true})
	_, _, _, _ = e.Export(Request{Caller: "r1", Role: "bogus"})
	trail := e.AuditTrail()
	if len(trail) != 2 {
		t.Fatalf("trail len = %d", len(trail))
	}
	for _, a := range trail {
		if a.Result != "denied" || a.Reason == "" {
			t.Fatalf("denied export not fully recorded: %+v", a)
		}
	}
}

// 错误响应不得泄露原始敏感字段（姓名/病历号/出生日期）。
func TestErrorsNeverLeakSensitiveFields(t *testing.T) {
	e := newLoadedExporter()
	sensitive := []string{"San Zhang", "Si Li", "MRN1001", "MRN1002", "1980-01-01", "1990-02-02"}
	errs := []error{}
	_, _, _, err := e.Export(Request{Caller: "r1", Role: RoleGeneral, Unmasked: true})
	errs = append(errs, err)
	_, _, _, err = e.Export(Request{Caller: "r1", Role: "bogus"})
	errs = append(errs, err)
	_, _, _, err = e.Export(Request{Caller: "r1", Role: RoleAudit, Unmasked: true, PatientIDs: []string{"PAT:MRN1001", "PAT:NOPE"}})
	errs = append(errs, err)
	for _, err := range errs {
		if err == nil {
			t.Fatal("expected error")
		}
		for _, s := range sensitive {
			if strings.Contains(err.Error(), s) {
				t.Fatalf("error leaks sensitive field %q: %v", s, err)
			}
		}
	}
}

// 同一请求重复执行结果一致（可重复验证）。
func TestExportDeterministic(t *testing.T) {
	e := newLoadedExporter()
	p1, _, _, _ := e.Export(Request{Caller: "r1", Role: RoleGeneral})
	p2, _, _, _ := e.Export(Request{Caller: "r1", Role: RoleGeneral})
	if len(p1) != len(p2) {
		t.Fatal("non-deterministic export")
	}
	for i := range p1 {
		if p1[i] != p2[i] {
			t.Fatalf("export differs between runs: %+v vs %+v", p1[i], p2[i])
		}
	}
}
