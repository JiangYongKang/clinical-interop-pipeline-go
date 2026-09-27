package pipeline

import (
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// Logger is the minimal structured logging surface.
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger is used when no logger is supplied.
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Pipeline wires parsing, normalization, storage and audit together.
type Pipeline struct {
	store *store.Store
	audit *audit.Log
	log   Logger
	now   func() time.Time
}

// New builds a pipeline with injectable clock and logger.
func New(st *store.Store, al *audit.Log, log Logger, now func() time.Time) *Pipeline {
	if log == nil {
		log = discardLogger{}
	}
	if now == nil {
		now = time.Now
	}
	return &Pipeline{store: st, audit: al, log: log, now: now}
}

// Ingest runs parse -> normalize -> store and logs the input id, decision
// basis and final status. Every failure returns a *domain.RejectError that
// contains only the caller ref, status and a value-free reason; raw payload
// bytes never reach logs, audit entries or error responses.
func (p *Pipeline) Ingest(format, ref string, payload []byte) (store.Outcome, *domain.RejectError) {
	now := p.now().UTC()
	out := store.Outcome{}

	doc, perr := ingest.Parse(format, payload, now)
	if perr != nil {
		return p.reject(ref, "parse", perr, now)
	}
	p.log.Printf("level=ingest stage=parse ref=%s msg_id=%s format=%s version=%s status=ok",
		ref, doc.MessageID, doc.Format, doc.Version)

	cs, err := normalize.Normalize(doc)
	if err != nil {
		return p.reject(ref, "normalize", err, now)
	}
	p.log.Printf("level=ingest stage=normalize ref=%s msg_id=%s entities=patient:%t,encounter:%t,observations:%d status=ok",
		ref, cs.MessageID, cs.Patient != nil, cs.Encounter != nil, len(cs.Observations))

	o := p.store.Apply(cs, now)
	out = o
	decision := "allow"
	if isRejectStatus(o.Status) {
		decision = "deny"
	}
	p.audit.Append("ingest", "ingest", ref, decision,
		"status="+string(o.Status)+";basis="+o.Reason,
		[]string{"Message/" + o.MessageID, "Patient/" + o.PatientMRN}, now)
	p.log.Printf("level=ingest stage=apply ref=%s msg_id=%s status=%s basis=%q digest=%s",
		ref, o.MessageID, o.Status, o.Reason, o.Digest)
	return out, nil
}

func (p *Pipeline) reject(ref, stage string, err error, at time.Time) (store.Outcome, *domain.RejectError) {
	rj := asReject(err)
	rj.Ref = ref
	// Defensive guarantee: only the typed status/reason escape, never any
	// payload fragment that an underlying error might carry.
	rj.Reason = stage + ":" + rj.Reason
	p.audit.Append("ingest", "ingest", ref, "deny",
		"stage="+stage+";status="+string(rj.Status), []string{"Ref/" + ref}, at)
	p.log.Printf("level=ingest stage=%s ref=%s status=%s decision=deny", stage, ref, rj.Status)
	return store.Outcome{Status: rj.Status}, rj
}

func asReject(err error) *domain.RejectError {
	if rj, ok := err.(*domain.RejectError); ok {
		return rj
	}
	return &domain.RejectError{Status: domain.StatusRejectedMalformed, Reason: "unclassified failure"}
}

func isRejectStatus(s domain.Status) bool {
	switch s {
	case domain.StatusAccepted, domain.StatusUpdated, domain.StatusDeduplicated:
		return false
	default:
		return true
	}
}
