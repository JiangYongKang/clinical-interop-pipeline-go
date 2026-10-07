package pipeline

import (
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// BatchItem is one wire message inside an all-or-nothing batch submission.
type BatchItem struct {
	Format  string
	Ref     string
	Payload []byte
}

// BatchResult is the disposition of one whole batch.
type BatchResult struct {
	Status      domain.Status
	Items       []store.Outcome
	FailedIndex int
	Reason      string
}

// IngestBatch accepts a whole batch or rejects it as a whole.
//
// Phase 1 parses and normalizes every message up front: the first parse or
// validation failure rejects the batch before ANY data is touched, with the
// caller ref of the offending item but no payload content. Phase 2 hands the
// normalized changesets to the store's atomic batch transaction, where stale /
// type-conflict / idempotency failures against committed state (and against
// earlier messages of the same batch) likewise abort the whole batch.
//
// Nothing is registered on failure, so correcting the rejected item and
// resending the exact batch applies normally; re-sending an accepted batch
// unchanged comes back wholly deduplicated.
func (p *Pipeline) IngestBatch(items []BatchItem) (*BatchResult, *domain.RejectError) {
	now := p.now().UTC()

	if len(items) == 0 {
		return p.batchReject(items, -1, domain.StatusRejectedMissingField,
			"batch:empty batch", "batch-empty", now)
	}

	css := make([]*normalize.ChangeSet, len(items))
	for i, it := range items {
		doc, perr := parseOne(it.Format, it.Payload, now)
		if perr != nil {
			return p.batchReject(items, i, perr.Status, "parse:"+perr.Reason, "parse", now)
		}
		cs, nerr := normalize.Normalize(doc)
		if nerr != nil {
			rj := asReject(nerr)
			return p.batchReject(items, i, rj.Status, "normalize:"+rj.Reason, "normalize", now)
		}
		css[i] = cs
	}

	bo := p.store.ApplyBatch(css, now)
	if isRejectStatus(bo.Status) {
		idx := bo.FailedIndex
		return p.batchReject(items, idx, bo.Status, "apply:"+bo.Reason, "apply", now)
	}

	// Success: one summary allow event (identifiers only, no payload), plus the
	// per-item allow/deny events so individual dispositions stay traceable.
	ids := make([]string, 0, len(css))
	var sb strings.Builder
	for i, o := range bo.Results {
		p.audit.Append("ingest", "ingest", items[i].Ref, "allow",
			"status="+string(o.Status)+";basis="+o.Reason,
			[]string{"Message/" + o.MessageID, "Patient/" + o.PatientMRN}, now)
		ids = append(ids, "Message/"+o.MessageID)
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(string(o.Status))
	}
	p.audit.Append("ingest", "ingest-batch", batchRef(items), "allow",
		"status="+string(bo.Status)+";items="+strconv.Itoa(len(bo.Results))+";statuses="+sb.String(),
		ids, now)
	p.log.Printf("level=ingest stage=batch size=%d status=%s statuses=%s",
		len(items), bo.Status, sb.String())
	return &BatchResult{Status: bo.Status, Items: bo.Results, Reason: bo.Reason}, nil
}

// batchReject records exactly one batch-level deny event (identifiers only;
// never any payload field) and returns the uniform typed error.
func (p *Pipeline) batchReject(items []BatchItem, index int, status domain.Status,
	reason, stage string, at time.Time) (*BatchResult, *domain.RejectError) {
	ref := "batch"
	if index >= 0 && index < len(items) && items[index].Ref != "" {
		ref = items[index].Ref
	}
	p.audit.Append("ingest", "ingest-batch", ref, "deny",
		"stage="+stage+";status="+string(status)+";index="+itoa64(int64(index))+";size="+itoa64(int64(len(items))),
		nil, at)
	p.log.Printf("level=ingest stage=%s batch_index=%d size=%d status=%s decision=deny",
		stage, index, len(items), status)
	return &BatchResult{Status: status, FailedIndex: index, Reason: reason},
		&domain.RejectError{Status: status, Ref: ref, Reason: reason}
}

func batchRef(items []BatchItem) string {
	for _, it := range items {
		if it.Ref != "" {
			return it.Ref
		}
	}
	return "batch"
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
