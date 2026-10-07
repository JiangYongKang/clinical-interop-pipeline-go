package pipeline

import (
	"sort"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/store"
)

// BatchMessage is one raw wire message of an atomic batch submission.
type BatchMessage struct {
	Ref     string
	Format  string
	Payload []byte
}

// BatchResult is the pipeline-level result of IngestBatch.
type BatchResult struct {
	BatchID string
	Status  domain.Status
	Reason  string
	Items   []store.ItemResult
}

// IngestBatch parses and normalizes every message first; if any single
// message is unacceptable (unsupported version/format, missing field, bad
// type, ...) the whole batch is rejected before touching the store. Only
// after every message normalizes is the batch handed to the store for an
// all-or-none atomic commit. Nothing rejected ever lands, and a prior failed
// submission never blocks a corrected re-submission.
func (p *Pipeline) IngestBatch(batchID string, msgs []BatchMessage) (*BatchResult, *domain.RejectError) {
	now := p.now().UTC()

	if strings.TrimSpace(batchID) == "" {
		return p.batchReject(batchID, "", domain.StatusRejectedMalformed, "batch id is required", now)
	}
	if len(msgs) == 0 {
		return p.batchReject(batchID, "", domain.StatusRejectedMalformed, "empty batch", now)
	}

	items := make([]store.BatchItem, 0, len(msgs))
	seenRef := map[string]bool{}
	seenMsg := map[string]bool{}
	for _, m := range msgs {
		ref := m.Ref
		if ref == "" {
			return p.batchReject(batchID, ref, domain.StatusRejectedMalformed, "message ref is required", now)
		}
		if seenRef[ref] {
			return p.batchReject(batchID, ref, domain.StatusRejectedDuplicateID, "duplicated ref within batch", now)
		}
		seenRef[ref] = true

		doc, perr := ingest.Parse(m.Format, m.Payload, now)
		if perr != nil {
			rj := asReject(perr)
			return p.batchReject(batchID, ref, rj.Status, "parse:"+string(rj.Status), now)
		}
		if seenMsg[doc.MessageID] {
			return p.batchReject(batchID, ref, domain.StatusRejectedDuplicateID, "duplicated message id within batch", now)
		}
		seenMsg[doc.MessageID] = true

		cs, nerr := normalize.Normalize(doc)
		if nerr != nil {
			rj := asReject(nerr)
			return p.batchReject(batchID, ref, rj.Status, "normalize:"+string(rj.Status), now)
		}
		p.log.Printf("level=batch stage=normalize batch=%s ref=%s msg_id=%s status=ok", batchID, ref, cs.MessageID)
		items = append(items, store.BatchItem{Ref: ref, CS: cs})
	}

	out := p.store.ApplyBatch(batchID, items, now)
	decision := "allow"
	if isRejectStatus(out.Status) {
		decision = "deny"
	}
	resources := []string{"Batch/" + batchID}
	msgIDs := make([]string, 0, len(items))
	for _, it := range items {
		msgIDs = append(msgIDs, "Message/"+it.CS.MessageID)
	}
	sort.Strings(msgIDs)
	resources = append(resources, msgIDs...)
	p.audit.Append("ingest", "batch", batchID, decision,
		"status="+string(out.Status)+";items="+itoa(len(items))+";basis="+out.Reason,
		resources, now)
	p.log.Printf("level=batch stage=apply batch=%s items=%d status=%s basis=%q digest=%s",
		batchID, len(items), out.Status, out.Reason, out.Digest)

	br := &BatchResult{BatchID: batchID, Status: out.Status, Reason: out.Reason, Items: out.Items}
	if decision == "deny" {
		return br, &domain.RejectError{Status: out.Status, Ref: batchID, Reason: out.Reason}
	}
	return br, nil
}

// batchReject audits the refusal (identifiers only, never raw content) and
// returns the single typed error shape. No store mutation has happened.
func (p *Pipeline) batchReject(batchID, ref string, st domain.Status, reason string, at time.Time) (*BatchResult, *domain.RejectError) {
	res := []string{"Batch/" + batchID}
	if ref != "" {
		res = append(res, "Ref/"+ref)
	}
	p.audit.Append("ingest", "batch", batchID, "deny", reason, res, at)
	p.log.Printf("level=batch stage=validate batch=%s ref=%s decision=deny reason=%s", batchID, ref, reason)
	return &BatchResult{BatchID: batchID, Status: st, Reason: reason},
		&domain.RejectError{Status: st, Ref: batchID, Reason: reason}
}
