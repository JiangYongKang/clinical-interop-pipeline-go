package store

import (
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

// BatchOutcome is the all-or-nothing result of applying one batch.
//
// On rejection (Status is a rejection status), FailedIndex identifies the
// offending message, Results is empty, and not a single message of the batch
// has been committed. On success Results carries one Outcome per input message
// in submission order; Status is:
//   - deduplicated when every message was an identical re-delivery,
//   - accepted otherwise (new data landed; per-item Outcomes hold details).
type BatchOutcome struct {
	Status      domain.Status
	Results     []Outcome
	FailedIndex int
	Reason      string
}

// ApplyBatch validates and commits a whole batch atomically: either every
// message is accepted/deduplicated, or none of them lands.
//
// Semantics:
//   - Every message is checked (idempotency, stale, type conflict, attributable
//     MRN) against committed state AND the earlier messages of this batch; the
//     first hard failure aborts everything and installs nothing.
//   - The same message id carrying the same content, anywhere in the batch or
//     in a prior run, is recognized as deduplicated; same id with different
//     content rejects the whole batch as conflict.
//   - Commit happens under one ordered set of shard locks, so concurrent single
//     ingests can never observe a half batch.
//   - Because nothing is registered on failure, fixing the rejected message
//     and resending the whole batch applies normally.
func (s *Store) ApplyBatch(css []*normalize.ChangeSet, now time.Time) BatchOutcome {
	if len(css) == 0 {
		return BatchOutcome{Status: domain.StatusRejectedMissingField,
			FailedIndex: -1, Reason: "empty batch"}
	}
	outs, idx, rj := s.applyTx(css, now)
	if rj != nil {
		return BatchOutcome{
			Status:      rj.Status,
			Results:     nil,
			FailedIndex: idx,
			Reason:      rj.Reason,
		}
	}
	allDedup := true
	for i := range outs {
		if outs[i].Status != domain.StatusDeduplicated {
			allDedup = false
			break
		}
	}
	status := domain.StatusAccepted
	if allDedup {
		status = domain.StatusDeduplicated
	}
	return BatchOutcome{Status: status, Results: outs}
}
