package store

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/normalize"
)

// BatchItem is one normalized message inside an atomic batch submission.
type BatchItem struct {
	Ref string
	CS  *normalize.ChangeSet
}

// ItemResult is the per-message disposition inside a batch outcome.
type ItemResult struct {
	Ref        string
	MessageID  string
	PatientMRN string
	VisitID    string
	Status     domain.Status
	Reason     string
	Digest     string
}

// BatchOutcome is the disposition of one whole-batch submission. A batch
// either commits completely or leaves no trace: a failed validation rejects
// the batch and mutates neither data nor the idempotency tables, so the
// caller can fix the offending message and re-submit the whole batch.
type BatchOutcome struct {
	BatchID string
	Status  domain.Status
	Reason  string
	Digest  string
	Items   []ItemResult
}

// batchRec remembers one committed batch for replay detection.
type batchRec struct {
	Digest string
	Items  []string // message ids, sorted
}

// batchDigest is the SHA-256 over the sorted per-message digests, so the
// same batch resubmitted in a different order still hashes identically.
func batchDigest(items []BatchItem) string {
	digs := make([]string, len(items))
	for i, it := range items {
		digs[i] = ContentDigest(it.CS)
	}
	sort.Strings(digs)
	sum := sha256.Sum256([]byte(strings.Join(digs, "\n")))
	return hex.EncodeToString(sum[:])
}

// ApplyBatch commits a whole batch atomically.
//
// All messages in a batch are validated against the stored state and against
// each other before anything is written: version/stale/type/id checks all
// run as one dry-run. Locks are acquired in one global order (distinct shard
// indices ascending, then the idempotency lock last), so concurrent batches
// can never deadlock. Semantics:
//
//   - any one unacceptable message       -> whole batch rejected, no effect;
//   - identical batch id + identical set -> deduplicated, reproducible;
//   - batch id reused with other content, or any message id of the batch was
//     already committed outside this batch -> rejected_batch;
//   - after fixing the offending message the exact batch (or a new batch id)
//     commits fully — a prior failure never poisons it.
func (s *Store) ApplyBatch(batchID string, items []BatchItem, now time.Time) BatchOutcome {
	now = now.UTC()
	res := BatchOutcome{BatchID: batchID}
	fail := func(st domain.Status, reason string) BatchOutcome {
		res.Status = st
		res.Reason = reason
		return res
	}
	if strings.TrimSpace(batchID) == "" {
		return fail(domain.StatusRejectedMalformed, "batch id is required")
	}
	if len(items) == 0 {
		return fail(domain.StatusRejectedMalformed, "empty batch")
	}

	// Basic per-item sanity + intra-batch duplicate message ids. A duplicated
	// message id inside one submission can never be an atomic all-or-none set.
	itemByMsg := map[string]int{}
	shards := map[uint32]bool{}
	for i, it := range items {
		if it.CS == nil {
			return fail(domain.StatusRejectedMalformed, "batch item without normalized content")
		}
		mrn := messageMRN(it.CS)
		if mrn == "" {
			return fail(domain.StatusRejectedMissingField, "message contains no attributable patient")
		}
		if _, dup := itemByMsg[it.CS.MessageID]; dup {
			return fail(domain.StatusRejectedDuplicateID, "duplicated message id within batch: "+it.CS.MessageID)
		}
		itemByMsg[it.CS.MessageID] = i
		shards[shardIndex(mrn)] = true
	}
	res.Digest = batchDigest(items)

	// Lock every involved shard in ascending index order, then the global
	// idempotency lock last: one global order => no lock cycle.
	order := make([]uint32, 0, len(shards))
	for idx := range shards {
		order = append(order, idx)
	}
	sort.Slice(order, func(a, b int) bool { return order[a] < order[b] })
	for _, idx := range order {
		s.shards[idx].mu.Lock()
	}
	defer func() {
		for _, idx := range order {
			s.shards[idx].mu.Unlock()
		}
	}()
	s.idemMu.Lock()
	defer s.idemMu.Unlock()

	// Batch-level idempotency.
	if rec, ok := s.batchIdem[batchID]; ok {
		if rec.Digest == res.Digest {
			res.Status = domain.StatusDeduplicated
			res.Reason = "identical re-delivery of already applied batch"
			res.Items = s.batchItemResults(batchID, items)
			return res
		}
		return fail(domain.StatusRejectedBatch, "batch id re-used with different content")
	}
	// Message ids must be unknown outside this batch, otherwise committing
	// would produce a non-reproducible partial overlap.
	for _, it := range items {
		if _, ok := s.idem[it.CS.MessageID]; ok {
			return fail(domain.StatusRejectedBatch, "batch overlaps an already committed message: "+it.CS.MessageID)
		}
	}

	// Dry-run: replay every message in deterministic message-id order against
	// the stored state plus an in-batch overlay. On the first violation the
	// whole batch is rejected without mutating anything.
	sortedIdx := make([]int, len(items))
	for i := range items {
		sortedIdx[i] = i
	}
	sort.Slice(sortedIdx, func(a, b int) bool { return items[sortedIdx[a]].CS.MessageID < items[sortedIdx[b]].CS.MessageID })

	pending := newPending()
	mrns := map[string]bool{}
	for _, i := range sortedIdx {
		cs := items[i].CS
		mrn := messageMRN(cs)
		mrns[mrn] = true
		sh := s.shards[shardIndex(mrn)]

		if cs.Patient != nil {
			if prov, ok := pending.patient[mrn]; ok {
				if !isNewer(cs.Patient.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "batch contains two non-monotonic patient conclusions: "+mrn)
				}
			} else if prov, ok := sh.patientProv[mrn]; ok {
				if !isNewer(cs.Patient.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "patient conclusion is not newer than stored version: "+mrn)
				}
			}
			pending.patient[mrn] = provenance{cs.Patient.UpdatedAt, cs.Rank, cs.MessageID}
		}
		if cs.Encounter != nil {
			e := cs.Encounter
			if prov, ok := pending.encounter[e.VisitID]; ok {
				if !isNewer(e.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "batch contains two non-monotonic encounter conclusions: "+e.VisitID)
				}
			} else if prov, ok := sh.encProv[e.VisitID]; ok {
				if !isNewer(e.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "encounter conclusion is not newer than stored version: "+e.VisitID)
				}
			}
			pending.encounter[e.VisitID] = provenance{e.UpdatedAt, cs.Rank, cs.MessageID}
		}
		for _, o := range cs.Observations {
			// type conflict vs stored value
			if ex, ok := sh.observations[o.ObservationID]; ok {
				if (ex.ValueNumber == nil) != (o.ValueNumber == nil) {
					return fail(domain.StatusRejectedTypeConflict, "observation value type conflicts with stored value: "+o.ObservationID)
				}
			}
			// type conflict vs an earlier message in this batch
			if po, ok := pending.obsEntity[o.ObservationID]; ok {
				if (po.ValueNumber == nil) != (o.ValueNumber == nil) {
					return fail(domain.StatusRejectedTypeConflict, "observation value type conflicts within batch: "+o.ObservationID)
				}
			}
			if prov, ok := pending.observation[o.ObservationID]; ok {
				if !isNewer(o.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "batch contains two non-monotonic observation conclusions: "+o.ObservationID)
				}
			} else if prov, ok := sh.obsProv[o.ObservationID]; ok {
				if !isNewer(o.UpdatedAt, cs.Rank, cs.MessageID, prov) {
					return fail(domain.StatusStale, "observation conclusion is not newer than stored version: "+o.ObservationID)
				}
			}
			pending.observation[o.ObservationID] = provenance{o.UpdatedAt, cs.Rank, cs.MessageID}
			pending.obsEntity[o.ObservationID] = o
		}
	}

	// Commit phase: dry-run passed, so every merge is known to succeed.
	anyUpdated := false
	itemStatus := map[string]domain.Status{}
	for _, i := range sortedIdx {
		cs := items[i].CS
		mrn := messageMRN(cs)
		sh := s.shards[shardIndex(mrn)]
		updated := false

		if cs.Patient != nil {
			if _, exists := sh.patients[mrn]; exists {
				mergePatient(sh.patients[mrn], cs.Patient)
				updated = true
			} else {
				sh.patients[mrn] = clonePatient(cs.Patient)
			}
			sh.patientProv[mrn] = provenance{cs.Patient.UpdatedAt, cs.Rank, cs.MessageID}
			sh.patRevs[mrn] = append(sh.patRevs[mrn], EntityRevision{
				Kind: "patient", ID: mrn, MRN: mrn,
				UpdatedAt: cs.Patient.UpdatedAt, Rank: cs.Rank, Writer: cs.MessageID,
				RecordedAt: now, Patient: clonePatient(sh.patients[mrn]),
			})
		}
		if cs.Encounter != nil {
			e := cs.Encounter
			if _, exists := sh.encounters[e.VisitID]; exists {
				mergeEncounter(sh.encounters[e.VisitID], e)
				updated = true
			} else {
				sh.encounters[e.VisitID] = cloneEncounter(e)
			}
			sh.encProv[e.VisitID] = provenance{e.UpdatedAt, cs.Rank, cs.MessageID}
			sh.encRevs[e.VisitID] = append(sh.encRevs[e.VisitID], EntityRevision{
				Kind: "encounter", ID: e.VisitID, MRN: e.MRN,
				UpdatedAt: e.UpdatedAt, Rank: cs.Rank, Writer: cs.MessageID,
				RecordedAt: now, Encounter: cloneEncounter(sh.encounters[e.VisitID]),
			})
		}
		for _, o := range cs.Observations {
			if _, exists := sh.observations[o.ObservationID]; exists {
				mergeObservation(sh.observations[o.ObservationID], o)
				updated = true
			} else {
				sh.observations[o.ObservationID] = cloneObservation(o)
			}
			sh.obsProv[o.ObservationID] = provenance{o.UpdatedAt, cs.Rank, cs.MessageID}
			sh.obsRevs[o.ObservationID] = append(sh.obsRevs[o.ObservationID], EntityRevision{
				Kind: "observation", ID: o.ObservationID, MRN: o.MRN,
				UpdatedAt: o.UpdatedAt, Rank: cs.Rank, Writer: cs.MessageID,
				RecordedAt: now, Observation: cloneObservation(sh.observations[o.ObservationID]),
			})
		}
		if updated {
			anyUpdated = true
			itemStatus[cs.MessageID] = domain.StatusUpdated
		} else {
			itemStatus[cs.MessageID] = domain.StatusAccepted
		}
		s.idem[cs.MessageID] = idemRec{Digest: ContentDigest(cs), MRN: mrn}
	}

	msgIDs := make([]string, 0, len(items))
	for _, it := range items {
		msgIDs = append(msgIDs, it.CS.MessageID)
	}
	sort.Strings(msgIDs)
	s.batchIdem[batchID] = batchRec{Digest: res.Digest, Items: msgIDs}

	res.Items = make([]ItemResult, len(items))
	for i, it := range items {
		cs := it.CS
		ir := ItemResult{
			Ref: it.Ref, MessageID: cs.MessageID, PatientMRN: messageMRN(cs),
			Status: itemStatus[cs.MessageID], Digest: ContentDigest(cs),
		}
		if cs.Encounter != nil {
			ir.VisitID = cs.Encounter.VisitID
		}
		res.Items[i] = ir
	}
	if anyUpdated {
		res.Status = domain.StatusUpdated
		res.Reason = "batch committed (some entities updated)"
	} else {
		res.Status = domain.StatusAccepted
		res.Reason = "batch committed (all entities new)"
	}
	return res
}

func (s *Store) batchItemResults(batchID string, items []BatchItem) []ItemResult {
	out := make([]ItemResult, len(items))
	for i, it := range items {
		cs := it.CS
		ir := ItemResult{
			Ref: it.Ref, MessageID: cs.MessageID, PatientMRN: messageMRN(cs),
			Status: domain.StatusDeduplicated, Digest: ContentDigest(cs),
		}
		if cs.Encounter != nil {
			ir.VisitID = cs.Encounter.VisitID
		}
		out[i] = ir
	}
	_ = batchID
	return out
}

// pendingOverlay holds the provisional conclusions written by earlier
// messages of the batch currently being dry-run validated.
type pendingOverlay struct {
	patient     map[string]provenance
	encounter   map[string]provenance
	observation map[string]provenance
	obsEntity   map[string]*domain.Observation
}

func newPending() *pendingOverlay {
	return &pendingOverlay{
		patient:     map[string]provenance{},
		encounter:   map[string]provenance{},
		observation: map[string]provenance{},
		obsEntity:   map[string]*domain.Observation{},
	}
}
