package store

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

func updateFor(msgID string, seq int64, name string) *model.Update {
	return &model.Update{
		MessageID: msgID,
		Sequence:  seq,
		Patients: []model.Patient{{
			ID: "PAT:MRN1001", MRN: "MRN1001", Name: name, Sequence: seq,
		}},
	}
}

func TestOutOfOrderStaleRejected(t *testing.T) {
	s := New()
	if r := s.Apply(updateFor("m-new", 20, "Newer")); r.Status != ingest.StatusApplied {
		t.Fatalf("newer apply: %+v", r)
	}
	r := s.Apply(updateFor("m-old", 10, "Older"))
	if r.Status != ingest.StatusStale || r.Reason != ingest.ReasonStaleVersion {
		t.Fatalf("stale expected, got %+v", r)
	}
	p, _, _ := s.Snapshot()
	if p[0].Name != "Newer" || p[0].Sequence != 20 {
		t.Fatalf("stale update overwrote newer data: %+v", p[0])
	}
}

func TestDuplicateMessageIdempotent(t *testing.T) {
	s := New()
	first := s.Apply(updateFor("m1", 5, "A"))
	second := s.Apply(updateFor("m1", 5, "A"))
	if second.Status != ingest.StatusDeduped || second.Reason != ingest.ReasonDuplicate {
		t.Fatalf("dedup expected, got %+v", second)
	}
	if first.Applied != second.Applied {
		t.Fatalf("replayed result must match first verdict: %+v vs %+v", first, second)
	}
	p, _, _ := s.Snapshot()
	if len(p) != 1 {
		t.Fatalf("duplicate message created records: %d", len(p))
	}
}

func TestSameSequenceDifferentMessageDeduped(t *testing.T) {
	s := New()
	s.Apply(updateFor("m1", 5, "A"))
	r := s.Apply(updateFor("m2", 5, "B"))
	if r.Status != ingest.StatusDeduped {
		t.Fatalf("equal sequence should dedupe, got %+v", r)
	}
	p, _, _ := s.Snapshot()
	if p[0].Name != "A" {
		t.Fatalf("equal-sequence update must not overwrite: %+v", p[0])
	}
}

// 并发：同一患者大量并发更新，不能出现重复记录、丢失更新或死锁。
func TestConcurrentSamePatient(t *testing.T) {
	s := New()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Apply(updateFor(fmt.Sprintf("m-%d", i), int64(i), fmt.Sprintf("v%d", i)))
		}(i)
	}
	wg.Wait()
	p, _, _ := s.Snapshot()
	if len(p) != 1 {
		t.Fatalf("duplicate patient records: %d", len(p))
	}
	if p[0].Sequence != n-1 || p[0].Name != fmt.Sprintf("v%d", n-1) {
		t.Fatalf("lost update: final seq=%d name=%s", p[0].Sequence, p[0].Name)
	}
}

// 并发重复投递同一消息：只允许一个生效。
func TestConcurrentDuplicateMessage(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	results := make([]ApplyResult, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = s.Apply(updateFor("same-msg", 1, "A"))
		}(i)
	}
	wg.Wait()
	applied, deduped := 0, 0
	for _, r := range results {
		switch r.Status {
		case ingest.StatusApplied:
			applied++
		case ingest.StatusDeduped:
			deduped++
		}
	}
	if applied != 1 || deduped != 49 {
		t.Fatalf("applied=%d deduped=%d", applied, deduped)
	}
}

// 跨版本（HL7 与 FHIR 来源）按同一序号轴裁决。
func TestCrossSourceVersionArbitration(t *testing.T) {
	s := New()
	s.Apply(&model.Update{MessageID: "hl7", SourceKind: model.SourceHL7v2, Sequence: 30,
		Patients: []model.Patient{{ID: "PAT:MRN1001", Name: "FromHL7", Sequence: 30}}})
	r := s.Apply(&model.Update{MessageID: "fhir", SourceKind: model.SourceFHIR, Sequence: 25,
		Patients: []model.Patient{{ID: "PAT:MRN1001", Name: "FromFHIR", Sequence: 25}}})
	if r.Status != ingest.StatusStale {
		t.Fatalf("older fhir update must be stale: %+v", r)
	}
	p, _, _ := s.Snapshot()
	if p[0].Name != "FromHL7" {
		t.Fatalf("cross-source arbitration wrong: %+v", p[0])
	}
}

// 同一批数据以不同顺序重复运行，最终快照必须一致（可重复验证）。
func TestDeterministicRegardlessOfOrder(t *testing.T) {
	run := func(order []int) []model.Patient {
		s := New()
		for _, i := range order {
			s.Apply(updateFor(fmt.Sprintf("m-%d", i), int64(i), fmt.Sprintf("v%d", i)))
		}
		p, _, _ := s.Snapshot()
		return p
	}
	base := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	want := run(base)
	for trial := 0; trial < 20; trial++ {
		shuffled := append([]int(nil), base...)
		rand.New(rand.NewSource(int64(trial))).Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got := run(shuffled)
		if len(got) != len(want) || got[0].Sequence != want[0].Sequence || got[0].Name != want[0].Name {
			t.Fatalf("trial %d: order-dependent result: %+v vs %+v", trial, got, want)
		}
	}
}
