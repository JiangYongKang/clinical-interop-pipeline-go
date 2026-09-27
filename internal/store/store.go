// Package store 提供并发安全、按版本序号裁决的幂等存储。
//
// 并发模型：写入路径使用单一互斥锁，读快照使用读锁。
// 任何时刻只持有一把锁、锁内不调用外部代码，因此不存在死锁与锁序问题。
package store

import (
	"log"
	"sort"
	"sync"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/model"
)

// Store 是本地内存存储。
type Store struct {
	mu           sync.RWMutex
	patients     map[string]model.Patient
	encounters   map[string]model.Encounter
	observations map[string]model.Observation
	seen         map[string]ApplyResult // messageID -> 首次裁决结果（幂等重放）
}

// New 创建空存储。
func New() *Store {
	return &Store{
		patients:     map[string]model.Patient{},
		encounters:   map[string]model.Encounter{},
		observations: map[string]model.Observation{},
		seen:         map[string]ApplyResult{},
	}
}

// ApplyResult 描述一次写入裁决结果，可重复验证。
type ApplyResult struct {
	MessageID    string
	Status       ingest.Status
	Reason       ingest.Reason
	Applied      int
	SkippedStale int
	SkippedDup   int
}

// Apply 原子地应用一次归一化更新：幂等去重、按序号裁决新旧。
// 规则：序号更大者胜出；序号相等视为同一逻辑版本的重复投递，去重跳过；
// 序号更小为过期数据，不得覆盖新结论。
func (s *Store) Apply(u *model.Update) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, ok := s.seen[u.MessageID]; ok {
		res := prev
		res.Status = ingest.StatusDeduped
		res.Reason = ingest.ReasonDuplicate
		log.Printf("store apply message_id=%s status=%s reason=%s (idempotent replay)",
			u.MessageID, res.Status, res.Reason)
		return res
	}

	res := ApplyResult{MessageID: u.MessageID}

	for _, p := range u.Patients {
		if cur, ok := s.patients[p.ID]; ok {
			switch {
			case p.Sequence > cur.Sequence:
				s.patients[p.ID] = p
				res.Applied++
			case p.Sequence == cur.Sequence:
				res.SkippedDup++
			default:
				res.SkippedStale++
			}
		} else {
			s.patients[p.ID] = p
			res.Applied++
		}
	}
	for _, e := range u.Encounters {
		if cur, ok := s.encounters[e.ID]; ok {
			switch {
			case e.Sequence > cur.Sequence:
				s.encounters[e.ID] = e
				res.Applied++
			case e.Sequence == cur.Sequence:
				res.SkippedDup++
			default:
				res.SkippedStale++
			}
		} else {
			s.encounters[e.ID] = e
			res.Applied++
		}
	}
	for _, o := range u.Observations {
		if cur, ok := s.observations[o.ID]; ok {
			switch {
			case o.Sequence > cur.Sequence:
				s.observations[o.ID] = o
				res.Applied++
			case o.Sequence == cur.Sequence:
				res.SkippedDup++
			default:
				res.SkippedStale++
			}
		} else {
			s.observations[o.ID] = o
			res.Applied++
		}
	}

	switch {
	case res.Applied > 0:
		res.Status = ingest.StatusApplied
	case res.SkippedDup > 0 && res.SkippedStale == 0:
		res.Status = ingest.StatusDeduped
		res.Reason = ingest.ReasonDuplicate
	default:
		res.Status = ingest.StatusStale
		res.Reason = ingest.ReasonStaleVersion
	}

	s.seen[u.MessageID] = res
	log.Printf("store apply message_id=%s seq=%d status=%s reason=%s applied=%d stale=%d dup=%d",
		u.MessageID, u.Sequence, res.Status, res.Reason, res.Applied, res.SkippedStale, res.SkippedDup)
	return res
}

// Snapshot 返回所有实体的深拷贝，按主键排序以保证输出可重复验证。
func (s *Store) Snapshot() (patients []model.Patient, encounters []model.Encounter, observations []model.Observation) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, p := range s.patients {
		patients = append(patients, p)
	}
	for _, e := range s.encounters {
		encounters = append(encounters, e)
	}
	for _, o := range s.observations {
		observations = append(observations, o)
	}
	sort.Slice(patients, func(i, j int) bool { return patients[i].ID < patients[j].ID })
	sort.Slice(encounters, func(i, j int) bool { return encounters[i].ID < encounters[j].ID })
	sort.Slice(observations, func(i, j int) bool { return observations[i].ID < observations[j].ID })
	return patients, encounters, observations
}
