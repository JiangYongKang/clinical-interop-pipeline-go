package audit

import (
	"strings"
	"testing"
	"time"
)

func TestLog_HashChainAndTamperDetection(t *testing.T) {
	l := New()
	at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		l.Append("actor", "export", "ref", "allow", "basis",
			[]string{"Patient/m1", "Observation/o1"}, at.Add(time.Duration(i)*time.Second))
	}
	if err := l.Verify(); err != nil {
		t.Fatalf("fresh chain must verify: %v", err)
	}
	entries := l.Entries()
	if entries[0].PrevHash != "" {
		t.Fatal("genesis prev hash must be empty")
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].PrevHash != entries[i-1].Hash {
			t.Fatal("chain linkage broken")
		}
	}

	// tamper with a historical entry and confirm verification catches it
	l.mu.Lock()
	l.events[2].Basis = "tampered"
	l.mu.Unlock()
	if err := l.Verify(); err == nil || !strings.Contains(err.Error(), "seq 3") {
		t.Fatalf("tampering must be detected at seq 3, got %v", err)
	}
}

func TestLog_DeterministicHashesAcrossInstances(t *testing.T) {
	build := func() []Event {
		l := New()
		at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
		l.Append("a", "ingest", "r", "allow", "b", []string{"x"}, at)
		return l.Entries()
	}
	a, b := build(), build()
	if a[0].Hash != b[0].Hash || a[0].Hash == "" {
		t.Fatal("identical events must produce identical hashes")
	}
}
