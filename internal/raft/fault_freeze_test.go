package raft

import (
	"strings"
	"testing"
	"time"
)

// TestFaultFreezeShortAndLong 覆盖第 5 行：短冻不触发重选，长冻触发重选且旧 leader 下台。
// 选举超时调到 1500ms，短档 500ms 才稳定落在截止之前，长档 4s 稳定越过上界。
func TestFaultFreezeShortAndLong(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, []string{"-election-timeout-ms", "1500"}, nil)

	leader, first := p.writeThrough("freeze-warm", "v0")
	termBefore := toFloat(first["term"])

	p.freeze(leader)
	time.Sleep(500 * time.Millisecond)
	p.thaw(leader)

	same, reply := p.writeThrough("after-short-freeze", "v1")
	if same != leader {
		t.Fatalf("a 500ms freeze re-elected: %s -> %s", leader, same)
	}
	if got := toFloat(reply["term"]); got != termBefore {
		t.Fatalf("term changed across a short freeze: %v -> %v", termBefore, got)
	}

	p.freeze(leader)
	time.Sleep(4 * time.Second)
	next, second := p.writeThrough("after-long-freeze", "v2")
	if next == leader {
		t.Fatalf("a 4s freeze did not move leadership away from %s", leader)
	}
	if toFloat(second["term"]) <= termBefore {
		t.Fatalf("term did not advance after the long freeze: %v -> %v", termBefore, second["term"])
	}
	p.thaw(leader)

	deadline := time.Now().Add(10 * time.Second)
	for {
		status, raw, err := p.getLinear(leader, "after-long-freeze")
		if err == nil && status == 503 {
			t.Logf("old leader refused the linear read after thaw: %s", firstBytes(raw, 120))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old leader never refused a linear read after thaw: status=%d err=%v", status, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	assertSingleLeaderPerTerm(t, p)
}

// TestFaultLeaseServed 确认租约路径真的被走到：窗口 1450ms 大于读超时 1000ms，
// 连续线性读里必须出现 read served by lease。
func TestFaultLeaseServed(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, []string{"-election-timeout-ms", "1500"}, nil)
	leader, _ := p.writeThrough("lease-warm", "v")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, raw, err := p.getLinear(leader, "lease-warm")
		if err == nil && status == 200 {
			if strings.Contains(p.logText(leader), "read served by lease") {
				t.Logf("lease read answered: %s", firstBytes(raw, 160))
				return
			}
		}
	}
	t.Fatalf("no read was served by the lease within 10s: %s", p.logTail(leader, 12))
}
