package raft

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestFaultPartition covers 第 2 行：leader 被切进少数派时多数派重选、旧 leader 写失败；
// 两个 follower 被切走时 leader 这边仍能提交。
func TestFaultPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	requireNetAdmin(t)
	p := newLoopbackProcessCluster(t, 5, nil, nil)
	t.Cleanup(func() { healNetwork(t) })

	leader, _ := p.writeThrough("partition-seed", "v")
	p.writeThrough("partition-seed-2", "v2")

	// 多数派侧重选：leader 与其余所有节点双向断开。
	isolateNode(t, p, leader)
	next, reply := p.writeThrough("majority-side", "v3")
	if next == leader {
		t.Fatalf("the isolated leader %s still acked a write", leader)
	}
	if toFloat(reply["term"]) == 0 {
		t.Fatalf("the majority side reply has no term: %v", reply)
	}
	status, _, err := p.put(leader, "isolated-write", "v4")
	if status == http.StatusOK {
		t.Fatalf("the isolated leader acked a write; partition did not hold")
	}
	t.Logf("majority side elected %s; isolated %s put status=%d err=%v", next, leader, status, err)

	healNetwork(t)
	deadline := time.Now().Add(20 * time.Second)
	for {
		ok := true
		for _, id := range p.ids {
			if err := p.waitValue(id, "majority-side", "v3", time.Second); err != nil {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the healed cluster did not bring every node to the new value")
		}
	}

	// 少数派隔离：切走两个非 leader，leader 这边剩 3/5，仍能提交。
	leader, _ = p.writeThrough("partition-seed-3", "v5")
	kicked := 0
	for _, id := range p.ids {
		if id == leader || kicked == 2 {
			continue
		}
		isolateNode(t, p, id)
		kicked++
	}
	if status, reply, err := p.put(leader, "leader-side", "v6"); err != nil || status != http.StatusOK {
		t.Fatalf("the leader with 3/5 alive did not commit: status=%d reply=%v err=%v", status, reply, err)
	}

	healNetwork(t)
	for _, id := range p.ids {
		if err := p.waitValue(id, "leader-side", "v6", 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	assertSingleLeaderPerTerm(t, p)
}

// TestFaultOneWayLeaderIsolation covers 第 3 行：只断一个方向，两个方向各跑一次。
func TestFaultOneWayLeaderIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	requireNetAdmin(t)
	p := newLoopbackProcessCluster(t, 5, nil, nil)
	t.Cleanup(func() { healNetwork(t) })

	leader, _ := p.writeThrough("oneway-seed", "v")

	// leader 收得到、发不出去：follower 收不到心跳，重选；旧 leader 看到更高 term 下台。
	for _, id := range p.ids {
		if id != leader {
			dropDirection(t, p, leader, id)
		}
	}
	next, reply := p.writeThrough("leader-cannot-send", "v2")
	if next == leader {
		t.Fatalf("the leader who cannot send still kept leadership")
	}
	if toFloat(reply["term"]) == 0 {
		t.Fatalf("reply has no term: %v", reply)
	}
	healNetwork(t)
	time.Sleep(500 * time.Millisecond)

	// leader 发得出去、收不到回执：follower 有心跳不会重选，写凑不齐多数派。
	leader, _ = p.writeThrough("oneway-seed-2", "v3")
	for _, id := range p.ids {
		if id != leader {
			dropDirection(t, p, id, leader)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	if status, _, _ := p.put(leader, "leader-cannot-receive", "v4"); status == http.StatusOK {
		t.Fatalf("a write was acked with every reply blocked")
	}
	// 强一致读要么 503，要么超时；不能返回 200。
	if status, _, err := p.getLinear(leader, "oneway-seed-2"); status == http.StatusOK {
		t.Fatalf("a linearizable read was served without a quorum: status=%d err=%v", status, err)
	}
	assertSingleLeaderPerTerm(t, p)

	healNetwork(t)
	if _, reply := p.writeThrough("oneway-healed", "v5"); toFloat(reply["term"]) == 0 {
		t.Fatalf("the healed cluster did not answer a write: %v", reply)
	}
}

// TestFaultPacketLevel covers 第 4 行：延迟超过 rpcTimeout、丢包、重复、乱序。
// 延迟档只证明超时重试期间不产生错误提交；恢复后所有已确认值必须原样读回。
func TestFaultPacketLevel(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	requireNetAdmin(t)

	t.Run("delay-over-rpc-timeout", func(t *testing.T) {
		p := newLoopbackProcessCluster(t, 5, nil, nil)
		t.Cleanup(func() { healNetwork(t) })
		leader, _ := p.writeThrough("delay-seed", "v")

		netemLo(t, "delay", "150ms")
		time.Sleep(500 * time.Millisecond)
		status, reply, _ := p.put(leader, "during-delay", "v2")
		if status == http.StatusOK {
			t.Logf("write acked despite a 150ms one-way delay (rpc timeout 100ms): %v", reply)
		}
		healNetwork(t)

		if err := p.waitValue(leader, "delay-seed", "v", 20*time.Second); err != nil {
			t.Fatal(err)
		}
		p.writeThrough("after-delay", "v3")
		for _, id := range p.ids {
			if err := p.waitValue(id, "after-delay", "v3", 20*time.Second); err != nil {
				t.Fatal(err)
			}
		}
		assertSingleLeaderPerTerm(t, p)
	})

	t.Run("loss-duplicate-reorder", func(t *testing.T) {
		p := newLoopbackProcessCluster(t, 5, nil, nil)
		t.Cleanup(func() { healNetwork(t) })
		leader, _ := p.writeThrough("packet-seed", "v")

		netemLo(t, "delay", "2ms", "loss", "20%", "duplicate", "10%", "reorder", "25%", "50%")
		acked := map[string]string{"packet-seed": "v"}
		for i := 0; i < 60; i++ {
			key := fmt.Sprintf("packet-%02d", i)
			value := fmt.Sprintf("v-%02d", i)
			if status, _, err := p.put(leader, key, value); err == nil && status == http.StatusOK {
				acked[key] = value
			}
		}
		healNetwork(t)

		next, _ := p.writeThrough("packet-heal", "v")
		for key, want := range acked {
			if err := p.waitValue(next, key, want, 30*time.Second); err != nil {
				t.Fatal(err)
			}
		}
		// 没有重启的进程里，同一个 index 只该 apply 一次。
		assertNoDuplicateApply(t, p, next)
		assertSingleLeaderPerTerm(t, p)
	})
}

// assertNoDuplicateApply 扫一台机器的 debug 日志，同一个 index 的 entry applied 只许出现一次。
func assertNoDuplicateApply(t *testing.T, p *processCluster, id string) {
	t.Helper()
	seen := map[uint64]int{}
	for _, line := range strings.Split(p.logText(id), "\n") {
		if !strings.Contains(line, `"msg":"entry applied"`) {
			continue
		}
		var rec struct {
			Index uint64 `json:"index"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Index > 0 {
			seen[rec.Index]++
		}
	}
	for index, n := range seen {
		if n > 1 {
			t.Errorf("%s applied index %d %d times without a restart", id, index, n)
		}
	}
}
