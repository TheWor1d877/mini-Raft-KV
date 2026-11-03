package raft

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// craftLeaderLog 把 Leader 的日志换成 {2@1, 2@2, 2@3, 3@4}：三条旧任期在前、
// 一条当前任期在后，不变式 3 的判据才有东西可切。调用方必须持锁。
func craftLeaderLog(t *testing.T, n *Node) {
	t.Helper()
	if err := n.log.truncateTo(0); err != nil {
		t.Fatalf("clear the fixture log: %v", err)
	}
	for index := uint64(1); index <= 3; index++ {
		mustAppend(t, n.log, 2, index, "")
	}
	mustAppend(t, n.log, 3, 4, "")
}

// 不变式 3：旧任期条目复制到多数派之后 commitIndex 一步都不许动，
// 等当前任期的条目提交了才被间接带走（3.1 §8、3.2 §6 的「允许旧任期直接推进」变异）。
func TestCommitCarriesOldTermEntriesOnlyIndirectly(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	craftLeaderLog(t, n)
	n.commitIndex, n.lastApplied = 0, 0
	n.matchIndex["node-2"], n.matchIndex["node-3"] = 3, 3
	n.advanceCommit()
	afterOldTerm := n.commitIndex

	n.matchIndex["node-2"], n.matchIndex["node-3"] = 4, 4
	n.advanceCommit()
	afterCurrentTerm := n.commitIndex
	n.mu.Unlock()

	if afterOldTerm != 0 {
		t.Fatalf("commitIndex = %d after three old-term entries reached a majority, want 0", afterOldTerm)
	}
	if afterCurrentTerm != 4 {
		t.Fatalf("commitIndex = %d once the current-term entry is replicated, want 4", afterCurrentTerm)
	}
}

// 当选那一轮的 no-op 提交推进 commitIndex；command 为空的条目跳过 KV，
// 但游标照走——KV 的 WAL 一个字节都没有（3.1 §7 应用、D4）。
func TestApplyMovesCursorWithoutWritingEmptyEntries(t *testing.T) {
	c, n, _ := leaderFixture(t)

	if e, ok := n.log.entryAt(2); !ok || len(e.cmd) != 0 {
		t.Fatalf("index 2 = %+v ok=%v, want the no-op the fixture committed", e, ok)
	}
	if got := n.Stats(); got.CommitIndex != 2 || got.LastApplied != 2 {
		t.Fatalf("stats = %+v, want both cursors at the no-op", got)
	}
	if got, err := n.Get("k"); !errors.Is(err, kvstore.ErrKeyNotFound) || got != "" {
		t.Errorf("Get(k) = %q %v, want nothing in the state machine", got, err)
	}
	if size := fileSize(t, c.kvCfg["node-1"].WALPath(1)); size != 0 {
		t.Errorf("kv wal bytes = %d, want 0: an empty command never reaches the KV", size)
	}
}

// apply 按 index 升序把已提交条目写进状态机，Propose 在 apply 完成之后才返回（3.1 §7 应用）。
func TestApplyRunsInIndexOrderAndFeedsKV(t *testing.T) {
	_, n, logs := leaderFixture(t)
	want := []struct{ key, value string }{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}

	for i, kv := range want {
		if _, _, err := n.Propose(context.Background(), kv.key, kv.value); err != nil {
			t.Fatalf("propose %s: %v", kv.key, err)
		}
		// 返回时这一条已经 apply：客户端拿到 200 的时候值一定读得到（不变式 4）。
		if got, err := n.Get(kv.key); err != nil || got != kv.value {
			t.Fatalf("Get(%s) = %q %v, want %s right after Propose returned", kv.key, got, err, kv.value)
		}
		if got := n.Stats().LastApplied; got != uint64(3+i) {
			t.Errorf("lastApplied = %d after propose %d, want %d", got, i, 3+i)
		}
	}

	// 观察者日志里 5 个节点共用一份，读的是 node-1 自己那一条链。
	applied := logs.FilterMessage("entry applied").FilterField(zap.String("node_id", "node-1")).All()
	if len(applied) != 5 {
		t.Fatalf("entry applied events = %d, want one per index 1..5: %+v", len(applied), applied)
	}
	for i, entry := range applied {
		if got := entry.ContextMap()["index"]; got != uint64(i+1) {
			t.Errorf("applied %d = %v, want index %d: the loop is strictly ascending", i, got, i+1)
		}
	}
}

// 一批的提交点是 kv.Flush：这批里只有空 command 的条目，Put 一步都不走，失败只能来自 Flush。
// 这时内存游标与 state 文件都必须停在上一批的边界上（不变式 4）。
// 这条专门杀「Flush 成功之前就把 lastApplied 写回 state」那处变异（3.2 §6）。
func TestFlushFailureKeepsLastAppliedOffState(t *testing.T) {
	c, n, logs := leaderFixture(t)
	before := n.Stats()

	c.breakKV("node-1")
	n.mu.Lock()
	mustAppend(t, n.log, n.term, before.LastLogIndex+1, "")
	n.matchIndex["node-2"] = before.LastLogIndex + 1
	n.matchIndex["node-3"] = before.LastLogIndex + 1
	n.advanceCommit()
	err := n.applyCommitted()
	got := Stats{CommitIndex: n.commitIndex, LastApplied: n.lastApplied}
	n.mu.Unlock()

	if !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("applyCommitted = %v, want ErrIO from the failed flush", err)
	}
	if got.CommitIndex != before.LastLogIndex+1 {
		t.Errorf("commitIndex = %d, want %d: replication is not what failed", got.CommitIndex, before.LastLogIndex+1)
	}
	if got.LastApplied != before.LastApplied {
		t.Errorf("lastApplied = %d, want it kept at %d", got.LastApplied, before.LastApplied)
	}
	disk, err := newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if disk.LastApplied != before.LastApplied {
		t.Errorf("state file lastApplied = %d, want %d: nothing written before Flush succeeded",
			disk.LastApplied, before.LastApplied)
	}
	// 失败之后节点照常跑：Tick 末尾与每条 success 回执都会再撞一次 Flush，
	// 日志里至少有一条 apply failed（3.1 §10 的下游链）。
	n.Tick()
	if count := countLogs(logs, "apply failed"); count < 1 {
		t.Errorf("apply failed entries = %d, want at least 1: %+v", count, logs.FilterMessage("apply failed").All())
	}

	// 重启补 apply：同一条已提交的条目从上一批的边界重试一次就追平（3.1 §7 启动）。
	recovered := c.recover("node-1")
	if got := recovered.Stats(); got.LastApplied != got.CommitIndex || got.LastApplied != before.LastLogIndex+1 {
		t.Errorf("stats after recovery = %+v, want both cursors at %d", got, before.LastLogIndex+1)
	}
	disk, err = newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load state after recovery: %v", err)
	}
	if disk.LastApplied != before.LastLogIndex+1 {
		t.Errorf("state file lastApplied = %d, want %d after the catch-up apply", disk.LastApplied, before.LastLogIndex+1)
	}
}

// 写路径关掉之后，客户端那条链拿到的是 ErrIO：条目已复制、已提交，但进不了状态机（D5 的下游）。
func TestProposeFailsWhenStateMachineIsClosed(t *testing.T) {
	c, n, _ := leaderFixture(t)
	before := n.Stats()

	c.breakKV("node-1")
	_, _, err := n.Propose(context.Background(), "k", "v")
	if !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("propose against a closed kv = %v, want ErrIO", err)
	}
	got := n.Stats()
	if got.CommitIndex != before.CommitIndex+1 {
		t.Errorf("commitIndex = %d, want %d: the entry replicated and committed anyway",
			got.CommitIndex, before.CommitIndex+1)
	}
	if got.LastApplied != before.LastApplied {
		t.Errorf("lastApplied = %d, want it kept at %d", got.LastApplied, before.LastApplied)
	}
	if value, err := n.Get("k"); err == nil || value != "" {
		t.Errorf("Get(k) = %q %v, want the write invisible until it reaches the state machine", value, err)
	}
}

// 崩溃恢复之后 map 的值等于 lastApplied 之前的条目集合；把游标倒回去重复 apply 同一条，
// 结果不变（D4 的幂等、3.2 §3 的 commit 行）。
func TestRecoveredStateMatchesAppliedEntries(t *testing.T) {
	c, n, _ := leaderFixture(t)
	if _, _, err := n.Propose(context.Background(), "k1", "v1"); err != nil {
		t.Fatalf("propose k1: %v", err)
	}
	if _, _, err := n.Propose(context.Background(), "k2", "v2"); err != nil {
		t.Fatalf("propose k2: %v", err)
	}
	stats := n.Stats()

	// 造出「条目已提交、游标只走到第一条」的盘上形状：raft 日志原样留着，只把 state 的游标拨回去。
	restarted := c.reseed("node-1",
		State{CurrentTerm: stats.Term, VotedFor: n.votedFor, CommitIndex: stats.CommitIndex, LastApplied: 1}, nil)
	if got := restarted.Stats(); got.CommitIndex != stats.CommitIndex {
		t.Fatalf("stats after restart = %+v, want commitIndex kept at %d", got, stats.CommitIndex)
	}
	for key, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		if got, err := restarted.Get(key); err != nil || got != want {
			t.Fatalf("Get(%s) after restart = %q %v, want %s", key, got, err, want)
		}
	}
	if got := restarted.Stats(); got.LastApplied != stats.LastApplied {
		t.Fatalf("lastApplied = %d, want the catch-up apply to reach %d", got.LastApplied, stats.LastApplied)
	}

	// 重复 apply 同一条：游标倒回 1 再走一遍，两个 key 的值不变。
	restarted.mu.Lock()
	restarted.lastApplied = 1
	err := restarted.applyCommitted()
	restarted.mu.Unlock()
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	for key, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		if got, err := restarted.Get(key); err != nil || got != want {
			t.Errorf("Get(%s) after re-apply = %q %v, want %s unchanged", key, got, err, want)
		}
	}
	if got := restarted.Stats(); got.LastApplied != stats.LastApplied {
		t.Errorf("lastApplied = %d, want it back at %d", got.LastApplied, stats.LastApplied)
	}
}
