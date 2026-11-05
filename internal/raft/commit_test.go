package raft

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

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

// 当选那一轮的 no-op 提交推进 commitIndex；command 为空的条目跳过状态机，
// 但游标照走——日志里那一帧仍在，状态机一个字节都没改（3.1 §7 应用、D4）。
func TestApplyMovesCursorWithoutWritingEmptyEntries(t *testing.T) {
	_, n, logs := leaderFixture(t)

	if e, ok := n.log.entryAt(2); !ok || len(e.cmd) != 0 {
		t.Fatalf("index 2 = %+v ok=%v, want the no-op the fixture committed", e, ok)
	}
	if got := n.Stats(); got.CommitIndex != 2 || got.LastApplied != 2 {
		t.Fatalf("stats = %+v, want both cursors at the no-op", got)
	}
	if got, err := n.Get("k"); !errors.Is(err, kvstore.ErrKeyNotFound) || got != "" {
		t.Errorf("Get(k) = %q %v, want nothing in the state machine", got, err)
	}
	if got := logs.FilterMessage("entry applied").FilterField(zap.String("component", "kv")).Len(); got != 0 {
		t.Errorf("state machine applies = %d, want 0: an empty command never reaches Apply", got)
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

// 合并之后一批的提交点只剩 state 游标写：数据的持久边界在日志追加的那次 Sync 上，
// 状态机改动只落内存。所以游标落盘失败时这一批照常生效，盘上的游标停在上一批的边界（不变式 4）。
// 这条专门杀「state 落盘排到状态机改动之前，失败就不 apply」那处变异：
// 那样这一条的值进不了 map，而日志与回执已经把它当成功（docs/8 §二、§六）。
func TestCursorWriteFailureStillAppliesTheEntry(t *testing.T) {
	c, n, logs := leaderFixture(t)
	before := n.Stats()
	command, err := kvstore.EncodeEntry("k", "v")
	if err != nil {
		t.Fatalf("encode the command: %v", err)
	}

	c.breakStateStore("node-1")
	n.mu.Lock()
	mustAppend(t, n.log, n.term, before.LastLogIndex+1, string(command))
	n.matchIndex["node-2"] = before.LastLogIndex + 1
	n.matchIndex["node-3"] = before.LastLogIndex + 1
	n.advanceCommit()
	committed := n.commitIndex
	n.mu.Unlock()

	// 提交点走生产路径那一条：Tick 末尾的 apply 撞上游标写失败，日志里留下 apply failed。
	n.Tick()
	got := n.Stats()
	if got.CommitIndex != committed || committed != before.LastLogIndex+1 {
		t.Fatalf("commitIndex = %d, want %d: replication is not what failed", got.CommitIndex, before.LastLogIndex+1)
	}
	if count := countLogs(logs, "apply failed"); count < 1 {
		t.Errorf("apply failed entries = %d, want at least 1: %+v", count, logs.FilterMessage("apply failed").All())
	}
	if got.LastApplied != before.LastLogIndex+1 {
		t.Errorf("lastApplied = %d, want the in-memory cursor to follow the state machine", got.LastApplied)
	}
	if value, err := n.Get("k"); err != nil || value != "v" {
		t.Errorf("Get(k) = %q %v, want the applied value readable: the entry is durable in the log", value, err)
	}
	disk, err := newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if disk.LastApplied != before.LastApplied {
		t.Errorf("state file lastApplied = %d, want it kept at %d: the write failed, the lag is harmless",
			disk.LastApplied, before.LastApplied)
	}

	// 游标写不进去的节点不是正常退出：Close 的落 state 那一步把 ErrIO 报出来（3.1 §4 的 Close 行）。
	if err := c.stopNode("node-1"); !errors.Is(err, kvstore.ErrIO) {
		t.Errorf("close with an unpersistable cursor = %v, want ErrIO", err)
	}
	c.start("node-1")
	recovered := c.node("node-1")

	// 盘上的游标停在旧边界，那条已经生效过的条目就此退回「在日志里、没应用」那一格：
	// apply 的内存结果不落盘就不算数，重启只看得到 state 与日志这两个文件（不变式 4）。
	if got := recovered.Stats(); got.LastApplied != before.LastApplied || got.LastLogIndex != before.LastLogIndex+1 {
		t.Fatalf("stats after restart = %+v, want the cursor at %d with %d entries in the log",
			got, before.LastApplied, before.LastLogIndex+1)
	}
	// Leader 的心跳把提交边界重新推过来：同一条日志再走一次 apply，值与游标都回来了。
	recovered.handleAppendEntries(AppendRequest{
		Term: recovered.Stats().Term, LeaderID: "node-4",
		PrevLogIndex: before.LastLogIndex + 1, PrevLogTerm: 3,
		LeaderCommit: before.LastLogIndex + 1,
	})
	if got := recovered.Stats(); got.LastApplied != before.LastLogIndex+1 || got.CommitIndex != got.LastApplied {
		t.Errorf("stats after the leader re-committed it = %+v, want both cursors at %d", got, before.LastLogIndex+1)
	}
	if value, err := recovered.Get("k"); err != nil || value != "v" {
		t.Errorf("Get(k) after the re-apply = %q %v, want the value back", value, err)
	}
	disk, err = newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load state after the re-apply: %v", err)
	}
	if disk.LastApplied != before.LastLogIndex+1 {
		t.Errorf("state file lastApplied = %d, want %d after the catch-up apply", disk.LastApplied, before.LastLogIndex+1)
	}
}

// 日志的写路径关掉之后，客户端那条链拿到的是 ErrIO：追加这一步就失败，
// 条目没进日志，也就没有提交与 apply（D5）。读路径与已经生效的写照常。
func TestProposeFailsWhenTheLogIsDisabled(t *testing.T) {
	c, n, _ := leaderFixture(t)
	before := n.Stats()
	putOne(t, n, "warm", "value")

	c.breakLog("node-1")
	_, _, err := n.Propose(context.Background(), "k", "v")
	if !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("propose against a disabled log = %v, want ErrIO", err)
	}
	if !n.writeDisabled() {
		t.Error("writeDisabled = false, want the failure to stay latched")
	}
	got := n.Stats()
	if got.CommitIndex != before.CommitIndex+1 {
		t.Errorf("commitIndex = %d, want %d: the warm write committed normally, the refused one not at all",
			got.CommitIndex, before.CommitIndex+1)
	}
	if value, err := n.Get("k"); err == nil || value != "" {
		t.Errorf("Get(k) = %q %v, want nothing: the refused entry never reached the log", value, err)
	}
	// 失效只关写：读路径照常，已经生效的值还在（D5 的下游）。
	if value, err := n.Get("warm"); err != nil || value != "value" {
		t.Errorf("Get(warm) after the failure = %q %v, want the read path untouched", value, err)
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

// waitApplyWaiter 等到有读挂进 applyWaiters：它停在那里等 apply 兑现（4.1 §7 等待应用）。
func waitApplyWaiter(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n.mu.Lock()
		parked := len(n.applyWaiters)
		n.mu.Unlock()
		if parked > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the read never parked on an apply waiter")
		}
		runtime.Gosched()
	}
}

// 已提交的条目解不出 KV 操作时，apply 停在这一条之前：游标不动、兑现不发生，
// 落在它上面的线性读只能挂在 applyWaiters 上等，等到超时才走，日志里留下三个游标
// （4.1 §8 第一条、D27、4.2 §3 的 commit 行）。
// 这条同时是「解不出来就跳过」那处变异的杀手：跳过会把游标推到读索引之后，
// 读于是拿到旧值还回成功，而客户端已经把这一条当成写进去了。
func TestLinearReadWaitsOnAnApplyThatCannotDecode(t *testing.T) {
	_, n, logs := leaderFixture(t)
	putOne(t, n, "k", "v")
	before := n.Stats()

	n.mu.Lock()
	mustAppend(t, n.log, n.term, before.LastLogIndex+1, "\x01\x00")
	n.matchIndex["node-2"] = before.LastLogIndex + 1
	n.matchIndex["node-3"] = before.LastLogIndex + 1
	n.advanceCommit()
	stats := Stats{CommitIndex: n.commitIndex, LastApplied: n.lastApplied}
	n.mu.Unlock()

	// 提交点走生产路径：Tick 末尾的 apply 撞上坏 command，日志里留下 apply failed。
	n.Tick()
	if count := countLogs(logs, "apply failed"); count < 1 {
		t.Errorf("apply failed entries = %d, want at least 1: %+v", count, logs.FilterMessage("apply failed").All())
	}
	if got := n.Stats(); got.LastApplied != before.LastApplied {
		t.Fatalf("lastApplied = %d, want it kept at %d: the batch that cannot decode never commits",
			got.LastApplied, before.LastApplied)
	}

	// 这条读落在卡住的那一段上：读索引是 commitIndex，lastApplied 还停在上一批的边界。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, _, err := n.Read(ctx, "k2", ConsistencyLinearizable)
		done <- err
	}()
	waitApplyWaiter(t, n)
	cancel()

	if err := <-done; !errors.Is(err, ErrCanceled) {
		t.Fatalf("read = %v, want ErrCanceled: the apply it waits for never comes", err)
	}
	// 超时之后那一格从 applyWaiters 里摘掉，不留下唤醒不了的等待（D28）。
	n.mu.Lock()
	waiters := len(n.applyWaiters)
	n.mu.Unlock()
	if waiters != 0 {
		t.Errorf("apply waiters = %d, want 0 after the timeout", waiters)
	}
	entry := logs.FilterMessage("read wait timed out").All()
	if len(entry) != 1 {
		t.Fatalf("read wait timed out entries = %d, want 1", len(entry))
	}
	fields := entry[0].ContextMap()
	if fields["read_index"] != uint64(stats.CommitIndex) {
		t.Errorf("read_index = %v, want %d", fields["read_index"], stats.CommitIndex)
	}
	if fields["last_applied"] != uint64(before.LastApplied) {
		t.Errorf("last_applied = %v, want %d", fields["last_applied"], before.LastApplied)
	}
	if fields["commit_index"] != uint64(stats.CommitIndex) {
		t.Errorf("commit_index = %v, want %d", fields["commit_index"], stats.CommitIndex)
	}
}

// 单节点集群的写不靠任何回执：replicaCount 恒计自己那一票，Majority()=1，
// Propose 追加之后当场推进提交点，同一次调用里就 apply 兑现（docs/8 §二）。
// 去掉 Propose 里那次 advanceCommit，提交点停在 0，这条写等不到 apply，只能按取消退出。
func TestSingleNodeCommitNeedsNoReplicaReply(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	n := c.node("node-1")

	runElection(t, n)
	if got := n.Stats(); got.Role != RoleLeader || got.CommitIndex != 1 || got.LastApplied != 1 {
		t.Fatalf("stats after the self-election = %+v, want leader with both cursors at the index-1 no-op", got)
	}
	if got := len(c.calls("node-1")); got != 0 {
		t.Errorf("RPCs sent by the one-member node = %d, want 0: the self-election has nobody to ask", got)
	}

	// 上界只给 5 秒：提交点不推进时这条 Propose 会一直等回调，
	// 没有超时的话用例是挂死而不是失败。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	index, _, err := n.Propose(ctx, "k", "v")
	if err != nil {
		t.Fatalf("propose on a one-member table: %v", err)
	}
	stats := n.Stats()
	if stats.CommitIndex != index || stats.LastApplied != index {
		t.Fatalf("cursors after the write = %+v, want both at index %d with no reply in flight", stats, index)
	}
	value, err := c.kv("node-1").Get("k")
	if err != nil || value != "v" {
		t.Errorf("read back = %q (%v), want v", value, err)
	}
}
