package raft

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// leaderNode 造一个 5 节点集群并选出 Leader，返回集群与 Leader 的 id。
func leaderNode(t *testing.T, mut func(*Config)) (*cluster, string) {
	t.Helper()
	c := newClusterCfg(t, 5, zap.NewNop(), mut)
	id := c.waitForLeader(t, 60)
	return c, id
}

// Leader 的 safeIndex 取全部成员 matchIndex 的最小值，自己算一个成员。
func TestSafeIndexOnLeaderTakesTheSmallestMatchIndex(t *testing.T) {
	c, leaderID := leaderNode(t, nil)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.role != RoleLeader {
		t.Fatalf("role = %v, want leader", leader.role)
	}
	safe := leader.safeIndex()
	if safe == 0 {
		t.Fatalf("safeIndex = 0, want the replicated index")
	}
	for _, peerID := range leader.cfg.otherPeers() {
		if got := leader.matchIndex[peerID]; got < safe {
			t.Fatalf("safeIndex = %d but %s only matched %d", safe, peerID, got)
		}
	}
	// Leader 自己算一个成员：它复制到的高度是日志末尾。
	if got := leader.log.lastIndex(); got < safe {
		t.Fatalf("safeIndex = %d but the leader itself only reached %d", safe, got)
	}
}

// Follower 的 safeIndex 取本地 commitIndex：它没有 matchIndex 可用。
func TestSafeIndexOnFollowerTakesLocalCommitIndex(t *testing.T) {
	c, leaderID := leaderNode(t, nil)
	if _, _, err := c.node(leaderID).Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	for _, id := range c.ids {
		if id == leaderID {
			continue
		}
		follower := c.node(id)
		follower.mu.Lock()
		safe := follower.safeIndex()
		commit := follower.commitIndex
		follower.mu.Unlock()
		if safe != commit {
			t.Fatalf("safeIndex on %s = %d, want the local commitIndex %d", id, safe, commit)
		}
	}
}

// 落后超过豁免阈值的成员不参与 safeIndex 计算，日志里出现豁免告警并带成员 id 与落后条数（5.1 §8 不变式 5）。
func TestExemptedMemberLeavesTheSafeIndexCalculation(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := newClusterCfg(t, 3, zap.New(core), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 5
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	// 一个成员被隔离：它复制不到新条目，matchIndex 停在原地。
	lagging := c.ids[0]
	if lagging == leaderID {
		lagging = c.ids[1]
	}
	c.block(leaderID, lagging)

	for i := range 20 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.tick(4)

	leader.mu.Lock()
	lag := leader.lagBehind(leader.matchIndex[lagging])
	exempt := leader.exempted(lagging, leader.matchIndex[lagging])
	safe := leader.safeIndex()
	leader.mu.Unlock()

	if !exempt {
		t.Fatalf("%s lags %d entries, want it exempt over the threshold of 5", lagging, lag)
	}
	if lag <= 5 {
		t.Fatalf("lag = %d, want it over the threshold of 5", lag)
	}
	leader.mu.Lock()
	laggingMatch := leader.matchIndex[lagging]
	leader.mu.Unlock()
	if safe <= laggingMatch {
		t.Fatalf("safeIndex = %d, want it above the exempted member's %d", safe, laggingMatch)
	}
	// 豁免在生成快照的那一次报出来：删前缀之前先算边界。
	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	if countLogs(logs, "snapshot exemption applied") == 0 {
		t.Fatal("no exemption warning in the log")
	}
	found := false
	for _, entry := range logs.FilterMessage("snapshot exemption applied").All() {
		if fieldOf(entry, "peer_id") == lagging {
			found = true
		}
	}
	if !found {
		t.Fatalf("the exemption warning does not name %s", lagging)
	}
}

// 删除边界取 min(snapshotIndex, safeIndex)：safeIndex 小于快照点时一条不删（5.1 §8 不变式 5）。
func TestDeletionBoundaryIsHeldBackWhenSafeIndexLags(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 0
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	// 一个成员被隔离：safeIndex 停在它复制到的位置。
	lagging := c.ids[0]
	if lagging == leaderID {
		lagging = c.ids[1]
	}
	c.block(leaderID, lagging)
	for i := range 10 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.tick(4)

	leader.mu.Lock()
	baseBefore := leader.log.baseIndex
	leader.mu.Unlock()

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	leader.mu.Lock()
	baseAfter := leader.log.baseIndex
	safe := leader.safeIndex()
	leader.mu.Unlock()

	if safe >= leader.snapshotIndex {
		t.Fatalf("safeIndex = %d, want it behind the snapshot point %d", safe, leader.snapshotIndex)
	}
	if baseAfter != baseBefore {
		t.Fatalf("baseIndex moved from %d to %d, want nothing deleted", baseBefore, baseAfter)
	}
}

// dropLogPrefix 的三种落点：整段文件删掉、保留的那一段截到帧起点、保留段之前的段文件删除而保留段长度不变。
func TestDropLogPrefixLandsOnThreePlaces(t *testing.T) {
	t.Run("whole segment removed", func(t *testing.T) {
		cfg := testConfig(t)
		l := openTestRaftLog(t, cfg)
		mustAppend(t, l, 1, 1, "a")
		mustAppend(t, l, 1, 2, "b")
		mustAppend(t, l, 1, 3, "c")

		if err := l.dropPrefix(2); err != nil {
			t.Fatalf("drop prefix: %v", err)
		}
		if l.baseIndex != 2 {
			t.Fatalf("baseIndex = %d, want 2", l.baseIndex)
		}
		if l.length() != 2 {
			t.Fatalf("entries left = %d, want 2", l.length())
		}
		if _, ok := l.entryAt(1); ok {
			t.Fatal("entry at index 1 is still reachable after the drop")
		}
		if e, ok := l.entryAt(2); !ok || e.index != 2 {
			t.Fatalf("entryAt(2) = %+v %v, want the entry at index 2", e, ok)
		}
	})

	t.Run("kept segment cut to its frame start", func(t *testing.T) {
		cfg := testConfig(t)
		l := openTestRaftLog(t, cfg)
		mustAppend(t, l, 1, 1, "a")
		mustAppend(t, l, 1, 2, "b")
		mustAppend(t, l, 1, 3, "c")

		sizeBefore := fileSize(t, cfg.WALPath(1))
		if err := l.dropPrefix(3); err != nil {
			t.Fatalf("drop prefix: %v", err)
		}
		sizeAfter := fileSize(t, cfg.WALPath(1))
		if sizeAfter >= sizeBefore {
			t.Fatalf("segment size = %d, want it below %d", sizeAfter, sizeBefore)
		}
		// 保留的那一段仍然从帧起点起：按新基线扫回来只剩 index 3。
		entries, err := scanWALFrom(cfg, zap.NewNop(), 3)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(entries) != 1 || entries[0].index != 3 {
			t.Fatalf("entries after the drop = %+v, want only index 3", entries)
		}
	})

	t.Run("earlier segments removed and the kept one unchanged", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.MaxFileSize = 100
		l := openTestRaftLog(t, cfg)
		for i := uint64(1); i <= 6; i++ {
			mustAppend(t, l, 1, i, "payload")
		}
		segs := testSegments(t, cfg)
		if len(segs) < 2 {
			t.Fatalf("segments = %d, want the log to have rolled over", len(segs))
		}
		kept := segs[len(segs)-1]
		sizeBefore := fileSize(t, kept.path)
		// 保留段的第一条：它的偏移是 0，删前缀时这一段不改长度。
		var keepFrom uint64
		for _, e := range l.entries {
			if e.seq == kept.seq {
				keepFrom = e.index
				break
			}
		}
		if keepFrom == 0 {
			t.Fatalf("no entry in segment %d", kept.seq)
		}

		if err := l.dropPrefix(keepFrom); err != nil {
			t.Fatalf("drop prefix: %v", err)
		}
		if got := fileSize(t, kept.path); got != sizeBefore {
			t.Fatalf("kept segment size = %d, want it unchanged at %d", got, sizeBefore)
		}
		if got := len(testSegments(t, cfg)); got != 1 {
			t.Fatalf("segments left = %d, want 1", got)
		}
	})
}

// 删除之后视图的基线前移，entryAt 按新基线取下标，两个 term 映射一起重建。
func TestDropLogPrefixMovesTheBaselineAndRebuildsTheTermMaps(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 1, 1, "a")
	mustAppend(t, l, 1, 2, "b")
	mustAppend(t, l, 2, 3, "c")
	mustAppend(t, l, 2, 4, "d")

	if err := l.dropPrefix(3); err != nil {
		t.Fatalf("drop prefix: %v", err)
	}
	if l.baseIndex != 3 {
		t.Fatalf("baseIndex = %d, want 3", l.baseIndex)
	}
	if _, ok := l.entryAt(2); ok {
		t.Fatal("entryAt(2) answered below the baseline")
	}
	if e, ok := l.entryAt(4); !ok || e.term != 2 {
		t.Fatalf("entryAt(4) = %+v %v, want the term 2 entry", e, ok)
	}
	first, ok := l.firstIndexOfTerm(2)
	if !ok || first != 3 {
		t.Fatalf("firstIndexOfTerm(2) = %d %v, want 3", first, ok)
	}
	last, ok := l.lastIndexOfTerm(2)
	if !ok || last != 4 {
		t.Fatalf("lastIndexOfTerm(2) = %d %v, want 4", last, ok)
	}
	if _, ok := l.firstIndexOfTerm(1); ok {
		t.Fatal("the dropped term is still in the term map")
	}
}

// 快照点被 lastApplied 卡住时日志删不动，并打出 apply 落后告警（5.1 §8 不变式 1）。
func TestSnapshotBlockedByApplyLagKeepsTheLog(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	leader.mu.Lock()
	// 把 lastApplied 压到 commitIndex 之前，模拟 apply 落后。
	leader.lastApplied = leader.commitIndex - 1
	lagged := leader.commitIndex - leader.lastApplied
	baseBefore := leader.log.baseIndex
	leader.mu.Unlock()

	core, logs := observer.New(zapcore.InfoLevel)
	leader.lg = zap.New(core)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	if countLogs(logs, "snapshot blocked by apply lag") == 0 {
		t.Fatal("no apply lag warning in the log")
	}
	leader.mu.Lock()
	baseAfter := leader.log.baseIndex
	leader.mu.Unlock()
	if baseAfter != baseBefore {
		t.Fatalf("baseIndex moved from %d to %d, want the log kept", baseBefore, baseAfter)
	}
	if lagged == 0 {
		t.Fatal("the test did not create an apply lag")
	}
}

// 两次触发之间小于最小间隔时第二次不触发。
func TestSecondTriggerInsideTheMinIntervalDoesNotRun(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotMinIntervalMS = 60_000
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	first, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("first trigger: %v", err)
	}

	leader.mu.Lock()
	leader.lastSnapshotMS = monotonicMS()
	leader.mu.Unlock()

	if _, _, err := leader.Propose(context.Background(), "k2", "v2"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	core, logs := observer.New(zapcore.InfoLevel)
	leader.lg = zap.New(core)
	leader.maybeTriggerSnapshot()
	if countLogs(logs, "snapshot created") != 0 {
		t.Fatal("a second snapshot ran inside the minimum interval")
	}

	leader.mu.Lock()
	same := leader.snapshotIndex
	leader.mu.Unlock()
	if same != first {
		t.Fatalf("snapshot index moved from %d to %d, want the first snapshot kept", first, same)
	}
}

// 两个阈值都为 0 时自动触发全关，手动入口照常可用。
func TestBothThresholdsZeroTurnOffTheAutomaticTrigger(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotEntries = 0
		cfg.SnapshotBytes = 0
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	for i := range 5 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.tick(4)

	core, logs := observer.New(zapcore.InfoLevel)
	leader.lg = zap.New(core)
	leader.maybeTriggerSnapshot()
	if countLogs(logs, "snapshot created") != 0 {
		t.Fatal("an automatic snapshot ran with both thresholds at zero")
	}

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("manual trigger: %v", err)
	}
	if countLogs(logs, "snapshot created") != 1 {
		t.Fatal("the manual trigger did not create a snapshot")
	}
}

// 条数达到阈值时自动触发一次快照。
func TestEntryThresholdTriggersASnapshot(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotEntries = 3
		cfg.SnapshotMinIntervalMS = 0
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	core, logs := observer.New(zapcore.InfoLevel)
	leader.lg = zap.New(core)

	for i := range 6 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.tick(4)

	if countLogs(logs, "snapshot created") == 0 {
		t.Fatal("no automatic snapshot over the entry threshold")
	}
}

// 快照生成期间再触发一次，第二次直接返回不生成（5.1 §8 不变式 4）。
func TestConcurrentTriggerIsRejected(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	leader.mu.Lock()
	leader.snapshotting = true
	leader.mu.Unlock()

	if _, err := leader.TriggerSnapshot(); err == nil {
		t.Fatal("the second trigger returned no error while a snapshot was in flight")
	}
	core, logs := observer.New(zapcore.InfoLevel)
	leader.lg = zap.New(core)
	leader.maybeTriggerSnapshot()
	if countLogs(logs, "snapshot created") != 0 {
		t.Fatal("a snapshot ran while one was already in flight")
	}
}

// 手动触发在 Follower 上回 ErrNotLeader，快照点选不出来时回 ErrStateMismatch。
func TestTriggerSnapshotRejectsTheWrongRoleAndTheMissingPoint(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)

	for _, id := range c.ids {
		if id == leaderID {
			continue
		}
		if _, err := c.node(id).TriggerSnapshot(); err == nil {
			t.Fatalf("TriggerSnapshot on the follower %s returned no error", id)
		}
	}

	// 一个还没提交任何条目的 Leader 没有快照点。
	solo := newClusterCfg(t, 1, zap.NewNop(), nil)
	soloID := solo.waitForLeader(t, 60)
	solo.node(soloID).mu.Lock()
	solo.node(soloID).lastApplied = 0
	solo.node(soloID).mu.Unlock()
	if _, err := solo.node(soloID).TriggerSnapshot(); err == nil {
		t.Fatal("TriggerSnapshot without a snapshot point returned no error")
	}
}

// 快照写盘之前状态机导出的是快照点那一刻的内容：导出与选点在同一段持锁内完成（5.1 §8 不变式 8）。
func TestSnapshotExportsTheStateAtTheSnapshotPoint(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	leader.mu.Lock()
	index := leader.snapshotIndex
	leader.mu.Unlock()

	snapper := newSnapshotter(c.raftCfg[leaderID], zap.NewNop())
	snap, entries, err := snapper.load()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snap == nil || snap.Index != index {
		t.Fatalf("snapshot = %+v, want the one at index %d", snap, index)
	}
	found := false
	for _, e := range entries {
		if e.Key == "k" && e.Value == "v" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the snapshot does not carry the committed write: %+v", entries)
	}
}

// 快照文件里的键值与状态机当前的内容一致（5.1 §8 不变式 7）。
func TestSnapshotContentMatchesTheStateMachine(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	for i := range 3 {
		key := "k" + string(rune('a'+i))
		if _, _, err := leader.Propose(context.Background(), key, "v"); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	snapper := newSnapshotter(c.raftCfg[leaderID], zap.NewNop())
	_, entries, err := snapper.load()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	live := c.kv(leaderID).Snapshot()
	if len(entries) != len(live) {
		t.Fatalf("snapshot holds %d entries, the state machine has %d", len(entries), len(live))
	}
	for i, e := range entries {
		if e != live[i] {
			t.Fatalf("entry %d = %+v, want %+v", i, e, live[i])
		}
	}
}

// 快照点之后的条目仍在日志里，删前缀只删快照点之前的那些。
func TestLogPrefixDeletionKeepsTheEntriesAfterTheSnapshotPoint(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	for i := range 4 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	leader.mu.Lock()
	snapIndex := leader.snapshotIndex
	lastIndex := leader.log.lastIndex()
	base := leader.log.baseIndex
	leader.mu.Unlock()

	if snapIndex != lastIndex {
		t.Fatalf("snapshot index = %d, want the last index %d", snapIndex, lastIndex)
	}
	if base != snapIndex+1 {
		t.Fatalf("baseIndex = %d, want %d after the prefix deletion", base, snapIndex+1)
	}
	leader.mu.Lock()
	_, ok := leader.log.entryAt(snapIndex)
	leader.mu.Unlock()
	if ok {
		t.Fatalf("the entry at the snapshot point is still in the log view")
	}
}

// Leader 删完前缀之后的下一次写入照常复制到多数派并提交：
// 组包时 prevLogIndex 落在基线前一格，那一格已经不在本地日志里，term 从 baseTerm 取（5.1 §5）。
func TestWritesAfterThePrefixDropReplicateFromTheBaseline(t *testing.T) {
	c, leaderID := leaderNode(t, nil)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k1", "v1"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(4)

	snapIndex, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	leader.mu.Lock()
	baseIndex := leader.log.baseIndex
	baseTerm := leader.log.baseTerm
	leader.mu.Unlock()
	if baseIndex != snapIndex+1 {
		t.Fatalf("baseIndex = %d, want %d after the prefix deletion", baseIndex, snapIndex+1)
	}

	sentBefore := len(c.calls(leaderID))
	index, _, err := leader.Propose(context.Background(), "k2", "v2")
	if err != nil {
		t.Fatalf("propose after the prefix drop: %v", err)
	}
	if index != snapIndex+1 {
		t.Fatalf("the new entry landed at %d, want %d", index, snapIndex+1)
	}

	appends := 0
	for _, s := range c.calls(leaderID)[sentBefore:] {
		if s.kind != "append" {
			continue
		}
		if s.app.PrevLogIndex != snapIndex {
			t.Fatalf("append to %s used prevLogIndex %d, want the slot before the baseline (%d)", s.to, s.app.PrevLogIndex, snapIndex)
		}
		if s.app.PrevLogTerm != baseTerm {
			t.Fatalf("append to %s used prevLogTerm %d, want the baseline term %d", s.to, s.app.PrevLogTerm, baseTerm)
		}
		appends++
	}
	if appends == 0 {
		t.Fatal("the leader sent no AppendEntries for the write")
	}

	leader.mu.Lock()
	commit := leader.commitIndex
	leader.mu.Unlock()
	if commit < index {
		t.Fatalf("commitIndex = %d, want it past the new entry %d", commit, index)
	}
	c.tick(2)
	for _, id := range c.ids {
		if id == leaderID {
			continue
		}
		if got, err := c.kv(id).Get("k2"); err != nil || got != "v2" {
			t.Fatalf("Get(k2) on %s = %q %v, want v2", id, got, err)
		}
	}
}

// 单节点集群的快照之后重启：状态机由快照重建，读得到全部写。
func TestSingleNodeRestartRebuildsTheStateFromTheSnapshot(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	for i := range 4 {
		if _, _, err := leader.Propose(context.Background(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	restarted := c.restart(leaderID)
	for i := range 4 {
		key := "k" + string(rune('a'+i))
		got, err := restarted.Get(key)
		if err != nil || got != "v" {
			t.Fatalf("Get(%s) after the restart = %q %v, want v", key, got, err)
		}
	}
}

// 状态机导出面：顺序稳定，整体替换之后被替换掉的键读不到。
func TestStateMachineExportAndReplace(t *testing.T) {
	kv := openTestKV(t)
	kv.Apply("b", "2")
	kv.Apply("a", "1")

	first := kv.Snapshot()
	second := kv.Snapshot()
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("exported %d and %d entries, want 2 each", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("export %d = %+v, want %+v: the order must be stable", i, first[i], second[i])
		}
	}
	if first[0].Key != "a" || first[1].Key != "b" {
		t.Fatalf("export order = %+v, want the keys ascending", first)
	}

	kv.Restore([]kvstore.Entry{{Key: "c", Value: "3"}})
	if _, err := kv.Get("a"); err == nil {
		t.Fatal("the replaced key is still readable")
	}
	got, err := kv.Get("c")
	if err != nil || got != "3" {
		t.Fatalf("Get(c) = %q %v, want 3", got, err)
	}

	kv.Restore(nil)
	if got := kv.Snapshot(); len(got) != 0 {
		t.Fatalf("export after an empty restore = %+v, want none", got)
	}
}

// openTestKV 开一份数据目录里的状态机，测试结束自动收尾。
func openTestKV(t *testing.T) *kvstore.KV {
	t.Helper()
	cfg := kvstore.DefaultConfig()
	cfg.DataDir = t.TempDir()
	kv, err := kvstore.Open(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

// 触发条件的最小间隔按墙上时间的毫秒算：一次触发之后立刻再判定要等够间隔。
func TestMinIntervalUsesTheLastSnapshotTime(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotMinIntervalMS = 50
		cfg.SnapshotEntries = 1
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	// 快照点之后再写一条：条数阈值到了，剩下的门只有最小间隔。
	if _, _, err := leader.Propose(context.Background(), "k2", "v2"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	c.tick(2)

	leader.mu.Lock()
	leader.lastSnapshotMS = monotonicMS()
	leader.mu.Unlock()

	leader.mu.Lock()
	blocked := !leader.shouldTriggerLocked()
	leader.mu.Unlock()
	if !blocked {
		t.Fatal("the trigger was allowed right after a snapshot")
	}

	time.Sleep(80 * time.Millisecond)
	leader.mu.Lock()
	allowed := leader.shouldTriggerLocked()
	leader.mu.Unlock()
	if !allowed {
		t.Fatal("the trigger stayed blocked after the interval passed")
	}
}
