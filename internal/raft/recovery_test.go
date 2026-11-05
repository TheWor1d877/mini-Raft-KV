package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 日志是唯一恢复源：把 state 的游标退回一半，重启后 [1, lastApplied] 由回放重建、
// (lastApplied, commitIndex] 由补 apply 接手，两半合起来要逐 key 等于写进去的值。
// 覆盖重复写的 key：后一条必须盖在前一条之上；当选那条 no-op 空 command 要跳过（docs/8 §二、§六）。
func TestRecoveryRebuildsTheMapFromTheLog(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	leaderID := c.waitForLeader(t, 60)

	want := map[string]string{}
	write := func(key, value string) {
		t.Helper()
		if _, _, err := c.node(leaderID).Propose(context.Background(), key, value); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
		want[key] = value
	}
	for i := range 40 {
		write(fmt.Sprintf("eq-%03d", i), fmt.Sprintf("v-%d", i))
	}
	for i := range 10 {
		write(fmt.Sprintf("eq-%03d", i*4), fmt.Sprintf("overwritten-%d", i))
	}

	// Leader 之外的那个节点先跑起来，让写入落进它的日志，再收尾。
	// 全集群都要 Tick：最后一次写的 commitIndex 是回执之后才涨的，
	// 只有 Leader 的下一轮心跳才把它带给这个 Follower。
	follower := c.ids[0]
	if follower == leaderID {
		follower = c.ids[1]
	}
	c.tick(4)
	before := c.node(follower).Stats()
	if before.CommitIndex == 0 {
		t.Fatalf("follower %s never committed anything: %+v", follower, before)
	}
	if err := c.stopNode(follower); err != nil {
		t.Fatalf("stop %s: %v", follower, err)
	}

	// 游标退回一半，让回放接手 [1, lastApplied]、applyCommitted 接手差额。
	sf := newStateFile(c.raftCfg[follower], zap.NewNop())
	st, err := sf.load()
	if err != nil {
		t.Fatalf("load state of %s: %v", follower, err)
	}
	if st.CommitIndex < 2 {
		t.Fatalf("commitIndex %d is too small to split the replay in half", st.CommitIndex)
	}
	st.LastApplied = st.CommitIndex / 2
	if err := sf.store(st); err != nil {
		t.Fatalf("rewrite lastApplied of %s: %v", follower, err)
	}

	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	restarted := c.restart(follower)
	fromLog := recoverValues(t, c.kv(follower), keys)
	after := restarted.Stats()
	if after.LastApplied != after.CommitIndex {
		t.Errorf("lastApplied %d != commitIndex %d after recovery: the gap was not applied",
			after.LastApplied, after.CommitIndex)
	}

	for _, key := range keys {
		if fromLog[key] != want[key] {
			t.Errorf("%s: log recovery = %q, written %q", key, fromLog[key], want[key])
		}
	}
}

// 游标指向的条目解不出一 KV 操作，就在启动期失败，不静默跳过（docs/8 §六第二条）。
// 帧头与 crc 都是好的，坏的是 command 内部：日志扫描看不出问题，只有 apply 这一层拦得住。
func TestRecoveryRefusesToSkipAnUndecodableEntry(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	if err := c.stopNode("node-1"); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	cfg := c.raftCfg["node-1"]

	l, err := openWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("open the log: %v", err)
	}
	mustAppend(t, l, 1, 1, "\x01\x00")
	if err := l.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := newStateFile(cfg, zap.NewNop()).store(State{CurrentTerm: 1, CommitIndex: 1, LastApplied: 1}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	kv, err := kvstore.Open(c.kvCfg["node-1"], zap.NewNop())
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	_, err = NewNode(cfg, kv, &stubTransport{c: c, id: "node-1"}, zap.NewNop())
	if !errors.Is(err, kvstore.ErrCorruptRecord) {
		t.Fatalf("NewNode over an undecodable entry = %v, want ErrCorruptRecord", err)
	}
}

func recoverValues(t *testing.T, kv *kvstore.KV, keys []string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get(%s) after recovery: %v", key, err)
		}
		out[key] = value
	}
	return out
}

// lastReplayedEntries 读最近一条 state rebuilt from wal 里的 replayed_entries。
// 这个字段是 zap.Int，f.String 是空串，只能走 ContextMap（fieldOf 只认字符串字段）。
func lastReplayedEntries(t *testing.T, logs *observer.ObservedLogs, nodeID string) int64 {
	t.Helper()
	seen := logs.FilterMessage("state rebuilt from wal").FilterField(zap.String("node_id", nodeID)).All()
	if len(seen) == 0 {
		t.Fatalf("no %q entry for %s", "state rebuilt from wal", nodeID)
	}
	got := seen[len(seen)-1].ContextMap()["replayed_entries"]
	n, ok := got.(int64)
	if !ok {
		t.Fatalf("replayed_entries = %#v, want an integer", got)
	}
	return n
}

// corruptSnapshotFile 翻掉快照文件的最后一个字节：指针里的 checksum 与文件对不上（5.1 §5）。
func corruptSnapshotFile(t *testing.T, cfg Config, index uint64) {
	t.Helper()
	path := cfg.SnapshotPath(index)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the snapshot file: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("rewrite the snapshot file: %v", err)
	}
}

// 启动的四步顺序：快照先给状态机，日志只补快照点之后那一段。
// 快照点之前的键在本地日志里已经没有条目了，读得到就只可能是快照给的；
// replayed_entries 数的是快照点之后解出 key/value 的那几条（5.1 §7 启动恢复、不变式 3、7）。
func TestRecoveryReadsTheSnapshotThenReplaysTheLogAfterIt(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := newClusterCfg(t, 1, zap.New(core), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	want := map[string]string{}
	for i := range 4 {
		key := fmt.Sprintf("pre-%d", i)
		if _, _, err := leader.Propose(context.Background(), key, "from-snapshot"); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
		want[key] = "from-snapshot"
	}
	c.tick(2)

	snapIndex, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	for i := range 2 {
		key := fmt.Sprintf("post-%d", i)
		if _, _, err := leader.Propose(context.Background(), key, "from-log"); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
		want[key] = "from-log"
	}
	c.tick(2)

	leader.mu.Lock()
	base := leader.log.baseIndex
	leader.mu.Unlock()
	if base != snapIndex+1 {
		t.Fatalf("baseIndex = %d, want %d: the prefix through the snapshot point should be gone", base, snapIndex+1)
	}

	restarted := c.restart(leaderID)
	if got := lastReplayedEntries(t, logs, leaderID); got != 2 {
		t.Errorf("replayed_entries = %d, want the 2 entries after the snapshot point", got)
	}
	after := restarted.Stats()
	if after.LastApplied != after.CommitIndex {
		t.Errorf("lastApplied %d != commitIndex %d after the restart", after.LastApplied, after.CommitIndex)
	}

	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	merged := recoverValues(t, c.kv(leaderID), keys)
	for _, key := range keys {
		if merged[key] != want[key] {
			t.Errorf("%s after the restart = %q, want %q", key, merged[key], want[key])
		}
	}
}

// 有快照时 (lastApplied, commitIndex] 的补 apply 照旧：游标退回快照点之后一格，
// 那一条由回放接手，它之后的一条由补 apply 接手，两条来源合起来等于写入终态（5.1 §7 第 3、4 步、§8 不变式 6、7）。
func TestRecoveryAppliesTheCommitGapOnTopOfASnapshot(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := newClusterCfg(t, 1, zap.New(core), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	want := map[string]string{"pre": "from-snapshot"}
	if _, _, err := leader.Propose(context.Background(), "pre", "from-snapshot"); err != nil {
		t.Fatalf("propose pre: %v", err)
	}
	c.tick(2)

	if _, err := leader.TriggerSnapshot(); err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	for key, value := range map[string]string{"mid": "from-replay", "tail": "from-gap-apply"} {
		if _, _, err := leader.Propose(context.Background(), key, value); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
		want[key] = value
	}
	c.tick(2)

	if err := c.stopNode(leaderID); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	sf := newStateFile(c.raftCfg[leaderID], zap.NewNop())
	st, err := sf.load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if st.CommitIndex-st.LastApplied != 0 {
		t.Fatalf("lastApplied %d != commitIndex %d before the rollback", st.LastApplied, st.CommitIndex)
	}
	st.LastApplied--
	if err := sf.store(st); err != nil {
		t.Fatalf("roll lastApplied back: %v", err)
	}

	restarted := c.restart(leaderID)
	after := restarted.Stats()
	if after.LastApplied != after.CommitIndex {
		t.Fatalf("lastApplied %d != commitIndex %d: the gap was not applied", after.LastApplied, after.CommitIndex)
	}
	if got := lastReplayedEntries(t, logs, leaderID); got != 1 {
		t.Errorf("replayed_entries = %d, want the 1 entry up to the rolled-back lastApplied", got)
	}
	for _, key := range []string{"pre", "mid", "tail"} {
		if got := recoverValues(t, c.kv(leaderID), []string{key})[key]; got != want[key] {
			t.Errorf("%s = %q, want %q", key, got, want[key])
		}
	}
}

// 快照文件损坏而日志前缀还在：按没有快照起，状态机整段由日志重建（5.1 §7 第 1 步、不变式 2）。
// 前缀在这一格还留着，靠的是删除边界被落后成员挡在快照点之前（5.1 §7 删除边界）。
func TestCorruptSnapshotFallsBackToTheLogWhenThePrefixIsStillThere(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := newClusterCfg(t, 3, zap.New(core), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 0
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	lagging := c.ids[0]
	if lagging == leaderID {
		lagging = c.ids[1]
	}
	c.block(leaderID, lagging)

	want := map[string]string{}
	keys := make([]string, 0, 3)
	for i := range 3 {
		key := fmt.Sprintf("k%d", i)
		if _, _, err := leader.Propose(context.Background(), key, "v"+key); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
		want[key] = "v" + key
		keys = append(keys, key)
	}
	c.tick(4)

	snapIndex, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	leader.mu.Lock()
	base := leader.log.baseIndex
	leader.mu.Unlock()
	if base != 1 {
		t.Fatalf("baseIndex = %d, want 1: the lagging member holds the deletion boundary back", base)
	}

	corruptSnapshotFile(t, c.raftCfg[leaderID], snapIndex)
	c.restart(leaderID)

	restarted := c.node(leaderID)
	restarted.mu.Lock()
	snapshotIndex := restarted.snapshotIndex
	baseAfter := restarted.log.baseIndex
	restarted.mu.Unlock()
	if snapshotIndex != 0 || baseAfter != 1 {
		t.Fatalf("snapshotIndex %d and baseIndex %d, want 0 and 1: the node should start without a snapshot",
			snapshotIndex, baseAfter)
	}
	if countLogs(logs, "snapshot checksum mismatch, starting without a snapshot") == 0 {
		t.Error("the corrupted snapshot was not reported in the log")
	}
	for _, key := range keys {
		if got := recoverValues(t, c.kv(leaderID), []string{key})[key]; got != want[key] {
			t.Errorf("%s = %q, want %q: the log should rebuild it", key, got, want[key])
		}
	}
}

// 快照损坏而日志前缀已经删掉：那一段状态只剩一份，启动当场失败并说清数据不可恢复，
// 不静默起一个空 map（5.2 §5 的可恢复性那行）。
func TestCorruptSnapshotWithThePrefixDroppedRefusesToStart(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.Propose(context.Background(), "k1", "v1"); err != nil {
		t.Fatalf("propose k1: %v", err)
	}
	c.tick(2)

	snapIndex, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	// 快照点之后再写一条：日志里只剩这一条，它前面的都已经删掉。
	if _, _, err := leader.Propose(context.Background(), "k2", "v2"); err != nil {
		t.Fatalf("propose k2: %v", err)
	}
	c.tick(2)

	if err := c.stopNode(leaderID); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	corruptSnapshotFile(t, c.raftCfg[leaderID], snapIndex)

	kv, err := kvstore.Open(c.kvCfg[leaderID], zap.NewNop())
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	_, err = NewNode(c.raftCfg[leaderID], kv, &stubTransport{c: c, id: leaderID}, zap.NewNop())
	if !errors.Is(err, ErrStateMismatch) {
		t.Fatalf("NewNode over a corrupt snapshot and a dropped prefix = %v, want ErrStateMismatch", err)
	}
	if !strings.Contains(err.Error(), "no usable snapshot") {
		t.Errorf("the startup error does not name the missing snapshot: %v", err)
	}
}
