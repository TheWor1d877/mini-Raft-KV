package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// installFixture 起一个 3 节点集群并写入两条，返回集群与一个 Follower 的 id。
func installFixture(t *testing.T) (*cluster, string) {
	t.Helper()
	c := newClusterCfg(t, 3, zap.NewNop(), nil)
	leaderID := c.waitForLeader(t, 60)
	for _, key := range []string{"k1", "k2"} {
		if _, _, err := c.node(leaderID).Propose(context.Background(), key, "v-"+key); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
	}
	c.tick(6)
	return c, leaderID
}

// followerOf 取集群里一个不是 Leader 的成员。
func followerOf(t *testing.T, c *cluster, leaderID string) string {
	t.Helper()
	for _, id := range c.ids {
		if id != leaderID {
			return id
		}
	}
	t.Fatal("no follower in the cluster")
	return ""
}

// snapshotBody 造一份快照文件的字节：一条键值。
func snapshotBody(t *testing.T, key, value string) []byte {
	t.Helper()
	body, err := encodeSnapshotFile([]kvstore.Entry{{Key: key, Value: value}})
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return body
}

// 收到合法快照后状态机被整体替换：快照里没有的键读不到。
func TestInstallSnapshotReplacesTheStateMachine(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	if _, err := f.kv.Get("k1"); err != nil {
		t.Fatalf("k1 is not in the follower before the install: %v", err)
	}

	body := snapshotBody(t, "snap", "sv")
	reply, err := f.handleInstallSnapshot(f.lastApplied, f.term, body)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if !reply.OK {
		t.Fatal("the reply is not ok")
	}

	if _, err := f.kv.Get("k1"); !errors.Is(err, kvstore.ErrKeyNotFound) {
		t.Fatalf("k1 is still readable after the install: %v", err)
	}
	value, err := f.kv.Get("snap")
	if err != nil {
		t.Fatalf("the snapshot key is not readable: %v", err)
	}
	if value != "sv" {
		t.Fatalf("snap = %q, want sv", value)
	}
}

// 请求 index 不大于本地快照点时直接回成功，状态机不动。
func TestInstallSnapshotWithAnOlderIndexIsIgnored(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	body := snapshotBody(t, "snap", "sv")
	if _, err := f.handleInstallSnapshot(f.lastApplied, f.term, body); err != nil {
		t.Fatalf("first install: %v", err)
	}

	// 本地快照点已经是 f.lastApplied：再来一份同样旧的快照不动状态机。
	reply, err := f.handleInstallSnapshot(f.lastApplied, f.term, snapshotBody(t, "other", "ov"))
	if err != nil {
		t.Fatalf("second install: %v", err)
	}
	if !reply.OK {
		t.Fatal("the reply is not ok")
	}
	if _, err := f.kv.Get("snap"); err != nil {
		t.Fatalf("the installed key is gone: %v", err)
	}
	if _, err := f.kv.Get("other"); !errors.Is(err, kvstore.ErrKeyNotFound) {
		t.Fatalf("the older snapshot was applied: %v", err)
	}
}

// 请求 index 小于本地 lastApplied 时同样不动状态机。
func TestInstallSnapshotBelowLastAppliedIsIgnored(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	f.mu.Lock()
	lastApplied := f.lastApplied
	f.mu.Unlock()
	if lastApplied < 2 {
		t.Fatalf("lastApplied = %d, want at least 2", lastApplied)
	}

	reply, err := f.handleInstallSnapshot(lastApplied-1, f.term, snapshotBody(t, "snap", "sv"))
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if !reply.OK {
		t.Fatal("the reply is not ok")
	}
	if _, err := f.kv.Get("k1"); err != nil {
		t.Fatalf("the state machine changed: %v", err)
	}
	if _, err := f.kv.Get("snap"); !errors.Is(err, kvstore.ErrKeyNotFound) {
		t.Fatalf("the snapshot was applied: %v", err)
	}
}

// magic、条目数与 crc 三项任一不符都判坏记录。
func TestInstallSnapshotRejectsACorruptFile(t *testing.T) {
	valid := snapshotBody(t, "snap", "sv")

	badMagic := append([]byte(nil), valid...)
	copy(badMagic, "MRSNAPXX")

	badCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badCount[snapshotMagicLen:snapshotHeaderLen], 2)

	badCRC := append([]byte(nil), valid...)
	badCRC[len(badCRC)-1] ^= 0xff

	cases := []struct {
		name string
		body []byte
	}{
		{"bad magic", badMagic},
		{"wrong entry count", badCount},
		{"bad frame crc", badCRC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, leaderID := installFixture(t)
			f := c.node(followerOf(t, c, leaderID))
			_, err := f.handleInstallSnapshot(f.lastApplied+1, f.term, tc.body)
			if !errors.Is(err, kvstore.ErrCorruptRecord) {
				t.Fatalf("err = %v, want ErrCorruptRecord", err)
			}
			if _, err := f.kv.Get("k1"); err != nil {
				t.Fatalf("the state machine changed after a corrupt install: %v", err)
			}
		})
	}
}

// 接收后游标推进：lastApplied 到快照 index，commitIndex 取本地值与快照 index 的较大者。
func TestInstallSnapshotAdvancesTheCursors(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	f.mu.Lock()
	commitBefore := f.commitIndex
	f.mu.Unlock()

	index := commitBefore + 5
	if _, err := f.handleInstallSnapshot(index, f.term, snapshotBody(t, "snap", "sv")); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	f.mu.Lock()
	lastApplied := f.lastApplied
	commitAfter := f.commitIndex
	f.mu.Unlock()

	if lastApplied != index {
		t.Fatalf("lastApplied = %d, want %d", lastApplied, index)
	}
	if commitAfter != index {
		t.Fatalf("commitIndex = %d, want %d", commitAfter, index)
	}
}

// 接收后 state 游标落盘：重启时读回来的 lastApplied 是快照 index。
func TestInstallSnapshotPersistsTheStateCursor(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	index := f.lastApplied + 1
	if _, err := f.handleInstallSnapshot(index, f.term, snapshotBody(t, "snap", "sv")); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	st, err := newStateFile(c.raftCfg[fid], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if st.LastApplied != index {
		t.Fatalf("the persisted lastApplied = %d, want %d", st.LastApplied, index)
	}
	if st.CommitIndex != index {
		t.Fatalf("the persisted commitIndex = %d, want %d", st.CommitIndex, index)
	}
}

// 接收后本地快照文件与指针被更新，日志里 <= 快照 index 的条目被删掉。
func TestInstallSnapshotWritesTheLocalSnapshotAndDropsTheLogPrefix(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	index := f.lastApplied
	if _, err := f.handleInstallSnapshot(index, f.term, snapshotBody(t, "snap", "sv")); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	path := c.raftCfg[fid].SnapshotPath(index)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the local snapshot file is missing: %v", err)
	}
	snap, entries, err := c.node(fid).snap.load()
	if err != nil {
		t.Fatalf("load the local snapshot: %v", err)
	}
	if snap == nil || snap.Index != index {
		t.Fatalf("the pointer does not point at %d: %+v", index, snap)
	}
	if len(entries) != 1 || entries[0].Key != "snap" {
		t.Fatalf("the local snapshot holds %+v, want the one installed key", entries)
	}

	f.mu.Lock()
	baseIndex := f.log.baseIndex
	length := f.log.length()
	f.mu.Unlock()
	if baseIndex != index+1 {
		t.Fatalf("baseIndex = %d, want %d", baseIndex, index+1)
	}
	if length != 0 {
		t.Fatalf("the log still holds %d entries, want none", length)
	}
}

// 快照点之后的键值仍来自日志：日志里留下的条目照旧进状态机。
func TestInstallSnapshotKeepsTheLogEntriesAfterTheSnapshotPoint(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	// 把 apply 拖在快照点之前：commitIndex 到 2，lastApplied 停在 1。
	f.mu.Lock()
	f.lastApplied = f.commitIndex - 1
	commitIndex := f.commitIndex
	f.mu.Unlock()
	if commitIndex < 2 {
		t.Fatalf("commitIndex = %d, want at least 2", commitIndex)
	}

	index := f.lastApplied
	if _, err := f.handleInstallSnapshot(index, f.term, snapshotBody(t, "snap", "sv")); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if err := f.applyCommitted(); err != nil {
		t.Fatalf("apply committed: %v", err)
	}
	value, err := f.kv.Get("k2")
	if err != nil {
		t.Fatalf("the entry after the snapshot point is not in the state machine: %v", err)
	}
	if value != "v-k2" {
		t.Fatalf("k2 = %q, want v-k2", value)
	}
	if _, err := f.kv.Get("snap"); err != nil {
		t.Fatalf("the installed key is gone: %v", err)
	}
}

// 重复收到同一份快照结果一致。
func TestInstallSnapshotIsIdempotent(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	body := snapshotBody(t, "snap", "sv")
	index := f.lastApplied
	first, err := f.handleInstallSnapshot(index, f.term, body)
	if err != nil {
		t.Fatalf("first install: %v", err)
	}
	second, err := f.handleInstallSnapshot(index, f.term, body)
	if err != nil {
		t.Fatalf("second install: %v", err)
	}
	if first != second {
		t.Fatalf("the two replies differ: %+v %+v", first, second)
	}

	f.mu.Lock()
	lastApplied := f.lastApplied
	commitIndex := f.commitIndex
	f.mu.Unlock()
	if lastApplied != index || commitIndex != index {
		t.Fatalf("the cursors moved: lastApplied = %d, commitIndex = %d, want %d", lastApplied, commitIndex, index)
	}
	value, err := f.kv.Get("snap")
	if err != nil || value != "sv" {
		t.Fatalf("snap = %q %v, want sv", value, err)
	}
}

// 补发之后 Leader 的下一条写入照常复制给这个成员并生效：
// 那一格已经从成员本地日志里删掉，一致性检查按「prevLogIndex 等于快照点、
// prevLogTerm 等于快照 term」对上（5.1 §5 的基线口径、§7 接收快照）。
func TestWritesAfterAnInstallReachTheCaughtUpMember(t *testing.T) {
	c, leaderID := installFixture(t)
	fid := followerOf(t, c, leaderID)
	f := c.node(fid)

	snapIndex := f.lastApplied
	if snapIndex < 2 {
		t.Fatalf("lastApplied = %d, want the two writes applied", snapIndex)
	}
	body, err := encodeSnapshotFile(c.kv(fid).Snapshot())
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	if _, err := f.handleInstallSnapshot(snapIndex, f.term, body); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	f.mu.Lock()
	baseIndex := f.log.baseIndex
	logLen := f.log.length()
	snapshotIndex := f.snapshotIndex
	snapshotTerm := f.snapshotTerm
	baseTerm := f.log.baseTerm
	f.mu.Unlock()
	if baseIndex != snapIndex+1 {
		t.Fatalf("baseIndex = %d, want %d after the prefix deletion", baseIndex, snapIndex+1)
	}
	if logLen != 0 {
		t.Fatalf("entries in view = %d, want the log emptied to the snapshot point", logLen)
	}
	if snapshotIndex != snapIndex || snapshotTerm != f.term {
		t.Fatalf("snapshot point = (%d,%d), want (%d,%d)", snapshotIndex, snapshotTerm, snapIndex, f.term)
	}
	if baseTerm != snapshotTerm {
		t.Fatalf("baseTerm = %d, want the snapshot term %d", baseTerm, snapshotTerm)
	}

	sentBefore := len(c.calls(leaderID))
	index, _, err := c.node(leaderID).Propose(context.Background(), "k3", "v-k3")
	if err != nil {
		t.Fatalf("propose after the install: %v", err)
	}
	if index != snapIndex+1 {
		t.Fatalf("the new entry landed at %d, want %d", index, snapIndex+1)
	}
	sentTo := false
	for _, s := range c.calls(leaderID)[sentBefore:] {
		if s.kind != "append" || s.to != fid {
			continue
		}
		sentTo = true
		if s.app.PrevLogIndex != snapIndex {
			t.Fatalf("append to %s used prevLogIndex %d, want the snapshot point %d", s.to, s.app.PrevLogIndex, snapIndex)
		}
		if s.app.PrevLogTerm != snapshotTerm {
			t.Fatalf("append to %s used prevLogTerm %d, want the snapshot term %d", s.to, s.app.PrevLogTerm, snapshotTerm)
		}
	}
	if !sentTo {
		t.Fatalf("the leader sent no AppendEntries to %s", fid)
	}

	c.tick(6)
	if got, err := c.kv(fid).Get("k3"); err != nil || got != "v-k3" {
		t.Fatalf("Get(k3) on %s = %q %v, want v-k3", fid, got, err)
	}
	f.mu.Lock()
	lastApplied := f.lastApplied
	commitIndex := f.commitIndex
	logLen = f.log.length()
	f.mu.Unlock()
	if commitIndex < index || lastApplied < index {
		t.Fatalf("cursors = commit %d / applied %d, want both past the new entry %d", commitIndex, lastApplied, index)
	}
	if logLen != 1 {
		t.Fatalf("entries in view = %d, want only the entry after the snapshot point", logLen)
	}
}
