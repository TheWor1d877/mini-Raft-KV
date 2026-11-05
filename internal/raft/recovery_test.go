package raft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"go.uber.org/zap"

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
