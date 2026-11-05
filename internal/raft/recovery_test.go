package raft

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 两条恢复路径要给出同一个 map：KV WAL 回放（合并前的主恢复源）与 raft 日志回放
// （合并后唯一的恢复源）。逐 key 对拍，另外再和「写进去的那个值」对一次地，
// 免得两条一起错还互相印证。docs/8 第 1 步的等价性证据，是后面删 KV WAL 的前提。
func TestRecoverySourcesAgreeKeyByKey(t *testing.T) {
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
	// 覆盖 10 个 key：两个恢复源都得让后写的盖在前面的之上。
	for i := range 10 {
		write(fmt.Sprintf("eq-%03d", i*4), fmt.Sprintf("overwritten-%d", i))
	}

	// Leader 之外的那个节点先跑起来，让写入落进它的日志与 KV WAL，再收尾。
	// 当选时写的那条 no-op 也在日志里，回放要跳过它（docs/8 §六）。
	follower := c.ids[0]
	if follower == leaderID {
		follower = c.ids[1]
	}
	// 全集群都要 Tick：最后一次写的 commitIndex 是回执之后才涨的，
	// 只有 Leader 的下一轮心跳才把它带给这个 Follower。
	c.tick(4)
	before := c.node(follower).Stats()
	if before.CommitIndex == 0 {
		t.Fatalf("follower %s never committed anything: %+v", follower, before)
	}
	if err := c.stopNode(follower); err != nil {
		t.Fatalf("stop %s: %v", follower, err)
	}

	// 游标退回一半，让日志回放接手 [1, lastApplied]、applyCommitted 接手差额，
	// 两半都盖到才对得起整条恢复链。
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

	// 路径 A：KV WAL 回放。
	kcfgWAL := c.kvCfg[follower]
	kcfgWAL.SkipWALRecovery = false
	kvWAL, err := kvstore.Open(kcfgWAL, zap.NewNop())
	if err != nil {
		t.Fatalf("open kv with WAL recovery: %v", err)
	}
	fromWAL := recoverValues(t, kvWAL, keys)
	if err := kvWAL.Close(); err != nil {
		t.Fatalf("close kv after WAL recovery: %v", err)
	}

	// 路径 B：跳过 KV WAL，由 NewNode 回放 raft 日志 + 补 apply 差额。
	kvLog, err := kvstore.Open(c.kvCfg[follower], zap.NewNop())
	if err != nil {
		t.Fatalf("open kv without WAL recovery: %v", err)
	}
	nodeLog, err := NewNode(c.raftCfg[follower], kvLog, &stubTransport{c: c, id: follower}, zap.NewNop())
	if err != nil {
		t.Fatalf("new node from the raft log: %v", err)
	}
	fromLog := recoverValues(t, kvLog, keys)
	after := nodeLog.Stats()
	if after.LastApplied != after.CommitIndex {
		t.Errorf("lastApplied %d != commitIndex %d after recovery: the gap was not applied",
			after.LastApplied, after.CommitIndex)
	}
	if err := nodeLog.Close(); err != nil {
		t.Fatalf("close the log-recovered node: %v", err)
	}
	if err := kvLog.Close(); err != nil {
		t.Fatalf("close kv after log recovery: %v", err)
	}

	for _, key := range keys {
		if fromWAL[key] != fromLog[key] {
			t.Errorf("%s: WAL recovery = %q, log recovery = %q, want the two sources to agree",
				key, fromWAL[key], fromLog[key])
			continue
		}
		if fromWAL[key] != want[key] {
			t.Errorf("%s: both sources recovered %q, written %q", key, fromWAL[key], want[key])
		}
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
