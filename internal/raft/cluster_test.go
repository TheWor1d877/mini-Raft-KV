package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// tick 给指定的一批节点各打 rounds 轮 Tick；不传 ids 就是全集群。
// 一轮就是每台机器各一次 Tick（D16）；选举与心跳只看 tick 计数，
// 租约走的是假时钟，假时钟每轮按 -tick-ms 前进一格（4.1 §7、D26）。
func (c *cluster) tick(rounds int, ids ...string) {
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	for range rounds {
		for _, id := range c.ids {
			if len(wanted) > 0 && !wanted[id] {
				continue
			}
			if n := c.nodes[id]; n != nil {
				n.Tick()
			}
		}
		c.clock.advance(time.Duration(c.raftCfg[c.ids[0]].TickMS) * time.Millisecond)
	}
}

// leaders 是此刻自认 Leader 的成员 id；已经收尾的节点不参与。
func (c *cluster) leaders(ids ...string) []string {
	var out []string
	for _, id := range c.ids {
		if len(ids) > 0 && !contains(ids, id) {
			continue
		}
		if n := c.nodes[id]; n != nil && n.Role() == RoleLeader {
			out = append(out, id)
		}
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, straw := range haystack {
		if straw == needle {
			return true
		}
	}
	return false
}

// dump 是给失败信息用的读数：一行一个节点的角色与 term。
func (c *cluster) dump() []string {
	var out []string
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			out = append(out, fmt.Sprintf("%s=down", id))
			continue
		}
		got := n.Stats()
		out = append(out, fmt.Sprintf("%s=%s@%d(commit %d)", id, got.Role, got.Term, got.CommitIndex))
	}
	return out
}

// waitForLeader 一轮一轮地打 Tick，直到集群里出现唯一一个 Leader。
// 预算按轮给：60 轮就是每台机器 3 秒的模拟时间（D16 的换算）。
func (c *cluster) waitForLeader(t *testing.T, rounds int) string {
	t.Helper()
	for range rounds {
		c.tick(1)
		got := c.leaders()
		switch len(got) {
		case 0:
		case 1:
			return got[0]
		default:
			t.Fatalf("more than one leader at once: %v, cluster = %v", got, c.dump())
		}
	}
	t.Fatalf("no leader within %d rounds, cluster = %v", rounds, c.dump())
	return ""
}

// splitAroundLeader 把 Leader 与成员表里它的下一个节点分到少数派那半，其余三个是多数派。
func splitAroundLeader(ids []string, leaderID string) (minority, majority []string) {
	for _, id := range ids {
		if id == leaderID {
			minority = append(minority, id)
			continue
		}
		if len(minority) == 1 {
			minority = append(minority, id)
			continue
		}
		majority = append(majority, id)
	}
	return minority, majority
}

// 无 Leader 起步：3 秒模拟时间内选出一个，而且只有一个（D16、D17）。
func TestClusterElectsExactlyOneLeader(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	if got := c.leaders(); len(got) != 0 {
		t.Fatalf("leaders before ticking = %v, want none", got)
	}

	leaderID := c.waitForLeader(t, 60)
	if got := c.node(leaderID).Stats(); got.Role != RoleLeader || got.Term < 1 {
		t.Fatalf("leader stats = %+v, want leader with a term above zero", got)
	}
}

// 写一条 key 之后 5 个节点的本地档读都读到同一个值：leaderCommit 跟着心跳扩散，
// 每个节点各自 apply（3.1 §7 第 6 步与「应用」段）。这一格查的是各节点各自的
// apply 结果，与领导权无关，所以走 local 档（4.1 §4、D24）。
func TestClusterReplicatesWriteToEveryNode(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	leaderID := c.waitForLeader(t, 60)

	if _, _, err := c.node(leaderID).Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose on %s: %v", leaderID, err)
	}
	c.tick(4)

	for _, id := range c.ids {
		if got, err := c.node(id).Get("k"); err != nil || got != "v" {
			t.Errorf("Get(k) on %s = %q %v, want v: every node applied the committed entry", id, got, err)
		}
	}
}

// 杀掉 Leader 之后剩余节点重新选主，新 Leader 的 term 高于旧的，已确认的写照常读得到。
func TestClusterReelectsAfterLeaderKilled(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	oldID := c.waitForLeader(t, 60)
	oldTerm := c.node(oldID).Stats().Term

	if _, _, err := c.node(oldID).Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose on %s: %v", oldID, err)
	}
	c.tick(4)

	c.closeNode(oldID)
	newID := c.waitForLeader(t, 60)
	if newID == oldID {
		t.Fatalf("the killed node elected itself")
	}
	if got := c.node(newID).Stats().Term; got <= oldTerm {
		t.Errorf("new leader term = %d, want above the old leader's %d", got, oldTerm)
	}
	for _, id := range c.ids {
		if id == oldID {
			continue
		}
		if got, err := c.node(id).Get("k"); err != nil || got != "v" {
			t.Errorf("Get(k) on %s after the failover = %q %v, want v", id, got, err)
		}
	}
}

// 2/3 分区：只有 3 的那半出得了新 Leader，它的 term 比旧 Leader 高一级。
// 少数派那半凑不齐多数派，旧 Leader 靠心跳维持着自封的位置，但一条也提交不了（3.2 §3 cluster 行）。
func TestClusterPartitionOnlyMajorityElects(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	oldID := c.waitForLeader(t, 60)
	oldTerm := c.node(oldID).Stats().Term

	minority, majority := splitAroundLeader(c.ids, oldID)
	c.partition(minority, majority)
	c.tick(60)

	newIDs := c.leaders(majority...)
	if len(newIDs) != 1 {
		t.Fatalf("majority leaders = %v, want exactly one: cluster = %v", newIDs, c.dump())
	}
	if got := c.node(newIDs[0]).Stats(); got.Term <= oldTerm {
		t.Errorf("new leader stats = %+v, want a term above the old leader's %d", got, oldTerm)
	}
	for _, id := range c.leaders(minority...) {
		if id != oldID {
			t.Errorf("the minority elected %s, want no new leader: two votes never reach 3", id)
		}
	}

	frozen := c.node(oldID).Stats().CommitIndex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.node(oldID).Propose(ctx, "k", "v"); !errors.Is(err, ErrCanceled) {
		t.Fatalf("propose inside the minority = %v, want ErrCanceled", err)
	}
	if got := c.node(oldID).Stats().CommitIndex; got != frozen {
		t.Errorf("commitIndex = %d, want it frozen at %d: two nodes are not a majority", got, frozen)
	}
	// term 膨胀的具体数字归 doc 6（D22：本轮无数值断言），这里只确认读数在 Stats() 里拿得到。
	if got := c.node(oldID).Stats().Term; got < oldTerm {
		t.Errorf("old leader term = %d, want it never to go backwards", got)
	}
}

// 合并之后旧 Leader 见到更高 term 自动降级，独走期间那条没提交的条目被新 Leader 的后缀覆盖（3.1 §7）。
func TestClusterHealOverwritesStaleLeaderSuffix(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	oldID := c.waitForLeader(t, 60)
	oldTerm := c.node(oldID).Stats().Term

	if _, _, err := c.node(oldID).Propose(context.Background(), "base", "b0"); err != nil {
		t.Fatalf("propose base: %v", err)
	}
	c.tick(4)

	minority, majority := splitAroundLeader(c.ids, oldID)
	c.partition(minority, majority)
	// 只推多数派那半：它选出一个更高 term 的新 Leader，少数派留在旧 term 上不动。
	c.tick(60, majority...)
	newIDs := c.leaders(majority...)
	if len(newIDs) != 1 {
		t.Fatalf("majority leaders after the heal = %v, want exactly one: cluster = %v", newIDs, c.dump())
	}
	newID := newIDs[0]

	// 旧 Leader 独走：本地收下一条写，少数派凑不齐多数派，ctx 取消之后 Propose 判负。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.node(oldID).Propose(ctx, "orphan", "x"); !errors.Is(err, ErrCanceled) {
		t.Fatalf("propose in the minority = %v, want ErrCanceled", err)
	}
	orphan := c.node(oldID).Stats()
	if orphan.CommitIndex >= orphan.LastLogIndex {
		t.Fatalf("stats = %+v, want the orphan entry present but uncommitted", orphan)
	}

	c.heal()
	// 合并后只推两轮：新 Leader 的心跳先到，旧 Leader 见到更高 term 就降级；
	// 推得更多会让少数派靠超时把 term 抬上去、反过来赢回选举，那是无 Pre-Vote 的正常行为，不是这条用例要断的。
	c.tick(2)
	if got := c.leaders(); len(got) != 1 || got[0] != newID {
		t.Fatalf("leaders after the heal = %v, want only %s: cluster = %v", got, newID, c.dump())
	}
	if got := c.node(oldID).Stats(); got.Role != RoleFollower || got.Term <= oldTerm {
		t.Fatalf("old leader stats = %+v, want follower at a term above %d", got, oldTerm)
	}

	// 新 Leader 在同一个 index 上写下自己的条目，旧 Leader 的这段后缀就此被覆盖。
	if _, _, err := c.node(newID).Propose(context.Background(), "wins", "y"); err != nil {
		t.Fatalf("propose wins: %v", err)
	}
	c.tick(4)

	old, leader := c.node(oldID), c.node(newID)
	if got, want := old.log.length(), leader.log.length(); got != want {
		t.Fatalf("old log length = %d, want %d equal to the leader's: cluster = %v",
			got, want, c.dump())
	}
	for index := uint64(1); index <= leader.log.lastIndex(); index++ {
		a, okA := old.log.entryAt(index)
		b, okB := leader.log.entryAt(index)
		if !okA || !okB || a.term != b.term || !bytes.Equal(a.cmd, b.cmd) {
			t.Errorf("index %d: old = %+v ok=%v, leader = %+v ok=%v, want the two logs identical",
				index, a, okA, b, okB)
		}
	}
	for _, id := range c.ids {
		if _, err := c.node(id).Get("orphan"); !errors.Is(err, kvstore.ErrKeyNotFound) {
			t.Errorf("Get(orphan) on %s = %v, want ErrKeyNotFound: the uncommitted write never applies",
				id, err)
		}
	}
}

// 两种读法在 Leader 上读到同一个值：租约档与 ReadIndex 档都过领导权这一关，
// 读索引都不小于写返回的 index（4.2 §3 的 cluster_test.go 行）。
func TestClusterReadsTheSameValueInBothReadModes(t *testing.T) {
	for _, mode := range []ReadMode{ReadModeLease, ReadModeReadIndex} {
		t.Run(string(mode), func(t *testing.T) {
			c := newClusterCfg(t, 5, zap.NewNop(), func(cfg *Config) {
				cfg.ReadMode = mode
			})
			leaderID := c.waitForLeader(t, 60)
			index, _, err := c.node(leaderID).Propose(context.Background(), "k", "v")
			if err != nil {
				t.Fatalf("propose on %s: %v", leaderID, err)
			}

			value, readIndex, _, err := c.node(leaderID).Read(context.Background(), "k", ConsistencyLinearizable)
			if err != nil {
				t.Fatalf("read on %s: %v", leaderID, err)
			}
			if value != "v" {
				t.Errorf("value = %q, want v", value)
			}
			if readIndex < index {
				t.Errorf("read index = %d, want at least the write's index %d", readIndex, index)
			}
		})
	}
}

// 杀掉 Leader 之后新 Leader 的第一个线性读要走一轮确认才返回，轮次编号从 1 起。
// 新节点没有租约，第一次读只能靠一轮确认拿到领导权（4.2 §3 的 cluster_test.go 行）。
func TestClusterFirstReadOnTheNewLeaderGoesThroughARound(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := newClusterCfg(t, 5, zap.New(core), func(cfg *Config) {
		cfg.ReadMode = ReadModeLease
	})
	oldID := c.waitForLeader(t, 60)
	if _, _, err := c.node(oldID).Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose on %s: %v", oldID, err)
	}
	c.tick(4)

	c.closeNode(oldID)
	newID := c.waitForLeader(t, 60)
	if newID == oldID {
		t.Fatal("the killed node elected itself")
	}

	n := c.node(newID)
	value, index, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable)
	if err != nil {
		t.Fatalf("read on %s: %v", newID, err)
	}
	if value != "v" {
		t.Errorf("value = %q, want v", value)
	}
	if index == 0 {
		t.Errorf("read index = 0, want the settled index")
	}
	sent := logs.FilterMessage("read round sent").All()
	if len(sent) != 1 {
		t.Fatalf("read round sent entries = %d, want 1", len(sent))
	}
	if got := sent[0].ContextMap()["read_round"]; got != uint64(1) {
		t.Errorf("read round = %v, want 1: the new leader has no lease to hit", got)
	}
}

// 分区成 2/3：少数派那半即使有节点自认 Leader，线性读全部失败——确认轮凑不齐 3 票，
// 读不返回任何值；本地档没有领导权确认，仍读得到旧值（不变式 6）。
func TestClusterLinearReadsFailInTheMinorityPartition(t *testing.T) {
	c := newClusterCfg(t, 5, zap.NewNop(), func(cfg *Config) {
		cfg.ReadMode = ReadModeLease
	})
	leaderID := c.waitForLeader(t, 60)
	if _, _, err := c.node(leaderID).Propose(context.Background(), "k", "v"); err != nil {
		t.Fatalf("propose on %s: %v", leaderID, err)
	}
	c.tick(4)

	minority, majority := splitAroundLeader(c.ids, leaderID)
	c.partition(minority, majority)

	n := c.node(leaderID)
	if got := n.Role(); got != RoleLeader {
		t.Fatalf("role of %s after the partition = %s, want it still believing it leads", leaderID, got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, _, err := n.Read(ctx, "k", ConsistencyLinearizable)
		done <- err
	}()
	// 等这一轮确认在途：少数派的请求只到得了半个集群，回执凑不齐 3 票。
	deadline := time.Now().Add(5 * time.Second)
	for n.ReadRound() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the read never started a confirmation round")
		}
		runtime.Gosched()
	}
	cancel()
	if err := <-done; !errors.Is(err, ErrCanceled) {
		t.Fatalf("linear read in the minority = %v, want ErrCanceled", err)
	}

	// 本地档读得到旧值：这一档没有领导权确认，也不参与线性一致的判定。
	value, _, _, err := n.Read(context.Background(), "k", ConsistencyLocal)
	if err != nil || value != "v" {
		t.Errorf("local read in the minority = %q %v, want v", value, err)
	}
}
