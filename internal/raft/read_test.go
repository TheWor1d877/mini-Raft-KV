package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// readFixture 是 5 节点集群，node-1 当 Leader，假时钟挂在集群上：
// 每发一条为在途确认轮发出的 AppendEntries，时钟前进 step 一格。
// step 选 205ms 时，两条请求正好把 450ms 的租约窗口烧到只剩 40ms（4.1 §7、4.2 §3）。
func readFixture(t *testing.T, step time.Duration, readTimeoutMS int64) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	c := newClusterCfg(t, 5, zap.New(core), func(cfg *Config) {
		cfg.ReadMode = ReadModeLease
		cfg.ReadTimeoutMS = readTimeoutMS
	})
	c.clockStep = step
	n := c.reseed("node-1", State{CurrentTerm: 2}, []seedEntry{{term: 2, index: 1}})
	c.injectVotes("node-1", grant(3), grant(3), deny(3), deny(3))
	runElection(t, n)
	if got := n.Role(); got != RoleLeader {
		t.Fatalf("fixture role = %s, want leader", got)
	}
	return c, n, logs
}

// putOne 先写一条，读才有值可读。
func putOne(t *testing.T, n *Node, key, value string) uint64 {
	t.Helper()
	index, _, err := n.Propose(context.Background(), key, value)
	if err != nil {
		t.Fatalf("propose %s: %v", key, err)
	}
	return index
}

// 未 Start 的节点也能读到全部游标（4.1 §4、4.4 T29.1）：
// 租约剩余与确认轮次两项读数与另外六个游标同属一次 Stats 快照，不因没 Start 就取不到。
func TestReadCursorsBeforeStart(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	n := c.node("node-1")

	if n.started {
		t.Fatal("node is started, want a node that never got Start")
	}
	if got := n.Stats(); got.Role != RoleFollower || got.Term != 0 {
		t.Fatalf("stats = %+v, want a fresh follower at term 0", got)
	}
	if got := n.LeaseRemaining(); got != 0 {
		t.Errorf("lease remaining = %d, want 0 with no lease", got)
	}
	if got := n.ReadRound(); got != 0 {
		t.Errorf("read round = %d, want 0 with no round in flight", got)
	}
	if got := n.Stats(); got.LeaseRemainingMS != 0 || got.ReadRound != 0 {
		t.Errorf("stats = %+v, want the two new cursors at zero", got)
	}
}

// 假时钟拨得动租约剩余读数（4.1 §7、4.4 T29.1）：
// 剩余量只经 Node.now 取时，改的是注入的时钟，不是墙上时间。
func TestReadLeaseRemainingFollowsInjectedClock(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	n := c.reseed("node-1", State{CurrentTerm: 2}, []seedEntry{{term: 2, index: 1}})

	clock := &fakeClock{}
	n.now = clock.now

	// 租约由后续步骤建立，这里按同一形状摆好：绑定当前任期、截止在起点之后 400ms。
	n.mu.Lock()
	n.leaseTerm = n.term
	n.leaseUntilNS = clock.now() + 400*int64(time.Millisecond)
	n.mu.Unlock()

	if got := n.LeaseRemaining(); got != 400 {
		t.Fatalf("lease remaining = %d, want 400ms", got)
	}
	if got := n.Stats().LeaseRemainingMS; got != 400 {
		t.Errorf("stats lease remaining = %d, want 400ms", got)
	}

	clock.advance(100 * time.Millisecond)
	if got := n.LeaseRemaining(); got != 300 {
		t.Errorf("lease remaining after 100ms = %d, want 300ms", got)
	}

	clock.advance(400 * time.Millisecond)
	if got := n.LeaseRemaining(); got != -100 {
		t.Errorf("lease remaining after 500ms = %d, want -100ms", got)
	}

	// 任期一变租约当场作废，剩余时间再多也不算数（不变式 3）。
	n.mu.Lock()
	n.term = 3
	n.mu.Unlock()
	if got := n.LeaseRemaining(); got != 0 {
		t.Errorf("lease remaining after a term change = %d, want 0", got)
	}
}

// 一次线性读要过的整条路：登记 → 确认轮 → 结算 → 等应用 → 取值。
// 返回值、读索引与任期三个游标都对得上，最后一条确认写就是读到的那一格。
func TestReadConfirmsLeadershipThenReturnsTheValue(t *testing.T) {
	c, n, logs := readFixture(t, 205*time.Millisecond, 10)
	putOne(t, n, "k", "v")
	before := len(c.calls("node-1"))

	value, index, term, err := n.Read(context.Background(), "k", ConsistencyLinearizable)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if value != "v" {
		t.Errorf("value = %q, want v", value)
	}
	if index != n.Stats().CommitIndex {
		t.Errorf("read index = %d, want the commit index %d", index, n.Stats().CommitIndex)
	}
	if term != n.Stats().Term {
		t.Errorf("read term = %d, want the current term %d", term, n.Stats().Term)
	}
	if got := n.ReadRound(); got != 0 {
		t.Errorf("read round after the read = %d, want 0: the round is settled", got)
	}
	if count := countLogs(logs, "read round sent"); count != 1 {
		t.Errorf("read round sent entries = %d, want 1", count)
	}
	if count := countLogs(logs, "read round settled"); count != 1 {
		t.Errorf("read round settled entries = %d, want 1", count)
	}
	// 一轮广播是 4 条 AppendEntries：按成员 id 升序各一条（4.2 §3 的 append 行）。
	sent := c.calls("node-1")
	if got := len(sent) - before; got != 4 {
		t.Errorf("append calls = %d, want 4 (one per peer)", got)
	}
	// 这 4 条都带着本轮编号：为读发起的广播与例行心跳在请求上只差这个字段（4.1 §7）。
	for _, call := range sent[before:] {
		if call.app.ReadRound != 1 {
			t.Errorf("read round on the request to %s = %d, want 1", call.to, call.app.ReadRound)
		}
	}
}

// 租约的起点取广播发起那一刻，结算时窗口还剩 40ms：租约成立，
// 剩下的毫秒数与读数对得上（4.1 §7、4.2 §3 的租约那格）。
func TestReadLeaseGrantedWhenTheWindowStillHasTime(t *testing.T) {
	_, n, logs := readFixture(t, 205*time.Millisecond, 10)
	putOne(t, n, "k", "v")

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("read: %v", err)
	}
	entry := logs.FilterMessage("lease granted").All()
	if len(entry) != 1 {
		t.Fatalf("lease granted entries = %d, want 1", len(entry))
	}
	if got := entry[0].ContextMap()["lease_remaining_ms"]; got != int64(40) {
		t.Errorf("lease remaining = %v, want 40ms", got)
	}
	if got := n.LeaseRemaining(); got != 40 {
		t.Errorf("lease remaining = %d, want 40ms", got)
	}
}

// 结算时窗口已经烧完：租约不成立，读数是负值（4.1 §7、4.2 §3 的 lease rejected 那格）。
func TestReadLeaseRejectedWhenTheWindowIsBurned(t *testing.T) {
	_, n, logs := readFixture(t, 250*time.Millisecond, 10)
	putOne(t, n, "k", "v")

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("read: %v", err)
	}
	entry := logs.FilterMessage("lease rejected").All()
	if len(entry) != 1 {
		t.Fatalf("lease rejected entries = %d, want 1", len(entry))
	}
	if got := entry[0].ContextMap()["lease_remaining_ms"]; got.(int64) >= 0 {
		t.Errorf("lease remaining = %v, want a negative value", got)
	}
	if got := n.LeaseRemaining(); got != 0 {
		t.Errorf("lease remaining = %d, want 0: the lease never took", got)
	}
}

// 命中租约的读一次 RPC 都不发（4.2 §3 的租约命中那格）。
func TestReadServedByLeaseSendsNoAppendEntries(t *testing.T) {
	c, n, logs := readFixture(t, 205*time.Millisecond, 10)
	putOne(t, n, "k", "v")

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("first read: %v", err)
	}
	before := len(c.calls("node-1"))

	value, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if value != "v" {
		t.Errorf("value = %q, want v", value)
	}
	if got := len(c.calls("node-1")) - before; got != 0 {
		t.Errorf("append calls for the lease hit = %d, want 0", got)
	}
	if count := countLogs(logs, "read served by lease"); count != 1 {
		t.Errorf("read served by lease entries = %d, want 1", count)
	}
}

// 剩余租约盖不住读超时的上界时，这条租约不复用：重新发一轮确认（4.1 §7 租约命中）。
func TestReadDoesNotReuseLeaseThatCannotCoverTheTimeout(t *testing.T) {
	c, n, _ := readFixture(t, 205*time.Millisecond, 50)
	putOne(t, n, "k", "v")

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("first read: %v", err)
	}
	before := len(c.calls("node-1"))

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if got := len(c.calls("node-1")) - before; got != 4 {
		t.Errorf("append calls for the second read = %d, want a fresh round of 4", got)
	}
}

// 合并窗口：一条读在轮次快照之后到达，本轮成员表不含它，它自己走下一轮
// （4.1 §7 发起确认轮、4.2 §3 的合并窗口那格）。
func TestReadRoundKeepsLateArrivalsForTheNextRound(t *testing.T) {
	_, n, logs := readFixture(t, 0, 10)

	// 第一条读登记后发起第一轮；第二条读在轮次在途时到达，进不了本轮成员表。
	n.mu.Lock()
	first := n.registerReadLocked()
	n.scheduleReadRound()
	n.mu.Unlock()
	if got := n.ReadRound(); got != 1 {
		t.Fatalf("read round = %d, want 1", got)
	}

	n.mu.Lock()
	second := n.registerReadLocked()
	n.scheduleReadRound() // 同一时刻至多一个在途轮次，这一调用什么也不做
	n.mu.Unlock()
	if got := n.ReadRound(); got != 1 {
		t.Fatalf("read round after the late arrival = %d, want 1", got)
	}

	n.drainOutbox()

	res := <-first.wait
	if res.err != nil {
		t.Fatalf("first read: %v", res.err)
	}
	if res.readIndex == 0 {
		t.Error("first read index = 0, want the settled index")
	}
	res = <-second.wait
	if res.err != nil {
		t.Fatalf("second read: %v", res.err)
	}

	sent := logs.FilterMessage("read round sent").All()
	if len(sent) != 2 {
		t.Fatalf("read round sent entries = %d, want 2", len(sent))
	}
	if got := sent[0].ContextMap()["reads"]; got != int64(1) {
		t.Errorf("first round reads = %v, want 1: the late arrival is not a member", got)
	}
	if got := sent[1].ContextMap()["reads"]; got != int64(1) {
		t.Errorf("second round reads = %v, want 1: the late arrival leads the next round", got)
	}
	if got := sent[1].ContextMap()["read_round"]; got != uint64(2) {
		t.Errorf("second round number = %v, want 2", got)
	}
}

// 跨轮回执不进计票：编号错位的回执丢掉，本轮的票数不受它影响（4.1 §7 回执计票、4.2 §3）。
func TestReadDropsRepliesFromAnotherRound(t *testing.T) {
	c, n, logs := readFixture(t, 0, 10)
	putOne(t, n, "k", "v")

	// node-2 的回执带一个错位的编号：它不并入计票。
	// 后两条跟着请求里的轮次编号走，用例不必先知道编号是多少。
	c.injectAppends("node-1", AppendReply{Term: 3, Success: true, ReadRound: 99})
	c.injectAppendRounds("node-1", AppendReply{Term: 3, Success: true}, AppendReply{Term: 3, Success: true})

	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("read: %v", err)
	}
	if count := countLogs(logs, "stale read round dropped"); count != 1 {
		t.Fatalf("stale read round dropped entries = %d, want 1", count)
	}
	settled := logs.FilterMessage("read round settled").All()
	if len(settled) != 1 {
		t.Fatalf("read round settled entries = %d, want 1", len(settled))
	}
	if got := settled[0].ContextMap()["acks"]; got != int64(3) {
		t.Errorf("acks = %v, want 3: the stale reply did not count", got)
	}
}

// 等待门限：读索引取结算时的 commitIndex 与到达时下界的较大者，
// lastApplied 追上来之前读不返回（4.1 §8 不变式 1、4.2 §3 的等待门限那格）。
func TestReadWaitsForApplyBeforeItReturns(t *testing.T) {
	_, n, _ := readFixture(t, 0, 10)
	putOne(t, n, "k", "v")

	before := n.Stats()
	n.mu.Lock()
	waiter := n.registerReadLocked()
	n.scheduleReadRound()
	n.mu.Unlock()
	n.drainOutbox()

	res := <-waiter.wait
	if res.err != nil {
		t.Fatalf("read: %v", res.err)
	}
	if res.readIndex != before.CommitIndex {
		t.Errorf("read index = %d, want the commit index %d", res.readIndex, before.CommitIndex)
	}

	// 把游标拉到前面，读这一格就排进 applyWaiters，finishApply 兑现之后才放行。
	n.mu.Lock()
	n.lastApplied = res.readIndex - 1
	wait := n.parkApplyWaiter(res.readIndex)
	if got := len(n.applyWaiters[res.readIndex]); got != 1 {
		t.Fatalf("apply waiters = %d, want 1", got)
	}
	n.mu.Unlock()

	select {
	case <-wait:
		t.Fatal("the parked read was released before the apply")
	default:
	}

	n.mu.Lock()
	n.lastApplied = res.readIndex
	n.wakeApplyWaiters(res.readIndex)
	n.mu.Unlock()

	select {
	case <-wait:
	default:
		t.Fatal("the parked read was not released by the apply")
	}
}

// 租约失效的四个入口：更高任期、转成 Follower、Close、重启。
// 失效之后旧租约上的读拿 ErrNotLeader 或 ErrCanceled，不降级成本地读（D24）。
func TestReadLeaseInvalidationEntries(t *testing.T) {
	t.Run("higher term", func(t *testing.T) {
		_, n, _ := readFixture(t, 205*time.Millisecond, 10)
		putOne(t, n, "k", "v")
		if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
			t.Fatalf("read: %v", err)
		}
		if got := n.LeaseRemaining(); got != 40 {
			t.Fatalf("lease remaining = %d, want 40ms before the invalidation", got)
		}

		n.mu.Lock()
		waiter := n.registerReadLocked()
		n.mu.Unlock()

		// 一个更高任期的 AppendEntries 把本节点压回 Follower，租约与在途轮次当场作废。
		n.handleAppendEntries(AppendRequest{Term: 9, LeaderID: "node-9", Entries: []LogEntry{}})

		res := <-waiter.wait
		if !errors.Is(res.err, ErrNotLeader) {
			t.Fatalf("read after the term change = %v, want ErrNotLeader", res.err)
		}
		if got := n.LeaseRemaining(); got != 0 {
			t.Errorf("lease remaining = %d, want 0 after the invalidation", got)
		}
		// 失效之后不降级成本地读：key 在 KV 里，读拿到的却是错误而不是值。
		value, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable)
		if !errors.Is(err, ErrNotLeader) {
			t.Errorf("read after the invalidation = %v, want ErrNotLeader", err)
		}
		if value != "" {
			t.Errorf("value after the invalidation = %q, want empty: kv.Get is never reached", value)
		}
	})

	t.Run("stepped down", func(t *testing.T) {
		_, n, _ := readFixture(t, 205*time.Millisecond, 10)
		putOne(t, n, "k", "v")
		if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
			t.Fatalf("read: %v", err)
		}
		if got := n.LeaseRemaining(); got != 40 {
			t.Fatalf("lease remaining = %d, want 40ms before the step-down", got)
		}

		n.mu.Lock()
		waiter := n.registerReadLocked()
		n.mu.Unlock()

		// 同任期转成 Follower：租约与在途轮次当场作废。
		if err := n.becomeFollower(n.term, "fixture steps down"); err != nil {
			t.Fatalf("becomeFollower: %v", err)
		}
		res := <-waiter.wait
		if !errors.Is(res.err, ErrNotLeader) {
			t.Fatalf("read after the step-down = %v, want ErrNotLeader", res.err)
		}
		if got := n.LeaseRemaining(); got != 0 {
			t.Errorf("lease remaining = %d, want 0 after the step-down", got)
		}
	})

	t.Run("close", func(t *testing.T) {
		_, n, _ := readFixture(t, 205*time.Millisecond, 10)
		putOne(t, n, "k", "v")
		if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
			t.Fatalf("read: %v", err)
		}

		n.mu.Lock()
		waiter := n.registerReadLocked()
		n.mu.Unlock()

		if err := n.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		res := <-waiter.wait
		if !errors.Is(res.err, ErrCanceled) {
			t.Fatalf("read after Close = %v, want ErrCanceled", res.err)
		}
		if got := n.LeaseRemaining(); got != 0 {
			t.Errorf("lease remaining = %d, want 0 after Close", got)
		}
		value, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable)
		if !errors.Is(err, ErrCanceled) {
			t.Errorf("read after Close = %v, want ErrCanceled", err)
		}
		if value != "" {
			t.Errorf("value after Close = %q, want empty: kv.Get is never reached", value)
		}
	})

	t.Run("restart", func(t *testing.T) {
		c, n, _ := readFixture(t, 205*time.Millisecond, 10)
		putOne(t, n, "k", "v")
		if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
			t.Fatalf("read: %v", err)
		}
		if got := n.LeaseRemaining(); got != 40 {
			t.Fatalf("lease remaining = %d, want 40ms before the restart", got)
		}
		restarted := c.restart("node-1")
		if got := restarted.LeaseRemaining(); got > 0 {
			t.Errorf("lease remaining after the restart = %d, want no lease", got)
		}
		if got := restarted.ReadRound(); got != 0 {
			t.Errorf("read round after the restart = %d, want 0", got)
		}
	})
}

// 非 Leader 的线性读回 ErrNotLeader；本地旧数据读是另一档，不参与这条判定（D24、4.1 §1）。
func TestReadOnFollowerIsNotLeader(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	only := c.node("node-1")

	if _, _, _, err := only.Read(context.Background(), "k", ConsistencyLinearizable); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("read on a follower = %v, want ErrNotLeader", err)
	}
	if _, _, _, err := only.Read(context.Background(), "k", ConsistencyLocal); !errors.Is(err, kvstore.ErrKeyNotFound) {
		t.Fatalf("local read on a follower = %v, want ErrKeyNotFound", err)
	}
}

// 等待门限的 30 条：commitIndex 已推进、lastApplied 落后 30 条时，这条读排进 applyWaiters，
// finishApply 兑现之后才拿到值；读索引取结算时的 commitIndex 与到达时的下界里的较大者（4.1 §8 不变式 1、D27）。
func TestReadParksUntilTheLaggingApplyCatchesUp(t *testing.T) {
	_, n, _ := readFixture(t, 0, 10)
	putOne(t, n, "k", "v")

	n.mu.Lock()
	for index := uint64(4); index <= 33; index++ {
		mustAppend(t, n.log, n.term, index, "")
	}
	n.commitIndex = 33
	n.lastApplied = 3
	waiter := n.registerReadLocked()
	n.readQueue = n.readQueue[:0]
	// 结算这一轮：读索引取结算时的 commitIndex 与到达时的下界里的较大者。
	n.activeRound = &activeRound{round: 1, term: n.term, startNS: n.now(), acks: 1, members: []readWaiter{waiter}}
	readIndex := max(n.commitIndex, waiter.lowerBond)
	n.activeRound = nil
	waiter.wait <- readResult{readIndex: readIndex, term: n.term}
	n.mu.Unlock()

	if readIndex != 33 {
		t.Fatalf("read index = %d, want 33", readIndex)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		value string
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := n.readValue(ctx, "k", readIndex)
		done <- outcome{value: value, err: err}
	}()
	waitApplyWaiter(t, n)

	select {
	case got := <-done:
		t.Fatalf("the read returned before the apply caught up: %+v", got)
	default:
	}

	n.mu.Lock()
	if err := n.finishApply(33); err != nil {
		n.mu.Unlock()
		t.Fatalf("finishApply: %v", err)
	}
	n.mu.Unlock()

	got := <-done
	if got.err != nil {
		t.Fatalf("read: %v", got.err)
	}
	if got.value != "v" {
		t.Errorf("value = %q, want v", got.value)
	}
	if readIndex != 33 {
		t.Errorf("read index = %d, want 33", readIndex)
	}
}

// 结算给出的读索引不会比读到达时看到的 commitIndex 更旧（4.1 §8 不变式 1）。
// 生产路径上 commitIndex 单调不减，两处取大取不到差别；用例把提交游标拨回去，
// 把结算里那条防御性的取大逼出来：结算那一刻的 commitIndex 比到达时的下界还旧。
func TestReadIndexNeverOlderThanTheArrivalBound(t *testing.T) {
	_, n, logs := readFixture(t, 0, 10)
	putOne(t, n, "k", "v")

	n.mu.Lock()
	waiter := n.registerReadLocked()
	bound := waiter.lowerBond
	n.readQueue = n.readQueue[:0]
	n.activeRound = &activeRound{round: 1, term: n.term, startNS: n.now(), acks: 1, members: []readWaiter{waiter}}
	n.commitIndex = bound - 1
	n.settleReadRound(n.activeRound)
	n.mu.Unlock()

	res := <-waiter.wait
	if res.err != nil {
		t.Fatalf("read: %v", res.err)
	}
	if res.readIndex != bound {
		t.Errorf("read index = %d, want the arrival bound %d", res.readIndex, bound)
	}
	// 结算那一轮落在日志里的读索引也是同一个数：它是结算时 commitIndex 与成员下界的较大者。
	settled := logs.FilterMessage("read round settled").All()
	if len(settled) != 1 {
		t.Fatalf("read round settled entries = %d, want 1", len(settled))
	}
	if got := settled[0].ContextMap()["read_index"]; got != bound {
		t.Errorf("settled read index = %v, want the arrival bound %d", got, bound)
	}
}

// 假时钟按 Tick 的节奏前进：调 N 次 Tick 之后租约少 N × -tick-ms，
// 租约的长度只按注入的单调时钟算（4.1 §7、D26）。
func TestReadLeaseExpiresAsTheInjectedClockAdvances(t *testing.T) {
	c, n, _ := readFixture(t, 205*time.Millisecond, 10)
	putOne(t, n, "k", "v")
	if _, _, _, err := n.Read(context.Background(), "k", ConsistencyLinearizable); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := n.LeaseRemaining(); got != 40 {
		t.Fatalf("lease remaining = %d, want 40ms", got)
	}

	c.tick(1)
	if got := n.LeaseRemaining(); got != -10 {
		t.Errorf("lease remaining after one Tick = %d, want -10: one tick is 50ms", got)
	}
	c.tick(11)
	if got := n.LeaseRemaining(); got != -560 {
		t.Errorf("lease remaining after 12 ticks = %d, want -560", got)
	}
}
