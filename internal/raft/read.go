package raft

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Consistency 是一次读请求的一致性档，对应 `/get` 的 consistency 两个取值（4.1 §4、D24）。
// 它与节点级的默认读法 Config.ReadMode（lease／readindex）是两个类型、两条轴：
// 这一档决定要不要做领导权确认，ReadMode 决定线性读走租约还是逐读确认。
type Consistency int8

const (
	ConsistencyLinearizable Consistency = iota
	ConsistencyLocal
)

// processStart 是单调时钟的原点。租约一律取相对它的纳秒数，不看墙上时间（D26）：
// 墙上时间的跳变会把租约算成比真实安全窗更长，而租约读要排除的恰恰是时钟漂移。
var processStart = time.Now()

// monotonicNow 是 Node.now 的真实实现，返回自进程启动以来的单调纳秒。
func monotonicNow() int64 { return int64(time.Since(processStart)) }

// leaseWindow 是租约窗口的纳秒数：租约起点之后还有这么长，租约才成立（4.1 §6）。
// 换算只在 NewNode 做一次，之后读路径只按这个窗口算截止时刻。
func leaseWindow(cfg Config) int64 {
	return (cfg.ElectionTimeoutMS - cfg.ClockDriftMS) * int64(time.Millisecond)
}

// readResult 是一次线性读拿到领导权确认后的结果：读索引与确认所用的任期。
// err 非空时两个游标回 0——失效与超时都从这条路把读送出去，不降级成本地读（D24）。
type readResult struct {
	readIndex uint64
	term      uint64
	err       error
}

// readWaiter 是读队列里的一格：到达序、登记时的 commitIndex（读索引的下界）、结果通道。
// wait 缓冲为 1，结算方在持有 Node 锁时送结果，读者已经离开锁也不会把结算堵死
// （与 registerProposal 同口径）。
type readWaiter struct {
	arrival   uint64
	lowerBond uint64
	wait      chan readResult
}

// readQueue 是排队的线性读，按到达序排（4.1 §7）。
type readQueue []readWaiter

// applyWaiters 是 index→等这个 index 被应用的读（4.1 §8 不变式 1）。
type applyWaiters map[uint64][]chan struct{}

// activeRound 是一轮在途的领导权确认，同一时刻至多一个（4.1 §7）。
// startNS 是本轮广播发起前取的单调时刻，也就是租约的起点；acks 含 Leader 自己那一票；
// members 是发起前已到达的读，之后到达的读留在队列里走下一轮（不变式 4）。
type activeRound struct {
	round   uint64
	term    uint64
	startNS int64
	acks    int
	members []readWaiter
}

// parseConsistency 解析 /get 的 consistency 参数：缺省线性一致，local 只能显式请求（D24）。
// 走租约还是逐读确认由节点级的 -read-mode 决定，与这个参数是两条轴（4.1 §6）。
func parseConsistency(raw string) (Consistency, error) {
	switch raw {
	case "", "linearizable":
		return ConsistencyLinearizable, nil
	case "local":
		return ConsistencyLocal, nil
	default:
		return ConsistencyLinearizable, fmt.Errorf("consistency %q must be linearizable or local", raw)
	}
}

// Read 是客户端读路径的唯一入口（4.1 §4）。
// local 档一步返回：读索引回 0、任期回本地任期；线性档先确认领导权（租约或确认轮），
// 再等状态机追上读索引，最后从 KV 取值——任何一步失败都不降级成本地读（D24）。
func (n *Node) Read(ctx context.Context, key string, mode Consistency) (string, uint64, uint64, error) {
	if mode == ConsistencyLocal {
		value, err := n.kv.Get(key)
		if err != nil {
			return "", 0, 0, err
		}
		n.mu.Lock()
		term := n.term
		n.mu.Unlock()
		return value, 0, term, nil
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return "", 0, 0, ErrCanceled
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return "", 0, 0, ErrNotLeader
	}
	lower := n.commitIndex
	if readIndex, term, ok := n.leaseServedLocked(lower); ok {
		n.mu.Unlock()
		value, err := n.readValue(ctx, key, readIndex)
		if err != nil {
			return "", 0, 0, err
		}
		return value, readIndex, term, nil
	}
	waiter := n.registerReadLocked()
	n.scheduleReadRound()
	n.mu.Unlock()

	n.drainOutbox()

	select {
	case res := <-waiter.wait:
		if res.err != nil {
			return "", 0, 0, res.err
		}
		value, err := n.readValue(ctx, key, res.readIndex)
		if err != nil {
			return "", 0, 0, err
		}
		return value, res.readIndex, res.term, nil
	case <-ctx.Done():
		n.cancelRead(waiter)
		return "", 0, 0, ErrCanceled
	}
}

// leaseServedLocked 判定这条读能不能走租约：租约绑定当前任期、还没过期，
// 而且剩余租约盖得住这次读的超时上界，否则不复用这条租约，改发一轮确认续期
// （4.1 §7 租约命中）。调用方必须持锁。
func (n *Node) leaseServedLocked(lower uint64) (uint64, uint64, bool) {
	if n.cfg.ReadMode != ReadModeLease || n.leaseTerm == 0 || n.leaseTerm != n.term {
		return 0, 0, false
	}
	now := n.now()
	if n.leaseUntilNS <= now {
		return 0, 0, false
	}
	if now+n.cfg.ReadTimeoutMS*int64(time.Millisecond) > n.leaseUntilNS {
		return 0, 0, false
	}
	readIndex := max(n.commitIndex, lower)
	n.lg.Info("read served by lease",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", n.term),
		zap.Uint64("read_index", readIndex),
		zap.Int64("lease_remaining_ms", (n.leaseUntilNS-now)/int64(time.Millisecond)))
	return readIndex, n.term, true
}

// readValue 等状态机追上读索引，再从 KV 取值（4.1 §8 不变式 1）。
// 等不到就是超时，不拿旧值顶上（D27）。
func (n *Node) readValue(ctx context.Context, key string, readIndex uint64) (string, error) {
	if err := n.waitApplied(ctx, readIndex); err != nil {
		return "", err
	}
	return n.kv.Get(key)
}

// registerReadLocked 把一条线性读排进读队列，返回它那一格。
// 下界取登记这一刻的 commitIndex，读索引不会比它旧（4.1 §8 不变式 1）。调用方必须持锁。
func (n *Node) registerReadLocked() readWaiter {
	n.readArrivals++
	waiter := readWaiter{arrival: n.readArrivals, lowerBond: n.commitIndex, wait: make(chan readResult, 1)}
	n.readQueue = append(n.readQueue, waiter)
	return waiter
}

// waitApplied 把读挂进 applyWaiters，等 lastApplied 追上 readIndex。
// finishApply 兑现这一格时读才返回；ctx 取消时把自己那一格摘掉（4.1 §7 等待应用）。
func (n *Node) waitApplied(ctx context.Context, readIndex uint64) error {
	n.mu.Lock()
	if n.lastApplied >= readIndex {
		n.mu.Unlock()
		return nil
	}
	wait := n.parkApplyWaiter(readIndex)
	n.mu.Unlock()

	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		n.mu.Lock()
		n.removeApplyWaiter(readIndex, wait)
		lastApplied, commitIndex := n.lastApplied, n.commitIndex
		n.mu.Unlock()
		n.lg.Warn("read wait timed out",
			zap.String("node_id", n.cfg.NodeID),
			zap.Uint64("read_index", readIndex),
			zap.Uint64("last_applied", lastApplied),
			zap.Uint64("commit_index", commitIndex))
		return ErrCanceled
	}
}

// parkApplyWaiter 把这条读挂进 applyWaiters，返回兑现通道。调用方必须持锁。
func (n *Node) parkApplyWaiter(readIndex uint64) chan struct{} {
	wait := make(chan struct{}, 1)
	n.applyWaiters[readIndex] = append(n.applyWaiters[readIndex], wait)
	return wait
}

// removeApplyWaiter 摘掉一条取消的等待：兑现方只叫醒还挂着的那一格（4.1 §8）。
func (n *Node) removeApplyWaiter(readIndex uint64, wait chan struct{}) {
	waiters := n.applyWaiters[readIndex]
	kept := waiters[:0]
	for _, ch := range waiters {
		if ch != wait {
			kept = append(kept, ch)
		}
	}
	if len(kept) == 0 {
		delete(n.applyWaiters, readIndex)
		return
	}
	n.applyWaiters[readIndex] = kept
}

// scheduleReadRound 在「读队列非空且没有在途轮次」时发起一轮确认（4.1 §7）。
// 本轮成员是发起前已到达的那些读，合并窗口就落在这一步：晚到的读留在队列里走下一轮。
// 调用方必须持锁。
func (n *Node) scheduleReadRound() {
	if len(n.readQueue) == 0 || n.activeRound != nil {
		return
	}
	n.readRoundSeq++
	members := make([]readWaiter, len(n.readQueue))
	copy(members, n.readQueue)
	n.readQueue = n.readQueue[:0]
	round := &activeRound{
		round:   n.readRoundSeq,
		term:    n.term,
		startNS: n.now(),
		acks:    1, // 自己那一票
		members: members,
	}
	n.activeRound = round
	n.lg.Info("read round sent",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", round.term),
		zap.Uint64("read_round", round.round),
		zap.Int("reads", len(members)),
		zap.Int64("start_mono_ms", round.startNS/int64(time.Millisecond)))
	for _, peerID := range n.cfg.otherPeers() {
		n.appendOnce(peerID)
	}
	// 成员表只有自己的时候没有回执可等：自票已经凑够多数派，当场结算（docs/8 §二 的单节点集群）。
	if round.acks >= n.cfg.Majority() {
		n.settleReadRound(round)
	}
}

// countReadRound 给在途确认轮计一票（4.1 §7 回执计票）。
// 编号对不上就是跨轮回执，丢弃并打日志；票到多数派立即结算。
// 调用方必须持锁。
func (n *Node) countReadRound(peerID string, reply AppendReply) {
	if reply.ReadRound == 0 {
		return
	}
	if n.activeRound == nil {
		// 迟到的回执：本轮已经结算，它不参与任何一轮的计票，也不算错位的回执。
		return
	}
	if reply.ReadRound != n.activeRound.round {
		n.lg.Info("stale read round dropped",
			zap.String("peer_id", peerID),
			zap.Uint64("read_round", reply.ReadRound),
			zap.Uint64("current_round", n.activeRound.round))
		return
	}
	round := n.activeRound
	if !reply.Success || round.term != n.term || n.role != RoleLeader {
		return
	}
	round.acks++
	if round.acks < n.cfg.Majority() {
		return
	}
	n.settleReadRound(round)
}

// settleReadRound 结算一轮确认：定读索引、决定租约成不成立、把结果发给本轮成员。
// 每个成员的读索引取结算时的 commitIndex 与它自己到达时的下界里的较大者
// （4.1 §7 结算、不变式 1）。调用方必须持锁。
func (n *Node) settleReadRound(round *activeRound) {
	n.activeRound = nil
	readIndex := n.commitIndex
	for _, member := range round.members {
		readIndex = max(readIndex, member.lowerBond)
	}

	// 租约的起点取广播发起那一刻，窗口按 -election-timeout-ms 减 -clock-drift-ms 算（4.1 §6）。
	if n.cfg.ReadMode == ReadModeLease {
		leaseUntil := round.startNS + n.leaseWindow
		remaining := (leaseUntil - n.now()) / int64(time.Millisecond)
		if remaining > 0 {
			n.leaseTerm = n.term
			n.leaseUntilNS = leaseUntil
			n.lg.Info("lease granted",
				zap.String("node_id", n.cfg.NodeID),
				zap.Uint64("term", n.term),
				zap.Int64("lease_remaining_ms", remaining))
		} else {
			n.lg.Info("lease rejected",
				zap.String("node_id", n.cfg.NodeID),
				zap.Uint64("term", n.term),
				zap.Uint64("read_round", round.round),
				zap.Int64("lease_remaining_ms", remaining))
		}
	}

	n.lg.Info("read round settled",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", round.term),
		zap.Uint64("read_round", round.round),
		zap.Int("acks", round.acks),
		zap.Uint64("read_index", readIndex),
		zap.Int64("round_cost_ms", (n.now()-round.startNS)/int64(time.Millisecond)))

	for _, member := range round.members {
		member.wait <- readResult{readIndex: max(n.commitIndex, member.lowerBond), term: round.term}
	}
	if len(n.readQueue) > 0 {
		n.scheduleReadRound()
	}
}

// cancelRead 把一条取消的读从队列与在途轮次里摘掉（4.1 §7）。
func (n *Node) cancelRead(waiter readWaiter) {
	n.mu.Lock()
	defer n.mu.Unlock()
	kept := n.readQueue[:0]
	for _, w := range n.readQueue {
		if w.arrival != waiter.arrival {
			kept = append(kept, w)
		}
	}
	n.readQueue = kept
	if round := n.activeRound; round != nil {
		members := round.members[:0]
		for _, w := range round.members {
			if w.arrival != waiter.arrival {
				members = append(members, w)
			}
		}
		round.members = members
	}
}

// invalidateLease 清空租约与在途轮次，并把没返回的读送走（4.1 §7 租约失效）。
// 失效之后没有「旧租约继续本地读」这条路径（D24）。调用方必须持锁。
func (n *Node) invalidateLease(reason string, err error) {
	if n.leaseTerm == 0 && n.activeRound == nil && len(n.readQueue) == 0 {
		return
	}
	n.lg.Info("lease invalidated",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("old_term", n.term),
		zap.String("reason", reason))
	n.leaseTerm = 0
	n.leaseUntilNS = 0
	for _, waiter := range n.readQueue {
		waiter.wait <- readResult{err: err}
	}
	n.readQueue = n.readQueue[:0]
	if round := n.activeRound; round != nil {
		for _, waiter := range round.members {
			waiter.wait <- readResult{err: err}
		}
		n.activeRound = nil
	}
}

// LeaseRemaining 给出当前租约距截止还剩多少毫秒，相对注入时钟；<=0 表示没有效租约（4.1 §4）。
// 未 Start 也能读，测试与注入脚本都从它取数（D26）。
func (n *Node) LeaseRemaining() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaseRemainingLocked()
}

// leaseRemainingLocked 只在租约绑定的任期与当前任期一致时给读数（不变式 3）：
// 任期一变，租约当场作废，剩余时间再多也不算数。
func (n *Node) leaseRemainingLocked() int64 {
	if n.leaseTerm == 0 || n.leaseTerm != n.term {
		return 0
	}
	return (n.leaseUntilNS - n.now()) / int64(time.Millisecond)
}

// ReadRound 是在途确认轮的编号，0 表示当前没有在途轮次（4.1 §4）。
func (n *Node) ReadRound() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.activeRound == nil {
		return 0
	}
	return n.activeRound.round
}
