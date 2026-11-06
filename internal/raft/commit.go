package raft

import (
	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// apply 每攒够这么多条写一次 state 游标：状态机改动只落内存，
// 这个边界只决定游标落盘的频率（docs/8 §二）。
const maxApplyEntries = 64

// advanceCommit 按不变式 3 推进 commitIndex：只因为「当前任期的条目被多数派复制」而前进，
// 旧任期的条目要等它后面的当前任期条目落定才被间接带走。
// 从日志末尾往前扫，第一个同时满足「属于当前任期」和「多数派已到该位置」的 index 就是新边界。
func (n *Node) advanceCommit() {
	pending := n.pendingConfig
	for index := n.log.lastIndex(); index > n.commitIndex; index-- {
		e, ok := n.log.entryAt(index)
		if !ok {
			continue
		}
		// 提交屏障：未提交的配置条目后面的普通条目不能先把它带过提交点（6.1 §8 不变式 5）。
		if pending != nil && index > pending.index {
			continue
		}
		if pending != nil && index == pending.index {
			// 配置条目要旧集合与新集合各有多数派（D38）。这一条不看任期：
			// 换届之后新 Leader 要能把上一任留下的配置条目结掉，否则屏障会卡住所有后续条目。
			if !n.majorityReplicated(pending.old, index) || !n.majorityReplicated(pending.new, index) {
				continue
			}
		} else {
			if e.term != n.term || !n.majorityReplicated(n.membership.voters, index) {
				continue
			}
		}
		n.commitIndex = index
		n.clearCommittedConfigLocked()
		return
	}
}

// replicaCount 是投票集合里日志至少到 index 的节点数。自己那条已在本地日志里，
// 且自己必在投票集合里，恒计入；matchIndex 只记其余成员（3.1 §7、6.1 §8 不变式 2）。
func (n *Node) replicaCount(voters []string, index uint64) int {
	count := 0
	for _, id := range voters {
		if id == n.cfg.NodeID || n.matchIndex[id] >= index {
			count++
		}
	}
	return count
}

// majorityReplicated 判断一组投票者里有没有多数派已经复制到 index。
func (n *Node) majorityReplicated(voters []string, index uint64) bool {
	if len(voters) == 0 {
		return false
	}
	return n.replicaCount(voters, index) >= len(voters)/2+1
}

// applyCommitted 把 (lastApplied, commitIndex] 应用进 KV 状态机（不变式 4）。
// 状态机改动只动内存：数据的落盘边界在日志追加那次 Sync 上已经定下，apply 之后只写 state 游标。
// 重复 apply 由赋值的幂等兜住（D4）。调用方必须持锁。
func (n *Node) applyCommitted() error {
	pending := 0
	for index := n.lastApplied + 1; index <= n.commitIndex; index++ {
		e, ok := n.log.entryAt(index)
		if !ok {
			return ErrStateMismatchAt(index, "commitIndex points past the wal")
		}
		if len(e.cmd) > 0 {
			if isConfigEntry(e.cmd) {
				// 配置条目没有 KV 副作用；视图在追加时已经生效，这里只校验记录完好（6.1 §7）。
				if _, err := decodeConfig(e.cmd, n.cfg.Peers); err != nil {
					return err
				}
			} else {
				key, value, err := kvstore.DecodeEntry(e.cmd, e.offset)
				if err != nil {
					return err
				}
				n.kv.Apply(key, value)
			}
		}
		n.lg.Debug("entry applied",
			zap.String("node_id", n.cfg.NodeID),
			zap.Uint64("index", index),
			zap.Uint64("term", e.term),
			zap.Uint64("commit_index", n.commitIndex),
			zap.Uint64("last_applied", index))
		pending++
		if pending == maxApplyEntries {
			if err := n.finishApply(index); err != nil {
				return err
			}
			pending = 0
		}
	}
	if pending > 0 {
		return n.finishApply(n.commitIndex)
	}
	return nil
}

// finishApply 是一批 apply 的提交点，顺序不能换：状态机改动已经在调用方落到内存 →
// 游标进内存并写回 state → 兑现这一段上的客户端等待。
// state 文件里只有游标，数据的落盘边界在日志追加的那次 Sync 上，所以游标落盘失败时
// 这一批照常兑现，错误只作为「apply failed」记进日志：盘上的游标停在旧边界，
// 重启从那里回放会把已经生效的条目再 apply 一遍，赋值的幂等兜住（D4）。
// 方向不能反：state 里的 lastApplied 永远不许领先于状态机真正应用的位置。
func (n *Node) finishApply(through uint64) error {
	previous := n.lastApplied
	n.lastApplied = through
	storeErr := n.state.store(n.currentState())
	n.wakeApplyWaiters(through)
	for index := previous + 1; index <= through; index++ {
		if ch, ok := n.proposals[index]; ok {
			ch <- nil
			delete(n.proposals, index)
		}
	}
	return storeErr
}

// wakeApplyWaiters 兑现 applyWaiters 里读索引不超过 through 的那些格（4.1 §8 不变式 1）。
// 兑现点在 state 落盘之后：读被叫醒时状态机已经真的追上了这个 index。
// 调用方必须持锁。
func (n *Node) wakeApplyWaiters(through uint64) {
	for index, waiters := range n.applyWaiters {
		if index > through {
			continue
		}
		for _, ch := range waiters {
			ch <- struct{}{}
		}
		delete(n.applyWaiters, index)
	}
}

// logApplyFailure 打的是已提交条目进不了状态机这条链：复制已经兑现，
// kv 层的失效行（2.1 §8）在同一条链的下游。
func (n *Node) logApplyFailure(err error) {
	n.lg.Error("apply failed",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("commit_index", n.commitIndex),
		zap.Uint64("last_applied", n.lastApplied),
		zap.Error(err))
}
