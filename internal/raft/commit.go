package raft

import (
	"context"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// apply 每攒够这么多条已提交条目调一次 kv.Flush，与 kvstore 的批次大小同值：
// 一批 apply 落一次 KV WAL 的 fsync（3.1 §7）。
const maxApplyEntries = 64

// advanceCommit 按不变式 3 推进 commitIndex：只因为「当前任期的条目被多数派复制」而前进，
// 旧任期的条目要等它后面的当前任期条目落定才被间接带走。
// 从日志末尾往前扫，第一个同时满足「属于当前任期」和「多数派已到该位置」的 index 就是新边界。
func (n *Node) advanceCommit() {
	for index := n.log.lastIndex(); index > n.commitIndex; index-- {
		e, ok := n.log.entryAt(index)
		if !ok || e.term != n.term {
			continue
		}
		if n.replicaCount(index) >= n.cfg.Majority() {
			n.commitIndex = index
			return
		}
	}
}

// replicaCount 是日志至少到 index 的节点数。自己那条已在本地日志里，恒计入；
// matchIndex 只记其余成员，Leader 不给自己发 AppendEntries（3.1 §7）。
func (n *Node) replicaCount(index uint64) int {
	count := 1
	for _, peerID := range n.cfg.otherPeers() {
		if n.matchIndex[peerID] >= index {
			count++
		}
	}
	return count
}

// applyCommitted 把 (lastApplied, commitIndex] 应用进 KV 状态机（不变式 4）。
// 内存游标只在 kv.Flush 成功之后才动：Flush 失败时游标与 state 文件都停在上一批的边界上，
// 下一轮从同一位置重试，重复 apply 由 KV 的幂等兜住（D4）。调用方必须持锁。
func (n *Node) applyCommitted() error {
	pending := 0
	for index := n.lastApplied + 1; index <= n.commitIndex; index++ {
		e, ok := n.log.entryAt(index)
		if !ok {
			return ErrStateMismatchAt(index, "commitIndex points past the raft log")
		}
		if len(e.cmd) > 0 {
			key, value, err := kvstore.DecodeEntry(e.cmd, e.offset)
			if err != nil {
				return err
			}
			if err := n.kv.Put(context.Background(), key, value); err != nil {
				return err
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

// finishApply 是一批 apply 的提交点，顺序不能换：KV 落盘 → 游标进内存并写回 state →
// 兑现这一段上的客户端等待。state 文件里 commitIndex 与 lastApplied 同批重写（不变式 4）。
func (n *Node) finishApply(through uint64) error {
	if err := n.kv.Flush(); err != nil {
		return err
	}
	previous := n.lastApplied
	n.lastApplied = through
	if err := n.state.store(n.currentState()); err != nil {
		return err
	}
	for index := previous + 1; index <= through; index++ {
		if ch, ok := n.proposals[index]; ok {
			ch <- nil
			delete(n.proposals, index)
		}
	}
	return nil
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
