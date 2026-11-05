package raft

import (
	"go.uber.org/zap"
)

// 一次 AppendEntries 最多带 64 条（3.1 §7）：单条 entry 有 4MB 上限，靠条数把一批的字节量卡住。
const maxAppendEntries = 64

// appendOnce 组一次发给 peer 的 AppendEntries 并排进 outbox：
// nextIndex 的前一条是 prevLogIndex／prevLogTerm，从 nextIndex 起最多 64 条随请求发出。
// 没有待复制的条目时带空数组，这一条就是心跳——复制与心跳共用这一个函数。
// 调用方必须持锁：它读 nextIndex，还要往 outbox 尾部追加。
func (n *Node) appendOnce(peerID string) {
	next := n.nextIndex[peerID]
	if next < firstRaftIndex {
		next = firstRaftIndex
	}
	req := AppendRequest{
		Term:         n.term,
		LeaderID:     n.cfg.NodeID,
		PrevLogIndex: next - 1,
		LeaderCommit: n.commitIndex,
		Entries:      []LogEntry{},
	}
	// 在途确认轮存在时把本轮编号带上：为读发起的广播与例行心跳共用这一个函数，
	// 区别只有这个字段是不是 0（4.1 §7）。
	if n.activeRound != nil {
		req.ReadRound = n.activeRound.round
	}
	if req.PrevLogIndex > 0 {
		if e, ok := n.log.entryAt(req.PrevLogIndex); ok {
			req.PrevLogTerm = e.term
		}
	}
	for _, e := range n.log.entriesFrom(next, maxAppendEntries) {
		req.Entries = append(req.Entries, LogEntry{Term: e.term, Index: e.index, Command: e.cmd})
	}
	n.outbox = append(n.outbox, rpcTask{peerID: peerID, app: &req})
}

// broadcastAppends 按成员 id 升序给其余节点各排一次，D23 要的发送顺序就是这条序列。
func (n *Node) broadcastAppends() {
	for _, peerID := range n.cfg.otherPeers() {
		n.appendOnce(peerID)
	}
}

// handleAppendEntries 走 3.1 §7 的六步，空条目数组与带条目的数组在同一条路径上。
// term 比较先于一切；一致性检查不过就带回退提示，过了才截后缀、追加并 Sync raft 日志；
// commitIndex 只跟着 Leader 给的值走，推进之后当场交给 apply 循环。
func (n *Node) handleAppendEntries(req AppendRequest) AppendReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1、2：旧 term 原样拒绝、不重置选举截止；更大的 term 先落 state 再生效（不变式 5）。
	// state 落不了盘就不接受这个 term，按本地 term 拒绝。
	if req.Term < n.term {
		return AppendReply{Term: n.term, ReadRound: req.ReadRound}
	}
	if req.Term > n.term {
		if err := n.becomeFollower(req.Term, "leader contact"); err != nil {
			return AppendReply{Term: n.term, ReadRound: req.ReadRound}
		}
	} else if n.role == RoleCandidate {
		// 同 term 里有别的节点在发 AppendEntries，说明它已经拿到多数派（3.1 §7）。
		if err := n.becomeFollower(n.term, "candidate heard a same-term leader"); err != nil {
			return AppendReply{Term: n.term, ReadRound: req.ReadRound}
		}
	}
	if n.role != RoleFollower {
		// 本节点自认 Leader：同 term 的第二个 Leader 不许把它的日志掀掉，拒绝且不回退提示。
		return AppendReply{Term: n.term, Success: false, ReadRound: req.ReadRound}
	}

	// 3：term 相等或刚被更新都算有效 Leader 联系，记下 leaderID 并重置选举截止。
	n.leaderID = req.LeaderID
	n.electionDeadline = n.rollElectionDeadline()

	// 4：一致性检查。prevLogIndex=0 表示从日志头开始，直接过。
	if req.PrevLogIndex > 0 {
		prev, ok := n.log.entryAt(req.PrevLogIndex)
		if !ok {
			// 本地日志没这么长：conflictTerm 回 null，conflictIndex 取本地长度+1。
			return n.rejectAppend(nil, n.log.lastIndex()+1, req.ReadRound)
		}
		if prev.term != req.PrevLogTerm {
			term := prev.term
			index, ok := n.log.firstIndexOfTerm(term)
			if !ok {
				index = prev.index
			}
			return n.rejectAppend(&term, index, req.ReadRound)
		}
	}

	// 5：带条目时才截掉 prevLogIndex 之后的本地后缀，再按序追加，Sync 成功才许回 success=true。
	// 空数组的心跳不截：它只确认位置，删日志的是带条目的那一次（3.1 §7 第 5 步）。
	if len(req.Entries) > 0 {
		if err := n.log.truncateTo(req.PrevLogIndex); err != nil {
			return AppendReply{Term: n.term, Success: false, ReadRound: req.ReadRound}
		}
		for _, e := range req.Entries {
			if _, err := n.log.appendEntry(e.Term, e.Index, e.Command); err != nil {
				return AppendReply{Term: n.term, Success: false, ReadRound: req.ReadRound}
			}
		}
		if err := n.log.sync(); err != nil {
			return AppendReply{Term: n.term, Success: false, ReadRound: req.ReadRound}
		}
	}

	// 6：commitIndex = min(leaderCommit, 本地最后一条 index)，推进之后当场 apply。
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.log.lastIndex())
	}
	// apply 失败不改这条回执：条目已在本地日志里 Sync 完成，复制的承诺就此兑现，
	// 状态机进不去是 kv 层那条链的事（D5）。
	if err := n.applyCommitted(); err != nil {
		n.logApplyFailure(err)
	}
	return AppendReply{Term: n.term, Success: true, ReadRound: req.ReadRound}
}

func (n *Node) rejectAppend(conflictTerm *uint64, conflictIndex, readRound uint64) AppendReply {
	return AppendReply{Term: n.term, Success: false, ConflictTerm: conflictTerm,
		ConflictIndex: conflictIndex, ReadRound: readRound}
}

// handleAppendReply 处理一条回执（3.1 §7 的 Leader 一侧）。
// 成功就推进 matchIndex／nextIndex 并试着提交，被拒就按 D20 回退 nextIndex 并立刻重发。
// 由回执路径在锁内调用。
func (n *Node) handleAppendReply(peerID string, req AppendRequest, reply AppendReply) {
	if reply.Term > n.term {
		_ = n.becomeFollower(reply.Term, "append reply carries a higher term")
		return
	}
	// term 更小的响应被忽略；上一任期的请求、降级之后的回执都不算数（3.2 §3 append 行）。
	if reply.Term < n.term || req.Term != n.term || n.role != RoleLeader {
		return
	}

	// 确认轮的计票与复制游标互不干扰：这一段只认轮次编号，不碰 matchIndex／nextIndex（4.1 §7）。
	n.countReadRound(peerID, reply)

	if reply.Success {
		through := req.PrevLogIndex + uint64(len(req.Entries))
		// 两个游标只前进：一次成功说明该节点至少到 through，落后的提示不能把它拉回去。
		n.matchIndex[peerID] = max(n.matchIndex[peerID], through)
		n.nextIndex[peerID] = max(n.nextIndex[peerID], through+1)
		n.advanceCommit()
		if err := n.applyCommitted(); err != nil {
			n.logApplyFailure(err)
		}
		return
	}

	fallback, resend := n.fallbackIndex(peerID, reply)
	if resend {
		n.nextIndex[peerID] = fallback
	} else {
		fallback = n.nextIndex[peerID]
	}
	n.logAppendDenied(peerID, req, reply, fallback)
	if resend {
		n.appendOnce(peerID)
	}
}

// fallbackIndex 按 D20 的三个分支算新的 nextIndex，第二个返回值是 false 表示没有可退的余地：
// nextIndex 已经在日志头，再发一次还是同一条被拒的请求，留给下一次心跳。
// 提示值不比原 nextIndex 更小时退一步，回退因此严格递减，一轮重发必然收敛（3.1 §7）。
func (n *Node) fallbackIndex(peerID string, reply AppendReply) (uint64, bool) {
	current := n.nextIndex[peerID]
	if current <= firstRaftIndex {
		return 0, false
	}
	fallback := reply.ConflictIndex
	if reply.ConflictTerm != nil {
		// 该 term 在本节点日志里存在：落在它最后一条之后；不存在就照 conflictIndex 走。
		if last, ok := n.log.lastIndexOfTerm(*reply.ConflictTerm); ok {
			fallback = last + 1
		}
	}
	if fallback >= current {
		fallback = current - 1
	}
	return fallback, fallback >= firstRaftIndex
}

// logAppendDenied 的字段口径见 3.1 §10；conflictTerm 是 null 时这个字段整个不出现。
func (n *Node) logAppendDenied(peerID string, req AppendRequest, reply AppendReply, nextIndex uint64) {
	fields := []zap.Field{
		zap.String("peer_id", peerID),
		zap.Uint64("prev_log_index", req.PrevLogIndex),
		zap.Uint64("conflict_index", reply.ConflictIndex),
		zap.Uint64("next_index", nextIndex),
	}
	if reply.ConflictTerm != nil {
		fields = append(fields, zap.Uint64("conflict_term", *reply.ConflictTerm))
	}
	n.lg.Info("append denied", fields...)
}
