package raft

import "go.uber.org/zap"

// handleRequestVote 走 3.1 §7 的五步判定，全部在锁内完成：
// term 比较先于一切，第 5 步的 votedFor 必须先落盘并 Sync 才许回 voteGranted=true（不变式 1、2）。
func (n *Node) handleRequestVote(req VoteRequest) VoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.lg.Info("request vote received",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", req.Term),
		zap.String("candidate_id", req.CandidateID),
		zap.Uint64("last_log_index", req.LastLogIndex),
		zap.Uint64("last_log_term", req.LastLogTerm))

	// 1. 更大的 term 先进账：更新 term、清空 votedFor、转 Follower、落 state，然后继续判定。
	//    落盘失败就不接受这个 term，按本地 term 回绝（不变式 5）。
	if req.Term > n.term {
		if err := n.becomeFollower(req.Term, "vote request carries a higher term"); err != nil {
			return n.denyVote(req, "state_unpersisted")
		}
	}
	// 2. 旧 term 直接拒，votedFor 不动。
	if req.Term < n.term {
		return n.denyVote(req, "term_old")
	}
	// 3. 本任期已经投过人，且不是这个候选人。
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return n.denyVote(req, "already_voted")
	}
	// 4. 候选人的日志至少要和本节点一样新。
	if !n.logIsCurrentEnough(req) {
		return n.denyVote(req, "log_stale")
	}
	// 5. 五条都过：记下这票并 Sync，回执在盘上落定之后才发出去。
	if err := n.transition(n.role, n.term, req.CandidateID, "vote granted"); err != nil {
		return n.denyVote(req, "state_unpersisted")
	}
	n.electionDeadline = n.rollElectionDeadline()
	n.lg.Info("vote granted",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", req.Term),
		zap.String("candidate_id", req.CandidateID))
	return VoteReply{Term: n.term, VoteGranted: true}
}

func (n *Node) denyVote(req VoteRequest, reason string) VoteReply {
	n.lg.Info("vote denied",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", req.Term),
		zap.String("candidate_id", req.CandidateID),
		zap.String("reason", reason))
	return VoteReply{Term: n.term}
}

// logIsCurrentEnough 逐项比 (lastLogTerm, lastLogIndex)：先比 term，term 相同再比 index。
// 只比 index 会让一个日志更短但 term 更高的候选人骗到票（3.2 §6 的第二条变异）。
func (n *Node) logIsCurrentEnough(req VoteRequest) bool {
	myTerm, myIndex := n.log.lastTerm(), n.log.lastIndex()
	if req.LastLogTerm != myTerm {
		return req.LastLogTerm > myTerm
	}
	return req.LastLogIndex >= myIndex
}

// handleVoteReply 计一票（3.1 §7 的发起选举一侧）。由 Tick 在锁外发完 RPC 之后当场调用。
func (n *Node) handleVoteReply(req VoteRequest, reply VoteReply) {
	if reply.Term > n.term {
		_ = n.becomeFollower(reply.Term, "vote reply carries a higher term")
		return
	}
	// 旧 term、旧轮次、被拒的票都不计数；角色已经不是 Candidate 说明这一轮作废了。
	if reply.Term < n.term || req.Term != n.term || n.role != RoleCandidate || !reply.VoteGranted {
		return
	}

	n.votes++
	needed := n.cfg.Majority()
	n.lg.Info("votes counted",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("term", n.term),
		zap.Int("votes", n.votes),
		zap.Int("needed", needed))
	if n.votes >= needed {
		n.becomeLeader("majority of votes")
	}
}
