package raft

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// voteFixture 是一个单节点集群，盘上是 term=2 加三条 term=2 的日志条目，votedFor 为空。
// term 与日志都走 load 路径进来，所以每条断言读的都是恢复之后的节点（3.2 §3 第三行）。
func voteFixture(t *testing.T) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	return seededVoteFixture(t, State{CurrentTerm: 2})
}

func seededVoteFixture(t *testing.T, st State) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.InfoLevel)
	c := newCluster(t, 1, zap.New(core))
	seeded := []seedEntry{{term: 2, index: 1}, {term: 2, index: 2}, {term: 2, index: 3}}
	return c, c.reseed("node-1", st, seeded), logs
}

// denyReason 读最近一条 vote denied 的 reason；没有这条日志就报错，
// 因为拒绝必须带原因（3.1 §10），静默拒绝等于测试没看到东西。
func denyReason(t *testing.T, logs *observer.ObservedLogs) string {
	t.Helper()
	denied := logs.FilterMessage("vote denied")
	if denied.Len() != 1 {
		t.Fatalf("vote denied entries = %d, want exactly 1: %+v", denied.Len(), denied.All())
	}
	return fieldOf(denied.All()[0], "reason")
}

// 第 2 步：旧 term 直接拒，votedFor 一个字都不动，回执带本地 term。
func TestVoteRejectsOlderTerm(t *testing.T) {
	_, n, logs := voteFixture(t)

	reply := n.handleRequestVote(VoteRequest{Term: 1, CandidateID: "node-2", LastLogIndex: 9, LastLogTerm: 1})
	if reply.VoteGranted || reply.Term != 2 {
		t.Fatalf("reply = %+v, want no vote and the local term 2", reply)
	}
	if got := denyReason(t, logs); got != "term_old" {
		t.Errorf("deny reason = %q, want term_old", got)
	}
	if n.votedFor != "" {
		t.Errorf("votedFor = %q, want it untouched by an old term", n.votedFor)
	}
	if n.Role() != RoleFollower || n.Stats().Term != 2 {
		t.Errorf("role/term changed by an old request: %s term=%d", n.Role(), n.Stats().Term)
	}
}

// 第 1 步：更大的 term 先更新 term、清空 votedFor，然后才继续判定投票资格。
// 这条同时是「不清 votedFor」那处变异的杀手（3.2 §6）：不清的话 node-3 拿到的是 already_voted。
func TestVoteHigherTermClearsVotedForFirst(t *testing.T) {
	c, n, logs := seededVoteFixture(t, State{CurrentTerm: 2, VotedFor: "node-2"})

	reply := n.handleRequestVote(VoteRequest{Term: 3, CandidateID: "node-3", LastLogIndex: 3, LastLogTerm: 2})
	if !reply.VoteGranted || reply.Term != 3 {
		t.Fatalf("reply = %+v, want the vote at the new term 3", reply)
	}
	if n.term != 3 || n.votedFor != "node-3" || n.Role() != RoleFollower {
		t.Fatalf("memory = term %d votedFor %q role %s", n.term, n.votedFor, n.Role())
	}

	disk, err := newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if disk.CurrentTerm != 3 || disk.VotedFor != "node-3" {
		t.Errorf("disk = %+v, want the term and the vote committed together", disk)
	}
	changed := logs.FilterMessage("term changed")
	if changed.Len() != 1 {
		t.Fatalf("term changed entries = %d, want 1", changed.Len())
	}
	if got := fieldOf(changed.All()[0], "reason"); got != "vote request carries a higher term" {
		t.Errorf("term change reason = %q", got)
	}
}

// 第 3 步：本任期投过人，候选人不是他就拒，原因 already_voted。
func TestVoteDeniedWhenAlreadyVoted(t *testing.T) {
	_, n, logs := seededVoteFixture(t, State{CurrentTerm: 2, VotedFor: "node-2"})

	reply := n.handleRequestVote(VoteRequest{Term: 2, CandidateID: "node-3", LastLogIndex: 9, LastLogTerm: 9})
	if reply.VoteGranted {
		t.Fatalf("reply = %+v, want a denial: node-2 already holds this term's vote", reply)
	}
	if got := denyReason(t, logs); got != "already_voted" {
		t.Errorf("deny reason = %q, want already_voted", got)
	}
	if n.votedFor != "node-2" {
		t.Errorf("votedFor = %q, want the earlier promise kept", n.votedFor)
	}
}

// 第 4 步的四个方向：term 大／term 同 index 大／term 同 index 同／term 小。
// 四个方向都用同一个候选人，免得 votedFor 抢先在第 3 步把票挡掉。
func TestVoteLogComparisonFourWays(t *testing.T) {
	_, n, logs := voteFixture(t)

	for _, c := range []struct {
		name string
		req  VoteRequest
		want bool
	}{
		{"same term, longer log", VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 4, LastLogTerm: 2}, true},
		{"same term, same log", VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 3, LastLogTerm: 2}, true},
		{"older term, longer log", VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 9, LastLogTerm: 1}, false},
		{"newer term, shorter log", VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 1, LastLogTerm: 3}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs.TakeAll()
			reply := n.handleRequestVote(c.req)
			if reply.VoteGranted != c.want {
				t.Fatalf("granted = %v, want %v for %+v", reply.VoteGranted, c.want, c.req)
			}
			if c.want {
				return
			}
			if got := denyReason(t, logs); got != "log_stale" {
				t.Errorf("deny reason = %q, want log_stale", got)
			}
		})
	}

	// 四条判定全过之后，这票得写进 state 文件才算投出去（不变式 2）。
	disk, err := newStateFile(n.cfg, zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if disk.VotedFor != "node-2" {
		t.Errorf("disk votedFor = %q, want node-2", disk.VotedFor)
	}
}

// 不变式 1 的重启那半段：投过票之后重启，同 term 再请求投票必须拒。
// 这条就是 D14 的直接证据——votedFor 不落盘的话重启就把票收回了。
func TestVotePromiseSurvivesRestart(t *testing.T) {
	c, n, logs := voteFixture(t)

	if reply := n.handleRequestVote(VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 3, LastLogTerm: 2}); !reply.VoteGranted {
		t.Fatalf("first vote = %+v, want granted", reply)
	}
	logs.TakeAll()
	again := c.restart("node-1")

	reply := again.handleRequestVote(VoteRequest{Term: 2, CandidateID: "node-3", LastLogIndex: 9, LastLogTerm: 9})
	if reply.VoteGranted {
		t.Fatalf("vote after restart = %+v, want denial", reply)
	}
	if got := denyReason(t, logs); got != "already_voted" {
		t.Errorf("deny reason after restart = %q, want already_voted", got)
	}
	if again.votedFor != "node-2" {
		t.Errorf("votedFor after restart = %q, want the persisted promise", again.votedFor)
	}
}

// 第 5 步：store 落不了盘就不回 voteGranted=true（不变式 2 的后半段，前半段在 state_test.go）。
func TestVoteNotGrantedWhenStateStoreFails(t *testing.T) {
	c, n, logs := voteFixture(t)
	c.breakStateStore("node-1")

	reply := n.handleRequestVote(VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 4, LastLogTerm: 2})
	if reply.VoteGranted {
		t.Fatalf("reply = %+v, want denial: the vote never reached the disk", reply)
	}
	if got := denyReason(t, logs); got != "state_unpersisted" {
		t.Errorf("deny reason = %q, want state_unpersisted", got)
	}
	if n.votedFor != "" {
		t.Errorf("votedFor = %q, want memory untouched by a failed store", n.votedFor)
	}
	// 落不了盘的节点，收尾也按 ErrIO 报出来，不会静默当成正常退出。
	if err := c.stopNode("node-1"); !errors.Is(err, kvstore.ErrIO) {
		t.Errorf("Close with a broken state store = %v, want ErrIO", err)
	}
}

// 更大的 term 连落盘都做不了时，按本地 term 回绝，不把好比对新的日志当成接受。
func TestVoteHigherTermWithFailedStoreKeepsLocalTerm(t *testing.T) {
	c, n, logs := voteFixture(t)
	c.breakStateStore("node-1")

	reply := n.handleRequestVote(VoteRequest{Term: 7, CandidateID: "node-2", LastLogIndex: 9, LastLogTerm: 9})
	if reply.VoteGranted || reply.Term != 2 {
		t.Fatalf("reply = %+v, want denial at the local term 2", reply)
	}
	if n.term != 2 {
		t.Errorf("term = %d, want the unpersisted term 7 rejected", n.term)
	}
	if got := denyReason(t, logs); got != "state_unpersisted" {
		t.Errorf("deny reason = %q, want state_unpersisted", got)
	}
	if err := c.stopNode("node-1"); !errors.Is(err, kvstore.ErrIO) {
		t.Errorf("Close with a broken state store = %v, want ErrIO", err)
	}
}

// 投出票与拒票都要重置选举截止只在 granted 那一支：拒绝的候选人不算 Leader（3.1 §7）。
func TestVoteGrantResetsElectionDeadline(t *testing.T) {
	_, n, _ := voteFixture(t)
	n.electionTicks = 3
	n.electionDeadline = 1

	_ = n.handleRequestVote(VoteRequest{Term: 2, CandidateID: "node-2", LastLogIndex: 3, LastLogTerm: 2})
	if n.electionDeadline < 3 || n.electionDeadline >= 6 {
		t.Errorf("deadline after granting = %d, want a fresh roll in [3,6)", n.electionDeadline)
	}
}
