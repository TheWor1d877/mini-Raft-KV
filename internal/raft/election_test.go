package raft

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// electionFixture 是 5 节点集群，node-1 从 term 2 起、日志里有一条 term=2 的条目。
// node-1 发出的每一票与每一次心跳都由夹具按序号注入回执，
// 所以对端节点的真实处理不会把事件混进这份观察者日志（D23）。
func electionFixture(t *testing.T) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.InfoLevel)
	c := newCluster(t, 5, zap.New(core))
	n := c.reseed("node-1", State{CurrentTerm: 2}, []seedEntry{{term: 2, index: 1}})
	return c, n, logs
}

func grant(term uint64) VoteReply { return VoteReply{Term: term, VoteGranted: true} }
func deny(term uint64) VoteReply  { return VoteReply{Term: term, VoteGranted: false} }

// appendOK 是夹具注入的 AppendEntries 回执：term 跟着本轮的 term 走，success 为真。
func appendOK() AppendReply {
	return AppendReply{Term: uint64(3), Success: true}
}

// tickTo 按次数驱动 Tick：次数取自节点自己的选举截止，全部用例零 sleep（D16）。
func tickTo(t *testing.T, n *Node, ticks int) {
	t.Helper()
	for range ticks {
		n.Tick()
	}
}

// runElection 按选举截止驱动 Tick，跑完一整轮：串行回执让当选也发生在这一次 Tick 里，
// 所以这里不断言角色，由用例自己按注入的回执断言 Candidate 还是 Leader（D23）。
func runElection(t *testing.T, n *Node) {
	t.Helper()
	tickTo(t, n, n.electionDeadline)
}

// 随机超时：截止全部落在 [T,2T)，200 次抽样的极差要大于 T/2，
// 否则「大家同时发起投票」的活锁风险根本没被这段代码挡住（3.1 §6）。
func TestElectionDeadlineIsRandomInRange(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), func(cfg *Config) {
		cfg.ElectionTimeoutMS = 1000
		cfg.TickMS = 50
	})
	n := c.node("node-1")
	if want, got := 1000/50, n.electionTicks; got != want {
		t.Fatalf("electionTicks = %d, want ElectionTimeoutMS/TickMS = %d", got, want)
	}

	const samples = 200
	low, high := 2*n.electionTicks, 0
	for range samples {
		got := n.rollElectionDeadline()
		if got < n.electionTicks || got >= 2*n.electionTicks {
			t.Fatalf("deadline %d outside [%d,%d)", got, n.electionTicks, 2*n.electionTicks)
		}
		if got < low {
			low = got
		}
		if got > high {
			high = got
		}
	}
	if spread, want := high-low, n.electionTicks/2; spread <= want {
		t.Errorf("spread of %d samples = %d, want more than T/2 = %d", samples, spread, want)
	}
}

// 截止的前一 tick 角色不动，到点那一 tick 转 Candidate：term++、votedFor=自己、state 已 Sync
// （不变式 5 的「先落盘再生效」，3.1 §7）。
func TestElectionTimeoutStartsRound(t *testing.T) {
	c, n, logs := electionFixture(t)
	c.injectVotes("node-1", deny(3), deny(3), deny(3), deny(3))
	deadline := n.electionDeadline

	tickTo(t, n, deadline-1)
	if got := n.Role(); got != RoleFollower {
		t.Fatalf("role one tick before the deadline = %s, want follower", got)
	}
	n.Tick()
	if got := n.Role(); got != RoleCandidate {
		t.Fatalf("role at the deadline = %s, want candidate", got)
	}

	if got := n.Stats(); got.Term != 3 || got.Role != RoleCandidate || got.Votes != 1 {
		t.Fatalf("stats = %+v, want term 3, candidate, one self vote", got)
	}
	if n.votedFor != "node-1" {
		t.Errorf("votedFor = %q, want the candidate itself", n.votedFor)
	}
	disk, err := newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if disk.CurrentTerm != 3 || disk.VotedFor != "node-1" {
		t.Errorf("disk = %+v, want the new term and the self vote persisted", disk)
	}

	roles := logs.FilterMessage("role changed")
	if roles.Len() != 1 {
		t.Fatalf("role changed entries = %d, want 1: %+v", roles.Len(), roles.All())
	}
	if got := fieldOf(roles.All()[0], "new_role"); got != "candidate" {
		t.Errorf("new_role = %q, want candidate", got)
	}
	terms := logs.FilterMessage("term changed")
	if terms.Len() != 1 {
		t.Fatalf("term changed entries = %d, want 1: %+v", terms.Len(), terms.All())
	}
	if got := fieldOf(terms.All()[0], "reason"); got != "election timeout" {
		t.Errorf("term change reason = %q, want election timeout", got)
	}
}

// 一轮选举发出的投票条数等于 peer 数、顺序按成员 id 升序、每条都带自己的日志位置（D23）。
func TestElectionSendsOneVotePerPeerInOrder(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", deny(3), deny(3), deny(3), deny(3))
	runElection(t, n)
	if got := n.Role(); got != RoleCandidate {
		t.Fatalf("role = %s, want the round to stop at candidate", got)
	}

	calls := c.calls("node-1")
	if len(calls) != 4 {
		t.Fatalf("sent RPCs = %d, want one per peer: %+v", len(calls), calls)
	}
	want := []string{"node-2", "node-3", "node-4", "node-5"}
	for i, call := range calls {
		if call.kind != "vote" || call.to != want[i] {
			t.Fatalf("call %d = %+v, want a vote to %s", i, call, want[i])
		}
		if got := call.vote; got.Term != 3 || got.CandidateID != "node-1" ||
			got.LastLogIndex != 1 || got.LastLogTerm != 2 {
			t.Errorf("vote request %d = %+v, want term 3 from node-1 with log position 2/1", i, got)
		}
	}
}

// 3 票当选：自票加两票到 N/2+1，转 Leader、两个复制游标初始化、
// 追加一条 command 为空字节的 no-op，并按成员顺序发出第一轮复制请求——这一轮就把 no-op 带出去。
// 四条 success 回执让 no-op 拿到多数派，commitIndex 与 lastApplied 一起走到 2（3.1 §7、§8 不变式 3）。
func TestElectionWinsWithMajority(t *testing.T) {
	c, n, logs := electionFixture(t)
	c.injectVotes("node-1", grant(3), grant(3), deny(3), deny(3))
	c.injectAppends("node-1", appendOK(), appendOK(), appendOK(), appendOK())
	runElection(t, n)

	if got := n.Stats(); got.Role != RoleLeader || got.Votes != 0 {
		t.Fatalf("stats = %+v, want leader with the tally cleared", got)
	}
	roles := logs.FilterMessage("role changed")
	if roles.Len() != 2 {
		t.Fatalf("role changed entries = %d, want 2: %+v", roles.Len(), roles.All())
	}
	if got := fieldOf(roles.All()[1], "new_role"); got != "leader" {
		t.Errorf("second role change = %q, want leader", got)
	}
	counted := logs.FilterMessage("votes counted")
	if counted.Len() != 2 {
		t.Fatalf("votes counted entries = %d, want 2", counted.Len())
	}
	final := counted.All()[1].ContextMap()
	if final["votes"] != int64(3) || final["needed"] != int64(3) {
		t.Errorf("final tally = %v/%v, want 3/3", final["votes"], final["needed"])
	}

	// 自己那一格没有回执可推进，留下的就是当选时置的初值。
	if got := n.nextIndex["node-1"]; got != 2 {
		t.Errorf("own nextIndex = %d, want lastLogIndex+1 = 2 as set at election time", got)
	}
	if got := n.matchIndex["node-1"]; got != 0 {
		t.Errorf("own matchIndex = %d, want 0", got)
	}
	// 四条 success 回执把游标推到 no-op 之后。
	if got := n.matchIndex["node-4"]; got != 2 {
		t.Errorf("matchIndex = %d, want the no-op replicated through index 2", got)
	}
	if got := n.nextIndex["node-4"]; got != 3 {
		t.Errorf("nextIndex = %d, want 3 once index 2 is acknowledged", got)
	}

	noop, ok := n.log.entryAt(2)
	if !ok {
		t.Fatalf("the no-op entry is missing: log length %d", n.log.length())
	}
	if noop.term != 3 || len(noop.cmd) != 0 {
		t.Errorf("no-op = term %d command %q, want term 3 with an empty command", noop.term, noop.cmd)
	}

	calls := c.calls("node-1")
	if len(calls) != 8 {
		t.Fatalf("sent RPCs = %d, want 4 votes plus one replication round", len(calls))
	}
	for _, call := range calls[4:] {
		req := call.app
		if call.kind != "append" || len(req.Entries) != 1 {
			t.Fatalf("first replication round = %+v, want one AppendEntries carrying the no-op", req)
		}
		if req.LeaderID != "node-1" || req.PrevLogIndex != 1 || req.PrevLogTerm != 2 {
			t.Errorf("request to %s = %+v, want prev at the seeded entry 2/1", call.to, req)
		}
		if e := req.Entries[0]; e.Index != 2 || e.Term != 3 || len(e.Command) != 0 {
			t.Errorf("entry to %s = %+v, want the no-op at index 2, term 3, empty command", call.to, e)
		}
		if req.LeaderCommit != 0 {
			t.Errorf("leaderCommit = %d, want 0: nothing is committed on this round yet", req.LeaderCommit)
		}
	}

	if got := n.Stats(); got.CommitIndex != 2 || got.LastApplied != 2 {
		t.Errorf("stats = %+v, want the no-op committed and applied at index 2", got)
	}
}

// 2 票不选：自票加一票不够 N/2+1，节点停在 Candidate（3.2 §6 的阈值变异）。
func TestElectionStopsBelowMajority(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", grant(3), deny(3), deny(3), deny(3))
	runElection(t, n)

	if got := n.Stats(); got.Role != RoleCandidate || got.Votes != 2 {
		t.Fatalf("stats = %+v, want candidate holding 2 votes", got)
	}
	if calls := c.calls("node-1"); len(calls) != 4 {
		t.Errorf("an unterminated tally produced extra RPCs: %+v", calls)
	}
}

// 未过半又到期：term++ 重开一轮，上一轮的票数不带过来（3.1 §7 末段）。
func TestElectionRestartsWithAnotherTerm(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", deny(3), deny(3), deny(3), deny(3),
		deny(4), deny(4), deny(4), deny(4))
	runElection(t, n)
	if got := n.Stats(); got.Role != RoleCandidate || got.Votes != 1 {
		t.Fatalf("first round = %+v, want candidate holding only the self vote", got)
	}

	runElection(t, n)

	if got := n.Stats(); got.Term != 4 || got.Role != RoleCandidate || got.Votes != 1 {
		t.Fatalf("stats after the second round = %+v, want term 4 with a fresh self vote", got)
	}
	if got := n.electionDeadline; got < n.electionTicks || got >= 2*n.electionTicks {
		t.Errorf("deadline re-roll = %d, want a fresh value in [%d,%d)",
			got, n.electionTicks, 2*n.electionTicks)
	}
	calls := c.calls("node-1")
	if len(calls) != 8 {
		t.Fatalf("sent RPCs = %d, want two rounds of 4", len(calls))
	}
	for _, call := range calls[4:] {
		if call.vote.Term != 4 {
			t.Fatalf("second-round vote = %+v, want term 4", call.vote)
		}
	}
}

// 同 term 的有效心跳让 Candidate 立即转 Follower，并把 Leader 记下来；
// term 更小的心跳既不动角色也不重置截止（3.1 §7 的第 2、3 步）。
func TestCandidateStepsDownOnSameTermHeartbeat(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", deny(3), deny(3), deny(3), deny(3))
	runElection(t, n)
	if got := n.Role(); got != RoleCandidate {
		t.Fatalf("role = %s, want candidate before the heartbeat test", got)
	}
	n.electionDeadline = 1

	reply := n.handleAppendEntries(AppendRequest{Term: 3, LeaderID: "node-3", Entries: []LogEntry{}})
	if !reply.Success || reply.Term != 3 {
		t.Fatalf("heartbeat reply = %+v, want success at term 3", reply)
	}
	if got := n.Role(); got != RoleFollower {
		t.Fatalf("role = %s, want follower", got)
	}
	id, addr, ok := n.Leader()
	if !ok || id != "node-3" || addr != c.raftCfg["node-1"].Addr("node-3") {
		t.Errorf("leader hint = %q %q %v, want node-3 at its peer address", id, addr, ok)
	}
	if got := n.electionDeadline; got < n.electionTicks || got >= 2*n.electionTicks {
		t.Errorf("deadline = %d, want the heartbeat to re-roll it into [%d,%d)",
			got, n.electionTicks, 2*n.electionTicks)
	}

	before := n.electionDeadline
	stale := n.handleAppendEntries(AppendRequest{Term: 2, LeaderID: "node-9", Entries: []LogEntry{}})
	if stale.Success || stale.Term != 3 {
		t.Errorf("stale heartbeat reply = %+v, want denial at the local term 3", stale)
	}
	if got := n.electionDeadline; got != before {
		t.Errorf("deadline = %d, want it untouched by a stale term", got)
	}
}

// Stats 的六个读数与内存一致，测试与注入脚本读的是同一份（3.1 §4）。
func TestStatsMatchesMemory(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", grant(3), grant(3), deny(3), deny(3))
	c.injectAppends("node-1", appendOK(), appendOK(), appendOK(), appendOK())
	runElection(t, n)

	got := n.Stats()
	if got.Term != n.term || got.Role != n.role || got.Votes != uint64(n.votes) ||
		got.LastLogIndex != n.log.lastIndex() || got.CommitIndex != n.commitIndex ||
		got.LastApplied != n.lastApplied {
		t.Fatalf("stats = %+v, want the memory state", got)
	}
	// 四条 success 回执把 no-op（index 2，当前任期）送进多数派，两个游标跟着走到 2。
	if got.LastLogIndex != 2 || got.CommitIndex != 2 || got.LastApplied != 2 {
		t.Errorf("stats = %+v, want two log entries, commitIndex 2 and lastApplied 2", got)
	}
}

// 在途提案的取消路径（T22.4）：转成 Follower 时未兑现的回调全部回 ErrNotLeader，
// Close 时回 ErrCanceled。登记走包内未导出函数，测试不起 goroutine。
func TestPendingProposalsCanceled(t *testing.T) {
	c, n, _ := electionFixture(t)
	c.injectVotes("node-1", grant(3), grant(3), deny(3), deny(3))
	c.injectAppends("node-1", appendOK(), appendOK(), appendOK(), appendOK())
	runElection(t, n)

	n.mu.Lock()
	ch := n.registerProposal(3)
	n.mu.Unlock()

	n.handleAppendEntries(AppendRequest{Term: 4, LeaderID: "node-4", Entries: []LogEntry{}})
	if err := <-ch; !errors.Is(err, ErrNotLeader) {
		t.Fatalf("proposal after step down = %v, want ErrNotLeader", err)
	}
	if got := n.Role(); got != RoleFollower {
		t.Fatalf("role = %s, want follower", got)
	}

	n.mu.Lock()
	keep := n.registerProposal(4)
	n.mu.Unlock()
	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := <-keep; !errors.Is(err, ErrCanceled) {
		t.Fatalf("proposal after Close = %v, want ErrCanceled", err)
	}
	delete(c.nodes, "node-1")
}

// 构造完即完成恢复：term 与 votedFor 取自盘上的 state，角色一律 Follower，选举截止落在 [T,2T)，
// 未 Start 也能读状态；已提交没应用的条目由构造补上，lastApplied 追平 commitIndex（3.1 §4 NewNode 行、§7 启动）。
func TestNewNodeRecoversFromDisk(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), func(cfg *Config) {
		cfg.ElectionTimeoutMS = 1000
		cfg.TickMS = 50
	})
	want := State{CurrentTerm: 3, VotedFor: "node-2", CommitIndex: 2, LastApplied: 1}
	n := c.reseed("node-1", want, []seedEntry{{term: 2, index: 1}, {term: 3, index: 2}})

	if got := n.Role(); got != RoleFollower {
		t.Errorf("role right after NewNode = %s, want %s", got, RoleFollower)
	}
	if id, addr, ok := n.Leader(); ok || id != "" || addr != "" {
		t.Errorf("leader right after NewNode = (%q, %q, %v), want nothing known", id, addr, ok)
	}
	if n.votedFor != want.VotedFor {
		t.Errorf("votedFor = %q, want the recovered %q", n.votedFor, want.VotedFor)
	}
	if got := n.Stats(); got.Term != want.CurrentTerm || got.CommitIndex != want.CommitIndex ||
		got.LastApplied != want.CommitIndex || got.LastLogIndex != 2 || got.Votes != 0 {
		t.Errorf("stats after recovery = %+v, want term 3, commit 2, lastApplied caught up to 2, lastLogIndex 2, votes 0", got)
	}
	disk, err := newStateFile(c.raftCfg["node-1"], zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if disk.LastApplied != want.CommitIndex || disk.CommitIndex != want.CommitIndex {
		t.Errorf("disk after the catch-up apply = %+v, want both cursors at 2", disk)
	}
	if d := n.electionDeadline; d < n.electionTicks || d >= 2*n.electionTicks {
		t.Errorf("election deadline after recovery = %d, want in [%d,%d)", d, n.electionTicks, 2*n.electionTicks)
	}
}

// state 的游标比日志长，两个文件至少有一个不是同一次落盘的产物：构造必须拒绝，
// 否则 apply 循环会去补一段盘上不存在的日志（3.1 §4 NewNode 行、不变式 4）。
func TestNewNodeRejectsStateAheadOfLog(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	if err := c.stopNode("node-1"); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	cfg := c.raftCfg["node-1"]

	for name, st := range map[string]State{
		"commitIndex past the last entry": {CurrentTerm: 2, CommitIndex: 1},
		"lastApplied past commitIndex":    {CurrentTerm: 2, CommitIndex: 1, LastApplied: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if err := newStateFile(cfg, zap.NewNop()).store(st); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			kv, err := kvstore.Open(c.kvCfg["node-1"], zap.NewNop())
			if err != nil {
				t.Fatalf("open kv: %v", err)
			}
			t.Cleanup(func() { _ = kv.Close() })

			if _, err := NewNode(cfg, kv, &stubTransport{c: c, id: "node-1"}, zap.NewNop()); !errors.Is(err, ErrStateMismatch) {
				t.Errorf("NewNode with %+v = %v, want ErrStateMismatch", st, err)
			}
		})
	}
}
