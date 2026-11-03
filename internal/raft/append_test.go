package raft

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// termPtr 给 AppendReply.ConflictTerm 取值；nil 就是 JSON 里的 null（3.1 §4）。
func termPtr(term uint64) *uint64 { return &term }

// followerFixture 是盘上三条 term=2 条目的单节点集群，与 voteFixture 共用同一份播种：
// AppendEntries 的 Follower 六步就在它上面走。
func followerFixture(t *testing.T) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	return seededVoteFixture(t, State{CurrentTerm: 2})
}

// leaderFixture 让 node-1 当选 Leader：投票 3 票到位，当选那一轮复制走真实处理，
// no-op 因此拿到多数派、commitIndex 与 lastApplied 都追到 2。之后要注入多少轮回执、
// 注入成 success 还是冲突，全部由用例自己按 outbox 的条数预置。
func leaderFixture(t *testing.T) (*cluster, *Node, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	c := newCluster(t, 5, zap.New(core))
	n := c.reseed("node-1", State{CurrentTerm: 2}, []seedEntry{{term: 2, index: 1}})
	c.injectVotes("node-1", grant(3), grant(3), deny(3), deny(3))
	runElection(t, n)

	if got := n.Role(); got != RoleLeader {
		t.Fatalf("fixture role = %s, want leader", got)
	}
	if got := n.Stats(); got.CommitIndex != 2 || got.LastApplied != 2 || got.LastLogIndex != 2 {
		t.Fatalf("fixture stats = %+v, want the no-op committed and applied at index 2", got)
	}
	return c, n, logs
}

// 第 4、5 步：一致性检查过了，才截掉 prevLogIndex 之后的本地后缀并按序追加。
// 截断是这条路径唯一删日志的动作（3.1 §7）。
func TestAppendReplacesConflictingSuffixAfterCheckPasses(t *testing.T) {
	_, n, _ := followerFixture(t)

	reply := n.handleAppendEntries(AppendRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 3, Index: 3, Command: []byte("a")}, {Term: 3, Index: 4, Command: []byte("b")}},
	})
	if !reply.Success || reply.Term != 2 {
		t.Fatalf("reply = %+v, want success at the local term 2", reply)
	}
	if got := n.log.length(); got != 4 {
		t.Fatalf("log length = %d, want the term-2 entry at index 3 replaced by two term-3 entries", got)
	}
	if e, ok := n.log.entryAt(3); !ok || e.term != 3 || string(e.cmd) != "a" {
		t.Errorf("index 3 = %+v ok=%v, want term 3 command a", e, ok)
	}
	if e, ok := n.log.entryAt(4); !ok || e.term != 3 || string(e.cmd) != "b" {
		t.Errorf("index 4 = %+v ok=%v, want term 3 command b", e, ok)
	}
	if e, ok := n.log.entryAt(2); !ok || e.term != 2 {
		t.Errorf("index 2 = %+v ok=%v, want the agreed prefix untouched", e, ok)
	}
}

// prevLogIndex=0 表示从日志头开始，没有可对的位置，直接过检查（3.1 §7 第 4 步）。
func TestAppendPrevLogIndexZeroSkipsConsistencyCheck(t *testing.T) {
	core, _ := observer.New(zapcore.InfoLevel)
	c := newCluster(t, 1, zap.New(core))
	n := c.node("node-1")

	reply := n.handleAppendEntries(AppendRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: 0,
		Entries: []LogEntry{{Term: 2, Index: 1, Command: []byte("a")}, {Term: 2, Index: 2, Command: []byte("b")}},
	})
	if !reply.Success || reply.Term != 2 {
		t.Fatalf("reply = %+v, want success at the term the request carried", reply)
	}
	if got := n.log.length(); got != 2 {
		t.Fatalf("log length = %d, want both entries appended", got)
	}
	if got := n.Stats(); got.Term != 2 || got.CommitIndex != 0 {
		t.Errorf("stats = %+v, want term 2 adopted and nothing committed", got)
	}
}

// 回退分支三：本地日志没这么长，conflictTerm 回 null、conflictIndex 取本地长度+1（D20）。
func TestAppendRejectsWhenLocalLogIsTooShort(t *testing.T) {
	_, n, _ := followerFixture(t)

	reply := n.handleAppendEntries(AppendRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: 5, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 2, Index: 6, Command: []byte("a")}},
	})
	if reply.Success || reply.Term != 2 {
		t.Fatalf("reply = %+v, want denial at the local term 2", reply)
	}
	if reply.ConflictTerm != nil {
		t.Errorf("conflictTerm = %d, want null when the log is too short", *reply.ConflictTerm)
	}
	if reply.ConflictIndex != 4 {
		t.Errorf("conflictIndex = %d, want lastIndex+1 = 4", reply.ConflictIndex)
	}
	if got := n.log.length(); got != 3 {
		t.Errorf("log length = %d, want a denied request to leave the log alone", got)
	}
}

// 回退分支二：prevLogTerm 对不上，回该 term 的第一条 index（D20）。
func TestAppendConflictPointsAtFirstEntryOfTerm(t *testing.T) {
	core, _ := observer.New(zapcore.InfoLevel)
	c := newCluster(t, 1, zap.New(core))
	n := c.reseed("node-1", State{CurrentTerm: 3},
		[]seedEntry{{term: 2, index: 1}, {term: 2, index: 2}, {term: 3, index: 3}})

	reply := n.handleAppendEntries(AppendRequest{
		Term: 3, LeaderID: "node-1", PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 3, Index: 4, Command: []byte("a")}},
	})
	if reply.Success {
		t.Fatalf("reply = %+v, want denial: index 3 carries term 3", reply)
	}
	if reply.ConflictTerm == nil || *reply.ConflictTerm != 3 {
		t.Errorf("conflictTerm = %v, want 3", reply.ConflictTerm)
	}
	if reply.ConflictIndex != 3 {
		t.Errorf("conflictIndex = %d, want the first entry of term 3", reply.ConflictIndex)
	}
}

// 第 3 步：term 相等也算有效 Leader 联系，选举截止重掷（3.1 §7）。
func TestAppendSameTermResetsElectionDeadline(t *testing.T) {
	_, n, _ := followerFixture(t)
	n.electionDeadline = 1

	n.handleAppendEntries(AppendRequest{Term: 2, LeaderID: "node-1", Entries: []LogEntry{}})

	if got := n.electionDeadline; got < n.electionTicks || got >= 2*n.electionTicks {
		t.Errorf("deadline = %d, want the re-roll inside [%d,%d)", got, n.electionTicks, 2*n.electionTicks)
	}
}

// 空条目数组的心跳只确认位置、不删日志：截后缀只发生在带条目的那一次（3.1 §7 第 5 步）。
// 少了这条规则，一个 nextIndex=1 的新 Leader 发一条心跳就会把 Follower 上自己还没复制到的条目全清掉。
func TestAppendHeartbeatWithEmptyEntriesKeepsLocalLog(t *testing.T) {
	_, n, _ := followerFixture(t)

	reply := n.handleAppendEntries(AppendRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: 1, PrevLogTerm: 2, Entries: []LogEntry{},
	})
	if !reply.Success {
		t.Fatalf("reply = %+v, want the heartbeat accepted", reply)
	}
	if got := n.log.length(); got != 3 {
		t.Fatalf("log length = %d, want indexes 2 and 3 kept by an empty-entries request", got)
	}
	for index, want := range map[uint64]uint64{2: 2, 3: 2} {
		if e, ok := n.log.entryAt(index); !ok || e.term != want {
			t.Errorf("index %d = %+v ok=%v, want it still there at term %d", index, e, ok, want)
		}
	}
}

// 第 6 步：commitIndex = min(leaderCommit, 本地最后一条 index)，多给的那截不认（3.1 §7）。
func TestAppendCapsLeaderCommitAtLocalLog(t *testing.T) {
	_, n, _ := followerFixture(t)

	reply := n.handleAppendEntries(AppendRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: 0, LeaderCommit: 10, Entries: []LogEntry{},
	})
	if !reply.Success {
		t.Fatalf("reply = %+v, want success", reply)
	}
	if got := n.Stats(); got.CommitIndex != 3 || got.LastApplied != 3 {
		t.Errorf("stats = %+v, want both cursors capped at the local lastIndex 3", got)
	}
}

// appendOnce 组包：prevLogIndex／prevLogTerm 取 nextIndex 的前一条，没有待复制条目时带空数组，
// 这一条就是心跳——复制与心跳共用这一个函数（3.1 §7）。
func TestAppendOnceBuildsRequestFromNextIndex(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	n.appendOnce("node-3")
	req := *n.outbox[0].app
	peerID := n.outbox[0].peerID
	mustAppend(t, n.log, n.term, 3, "a")
	n.appendOnce("node-3")
	withEntry := *n.outbox[1].app
	n.mu.Unlock()

	if peerID != "node-3" {
		t.Errorf("queued for %q, want node-3", peerID)
	}
	if req.Term != 3 || req.LeaderID != "node-1" || req.PrevLogIndex != 2 || req.PrevLogTerm != 3 {
		t.Errorf("heartbeat = %+v, want term 3 from node-1 with prev at the no-op 3/2", req)
	}
	if len(req.Entries) != 0 {
		t.Errorf("heartbeat entries = %d, want an empty array: nothing is pending for node-3", len(req.Entries))
	}
	if len(withEntry.Entries) != 1 {
		t.Fatalf("entries = %+v, want the one pending entry", withEntry.Entries)
	}
	if e := withEntry.Entries[0]; e.Index != 3 || e.Term != 3 || string(e.Command) != "a" {
		t.Errorf("entry = %+v, want 3/3 command a", e)
	}
	if withEntry.PrevLogIndex != 2 || withEntry.PrevLogTerm != 3 {
		t.Errorf("prev = %d/%d, want 2/3", withEntry.PrevLogIndex, withEntry.PrevLogTerm)
	}
}

// 一轮带出去的条目不超过 64 条：单条 entry 有 4MB 上限，靠条数把一批的字节量卡住（3.1 §7）。
func TestAppendOnceCarriesAtMost64Entries(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	for range maxAppendEntries {
		mustAppend(t, n.log, n.term, n.log.lastIndex()+1, "")
	}
	n.appendOnce("node-2")
	req := *n.outbox[0].app
	n.mu.Unlock()

	if got := len(req.Entries); got != maxAppendEntries {
		t.Fatalf("entries = %d, want the batch capped at %d", got, maxAppendEntries)
	}
	if first, last := req.Entries[0], req.Entries[maxAppendEntries-1]; first.Index != 3 || last.Index != 66 {
		t.Errorf("batch = %d..%d, want indexes 3 through 66", first.Index, last.Index)
	}
	if req.PrevLogIndex != 2 {
		t.Errorf("prevLogIndex = %d, want 2: the batch starts at nextIndex", req.PrevLogIndex)
	}
}

// 一轮心跳按成员 id 升序发给其余节点，一条一个来回（D23）。
func TestBroadcastAppendsFollowsPeerOrder(t *testing.T) {
	c, n, _ := leaderFixture(t)
	before := len(c.calls("node-1"))

	tickTo(t, n, n.heartbeatTicks)

	got := c.sentTo("node-1")[before:]
	want := []string{"node-2", "node-3", "node-4", "node-5"}
	for i, id := range want {
		if got[i] != id {
			t.Fatalf("send order = %v, want the peer ids ascending", got)
		}
	}
}

// success 回执把两个游标推到请求覆盖的位置之后，够多数派就当场提交并 apply（3.1 §7 第 7 步）。
func TestAppendSuccessAdvancesCursorsAndCommits(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	mustAppend(t, n.log, n.term, 3, "")
	req := AppendRequest{Term: 3, LeaderID: "node-1", PrevLogIndex: 2, PrevLogTerm: 3,
		Entries: []LogEntry{{Term: 3, Index: 3}}}
	for _, peerID := range []string{"node-2", "node-3"} {
		n.handleAppendReply(peerID, req, AppendReply{Term: 3, Success: true})
	}
	stats := Stats{CommitIndex: n.commitIndex, LastApplied: n.lastApplied}
	match, next := n.matchIndex["node-2"], n.nextIndex["node-2"]
	pending := len(n.outbox)
	n.mu.Unlock()

	if match != 3 || next != 4 {
		t.Errorf("matchIndex／nextIndex = %d／%d, want 3／4 after index 3 is acknowledged", match, next)
	}
	if stats.CommitIndex != 3 || stats.LastApplied != 3 {
		t.Errorf("commitIndex／lastApplied = %d／%d, want 3／3: self plus two peers is a majority",
			stats.CommitIndex, stats.LastApplied)
	}
	if pending != 0 {
		t.Errorf("outbox = %d, want a success reply to schedule no resend", pending)
	}
}

// D20 的三个分支各一次 RPC 就把 nextIndex 落到正确位置；第四格是收敛兜底：
// 提示值不比原 nextIndex 更小时退到 nextIndex-1，回退因此严格递减（3.1 §7、D20）。
func TestAppendConflictFallbackLocationsNextIndex(t *testing.T) {
	cases := []struct {
		name    string
		current uint64
		reply   AppendReply
		want    uint64
		resends bool
	}{{
		// 冲突 term 在本地日志里：落在它最后一条之后，提示的 conflictIndex 不作数。
		name:    "conflict term exists locally",
		current: 5,
		reply:   AppendReply{Term: 3, Success: false, ConflictTerm: termPtr(2), ConflictIndex: 9},
		want:    2,
		resends: true,
	}, {
		// 冲突 term 不在本地日志里：照 Follower 给的 conflictIndex 走。
		name:    "conflict term absent locally",
		current: 5,
		reply:   AppendReply{Term: 3, Success: false, ConflictTerm: termPtr(9), ConflictIndex: 3},
		want:    3,
		resends: true,
	}, {
		// Follower 日志太短：conflictTerm 是 null，conflictIndex 就是它的长度+1。
		name:    "peer log is shorter",
		current: 5,
		reply:   AppendReply{Term: 3, Success: false, ConflictIndex: 1},
		want:    1,
		resends: true,
	}, {
		// 提示值不比原 nextIndex 小：退一步，重发因此严格递减。
		name:    "hint not smaller than nextIndex",
		current: 5,
		reply:   AppendReply{Term: 3, Success: false, ConflictTerm: termPtr(9), ConflictIndex: 5},
		want:    4,
		resends: true,
	}, {
		// 已经退到日志头：没有余地，保留 nextIndex、不重发，留给下一次心跳。
		name:    "nextIndex already at the log head",
		current: 1,
		reply:   AppendReply{Term: 3, Success: false, ConflictIndex: 1},
		want:    1,
		resends: false,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, n, _ := leaderFixture(t)

			n.mu.Lock()
			// 把游标抬到 5，四个分支才落在四个不同的位置上。
			n.nextIndex["node-2"] = tc.current
			beforeMatch := n.matchIndex["node-2"]
			req := AppendRequest{Term: 3, LeaderID: "node-1", PrevLogIndex: tc.current - 1, PrevLogTerm: 3}
			n.handleAppendReply("node-2", req, tc.reply)
			got := n.nextIndex["node-2"]
			afterMatch := n.matchIndex["node-2"]
			resend := len(n.outbox)
			var next AppendRequest
			if resend > 0 {
				next = *n.outbox[0].app
			}
			n.mu.Unlock()

			if got != tc.want {
				t.Fatalf("nextIndex = %d, want %d", got, tc.want)
			}
			if afterMatch != beforeMatch {
				t.Errorf("matchIndex = %d, want it untouched by a denial (was %d)", afterMatch, beforeMatch)
			}
			if (resend > 0) != tc.resends {
				t.Fatalf("resends = %d, want one: %v", resend, tc.resends)
			}
			if tc.resends && next.PrevLogIndex != tc.want-1 {
				t.Errorf("resend prevLogIndex = %d, want %d: a single round trip lands on the hint",
					next.PrevLogIndex, tc.want-1)
			}
		})
	}
}

// 旧 term 的响应、上一任期发出的请求都被忽略：为 index 3 拿到的两次假回执一条都不算数（3.2 §3 append 行）。
func TestAppendIgnoresStaleReplies(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	mustAppend(t, n.log, n.term, 3, "")
	req := AppendRequest{Term: 3, LeaderID: "node-1", PrevLogIndex: 2, PrevLogTerm: 3,
		Entries: []LogEntry{{Term: 3, Index: 3}}}
	n.handleAppendReply("node-2", req, AppendReply{Term: 2, Success: true})
	n.handleAppendReply("node-3", AppendRequest{Term: 2, LeaderID: "node-1", PrevLogIndex: 2},
		AppendReply{Term: 3, Success: true})
	got := Stats{CommitIndex: n.commitIndex, LastApplied: n.lastApplied, Role: n.role}
	match, next := n.matchIndex["node-2"], n.nextIndex["node-2"]
	n.mu.Unlock()

	if match != 2 || next != 3 {
		t.Errorf("matchIndex／nextIndex = %d／%d, want 2／3: neither stale reply counts", match, next)
	}
	if got.CommitIndex != 2 || got.LastApplied != 2 || got.Role != RoleLeader {
		t.Errorf("stats = %+v, want nothing committed and the role kept", got)
	}
}

// 回执带更高 term：立即降级，这条请求作废（不变式 5）。
func TestAppendReplyWithHigherTermStepsDown(t *testing.T) {
	_, n, _ := leaderFixture(t)

	n.mu.Lock()
	n.handleAppendReply("node-2", AppendRequest{Term: 3, PrevLogIndex: 2}, AppendReply{Term: 7})
	n.mu.Unlock()

	if got := n.Stats(); got.Role != RoleFollower || got.Term != 7 {
		t.Fatalf("stats = %+v, want follower at term 7", got)
	}
}

// append denied 的字段口径（3.1 §10）：conflictTerm 为 null 时这个字段整个不出现。
func TestAppendDeniedLogFields(t *testing.T) {
	_, n, logs := leaderFixture(t)
	// 夹具那一轮选举里，空日志的节点先拒过一次，读的是这之后的两条。
	before := countLogs(logs, "append denied")

	n.mu.Lock()
	n.handleAppendReply("node-2", AppendRequest{Term: 3, LeaderID: "node-1", PrevLogIndex: 2, PrevLogTerm: 3},
		AppendReply{Term: 3, Success: false, ConflictTerm: termPtr(2), ConflictIndex: 1})
	n.handleAppendReply("node-3", AppendRequest{Term: 3, LeaderID: "node-1", PrevLogIndex: 4, PrevLogTerm: 3},
		AppendReply{Term: 3, Success: false, ConflictIndex: 1})
	n.mu.Unlock()

	denied := logs.FilterMessage("append denied").All()
	if len(denied) != before+2 {
		t.Fatalf("append denied entries = %d, want 2 more than the fixture left: %+v", len(denied), denied[before:])
	}
	withTerm := denied[before].ContextMap()
	if withTerm["peer_id"] != "node-2" || withTerm["prev_log_index"] != uint64(2) ||
		withTerm["conflict_term"] != uint64(2) || withTerm["conflict_index"] != uint64(1) ||
		withTerm["next_index"] != uint64(2) {
		t.Errorf("fields = %v, want peer_id node-2, prev 2, conflict 2/1, nextIndex 2", withTerm)
	}
	withoutTerm := denied[before+1].ContextMap()
	if _, ok := withoutTerm["conflict_term"]; ok {
		t.Errorf("fields = %v, want conflict_term absent for a too-short peer log", withoutTerm)
	}
	if withoutTerm["next_index"] != uint64(1) {
		t.Errorf("next_index = %v, want 1", withoutTerm["next_index"])
	}
}
