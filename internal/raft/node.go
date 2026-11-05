package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// Role 只有三个取值：没有 Pre-Vote，也没有 leadership transfer（3.1 §1）。
// 字符串形态 follower／candidate／leader 用在日志与错误里。
type Role int8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "follower"
	}
}

// Stats 是测试与注入脚本的读数窗口，字段口径见 3.1 §4；
// 租约剩余与确认轮次两项归 doc 4（4.1 §4）。
type Stats struct {
	Term, CommitIndex, LastApplied, LastLogIndex, Votes uint64
	Role                                                Role
	LeaseRemainingMS                                    int64
	ReadRound                                           uint64
}

// 两个 RPC 的请求与响应。字段名与 doc 3 的 JSON 一一对应，映射写在 rpc.go（M12）。
type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

type VoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

type AppendRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leaderCommit"`
	// ReadRound 是这一轮确认的编号，0 表示这一条不为任何读做确认（4.1 §4、D25）。
	// Follower 不解释它，只把请求里带来的编号原样回显。
	ReadRound uint64 `json:"readRound"`
}

// ConflictTerm 是指针：Follower 日志太短那一格要回 "conflictTerm": null（3.1 §4）。
type AppendReply struct {
	Term          uint64  `json:"term"`
	Success       bool    `json:"success"`
	ConflictTerm  *uint64 `json:"conflictTerm"`
	ConflictIndex uint64  `json:"conflictIndex"`
	// ReadRound 原样带回发起时的编号（4.1 §4、D25）。
	ReadRound uint64 `json:"readRound"`
}

// Transport 是节点间的发送通道。httpTransport（M12）与测试的内存实现调的是同一对处理函数。
type Transport interface {
	RequestVote(to string, req VoteRequest) (VoteReply, error)
	AppendEntries(to string, req AppendRequest) (AppendReply, error)
}

// rpcTask 是 outbox 里的一次发送。调用一律在 Node 锁外发起，回执在同一调用栈里当场处理（D23）。
// app 是任意一次 AppendEntries：带条目就是复制，空条目数组就是心跳。
type rpcTask struct {
	peerID string
	vote   *VoteRequest
	app    *AppendRequest
}

// Node 是一个 raft 节点，一个进程一个（D17）。
// 一把互斥锁护住四个标量、日志视图、投票计数、在途提案表与读路径的三张表（3.1 §4、4.1 §9）。
type Node struct {
	cfg Config
	kv  *kvstore.KV
	tp  Transport
	lg  *zap.Logger

	mu    sync.Mutex
	state *stateFile
	log   *raftLog

	role        Role
	term        uint64
	votedFor    string
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// 两个周期都是 tick 数，换算只在 NewNode 做一次；不读墙上时间（D16）。
	electionTicks    int
	heartbeatTicks   int
	electionDeadline int
	heartbeatElapsed int
	votes            int

	// 复制游标：初值在当选时置好，成功回执推进它、冲突提示回退它（append.go）。
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// proposals 是 index→完成通知；登记与兑现都是包内未导出函数（T22.4）。
	proposals map[uint64]chan error

	// 读路径的状态：租约绑定任期与截止时刻，在途确认轮至多一个，
	// 读队列与 apply 等待表挂在这把锁上。全部只活在内存里，一个字节都不落盘（4.1 §5、基线共同不变量 4）。
	leaseTerm    uint64
	leaseUntilNS int64
	leaseWindow  int64
	readRoundSeq uint64
	readArrivals uint64
	readQueue    readQueue
	activeRound  *activeRound
	applyWaiters applyWaiters

	// now 是租约唯一的取时入口：真实实现是单调纳秒，测试夹具换成一根可被用例拨动的计数器。
	// 除 read.go 之外一律不许读表，选举与心跳只看 tick 计数（D16、D26）。
	now func() int64

	outbox  []rpcTask
	started bool
	closed  bool
}

// NewNode 读回 state 与 raft 日志，构造完即完成恢复，未 Start 也能读状态（3.1 §4）。
// 角色一律从 Follower 起，重启不保留旧角色（3.1 §7）。
func NewNode(cfg Config, kv *kvstore.KV, tp Transport, lg *zap.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.ensureDirs(); err != nil {
		return nil, err
	}

	st, err := newStateFile(cfg, lg).load()
	if err != nil {
		return nil, err
	}
	rl, err := openRaftLog(cfg, lg)
	if err != nil {
		return nil, err
	}
	if err := checkCursors(st, rl.lastIndex()); err != nil {
		_ = rl.close()
		return nil, err
	}

	n := &Node{
		cfg:            cfg,
		kv:             kv,
		tp:             tp,
		lg:             kvstore.WithComponent(lg, ComponentRaft),
		state:          newStateFile(cfg, lg),
		log:            rl,
		role:           RoleFollower,
		term:           st.CurrentTerm,
		votedFor:       st.VotedFor,
		commitIndex:    st.CommitIndex,
		lastApplied:    st.LastApplied,
		electionTicks:  ticks(cfg.ElectionTimeoutMS, cfg.TickMS),
		heartbeatTicks: ticks(cfg.HeartbeatMS, cfg.TickMS),
		proposals:      map[uint64]chan error{},
		applyWaiters:   applyWaiters{},
		now:            monotonicNow,
		leaseWindow:    leaseWindow(cfg),
	}
	n.electionDeadline = n.rollElectionDeadline()
	// 启动序列的第一步：map 由日志回放重建，KV WAL 不参与恢复（docs/8 第 1 步）。
	if err := n.replayLog(); err != nil {
		_ = n.log.close()
		return nil, err
	}
	// 启动序列的最后一步：把已提交但没进状态机的条目补 apply（3.1 §7）。
	if err := n.applyCommitted(); err != nil {
		_ = n.log.close()
		return nil, err
	}
	return n, nil
}

// replayLog 把 [1, lastApplied] 的条目重放进状态机。日志是唯一恢复源之后 map 由这里建起来，
// (lastApplied, commitIndex] 交给 applyCommitted 补。
// 游标指向的条目解不出 KV 操作就在启动期失败，不静默跳过（docs/8 §六）。
func (n *Node) replayLog() error {
	replayed := 0
	for _, e := range n.log.entries {
		if e.index > n.lastApplied {
			break
		}
		if len(e.cmd) == 0 {
			continue
		}
		key, value, err := kvstore.DecodeEntry(e.cmd, e.offset)
		if err != nil {
			return err
		}
		n.kv.Apply(key, value)
		replayed++
	}
	n.lg.Info("state rebuilt from raft log",
		zap.String("node_id", n.cfg.NodeID),
		zap.Int("replayed_entries", replayed),
		zap.Uint64("last_applied", n.lastApplied))
	return nil
}

// checkCursors 挡的是 state 与 raft 日志对不上的那一格（不变式 4 的下半段）：
// 游标比日志还长，说明两个文件里至少有一个不是同一次提交的产物。
func checkCursors(st State, lastLogIndex uint64) error {
	if st.CommitIndex > lastLogIndex {
		return ErrStateMismatchAt(st.CommitIndex, "commitIndex is past the end of the raft log")
	}
	if st.LastApplied > st.CommitIndex {
		return ErrStateMismatchAt(st.LastApplied, "lastApplied is past commitIndex")
	}
	return nil
}

// ticks 把毫秒换算成 tick 数，向下取整、至少 1（3.1 §6）。
func ticks(periodMS, tickMS int64) int {
	n := int(periodMS / tickMS)
	if n < 1 {
		return 1
	}
	return n
}

// rollElectionDeadline 取 [electionTicks, 2×electionTicks) 的均匀随机值。
// 随机化是防止大家同时发起投票（doc 3 的选举超时一节）。
func (n *Node) rollElectionDeadline() int {
	return n.electionTicks + rand.Intn(n.electionTicks)
}

// Start 只置运行标志：心跳与选举都由外层调 Tick() 驱动，Node 内部不起定时器（D16、D23）。
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return kvstore.ErrIOFrom("start raft node", 0, errors.New("raft node is closed"))
	}
	if n.started {
		return errors.New("raft node already started")
	}
	n.started = true
	return nil
}

// Tick 一次只做一件事：Leader 推进心跳周期，其余角色推进选举截止（3.1 §4）。
// 到点之后要发的 RPC 先进 outbox，再由 drainOutbox 在锁外串行跑完，
// 处理回执时若又产生新的发送（冲突重发、当选后的第一轮复制）照样排在后面，不跨出本次调用。
// 末尾补一次 apply：回执里推进的 commitIndex 要在这里进状态机（3.1 §7）。
func (n *Node) Tick() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.tickLocked()
	n.mu.Unlock()

	n.drainOutbox()

	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.applyCommitted(); err != nil {
		n.logApplyFailure(err)
	}
}

// drainOutbox 串行跑完 outbox 里的每一条：发送与等回执都不持锁（D23、3.1 §4）。
func (n *Node) drainOutbox() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for len(n.outbox) > 0 {
		task := n.outbox[0]
		n.outbox = n.outbox[1:]
		n.mu.Unlock()
		n.runTask(task)
		n.mu.Lock()
	}
}

func (n *Node) tickLocked() {
	if n.role == RoleLeader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed < n.heartbeatTicks {
			return
		}
		n.heartbeatElapsed = 0
		n.broadcastAppends()
		// Leader 分支是确认轮的第三个触发点：队列里有读而没有在途轮次就补发一轮（4.1 §7）。
		n.scheduleReadRound()
		return
	}
	n.electionDeadline--
	if n.electionDeadline > 0 {
		return
	}
	n.becomeCandidate()
}

// runTask 发一条 RPC 并当场处理回执；发送与等待回执时都没有持锁。
func (n *Node) runTask(task rpcTask) {
	addr := n.cfg.Addr(task.peerID)
	switch {
	case task.vote != nil:
		req := *task.vote
		n.lg.Info("request vote sent",
			zap.String("peer_id", task.peerID),
			zap.Uint64("term", req.Term),
			zap.String("candidate_id", req.CandidateID),
			zap.Uint64("last_log_index", req.LastLogIndex),
			zap.Uint64("last_log_term", req.LastLogTerm))
		reply, err := n.tp.RequestVote(addr, req)
		if err != nil {
			n.logSendFailed("request vote failed", task.peerID, req.Term, err)
			return
		}
		n.mu.Lock()
		n.handleVoteReply(req, reply)
		n.mu.Unlock()

	default:
		req := *task.app
		reply, err := n.tp.AppendEntries(addr, req)
		if err != nil {
			n.logSendFailed("append failed", task.peerID, req.Term, err)
			return
		}
		n.mu.Lock()
		n.handleAppendReply(task.peerID, req, reply)
		n.mu.Unlock()
	}
}

func (n *Node) logSendFailed(msg, peerID string, term uint64, err error) {
	n.lg.Warn(msg,
		zap.String("peer_id", peerID),
		zap.Uint64("term", term),
		zap.Error(err))
}

// transition 是三个转移函数共同的落点：新的四个标量先写 state 文件并 Sync，
// 成功了才改内存，然后才打 term changed 与 role changed（不变式 5）。
// 落盘失败时内存一个字都不动，调用方也就停在旧角色上。
func (n *Node) transition(role Role, term uint64, votedFor, reason string) error {
	next := State{
		CurrentTerm: term,
		VotedFor:    votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
	if err := n.state.store(next); err != nil {
		n.lg.Error("raft state transition not persisted",
			zap.String("node_id", n.cfg.NodeID),
			zap.String("reason", reason),
			zap.Error(err))
		return err
	}
	if term != n.term {
		n.lg.Info("term changed",
			zap.String("node_id", n.cfg.NodeID),
			zap.Uint64("old_term", n.term),
			zap.Uint64("new_term", term),
			zap.String("reason", reason))
	}
	if role != n.role {
		n.lg.Info("role changed",
			zap.String("node_id", n.cfg.NodeID),
			zap.String("old_role", n.role.String()),
			zap.String("new_role", role.String()),
			zap.Uint64("term", term))
	}
	// 任期或角色一变，租约与在途轮次当场作废，没返回的读跟着走（4.1 §7 租约失效）。
	if term != n.term {
		n.invalidateLease("higher_term", ErrNotLeader)
	} else if role != RoleLeader {
		n.invalidateLease("stepped_down", ErrNotLeader)
	}
	n.role, n.term, n.votedFor = role, term, votedFor
	return nil
}

// becomeCandidate 递增 term、把票投给自己、重置选举截止，然后给其余成员各排一条投票。
// 自票计入多数派（3.1 §7）；成员表只有自己时这一票当场就是多数派，选举在发起的那一拍结束，
// 不必等一条永远不会回来的回执（docs/8 §二 的单节点集群）。
func (n *Node) becomeCandidate() {
	if err := n.transition(RoleCandidate, n.term+1, n.cfg.NodeID, "election timeout"); err != nil {
		return
	}
	n.electionDeadline = n.rollElectionDeadline()
	n.votes = 1
	n.leaderID = ""

	req := VoteRequest{
		Term:         n.term,
		CandidateID:  n.cfg.NodeID,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	for _, peerID := range n.cfg.otherPeers() {
		n.outbox = append(n.outbox, rpcTask{peerID: peerID, vote: &req})
	}
	if n.votes >= n.cfg.Majority() {
		n.becomeLeader("self-vote is a majority")
	}
}

// becomeFollower 接受一个新 term（与当前相同就沿用），清空 votedFor、停止计票，
// 并把在途提案一律回 ErrNotLeader（T22.4）。state 没落盘时什么都不改，把错误交回调用方。
func (n *Node) becomeFollower(term uint64, reason string) error {
	if err := n.transition(RoleFollower, term, "", reason); err != nil {
		return err
	}
	n.votes = 0
	n.leaderID = ""
	n.electionDeadline = n.rollElectionDeadline()
	n.cancelProposals(ErrNotLeader)
	return nil
}

// becomeLeader 初始化两个复制游标，追加一条 no-op，并立刻按成员 id 升序排出一轮复制请求
// ——这一轮就把 no-op 带出去（3.1 §7）。
// no-op 的 command 是空字节串，apply 时跳过 KV，只用来尽快推进 commitIndex。
func (n *Node) becomeLeader(reason string) {
	if err := n.transition(RoleLeader, n.term, n.votedFor, reason); err != nil {
		return
	}
	n.leaderID = n.cfg.NodeID
	n.heartbeatElapsed = 0
	n.votes = 0

	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	first := n.log.lastIndex() + 1
	for _, peerID := range n.cfg.peerIDs() {
		n.nextIndex[peerID] = first
		n.matchIndex[peerID] = 0
	}

	if _, err := n.log.appendEntry(n.term, n.log.lastIndex()+1, nil); err != nil {
		n.lg.Error("no-op entry not appended",
			zap.String("node_id", n.cfg.NodeID), zap.Uint64("term", n.term), zap.Error(err))
		return
	}
	if err := n.log.sync(); err != nil {
		n.lg.Error("no-op entry not persisted",
			zap.String("node_id", n.cfg.NodeID), zap.Uint64("term", n.term), zap.Error(err))
		return
	}
	// 单成员表的多数派是 1，这条 no-op 当场就够数；多成员仍等回执（3.1 §4）。
	n.advanceCommit()
	n.broadcastAppends()
}

// Role 是当前角色；未 Start 也能读（3.1 §4）。
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Leader 给出已知 Leader 的 id 与监听地址；没听说过就返回 ok=false。
func (n *Node) Leader() (id string, addr string, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return "", "", false
	}
	return n.leaderID, n.cfg.Addr(n.leaderID), true
}

// writeDisabled 报告 raft 日志的写路径是否已被一次 Sync 失败关掉（D5）。
// /put 用它决定要不要通知进程层收尾：读路径此时还在服务，只有写死了才该退出。
func (n *Node) writeDisabled() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.disabled()
}

// Stats 是八个读数的一次快照（3.1 §4、4.1 §4）。
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	var round uint64
	if n.activeRound != nil {
		round = n.activeRound.round
	}
	return Stats{
		Term:             n.term,
		CommitIndex:      n.commitIndex,
		LastApplied:      n.lastApplied,
		LastLogIndex:     n.log.lastIndex(),
		Votes:            uint64(n.votes),
		Role:             n.role,
		LeaseRemainingMS: n.leaseRemainingLocked(),
		ReadRound:        round,
	}
}

// Propose 把一次客户端写放进 raft 日志并等它生效：
// 追加 entry、按 -raft-fsync 决定是否 Sync、登记回调、按成员 id 升序各发一次 appendOnce，
// 回执处理里推进 commitIndex，apply 兑现到这条 index 才返回（3.1 §4、§7，D19）。
// 两个返回值就是这条 entry 的位置与任期，`/put` 响应体读的是它们；出错时回 0。
func (n *Node) Propose(ctx context.Context, key, value string) (uint64, uint64, error) {
	command, err := kvstore.EncodeEntry(key, value)
	if err != nil {
		return 0, 0, err
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return 0, 0, ErrCanceled
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	index, term := n.log.lastIndex()+1, n.term
	if _, err := n.log.appendEntry(term, index, command); err != nil {
		n.mu.Unlock()
		return 0, 0, err
	}
	if err := n.log.sync(); err != nil {
		// Sync 失败时盘上边界未知，写路径由 raftLog 自己关掉（D5），这条不登记回调。
		n.mu.Unlock()
		return 0, 0, err
	}
	wait := n.registerProposal(index)
	// 自己这一票恒计入 replicaCount，成员表只有自己时多数派已经凑够，
	// 当场推进提交点；多成员时这一句什么也不做，仍等回执。
	n.advanceCommit()
	n.broadcastAppends()
	n.mu.Unlock()

	n.drainOutbox()

	n.mu.Lock()
	err = n.applyCommitted()
	n.mu.Unlock()
	if err != nil {
		n.rejectProposal(index, err)
	}

	select {
	case err := <-wait:
		if err != nil {
			return 0, 0, err
		}
		return index, term, nil
	case <-ctx.Done():
		// 取消只撤自己这一格：条目已经在日志里，继续复制、照常提交（3.1 §4）。
		n.rejectProposal(index, ErrCanceled)
		return 0, 0, ErrCanceled
	}
}

// Get 是本地档读：不确认领导权，可能读到旧值，等价于 `/get?consistency=local`。
// 客户端读路径走 Read（4.1 §4），默认档是线性一致读。
func (n *Node) Get(key string) (string, error) { return n.kv.Get(key) }

func (n *Node) currentState() State {
	return State{
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}

// registerProposal 给一个日志 index 挂上完成通知。缓冲为 1，
// 让兑现方在没有读者时也不会阻塞——ctx 取消之后没人读就是这种场景。
func (n *Node) registerProposal(index uint64) chan error {
	ch := make(chan error, 1)
	n.proposals[index] = ch
	return ch
}

// rejectProposal 作废一格在途登记：apply 出错或 ctx 取消时给它一个确定的错误，
// 兑现与作废都只发生一次，Propose 因此不会在已经判负的 index 上永久等待。
func (n *Node) rejectProposal(index uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.proposals[index]; ok {
		ch <- err
		delete(n.proposals, index)
	}
}

// cancelProposals 把未兑现的在途提案全部回一个错误，转成 Follower 与 Close 都走这里。
func (n *Node) cancelProposals(err error) {
	for index, ch := range n.proposals {
		ch <- err
		delete(n.proposals, index)
	}
}

// Close 依次关掉在途提案、作废租约、落 state、关日志（3.1 §4）。
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	n.outbox = nil
	n.cancelProposals(ErrCanceled)
	n.invalidateLease("closed", ErrCanceled)

	var err error
	if storeErr := n.state.store(n.currentState()); storeErr != nil {
		err = errors.Join(err, storeErr)
	}
	if logErr := n.log.close(); logErr != nil {
		err = errors.Join(err, logErr)
	}
	return err
}
