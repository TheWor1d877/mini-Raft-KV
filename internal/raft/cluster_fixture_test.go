package raft

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// errFixtureDown 是分区开关让调用直接返回的错误：M11 与 M13 的分区用例靠它把一票变成一次失败（3.2 §3 夹具行）。
var errFixtureDown = errors.New("fixture: peer is partitioned away")

// errFixtureGone 是「目标节点已经收尾」：真机上是连不上，夹具里同样只回一个错误，
// 这一票计为一次失败（D23）。杀掉 Leader 的用例走的就是这条路。
var errFixtureGone = errors.New("fixture: peer process is gone")

// sentRPC 是夹具记下的一次发送。串行 RPC 让「第几条」有确定含义，
// 注入按这个序号取回执，顺序断言按 from 的发出次序读（D23）。
type sentRPC struct {
	from string
	to   string
	kind string // vote 或 append
	vote VoteRequest
	app  AppendRequest
}

// appendReplySpec 是一条预置的 AppendEntries 回执。
// followRound 为真时，回执的 readRound 取请求里带的编号：确认轮的编号用例不必先知道，
// 就能按轮次计票（4.1 §7、4.2 §3 的夹具行）。
type appendReplySpec struct {
	reply       AppendReply
	followRound bool
}

// fakeClock 是读路径的假时钟：Node.now 只从这里取（4.1 §7、D26）。
// 用例拨它，串行 RPC 的耗时也落在这根线上。
type fakeClock struct{ ns int64 }

func (c *fakeClock) now() int64 { return c.ns }

func (c *fakeClock) advance(d time.Duration) { c.ns += int64(d) }

// stubTransport 是一个节点的发送通道：调用记进集群，先看注入的回执，
// 再看分区开关，都没有就落到对端节点的同一对处理函数上（3.1 §4）。
type stubTransport struct {
	c  *cluster
	id string
}

func (s *stubTransport) RequestVote(to string, req VoteRequest) (VoteReply, error) {
	s.c.sent = append(s.c.sent, sentRPC{from: s.id, to: s.c.idOf(to), kind: "vote", vote: req})
	if replies := s.c.voteQueue[s.id]; len(replies) > 0 {
		s.c.voteQueue[s.id] = replies[1:]
		return replies[0], nil
	}
	if s.c.blocked[s.id][to] {
		return VoteReply{}, errFixtureDown
	}
	peer, ok := s.c.running(s.c.idOf(to))
	if !ok {
		return VoteReply{}, errFixtureGone
	}
	return peer.handleRequestVote(req), nil
}

func (s *stubTransport) AppendEntries(to string, req AppendRequest) (AppendReply, error) {
	s.c.sent = append(s.c.sent, sentRPC{from: s.id, to: s.c.idOf(to), kind: "append", app: req})
	s.c.advanceClock(s.id)
	if specs := s.c.appendQueue[s.id]; len(specs) > 0 {
		spec := specs[0]
		s.c.appendQueue[s.id] = specs[1:]
		reply := spec.reply
		if spec.followRound {
			reply.ReadRound = req.ReadRound
		}
		return reply, nil
	}
	if s.c.blocked[s.id][to] {
		return AppendReply{}, errFixtureDown
	}
	peer, ok := s.c.running(s.c.idOf(to))
	if !ok {
		return AppendReply{}, errFixtureGone
	}
	return peer.handleAppendEntries(req), nil
}

// cluster 是 N 个真节点的夹具：每个节点各自一份数据目录、一份 kvstore.KV 和一个 Node，
// 三份配置按 id 存着，重启用的还是同一份数据目录（3.2 §3 夹具行）。
type cluster struct {
	t   *testing.T
	lg  *zap.Logger
	ids []string

	nodes   map[string]*Node
	kvs     map[string]*kvstore.KV
	raftCfg map[string]Config
	kvCfg   map[string]kvstore.Config

	sent      []sentRPC
	voteQueue map[string][]VoteReply
	// appendQueue 与 blocked 从 M11 起用得上，夹具一次建齐（T21.5）。
	appendQueue map[string][]appendReplySpec
	blocked     map[string]map[string]bool
	addrID      map[string]string
	closed      bool

	// clock 是读路径的假时钟：节点们共用一根计数器，用例拨它，
	// 或者让每条为在途确认轮发出的 AppendEntries 前进 clockStep（4.1 §7）。
	clock *fakeClock
	// clockStep 是每条在途确认轮的请求推进的时长；0 表示时钟只由用例拨。
	clockStep time.Duration
}

func newCluster(t *testing.T, size int, lg *zap.Logger) *cluster {
	t.Helper()
	return newClusterCfg(t, size, lg, nil)
}

// newClusterCfg 起 size 个节点，mut 在每台机器的 raft 配置上生效——选举用例要换时钟，
// 而 electionTicks 只在 NewNode 换算一次（3.1 §6）。
func newClusterCfg(t *testing.T, size int, lg *zap.Logger, mut func(*Config)) *cluster {
	t.Helper()
	c := &cluster{
		t: t, lg: lg,
		nodes:   map[string]*Node{},
		kvs:     map[string]*kvstore.KV{},
		raftCfg: map[string]Config{},
		kvCfg:   map[string]kvstore.Config{},

		voteQueue:   map[string][]VoteReply{},
		appendQueue: map[string][]appendReplySpec{},
		blocked:     map[string]map[string]bool{},
		addrID:      map[string]string{},
		clock:       &fakeClock{},
	}
	peers := map[string]string{}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node-%d", i)
		c.ids = append(c.ids, id)
		addr := fmt.Sprintf("127.0.0.1:88%02d", i)
		peers[id] = addr
		c.addrID[addr] = id
	}
	for _, id := range c.ids {
		cfg := validConfig()
		cfg.NodeID = id
		cfg.Peers = peers
		cfg.DataDir = t.TempDir()
		cfg.LogLevel = "debug"
		if mut != nil {
			mut(&cfg)
		}
		c.raftCfg[id] = cfg

		kcfg := kvstore.DefaultConfig()
		kcfg.DataDir = cfg.DataDir
		kcfg.MaxFileSize = cfg.MaxFileSize
		c.kvCfg[id] = kcfg
		c.start(id)
	}
	t.Cleanup(c.close)
	return c
}

// start 按存好的配置开一份 KV 与一个 Node。新建与重启都走这里，所以两者读的是同一批文件。
func (c *cluster) start(id string) {
	t := c.t
	kv, err := kvstore.Open(c.kvCfg[id], c.lg)
	if err != nil {
		t.Fatalf("open kv for %s: %v", id, err)
	}
	c.kvs[id] = kv

	node, err := NewNode(c.raftCfg[id], kv, &stubTransport{c: c, id: id}, c.lg)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	// 租约的取时只经 Node.now，夹具把这一处换成假时钟（4.1 §7、D26）。
	node.now = c.clock.now
	c.nodes[id] = node
}

// restart 关掉节点与 KV 再按同一份数据目录重建：M10 的重启用例走的就是这条路。
func (c *cluster) restart(id string) *Node {
	c.closeNode(id)
	c.start(id)
	return c.nodes[id]
}

// breakLog 关掉这个节点 raft 日志的写路径：等价于一次 Sync 失败之后盘上边界未知的状态（D5）。
// 追加与 Sync 从此一律拒绝，读路径与状态机照常——合并之后失效源只有这一处。
func (c *cluster) breakLog(id string) {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.log.failed = true
}

// recover 按同一份数据目录重建节点，走的就是 load + 补 apply 那条恢复路径（3.1 §7 启动）。
func (c *cluster) recover(id string) *Node {
	c.closeNode(id)
	c.start(id)
	return c.node(id)
}

// closeNode 收尾一个节点，用于「进程退出后重启」的场景；错误一律当夹具的失败。
func (c *cluster) closeNode(id string) {
	if err := c.stopNode(id); err != nil {
		c.t.Fatalf("close node %s: %v", id, err)
	}
}

// stopNode 收尾一个节点并把错误交给调用方：Close 的落 state 那一步就是要看 ErrIO
// （3.1 §4 的 Close 行）。调用过的节点从夹具里摘掉，Cleanup 不再重复收尾。
func (c *cluster) stopNode(id string) error {
	var err error
	if n := c.nodes[id]; n != nil {
		delete(c.nodes, id)
		err = n.Close()
	}
	if kv := c.kvs[id]; kv != nil {
		delete(c.kvs, id)
		if cerr := kv.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func (c *cluster) close() {
	if c.closed {
		return
	}
	c.closed = true
	for _, id := range c.ids {
		c.closeNode(id)
	}
}

func (c *cluster) node(id string) *Node {
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("node %q is not running in this fixture", id)
	}
	return n
}

// running 是 node 的不报错版本：夹具的 transport 用它判断对端还在不在，
// 不在就只回一个错误，等价于真机上连不上。
func (c *cluster) running(id string) (*Node, bool) {
	n, ok := c.nodes[id]
	return n, ok
}

func (c *cluster) kv(id string) *kvstore.KV {
	kv, ok := c.kvs[id]
	if !ok {
		c.t.Fatalf("node %q has no open kv in this fixture", id)
	}
	return kv
}

// injectVotes 预置 from 后续几票的回执，顺序与串行发出的次序一一对应（D23）。
func (c *cluster) injectVotes(from string, replies ...VoteReply) {
	c.voteQueue[from] = append(c.voteQueue[from], replies...)
}

// injectAppends 同上，注入的是 AppendEntries 的回执。
// 选举类用例把心跳回执也注入，回执才不会被对端节点的真实处理污染观察者日志。
func (c *cluster) injectAppends(from string, replies ...AppendReply) {
	for _, reply := range replies {
		c.appendQueue[from] = append(c.appendQueue[from], appendReplySpec{reply: reply})
	}
}

// injectAppendRounds 注入的回执跟着请求里的确认轮编号走：回执的 readRound 取请求的值，
// 用例不用先知道轮次编号就能计票（4.1 §7）。
func (c *cluster) injectAppendRounds(from string, replies ...AppendReply) {
	for _, reply := range replies {
		c.appendQueue[from] = append(c.appendQueue[from], appendReplySpec{reply: reply, followRound: true})
	}
}

// advanceClock 在确认轮在途时把假时钟拨一格：串行 RPC 的耗时落在这根线上，
// 租约窗口就是被它算掉的（4.1 §7）。轮次不在途时不拨，时钟只由用例拨。
func (c *cluster) advanceClock(from string) {
	if c.clockStep <= 0 {
		return
	}
	n := c.nodes[from]
	if n == nil {
		return
	}
	n.mu.Lock()
	inFlight := n.activeRound != nil
	n.mu.Unlock()
	if inFlight {
		c.clock.advance(c.clockStep)
	}
}

// seedEntry 是 reseed 要预先落到盘上的一条 raft 日志条目。
// cmd 留空就是 no-op：M11 之后 apply 会跑起来，播种条目不带 key/value 才不会被解坏。
type seedEntry struct {
	term  uint64
	index uint64
	cmd   string
}

// reseed 先收尾，再按 load 路径要读的样子把 state 与 raft 日志写好，然后重启这个节点。
// term 与 votedFor 走的是真实的恢复路径，不是测试直接改内存字段（不变式 1 的重启那半段）。
func (c *cluster) reseed(id string, st State, entries []seedEntry) *Node {
	c.closeNode(id)

	if err := newStateFile(c.raftCfg[id], zap.NewNop()).store(st); err != nil {
		c.t.Fatalf("seed state for %s: %v", id, err)
	}
	l, err := openWAL(c.raftCfg[id], zap.NewNop())
	if err != nil {
		c.t.Fatalf("open wal for %s: %v", id, err)
	}
	for _, e := range entries {
		mustAppend(c.t, l, e.term, e.index, e.cmd)
	}
	if err := l.sync(); err != nil {
		c.t.Fatalf("sync seeded log for %s: %v", id, err)
	}
	if err := l.close(); err != nil {
		c.t.Fatalf("close seeded log for %s: %v", id, err)
	}

	c.start(id)
	return c.node(id)
}

// breakStateStore 让这个节点的 state 落不了盘：任何一次 store 都会拿到 ErrIO。
// 投票侧的断言是「拿不到 ErrIO 就不回 voteGranted=true」（不变式 2 的后半段）。
func (c *cluster) breakStateStore(id string) {
	c.node(id).state.openTmp = func(string) (*os.File, error) {
		return nil, errors.New("fixture: state cannot be persisted")
	}
}

// block 让 from 对 to 的调用直接返回 errFixtureDown，这一票计为一次失败。
func (c *cluster) block(from, to string) {
	if c.blocked[from] == nil {
		c.blocked[from] = map[string]bool{}
	}
	c.blocked[from][c.raftCfg[from].Addr(to)] = true
}

// partition 让两组之间的调用双向不通、组内照常：M13 的 2/3 分区就是这个形状（3.2 §3 夹具行）。
func (c *cluster) partition(groupA, groupB []string) {
	for _, from := range groupA {
		for _, to := range groupB {
			c.block(from, to)
		}
	}
	for _, from := range groupB {
		for _, to := range groupA {
			c.block(from, to)
		}
	}
}

// heal 清掉所有分区开关：合并之后的断言走这条路。
func (c *cluster) heal() {
	c.blocked = map[string]map[string]bool{}
}

// idOf 把 Transport 收到的监听地址换回成员 id：顺序断言读的是 id（D23）。
func (c *cluster) idOf(addr string) string {
	id, ok := c.addrID[addr]
	if !ok {
		c.t.Fatalf("address %q is not in the peer table", addr)
	}
	return id
}

// calls 是 from 发出的 RPC，按发出次序。
func (c *cluster) calls(from string) []sentRPC {
	var out []sentRPC
	for _, s := range c.sent {
		if s.from == from {
			out = append(out, s)
		}
	}
	return out
}

// sentTo 是 from 依次发给了谁：D23 之后这条序列就是成员 id 的升序。
func (c *cluster) sentTo(from string) []string {
	var out []string
	for _, s := range c.calls(from) {
		out = append(out, s.to)
	}
	return out
}

// 租约的取时只走注入位：墙上时间不进状态机判定（D26、4.1 §3 的禁止项）。
// 这条用一次源码扫描实现：四个文件里出现 time.Now() 或 time.Since 就是命中，
// 注入位 n.now() 也只许 read.go 调。
func TestClockInjectionKeepsWallTimeOutOfTheStateMachine(t *testing.T) {
	for _, name := range []string{"node.go", "append.go", "commit.go", "vote.go"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, banned := range []string{"time.Now()", "time.Since"} {
			if bytes.Contains(data, []byte(banned)) {
				t.Errorf("%s contains %q: the wall clock must not reach the state machine", name, banned)
			}
		}
	}
	for _, name := range []string{"node.go", "append.go", "commit.go", "vote.go", "rpc.go", "state.go"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if bytes.Contains(data, []byte("n.now(")) {
			t.Errorf("%s calls the injected clock: only read.go may", name)
		}
	}
}

// 夹具自己的那条断言：raft/ 的独占由 kvstore.Open 的 LOCK 覆盖，raft 不取第二把锁（3.1 §4 NewNode 行）。
func TestFixtureLockCoversRaftDir(t *testing.T) {
	c := newCluster(t, 1, zap.NewNop())
	cfg := c.raftCfg["node-1"]

	if _, err := os.Stat(cfg.RaftDir()); err != nil {
		t.Fatalf("raft dir under the locked data dir: %v", err)
	}
	if _, err := kvstore.Open(c.kvCfg["node-1"], zap.NewNop()); !errors.Is(err, kvstore.ErrLocked) {
		t.Fatalf("second Open on %s = %v, want ErrLocked", cfg.DataDir, err)
	}
}
