package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// httpCluster 是 M12 的夹具：节点各挂一个真 HTTP 服务器，RPC 走 httpTransport、
// 客户端与 /raft/* 都落在同一个 handler 上（3.1 §4「两个实现调同一批处理函数」）。
// 时钟仍由测试按次数推 Tick，零 sleep（D16）。
type httpCluster struct {
	t    *testing.T
	ids  []string
	node map[string]*Node
	kv   map[string]*kvstore.KV
	srv  map[string]*http.Server
	ln   map[string]net.Listener

	url  map[string]string // 带 scheme 的客户端地址
	addr map[string]string // 成员表里的那种 host:port，leader_addr 比这个

	// disabled 记每台机器的失效回调次数：D5 的「只触发一次」由它判，入口层据此把退出码定在 1。
	disabled map[string]*atomic.Int64
	broken   map[string]bool
}

func newHTTPCluster(t *testing.T, size int, lg *zap.Logger) *httpCluster {
	t.Helper()
	return newHTTPClusterCfg(t, size, lg, nil)
}

// newHTTPClusterCfg 在 newHTTPCluster 之上多一个改配置的钩子：读路径的开关与超时
// 只有这一处能换（4.1 §6）。
func newHTTPClusterCfg(t *testing.T, size int, lg *zap.Logger, mut func(*Config)) *httpCluster {
	t.Helper()
	c := &httpCluster{
		t:    t,
		node: map[string]*Node{}, kv: map[string]*kvstore.KV{},
		srv: map[string]*http.Server{}, ln: map[string]net.Listener{},
		url: map[string]string{}, addr: map[string]string{},
		disabled: map[string]*atomic.Int64{}, broken: map[string]bool{},
	}
	peers := map[string]string{}
	// 监听地址先于 Node 建：成员表要在 NewNode 之前定下来，端口只能靠先占位拿到。
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node-%d", i)
		c.ids = append(c.ids, id)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen for %s: %v", id, err)
		}
		c.ln[id] = ln
		c.addr[id] = ln.Addr().String()
		c.url[id] = "http://" + ln.Addr().String()
		peers[id] = ln.Addr().String()
		c.disabled[id] = &atomic.Int64{}
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

		kcfg := kvstore.DefaultConfig()
		kcfg.DataDir = cfg.DataDir
		kcfg.MaxFileSize = cfg.MaxFileSize
		kcfg.Fsync = cfg.Fsync

		kv, err := kvstore.Open(kcfg, lg)
		if err != nil {
			t.Fatalf("open kv for %s: %v", id, err)
		}
		c.kv[id] = kv

		n, err := NewNode(cfg, kv, NewTransport(cfg), lg)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		c.node[id] = n

		count := c.disabled[id]
		srv := &http.Server{
			Handler:           NewHandler(n, lg, func() { count.Add(1) }),
			ReadHeaderTimeout: 5 * time.Second,
		}
		c.srv[id] = srv
		// Serve 拿的是局部变量：夹具的 map 还在往后填，goroutine 里读它就是并发读写。
		ln := c.ln[id]
		go func(id string) {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("serve %s: %v", id, err)
			}
		}(id)
	}
	t.Cleanup(c.close)
	return c
}

func (c *httpCluster) close() {
	for _, id := range c.ids {
		if n := c.node[id]; n != nil {
			err := n.Close()
			// breakSync 过的节点收尾必带 ErrIO，那是夹具造的失效，不当收尾失败。
			if c.broken[id] && errors.Is(err, kvstore.ErrIO) {
				err = nil
			}
			if err != nil {
				c.t.Errorf("close node %s: %v", id, err)
			}
		}
		if kv := c.kv[id]; kv != nil {
			if err := kv.Close(); err != nil {
				c.t.Errorf("close kv %s: %v", id, err)
			}
		}
		if srv := c.srv[id]; srv != nil {
			_ = srv.Close()
		}
	}
}

// stopNode 关掉一台节点的服务器并把它从 tick 循环里摘掉：之后 Leader 发给它的 RPC
// 一律拿到连接错误，等价真机上那台宕了（3.4 T25.3）。节点与 KV 一起摘，收尾不再碰它们。
func (c *httpCluster) stopNode(id string) {
	c.t.Helper()
	c.node[id] = nil
	if kv := c.kv[id]; kv != nil {
		if err := kv.Close(); err != nil {
			c.t.Fatalf("close kv %s: %v", id, err)
		}
		c.kv[id] = nil
	}
	if srv := c.srv[id]; srv != nil {
		_ = srv.Close()
		c.srv[id] = nil
	}
}

// tick 给全部节点各打 rounds 次 Tick；一轮就是每台机器一次（D16）。
func (c *httpCluster) tick(rounds int) {
	for range rounds {
		for _, id := range c.ids {
			if n := c.node[id]; n != nil {
				n.Tick()
			}
		}
	}
}

// waitForLeader 一轮一轮打到集群里只有一个自认 Leader；同时两个就是裂脑。
func (c *httpCluster) waitForLeader(rounds int) string {
	c.t.Helper()
	for range rounds {
		c.tick(1)
		var got []string
		for _, id := range c.ids {
			if n := c.node[id]; n != nil && n.Role() == RoleLeader {
				got = append(got, id)
			}
		}
		switch len(got) {
		case 0:
		case 1:
			return got[0]
		default:
			c.t.Fatalf("more than one leader at once: %v", got)
		}
	}
	c.t.Fatalf("no leader within %d rounds", rounds)
	return ""
}

// post 发一条 JSON 请求，返回状态码与原始响应体：形状断言读的是原文，不是解码后的结构。
func (c *httpCluster) post(id, path, body string) (int, []byte) {
	c.t.Helper()
	resp, err := http.Post(c.url[id]+path, "application/json", strings.NewReader(body))
	if err != nil {
		c.t.Fatalf("post %s%s: %v", id, path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, readAll(c.t, resp)
}

func (c *httpCluster) get(id, path string) (int, []byte) {
	c.t.Helper()
	resp, err := http.Get(c.url[id] + path)
	if err != nil {
		c.t.Fatalf("get %s%s: %v", id, path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, readAll(c.t, resp)
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read reply from %s: %v", resp.Request.URL.Path, err)
	}
	return raw
}

// putKV 走客户端那条路：状态码不是 200 就说明用例的前提塌了。
func (c *httpCluster) putKV(id, key, value string) map[string]any {
	c.t.Helper()
	status, raw := c.post(id, "/put", fmt.Sprintf(`{"key":%q,"value":%q}`, key, value))
	if status != http.StatusOK {
		c.t.Fatalf("put %s on %s = %d: %s", key, id, status, raw)
	}
	return decodeMap(c.t, raw)
}

// getKV 走本地旧数据读那一档：这一格断言的是「每个节点各自 apply 过这条写」，
// 与领导权无关，所以显式带 consistency=local（4.1 §4、D24）。
// 线性一致读那一档另走默认参数，见 TestGetContractTwoConsistencyModes。
func (c *httpCluster) getKV(id, key string) (int, string) {
	c.t.Helper()
	status, raw := c.get(id, "/get?key="+key+"&consistency=local")
	if status != http.StatusOK {
		return status, ""
	}
	value, _ := decodeMap(c.t, raw)["value"].(string)
	return status, value
}

func decodeMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body is not one JSON object: %s: %v", firstBytes(raw, 200), err)
	}
	return out
}

// keysOf 是响应体的键名清单，排序后拼成一串：少一个键或多一个键都会在这条断言上现形。
func keysOf(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

// 5 节点的 HTTP 集群选出一个 Leader、把一次写复制到全员：M12 收口判据的进程内版本，
// 真进程的对照在 cluster_process_test.go。
func TestHTTPClusterWritesReachEveryNode(t *testing.T) {
	c := newHTTPCluster(t, 5, zap.NewNop())
	leader := c.waitForLeader(60)

	var prev uint64
	for i, key := range []string{"k1", "k2", "k3"} {
		reply := c.putKV(leader, key, fmt.Sprintf("v%d", i))
		if reply["ok"] != true {
			t.Fatalf("reply shape = %v, want ok=true", reply)
		}
		if got := reply["req_id"]; got == nil || got == "" {
			t.Fatalf("reply %v has no req_id", reply)
		}
		index := uint64(reply["index"].(float64))
		if index <= prev {
			t.Fatalf("index %d does not follow %d", index, prev)
		}
		prev = index
		if term, want := uint64(reply["term"].(float64)), c.node[leader].Stats().Term; term != want {
			t.Fatalf("term = %d, want the leader's current term %d", term, want)
		}
	}

	// 心跳把 commitIndex 递给其余四台，它们的 apply 才追得上。
	c.tick(8)
	for _, id := range c.ids {
		if status, value := c.getKV(id, "k3"); status != http.StatusOK || value != "v2" {
			t.Fatalf("%s reads %q (status %d), want v2 on every node", id, value, status)
		}
	}

	follower := c.ids[0]
	if follower == leader {
		follower = c.ids[1]
	}
	status, raw := c.post(follower, "/put", `{"key":"k9","value":"v9"}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("put on follower %s = %d: %s", follower, status, raw)
	}
	body := decodeMap(t, raw)
	if body["code"] != "not_leader" {
		t.Fatalf("code = %v, want not_leader: %s", body["code"], raw)
	}
	if body["leader_id"] != leader || body["leader_addr"] != c.addr[leader] {
		t.Fatalf("leader hint = %v/%v, want %s/%s",
			body["leader_id"], body["leader_addr"], leader, c.addr[leader])
	}
	if keysOf(body) != "code,error,leader_addr,leader_id,req_id" {
		t.Fatalf("not_leader body keys = %q: %s", keysOf(body), raw)
	}
}

// 没听说过 Leader 的节点上 `/put` 也是 503：两个提示回空串，五个键照旧全在（3.1 §4 的固定形状）。
func TestNotLeaderHintKeysStayWhenLeaderUnknown(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	id := c.ids[0]

	status, raw := c.post(id, "/put", `{"key":"k","value":"v"}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("put on a node that never heard from a leader = %d: %s", status, raw)
	}
	body := decodeMap(t, raw)
	if body["code"] != "not_leader" {
		t.Fatalf("code = %v, want not_leader: %s", body["code"], raw)
	}
	if body["leader_id"] != "" || body["leader_addr"] != "" {
		t.Fatalf("leader hint = %v/%v, want two empty strings: %s",
			body["leader_id"], body["leader_addr"], raw)
	}
	if keysOf(body) != "code,error,leader_addr,leader_id,req_id" {
		t.Fatalf("not_leader body keys = %q, want the five documented ones: %s", keysOf(body), raw)
	}
}

// 一台节点的服务器关掉之后 Leader 照样提交：多数派还在，给死节点的 RPC 只被忽略（3.4 T25.3 的判据）。
func TestLeaderCommitsWhileOnePeerIsDown(t *testing.T) {
	c := newHTTPCluster(t, 5, zap.NewNop())
	leader := c.waitForLeader(60)

	first := c.putKV(leader, "before", "v0")
	prev := uint64(first["index"].(float64))

	down := ""
	for _, id := range c.ids {
		if id != leader {
			down = id
			break
		}
	}
	c.stopNode(down)

	second := c.putKV(leader, "after", "v1")
	index := uint64(second["index"].(float64))
	if index <= prev {
		t.Fatalf("index %d does not follow %d with %s down", index, prev, down)
	}

	c.tick(8)
	for _, id := range c.ids {
		if id == down {
			continue
		}
		if status, value := c.getKV(id, "after"); status != http.StatusOK || value != "v1" {
			t.Fatalf("%s reads %q (status %d), want v1 with %s down", id, value, status, down)
		}
	}
}

// /get 的两档一致性：默认档是线性一致读，非 Leader 上回 503；
// consistency=local 是显式请求的本地旧数据读，任意节点都回 200（4.1 §4、D24）。
// 成功响应固定六键，503 的五个键与 /put 那格逐键相同。
func TestGetContractTwoConsistencyModes(t *testing.T) {
	c := newHTTPCluster(t, 5, zap.NewNop())
	leader := c.waitForLeader(60)
	follower := ""
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.putKV(leader, "k", "v")
	c.tick(8)

	// 默认档：Leader 上 200，六个键一个不多一个不少。
	status, raw := c.get(leader, "/get?key=k")
	if status != http.StatusOK {
		t.Fatalf("linearizable get on the leader = %d: %s", status, raw)
	}
	body := decodeMap(t, raw)
	if got := keysOf(body); got != "key,read_index,req_id,stale,term,value" {
		t.Fatalf("get body keys = %q, want the six documented ones: %s", got, raw)
	}
	if body["value"] != "v" || body["key"] != "k" {
		t.Fatalf("get body = %v, want k=v", body)
	}
	if body["stale"] != false {
		t.Fatalf("stale = %v on a linearizable read, want false", body["stale"])
	}
	if index := body["read_index"].(float64); index < 1 {
		t.Fatalf("read_index = %v, want at least 1: the leader committed the no-op", index)
	}
	if term := body["term"].(float64); term != float64(c.node[leader].Stats().Term) {
		t.Fatalf("term = %v, want the leader's current term", term)
	}

	// 默认档在 Follower 上是 503：五个键与 /put 那格逐键相同。
	status, raw = c.get(follower, "/get?key=k")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("linearizable get on follower %s = %d: %s", follower, status, raw)
	}
	refused := decodeMap(t, raw)
	if refused["code"] != "not_leader" {
		t.Fatalf("code = %v, want not_leader: %s", refused["code"], raw)
	}
	if got := keysOf(refused); got != "code,error,leader_addr,leader_id,req_id" {
		t.Fatalf("not_leader body keys = %q, want the same five as /put: %s", got, raw)
	}
	if refused["leader_id"] != leader {
		t.Fatalf("leader hint = %v, want %s", refused["leader_id"], leader)
	}

	// local 档：Follower 上也是 200，读索引回 0、stale 回 true。
	status, raw = c.get(follower, "/get?key=k&consistency=local")
	if status != http.StatusOK {
		t.Fatalf("local get on follower %s = %d: %s", follower, status, raw)
	}
	local := decodeMap(t, raw)
	if got := keysOf(local); got != "key,read_index,req_id,stale,term,value" {
		t.Fatalf("local get body keys = %q, want the six documented ones: %s", got, raw)
	}
	if local["stale"] != true {
		t.Fatalf("stale = %v on a local read, want true", local["stale"])
	}
	if index := local["read_index"].(float64); index != 0 {
		t.Fatalf("read_index = %v on a local read, want 0: no confirmation happened", index)
	}
	if local["value"] != "v" {
		t.Fatalf("local read value = %v, want v: the follower applied the committed entry", local["value"])
	}
}

// consistency 只取两个值，其余一律 400；非 GET 方法在 /get 上是 405（4.1 §4）。
func TestGetRejectsAnUnknownConsistency(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	id := c.ids[0]

	status, raw := c.get(id, "/get?key=k&consistency=bogus")
	if status != http.StatusBadRequest {
		t.Fatalf("unknown consistency = %d: %s", status, firstBytes(raw, 200))
	}
	if code := decodeMap(t, raw)["code"]; code != "bad_request" {
		t.Fatalf("code = %v, want bad_request", code)
	}

	status, raw = c.post(id, "/get", `{"key":"k"}`)
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("POST on /get = %d: %s", status, firstBytes(raw, 200))
	}
}

// append-entries 的 readRound 在必填之列：缺了 400，带 0 通过；Follower 原样回显（4.1 §4、D25）。
func TestAppendEntriesRequiresReadRound(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	id := c.ids[0]

	body := `{"term":3,"leaderId":"node-9","prevLogIndex":0,"prevLogTerm":0,"entries":[],"leaderCommit":0`
	status, raw := c.post(id, "/raft/append-entries", body+"}")
	if status != http.StatusBadRequest {
		t.Fatalf("append without readRound = %d: %s", status, firstBytes(raw, 200))
	}
	if code := decodeMap(t, raw)["code"]; code != "bad_request" {
		t.Fatalf("code = %v, want bad_request", code)
	}

	status, raw = c.post(id, "/raft/append-entries", body+`,"readRound":0}`)
	if status != http.StatusOK {
		t.Fatalf("append with readRound 0 = %d: %s", status, raw)
	}
	if reply := decodeMap(t, raw); reply["readRound"] != float64(0) {
		t.Fatalf("reply readRound = %v, want the request's 0", reply["readRound"])
	}

	status, raw = c.post(id, "/raft/append-entries", body+`,"readRound":7}`)
	if status != http.StatusOK {
		t.Fatalf("append with readRound 7 = %d: %s", status, raw)
	}
	if reply := decodeMap(t, raw); reply["readRound"] != float64(7) {
		t.Fatalf("reply readRound = %v, want the request's 7: the follower must echo it", reply["readRound"])
	}
}

// 读超时的上界由 -read-timeout-ms 给：确认轮凑不齐多数派时这条读拿 500 canceled，
// applyWaiters 里不留唤醒不了的等待（4.1 §7、D28）。
func TestGetTimesOutWhenTheConfirmationCannotSettle(t *testing.T) {
	c := newHTTPClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.ReadTimeoutMS = 50
	})
	leader := c.waitForLeader(60)

	// 两个对端都停掉：确认轮拿不到多数派的票，读只能等超时。
	for _, id := range c.ids {
		if id != leader {
			c.stopNode(id)
		}
	}

	status, raw := c.get(leader, "/get?key=k")
	if status != http.StatusInternalServerError {
		t.Fatalf("get without a majority = %d: %s", status, firstBytes(raw, 200))
	}
	body := decodeMap(t, raw)
	if body["code"] != "canceled" {
		t.Fatalf("code = %v, want canceled: %s", body["code"], firstBytes(raw, 200))
	}
	if body["value"] != nil {
		t.Fatalf("a failed read must not carry a value: %s", raw)
	}

	n := c.node[leader]
	n.mu.Lock()
	waiters := len(n.applyWaiters)
	n.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("applyWaiters holds %d entries after the timeout, want none", waiters)
	}
}

// 线上形状逐项核对：字段名、类型，以及 conflictTerm 的空值是 JSON 的 null 而不是 0（3.1 §4）。
func TestRPCWireShape(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	id := c.ids[0]

	status, raw := c.post(id, "/raft/request-vote",
		`{"term":3,"candidateId":"node-9","lastLogIndex":7,"lastLogTerm":2}`)
	if status != http.StatusOK {
		t.Fatalf("request-vote = %d: %s", status, raw)
	}
	vote := decodeMap(t, raw)
	if got := keysOf(vote); got != "term,voteGranted" {
		t.Fatalf("vote reply keys = %q, want term,voteGranted: %s", got, raw)
	}
	if vote["term"] != float64(3) || vote["voteGranted"] != true {
		t.Fatalf("vote reply = %v, want term 3 and voteGranted true", vote)
	}
	if stats := c.node[id].Stats(); stats.Term != 3 {
		t.Fatalf("term after the vote = %d, want the request's 3: the field was not read", stats.Term)
	}

	// 同任期的第二个候选人：这一票已经给出去了，拒绝才证明 votedFor 落了盘。
	_, raw = c.post(id, "/raft/request-vote",
		`{"term":3,"candidateId":"node-8","lastLogIndex":7,"lastLogTerm":2}`)
	if denied := decodeMap(t, raw); denied["voteGranted"] != false || denied["term"] != float64(3) {
		t.Fatalf("second candidate in the same term = %v, want term 3 and voteGranted false", denied)
	}

	status, raw = c.post(id, "/raft/append-entries",
		`{"term":5,"leaderId":"node-9","prevLogIndex":5,"prevLogTerm":5,"entries":[],"leaderCommit":0,"readRound":0}`)
	if status != http.StatusOK {
		t.Fatalf("append-entries = %d: %s", status, raw)
	}
	app := decodeMap(t, raw)
	if got := keysOf(app); got != "conflictIndex,conflictTerm,readRound,success,term" {
		t.Fatalf("append reply keys = %q, want conflictIndex,conflictTerm,readRound,success,term: %s", got, raw)
	}
	if app["term"] != float64(5) || app["success"] != false || app["conflictIndex"] != float64(1) {
		t.Fatalf("append reply = %v, want term 5, success false, conflictIndex 1", app)
	}
	if conflict, ok := app["conflictTerm"]; !ok || conflict != nil {
		t.Fatalf(`conflictTerm = %v (present: %v), want the JSON null doc 3 spells for a short log`, conflict, ok)
	}

	// command 是不透明字节：这串 base64 解不成 kv 条目，raft 照样收下，因为它一个字节都不解（D4）。
	status, raw = c.post(id, "/raft/append-entries",
		`{"term":5,"leaderId":"node-9","prevLogIndex":0,"prevLogTerm":0,`+
			`"entries":[{"term":5,"index":1,"command":"bm90LWEta3YtZW50cnk="}],"leaderCommit":0,"readRound":0}`)
	if status != http.StatusOK {
		t.Fatalf("append with an opaque command = %d: %s", status, raw)
	}
	if ack := decodeMap(t, raw); ack["success"] != true {
		t.Fatalf("append with an opaque command = %v, want success true", ack)
	}
	if stats := c.node[id].Stats(); stats.LastLogIndex != 1 {
		t.Fatalf("lastLogIndex = %d, want the opaque entry stored at 1", stats.LastLogIndex)
	}
}

// 非法请求的四种形状各有自己的状态码：非法 JSON、缺字段、一条请求塞两次写、方法与路径不对（3.1 §4 形状表）。
func TestIllegalRequestsGetTheirOwnStatusCode(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	id := c.ids[0]

	cases := []struct {
		name   string
		path   string
		body   string
		status int
		code   string
	}{
		{"vote with two fields missing", "/raft/request-vote", `{"term":3}`,
			http.StatusBadRequest, "bad_request"},
		{"append with leaderCommit missing", "/raft/append-entries",
			`{"term":3,"leaderId":"node-9","prevLogIndex":0,"prevLogTerm":0,"entries":[]}`,
			http.StatusBadRequest, "bad_request"},
		{"not JSON at all", "/raft/request-vote", `term=3`, http.StatusBadRequest, "bad_request"},
		{"an array where an object belongs", "/raft/request-vote", `[{"term":3}]`,
			http.StatusBadRequest, "bad_request"},
		{"two requests in one body", "/raft/request-vote",
			`{"term":3,"candidateId":"n","lastLogIndex":0,"lastLogTerm":0}` +
				`{"term":4,"candidateId":"n","lastLogIndex":0,"lastLogTerm":0}`,
			http.StatusBadRequest, "bad_request"},
		{"put without value", "/put", `{"key":"k"}`, http.StatusBadRequest, "bad_request"},
		{"put with an empty body", "/put", ``, http.StatusBadRequest, "bad_request"},
		{"vote with everything present", "/raft/request-vote",
			`{"term":3,"candidateId":"n","lastLogIndex":0,"lastLogTerm":0}`, http.StatusOK, ""},
	}
	for _, tc := range cases {
		status, raw := c.post(id, tc.path, tc.body)
		if status != tc.status {
			t.Fatalf("%s: status = %d, want %d: %s", tc.name, status, tc.status, firstBytes(raw, 200))
		}
		if tc.code == "" {
			continue
		}
		if got := decodeMap(t, raw)["code"]; got != tc.code {
			t.Fatalf("%s: code = %v, want %q: %s", tc.name, got, tc.code, firstBytes(raw, 200))
		}
	}

	if status, raw := c.get(id, "/raft/request-vote"); status != http.StatusMethodNotAllowed {
		t.Fatalf("GET on a POST route = %d: %s", status, firstBytes(raw, 200))
	}
	if status, raw := c.get(id, "/raft/nope"); status != http.StatusNotFound {
		t.Fatalf("unknown route = %d: %s", status, firstBytes(raw, 200))
	}
	if status, raw := c.get(id, "/get"); status != http.StatusBadRequest {
		t.Fatalf("/get without key = %d: %s", status, firstBytes(raw, 200))
	}

	// 超限行：一条请求体比 2.1 的单条上限还长，只能按 413 拒，不能截断着收。
	oversize := fmt.Sprintf(`{"key":"k","value":%q}`, strings.Repeat("v", kvstore.MaxEntrySize))
	status, raw := c.post(id, "/put", oversize)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize put = %d: %s", status, firstBytes(raw, 200))
	}
	if code := decodeMap(t, raw)["code"]; code != "too_large" {
		t.Fatalf("oversize put code = %v, want too_large", code)
	}
}

// 管道 fd 造一次 fsync 失败：追加照常、Sync 必挂，写路径就此关闭（D5）。
// /put 一律 500 io_error，/get 照常服务，失效回调只响一次——入口层据此退出码 1。
func TestRaftLogSyncFailureClosesTheWritePath(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)

	reply := c.putKV(leader, "before", "kept")
	index := uint64(reply["index"].(float64))
	if want := c.node[leader].Stats(); want.CommitIndex < index {
		t.Fatalf("commitIndex = %d, want at least %d", want.CommitIndex, index)
	}

	c.breakSync(leader)
	status, raw := c.post(leader, "/put", `{"key":"after","value":"dropped"}`)
	if status != http.StatusInternalServerError {
		t.Fatalf("put after the sync failure = %d: %s", status, firstBytes(raw, 200))
	}
	if body := decodeMap(t, raw); body["code"] != "io_error" {
		t.Fatalf("code = %v, want io_error: %s", body["code"], firstBytes(raw, 200))
	}
	if stats := c.node[leader].Stats(); stats.CommitIndex != index {
		t.Fatalf("commitIndex moved to %d while the write path is closed, want it kept at %d",
			stats.CommitIndex, index)
	}

	// 读路径不受影响（D5）。
	if status, value := c.getKV(leader, "before"); status != http.StatusOK || value != "kept" {
		t.Fatalf("/get after the failure = %d %q, want 200 kept", status, value)
	}

	// 第二笔写照样失败，但失效回调不能再响第二次：入口层只该退出一次。
	if status, _ := c.post(leader, "/put", `{"key":"again","value":"dropped"}`); status != http.StatusInternalServerError {
		t.Fatalf("second put = %d, want 500", status)
	}
	if count := c.disabled[leader].Load(); count != 1 {
		t.Fatalf("write-disabled callback fired %d times, want exactly 1", count)
	}
	if !c.node[leader].writeDisabled() {
		t.Fatal("the node still reports its write path open")
	}
}

// breakSync 把 raft 日志的写句柄换成一根管道的写作端：Write 全部成功，
// Sync 打的是管道上的 fsync，Linux 固定回 EINVAL——失效只能来自这一格（3.2 §3 的造错手法）。
func (c *httpCluster) breakSync(id string) {
	t := c.t
	t.Helper()
	n := c.node[id]
	// 拿住节点锁才能一次换掉句柄：Propose 的追加与 Sync 都在同一把锁里。
	n.mu.Lock()
	defer n.mu.Unlock()

	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	go func() { _, _ = io.Copy(io.Discard, pr) }()
	t.Cleanup(func() { _ = pr.Close() })

	if err := n.log.file.Close(); err != nil {
		t.Fatalf("close the real raft log file: %v", err)
	}
	n.log.file = pw
	c.broken[id] = true
}

func firstBytes(raw []byte, n int) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) > n {
		return string(raw[:n]) + "..."
	}
	return string(raw)
}
