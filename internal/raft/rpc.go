package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 四条路径共用一个端口：客户端与 RPC 打在同一个监听地址上（3.1 §4、D11）。
const (
	routeRequestVote     = "/raft/request-vote"
	routeAppendEntries   = "/raft/append-entries"
	routeSnapshot        = "/raft/snapshot"
	routeInstallSnapshot = "/raft/install-snapshot"
	routeMembership      = "/raft/membership"
	routePut             = "/put"
	routeGet             = "/get"

	// 一轮 AppendEntries 最多 64 条、每条命令的上限是 2.1 的 MaxEntrySize，
	// 请求体的天花板按这两个值相乘再留一帧的余量；回执是小对象。
	maxRPCBodyBytes   = int64(maxAppendEntries)*kvstore.MaxEntrySize + (1 << 20)
	maxReplyBodyBytes = int64(1 << 20)

	// 对端非 200 时只读这么多个字节进错误文本：那是给人看的线索，不是载荷。
	maxErrorBodyBytes = 512

	// 补发快照的请求体是快照文件的原始字节，它比一批 AppendEntries 大得多，
	// 上限按单条 entry 的四倍给：状态机的键值条数与日志条数同量级。
	maxSnapshotBodyBytes = int64(64 * kvstore.MaxEntrySize)
)

// httpTransport 是 Transport 的真实现：一条 RPC 一个 POST，JSON 编解码（D18）。
// 超时取 min(2×-tick-ms, -election-timeout-ms)（3.1 §9）：RPC 在 Tick() 里串行发出，
// 单个死节点占的时间必须远小于选举窗口，否则一轮投票会被四个超时之和拖过 T。
type httpTransport struct {
	client *http.Client
}

// NewTransport 给入口层用；单测里的内存夹具实现的是同一个接口，两边调的是同一批处理函数（3.1 §4）。
func NewTransport(cfg Config) Transport {
	return &httpTransport{client: &http.Client{Timeout: cfg.rpcTimeout()}}
}

func (t *httpTransport) RequestVote(to string, req VoteRequest) (VoteReply, error) {
	var reply VoteReply
	if err := t.post(to, routeRequestVote, req, &reply); err != nil {
		return VoteReply{}, err
	}
	return reply, nil
}

func (t *httpTransport) AppendEntries(to string, req AppendRequest) (AppendReply, error) {
	// 空数组是心跳的线上形状：nil 会被编码成 null，而 §4 的形状表写的是 entries=[]。
	if req.Entries == nil {
		req.Entries = []LogEntry{}
	}
	var reply AppendReply
	if err := t.post(to, routeAppendEntries, req, &reply); err != nil {
		return AppendReply{}, err
	}
	return reply, nil
}

// InstallSnapshot 补发一份快照：请求体是快照文件的原始字节，index 与 term 走 query（D32）。
func (t *httpTransport) InstallSnapshot(to string, index, term uint64, body []byte) error {
	url := fmt.Sprintf("http://%s%s?index=%d&term=%d", to, routeInstallSnapshot, index, term)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return kvstore.ErrIOFrom("build "+routeInstallSnapshot+" request", 0, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := t.client.Do(req)
	if err != nil {
		return kvstore.ErrIOFrom("send "+routeInstallSnapshot+" to "+to, 0, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReplyBodyBytes))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return fmt.Errorf("%s on %s answered %d: %s: %w",
			routeInstallSnapshot, to, resp.StatusCode, bytes.TrimSpace(detail), kvstore.ErrIO)
	}
	var reply InstallSnapshotReply
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplyBodyBytes)).Decode(&reply); err != nil {
		return kvstore.ErrIOFrom("decode "+routeInstallSnapshot+" reply from "+to, 0, err)
	}
	return nil
}

// post 发一条 RPC。连不上、超时、状态码不是 200 都只回一个错误，
// 上层把这视为一次失败、不重试（D23）。
func (t *httpTransport) post(addr, route string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return kvstore.ErrIOFrom("encode "+route+" request", 0, err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+route, bytes.NewReader(payload))
	if err != nil {
		return kvstore.ErrIOFrom("build "+route+" request", 0, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return kvstore.ErrIOFrom("send "+route+" to "+addr, 0, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReplyBodyBytes))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return fmt.Errorf("%s on %s answered %d: %s: %w",
			route, addr, resp.StatusCode, bytes.TrimSpace(detail), kvstore.ErrIO)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplyBodyBytes)).Decode(out); err != nil {
		return kvstore.ErrIOFrom("decode "+route+" reply from "+addr, 0, err)
	}
	return nil
}

// nodeHandler 把四条路径接在同一个 Node 上：两个 RPC 走的就是内存夹具调的那批未导出处理函数，
// /put 与 /get 走客户端路径。角色判断、复制、提交全在 Node 里，这里只做校验、映射与 req_id（D10）。
type nodeHandler struct {
	n  *Node
	lg *zap.Logger

	// 进程内自增，保证同一份日志里 req_id 不重复；跨进程靠各自的日志文件分开（2.1 §8）。
	seq      atomic.Uint64
	quit     func()
	quitting atomic.Bool
}

// NewHandler 起 HTTP 入口。onWriteDisabled 在写路径永久关闭后被调用一次，
// 由入口层结束进程（D5，与 2.1 §4 的退出码一档）。
func NewHandler(n *Node, lg *zap.Logger, onWriteDisabled func()) http.Handler {
	return &nodeHandler{
		n:    n,
		lg:   kvstore.WithComponent(lg, ComponentRaft).With(zap.String("node_id", n.cfg.NodeID)),
		quit: onWriteDisabled,
	}
}

func (h *nodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := "req-" + strconv.FormatUint(h.seq.Add(1), 10)

	switch {
	case r.URL.Path == routeRequestVote && r.Method == http.MethodPost:
		h.handleVote(w, r, reqID, start)
	case r.URL.Path == routeAppendEntries && r.Method == http.MethodPost:
		h.handleAppend(w, r, reqID, start)
	case r.URL.Path == routeSnapshot && r.Method == http.MethodPost:
		h.handleSnapshot(w, r, reqID, start)
	case r.URL.Path == routeInstallSnapshot && r.Method == http.MethodPost:
		h.handleInstallSnapshot(w, r, reqID, start)
	case r.URL.Path == routeMembership && r.Method == http.MethodPost:
		h.handleMembership(w, r, reqID, start)
	case r.URL.Path == routeMembership && r.Method == http.MethodGet:
		h.handleMembershipQuery(w, r, reqID, start)
	case r.URL.Path == routePut && r.Method == http.MethodPost:
		h.handlePut(w, r, reqID, start)
	case r.URL.Path == routeGet && r.Method == http.MethodGet:
		h.handleGet(w, r, reqID, start)
	case h.isKnownRoute(r.URL.Path):
		h.fail(w, r.URL.Path, reqID, http.StatusMethodNotAllowed,
			"method_not_allowed", "routes are POST /raft/request-vote, POST /raft/append-entries, POST /raft/snapshot, POST /raft/install-snapshot, POST /raft/membership, GET /raft/membership, POST /put and GET /get", 0, start)
	default:
		h.fail(w, r.URL.Path, reqID, http.StatusNotFound,
			"not_found", "unknown route", 0, start)
	}
}

func (h *nodeHandler) isKnownRoute(path string) bool {
	switch path {
	case routeRequestVote, routeAppendEntries, routeSnapshot, routeInstallSnapshot, routeMembership, routePut, routeGet:
		return true
	default:
		return false
	}
}

// handleVote 与 handleAppend 只搬运：解出请求体，交给 Node 的那五步／六步，把回执原样写回去。
// 回执的形状是 §4 的两个 reply 结构，不带 req_id——那是客户端那两条路径的字段。
func (h *nodeHandler) handleVote(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	var req VoteRequest
	if err := decodeOne(r, maxRPCBodyBytes, voteFields, &req); err != nil {
		h.badBody(w, routeRequestVote, reqID, err, start)
		return
	}
	h.reply(w, routeRequestVote, reqID, h.n.handleRequestVote(req), start)
}

func (h *nodeHandler) handleAppend(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	var req AppendRequest
	if err := decodeOne(r, maxRPCBodyBytes, appendFields, &req); err != nil {
		h.badBody(w, routeAppendEntries, reqID, err, start)
		return
	}
	h.reply(w, routeAppendEntries, reqID, h.n.handleAppendEntries(req), start)
}

// snapshotResponse 是手动触发快照的回执（5.1 §4 的形状表）。
type snapshotResponse struct {
	OK            bool   `json:"ok"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Entries       int    `json:"entries"`
}

// handleSnapshot 手动触发一次快照：非 Leader 回 503，快照点选不出来回 409。
// 回执里的条数读的是刚提交的元数据指针：那个数是快照文件的属性，不在 Node 上留副本。
func (h *nodeHandler) handleSnapshot(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	index, err := h.n.TriggerSnapshot()
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			h.notLeader(w, routeSnapshot, reqID, err, start)
			return
		}
		h.failWithError(w, routeSnapshot, reqID, err, start)
		return
	}
	pointer, err := h.n.snap.readPointer()
	if err != nil {
		h.failWithError(w, routeSnapshot, reqID, err, start)
		return
	}
	h.ok(w, routeSnapshot, reqID, snapshotResponse{OK: true, SnapshotIndex: index, Entries: pointer.Entries}, start)
}

// handleInstallSnapshot 接收对端补发的快照：请求体是快照文件的原始字节，
// index 与 term 走 query，缺一个就是 400（5.1 §4 的形状表）。
func (h *nodeHandler) handleInstallSnapshot(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	query := r.URL.Query()
	if !query.Has("index") || !query.Has("term") {
		h.fail(w, routeInstallSnapshot, reqID, http.StatusBadRequest,
			"bad_request", "query parameters index and term are required", 0, start)
		return
	}
	index, err := strconv.ParseUint(query.Get("index"), 10, 64)
	if err != nil {
		h.fail(w, routeInstallSnapshot, reqID, http.StatusBadRequest,
			"bad_request", "query parameter index is not a number", 0, start)
		return
	}
	term, err := strconv.ParseUint(query.Get("term"), 10, 64)
	if err != nil {
		h.fail(w, routeInstallSnapshot, reqID, http.StatusBadRequest,
			"bad_request", "query parameter term is not a number", 0, start)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxSnapshotBodyBytes))
	if err != nil {
		var over *http.MaxBytesError
		if errors.As(err, &over) {
			h.fail(w, routeInstallSnapshot, reqID, http.StatusRequestEntityTooLarge,
				"too_large", "snapshot body over the limit", 0, start)
			return
		}
		h.fail(w, routeInstallSnapshot, reqID, http.StatusBadRequest,
			"bad_request", err.Error(), 0, start)
		return
	}
	reply, err := h.n.handleInstallSnapshot(index, term, body)
	if err != nil {
		h.failWithError(w, routeInstallSnapshot, reqID, err, start)
		return
	}
	h.ok(w, routeInstallSnapshot, reqID, reply, start)
}

// membershipRequest 是 POST /raft/membership 的请求体（6.1 §4 的形状表）。
type membershipRequest struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

type membershipResponse struct {
	OK    bool   `json:"ok"`
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

type membershipQueryResponse struct {
	Voters   []string `json:"voters"`
	Learners []string `json:"learners"`
	Index    uint64   `json:"index"`
	Term     uint64   `json:"term"`
	LeaderID string   `json:"leader_id"`
}

// handleMembership 提交一次角色变更：缺字段或 role 不是 voter／learner 回 400，
// 非 Leader 回 503，四类拒绝回 409 config_rejected（6.1 §4、D40）。
func (h *nodeHandler) handleMembership(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	var body membershipRequest
	if err := decodeOne(r, maxRPCBodyBytes, membershipFields, &body); err != nil {
		h.badBody(w, routeMembership, reqID, err, start)
		return
	}
	if body.Role != MemberVoter && body.Role != MemberLearner {
		h.fail(w, routeMembership, reqID, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("role %q must be %s or %s", body.Role, MemberVoter, MemberLearner), 0, start)
		return
	}
	index, term, err := h.n.changeMembership(body.ID, body.Role)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			h.notLeader(w, routeMembership, reqID, err, start)
			return
		}
		h.failWithError(w, routeMembership, reqID, err, start)
		return
	}
	h.ok(w, routeMembership, reqID, membershipResponse{OK: true, Index: index, Term: term}, start)
}

// handleMembershipQuery 读当前视图：voters 与 learners 都升序，并带 configIndex 与 term（6.1 §4）。
func (h *nodeHandler) handleMembershipQuery(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	voters, learners, index, term := h.n.Membership()
	leaderID, _, _ := h.n.Leader()
	h.ok(w, routeMembership, reqID, membershipQueryResponse{
		Voters: voters, Learners: learners, Index: index, Term: term, LeaderID: leaderID,
	}, start)
}

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type putResponse struct {
	OK    bool   `json:"ok"`
	ReqID string `json:"req_id"`
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	ReqID string `json:"req_id"`
	// ReadIndex 与 Term 是线性一致读的两个游标；local 档的读索引回 0、任期回本地任期（4.1 §4、D24）。
	ReadIndex uint64 `json:"read_index"`
	Term      uint64 `json:"term"`
	// Stale 只在 local 档为真：这一读没有领导权确认（D24、不变式 6）。
	Stale bool `json:"stale"`
}

// errorResponse 的形状沿用 2.1 §4：error 给人看，code 给机器判，offset 指向坏记录的字节位置。
// 两个 leader 字段只在 not_leader 那一格出现，其余错误里连键都不写。
type errorResponse struct {
	Error  string `json:"error"`
	Code   string `json:"code"`
	ReqID  string `json:"req_id"`
	Offset int64  `json:"offset,omitempty"`
}

// notLeaderResponse 只在 /put 的 503 上用：两个 leader 提示不带 omitempty，
// 没听说过 Leader 时回空串，键仍在（3.1 §4「五个键固定」）。
type notLeaderResponse struct {
	Error      string `json:"error"`
	Code       string `json:"code"`
	ReqID      string `json:"req_id"`
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
}

// 三条 POST 路径各自的必填字段：缺一个就是 400，不能让一个字段都不带的请求
// 静默退化成「term 0 的心跳」——RPC 的每个字段都参与状态机判定（3.1 §4）。
// append-entries 的 readRound 也在必填之列：补零之后，为读发起的确认轮与例行心跳
// 在请求上就分不出来了（4.1 §4、D25）。
var (
	voteFields       = []string{"term", "candidateId", "lastLogIndex", "lastLogTerm"}
	appendFields     = []string{"term", "leaderId", "prevLogIndex", "prevLogTerm", "entries", "leaderCommit", "readRound"}
	membershipFields = []string{"id", "role"}
	putFields        = []string{"key", "value"}
)

func (h *nodeHandler) handlePut(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	var body putRequest
	if err := decodeOne(r, kvstore.MaxEntrySize, putFields, &body); err != nil {
		h.badBody(w, routePut, reqID, err, start)
		return
	}

	ctx := kvstore.WithRequestID(r.Context(), reqID)
	index, term, err := h.n.Propose(ctx, body.Key, body.Value)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			h.notLeader(w, routePut, reqID, err, start)
			return
		}
		if errors.Is(err, kvstore.ErrIO) && h.n.writeDisabled() && h.quitting.CompareAndSwap(false, true) {
			// D5：raft 日志的写路径已经永久关闭，进程留着只会继续返回错误，交给入口层退出。
			h.lg.Error("write path disabled, exiting", zap.String("req_id", reqID))
			if h.quit != nil {
				go h.quit()
			}
		}
		h.failWithError(w, routePut, reqID, err, start)
		return
	}
	h.ok(w, routePut, reqID, putResponse{OK: true, ReqID: reqID, Index: index, Term: term}, start)
}

func (h *nodeHandler) handleGet(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	if !r.URL.Query().Has("key") {
		h.fail(w, routeGet, reqID, http.StatusBadRequest,
			"bad_request", "query parameter key is required", 0, start)
		return
	}
	mode, err := parseConsistency(r.URL.Query().Get("consistency"))
	if err != nil {
		h.fail(w, routeGet, reqID, http.StatusBadRequest, "bad_request", err.Error(), 0, start)
		return
	}
	key := r.URL.Query().Get("key")
	// 单次读的上界由 -read-timeout-ms 给，Node 内部不起计时器（D28）。
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(h.n.cfg.ReadTimeoutMS)*time.Millisecond)
	defer cancel()
	value, index, term, err := h.n.Read(ctx, key, mode)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			h.notLeader(w, routeGet, reqID, err, start)
			return
		}
		h.failWithError(w, routeGet, reqID, err, start)
		return
	}
	h.ok(w, routeGet, reqID, getResponse{
		Key: key, Value: value, ReqID: reqID,
		ReadIndex: index, Term: term, Stale: mode == ConsistencyLocal,
	}, start)
}

// decodeOne 收一条请求体：超限、非法 JSON、缺字段、把多条写塞进一个请求都在这一步挡住（2.1 §4 同口径）。
// 整读一次再解两次：先解成字段集合查必填，再解进结构体。
func decodeOne(r *http.Request, limit int64, required []string, out any) error {
	raw, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, limit))
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range required {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("field %q is required", key)
		}
	}
	return json.Unmarshal(raw, out)
}

func (h *nodeHandler) badBody(w http.ResponseWriter, route, reqID string, err error, start time.Time) {
	var over *http.MaxBytesError
	if errors.As(err, &over) {
		h.fail(w, route, reqID, http.StatusRequestEntityTooLarge,
			"too_large", "request body over the entry limit", 0, start)
		return
	}
	h.fail(w, route, reqID, http.StatusBadRequest,
		"bad_request", "body must be one JSON object: "+err.Error(), 0, start)
}

// notLeader 是客户端那两条路径在非 Leader 上的那一格：503 带 leader 提示，
// 客户端按提示改投（3.1 §4、4.1 §4）。
// 这个节点没听说过 Leader 时两个字段留空，形状不变。
func (h *nodeHandler) notLeader(w http.ResponseWriter, route, reqID string, err error, start time.Time) {
	id, addr, ok := h.n.Leader()
	if !ok {
		id, addr = "", ""
	}
	h.lg.Warn("http request failed",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", http.StatusServiceUnavailable),
		zap.String("code", "not_leader"),
		zap.String("leader_id", id),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	writeJSON(w, http.StatusServiceUnavailable, notLeaderResponse{
		Error: err.Error(), Code: "not_leader", ReqID: reqID, LeaderID: id, LeaderAddr: addr,
	})
}

func (h *nodeHandler) failWithError(w http.ResponseWriter, route, reqID string, err error, start time.Time) {
	status, code := statusFor(err)
	offset, _ := kvstore.CorruptOffset(err)
	h.fail(w, route, reqID, status, code, err.Error(), offset, start)
}

func (h *nodeHandler) fail(w http.ResponseWriter, route, reqID string, status int, code, detail string, offset int64, start time.Time) {
	h.lg.Warn("http request failed",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", status),
		zap.String("code", code),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	writeJSON(w, status, errorResponse{Error: detail, Code: code, ReqID: reqID, Offset: offset})
}

// ok 是客户端那两条路径的成功回执：响应体自己带 req_id。
func (h *nodeHandler) ok(w http.ResponseWriter, route, reqID string, payload any, start time.Time) {
	h.lg.Debug("http request served",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", http.StatusOK),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	writeJSON(w, http.StatusOK, payload)
}

// reply 是 RPC 两条路径的成功回执：写出去的是 §4 的 reply 结构本身，字段一个不多。
func (h *nodeHandler) reply(w http.ResponseWriter, route, reqID string, payload any, start time.Time) {
	h.lg.Debug("rpc request served",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", http.StatusOK),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	writeJSON(w, http.StatusOK, payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// 响应已经写了头，编码失败只能截断在这一条上，没有别的兜底位置（2.1 §4 同款）。
	_ = json.NewEncoder(w).Encode(payload)
}
