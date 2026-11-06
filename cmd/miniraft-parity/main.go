// Package main 是 etcd 对拍客户端：同一串操作分别打给 mini-Raft 与 etcd，逐条落 ops.jsonl，
// 再按 docs/8 第四节的五档负载与第七节的字段口径出对照读数。两侧适配器在一份代码里，
// mini-Raft 走 /put 与 /get 的 JSON，etcd 走 gRPC-Gateway 的 /v3/kv/*，key 与 value 各解一层 base64（D9）。
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	targetMini = "mini"
	targetEtcd = "etcd"

	keyPrefix     = "parity-"
	clientTimeout = 2 * time.Second
)

// Rec 是一条操作的历史，字段口径见 docs/8 第七节。
type Rec struct {
	TS         int64  `json:"ts_client_ms"`
	Done       int64  `json:"ts_client_done_ms"`
	Phase      string `json:"phase"`
	Op         string `json:"op"`
	Key        string `json:"key,omitempty"`
	ValueSeq   int64  `json:"value_seq"`
	Status     string `json:"status"`
	Index      uint64 `json:"index,omitempty"`
	Term       uint64 `json:"term,omitempty"`
	Revision   uint64 `json:"revision,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"`
	Code       string `json:"code,omitempty"`
	LatencyUs  int64  `json:"latency_us"`
	Note       string `json:"note,omitempty"`
}

// Result 是一次调用的归一化读数：HTTP 语义两边走不到同一个码，状态一律收成
// 200 / 404 / 503 / timeout / err:<原文>，etcd 的 gRPC 错码留在 Code。
type Result struct {
	Status   string
	Index    uint64
	Term     uint64
	Revision uint64
	Value    string
	Leader   string
	Code     string
	Note     string
	Body     string
	Latency  time.Duration
}

type target struct {
	kind    string
	addrs   []string
	client  *http.Client
	dir     string
	shapes  map[string]any
	opsFile *os.File
	enc     *json.Encoder
	exp     map[string]*opSet
}

func main() {
	var (
		phase      = flag.String("phase", "topology", "topology|single|batch|stale|retry|bigvalue|freeze|partition|prevote|failover|readback")
		miniAddrs  = flag.String("mini", "127.0.0.1:8801,127.0.0.1:8802,127.0.0.1:8803,127.0.0.1:8804,127.0.0.1:8805", "mini-Raft 五个客户端地址，顺序即 node-1..node-5")
		etcdAddrs  = flag.String("etcd", "127.0.0.1:9801,127.0.0.1:9802,127.0.0.1:9803,127.0.0.1:9804,127.0.0.1:9805", "etcd 五个客户端地址，顺序即 node-1..node-5")
		out        = flag.String("out", "data/parity/latest", "本轮产物目录")
		ops        = flag.Int("ops", 200, "batch 与 failover 档的操作条数")
		cadence    = flag.Duration("cadence", 100*time.Millisecond, "failover 档的发写节奏")
		killstamp  = flag.String("killstamp", "", "驱动写入 kill 时刻(ms) 的文件，failover 档轮询")
		faultStamp = flag.String("faultstamp", "", "驱动写入冻主或隔离时刻(ms) 的文件，freeze 与 partition 档轮询")
		faultDur   = flag.Duration("faultdur", 7*time.Second, "freeze 与 partition 档的采样总长")
		faultFor   = flag.Duration("faultfor", 4*time.Second, "freeze 与 partition 档的故障持续时长，用来划窗口")
		aggDir     = flag.String("aggdir", "data/parity", "stats 档读的轮次根目录")
		rounds     = flag.Int("rounds", 5, "stats 档合并最近几轮")
	)
	flag.Parse()

	if *phase == "stats" {
		runStats(*aggDir, *rounds)
		return
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		die("创建产物目录: %v", err)
	}

	mini := newTarget(targetMini, splitAddrs(*miniAddrs), *out)
	etcdT := newTarget(targetEtcd, splitAddrs(*etcdAddrs), *out)
	defer func() {
		_ = mini.Close()
		_ = etcdT.Close()
	}()

	switch *phase {
	case "topology":
		runTopology(mini, etcdT)
	case "single":
		runSingle(mini, etcdT)
	case "batch":
		runBatch(mini, etcdT, *ops)
	case "stale":
		runStale(mini, etcdT)
	case "retry":
		runRetry(mini, etcdT)
	case "bigvalue":
		runBigValue(mini, etcdT)
	case "freeze":
		runFreeze(mini, etcdT, *faultDur, *faultFor, *faultStamp)
	case "partition":
		runPartition(mini, etcdT, *faultDur, *faultFor, *faultStamp)
	case "prevote":
		runPreVote(mini, etcdT, *faultDur, *faultFor, *faultStamp)
	case "failover":
		runFailover(mini, etcdT, *ops, *cadence, *killstamp)
	case "readback":
		runReadback(mini, etcdT)
	default:
		die("未知档位 %q", *phase)
	}
}

func newTarget(kind string, addrs []string, dir string) *target {
	client := &http.Client{Timeout: clientTimeout}
	// 分区档里节点之间按地址成对切断，客户端要绑一个不属于任何节点的源地址，
	// 否则它和第一台共用源地址，会被当成集群内部流量一起切掉。etcd 每台在自己的网络
	// 命名空间里，客户端要绑网桥地址 10.20.0.1 才路由得到 10.20.0.2x，两边分开给。
	bind := os.Getenv("PARITY_CLIENT_BIND_" + strings.ToUpper(kind))
	if bind == "" {
		bind = os.Getenv("PARITY_CLIENT_BIND")
	}
	if ip := net.ParseIP(bind); ip != nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.DialContext = (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}}).DialContext
		client.Transport = tr
	}
	t := &target{
		kind:   kind,
		addrs:  addrs,
		client: client,
		dir:    dir,
		shapes: map[string]any{},
	}
	f, err := os.OpenFile(filepath.Join(dir, "ops-"+kind+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		die("打开 ops-%s.jsonl: %v", kind, err)
	}
	t.opsFile = f
	t.enc = json.NewEncoder(f)
	t.exp = loadExpect(dir, kind)
	return t
}

func (t *target) Close() error {
	saveExpect(t)
	return t.opsFile.Close()
}

func (t *target) record(r Rec) {
	if r.TS == 0 {
		r.TS = time.Now().UnixMilli()
	}
	_ = t.enc.Encode(r)
}

func (t *target) writeOp(phase, key string, seq int64, res Result) Rec {
	return Rec{
		TS: res.stampMs(), Done: time.Now().UnixMilli(), Phase: phase, Op: "put", Key: key, ValueSeq: seq,
		Status: res.Status, Index: res.Index, Term: res.Term, Revision: res.Revision,
		LeaderHint: res.Leader, Code: res.Code, LatencyUs: res.Latency.Microseconds(), Note: res.Note,
	}
}

// write 把一次写连同读数落进 ops-<target>.jsonl，并把这一发记进期望表：
// 回执（200）与未回执（其余）分开记，readback 档按这两列算丢与多。
func (t *target) write(phase, addr, key, value string, seq int64) Rec {
	res := t.put(addr, key, value)
	rec := t.writeOp(phase, key, seq, res)
	t.track(key, seq, res.Status == "200")
	t.record(rec)
	return rec
}

// read 同样先取数再落盘：读到的序号就是 value_seq，单调性检查直接用它。
func (t *target) read(phase, addr, key string, stale bool) Rec {
	res := t.get(addr, key, stale)
	rec := Rec{
		TS: res.stampMs(), Done: time.Now().UnixMilli(), Phase: phase, Op: "get", Key: key, ValueSeq: seqOf(res.Value),
		Status: res.Status, Index: res.Index, Term: res.Term, Revision: res.Revision,
		LeaderHint: res.Leader, Code: res.Code, LatencyUs: res.Latency.Microseconds(), Note: res.Value,
	}
	t.record(rec)
	return rec
}

// stampMs 返回调用发出那一刻的毫秒时刻，不是返回时刻。
func (r Result) stampMs() int64 {
	return time.Now().Add(-r.Latency).UnixMilli()
}

// splitAddrs 拆成员串，端口顺序与两个起进程脚本的 node-1..node-5 同一份口径。
func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func key(name string) string { return keyPrefix + name }

// seqOf 从 "seq-<n>|<负载>" 里取序号，取不出来记 -1，单调性检查据此跳过这一条。
func seqOf(v string) int64 {
	head, _, _ := strings.Cut(v, "|")
	if !strings.HasPrefix(head, "seq-") {
		return -1
	}
	n, err := strconv.ParseInt(head[len("seq-"):], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func seqValue(n int64, load string) string { return fmt.Sprintf("seq-%d|%s", n, load) }

func die(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}

// ----- 两个适配器 -----

func (t *target) url(addr, path string) string { return "http://" + addr + path }

func (t *target) post(addr, path string, body []byte) Result {
	start := time.Now()
	req, err := http.NewRequest(http.MethodPost, t.url(addr, path), bytes.NewReader(body))
	if err != nil {
		return Result{Status: "err:" + err.Error(), Latency: time.Since(start)}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return Result{Status: statusOfErr(err), Latency: time.Since(start)}
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	return Result{Status: strconv.Itoa(resp.StatusCode), Body: string(raw), Latency: time.Since(start)}
}

func (t *target) getRaw(addr, urlPath string) Result {
	start := time.Now()
	resp, err := t.client.Get(t.url(addr, urlPath))
	if err != nil {
		return Result{Status: statusOfErr(err), Latency: time.Since(start)}
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	return Result{Status: strconv.Itoa(resp.StatusCode), Body: string(raw), Latency: time.Since(start)}
}

// put 与 get 按 kind 分派，返回值统一成 Result：mini-Raft 的 index 直接给，
// etcd 没有条目号，用 header.revision 顶同一个格子（docs/8 第六节的映射那格）。
func (t *target) put(addr, key, value string) Result {
	if t.kind == targetMini {
		res := t.post(addr, "/put", []byte(`{"key":"`+jsonString(key)+`","value":"`+jsonString(value)+`"}`))
		var body struct {
			OK    bool   `json:"ok"`
			Index uint64 `json:"index"`
			Term  uint64 `json:"term"`
		}
		if json.Unmarshal([]byte(res.Body), &body) == nil && body.Index != 0 {
			res.Index, res.Term = body.Index, body.Term
		}
		t.fillError(&res)
		return res
	}
	body := []byte(`{"key":"` + base64.StdEncoding.EncodeToString([]byte(key)) +
		`","value":"` + base64.StdEncoding.EncodeToString([]byte(value)) + `"}`)
	res := t.post(addr, "/v3/kv/put", body)
	t.fillEtcdHeader(&res)
	t.fillEtcdError(&res)
	return res
}

func (t *target) get(addr, key string, stale bool) Result {
	if t.kind == targetMini {
		consistency := "linearizable"
		if stale {
			consistency = "local"
		}
		res := t.getRaw(addr, "/get?key="+queryString(key)+"&consistency="+consistency)
		var body struct {
			Value     string `json:"value"`
			ReadIndex uint64 `json:"read_index"`
			Term      uint64 `json:"term"`
			Stale     bool   `json:"stale"`
		}
		if json.Unmarshal([]byte(res.Body), &body) == nil && res.Status == "200" {
			res.Value, res.Index, res.Term = body.Value, body.ReadIndex, body.Term
		}
		t.fillError(&res)
		return res
	}
	req := `{"key":"` + base64.StdEncoding.EncodeToString([]byte(key)) + `"}`
	if stale {
		req = `{"key":"` + base64.StdEncoding.EncodeToString([]byte(key)) + `","serializable":true}`
	}
	res := t.post(addr, "/v3/kv/range", []byte(req))
	t.fillEtcdHeader(&res)
	t.fillEtcdError(&res)
	var body struct {
		Kvs []struct {
			Value string `json:"value"`
		} `json:"kvs"`
	}
	if json.Unmarshal([]byte(res.Body), &body) == nil && len(body.Kvs) > 0 {
		if v, err := base64.StdEncoding.DecodeString(body.Kvs[0].Value); err == nil {
			res.Value = string(v)
		}
	} else if res.Status == "200" {
		res.Status = "404"
	}
	return res
}

// fillEtcdHeader 取 header 里的 revision 与 raft_term：etcd 把这两个数写成字符串，
// 与 mini-Raft 的 JSON 数字差一层 protobuf 的 JSON 约定（D9）。
func (t *target) fillEtcdHeader(res *Result) {
	var body struct {
		Header struct {
			Revision string `json:"revision"`
			RaftTerm string `json:"raft_term"`
			MemberID string `json:"member_id"`
		} `json:"header"`
	}
	if json.Unmarshal([]byte(res.Body), &body) != nil {
		return
	}
	res.Revision, _ = strconv.ParseUint(body.Header.Revision, 10, 64)
	res.Term, _ = strconv.ParseUint(body.Header.RaftTerm, 10, 64)
}

// fillError 把 mini-Raft 的错误码与 Leader 提示收进 Result；非 200 才有内容。
func (t *target) fillError(res *Result) {
	if res.Status == "200" {
		return
	}
	var body struct {
		Code       string `json:"code"`
		Error      string `json:"error"`
		LeaderID   string `json:"leader_id"`
		LeaderAddr string `json:"leader_addr"`
	}
	if json.Unmarshal([]byte(res.Body), &body) != nil {
		return
	}
	res.Code, res.Leader, res.Note = body.Code, body.LeaderAddr, body.Error
}

// fillEtcdError 取 gRPC-Gateway 的错误体：码是 gRPC 的数字码（3 = INVALID_ARGUMENT，
// 8 = RESOURCE_EXHAUSTED），HTTP 侧只落到 4xx/5xx，比 mini-Raft 的 code 字符串粗一层。
func (t *target) fillEtcdError(res *Result) {
	if res.Status == "200" {
		return
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(res.Body), &body) != nil || body.Code == 0 {
		return
	}
	res.Code, res.Note = "grpc_"+strconv.Itoa(body.Code), body.Message
}

// leader 查两边的 Leader 下标：mini-Raft 的 membership 给 leader_id（node-i），
// etcd 的 maintenance/status 给 member_id，只有 member/list 的地址能把它换回 node-i 的口径。
func (t *target) leader(addr string) (int, uint64, error) {
	if t.kind == targetMini {
		res := t.getRaw(addr, "/raft/membership")
		var body struct {
			LeaderID string `json:"leader_id"`
			Term     uint64 `json:"term"`
		}
		if json.Unmarshal([]byte(res.Body), &body) != nil {
			return -1, 0, fmt.Errorf("membership 读数不可解析：%s", res.Body)
		}
		i := idIndex(body.LeaderID)
		if i < 0 {
			return -1, 0, fmt.Errorf("membership 里没有 leader_id：%s", res.Body)
		}
		return i, body.Term, nil
	}
	res := t.post(addr, "/v3/maintenance/status", []byte(`{}`))
	var status struct {
		Leader   string `json:"leader"`
		RaftTerm string `json:"raftTerm"`
	}
	if json.Unmarshal([]byte(res.Body), &status) != nil {
		return -1, 0, fmt.Errorf("status 读数不可解析：%s", res.Body)
	}
	term, _ := strconv.ParseUint(status.RaftTerm, 10, 64)
	if status.Leader == "" || status.Leader == "0" {
		return -1, term, fmt.Errorf("status 里没有 leader：%s", res.Body)
	}
	list := t.post(addr, "/v3/cluster/member/list", []byte(`{}`))
	var members struct {
		Members []struct {
			ID         string   `json:"ID"`
			Name       string   `json:"name"`
			ClientURLs []string `json:"clientURLs"`
		} `json:"members"`
	}
	if json.Unmarshal([]byte(list.Body), &members) != nil {
		return -1, term, fmt.Errorf("member/list 读数不可解析：%s", list.Body)
	}
	for _, m := range members.Members {
		if m.ID != status.Leader {
			continue
		}
		for i, a := range t.addrs {
			for _, u := range m.ClientURLs {
				if strings.TrimPrefix(u, "http://") == a {
					return i, term, nil
				}
			}
		}
	}
	return -1, term, fmt.Errorf("leader %s 不在地址表里", status.Leader)
}

// membersText 给一行成员表读数：mini-Raft 报 voters，etcd 报 name@clientURL。
func (t *target) membersText(addr string) string {
	if t.kind == targetMini {
		res := t.getRaw(addr, "/raft/membership")
		var body struct {
			Voters   []string `json:"voters"`
			Learners []string `json:"learners"`
			Index    uint64   `json:"index"`
		}
		if json.Unmarshal([]byte(res.Body), &body) != nil {
			return "unparsed:" + res.Body
		}
		return strings.Join(body.Voters, ",") + " learners=" + strings.Join(body.Learners, ",") +
			" config_index=" + strconv.FormatUint(body.Index, 10)
	}
	res := t.post(addr, "/v3/cluster/member/list", []byte(`{}`))
	var members struct {
		Members []struct {
			Name       string   `json:"name"`
			ClientURLs []string `json:"clientURLs"`
		} `json:"members"`
	}
	if json.Unmarshal([]byte(res.Body), &members) != nil {
		return "unparsed:" + res.Body
	}
	out := make([]string, len(members.Members))
	for i, m := range members.Members {
		out[i] = m.Name + "@" + strings.Join(m.ClientURLs, "+")
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func idIndex(id string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(id, "node-"))
	if err != nil || n < 1 {
		return -1
	}
	return n - 1
}

func statusOfErr(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "refused"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "err:" + err.Error()
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

func queryString(s string) string {
	return strings.NewReplacer(" ", "%20", "&", "%26", "=", "%3D", "|", "%7C", "+", "%2B").Replace(s)
}

// ----- 统计与输出 -----

type stat struct {
	Phase     string
	Attempted int
	OK        int
	Non200    int
	Timeout   int
	P50Us     int64
	P99Us     int64
	Extra     string
}

func summarize(phase string, recs []Rec) stat {
	s := stat{Phase: phase, Attempted: len(recs)}
	lat := make([]int64, 0, len(recs))
	for _, r := range recs {
		switch r.Status {
		case "200":
			s.OK++
		case "timeout":
			s.Timeout++
		default:
			s.Non200++
		}
		lat = append(lat, r.LatencyUs)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if len(lat) > 0 {
		s.P50Us = lat[(len(lat)-1)*50/100]
		s.P99Us = lat[(len(lat)-1)*99/100]
	}
	return s
}

func (s stat) String() string {
	extra := ""
	if s.Extra != "" {
		extra = "\t" + s.Extra
	}
	return fmt.Sprintf("%s\tattempted=%d ok=%d non200=%d timeout=%d p50_us=%d p99_us=%d%s",
		s.Phase, s.Attempted, s.OK, s.Non200, s.Timeout, s.P50Us, s.P99Us, extra)
}

func report(t *target, stats ...stat) {
	for _, s := range stats {
		fmt.Printf("%s\t%s\n", t.kind, s.String())
	}
}
