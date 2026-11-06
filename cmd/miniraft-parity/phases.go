package main

// 五个无故障档位与杀主、读回两档的打法，按 docs/8 第四节与第五节逐条落。
// 每档两侧跑的是同一串操作：同一个 key 名、同一个序号、同一个顺序。

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	singleKeys = 20
	staleTries = 20
)

// bigSizes 覆盖两侧的边界：mini-Raft 的 value 上限是 1MiB（kvstore.MaxValueSize），
// etcd 的 max-request-bytes 是 1572864、gRPC 单次消息上限是 2MiB。
var bigSizes = []int{1 << 20, 1<<20 + 1, 1572864, 1572865, 2 << 20, 4 << 20}

// opSet 记一个 key 的两列序号：客户端发过哪几发、哪几发拿到了 200。
// 读回档拿它算两个数——已回执却读不到的条数，和没回执却已提交的条数。
type opSet struct {
	Acked     []int64 `json:"acked"`
	Attempted []int64 `json:"attempted"`
}

func expectPath(dir, kind string) string { return filepath.Join(dir, "expect-"+kind+".json") }

func loadExpect(dir, kind string) map[string]*opSet {
	m := map[string]*opSet{}
	raw, err := os.ReadFile(expectPath(dir, kind))
	if err != nil {
		return m
	}
	_ = json.Unmarshal(raw, &m)
	return m
}

func saveExpect(t *target) {
	raw, err := json.MarshalIndent(t.exp, "", "\t")
	if err != nil {
		return
	}
	_ = os.WriteFile(expectPath(t.dir, t.kind), raw, 0o644)
}

func (t *target) track(key string, seq int64, acked bool) {
	set := t.exp[key]
	if set == nil {
		set = &opSet{}
		t.exp[key] = set
	}
	set.Attempted = append(set.Attempted, seq)
	if acked {
		set.Acked = append(set.Acked, seq)
	}
}

func (t *target) addr(i int) string {
	if i < 0 || i >= len(t.addrs) {
		die("%s 侧地址下标 %d 越界", t.kind, i)
	}
	return t.addrs[i]
}

// leaderIndex 问出 Leader 的下标：先从能连通的进程问起，集群刚起或刚杀完主时留出选举窗口再问，
// 十秒还问不出来这档就没法配平，直接退。问到之后还要打在它身上验一发：被 kill 掉的那台
// 会继续出现在别的节点的角色表里一会儿，只看角色表会把读回打到已经没了的地址上。
func (t *target) leaderIndex() (int, uint64) {
	for i := 0; i < 40; i++ {
		if idx, term := t.probeLeader(); idx >= 0 && t.isLeader(t.addr(idx), idx) {
			return idx, term
		}
		time.Sleep(250 * time.Millisecond)
	}
	die("%s 侧问不出 Leader，先确认集群起没起、选举窗口够不够", t.kind)
	return -1, 0
}

// isLeader 用这个地址自己的角色表加一发本地读确认：进程在回话，而且它自己就认为自己是主。
func (t *target) isLeader(addr string, idx int) bool {
	res := t.get(addr, "__probe__", true)
	if res.Status != "200" && res.Status != "404" {
		return false
	}
	li, _, err := t.leader(addr)
	return err == nil && li == idx
}

func (t *target) probeLeader() (int, uint64) {
	for _, a := range t.addrs {
		if i, term, err := t.leader(a); err == nil {
			return i, term
		}
	}
	return -1, 0
}

// follower 取一个不是 Leader 的下标：两侧都取地址表里第一个满足条件的，角色对齐。
func follower(li int, n int) int {
	for i := 0; i < n; i++ {
		if i != li {
			return i
		}
	}
	return -1
}

func runTopology(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, term := t.leaderIndex()
		f := follower(li, len(t.addrs))
		la, fa := t.addr(li), t.addr(f)
		w := t.put(la, key("topology"), seqValue(1, "topology"))
		g := t.get(la, key("topology"), false)
		s := t.get(la, key("topology"), true)
		fw := t.put(fa, key("topology"), seqValue(2, "topology"))
		fg := t.get(fa, key("topology"), false)
		fgs := t.get(fa, key("topology"), true)
		rows := []string{
			"target\t" + t.kind,
			"addrs\t" + strings.Join(t.addrs, ","),
			"leader\t" + fmt.Sprintf("node-%d\t%s\tterm=%d", li+1, la, term),
			"members\t" + t.membersText(la),
			"put_keys\t" + jsonKeys(w.Body) + "\tstatus=" + w.Status,
			"get_keys\t" + jsonKeys(g.Body) + "\tstatus=" + g.Status,
			"stale_get_keys\t" + jsonKeys(s.Body) + "\tstatus=" + s.Status,
			"follower_put\tstatus=" + fw.Status + "\tcode=" + fw.Code + "\thint=" + fw.Leader +
				"\tlag_us=" + strconv.FormatInt(fw.Latency.Microseconds(), 10),
			"follower_get\tstatus=" + fg.Status + "\tcode=" + fg.Code + "\thint=" + fg.Leader +
				"\tlag_us=" + strconv.FormatInt(fg.Latency.Microseconds(), 10),
			"follower_stale_get\tstatus=" + fgs.Status + "\tvalue=" + fgs.Value,
		}
		if t.kind == targetEtcd {
			rows = append(rows, "version\t"+t.getRaw(la, "/version").Body)
		}
		writeRows(filepath.Join(t.dir, "topology-"+t.kind+".txt"), rows)
		for _, r := range rows {
			fmt.Printf("%s\ttopology\t%s\n", t.kind, r)
		}
	}
}

func runSingle(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la := t.addr(li)
		var ws, rs []Rec
		for i := int64(1); i <= singleKeys; i++ {
			ws = append(ws, t.write("single", la, key("single"), seqValue(i, "single"), i))
			rs = append(rs, t.read("single", la, key("single"), false))
		}
		v, first, last := monotonic(rs)
		extra := fmt.Sprintf("ack=%d/%d seq_first=%d seq_last=%d violations=%d %s",
			countStatus(ws, "200"), len(ws), first, last, v, seqOrder(t, "single", key("single"), ws))
		emit(t, "single", ws, rs, extra)
	}
}

func runBatch(mini, etcdT *target, n int) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la := t.addr(li)
		var ws, rs []Rec
		start := time.Now()
		for i := int64(1); i <= int64(n); i++ {
			k := key("batch-" + strconv.FormatInt(i, 10))
			ws = append(ws, t.write("batch", la, k, seqValue(i, "batch"), i))
		}
		writeDur := time.Since(start)
		start = time.Now()
		for i := int64(1); i <= int64(n); i++ {
			rs = append(rs, t.read("batch", la, key("batch-"+strconv.FormatInt(i, 10)), false))
		}
		readDur := time.Since(start)
		v, _, _ := monotonic(rs)
		extra := fmt.Sprintf("ops=%d ack=%d wall_write_ms=%d wall_read_ms=%d read_404=%d violations=%d",
			n, countStatus(ws, "200"), writeDur.Milliseconds(), readDur.Milliseconds(),
			countStatus(rs, "404"), v)
		emit(t, "batch", ws, rs, extra)
	}
}

// runStale 量的是一次线性一致写落地后，紧跟着一发旧值读读到旧值的比例与滞后条数。
// 线性一致档那一读打在 Leader 上：mini-Raft 的 Follower 直接回 503，这格在 topology 档已经量过。
func runStale(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la, fa := t.addr(li), t.addr(follower(li, len(t.addrs)))
		var ws, rs, ls []Rec
		var lag, fresh, behind, maxBehind, linearWrong int64
		for i := int64(1); i <= staleTries; i++ {
			ws = append(ws, t.write("stale", la, key("stale"), seqValue(i, "stale"), i))
			r := t.read("stale", la, key("stale"), false)
			rs = append(rs, r)
			if r.ValueSeq != i {
				linearWrong++
			}
			s := t.read("stale", fa, key("stale"), true)
			ls = append(ls, s)
			d := i - s.ValueSeq
			if d > 0 {
				lag++
				behind += d
				if d > maxBehind {
					maxBehind = d
				}
			} else {
				fresh++
			}
		}
		extra := fmt.Sprintf("follower=%s linear_wrong=%d stale_lag=%d stale_fresh=%d behind_sum=%d behind_max=%d sample=%s",
			fa, linearWrong, lag, fresh, behind, maxBehind, joinFirst(ls, 5))
		emit(t, "stale", ws, append(append([]Rec{}, rs...), ls...), extra)
	}
}

func runRetry(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la := t.addr(li)
		var ws []Rec
		for i := int64(1); i <= 2; i++ {
			ws = append(ws, t.write("retry", la, key("retry"), seqValue(9, "retry"), 9))
		}
		r := t.read("retry", la, key("retry"), false)
		extra := fmt.Sprintf("same_value_twice %s", seqOrder(t, "retry", key("retry"), ws))
		emit(t, "retry", ws, []Rec{r}, extra)
	}
}

func runBigValue(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la := t.addr(li)
		var rows []string
		for _, size := range bigSizes {
			k := key(fmt.Sprintf("big-%d", size))
			res := t.put(la, k, strings.Repeat("x", size))
			t.record(Rec{
				TS: res.stampMs(), Done: time.Now().UnixMilli(), Phase: "bigvalue", Op: "put", Key: k, ValueSeq: int64(size),
				Status: res.Status, Index: res.Index, Term: res.Term, Revision: res.Revision,
				LeaderHint: res.Leader, Code: res.Code, LatencyUs: res.Latency.Microseconds(), Note: res.Note,
			})
			rows = append(rows, fmt.Sprintf("bytes=%d\tstatus=%s\tcode=%s\tnote=%s\tlag_us=%d",
				size, res.Status, res.Code, trim(res.Note, 90), res.Latency.Microseconds()))
		}
		writeRows(filepath.Join(t.dir, "bigvalue-"+t.kind+".txt"), rows)
		for _, r := range rows {
			fmt.Printf("%s\tbigvalue\t%s\n", t.kind, r)
		}
	}
}

// runFailover 两侧并行：同一个 100ms 节奏各自打给自己的 Leader，驱动脚本在同一时刻 kill -9 两边的 Leader。
// 写端点选 Leader，失败之后由 retarget 换地址：mini-Raft 的 Follower 不回写、只给提示，
// etcd 的 Follower 会代转，所以两侧的「不可用窗口」量的都是重新有主，客户端跟随那段也算在里面。
func runFailover(mini, etcdT *target, n int, cadence time.Duration, killstamp string) {
	if killstamp == "" {
		die("failover 档要 -killstamp 指向驱动写的时刻文件")
	}
	type job struct {
		t        *target
		li       int
		term     uint64
		endpoint string
	}
	jobs := make([]job, 0, 2)
	for _, t := range []*target{mini, etcdT} {
		li, term := t.leaderIndex()
		jobs = append(jobs, job{t: t, li: li, term: term, endpoint: t.addr(li)})
	}
	var wg sync.WaitGroup
	var rows []string
	var mu sync.Mutex
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			r := j.t.failoverRun(n, cadence, killstamp, j.endpoint, j.li, j.term)
			mu.Lock()
			rows = append(rows, r...)
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	for _, r := range rows {
		fmt.Println(r)
	}
}

func (t *target) failoverRun(n int, cadence time.Duration, killstamp, endpoint string, li int, baseTerm uint64) []string {
	var killMs atomic.Int64
	stop := make(chan struct{})
	go pollKill(killstamp, &killMs, stop)
	start := endpoint
	recs := make([]Rec, 0, n)
	next := time.Now()
	follows, probes := 0, 0
	for i := int64(1); i <= int64(n); i++ {
		// 每一发占一个 key：读回档就能逐条判「这发到底提交没有」，不必靠序号前缀去推。
		rec := t.write("failover", endpoint, key(fmt.Sprintf("fo-%d", i)), seqValue(i, "fo"), i)
		if rec.Status != "200" {
			probes++
			// 客户端跟随：mini-Raft 的 503 带 leader_addr，etcd 的回话里没有地址，只能重问角色表。
			if na := t.retarget(endpoint, rec.LeaderHint); na != "" {
				endpoint = na
				follows++
			}
		}
		recs = append(recs, rec)
		if d := time.Until(next.Add(cadence)); d > 0 {
			next = next.Add(cadence)
			time.Sleep(d)
		} else {
			next = time.Now()
		}
	}
	close(stop)
	ms := killMs.Load()
	if ms == 0 {
		return []string{fmt.Sprintf("%s\tfailover\t没读到 kill 时刻，本轮作废", t.kind)}
	}
	// 发完就收的话选举可能还没走完，新主还没当上；等一小会儿再取，取到的必须是自己也认账的那台。
	nl, endTerm := t.probeLeader()
	for i := 0; i < 24 && !(nl >= 0 && t.isLeader(t.addr(nl), nl)); i++ {
		time.Sleep(250 * time.Millisecond)
		nl, endTerm = t.probeLeader()
	}
	firstFail, recover, codes := window(recs, ms)
	extra := fmt.Sprintf("kill_ms=%d start_endpoint=%s endpoint_at_end=%s leader_before=%s base_term=%d end_term=%d term_delta=%s new_leader=%s retargets=%d failed_sends=%d %s",
		ms, start, endpoint, nodeName(li), baseTerm, endTerm, deltaText(endTerm, baseTerm), nodeName(nl), follows, probes, codes)
	if recover > 0 {
		extra += fmt.Sprintf(" unavailable_ms=%d first_fail_after_kill_ms=%d", recover-ms, firstFail-ms)
	} else {
		extra += " unavailable_ms=none-in-window"
	}
	rows := []string{fmt.Sprintf("%s\t%s", t.kind, summarize("failover/put", recs).String())}
	rows = append(rows, fmt.Sprintf("%s\tfailover/extra\t%s", t.kind, extra))
	writeRows(filepath.Join(t.dir, "failover-"+t.kind+".txt"), []string{extra})
	return rows
}

// retarget 给出下一发写该打哪个地址：先用对端给的 Leader 提示，再逐个进程重问角色表，
// 问到的地址必须自己能应答才算数。回空串表示这一发没有可用的新地址，下一发继续打原处。
func (t *target) retarget(cur, hint string) string {
	cands := make([]string, 0, len(t.addrs)+1)
	if hint != "" && hint != cur {
		cands = append(cands, hint)
	}
	for _, a := range t.addrs {
		if a == cur {
			continue
		}
		if i, _, err := t.leader(a); err == nil {
			cands = append(cands, t.addr(i))
		}
		cands = append(cands, a)
	}
	for _, c := range cands {
		if c == "" || c == cur {
			continue
		}
		if t.alive(c) {
			return c
		}
	}
	return ""
}

// alive 探一个进程还在不在：etcd 问 /version，mini-Raft 问 /raft/membership，都只要它能回话。
func (t *target) alive(addr string) bool {
	if t.kind == targetEtcd {
		return t.getRaw(addr, "/version").Status == "200"
	}
	return t.getRaw(addr, "/raft/membership").Status == "200"
}

func pollKill(path string, ms *atomic.Int64, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if raw, err := os.ReadFile(path); err == nil {
			if v, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); v > 0 {
				ms.Store(v)
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// window 算不可用窗口：kill 之后第一发失败的时刻、第一发成功回到 200 的时刻，以及窗口里出现过的状态码集合。
// 门槛用回话到达的时刻而不是发写时刻：etcd 那发挂住的写在 2 秒后才回话，按发写时刻算会漏进窗口外。
func window(recs []Rec, killMs int64) (firstFail, recover int64, codes string) {
	seen := map[string]int{}
	var order []string
	for _, r := range recs {
		if r.Done < killMs {
			continue
		}
		if _, ok := seen[r.Status]; !ok {
			seen[r.Status] = 0
			order = append(order, r.Status)
		}
		seen[r.Status]++
		if r.Status != "200" {
			if firstFail == 0 {
				firstFail = r.Done
			}
			continue
		}
		if firstFail != 0 && recover == 0 {
			recover = r.Done
		}
	}
	parts := make([]string, 0, len(order))
	for _, s := range order {
		parts = append(parts, s+"×"+strconv.Itoa(seen[s]))
	}
	return firstFail, recover, "codes=" + strings.Join(parts, ",")
}

func runReadback(mini, etcdT *target) {
	for _, t := range []*target{mini, etcdT} {
		li, _ := t.leaderIndex()
		la := t.addr(li)
		names := make([]string, 0, len(t.exp))
		for k := range t.exp {
			names = append(names, k)
		}
		sort.Strings(names)
		var rows []string
		var lost, ghost, keys int
		for _, k := range names {
			set := t.exp[k]
			r := t.get(la, k, false)
			obs := seqOf(r.Value)
			if obs < 0 {
				obs = 0
			}
			l := countGT(set.Acked, obs)
			// 一个 key 只留最后一次写的值：超时但被后一发覆盖掉的写读不出来，也算不出「可见」。
			// 能判定的只有一件事——现在读到的这个值，来自一发拿过 200 的写，还是来自没回执的写。
			var maxAcked int64
			for _, s := range set.Acked {
				if s > maxAcked {
					maxAcked = s
				}
			}
			g := 0
			if obs > maxAcked {
				g = 1
			}
			if l > 0 || g > 0 {
				keys++
			}
			lost += l
			ghost += g
			rows = append(rows, fmt.Sprintf("key=%s\tstatus=%s\tobserved=%d\tacked=%d\tattempted=%d\tlost=%d\tghost=%d",
				k, r.Status, obs, len(set.Acked), len(set.Attempted), l, g))
		}
		rows = append(rows, fmt.Sprintf("total\tkeys_with_diff=%d\tacked_lost=%d\tunacked_visible=%d", keys, lost, ghost))
		writeRows(filepath.Join(t.dir, "readback-"+t.kind+".txt"), rows)
		fmt.Printf("%s\treadback\tacked_lost=%d unacked_visible=%d keys=%d\n", t.kind, lost, ghost, len(names))
		for _, r := range rows {
			if strings.Contains(r, "lost=0\tghost=0") {
				continue
			}
			fmt.Printf("%s\treadback\t%s\n", t.kind, r)
		}
	}
}

// ----- 读数小工具 -----

// seqOrder 报一个 key 上序号的推进节奏：mini-Raft 看 index，etcd 看 revision。
// 两侧都是每发写占一格，这一格就落在 docs/8 第一节的「顺序」那行。
func seqOrder(t *target, phase, k string, ws []Rec) string {
	if len(ws) == 0 {
		return ""
	}
	first, last := ws[0], ws[len(ws)-1]
	step := 0.0
	if len(ws) > 1 {
		if t.kind == targetMini {
			step = float64(last.Index-first.Index) / float64(len(ws)-1)
			return fmt.Sprintf("index_first=%d index_last=%d index_step=%.2f", first.Index, last.Index, step)
		}
		step = float64(last.Revision-first.Revision) / float64(len(ws)-1)
		return fmt.Sprintf("revision_first=%d revision_last=%d revision_step=%.2f", first.Revision, last.Revision, step)
	}
	if t.kind == targetMini {
		return fmt.Sprintf("index_first=%d index_last=%d", first.Index, last.Index)
	}
	return fmt.Sprintf("revision_first=%d revision_last=%d", first.Revision, last.Revision)
}

func monotonic(recs []Rec) (violations int, first, last int64) {
	prev := int64(-1)
	for _, r := range recs {
		if r.ValueSeq < 0 {
			continue
		}
		if first == 0 && prev < 0 {
			first = r.ValueSeq
		}
		if prev >= 0 && r.ValueSeq < prev {
			violations++
		}
		prev = r.ValueSeq
		last = r.ValueSeq
	}
	return violations, first, last
}

func joinFirst(recs []Rec, n int) string {
	if len(recs) < n {
		n = len(recs)
	}
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = strconv.FormatInt(recs[i].ValueSeq, 10)
	}
	return strings.Join(parts, ",")
}

func countStatus(recs []Rec, status string) int {
	n := 0
	for _, r := range recs {
		if r.Status == status {
			n++
		}
	}
	return n
}

func countGT(seqs []int64, v int64) int {
	n := 0
	for _, s := range seqs {
		if s > v {
			n++
		}
	}
	return n
}

// jsonKeys 把响应体的顶层键按字典序拼一行：键集合的差异要能指回一条设计决定（docs/8 第一节）。
func jsonKeys(body string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return "unparsed"
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func writeRows(path string, rows []string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	for _, r := range rows {
		_, _ = f.WriteString(r + "\n")
	}
}

func trim(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// emit 出一档的三行读数：写侧统计、读侧统计、这档特有的额外字段。
func emit(t *target, phase string, ws, rs []Rec, extra string) {
	w := summarize(phase+"/put", ws)
	r := summarize(phase+"/get", rs)
	w.Extra = extra
	rows := []string{fmt.Sprintf("%s\t%s", t.kind, w.String()), fmt.Sprintf("%s\t%s", t.kind, r.String())}
	for _, row := range rows {
		fmt.Println(row)
	}
}
