package main

// 冻住主节点与隔离多数派两档（docs/8 第五节）：驱动写进时刻文件之后停进程或切端口，
// 客户端持续打同一串操作，恢复之后再问一遍谁是主、任期涨了几次。
// 读数落 freeze-<kind>.txt 与 partition-<kind>.txt，字段口径与其余档位一致。

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 两档的单发超时：被冻住或切断的进程不会回话，等 2 秒会把采样点挤没。
// partition 档每轮要发三发，超时压到 300ms 才能在一段固定时长里留下足够多的样本。
const (
	freezeTimeout    = 500 * time.Millisecond
	partitionTimeout = 300 * time.Millisecond
)

// runFreeze 两侧并行：客户端先问出主节点，驱动把两侧主节点 SIGSTOP，客户端照常打它，
// 到点 SIGCONT，客户端再问一遍主节点与任期，并看旧主节点还认不认自己。
func runFreeze(mini, etcdT *target, dur, stopFor time.Duration, stamp string) {
	if stamp == "" {
		die("freeze 档要 -freezestamp 指向驱动写的时刻文件")
	}
	var wg sync.WaitGroup
	for _, t := range []*target{mini, etcdT} {
		wg.Add(1)
		go func(t *target) {
			defer wg.Done()
			t.freezeRun(dur, stopFor, stamp)
		}(t)
	}
	wg.Wait()
}

func (t *target) freezeRun(dur, stopFor time.Duration, stamp string) {
	li, baseTerm := t.leaderIndex()
	ep := t.addr(li)
	t.client.Timeout = freezeTimeout

	var froze atomic.Int64
	stopPoll := make(chan struct{})
	go pollKill(stamp, &froze, stopPoll)

	deadline := time.Now().Add(dur)
	seq := int64(0)
	var ws, reads []Rec
	for time.Now().Before(deadline) {
		seq++
		ws = append(ws, t.write("freeze", ep, key("freeze"), seqValue(seq, "freeze"), seq))
		reads = append(reads, t.read("freeze", ep, key("freeze"), false))
		reads = append(reads, t.read("freeze", ep, key("freeze"), true))
		time.Sleep(50 * time.Millisecond)
	}
	close(stopPoll)

	fms := froze.Load()
	if fms == 0 {
		rows := []string{fmt.Sprintf("%s\tfreeze\t没读到冻主时刻，本轮作废", t.kind)}
		writeRows(filepath.Join(t.dir, "freeze-"+t.kind+".txt"), rows)
		fmt.Println(rows[0])
		return
	}
	thaw := fms + stopFor.Milliseconds()

	served, failed, firstFail := 0, 0, int64(0)
	for _, r := range append(append([]Rec{}, ws...), reads...) {
		// 按发出时刻判窗口：冻住之前发出、解冻之后才回话的那几条不算冻住期内被服务。
		if r.TS < fms || r.TS >= thaw {
			continue
		}
		if r.Status == "200" {
			served++
			continue
		}
		failed++
		if firstFail == 0 && r.Done >= fms {
			firstFail = r.Done
		}
	}

	nl, term := t.probeLeader()
	still, oldTerm := t.claimsLeadership(ep, li)
	post := t.read("freeze", ep, key("freeze"), false)

	extra := fmt.Sprintf("freeze_ms=%d stop_ms=%d leader_before=%s base_term=%d served_in_window=%d failed_in_window=%d",
		fms, stopFor.Milliseconds(), nodeName(li), baseTerm, served, failed)
	if firstFail > 0 {
		extra += fmt.Sprintf(" first_fail_after_freeze_ms=%d", firstFail-fms)
	}
	after := fmt.Sprintf("leader_now=%s term_now=%d term_delta=%s old_node_still_claims=%t old_term=%d post_thaw_linear=%s/%s",
		nodeName(nl), term, deltaText(term, baseTerm), still, oldTerm, post.Status, post.Code)

	rows := []string{
		fmt.Sprintf("%s\t%s", t.kind, summarize("freeze/put", ws).String()),
		fmt.Sprintf("%s\t%s", t.kind, summarize("freeze/get", reads).String()),
		fmt.Sprintf("%s\tfreeze/during\t%s", t.kind, extra),
		fmt.Sprintf("%s\tfreeze/after_thaw\t%s", t.kind, after),
	}
	writeRows(filepath.Join(t.dir, "freeze-"+t.kind+".txt"), rows)
	for _, r := range rows {
		fmt.Println(r)
	}
}

// runPartition 两侧并行：驱动按端口切断两侧主节点与其余成员之间的流量，
// 客户端往被隔离的主节点写、同时盯着多数派那一侧，恢复之后再看任期涨到哪。
func runPartition(mini, etcdT *target, dur, isoFor time.Duration, stamp string) {
	if stamp == "" {
		die("partition 档要 -isostamp 指向驱动写的时刻文件")
	}
	var wg sync.WaitGroup
	for _, t := range []*target{mini, etcdT} {
		wg.Add(1)
		go func(t *target) {
			defer wg.Done()
			t.partitionRun(dur, isoFor, stamp)
		}(t)
	}
	wg.Wait()
}

// view 是某一刻从多数派侧问到的角色表：Leader 为 node 下标，OK 为 false 表示这一问没有可用回答。
type view struct {
	Done   int64
	Leader int
	Term   uint64
	OK     bool
}

func (t *target) partitionRun(dur, isoFor time.Duration, stamp string) {
	li, baseTerm := t.leaderIndex()
	ep := t.addr(li)
	t.client.Timeout = partitionTimeout

	var isoStart atomic.Int64
	stopPoll := make(chan struct{})
	go pollKill(stamp, &isoStart, stopPoll)

	seq := int64(0)
	var ws, linearReads, localReads []Rec
	var views []view
	var isoTerms []view
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		seq++
		ws = append(ws, t.write("partition", ep, key("partition"), seqValue(seq, "partition"), seq))
		linearReads = append(linearReads, t.read("partition", ep, key("partition"), false))
		localReads = append(localReads, t.read("partition", ep, key("partition"), true))
		if nl, term, err := t.leaderExcluding(li); err == nil {
			views = append(views, view{Done: time.Now().UnixMilli(), Leader: nl, Term: term, OK: true})
		} else {
			views = append(views, view{Done: time.Now().UnixMilli(), Leader: -1})
		}
		claims, term := t.claimsLeadership(ep, li)
		if claims {
			isoTerms = append(isoTerms, view{Done: time.Now().UnixMilli(), Leader: li, Term: term, OK: true})
		} else {
			isoTerms = append(isoTerms, view{Done: time.Now().UnixMilli(), Leader: -1, Term: term})
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(stopPoll)

	ims := isoStart.Load()
	if ims == 0 {
		rows := []string{fmt.Sprintf("%s\tpartition\t没读到隔离时刻，本轮作废", t.kind)}
		writeRows(filepath.Join(t.dir, "partition-"+t.kind+".txt"), rows)
		fmt.Println(rows[0])
		return
	}
	isoEnd := ims + isoFor.Milliseconds()

	var writeOK, writeTimeout, linearOK, localOK int
	for _, r := range ws {
		if r.TS < ims || r.TS >= isoEnd {
			continue
		}
		switch r.Status {
		case "200":
			writeOK++
		case "timeout":
			writeTimeout++
		}
	}
	for _, r := range linearReads {
		if r.TS >= ims && r.TS < isoEnd && r.Status == "200" {
			linearOK++
		}
	}
	for _, r := range localReads {
		if r.TS >= ims && r.TS < isoEnd && r.Status == "200" {
			localOK++
		}
	}

	firstMajority, electedMS, leaderAtEnd, termAtEnd := 0, int64(-1), -1, uint64(0)
	var lastOK *view
	changes := 0
	for i := range views {
		v := &views[i]
		if !v.OK {
			continue
		}
		if v.Done >= ims && firstMajority == 0 {
			firstMajority = v.Leader + 1
			electedMS = v.Done - ims
		}
		if v.Done < isoEnd {
			leaderAtEnd, termAtEnd = v.Leader, v.Term
		} else if lastOK != nil && (v.Leader != lastOK.Leader || v.Term != lastOK.Term) {
			changes++
		}
		lastOK = v
	}
	finalLeader, finalTerm := -1, uint64(0)
	if lastOK != nil {
		finalLeader, finalTerm = lastOK.Leader, lastOK.Term
	}
	// 隔离侧自己的读数：少数派上的任期有没有被一轮轮竞选推高、临到解冻时它还认不认自己是主。
	// 末尾留 350ms 不采：规则拆除与客户端记下的隔离时刻之间有抖动，贴着边界的样本会把解冻后的状态算进来。
	isoEndGuard := isoEnd - 350
	isoClaimedAtEnd, isoTermMax := false, uint64(0)
	isoClaimSamples, isoClaimTrue := 0, 0
	for _, v := range isoTerms {
		if v.Done < ims || v.Done >= isoEndGuard {
			continue
		}
		isoClaimSamples++
		isoClaimedAtEnd = v.Leader >= 0
		if isoClaimedAtEnd {
			isoClaimTrue++
		}
		if v.Term > isoTermMax {
			isoTermMax = v.Term
		}
	}
	still, isoTerm := t.claimsLeadership(ep, li)

	head := fmt.Sprintf("iso_ms=%d iso_dur=%d leader_before=%s base_term=%d",
		ims, isoFor.Milliseconds(), nodeName(li), baseTerm)
	during := fmt.Sprintf("write_in_window ok=%d timeout=%d linear_read_ok=%d local_read_ok=%d",
		writeOK, writeTimeout, linearOK, localOK)
	majority := fmt.Sprintf("first_majority_leader=%s elected_ms=%d leader_at_iso_end=%s term_at_iso_end=%d",
		nodeName(firstMajority-1), electedMS, nodeName(leaderAtEnd), termAtEnd)
	after := fmt.Sprintf("leader_final=%s term_final=%d term_delta=%s leader_changes_after_heal=%d isolated_claimed_at_end=%t isolated_claim_samples=%d isolated_claim_true=%d isolated_term_max=%d isolated_term_after=%d isolated_still_claims=%t",
		nodeName(finalLeader), finalTerm, deltaText(finalTerm, baseTerm), changes, isoClaimedAtEnd, isoClaimSamples, isoClaimTrue, isoTermMax, isoTerm, still)

	rows := []string{
		fmt.Sprintf("%s\t%s", t.kind, summarize("partition/put", ws).String()),
		fmt.Sprintf("%s\t%s", t.kind, summarize("partition/get", append(append([]Rec{}, linearReads...), localReads...)).String()),
		fmt.Sprintf("%s\tpartition/state\t%s", t.kind, head),
		fmt.Sprintf("%s\tpartition/during\t%s", t.kind, during),
		fmt.Sprintf("%s\tpartition/majority\t%s", t.kind, majority),
		fmt.Sprintf("%s\tpartition/after_heal\t%s", t.kind, after),
	}
	writeRows(filepath.Join(t.dir, "partition-"+t.kind+".txt"), rows)
	for _, r := range rows {
		fmt.Println(r)
	}
}

// runPreVote 两侧并行：驱动把一台非主节点切出去 6 秒，客户端只盯这台自己的任期。
// 没有 Pre-Vote 的实现每个选举周期都把任期加一；有 Pre-Vote 的拿不到多数派就不加。
// 解冻后再看集群是否因为这台的任期更高而换届，这一格就是 Pre-Vote 那一行归因的读数。
func runPreVote(mini, etcdT *target, dur, isoFor time.Duration, stamp string) {
	if stamp == "" {
		die("prevote 档要 -faultstamp 指向驱动写的时刻文件")
	}
	var wg sync.WaitGroup
	for _, t := range []*target{mini, etcdT} {
		wg.Add(1)
		go func(t *target) {
			defer wg.Done()
			t.prevoteRun(dur, isoFor, stamp)
		}(t)
	}
	wg.Wait()
}

func (t *target) prevoteRun(dur, isoFor time.Duration, stamp string) {
	li, _ := t.leaderIndex()
	ni := follower(li, len(t.addrs))
	ep := t.addr(ni)
	t.client.Timeout = partitionTimeout

	var isoStart atomic.Int64
	stopPoll := make(chan struct{})
	go pollKill(stamp, &isoStart, stopPoll)

	type sample struct {
		Done   int64
		Term   uint64
		Claims bool
	}
	var samples []sample
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		claims, term := t.claimsLeadership(ep, ni)
		samples = append(samples, sample{Done: time.Now().UnixMilli(), Term: term, Claims: claims})
		time.Sleep(150 * time.Millisecond)
	}
	close(stopPoll)

	ims := isoStart.Load()
	if ims == 0 || len(samples) == 0 {
		rows := []string{fmt.Sprintf("%s\tprevote\t没读到隔离时刻，本轮作废", t.kind)}
		writeRows(filepath.Join(t.dir, "prevote-"+t.kind+".txt"), rows)
		fmt.Println(rows[0])
		return
	}
	isoEnd := ims + isoFor.Milliseconds()

	baseTerm, termMax, termAfter := samples[0].Term, uint64(0), uint64(0)
	inWindow, afterWindow, afterFailed := 0, 0, 0
	for _, s := range samples {
		if s.Done >= ims && s.Done < isoEnd {
			inWindow++
			if s.Term > termMax {
				termMax = s.Term
			}
		}
		if s.Done >= isoEnd {
			afterWindow++
			if s.Term == 0 {
				afterFailed++
				continue
			}
			termAfter = s.Term
		}
	}
	if termMax < baseTerm {
		termMax = baseTerm
	}
	nl, finalTerm := t.probeLeader()

	head := fmt.Sprintf("node=%s base_term=%d leader_before=%s leader_term_before=%d",
		nodeName(ni), baseTerm, nodeName(li), baseTerm)
	during := fmt.Sprintf("samples=%d term_max=%d term_delta=%s", inWindow, termMax, deltaText(termMax, baseTerm))
	after := fmt.Sprintf("term_after=%s samples_after=%d failed_after=%d leader_after=%s leader_term_after=%d leader_changed=%t",
		deltaText(termAfter, baseTerm), afterWindow, afterFailed, nodeName(nl), finalTerm, nl >= 0 && nl != li)

	rows := []string{
		fmt.Sprintf("%s\tprevote/state\t%s", t.kind, head),
		fmt.Sprintf("%s\tprevote/during\t%s", t.kind, during),
		fmt.Sprintf("%s\tprevote/after_heal\t%s", t.kind, after),
	}
	writeRows(filepath.Join(t.dir, "prevote-"+t.kind+".txt"), rows)
	for _, r := range rows {
		fmt.Println(r)
	}
}

// nodeName 把 0 起的下标写成 node-1..node-5；没有可用的主时写 none。
func nodeName(i int) string {
	if i < 0 {
		return "none"
	}
	return "node-" + strconv.Itoa(i+1)
}

// deltaText 只在两个任期可比时才做减法；问不到终值时写 none，不让 uint64 回绕成一个天文数字。
func deltaText(after, before uint64) string {
	if after == 0 || before == 0 || after < before {
		return "none"
	}
	return strconv.FormatUint(after-before, 10)
}

// leaderExcluding 问多数派那一侧的主节点：跳过被隔离的那台，返回第一个自认为知道主的回答。
func (t *target) leaderExcluding(skip int) (int, uint64, error) {
	var lastErr error
	for i, a := range t.addrs {
		if i == skip {
			continue
		}
		li, term, err := t.leader(a)
		if err != nil {
			lastErr = err
			continue
		}
		if li != skip {
			return li, term, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("多数派侧没有报出主节点")
	}
	return -1, 0, lastErr
}

// claimsLeadership 读一台节点自己的看法：mini-Raft 看 membership 的 leader_id，
// etcd 看 status 里 leader 是否等于自己的 member_id。任期一并取回。
func (t *target) claimsLeadership(addr string, idx int) (bool, uint64) {
	if t.kind == targetMini {
		res := t.getRaw(addr, "/raft/membership")
		var body struct {
			LeaderID string `json:"leader_id"`
			Term     uint64 `json:"term"`
		}
		if json.Unmarshal([]byte(res.Body), &body) != nil {
			return false, 0
		}
		return idIndex(body.LeaderID) == idx, body.Term
	}
	res := t.post(addr, "/v3/maintenance/status", []byte(`{}`))
	var body struct {
		Leader   string `json:"leader"`
		MemberID string `json:"member_id"`
		RaftTerm string `json:"raftTerm"`
	}
	if json.Unmarshal([]byte(res.Body), &body) != nil {
		return false, 0
	}
	term, _ := strconv.ParseUint(body.RaftTerm, 10, 64)
	return body.Leader != "" && body.Leader != "0" && body.Leader == body.MemberID, term
}
