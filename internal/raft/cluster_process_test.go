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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// processCluster 是 M12 的进程夹具：测试自己 go build 出入口二进制，起 size 个真进程，
// 端口从临时监听要、数据目录归 t.TempDir（3.2 §3 的进程用例行）。
// 本文件是唯一读墙上时间的地方：真实时序要量的就是它，D16 的零 sleep 只约束单进程用例。
type processCluster struct {
	t     *testing.T
	bin   string
	root  string
	ids   []string
	addr  map[string]string
	peers string
	cmd   map[string]*exec.Cmd

	// flags 是给每台机器都追加的启动参数：快照档的开关只在这里能换（5.1 §6）。
	flags []string
	// env 是每台机器额外的环境变量：故障注入的停点只挂在被注入的那一台上。
	env map[string][]string
}

func buildNodeBinary(t *testing.T) string {
	t.Helper()
	// 测试的工作目录就是包目录，往上两级是模块根，cmd/miniraft-node 在那下面。
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("module root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "cmd", "miniraft-node")); err != nil {
		t.Fatalf("entry package under %s: %v", root, err)
	}
	bin := filepath.Join(t.TempDir(), "miniraft-node")
	build := exec.Command("go", "build", "-o", bin, "./cmd/miniraft-node")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the node entry: %v: %s", err, out)
	}
	return bin
}

func newProcessCluster(t *testing.T, size int) *processCluster {
	t.Helper()
	return newProcessClusterFlags(t, size, nil)
}

// newProcessClusterFlags 起 size 个进程，flags 追加在每台机器的命令行上。
func newProcessClusterFlags(t *testing.T, size int, flags []string) *processCluster {
	t.Helper()
	return newProcessClusterAtRoot(t, size, flags, t.TempDir())
}

// newProcessClusterAtRoot 把数据目录放到调用方指定的位置：磁盘故障用例要把节点放进小容量的 tmpfs。
func newProcessClusterAtRoot(t *testing.T, size int, flags []string, root string) *processCluster {
	t.Helper()
	p := &processCluster{
		flags: flags,
		t:     t,
		bin:   buildNodeBinary(t),
		root:  root,
		cmd:   map[string]*exec.Cmd{},
		addr:  map[string]string{},
		env:   map[string][]string{},
	}
	var items []string
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node-%d", i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("probe a free port for %s: %v", id, err)
		}
		p.addr[id] = ln.Addr().String()
		if err := ln.Close(); err != nil {
			t.Fatalf("release the probe listener for %s: %v", id, err)
		}
		p.ids = append(p.ids, id)
		items = append(items, id+"="+p.addr[id])
	}
	p.peers = strings.Join(items, ",")

	p.start()
	t.Cleanup(func() { p.stopAll() })
	return p
}

// start 起全部节点并逐个探活；数据目录留着，所以 stopAll 之后再调一次就是重启。
func (p *processCluster) start() {
	p.t.Helper()
	for _, id := range p.ids {
		p.startOne(id)
	}
}

// startOne 只起一台并等它就维：被 kill9 停下的节点按同一份数据目录重起来走的就是这条路。
func (p *processCluster) startOne(id string) {
	p.t.Helper()
	p.spawnOne(id)
	p.waitReady(id)
}

// spawnOne 起一台但不等就维：回放中途被杀的用例要自己挑杀的时刻，等就维就错过了窗口。
func (p *processCluster) spawnOne(id string) {
	p.t.Helper()
	if c := p.cmd[id]; c != nil && c.ProcessState == nil {
		p.t.Fatalf("node %s is already running in this fixture", id)
	}
	dir := filepath.Join(p.root, id)
	args := []string{"-node-id", id, "-peers", p.peers, "-data-dir", dir, "-log-level", "debug"}
	args = append(args, p.flags...)
	cmd := exec.Command(p.bin, args...)
	if extra := p.env[id]; len(extra) > 0 {
		cmd.Env = append(os.Environ(), extra...)
	}
	// 节点的日志都在数据目录的文件里，stdout 只是兜底：留着管道会让 Cleanup 等不到 EOF。
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		p.t.Fatalf("start %s: %v", id, err)
	}
	p.cmd[id] = cmd
}

func (p *processCluster) url(id, path string) string {
	return "http://" + p.addr[id] + path
}

// waitReady 只认 handler 的答复：404 与 200 都是 /get 起来了，连不上与 5xx 都算没就绪。
// 探活走本地档：这一档在任意节点都回 200 或 404，503 只可能来自缺省档（那时这台不是 Leader，
// 不能当成没就绪）（4.1 §4、D24）。
func (p *processCluster) waitReady(id string) {
	p.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, _, err := p.get(id, "__probe__")
		if err == nil && (status == http.StatusNotFound || status == http.StatusOK) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Fatalf("node %s never answered /get on %s: %s", id, p.addr[id], p.logTail(id, 8))
}

// get 走显式的本地旧数据档：任意节点都回 200（key 不存在是 404），读到的可能是旧值。
// D24 之后 raft 侧所有只是想读回一个值的调用点都要显式带 consistency=local，
// 缺省档是线性一致读，在 Follower 上只会拿到 503 not_leader。
func (p *processCluster) get(id, key string) (int, []byte, error) {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(p.url(id, "/get?key="+key+"&consistency=local"))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, err
}

// getLinear 走缺省的线性一致档：只有 Leader 服务，其余节点回 503 not_leader，
// 等不到状态机应用或等待超时是 500 canceled（4.1 §4、D24）。
func (p *processCluster) getLinear(id, key string) (int, []byte, error) {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(p.url(id, "/get?key="+key))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, err
}

// put 打一次客户端写；连不上与超时都只回错误，调用方把它当成「这台不是 Leader」。
func (p *processCluster) put(id, key, value string) (int, map[string]any, error) {
	body := fmt.Sprintf(`{"key":%q,"value":%q}`, key, value)
	resp, err := (&http.Client{Timeout: 3 * time.Second}).
		Post(p.url(id, "/put"), "application/json", strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return resp.StatusCode, nil, fmt.Errorf("%s: body %s", err, firstBytes(raw, 200))
	}
	return resp.StatusCode, out, nil
}

// writeThrough 依次敲每台机器，直到有一台把这条写提交掉：acked 的那台就是当时的 Leader。
// 预算给到 20 秒，覆盖一轮选举窗口加几次 term 递增（D16 的 ±一个 tick 也在这里面）。
func (p *processCluster) writeThrough(key, value string) (string, map[string]any) {
	p.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range p.ids {
			if p.cmd[id] == nil || p.cmd[id].ProcessState != nil {
				continue
			}
			status, body, err := p.put(id, key, value)
			if err != nil || status != http.StatusOK {
				continue
			}
			return id, body
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Fatalf("no node acked the write %s=%s within 20s", key, value)
	return "", nil
}

// kill9 不给收尾机会：进程直接没了，之后的重选主与恢复读的就是盘上的样子。
func (p *processCluster) kill9(id string) {
	p.t.Helper()
	cmd := p.cmd[id]
	if cmd == nil || cmd.Process == nil {
		p.t.Fatalf("node %s is not running in this fixture", id)
	}
	// 整个进程组一起杀：节点可能被 strace 之类的父进程包着，只杀父进程会把节点留成孤儿。
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		if err := cmd.Process.Kill(); err != nil {
			p.t.Fatalf("kill -9 %s: %v", id, err)
		}
	}
	state, err := cmd.Process.Wait()
	if err != nil {
		p.t.Errorf("reap %s: %v", id, err)
	}
	// ProcessState 要落到 cmd 上：写入侧与重起侧都据此判这台还在不在。
	cmd.ProcessState = state
}

// stopAll 发 SIGTERM 并等每个进程自己退出；退出码来自真进程，不是夹具推断的。
func (p *processCluster) stopAll() map[string]int {
	codes := map[string]int{}
	for _, id := range p.ids {
		cmd := p.cmd[id]
		if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
			delete(p.cmd, id)
			continue
		}
		// 整个进程组一起发信号：节点可能被 strace 之类的父进程包着，
		// 只发给父进程会留下一个还占着管道的子进程，Wait 就等不到头。
		sigErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if sigErr != nil {
			sigErr = cmd.Process.Signal(syscall.SIGTERM)
		}
		if sigErr != nil && !errors.Is(sigErr, os.ErrProcessDone) && !errors.Is(sigErr, syscall.ESRCH) {
			p.t.Errorf("signal %s: %v", id, sigErr)
			continue
		}
		// 收尾窗口本身 5 秒，多给 10 秒等 fsync；超时就 kill -9，别让用例挂在这里。
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
			p.t.Errorf("node %s did not exit on SIGTERM within 10s", id)
		}
		codes[id] = cmd.ProcessState.ExitCode()
		delete(p.cmd, id)
	}
	return codes
}

// logPath 是这台进程写日志的文件：补发快照有没有落到它头上，读的就是这份原文。
func (p *processCluster) logPath(id string) string {
	return filepath.Join(p.root, id, "log", NodeLogFileName)
}

// postRaw 发一次请求体是原始字节的 POST：补发快照那条路径不带 JSON（D32）。
func (p *processCluster) postRaw(id, path, body string) (int, []byte, error) {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).
		Post(p.url(id, path), "application/octet-stream", strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, err
}

// walFirstIndex 读盘上 raft 日志的第一条 index，也就是这台机器此刻的日志基线。
// 日志被删空时没有帧可读，回 0。
func (p *processCluster) walFirstIndex(id string) uint64 {
	p.t.Helper()
	paths, err := filepath.Glob(filepath.Join(p.root, id, RaftDirName, WALDirName, "*.wal"))
	if err != nil {
		p.t.Fatalf("glob the wal of %s: %v", id, err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			p.t.Fatalf("read %s: %v", path, err)
		}
		if len(raw) < kvstore.FrameHeaderSize {
			continue
		}
		size, checksum, err := kvstore.DecodeFrame(raw[:kvstore.FrameHeaderSize], 0)
		if err != nil {
			p.t.Fatalf("frame header at the head of %s: %v", path, err)
		}
		end := kvstore.FrameHeaderSize + int(size)
		if end > len(raw) {
			continue
		}
		payload := raw[kvstore.FrameHeaderSize:end]
		if err := kvstore.CheckFrame(size, checksum, payload, 0); err != nil {
			p.t.Fatalf("first frame of %s is corrupt: %v", path, err)
		}
		_, index, _, err := decodeWALRecord(payload, 0)
		if err != nil {
			p.t.Fatalf("decode the first record of %s: %v", path, err)
		}
		return index
	}
	return 0
}

// diskSnapshotIndex 读盘上的元数据指针，取 snapshotIndex；没有指针文件就是 0。
func (p *processCluster) diskSnapshotIndex(id string) uint64 {
	p.t.Helper()
	raw, err := os.ReadFile(filepath.Join(p.root, id, RaftDirName, SnapshotDirName, SnapshotPointerName))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		p.t.Fatalf("read the snapshot pointer of %s: %v", id, err)
	}
	var pointer snapshotPointer
	if err := json.Unmarshal(raw, &pointer); err != nil {
		p.t.Fatalf("snapshot pointer of %s: %v: %s", id, err, raw)
	}
	return pointer.SnapshotIndex
}

// readExitCode 跑一个注定起不来的进程并取它的退出码：stderr 一并留着，那是 3.1 §4 说的
// 「起不来的原因同时写日志与 stderr」里唯一能在这个阶段看见的一半。
func readExitCode(t *testing.T, bin string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatalf("run %s: %v", bin, err)
	}
	if exit != nil {
		return exit.ExitCode(), out.String()
	}
	return 0, out.String()
}

// logTail 取节点日志的末尾几行，只给失败信息用。
func (p *processCluster) logTail(id string, lines int) string {
	path := filepath.Join(p.root, id, "log", NodeLogFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("no log at %s: %v", path, err)
	}
	all := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

// 5 个真进程跑通一次写，其余 4 台 /get 读到同一个值；SIGTERM 收尾退出码 0，
// 重启之后已确认的写照样读得回（M12 收口判据与 3.2 §3 的进程用例行）。
func TestFiveProcessesWriteAndReadBack(t *testing.T) {
	if testing.Short() {
		t.Skip("进程用例要起 5 个真进程并等选举窗口，-short 下跳过")
	}
	p := newProcessCluster(t, 5)

	leader, reply := p.writeThrough("smoke", "kept")
	t.Logf("acked by %s, reply = %v", leader, reply)
	if got := reply["code"]; got == "not_leader" {
		t.Fatalf("writeThrough accepted a %v body, want the leader's 200", got)
	}

	// 复制与 apply 靠心跳递出去，等一下再读是真实时序，不是单进程用例里的 Tick 计数。
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range p.ids {
		for {
			status, raw, err := p.get(id, "smoke")
			if err == nil && status == http.StatusOK && strings.Contains(string(raw), `"value":"kept"`) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never read the acked write (last %d %s): %s",
					id, status, firstBytes(raw, 200), p.logTail(id, 8))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	codes := p.stopAll()
	if len(codes) != len(p.ids) {
		t.Fatalf("SIGTERM covered %d of %d nodes", len(codes), len(p.ids))
	}
	for id, code := range codes {
		if code != 0 {
			t.Fatalf("node %s exited %d on SIGTERM, want 0: %s", id, code, p.logTail(id, 5))
		}
	}

	// 同一批数据目录重启：已确认的写必须还在，这是 Close 契约里 Flush 与落 state 那两步。
	p.start()
	for _, id := range p.ids {
		status, raw, err := p.get(id, "smoke")
		if err != nil || status != http.StatusOK || !strings.Contains(string(raw), `"value":"kept"`) {
			t.Fatalf("%s after the restart = %d %s (err %v), want the acked value back",
				id, status, firstBytes(raw, 200), err)
		}
	}
}

// 杀掉 Leader 之后其余 4 台重选主：耗时与 term 变化都记进输出，term 只断言「更大」（D22：本轮不钉数字）。
func TestKillLeaderReElectsAndKeepsTheValue(t *testing.T) {
	if testing.Short() {
		t.Skip("进程用例要等真实选举窗口，-short 下跳过")
	}
	p := newProcessCluster(t, 5)

	leader, first := p.writeThrough("before-kill", "v1")
	termBefore := first["term"]
	p.kill9(leader)

	start := time.Now()
	next, second := p.writeThrough("after-kill", "v2")
	elapsed := time.Since(start)

	termAfter := second["term"]
	if next == leader {
		t.Fatalf("the killed node %s came back and acked a write", leader)
	}
	if termAfter == termBefore {
		t.Fatalf("term stayed at %v after killing the leader of that term", termBefore)
	}
	if toFloat(termAfter) < toFloat(termBefore) {
		t.Fatalf("term went backwards: %v then %v", termBefore, termAfter)
	}
	t.Logf("killed=%s elected=%s elected_in_ms=%.1f term %v -> %v",
		leader, next, float64(elapsed.Microseconds())/1000, termBefore, termAfter)

	// 新 Leader 的第一条心跳把旧写带到其余节点；杀进程之前的写不能凭空消失。
	for _, id := range p.ids {
		if id == leader {
			continue
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			status, raw, err := p.get(id, "before-kill")
			if err == nil && status == http.StatusOK && strings.Contains(string(raw), `"value":"v1"`) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s lost the write made before the kill (%d %s, err %v): %s",
					id, status, firstBytes(raw, 120), err, p.logTail(id, 8))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// 一台节点被停到 Leader 删掉日志前缀，重新起来后 Leader 给它补发一份快照，
// 它把状态机补齐并读得到快照点之后的所有写，两台的日志基线落在同一格（5.1 §7 接收快照、5.2 §3 的进程行）。
func TestStoppedMemberCatchesUpWithAnInstalledSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("进程用例要起 5 个真进程，-short 下跳过")
	}
	// 落后一条就豁免：停掉的那台卡不住删除边界。
	p := newProcessClusterFlags(t, 5, []string{"-snapshot-exempt-entries", "1"})

	leader, _ := p.writeThrough("warm", "v0")
	victim := ""
	for _, id := range p.ids {
		if id != leader {
			victim = id
			break
		}
	}
	p.kill9(victim)

	before := []string{"warm", "k1", "k2", "k3"}
	for _, key := range before[1:] {
		p.writeThrough(key, "v-"+key)
	}

	status, raw, err := p.postRaw(leader, "/raft/snapshot", "")
	if err != nil {
		t.Fatalf("trigger a snapshot on %s: %v", leader, err)
	}
	if status != http.StatusOK {
		t.Fatalf("snapshot on the leader = %d: %s", status, firstBytes(raw, 200))
	}
	var reply map[string]any
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("snapshot reply is not one JSON object: %s", firstBytes(raw, 200))
	}
	snapIndex := toFloat(reply["snapshot_index"])
	if snapIndex < 1 || reply["ok"] != true {
		t.Fatalf("snapshot reply = %v, want ok with a snapshot index", reply)
	}
	if got := p.diskSnapshotIndex(leader); uint64(snapIndex) != got {
		t.Fatalf("the reply says %v but the pointer on disk says %d", reply["snapshot_index"], got)
	}

	// 快照点之后的写：补发之后还得靠日志把这两条补上。
	after := []string{"k4", "k5"}
	for _, key := range after {
		p.writeThrough(key, "v-"+key)
	}
	if got := p.walFirstIndex(leader); got != uint64(snapIndex)+1 {
		t.Fatalf("the leader log starts at %d, want the snapshot point %d plus one", got, uint64(snapIndex))
	}

	p.startOne(victim)

	written := map[string]string{"warm": "v0"}
	for _, key := range append(before[1:], after...) {
		written[key] = "v-" + key
	}
	for key, want := range written {
		deadline := time.Now().Add(30 * time.Second)
		for {
			status, raw, err := p.get(victim, key)
			if err == nil && status == http.StatusOK && strings.Contains(string(raw), `"value":"`+want+`"`) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never read back %s after the catch-up (last %d %s, err %v): %s",
					victim, key, status, firstBytes(raw, 120), err, p.logTail(victim, 10))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	if !fileContains(p.logPath(victim), "snapshot installed") {
		t.Fatalf("%s never logged snapshot installed: %s", victim, p.logTail(victim, 15))
	}
	if got := p.diskSnapshotIndex(victim); uint64(snapIndex) != got {
		t.Fatalf("the installed member points at %d, want the leader's snapshot index %d", got, uint64(snapIndex))
	}
	if got := p.walFirstIndex(victim); got != uint64(snapIndex)+1 {
		t.Fatalf("the installed member log starts at %d, want the same baseline as the leader (%d)",
			got, uint64(snapIndex)+1)
	}
}

// 重选主窗口里的读结果：kill -9 之前起 20 条在途写，窗口内对旧 Leader 与各节点持续发缺省档的
// /get，逐条记录状态码、值与读索引（4.2 的 cluster_process_test.go 行、T30.4）。
// 判据有四条：结果只有 200、503 not_leader、500 canceled 三种；
// 出现 200 时读索引不小于那条写的日志 index，读到的值就是读索引之前最后一条写的值；
// 窗口里一次都不出现「200 且值旧于最后一条已确认写」；
// 本地档在在途写还没提交时读到的是 kill 前那条已确认写的旧值。
func TestReelectionWindowLinearReadResults(t *testing.T) {
	if testing.Short() {
		t.Skip("进程用例要起 5 个真进程并等重选主窗口，-short 下跳过")
	}
	p := newProcessCluster(t, 5)

	const key = "window"
	const confirmedValue = "confirmed-v0"
	leader, acked := p.writeThrough(key, confirmedValue)
	confirmedIndex := toFloat(acked["index"])
	if confirmedIndex <= 0 {
		t.Fatalf("the acked write reported index %v, want a positive one", acked["index"])
	}

	// 20 条在途写：并发打向现任 Leader，值各不相同。
	// 值里垫一段填充是为了把每条的 fsync 拉长，kill 才落在写与写之间：
	// 被杀的那台来不及把新的 commitIndex 告诉跟随者，它们就还停在 kill 前那条已确认写的值上。
	padding := strings.Repeat("x", 256*1024)
	values := make([]string, 20)
	for i := range values {
		values[i] = fmt.Sprintf("inflight-%02d", i) + padding
	}
	inflightTags := map[string]bool{}
	for _, value := range values {
		inflightTags[valueTag(value)] = true
	}

	type putOutcome struct {
		value  string
		status int
		index  float64
		err    error
	}
	outcomes := make(chan putOutcome, len(values))
	for _, value := range values {
		go func(value string) {
			status, body, err := p.put(leader, key, value)
			out := putOutcome{value: value, status: status, err: err}
			if err == nil && status == http.StatusOK {
				out.index = toFloat(body["index"])
			}
			outcomes <- out
		}(value)
	}

	// 在途写刚发出去、还没提交的那一刻，本地档读到的就是 kill 前那条已确认写的值：
	// 这一格是「读到旧值」的装置证据（4.2 的 cluster_process_test.go 行）。
	// 旧 Leader 这时已经应用了那条已确认写，而在途写要等多数派回执才提交。
	inFlightLocal := ""
	{
		status, raw, err := p.get(leader, key)
		if err == nil && status == http.StatusOK {
			var body map[string]any
			if json.Unmarshal(raw, &body) == nil {
				value, _ := body["value"].(string)
				inFlightLocal = valueTag(value)
			}
		}
	}

	// 等第一条在途写被确认：被确认的那条才是新 Leader 会提交的那条，
	// 窗口里的 200 才可能读到在途写的值而不是 kill 前那条已确认写的值。
	// 通道上阻塞着等，确认一回来就动手，不给 kill 留出额外的延迟。
	ackedIndex := map[string]float64{}
	firstAcked := false
	ackDeadline := time.Now().Add(10 * time.Second)
	for !firstAcked {
		select {
		case out := <-outcomes:
			if out.err == nil && out.status == http.StatusOK {
				ackedIndex[valueTag(out.value)] = out.index
				firstAcked = true
			}
		case <-time.After(time.Until(ackDeadline)):
			t.Fatalf("no in-flight write was acked within 10s")
		}
	}

	p.kill9(leader)

	// 窗口：旧 Leader 的地址连不上，其余节点在选出新 Leader 之前回 503，
	// 之后回 200 或者 500。逐条记下来，判据只认这三类。
	type linearSample struct {
		node      string
		tag       string
		readIndex float64
	}
	var linear []linearSample
	statusCounts := map[int]int{}
	codeCounts := map[int]map[string]int{}
	unreachable := 0
	localValues := map[string]int{}

	start := time.Now()
	for time.Since(start) < 3*time.Second {
		// 本地档先读：kill 之后的第一读就是最早的一次旧值观测。
		for _, id := range p.ids {
			if id == leader {
				continue
			}
			status, raw, err := p.get(id, key)
			if err != nil || status != http.StatusOK {
				continue
			}
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				continue
			}
			value, _ := body["value"].(string)
			localValues[valueTag(value)]++
		}
		for _, id := range p.ids {
			status, raw, err := p.getLinear(id, key)
			if err != nil {
				// 旧 Leader 已经被 kill -9，连不上不是 HTTP 状态码，单独记。
				unreachable++
				continue
			}
			statusCounts[status]++
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("%s answered %d with a body that is not JSON: %s", id, status, firstBytes(raw, 200))
				continue
			}
			if code, ok := body["code"].(string); ok && code != "" {
				if codeCounts[status] == nil {
					codeCounts[status] = map[string]int{}
				}
				codeCounts[status][code]++
			}
			if status != http.StatusOK {
				continue
			}
			value, _ := body["value"].(string)
			readIndex, _ := body["read_index"].(float64)
			linear = append(linear, linearSample{node: id, tag: valueTag(value), readIndex: readIndex})
		}
		time.Sleep(40 * time.Millisecond)
	}

	// 断言 1：缺省档的结果只有 200、503 not_leader、500 canceled 三种。
	for status, count := range statusCounts {
		switch status {
		case http.StatusOK:
		case http.StatusServiceUnavailable:
			if codeCounts[status]["not_leader"] != count {
				t.Errorf("the 503 answers carried the codes %v, want only not_leader", codeCounts[status])
			}
		case http.StatusInternalServerError:
			if codeCounts[status]["canceled"] != count {
				t.Errorf("the 500 answers carried the codes %v, want only canceled", codeCounts[status])
			}
		default:
			t.Errorf("the default /get answered %d on %d observations, want only 200, 503 or 500", status, count)
		}
	}

	// 断言 2 与 3：200 的值不能旧于最后一条已确认写，读索引不能小于那条写的日志 index，
	// 而且读到的必须是读索引之前最后一条写的值——读索引落在哪条写上，读到的就得是哪条的值。
	// 日志 index 从服务这次读的节点自己的 raft 日志里取：它是新 Leader，
	// 日志里就是它提交过的那几条写（被杀的 Leader 盘上还留着它没提交出去的那几条）。
	nodeIndexes := map[string]map[string]uint64{}
	if len(linear) == 0 {
		t.Errorf("the window saw no successful linear read at all")
	}
	highest := confirmedIndex
	linearValues := map[string]int{}
	for _, sample := range linear {
		indexes, known := nodeIndexes[sample.node]
		if !known {
			indexes = raftLogValueIndex(p.root, sample.node, key)
			nodeIndexes[sample.node] = indexes
		}
		index, known := indexes[sample.tag]
		linearValues[sample.tag]++
		if !known {
			t.Errorf("the default /get answered the value %q that never entered %s's log", sample.tag, sample.node)
			continue
		}
		if sample.readIndex < float64(index) {
			t.Errorf("read_index %.0f is below the log index %d of the value %q",
				sample.readIndex, index, sample.tag)
		}
		if float64(index) < confirmedIndex {
			t.Errorf("the default /get answered the value %q whose log index %d is older than the last confirmed write at %.0f",
				sample.tag, index, confirmedIndex)
		}
		if float64(index) < highest {
			t.Errorf("the default /get answered the value %q at index %d after the value at index %.0f had been read",
				sample.tag, index, highest)
		}
		if float64(index) > highest {
			highest = float64(index)
		}
		// 读索引之前最后一条写：新 Leader 刚当选、还没提交任何新东西时，读索引仍停在已确认写上，
		// 这时读到的就是那条已确认写的值；一旦读索引越过它，读到的必须是在途写里的某一条。
		latest := uint64(0)
		for _, other := range indexes {
			if other <= uint64(sample.readIndex) && other > latest {
				latest = other
			}
		}
		if index != latest {
			t.Errorf("the default /get answered the value %q at index %d with read_index %.0f, want the write at index %d",
				sample.tag, index, sample.readIndex, latest)
		}
		if !inflightTags[sample.tag] && sample.readIndex > confirmedIndex {
			t.Errorf("the default /get answered the pre-kill confirmed value %q with read_index %.0f, want one of the in-flight writes",
				sample.tag, sample.readIndex)
		}
	}

	// 断言 4：本地档读到过 kill 前那条已确认写的旧值。
	// 在途写刚发出去、还没提交的那一读一定拿得到这个值；kill 之后那几台跟随者还停不停在旧值上，
	// 取决于它们学到新 commitIndex 的时刻，这一格只报数不判成败。
	if inFlightLocal != valueTag(confirmedValue) {
		t.Errorf("the local档 read %q while the in-flight writes were not committed, want the pre-kill confirmed value %q",
			inFlightLocal, valueTag(confirmedValue))
	}

	t.Logf("window=3s status_counts=%v codes=%v unreachable=%d linear_200=%d linear_values=%v local_values=%v local_inflight=%q",
		statusCounts, codeCounts, unreachable, len(linear), linearValues, localValues, inFlightLocal)
	t.Logf("killed=%s confirmed=%s@%.0f acked_inflight=%v killed_log_writes=%d",
		leader, confirmedValue, confirmedIndex, ackedIndex, len(raftLogValueIndex(p.root, leader, key)))
}

// valueTag 取值的标签：在途写与已确认写的值都以固定前缀开头，标签就是那段前缀。
// 值里垫了填充，窗口里的记录按标签比，就不必带着整条值走。
func valueTag(value string) string {
	if len(value) > 11 {
		return value[:11]
	}
	return value
}

// raftLogValueIndex 扫节点盘上的 raft 日志，取出每个写入值的标签与它的日志 index。
// 帧头 8 字节（len+crc），entry 是 op|term 8B|index 8B|command（3.1 §5）；
// 尾部被 kill -9 截断的半条记录停在坏帧处，不往后读。
func raftLogValueIndex(root, nodeID, key string) map[string]uint64 {
	out := map[string]uint64{}
	paths, err := filepath.Glob(filepath.Join(root, nodeID, RaftDirName, WALDirName, "*.wal"))
	if err != nil {
		return out
	}
	sort.Strings(paths)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for off := 0; off+kvstore.FrameHeaderSize <= len(raw); {
			size, checksum, err := kvstore.DecodeFrame(raw[off:off+kvstore.FrameHeaderSize], int64(off))
			if err != nil {
				break
			}
			end := off + kvstore.FrameHeaderSize + int(size)
			if end > len(raw) {
				break
			}
			payload := raw[off+kvstore.FrameHeaderSize : end]
			if err := kvstore.CheckFrame(size, checksum, payload, int64(off)); err != nil {
				break
			}
			_, index, cmd, err := decodeWALRecord(payload, int64(off))
			if err != nil {
				break
			}
			if len(cmd) > 0 {
				k, v, err := kvstore.DecodeEntry(cmd, int64(off))
				if err == nil && k == key {
					out[valueTag(v)] = index
				}
			}
			off = end
		}
	}
	return out
}

// 三档退出码的入口形状：配置非法与成员表里没有本 id 都是 2，
// 数据目录被另一个进程锁住是 1（3.1 §4 沿用 2.1 §4）。
func TestEntryExitCodes(t *testing.T) {
	if testing.Short() {
		t.Skip("进程用例要起真进程，-short 下跳过")
	}
	bin := buildNodeBinary(t)
	dir := t.TempDir()
	peers := "node-1=" + portFor(1) + ",node-2=" + portFor(2)

	cases := []struct {
		name string
		args []string
	}{
		{"node id missing from the peer set",
			[]string{"-node-id", "node-9", "-peers", peers, "-data-dir", filepath.Join(dir, "b")}},
		{"peer item without an equals sign",
			[]string{"-node-id", "node-1", "-peers", "node-1", "-data-dir", filepath.Join(dir, "c")}},
		{"heartbeat not shorter than the election timeout",
			[]string{"-node-id", "node-1", "-peers", peers, "-data-dir", filepath.Join(dir, "d"),
				"-election-timeout-ms", "100", "-heartbeat-ms", "200"}},
	}
	for _, tc := range cases {
		code, stderr := readExitCode(t, bin, tc.args...)
		if code != 2 {
			t.Fatalf("%s: exit code = %d, want 2: %s", tc.name, code, firstBytes([]byte(stderr), 200))
		}
		// 3.1 §4 末尾那句「起不来的原因同时写日志与 stderr」：这一阶段日志还没开，stderr 是唯一可见面。
		if !strings.Contains(stderr, "miniraft-node:") {
			t.Fatalf("%s: the reason did not reach stderr: %q", tc.name, firstBytes([]byte(stderr), 200))
		}
	}

	// 目录锁那一格：先让一个真进程占住同一份数据目录，第二个才是「打不开数据目录」的 1。
	held := startHolding(t, bin, peers, filepath.Join(dir, "a"))
	defer held.stop()
	code, stderr := readExitCode(t, bin,
		"-node-id", "node-2", "-peers", peers, "-data-dir", filepath.Join(dir, "a"))
	if code != 1 {
		t.Fatalf("locked data dir: exit code = %d, want 1: %s", code, firstBytes([]byte(stderr), 200))
	}
	if !strings.Contains(stderr, kvstore.ErrLocked.Error()) {
		t.Fatalf("locked data dir: stderr lost the reason: %q", firstBytes([]byte(stderr), 200))
	}
}

// heldNode 是一个已经占到数据目录锁、正在监听的真进程。
type heldNode struct {
	t   *testing.T
	cmd *exec.Cmd
}

// startHolding 起一个节点并等它把锁拿到：日志里的 node started 就是那一步的落点（2.1 §8 的入口起停行）。
func startHolding(t *testing.T, bin, peers, dir string) *heldNode {
	t.Helper()
	cmd := exec.Command(bin, "-node-id", "node-1", "-peers", peers, "-data-dir", dir, "-log-level", "debug")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the lock holder: %v", err)
	}
	path := filepath.Join(dir, "log", NodeLogFileName)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fileContains(path, "node started") {
			return &heldNode{t: t, cmd: cmd}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatalf("the lock holder never logged node started in %s", path)
	return nil
}

func (h *heldNode) stop() {
	if h == nil || h.cmd == nil || h.cmd.ProcessState != nil {
		return
	}
	if err := h.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = h.cmd.Process.Kill()
	}
	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = h.cmd.Process.Kill()
		<-done
		h.t.Errorf("the lock holder did not exit on SIGTERM within 10s")
	}
}

func fileContains(path, want string) bool {
	raw, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(raw), want)
}

func toFloat(v any) float64 {
	number, _ := v.(float64)
	return number
}

// portFor 要一个没人用的端口号，只用来拼 -peers：这些进程根本不会跑到监听那一步。
func portFor(nth int) string {
	return fmt.Sprintf("127.0.0.1:%d", 20000+nth)
}
