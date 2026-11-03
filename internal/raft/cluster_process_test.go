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
	p := &processCluster{
		t:    t,
		bin:  buildNodeBinary(t),
		root: t.TempDir(),
		cmd:  map[string]*exec.Cmd{},
		addr: map[string]string{},
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
		dir := filepath.Join(p.root, id)
		cmd := exec.Command(p.bin,
			"-node-id", id, "-peers", p.peers, "-data-dir", dir, "-log-level", "debug")
		// 节点的日志都在数据目录的文件里，stdout 只是兜底：留着管道会让 Cleanup 等不到 EOF。
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			p.t.Fatalf("start %s: %v", id, err)
		}
		p.cmd[id] = cmd
	}
	for _, id := range p.ids {
		p.waitReady(id)
	}
}

func (p *processCluster) url(id, path string) string {
	return "http://" + p.addr[id] + path
}

// waitReady 只认 handler 的答复：404 与 200 都是 /get 起来了，连不上与 5xx 都算没就绪。
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

func (p *processCluster) get(id, key string) (int, []byte, error) {
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
	if err := cmd.Process.Kill(); err != nil {
		p.t.Fatalf("kill -9 %s: %v", id, err)
	}
	_, err := cmd.Process.Wait()
	if err != nil {
		p.t.Errorf("reap %s: %v", id, err)
	}
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
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			p.t.Errorf("signal %s: %v", id, err)
			continue
		}
		// 收尾窗口本身 5 秒，多给 10 秒等 fsync；超时就 kill -9，别让用例挂在这里。
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			p.t.Errorf("node %s did not exit on SIGTERM within 10s", id)
		}
		codes[id] = cmd.ProcessState.ExitCode()
		delete(p.cmd, id)
	}
	return codes
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
