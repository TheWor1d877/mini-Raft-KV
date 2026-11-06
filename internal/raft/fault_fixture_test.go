package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMain 只做一件环境准备：用户在独立的网络命名空间里跑故障用例时，
// loopback 默认是 down，先拉到 up；普通 go test 下这条命令失败也不影响任何用例。
func TestMain(m *testing.M) {
	_ = exec.Command("ip", "link", "set", "lo", "up").Run()
	os.Exit(m.Run())
}

// newLoopbackProcessCluster 起 size 个真进程，每台绑在 127.0.0.<20+i> 上。
// 各节点源地址不同，网络故障才能按「从谁到谁」切断；envOf 给每台机器配独立环境变量，
// 故障注入的停点与快照停点都只挂在目标节点上。
func newLoopbackProcessCluster(t *testing.T, size int, flags []string, envOf func(string) []string) *processCluster {
	t.Helper()
	root := t.TempDir()
	p := &processCluster{
		t:     t,
		bin:   buildNodeBinary(t),
		root:  root,
		cmd:   map[string]*exec.Cmd{},
		addr:  map[string]string{},
		env:   map[string][]string{},
		flags: flags,
	}
	var items []string
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node-%d", i)
		ip := fmt.Sprintf("127.0.0.%d", 20+i)
		ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			t.Fatalf("probe a free port on %s for %s: %v", ip, id, err)
		}
		p.addr[id] = ln.Addr().String()
		if err := ln.Close(); err != nil {
			t.Fatalf("release the probe listener on %s: %v", ip, err)
		}
		if envOf != nil {
			p.env[id] = envOf(id)
		}
		p.ids = append(p.ids, id)
		items = append(items, id+"="+p.addr[id])
	}
	p.peers = strings.Join(items, ",")
	p.start()
	t.Cleanup(func() { p.stopAll() })
	return p
}

func (p *processCluster) ipOf(t *testing.T, id string) string {
	t.Helper()
	host, _, err := net.SplitHostPort(p.addr[id])
	if err != nil {
		t.Fatalf("address of %s: %v", id, err)
	}
	return host
}

func (p *processCluster) pidOf(t *testing.T, id string) int {
	t.Helper()
	cmd := p.cmd[id]
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		t.Fatalf("%s is not running", id)
	}
	return cmd.Process.Pid
}

func (p *processCluster) freeze(id string) {
	p.t.Helper()
	if err := syscall.Kill(p.pidOf(p.t, id), syscall.SIGSTOP); err != nil {
		p.t.Fatalf("SIGSTOP %s: %v", id, err)
	}
}

func (p *processCluster) thaw(id string) {
	p.t.Helper()
	if err := syscall.Kill(p.pidOf(p.t, id), syscall.SIGCONT); err != nil {
		p.t.Fatalf("SIGCONT %s: %v", id, err)
	}
}

// requireNetAdmin 检查当前进程是否能在所在网络命名空间里改 iptables；
// 普通 go test 下跳过，故障脚本会在 unshare -rn 里再跑一次。
func requireNetAdmin(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("network fault injection needs CAP_NET_ADMIN; run under `unshare -rn`")
	}
	if out, err := exec.Command("iptables", "-w", "-S").CombinedOutput(); err != nil {
		t.Skipf("iptables is not usable here: %v: %s", err, out)
	}
}

// requireMount 检查能否在挂载命名空间里挂 tmpfs；磁盘故障用例靠它造写满与只读。
func requireMount(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("mount fault injection needs CAP_SYS_ADMIN; run under `unshare -rm`")
	}
}

func runHost(t *testing.T, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustRunHost(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := runHost(t, name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return out
}

// dropDirection 让 from 发往 to 的包在出站时丢掉；反方向单独加，才测得出单向失联。
func dropDirection(t *testing.T, p *processCluster, from, to string) {
	t.Helper()
	requireNetAdmin(t)
	mustRunHost(t, "iptables", "-w", "-A", "OUTPUT",
		"-s", p.ipOf(t, from), "-d", p.ipOf(t, to), "-j", "DROP")
}

// isolateNode 双向切断一台机器与其余所有机器的流量：每个方向各一条规则。
func isolateNode(t *testing.T, p *processCluster, id string) {
	t.Helper()
	requireNetAdmin(t)
	self := p.ipOf(t, id)
	for _, other := range p.ids {
		if other == id {
			continue
		}
		peer := p.ipOf(t, other)
		mustRunHost(t, "iptables", "-w", "-A", "OUTPUT", "-s", self, "-d", peer, "-j", "DROP")
		mustRunHost(t, "iptables", "-w", "-A", "OUTPUT", "-s", peer, "-d", self, "-j", "DROP")
	}
}

// healNetwork 清掉所有方向规则与 lo 上的 netem，下一段重新注入。
func healNetwork(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		return
	}
	_, _ = runHost(t, "iptables", "-w", "-F")
	_, _ = runHost(t, "tc", "qdisc", "del", "dev", "lo", "root")
}

func netemLo(t *testing.T, args ...string) {
	t.Helper()
	requireNetAdmin(t)
	full := append([]string{"qdisc", "add", "dev", "lo", "root", "netem"}, args...)
	mustRunHost(t, "tc", full...)
}

func (p *processCluster) logText(id string) string {
	raw, err := os.ReadFile(p.logPath(id))
	if err != nil {
		return ""
	}
	return string(raw)
}

// waitLog 等一条日志出现；注入停点与恢复完成都以日志原文为准。
func (p *processCluster) waitLog(id, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(p.logText(id), want) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// leaderTerms 数每台机器在哪些 term 上把角色改成 leader；用来验「同 term 至多一个 leader」。
func (p *processCluster) leaderTerms(t *testing.T, id string) map[uint64]int {
	t.Helper()
	out := map[uint64]int{}
	for _, line := range strings.Split(p.logText(id), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Msg     string `json:"msg"`
			NewRole string `json:"new_role"`
			Term    uint64 `json:"term"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec.Msg == "role changed" && rec.NewRole == "leader" {
			out[rec.Term]++
		}
	}
	return out
}

func assertSingleLeaderPerTerm(t *testing.T, p *processCluster) {
	t.Helper()
	seen := map[uint64][]string{}
	for _, id := range p.ids {
		for term, n := range p.leaderTerms(t, id) {
			for i := 0; i < n; i++ {
				seen[term] = append(seen[term], id)
			}
		}
	}
	for term, nodes := range seen {
		if len(nodes) > 1 {
			t.Errorf("term %d has %d leaders: %v", term, len(nodes), nodes)
		}
	}
}

// waitValue 在一台机器上用本地档轮询一个键的值；恢复后的读回都走这条。
func (p *processCluster) waitValue(id, key, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		status, raw, err := p.get(id, key)
		if err == nil && status == 200 && strings.Contains(string(raw), `"value":"`+want+`"`) {
			return nil
		}
		last = fmt.Sprintf("status=%d body=%s err=%v", status, firstBytes(raw, 120), err)
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s never read %s=%s within %v (last %s)", id, key, want, timeout, last)
}

// readTerm 选一台在跑的机器做一次写，返回回执里的 term；找不到 leader 时返回错误。
func (p *processCluster) readTerm(t *testing.T) (string, float64) {
	t.Helper()
	leader, reply := p.writeThrough(fmt.Sprintf("probe-%d", time.Now().UnixNano()), "v")
	return leader, toFloat(reply["term"])
}
