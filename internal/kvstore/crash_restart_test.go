package kvstore

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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// 进程级用例（T13 这条线）：真的起 cmd/miniraft-kv——现在是单节点 raft 集群——
// 发请求、kill -9、重启对账。写路径每条一次 Sync（docs/8 §二），
// 已确认的写一条都不许丢；KV WAL 的批次窗口（旧 D6）整体作废。
// 慢，只在回归与注入实验里跑，`-short` 直接跳过（2.2 §1）。
func skipProcessTest(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("process-level crash test skipped under -short")
	}
	if runtime.GOOS != "linux" {
		t.Skipf("kill -9 与 flock 的语义按 Linux 断言，当前是 %s", runtime.GOOS)
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no Go toolchain to build the binary")
	}
}

var (
	binaryOnce sync.Once
	binaryPath string
	binaryDir  string
	binaryErr  error
)

// 编译产物放在临时目录，整个测试跑完再回收。
func TestMain(m *testing.M) {
	code := m.Run()
	if binaryDir != "" {
		_ = os.RemoveAll(binaryDir)
	}
	os.Exit(code)
}

// 编译一次，供所有进程级用例复用；产物放在测试二进制之外，不落仓库根目录。
func kvBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "miniraft-kv-bin")
		if err != nil {
			binaryErr = err
			return
		}
		binaryPath = filepath.Join(dir, "miniraft-kv")
		binaryDir = dir
		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/miniraft-kv")
		cmd.Dir = filepath.Join("..", "..")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			binaryErr = fmt.Errorf("go build ./cmd/miniraft-kv: %v: %s", err, stderr.String())
			return
		}
	})
	if binaryErr != nil {
		t.Fatalf("%v", binaryErr)
	}
	return binaryPath
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

type kvProcess struct {
	t       *testing.T
	cmd     *exec.Cmd
	dataDir string
	addr    string
	out     *bytes.Buffer
	exited  bool
}

// 节点日志落在 <data-dir>/log/miniraft-node.log（docs/8 §二）；进程只往 stderr 打致命错误。
// 退出码 1 的原因全在日志文件里，失败时必须把尾巴打出来。
func (p *kvProcess) logPath() string {
	return filepath.Join(p.dataDir, "log", "miniraft-node.log")
}

func (p *kvProcess) logTail(lines int) string {
	data, err := os.ReadFile(p.logPath())
	if err != nil {
		return fmt.Sprintf("<read %s: %v>", p.logPath(), err)
	}
	all := bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n"))
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return "\n" + string(bytes.Join(all, []byte("\n")))
}

// 起进程并等到 HTTP 可用。extra 传额外的命令行开关。
func startKVProcess(t *testing.T, dataDir string, extra ...string) *kvProcess {
	t.Helper()
	return startKVProcessAt(t, dataDir, freeAddr(t), extra...)
}

func startKVProcessAt(t *testing.T, dataDir, addr string, extra ...string) *kvProcess {
	t.Helper()
	args := append([]string{"-data-dir", dataDir, "-addr", addr, "-log-level", "debug"}, extra...)
	// #nosec G204 -- 参数全部由测试自己拼，路径来自 t.TempDir()。
	cmd := exec.Command(kvBinary(t), args...)
	out := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start miniraft-kv: %v", err)
	}
	p := &kvProcess{t: t, cmd: cmd, dataDir: dataDir, addr: addr, out: out}
	t.Cleanup(func() {
		if !p.exited {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	})
	p.waitReady()
	p.waitLeader()
	return p
}

// 就绪判据：单节点进程以 Leader 身份答一次默认档（线性一致）/get。
// 连不上、5xx（含 503 not_leader）都算没就绪——参照 scripts/crash_inject.sh 的 wait_ready，
// 把「只认 handler 的答复」收紧成「只认 Leader 的答复」：写路径下一条就要用。轮询到超时为止，不睡固定时长。
func (p *kvProcess) waitReady() {
	p.t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(p.url("/get?key=probe"))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.t.Fatalf("miniraft-kv never served a linearizable /get on %s\nstdout:\n%s\nlast log lines:%s",
		p.addr, p.out.String(), p.logTail(20))
}

func (p *kvProcess) url(path string) string { return "http://" + p.addr + path }

// waitLeader 等到本节点自己当选。单节点集群的多数派是 1（docs/8 §二），
// 但进程起来先停在 Follower，要等满一个选举截止才发起选举、自票胜出转 Leader，
// 写路径用例的 /put 在这之前只会拿到 503 not_leader。
// 判据用缺省档的线性一致读：Follower 一律 503，Leader 上 probe 这个 key 不存在是 404。
func (p *kvProcess) waitLeader() {
	p.t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(p.url("/get?key=probe"))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.t.Fatalf("miniraft-kv never became the leader on %s\nstdout:\n%s\nlast log lines:%s",
		p.addr, p.out.String(), p.logTail(20))
}

// put 发一次写并解析响应体：/put 成功回执带 index 与 term（docs/8 §二的 raft handler）。
func (p *kvProcess) put(key, value string) (int, uint64, error) {
	body, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		return 0, 0, err
	}
	resp, err := http.Post(p.url("/put"), "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, 0, nil
	}
	var reply struct {
		OK    bool   `json:"ok"`
		Index uint64 `json:"index"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return resp.StatusCode, 0, fmt.Errorf("put reply %q is not the documented shape: %w", data, err)
	}
	if !reply.OK {
		return resp.StatusCode, 0, fmt.Errorf("put reply has ok=false: %s", data)
	}
	return resp.StatusCode, reply.Index, nil
}

func (p *kvProcess) get(key string) (int, string, error) {
	return p.getAt(key, "")
}

func (p *kvProcess) getLocal(key string) (int, string, error) {
	return p.getAt(key, "&consistency=local")
}

func (p *kvProcess) getAt(key, consistency string) (int, string, error) {
	resp, err := http.Get(p.url("/get?key=" + key + consistency))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, "", err
	}
	var reply struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return resp.StatusCode, "", nil
	}
	return resp.StatusCode, reply.Value, nil
}

// kill -9：单节点写路径每条一次 Sync，已确认的写全部在盘上（docs/8 §二，旧 D6 作废）。
func (p *kvProcess) kill9() {
	p.t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		p.t.Fatalf("kill -9: %v", err)
	}
	err := p.cmd.Wait()
	p.exited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		p.t.Fatalf("killed process exited with %v, want a signal death", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		p.t.Errorf("death status = %v, want a SIGKILL", exit.Sys())
	}
}

// SIGTERM：走优雅关闭，退出码必须是 0。
func (p *kvProcess) terminateWantClean() {
	p.t.Helper()
	p.terminateWithin(10 * time.Second)
}

// terminateWithin 发 SIGTERM，核对退出码，并给「信号到进程退出」设上限 limit。
// 时长这条是收尾窗口的判据：沉默连接在场时，旧实现要等满 Go 内部那 5 秒才脱身，
// 只看退出码的话把窗口调大照样能过（2.1 §4）。
func (p *kvProcess) terminateWithin(limit time.Duration) {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatalf("sigterm: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	start := time.Now()
	select {
	case err := <-done:
		p.exited = true
		elapsed := time.Since(start)
		if err != nil {
			p.t.Fatalf("graceful shutdown exited with %v after %s, want 0\nstdout:\n%s\nlast log lines:%s",
				err, elapsed, p.out.String(), p.logTail(20))
		}
		if elapsed > limit {
			p.t.Fatalf("graceful shutdown took %s, over the %s limit\nstdout:\n%s\nlast log lines:%s",
				elapsed, limit, p.out.String(), p.logTail(20))
		}
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		p.t.Fatalf("no clean exit within 15s after SIGTERM\nstdout:\n%s\nlast log lines:%s",
			p.out.String(), p.logTail(20))
	}
}

// openSilentConn 连上端口但一个字节都不发：服务端这条连接停在 StateNew，
// 也就是「已经受理、还没读到请求头」那一类（2.1 §4）。
func (p *kvProcess) openSilentConn() net.Conn {
	p.t.Helper()
	c, err := net.Dial("tcp", p.addr)
	if err != nil {
		p.t.Fatalf("dial %s: %v", p.addr, err)
	}
	p.t.Cleanup(func() { _ = c.Close() })
	return c
}

// writeRange 写入 n 条互不相同的 key，要求逐条确认（2xx），并核对日志 index 严格递增。
// 起点不钉死：当选 Leader 会在日志尾部追加一条 no-op（3.1 §4），第一条客户端写
// 因此落在 index 2 上；重启的进程再当选又会多一条。本包只认「递增」，
// 返回首尾两个 index，条数由调用方自己按 last-first+1 核。
func (p *kvProcess) writeRange(n int) (uint64, uint64) {
	p.t.Helper()
	var first, last uint64
	for i := 0; i < n; i++ {
		code, index, err := p.put(fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
		if err != nil {
			p.t.Fatalf("put %d: %v", i, err)
		}
		if code != http.StatusOK {
			p.t.Fatalf("put %d stopped with %d, want every write acked", i, code)
		}
		if i == 0 {
			first = index
		} else if index != last+1 {
			p.t.Fatalf("put %d landed at index %d, want %d: the acked writes must be contiguous", i, index, last+1)
		}
		last = index
	}
	return first, last
}

// 重启后逐个 key 对账（本地档读）：读到的必须是当初写进去的值，读不到的算丢写。
// 返回恢复条数与逐条值相等的断言一并完成——丢任何一条都直接 Fatalf。
func (p *kvProcess) reconcile(total int) {
	p.t.Helper()
	for i := 0; i < total; i++ {
		key, want := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)
		code, got, err := p.getLocal(key)
		if err != nil {
			p.t.Fatalf("get %s: %v", key, err)
		}
		if code != http.StatusOK || got != want {
			p.t.Fatalf("after restart %s = %d %q, want 200 %q: an acked write was lost", key, code, got, want)
		}
	}
}

func (p *kvProcess) processLog() string {
	p.t.Helper()
	data, err := os.ReadFile(p.logPath())
	if err != nil {
		p.t.Fatalf("read the log file: %v", err)
	}
	return string(data)
}

// servedLines 数某一类路由的成功访问日志行。就绪与选主的轮询都打在 /get 上，
// 次数随选举快慢浮动，按路由分开数才不用替探测兜底。
func servedLines(logText, route string) int {
	var n int
	for _, line := range strings.Split(logText, "\n") {
		if strings.Contains(line, `"msg":"http request served"`) &&
			strings.Contains(line, `"route":"`+route+`"`) {
			n++
		}
	}
	return n
}

// raft/state 的 JSON 游标：kvstore 不依 raft 包（会成循环），直接解析文件（docs/8 §二）。
type raftStateDoc struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
}

func readRaftState(t *testing.T, dataDir string) raftStateDoc {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "raft", "state"))
	if err != nil {
		t.Fatalf("read raft/state: %v", err)
	}
	var st raftStateDoc
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatalf("raft/state is not the documented JSON: %v (raw %q)", err, data)
	}
	return st
}

func raftSegments(t *testing.T, dataDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dataDir, "raft", "log"))
	if err != nil {
		t.Fatalf("read raft/log: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".rlog") {
			out = append(out, e.Name())
		}
	}
	return out
}

// kill -9 之后已确认的写一条不少：写路径每条一次 Sync，旧「批次未满丢 0..63 条」的
// D6 窗口不复存在（docs/8 §二）。同时钉住目录布局：没有 wal/、raft/log 有段、
// raft/state 的 lastApplied 不小于确认过的条数。
func TestCrashAfterAckedWritesLosesNothing(t *testing.T) {
	skipProcessTest(t)
	const total = 200
	dataDir := t.TempDir()

	p := startKVProcess(t, dataDir)
	first, acked := p.writeRange(total)
	if acked-first+1 != uint64(total) {
		t.Fatalf("acked writes span [%d,%d], want %d contiguous entries", first, acked, total)
	}
	if first != 2 {
		t.Errorf("first acked write landed at index %d, want 2: index 1 is the election no-op", first)
	}
	p.kill9()

	// 崩溃现场的目录账：KV WAL 已取消，日志与游标是唯一的持久层。
	if _, err := os.Stat(filepath.Join(dataDir, "wal")); !os.IsNotExist(err) {
		t.Errorf("a wal/ directory exists in %s: KV WAL was retired (docs/8 §二)", dataDir)
	}
	if got := raftSegments(t, dataDir); len(got) == 0 {
		t.Error("no .rlog segment under raft/log after 200 acked writes")
	}
	st := readRaftState(t, dataDir)
	if st.LastApplied < acked {
		t.Errorf("lastApplied = %d after crash, want >= %d acked writes", st.LastApplied, acked)
	}
	if st.CommitIndex < acked {
		t.Errorf("commitIndex = %d after crash, want >= %d acked writes", st.CommitIndex, acked)
	}

	restarted := startKVProcess(t, dataDir)
	restarted.reconcile(total)
	// 默认档线性一致读在重启后的单节点 Leader 上照常服务。
	if code, got, err := restarted.get("k0199"); err != nil || code != http.StatusOK || got != "v0199" {
		t.Errorf("linearizable read after restart = %d %q %v, want 200 v0199", code, got, err)
	}
	restarted.terminateWantClean()
}

// SIGTERM 走完整收尾：重启一条不丢，退出码 0，LOCK 文件仍在。
// 日志字段口径（docs/8 §二）：入口行是 node started / node stopped，component=entry。
func TestGracefulShutdownLosesNothing(t *testing.T) {
	skipProcessTest(t)
	const total = 100
	dataDir := t.TempDir()

	p := startKVProcess(t, dataDir)
	p.writeRange(total)
	p.terminateWantClean()

	if _, err := os.Stat(filepath.Join(dataDir, LockFileName)); err != nil {
		t.Errorf("the lock file disappeared on shutdown: %v", err)
	}

	// 只数第一个进程的尾巴：重启进程紧接着会往同一份日志追加。
	firstRun := p.processLog()
	for _, want := range []string{`"msg":"node started"`, `"component":"entry"`, `"msg":"node stopped"`, `"exit_code":0`} {
		if !strings.Contains(firstRun, want) {
			t.Errorf("first run log has no %s%s", want, p.logTail(20))
		}
	}
	for _, forbidden := range []string{"v0000", "v0099", `"value":"`} {
		if strings.Contains(firstRun, forbidden) {
			t.Errorf("log leaks %q", forbidden)
		}
	}
	// 访问日志按路由分开数：/get 上的探测次数取决于选举要轮询几轮，不钉死；
	// /put 的成功行必须一条写一行（2.1 §8）。
	if n := servedLines(firstRun, "/put"); n != total {
		t.Errorf("access log has %d served /put lines, want %d (one per acked write)", n, total)
	}

	restarted := startKVProcess(t, dataDir)
	restarted.reconcile(total)
	restarted.terminateWantClean()
}

// 只连不说话的连接占不住收尾窗口（2.1 §4）：net/http 把这种连接当忙连接，
// 要等它自己满 5 秒才肯当空闲，旧实现里这 5 秒和 shutdownGrace 撞在同一时刻，
// 于是 Shutdown 返回 context deadline exceeded、进程退 1，而数据一条没丢。
func TestSilentConnectionDoesNotBlockShutdown(t *testing.T) {
	skipProcessTest(t)
	const total = 20
	dataDir := t.TempDir()

	p := startKVProcess(t, dataDir)
	p.writeRange(total)
	p.openSilentConn()
	// 沉默连接光 dial 不算被受理：accept 循环按顺序收连接，所以让它随后服务一次全新拨号即可反证。
	// keep-alive 必须关掉，否则这次请求可能复用旧连接，顺序就落不到 accept 上。
	noKeep := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := noKeep.Get(p.url("/get?key=probe&consistency=local"))
	if err != nil {
		t.Fatalf("get after the silent dial: %v", err)
	}
	resp.Body.Close()
	noKeep.CloseIdleConnections()

	p.terminateWithin(4 * time.Second)

	// 这条日志说明收尾确实关掉了沉默连接。条数不钉死：本用例自己的客户端也会留下受理完还没读的连接。
	if !strings.Contains(p.processLog(), `"msg":"closed connections that never sent a request"`) {
		t.Errorf("shutdown did not close any silent connection%s", p.logTail(6))
	}

	restarted := startKVProcess(t, dataDir)
	restarted.reconcile(total)
	restarted.terminateWantClean()
}

// 建新段被权限挡住时，进程只回 500，不自杀：raftLog 的失效只由 Sync 失败触发，
// 建段失败不置失效（3.1 §7）。读路径继续服务，恢复写权限后追加照常（D5 新口径）。
func TestProcessSurvivesBlockedSegmentCreation(t *testing.T) {
	skipProcessTest(t)
	dataDir := t.TempDir()
	raftLogDir := filepath.Join(dataDir, "raft", "log")
	// 一条 ("kNNNN","vNNNN") 记录 52B 帧：128B 的段上限让每隔两条滚一次段。
	p := startKVProcess(t, dataDir, "-max-file-size", "128")

	const total = 10
	p.writeRange(total)
	if got := len(raftSegments(t, dataDir)); got < 2 {
		t.Fatalf("setup wrote %d segments under a 128-byte cap, want rollover to have happened", got)
	}

	if err := os.Chmod(raftLogDir, 0o555); err != nil {
		t.Fatalf("chmod raft log dir: %v", err)
	}
	restored := false
	restore := func() {
		if !restored {
			if err := os.Chmod(raftLogDir, 0o755); err != nil {
				t.Fatalf("restore chmod: %v", err)
			}
			restored = true
		}
	}
	t.Cleanup(restore)

	// 当前段里塞得下的写照常确认；一旦越界要建新段就撞 EACCES。
	// 循环到出现第一条 500 为止，段大小不由本包精确掌控，不赌第几条触发。
	blockedAcked := 0
	var rejectedKey string
	for i := 0; i < 12; i++ {
		key, value := fmt.Sprintf("b%04d", i), fmt.Sprintf("v%04d", i)
		code, _, err := p.put(key, value)
		if err != nil {
			t.Fatalf("put %s during a blocked rollover: %v", key, err)
		}
		if code == http.StatusOK {
			blockedAcked++
			continue
		}
		if code == http.StatusInternalServerError {
			rejectedKey = key
			break
		}
		t.Fatalf("put %s during a blocked rollover = %d, want 200 or 500", key, code)
	}
	if rejectedKey == "" {
		t.Fatalf("12 puts against a 555-mode raft log dir never forced a rollover")
	}

	// 读路径必须还活着，而且是默认档线性一致读：进程没被这条 500 带走。
	if c, v, err := p.get("k0000"); err != nil || c != http.StatusOK || v != "v0000" {
		t.Fatalf("read path after a blocked rollover = %d %q %v", c, v, err)
	}

	restore()
	if code, _, err := p.put("after", "block"); err != nil || code != http.StatusOK {
		t.Fatalf("put after the dir became writable again = %d, %v", code, err)
	}
	// 收尾退出码 0：一次建段失败不属于 D5 的失效语义，不该触发进程自杀。
	p.terminateWantClean()

	restarted := startKVProcess(t, dataDir)
	restarted.reconcile(total)
	for i := 0; i < blockedAcked; i++ {
		key, want := fmt.Sprintf("b%04d", i), fmt.Sprintf("v%04d", i)
		if c, v, err := restarted.getLocal(key); err != nil || c != http.StatusOK || v != want {
			t.Errorf("acked write %s during the block = %d %q %v after restart", key, c, v, err)
		}
	}
	if c, _, err := restarted.getLocal(rejectedKey); err != nil || c != http.StatusNotFound {
		t.Errorf("the rejected put %s came back after restart: %d %v", rejectedKey, c, err)
	}
	if c, v, err := restarted.getLocal("after"); err != nil || c != http.StatusOK || v != "block" {
		t.Errorf("write after recovery = %d %q %v", c, v, err)
	}
	restarted.terminateWantClean()
}

// 地址被占时进程必须自己退掉，退出码 1，原因同时落日志和 stderr。
// 起不来也要放掉目录锁，否则一次失败启动会把目录焊死。
func TestProcessExitsWhenAddressIsTaken(t *testing.T) {
	skipProcessTest(t)
	dataDir := t.TempDir()
	blocker, err := net.Listen("tcp", freeAddr(t))
	if err != nil {
		t.Fatalf("hold an address: %v", err)
	}
	defer func() { _ = blocker.Close() }()

	args := []string{"-data-dir", dataDir, "-addr", blocker.Addr().String(), "-log-level", "debug"}
	// #nosec G204 -- 参数全部由测试自己拼，路径来自 t.TempDir()。
	cmd := exec.Command(kvBinary(t), args...)
	out := new(bytes.Buffer)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start miniraft-kv: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	var code int
	select {
	case err := <-exited:
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("start miniraft-kv on a taken address = %v, want a non-zero exit", err)
		}
		code = exit.ExitCode()
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the process kept running on an address somebody else holds")
	}
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (2.1 §4)", code)
	}
	if !strings.Contains(out.String(), "listen") {
		t.Errorf("stderr says nothing about the listen failure: %q", out.String())
	}
	p := &kvProcess{t: t, dataDir: dataDir}
	logged := p.processLog()
	if !strings.Contains(logged, `"msg":"listen failed"`) {
		t.Errorf("the node log has no listen failed line:%s", p.logTail(10))
	}
	// 旧格里「wal closed」随 KV WAL 取消：Close 现在只剩放锁与关日志，
	// 是否真放了锁由下面的同目录重启证明。

	blockerAddr := blocker.Addr().String()
	if err := blocker.Close(); err != nil {
		t.Fatalf("release the address: %v", err)
	}
	restarted := startKVProcessAt(t, dataDir, blockerAddr)
	if c, _, err := restarted.get("nothing"); err != nil || c != http.StatusNotFound {
		t.Errorf("restart after a failed start = %d %v", c, err)
	}
	restarted.terminateWantClean()
}
