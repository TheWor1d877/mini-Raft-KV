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

// 进程级用例：真的起 cmd/miniraft-kv，发请求、kill -9、重启对账。
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
	return p
}

func (p *kvProcess) waitReady() {
	p.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + p.addr + "/get?key=probe")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.t.Fatalf("miniraft-kv never served on %s\noutput:\n%s", p.addr, p.out.String())
}

func (p *kvProcess) url(path string) string { return "http://" + p.addr + path }

func (p *kvProcess) put(key, value string) (int, error) {
	body, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		return 0, err
	}
	resp, err := http.Post(p.url("/put"), "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (p *kvProcess) get(key string) (int, string, error) {
	resp, err := http.Get(p.url("/get?key=" + key))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
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

// kill -9：不等收尾，批次里没落盘的记录由恢复窗口兜住（D6）。
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
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatalf("sigterm: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		p.exited = true
		if err != nil {
			p.t.Fatalf("graceful shutdown exited with %v, want 0\noutput:\n%s", err, p.out.String())
		}
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		p.t.Fatalf("no clean exit within 10s after SIGTERM\noutput:\n%s", p.out.String())
	}
}

// writeRange 写入 n 条互不相同的 key，返回服务端确认成功（2xx）的条数。
func (p *kvProcess) writeRange(n int) int {
	p.t.Helper()
	acked := 0
	for i := 0; i < n; i++ {
		code, err := p.put(fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
		if err != nil {
			p.t.Fatalf("put %d: %v", i, err)
		}
		if code == http.StatusOK {
			acked++
			continue
		}
		// 500 说明写路径已经关掉，这条和之后的都拿不到确认。
		p.t.Logf("put %d stopped with %d", i, code)
		return acked
	}
	return acked
}

// 重启后逐个 key 对账：读到的必须是当初写进去的值，读不到的算丢写。
func (p *kvProcess) reconcile(total int) int {
	p.t.Helper()
	recovered := 0
	for i := 0; i < total; i++ {
		key, want := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)
		code, got, err := p.get(key)
		if err != nil {
			p.t.Fatalf("get %s: %v", key, err)
		}
		switch {
		case code == http.StatusOK && got == want:
			recovered++
		case code == http.StatusNotFound:
			// 没恢复：落在丢写窗口里。
		default:
			p.t.Errorf("%s: %d %q, want %q or 404", key, code, got, want)
		}
	}
	return recovered
}

func walDirFiles(t *testing.T, dataDir string) (segments, tmps []string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dataDir, WALDirName))
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".tmp"):
			tmps = append(tmps, e.Name())
		case strings.HasSuffix(e.Name(), ".wal"):
			segments = append(segments, e.Name())
		}
	}
	return segments, tmps
}

// 批次未满就 kill -9：丢写只可能落在最近 63 条里，且不许出现未确认的记录（D6）。
func TestCrashInsideBatch(t *testing.T) {
	skipProcessTest(t)
	const total = 200
	dataDir := t.TempDir()

	p := startKVProcess(t, dataDir)
	acked := p.writeRange(total)
	if acked != total {
		t.Fatalf("server acked %d of %d writes", acked, total)
	}
	p.kill9()

	restarted := startKVProcess(t, dataDir)
	recovered := restarted.reconcile(total)
	if recovered < acked-DefaultBatchSize+1 {
		t.Errorf("recovered %d of %d acked writes, lost %d, want at most %d (D6)",
			recovered, acked, acked-recovered, DefaultBatchSize-1)
	}
	if recovered > acked {
		t.Errorf("recovered %d records but only %d were acked: phantom writes", recovered, acked)
	}
	t.Logf("kill -9 mid-batch: acked=%d recovered=%d lost=%d", acked, recovered, acked-recovered)
}

// 频繁换段时被杀：恢复只认文件名序，段数继续增长，记录不丢未确认的那部分。
func TestCrashDuringRollover(t *testing.T) {
	skipProcessTest(t)
	dataDir := t.TempDir()

	const total = 400
	p := startKVProcess(t, dataDir, "-max-file-size", "512")
	acked := p.writeRange(total)
	p.kill9()

	segments, tmps := walDirFiles(t, dataDir)
	if len(segments) < 2 {
		t.Fatalf("only %d segments after %d writes under a 512-byte limit", len(segments), acked)
	}
	restarted := startKVProcess(t, dataDir)
	recovered := restarted.reconcile(total)
	if recovered < acked-DefaultBatchSize+1 {
		t.Errorf("recovered %d of %d acked writes, want at most %d lost", recovered, acked, DefaultBatchSize-1)
	}
	if recovered > acked {
		t.Errorf("recovered %d records but only %d were acked", recovered, acked)
	}
	// 未提交的临时段必须被删掉，正式段一个都不能少。
	_, afterTmps := walDirFiles(t, dataDir)
	if len(afterTmps) != 0 {
		t.Errorf("leftover *.tmp segments after restart: %v (before=%v)", afterTmps, tmps)
	}
	t.Logf("kill -9 across rollovers: acked=%d recovered=%d segments=%d stale_tmp=%d",
		acked, recovered, len(segments), len(tmps))
}

// 上一次留下一个还没改名的临时段：恢复必须在扫描之前删掉它，不碰已提交的段，也不把序号推高。
func TestLeftoverUncommittedSegment(t *testing.T) {
	skipProcessTest(t)
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dataDir

	first := startKVProcess(t, dataDir, "-max-file-size", "512")
	if first.writeRange(20) != 20 {
		t.Fatalf("setup writes were not acked")
	}
	// 用正常收尾把 20 条钉到盘上，本用例断的是残留临时段的处理，不是丢写窗口。
	first.terminateWantClean()

	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("setup produced %d segments, want several", len(segs))
	}
	lastSeq := segs[len(segs)-1].seq

	// 造一个「已创建、未改名」的残留，序号比真实最大段还大一档，里面还写着字节。
	stale := filepath.Join(dataDir, WALDirName, fmt.Sprintf(WALTmpFileFormat, lastSeq+1))
	if err := os.WriteFile(stale, []byte("000000"), 0o644); err != nil {
		t.Fatalf("write leftover: %v", err)
	}

	restarted := startKVProcess(t, dataDir)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("the uncommitted segment %s survived recovery (err=%v)", stale, err)
	}
	if got := restarted.reconcile(20); got != 20 {
		t.Errorf("recovered %d of 20 records that were already durable", got)
	}

	// 接着写一条再正常收尾，检查恢复之后的追加与编号。
	if code, err := restarted.put("after", "restart"); err != nil || code != http.StatusOK {
		t.Fatalf("put after recovery = %d, %v", code, err)
	}
	restarted.terminateWantClean()
	// 先看一眼临时段的总数：listSegments 自己会删 tmp，放在后面就测不出问题了。
	if _, tmps := walDirFiles(t, dataDir); len(tmps) != 0 {
		t.Errorf("temp segments left behind: %v", tmps)
	}
	after, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments after restart = %v", err)
	}

	// 段号不许因为那个临时文件跳号：恢复删 tmp 在扫描之前，O_EXCL 也才腾得出名字。
	if maxSeq := after[len(after)-1].seq; maxSeq > lastSeq+1 {
		t.Errorf("highest segment is %d, want at most %d: the leftover tmp consumed a segment number", maxSeq, lastSeq+1)
	}
	for i, sg := range after {
		if sg.seq != uint64(i+1) {
			t.Errorf("segment %d is numbered %d, want a gapless run from 1", i, sg.seq)
		}
	}
}

// SIGTERM 走完整收尾：未满的一批被落盘，重启一条不丢，退出码为 0。
func TestGracefulShutdownLosesNothing(t *testing.T) {
	skipProcessTest(t)
	const total = 100
	dataDir := t.TempDir()

	p := startKVProcess(t, dataDir)
	acked := p.writeRange(total)
	if acked != total {
		t.Fatalf("server acked %d of %d", acked, total)
	}
	p.terminateWantClean()

	restarted := startKVProcess(t, dataDir)
	if got := restarted.reconcile(total); got != total {
		t.Errorf("after a clean shutdown only %d of %d records came back", got, total)
	}
	if _, err := os.Stat(filepath.Join(dataDir, LockFileName)); err != nil {
		t.Errorf("the lock file disappeared on shutdown: %v", err)
	}

	// 日志落在 data/log/ 下，入口行带 component=entry，且写进去的 value 一个都不许出现（2.1 §8）。
	log := processLog(t, dataDir)
	for _, want := range []string{`"msg":"server started"`, `"component":"entry"`, `"msg":"server stopped"`, `"exit_code":0`} {
		if !strings.Contains(log, want) {
			t.Errorf("log has no %s", want)
		}
	}
	for _, forbidden := range []string{"v0000", "v0099", `"value":"`} {
		if strings.Contains(log, forbidden) {
			t.Errorf("log leaks %q", forbidden)
		}
	}
	if n := strings.Count(log, `"msg":"http request served"`); n != total*2 {
		t.Errorf("access log has %d lines, want %d (one per request to this process)", n, total*2)
	}
}

func processLog(t *testing.T, dataDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, LogDirName, LogFileName))
	if err != nil {
		t.Fatalf("read the log file: %v", err)
	}
	return string(data)
}

// 建新段被权限挡住时，进程只回 500，不自杀：这类失败盘上边界已知，不属于失效语义（D5）。
func TestProcessSurvivesBlockedRollover(t *testing.T) {
	skipProcessTest(t)
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, WALDirName)
	// 一条 ("kNNNN","vNNNN") 的记录 27B：10 条正好 270B，下一条 29B 必然越过 280B 的段上限。
	p := startKVProcess(t, dataDir, "-max-file-size", "280")

	const total = 10
	if p.writeRange(total) != total {
		t.Fatalf("setup writes were not all acked")
	}
	if segs, _ := walDirFiles(t, dataDir); len(segs) != 1 {
		t.Fatalf("setup wrote %d segments, want the 11th record to be the one that rolls", len(segs))
	}
	if err := os.Chmod(walDir, 0o555); err != nil {
		t.Fatalf("chmod wal dir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(walDir, 0o755) })

	code, err := p.put("blocked", "value")
	if err != nil {
		t.Fatalf("put during a blocked rollover: %v", err)
	}
	if code != http.StatusInternalServerError {
		t.Errorf("put during a blocked rollover = %d, want 500", code)
	}
	// 读路径必须还活着：进程没被这条错误带走。
	if c, v, err := p.get("k0000"); err != nil || c != http.StatusOK || v != "v0000" {
		t.Fatalf("read path after a blocked rollover = %d %q %v", c, v, err)
	}

	if err := os.Chmod(walDir, 0o755); err != nil {
		t.Fatalf("restore chmod: %v", err)
	}
	if code, err := p.put("after", "block"); err != nil || code != http.StatusOK {
		t.Fatalf("put after the dir became writable again = %d, %v", code, err)
	}
	p.terminateWantClean()

	restarted := startKVProcess(t, dataDir)
	if got := restarted.reconcile(total); got != total {
		t.Errorf("recovered %d of %d records that were acked before the failure", got, total)
	}
	if c, _, err := restarted.get("blocked"); err != nil || c != http.StatusNotFound {
		t.Errorf("the rejected put came back after restart: %d %v", c, err)
	}
	if c, v, err := restarted.get("after"); err != nil || c != http.StatusOK || v != "block" {
		t.Errorf("write after recovery = %d %q %v", c, v, err)
	}
}

// 地址被占时进程必须自己退掉，退出码 1，原因同时落日志和 stderr。
// 只写日志文件的话，脚本侧只看得到「进程没了」，查不到为什么。
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
	logged := processLog(t, dataDir)
	if !strings.Contains(logged, `"msg":"listen failed"`) {
		t.Errorf("the log file has no listen failed line:\n%s", logged)
	}
	// 起不来也要把已经打开的 store 收尾：放掉 fd、留下 wal closed，不把目录停在半开状态。
	if !strings.Contains(logged, `"msg":"wal closed"`) {
		t.Errorf("the failed start left the store open:\n%s", logged)
	}

	// 失败路径也要把目录锁放掉，否则同一次启动失败会把目录焊死。
	blockerAddr := blocker.Addr().String()
	if err := blocker.Close(); err != nil {
		t.Fatalf("release the address: %v", err)
	}
	restarted := startKVProcessAt(t, dataDir, blockerAddr)
	if c, _, err := restarted.get("nothing"); err != nil || c != http.StatusNotFound {
		t.Errorf("restart after a failed start = %d %v", c, err)
	}
}
