package raft

import (
	"fmt"
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

// mountTmpfs 在 dir 上挂一个小 tmpfs：写满与只读两档都靠它造。
func mountTmpfs(t *testing.T, dir, size string) {
	t.Helper()
	requireMount(t)
	mustRunHost(t, "mount", "-t", "tmpfs", "-o", "size="+size, "tmpfs", dir)
	t.Cleanup(func() { _, _ = runHost(t, "umount", dir) })
}

// remountTmpfs 改挂载选项；只读档要求此时没有进程在这块盘上持有可写句柄。
func remountTmpfs(t *testing.T, dir, opts string) {
	t.Helper()
	mustRunHost(t, "mount", "-o", "remount,"+opts, "tmpfs", dir)
}

// waitNodeReady 是低配探活：节点自己决定退出时，调用方要能拿到 false 而不是被 Fatalf 打断。
func (p *processCluster) waitNodeReady(id string, timeout time.Duration) bool {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, _, err := p.get(id, "__probe__")
		if err == nil && (status == http.StatusNotFound || status == http.StatusOK) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// stopOne 走正常退出路径停一台机器：SIGTERM 之后节点自己把状态刷完。
func (p *processCluster) stopOne(id string) int {
	p.t.Helper()
	cmd := p.cmd[id]
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return -1
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatalf("SIGTERM %s: %v", id, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		p.t.Fatalf("node %s did not exit on SIGTERM within 10s", id)
	}
	return cmd.ProcessState.ExitCode()
}

// lastWALSegment 取排序后的最后一段：尾截断要动的就是它。
func (p *processCluster) lastWALSegment(t *testing.T, id string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(p.root, id, RaftDirName, WALDirName, "*.wal"))
	if err != nil {
		t.Fatalf("glob the wal of %s: %v", id, err)
	}
	if len(paths) == 0 {
		t.Fatalf("%s has no wal segment", id)
	}
	sort.Strings(paths)
	return paths[len(paths)-1]
}

// scanWALFile 自己解一遍段文件：返回最后一条完整记录的 index、term 与完整字节数。
// 解析不了就停在坏帧起点，和恢复路径看到的边界一致。
func scanWALFile(t *testing.T, path string) (lastIndex, lastTerm uint64, complete int64) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for off := int64(0); off+kvstore.FrameHeaderSize <= int64(len(raw)); {
		size, checksum, err := kvstore.DecodeFrame(raw[off:off+kvstore.FrameHeaderSize], off)
		if err != nil {
			break
		}
		end := off + kvstore.FrameHeaderSize + int64(size)
		if end > int64(len(raw)) {
			break
		}
		payload := raw[off+kvstore.FrameHeaderSize : end]
		if err := kvstore.CheckFrame(size, checksum, payload, off); err != nil {
			break
		}
		term, index, _, err := decodeWALRecord(payload, off)
		if err != nil {
			break
		}
		lastIndex, lastTerm, complete = index, term, end
		off = end
	}
	return lastIndex, lastTerm, complete
}

// TestFaultDiskFull 覆盖写满：小 tmpfs 填到 append 报 ENOSPC，写必须明确失败；
// 腾出空间重起后，已回执的写全部读得回，被拒的那条不能凭空出现。
func TestFaultDiskFull(t *testing.T) {
	if testing.Short() {
		t.Skip("disk fault case")
	}
	root := t.TempDir()
	mountTmpfs(t, root, "1M")
	p := newProcessClusterAtRoot(t, 1, nil, root)
	id := p.ids[0]

	warm := map[string]string{}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("full-warm-%d", i)
		p.writeThrough(key, "v")
		warm[key] = "v"
	}

	payload := strings.Repeat("x", 16<<10)
	acked := map[string]string{}
	failedKey, failedCode := "", ""
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("full-%03d", i)
		status, body, err := p.put(id, key, payload)
		if err == nil && status == http.StatusOK {
			acked[key] = payload
			continue
		}
		failedKey = key
		if body != nil {
			failedCode, _ = body["code"].(string)
		}
		t.Logf("the 1MB tmpfs rejected %s: status=%d code=%s err=%v", key, status, failedCode, err)
		break
	}
	if failedKey == "" {
		t.Fatalf("200 16KB writes all fit in a 1MB tmpfs; the fault never fired")
	}
	if failedCode != "io_error" {
		t.Fatalf("the rejected write carries code %q, want io_error", failedCode)
	}
	if len(acked) == 0 {
		t.Fatalf("no write was acked before the disk filled up")
	}

	// ENOSPC 是写盘失败而不是提交失败：被拒的那条不能进状态机。
	if status, _, err := p.get(id, failedKey); err != nil || status != http.StatusNotFound {
		t.Fatalf("the rejected write %s is readable: status %d err %v", failedKey, status, err)
	}
	if p.cmd[id].ProcessState != nil {
		t.Fatalf("the node exited on ENOSPC; want a clear 500 with the process still serving")
	}

	p.kill9(id)
	remountTmpfs(t, root, "size=16M")
	p.startOne(id)
	p.writeThrough("full-after", "v")

	for key, want := range warm {
		if err := p.waitValue(id, key, want, 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for key, want := range acked {
		if err := p.waitValue(id, key, want, 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if status, _, err := p.get(id, failedKey); err != nil || status != http.StatusNotFound {
		t.Fatalf("the rejected write %s came back after the restart: status %d err %v", failedKey, status, err)
	}
	if err := p.waitValue(id, "full-after", "v", 20*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestFaultReadOnlyDisk 覆盖只读挂载：节点起不来时必须明确报错，不能把半条记录留在盘上；
// 恢复可写后原数据全部回来。
func TestFaultReadOnlyDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("disk fault case")
	}
	root := t.TempDir()
	mountTmpfs(t, root, "8M")
	p := newProcessClusterAtRoot(t, 1, nil, root)
	id := p.ids[0]

	warm := map[string]string{}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("ro-warm-%d", i)
		p.writeThrough(key, "v")
		warm[key] = "v"
	}

	// 只读 remount 会被打开中的可写句柄挡住，先停进程再切挂载。
	p.kill9(id)
	remountTmpfs(t, root, "ro")

	code, stderr := readExitCode(t, p.bin,
		"-node-id", id, "-peers", p.peers, "-data-dir", filepath.Join(root, id), "-log-level", "debug")
	if code == 0 {
		t.Fatalf("the node started on a read-only data dir: %s", firstBytes([]byte(stderr), 200))
	}
	if !strings.Contains(strings.ToLower(stderr), "read-only file system") {
		t.Fatalf("the read-only startup reason did not reach stderr: %q", firstBytes([]byte(stderr), 300))
	}
	t.Logf("read-only startup exited %d with %q", code, firstBytes([]byte(strings.TrimSpace(stderr)), 160))

	remountTmpfs(t, root, "rw")
	p.startOne(id)
	p.writeThrough("ro-after", "v")
	for key, want := range warm {
		if err := p.waitValue(id, key, want, 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.waitValue(id, "ro-after", "v", 20*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestFaultFsyncEIO 用 strace 给 fsync 注入 EIO：写路径报错之后不能有回执写，
// 去掉注入重起，注入期的已确认写全部读得回，被拒的 key 一条都不能出现。
func TestFaultFsyncEIO(t *testing.T) {
	if testing.Short() {
		t.Skip("disk fault case")
	}
	stracePath, err := exec.LookPath("strace")
	if err != nil {
		t.Skip("strace is not installed")
	}
	p := newLoopbackProcessCluster(t, 1, nil, nil)
	id := p.ids[0]

	warm := map[string]string{}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("eio-warm-%d", i)
		p.writeThrough(key, "v")
		warm[key] = "v"
	}
	p.kill9(id)

	// 预热之后重起，把从第一次 fsync 起的调用全部打成 EIO；注入停点不碰预热期的已确认写。
	trace := filepath.Join(t.TempDir(), "fsync-eio.trace")
	wrapper := filepath.Join(t.TempDir(), "node-under-strace")
	script := fmt.Sprintf("#!/bin/sh\nexec %s -f -e trace=fsync -e inject=fsync:error=EIO:when=1+ -o \"$MINIRAFT_STRACE_LOG\" %q \"$@\"\n",
		stracePath, p.bin)
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatalf("write the strace wrapper: %v", err)
	}
	realBin := p.bin
	p.bin = wrapper
	p.env[id] = []string{"MINIRAFT_STRACE_LOG=" + trace}

	p.spawnOne(id)
	if p.waitNodeReady(id, 5*time.Second) {
		t.Logf("%s still answers reads under the injected EIO", id)
	}
	// 第一次选举尝试会先落一次状态：等注入的 EIO 真的打到盘上，再判写路径。
	if !p.waitLog(id, "input/output error", 5*time.Second) {
		t.Fatalf("the injected EIO never reached the state write: %s", p.logTail(id, 10))
	}

	var codes []string
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("eio-rejected-%d", i)
		status, body, err := p.put(id, key, "v")
		if err == nil && status == http.StatusOK {
			t.Fatalf("the write %s was acked while every fsync returned EIO", key)
		}
		if body != nil {
			if code, _ := body["code"].(string); code != "" {
				codes = append(codes, code)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !fileContains(trace, "EIO") {
		t.Fatalf("strace never injected an EIO: %s", p.logTail(id, 8))
	}
	t.Logf("rejected write codes under EIO: %v", codes)

	p.kill9(id)
	p.bin = realBin
	p.clearEnv(id)
	p.startOne(id)
	p.writeThrough("eio-after", "v")
	for key, want := range warm {
		if err := p.waitValue(id, key, want, 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("eio-rejected-%d", i)
		if status, _, err := p.get(id, key); err != nil || status != http.StatusNotFound {
			t.Fatalf("the rejected write %s survived the EIO window: status %d err %v", key, status, err)
		}
	}
	if err := p.waitValue(id, "eio-after", "v", 20*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestFaultTornWALTail 覆盖尾记录不完整：杀进程后截掉最后几字节，重启要么
// 认出半条记录、裁到上一条完整记录并追平，要么明确报状态对不上；都不能静默带半条记录起来。
// 第二个子用例直接追加一条写了一半的记录，确定性地走「识别坏尾并裁剪」这条恢复路径。
func TestFaultTornWALTail(t *testing.T) {
	if testing.Short() {
		t.Skip("disk fault case")
	}
	t.Run("truncated-applied-record", func(t *testing.T) {
		p := newLoopbackProcessCluster(t, 5, nil, nil)
		written := map[string]string{}
		leader, _ := p.writeThrough("torn-warm", "v")
		written["torn-warm"] = "v"
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("torn-%02d", i)
			p.writeThrough(key, "v")
			written[key] = "v"
		}
		var victim string
		for _, id := range p.ids {
			if id != leader {
				victim = id
				break
			}
		}
		p.kill9(victim)

		path := p.lastWALSegment(t, victim)
		lastIndex, _, complete := scanWALFile(t, path)
		if lastIndex < 2 || complete == 0 {
			t.Fatalf("%s has too few complete records to tear: index %d end %d", victim, lastIndex, complete)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if err := os.Truncate(path, info.Size()-3); err != nil {
			t.Fatalf("tear the tail of %s: %v", path, err)
		}
		t.Logf("%s torn: last complete index %d at byte %d of %d", victim, lastIndex, complete, info.Size())

		p.spawnOne(victim)
		if p.waitNodeReady(victim, 5*time.Second) {
			if !fileContains(p.logPath(victim), "wal truncated") {
				t.Fatalf("%s started but never logged the tail truncation: %s", victim, p.logTail(victim, 12))
			}
			t.Logf("%s dropped the torn tail and came back as a follower", victim)
			p.writeThrough("torn-after", "v")
			for key, want := range written {
				if err := p.waitValue(victim, key, want, 30*time.Second); err != nil {
					t.Fatal(err)
				}
			}
			return
		}
		// 被截掉的若是已经应用的那条，状态文件会指到日志末尾之外；这时只能明确报错。
		logs := p.logText(victim)
		if !strings.Contains(logs, "commitIndex is past the end of the wal") {
			t.Fatalf("%s neither recovered nor reported the state mismatch: %s", victim, p.logTail(victim, 12))
		}
		t.Logf("%s refused to start with an explicit state mismatch (accepted outcome)", victim)
	})

	t.Run("torn-append", func(t *testing.T) {
		p := newLoopbackProcessCluster(t, 5, nil, nil)
		written := map[string]string{}
		leader, _ := p.writeThrough("torn2-warm", "v")
		written["torn2-warm"] = "v"
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("torn2-%02d", i)
			p.writeThrough(key, "v")
			written[key] = "v"
		}
		victim := ""
		for _, id := range p.ids {
			if id != leader {
				victim = id
				break
			}
		}
		if code := p.stopOne(victim); code != 0 {
			t.Fatalf("%s exited %d on SIGTERM", victim, code)
		}

		path := p.lastWALSegment(t, victim)
		lastIndex, lastTerm, complete := scanWALFile(t, path)
		if lastIndex == 0 {
			t.Fatalf("%s has no complete record to build on", victim)
		}
		// 一条写到一半的记录：帧头完整、正文少三字节，恢复时只能判坏尾并裁到 complete。
		frame, err := kvstore.EncodeFrame(encodeWALRecord(lastTerm, lastIndex+1, []byte("torn-append")))
		if err != nil {
			t.Fatalf("encode the torn frame: %v", err)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("open %s to append the torn frame: %v", path, err)
		}
		if _, err := f.Write(frame[:len(frame)-3]); err != nil {
			_ = f.Close()
			t.Fatalf("append the torn frame: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close the torn wal: %v", err)
		}

		p.startOne(victim)
		if !fileContains(p.logPath(victim), "wal truncated") {
			t.Fatalf("%s never logged the tail truncation: %s", victim, p.logTail(victim, 12))
		}
		p.writeThrough("torn2-after", "v")
		for key, want := range written {
			if err := p.waitValue(victim, key, want, 30*time.Second); err != nil {
				t.Fatal(err)
			}
		}
		if info, err := os.Stat(path); err == nil && info.Size() < complete {
			t.Fatalf("%s shrank below the last complete record: %d < %d", path, info.Size(), complete)
		}
	})
}

// TestFaultPowerLossUncovered 记未覆盖：kill -9 只代表进程崩溃。
func TestFaultPowerLossUncovered(t *testing.T) {
	t.Skip("真掉电没有装置：kill -9 不丢页缓存，设备缓存是否会丢已 fsync 的数据在这里测不出来")
}

// TestFaultWallClockJumpUncovered 记未覆盖：改墙钟要 CAP_SYS_TIME。
func TestFaultWallClockJumpUncovered(t *testing.T) {
	t.Skip("改墙钟要 CAP_SYS_TIME，普通测试跑不了；墙钟不进状态机的静态审计见 TestClockInjectionKeepsWallTimeOutOfTheStateMachine")
}
