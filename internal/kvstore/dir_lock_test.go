package kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
)

const (
	lockChildEnv = "MINIRAFT_LOCK_CHILD"
	lockDirEnv   = "MINIRAFT_LOCK_DIR"
	lockMarker   = "child-locked"
)

func TestSecondProcessIsRejected(t *testing.T) {
	cfg := newTestWALConfig(t)
	lg, records := observerLogger()

	lock, err := lockAndPrepare(cfg, lg)
	if err != nil {
		t.Fatalf("first lockAndPrepare = %v", err)
	}
	defer func() { _ = lock.unlock() }()

	_, err = lockAndPrepare(cfg, lg)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second lockAndPrepare err = %v, want ErrLocked", err)
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) || errno != syscall.EWOULDBLOCK {
		t.Errorf("errno = %v, want EWOULDBLOCK", errno)
	}

	if _, err := os.Stat(cfg.LockPath()); err != nil {
		t.Errorf("lock file missing: %v", err)
	}
	if _, err := os.Stat(cfg.WALDir()); err != nil {
		t.Errorf("wal dir missing: %v", err)
	}

	held := records.FilterMessage("data dir locked").All()
	if len(held) != 1 {
		t.Fatalf("refused lock logged %d times, want 1", len(held))
	}
	if held[0].ContextMap()["lock_path"] != cfg.LockPath() {
		t.Errorf("lock log fields = %v", held[0].ContextMap())
	}
	if len(records.FilterMessage("data dir locked").All()) != 1 {
		t.Error("successful lock was not logged")
	}
}

// 锁随 fd 关闭而释放：同一进程内 Close 之后能再次 Open。
func TestUnlockAllowsReopen(t *testing.T) {
	cfg := newTestWALConfig(t)

	lock, err := lockAndPrepare(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	w, err := newWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("newWAL = %v", err)
	}
	appendEntry(t, w, "k1", "v1")
	if err := w.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := lock.unlock(); err != nil {
		t.Fatalf("unlock = %v", err)
	}

	lock2, err := lockAndPrepare(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("relock = %v", err)
	}
	defer func() { _ = lock2.unlock() }()
	w2, err := newWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("newWAL after relock = %v", err)
	}
	defer func() { _ = w2.Close() }()

	if w2.seq != w.seq {
		t.Errorf("reopened segment %d, want the active %d", w2.seq, w.seq)
	}
	if w2.size != w.size {
		t.Errorf("reopened size %d, want %d", w2.size, w.size)
	}
	appendEntry(t, w2, "k2", "v2")
	if got := readRecords(t, w2.activePath()); len(got) != 2 {
		t.Errorf("file holds %d records after reopening and appending, want 2", len(got))
	}
}

// kill -9 之后锁由内核释放，重启不会被自己留下的状态挡住。
// 子进程分支用同一个测试二进制跑：拿到锁后挂住，等父进程发 SIGKILL。
func TestLockSurvivesProcessDeath(t *testing.T) {
	if dir := os.Getenv(lockDirEnv); dir != "" {
		cfg := DefaultConfig()
		cfg.DataDir = dir
		if _, err := lockAndPrepare(cfg, zap.NewNop()); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(dir+"/"+lockMarker, []byte("held"), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		select {}
	}

	dir := t.TempDir()
	stderr := &bytes.Buffer{}
	cmd := exec.Command(os.Args[0], "-test.run=TestLockSurvivesProcessDeath")
	cmd.Env = append(os.Environ(), lockChildEnv+"=1", lockDirEnv+"="+dir)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	killed := false
	defer func() {
		if !killed {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(dir + "/" + lockMarker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child never took the lock, stderr: %s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	killed = true
	_ = cmd.Wait()

	cfg := DefaultConfig()
	cfg.DataDir = dir
	lock, err := lockAndPrepare(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("relock after kill -9 = %v", err)
	}
	if err := lock.unlock(); err != nil {
		t.Fatalf("unlock = %v", err)
	}
}
