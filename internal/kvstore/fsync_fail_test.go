package kvstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

// 把活跃段的 fd 换成只读：Write 直接 EBADF，模拟追加失败（磁盘真满造不出来，用这个代替，D5）。
// 返回的函数把可写 fd 换回来，测试后面的 Close 因此能正常落盘。
func makeWALWriteFail(t *testing.T, w *wal) func() {
	t.Helper()
	path := w.activePath()
	if err := w.file.Close(); err != nil {
		t.Fatalf("close wal fd: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		t.Fatalf("reopen %s read-only: %v", path, err)
	}
	w.file = f
	return func() { w.file = reopenWritable(t, path) }
}

// 把活跃段的 fd 换成管道：Write 成功、Sync 一定 EINVAL，单独把 fsync 失败隔离出来。
// 管道的读端一直有人排空，写入不会阻塞。
func detachWALToPipe(t *testing.T, w *wal) func() {
	t.Helper()
	path := w.activePath()
	if err := w.file.Close(); err != nil {
		t.Fatalf("close wal fd: %v", err)
	}
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	go func() {
		buf := make([]byte, 4096)
		for {
			if n, err := readEnd.Read(buf); n <= 0 || err != nil {
				return
			}
		}
	}()
	w.file = writeEnd
	return func() {
		_ = writeEnd.Close()
		_ = readEnd.Close()
		w.file = reopenWritable(t, path)
	}
}

func reopenWritable(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen %s writable: %v", path, err)
	}
	return f
}

// fsync 失败 → 失效：标志置位、后续 Put 一律 ErrIO 且原因只报一次、Get 仍可用（D5）。
func TestFsyncFailureDisablesWritesOnly(t *testing.T) {
	cfg := kvConfig(t)
	k, records := openKV(t, cfg)

	const acked = DefaultBatchSize - 1
	for i := 0; i < acked; i++ {
		mustPut(t, k, fmt.Sprintf("k%03d", i), "v")
	}
	if got := syncEvents(records); len(got) != 0 {
		t.Fatalf("%d records synced %d times, want the batch still open", acked, len(got))
	}

	restore := detachWALToPipe(t, k.wal)
	err := k.Put(context.Background(), fmt.Sprintf("k%03d", acked), "v")
	if !errors.Is(err, ErrIO) {
		t.Fatalf("Put at the fsync boundary = %v, want ErrIO", err)
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) || errno != syscall.EINVAL {
		t.Errorf("errno = %v, want the fsync failure surfaced as EINVAL", errno)
	}
	if !k.WriteDisabled() {
		t.Fatalf("write path still open after a failed fsync")
	}

	// 已攒在批次里的 63 条不受影响，那条失败的写入没进 map。
	for i := 0; i < acked; i++ {
		if v, err := k.Get(fmt.Sprintf("k%03d", i)); err != nil || v != "v" {
			t.Fatalf("Get k%03d = %q, %v after the write path closed, want v", i, v, err)
		}
	}
	if _, err := k.Get(fmt.Sprintf("k%03d", acked)); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("the fsync-failed record is visible: %v", err)
	}

	// 后续 Put 全部失败，失效原因只报一次，也不再碰追加路径。
	for i := 0; i < 5; i++ {
		if err := k.Put(context.Background(), "later", "v"); !errors.Is(err, ErrIO) {
			t.Fatalf("Put after disable = %v, want ErrIO", err)
		}
	}
	if n := records.FilterMessage("wal disabled").Len(); n != 1 {
		t.Errorf("disable logged %d times, want 1", n)
	}
	if n := records.FilterMessage("append failed").Len(); n != 0 {
		t.Errorf("a disabled wal still tried to append %d times", n)
	}
	disabled := records.FilterMessage("wal disabled").All()[0].ContextMap()
	if disabled["failed_at_seq"] != uint64(acked+1) {
		t.Errorf("failed_at_seq = %v, want %d", disabled["failed_at_seq"], acked+1)
	}
	if disabled["component"] != ComponentWAL {
		t.Errorf("component = %v, want %q", disabled["component"], ComponentWAL)
	}

	// Flush 与 Close 都不该在已失效的写路径上再报一次 fsync 错。
	if err := k.Flush(); !errors.Is(err, ErrIO) {
		t.Errorf("Flush on a disabled wal = %v, want ErrIO", err)
	}
	restore()
	if err := k.Close(); err != nil {
		t.Errorf("Close on a disabled wal = %v, want the pending batch left alone", err)
	}
	if n := records.FilterMessage("wal disabled").Len(); n != 1 {
		t.Errorf("disable logged %d times by Close, want 1", n)
	}

	// 重启后读到的，必须等于盘上真的有的那些记录。
	reopened, _ := openKV(t, cfg)
	onDisk := recordsOnDisk(t, cfg)
	if len(onDisk) != acked {
		t.Fatalf("disk holds %d records, want the %d acked before the failure", len(onDisk), acked)
	}
	for _, rec := range onDisk {
		if v, err := reopened.Get(rec[0]); err != nil || v != rec[1] {
			t.Fatalf("after restart %q = %q, %v, want %q", rec[0], v, err, rec[1])
		}
	}
}

// 滚动建新段被权限挡住：Put 返回 ErrIO，但不进失效——此时换段前的批次已经落盘，边界是已知的（D5）。
func TestRolloverFailureDoesNotDisable(t *testing.T) {
	cfg := kvConfig(t)
	// 一条 ("k1","v1") 的记录 21B，第二条必然越过 32B 的段上限，走到建段这一步。
	cfg.MaxFileSize = 32
	k, records := openKV(t, cfg)

	mustPut(t, k, "k1", "v1")
	if err := os.Chmod(cfg.WALDir(), 0o555); err != nil {
		t.Fatalf("chmod wal dir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(cfg.WALDir(), 0o755) })

	err := k.Put(context.Background(), "k2", "v2")
	if !errors.Is(err, ErrIO) {
		t.Fatalf("Put across a blocked rollover = %v, want ErrIO", err)
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) || errno != syscall.EACCES {
		t.Errorf("errno = %v, want EACCES from creating the new segment", errno)
	}
	if k.WriteDisabled() {
		t.Errorf("write path disabled although the previous batch is already durable")
	}
	if _, err := k.Get("k2"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("the rejected put is visible in the map: %v", err)
	}
	if n := records.FilterMessage("wal disabled").Len(); n != 0 {
		t.Errorf("a known-boundary failure logged disable %d times", n)
	}

	// 权限回来后继续可写，且已确认的记录一条不少。
	if err := os.Chmod(cfg.WALDir(), 0o755); err != nil {
		t.Fatalf("restore chmod: %v", err)
	}
	mustPut(t, k, "k2", "v2")
	mustPut(t, k, "k3", "v3")
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	reopened, _ := openKV(t, cfg)
	for key, want := range map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"} {
		if v, err := reopened.Get(key); err != nil || v != want {
			t.Errorf("after restart %s = %q, %v, want %q", key, v, err, want)
		}
	}
	if got := recordsOnDisk(t, cfg); len(got) != 3 {
		t.Errorf("disk holds %d records, want three: the rejected put must not be among them", len(got))
	}
	if n := len(records.FilterMessage("wal rolled over").All()); n == 0 {
		t.Errorf("no rollover logged after the dir became writable again")
	}
}

// 失效标志不持久：重启走正常恢复流程，写路径重新打开。
func TestDisabledStateDoesNotSurviveRestart(t *testing.T) {
	cfg := kvConfig(t)
	cfg.BatchSize = 1
	k, records := openKV(t, cfg)

	mustPut(t, k, "k1", "v1")
	restore := detachWALToPipe(t, k.wal)
	if err := k.Put(context.Background(), "k2", "v2"); !errors.Is(err, ErrIO) {
		t.Fatalf("Put with a broken sync = %v, want ErrIO", err)
	}
	if !k.WriteDisabled() {
		t.Fatalf("expected the write path to be disabled")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	restore()
	if n := records.FilterMessage("wal disabled").Len(); n != 1 {
		t.Errorf("disable logged %d times, want 1", n)
	}

	reopened, _ := openKV(t, cfg)
	if reopened.WriteDisabled() {
		t.Errorf("a fresh Open inherits the disabled flag")
	}
	mustPut(t, reopened, "k3", "v3")
	if v, err := reopened.Get("k1"); err != nil || v != "v1" {
		t.Errorf("after restart k1 = %q, %v, want v1", v, err)
	}
	if _, err := reopened.Get("k2"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("the record that went to the pipe came back: %v", err)
	}
}
