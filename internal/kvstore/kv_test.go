package kvstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// 起一个 KV，日志走 observer，测试结束关掉。
func openKV(t *testing.T, cfg Config) (*KV, *observer.ObservedLogs) {
	t.Helper()
	lg, records := observerLogger()
	k, err := Open(cfg, lg)
	if err != nil {
		t.Fatalf("Open(%+v) = %v", cfg, err)
	}
	t.Cleanup(func() { _ = k.Close() })
	return k, records
}

func kvConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "debug"
	return cfg
}

func mustPut(t *testing.T, k *KV, key, value string) {
	t.Helper()
	if err := k.Put(context.Background(), key, value); err != nil {
		t.Fatalf("Put(%q,%q) = %v", key, value, err)
	}
}

// 按 §6 的段序把盘上的记录全部读回来，用于和 map 对账。
func recordsOnDisk(t *testing.T, cfg Config) [][2]string {
	t.Helper()
	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	var out [][2]string
	for _, s := range segs {
		out = append(out, readRecords(t, s.path)...)
	}
	return out
}

func TestGetMissingKeyIsErrKeyNotFound(t *testing.T) {
	k, _ := openKV(t, kvConfig(t))

	value, err := k.Get("absent")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get on absent key = %v, want ErrKeyNotFound", err)
	}
	if value != "" {
		t.Errorf("Get returned %q alongside a not-found error", value)
	}
}

// 空串 key、空串 value 都是合法写入，且与「不存在」区分得开（D4）。
func TestEmptyKeyAndEmptyValueAreDistinctFromAbsent(t *testing.T) {
	cfg := kvConfig(t)
	k, _ := openKV(t, cfg)

	mustPut(t, k, "", "")
	mustPut(t, k, "empty-value", "")

	for _, key := range []string{"", "empty-value"} {
		value, err := k.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) = %v, want the empty string to be a stored value", key, err)
		}
		if value != "" {
			t.Errorf("Get(%q) = %q, want empty string", key, value)
		}
	}
	if _, err := k.Get("never-written"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("absent key after two empty-value puts = %v", err)
	}

	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	reopened, _ := openKV(t, cfg)
	if value, err := reopened.Get(""); err != nil || value != "" {
		t.Errorf("after restart Get(\"\") = %q, %v", value, err)
	}
}

// 同 key 同 value 重放十次，终态与一次相同；WAL 里十次都留着，重放后仍然一致。
func TestReplayingSamePutIsIdempotent(t *testing.T) {
	cfg := kvConfig(t)
	k, _ := openKV(t, cfg)

	for i := 0; i < 10; i++ {
		mustPut(t, k, "k1", "v1")
	}
	mustPut(t, k, "k1", "v2")
	for i := 0; i < 10; i++ {
		mustPut(t, k, "k1", "v2")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	if got := recordsOnDisk(t, cfg); len(got) != 21 {
		t.Fatalf("wal holds %d records, want every replay appended", len(got))
	}
	reopened, _ := openKV(t, cfg)
	if value, err := reopened.Get("k1"); err != nil || value != "v2" {
		t.Fatalf("after restart k1 = %q, %v, want v2", value, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	third, _ := openKV(t, cfg)
	if value, err := third.Get("k1"); err != nil || value != "v2" {
		t.Errorf("second restart k1 = %q, %v, want v2", value, err)
	}
}

// 超限在编码层就拒掉：不落字节、不改 map。
func TestOversizedKeyAndValueRejected(t *testing.T) {
	cfg := kvConfig(t)
	k, _ := openKV(t, cfg)

	if err := k.Put(context.Background(), strings.Repeat("k", MaxKeySize+1), "v"); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized key = %v, want ErrTooLarge", err)
	}
	if err := k.Put(context.Background(), "k", strings.Repeat("v", MaxValueSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized value = %v, want ErrTooLarge", err)
	}
	if got := recordsOnDisk(t, cfg); len(got) != 0 {
		t.Errorf("oversized puts left %d records on disk", len(got))
	}
	for _, key := range []string{strings.Repeat("k", MaxKeySize+1), "k"} {
		if _, err := k.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Get(%q) after a rejected put = %v", key, err)
		}
	}

	// 恰好上限必须能写进去，否则上面两条拒的是合法输入。
	mustPut(t, k, strings.Repeat("k", MaxKeySize), strings.Repeat("v", MaxValueSize))
	if got := recordsOnDisk(t, cfg); len(got) != 1 {
		t.Errorf("boundary put recorded %d times", len(got))
	}
}

// 追加失败：不更新 map、不产生记录，之后重启与盘上一致。
func TestPutFailureLeavesMapUnchanged(t *testing.T) {
	cfg := kvConfig(t)
	k, records := openKV(t, cfg)

	mustPut(t, k, "k1", "v1")
	if err := k.Flush(); err != nil {
		t.Fatalf("Flush = %v", err)
	}
	restore := makeWALWriteFail(t, k.wal)

	var errno syscall.Errno
	err := k.Put(context.Background(), "k2", "v2")
	if !errors.Is(err, ErrIO) {
		t.Fatalf("Put with a broken wal fd = %v, want ErrIO", err)
	}
	if !errors.As(err, &errno) || errno != syscall.EBADF {
		t.Errorf("errno = %v, want EBADF in the error chain", errno)
	}
	if _, err := k.Get("k2"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("k2 visible after a failed put: %v", err)
	}
	if v, err := k.Get("k1"); err != nil || v != "v1" {
		t.Errorf("k1 = %q, %v after a failed put, want v1", v, err)
	}
	if got := recordsOnDisk(t, cfg); len(got) != 1 {
		t.Errorf("disk holds %d records after a failed put, want 1", len(got))
	}
	if n := records.FilterMessage("put rejected").Len(); n != 1 {
		t.Errorf("put rejected logged %d times, want 1", n)
	}
	restore()
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	reopened, _ := openKV(t, cfg)
	if v, err := reopened.Get("k1"); err != nil || v != "v1" {
		t.Errorf("after restart k1 = %q, %v, want v1", v, err)
	}
	if _, err := reopened.Get("k2"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("the failed put came back after restart: %v", err)
	}
}

// Put 的 op_seq 与 Debug 日志里的 req_id 一一对上（2.1 §8）。
func TestPutLogsReqIDAndOpSeq(t *testing.T) {
	k, records := openKV(t, kvConfig(t))

	if err := k.Put(WithRequestID(context.Background(), "req-42"), "k1", "v1"); err != nil {
		t.Fatalf("Put = %v", err)
	}
	entries := records.FilterMessage("put applied").All()
	if len(entries) != 1 {
		t.Fatalf("put applied logged %d times", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["req_id"] != "req-42" {
		t.Errorf("req_id = %v, want req-42", fields["req_id"])
	}
	if fields["op_seq"] != uint64(1) {
		t.Errorf("op_seq = %v, want 1", fields["op_seq"])
	}
	if fields["component"] != ComponentKV {
		t.Errorf("component = %v, want %q", fields["component"], ComponentKV)
	}

	// 超限在编码层就拒掉，不进写路径，也就不该有「put rejected」。
	if err := k.Put(WithRequestID(context.Background(), "req-99"), strings.Repeat("a", MaxKeySize+1), "v"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized put = %v, want ErrTooLarge", err)
	}
	if n := records.FilterMessage("put rejected").Len(); n != 0 {
		t.Errorf("an oversized put logged %d write failures", n)
	}

	// 追加失败也要带 req_id，否则排查时串不起一次请求。
	restore := makeWALWriteFail(t, k.wal)
	if err := k.Put(WithRequestID(context.Background(), "req-77"), "k2", "v2"); err == nil {
		t.Fatalf("put on a broken wal fd succeeded")
	}
	failed := records.FilterMessage("put rejected").All()
	if len(failed) != 1 {
		t.Fatalf("put rejected logged %d times, want 1", len(failed))
	}
	if got := failed[0].ContextMap()["req_id"]; got != "req-77" {
		t.Errorf("failure log req_id = %v, want req-77", got)
	}
	restore()
}

// Close 落掉未满的一批并释放目录锁；重启能拿到锁并读回全部写入。
func TestCloseFlushesAndReleasesLock(t *testing.T) {
	cfg := kvConfig(t)
	cfg.BatchSize = 1000
	k, _ := openKV(t, cfg)

	for i := 0; i < 5; i++ {
		mustPut(t, k, fmt.Sprintf("k%d", i), "v")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if _, err := Open(cfg, zap.NewNop()); err != nil {
		t.Fatalf("second Open after Close = %v, want the lock released", err)
	}
	if got := recordsOnDisk(t, cfg); len(got) != 5 {
		t.Errorf("disk holds %d records after Close, want 5", len(got))
	}
}

// 目录被别的进程占着时 Open 报 ErrLocked，并且不会把别人的锁放掉。
func TestOpenOnLockedDirReturnsErrLocked(t *testing.T) {
	cfg := kvConfig(t)
	lock, err := lockAndPrepare(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	t.Cleanup(func() { _ = lock.unlock() })

	if _, err := Open(cfg, zap.NewNop()); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open on a locked dir = %v, want ErrLocked", err)
	}
	// 别人的锁还在：能再拿一次，说明失败那次没有误释放持有者的 fd。
	again, err := lockDir(cfg, zap.NewNop())
	if err == nil {
		again.unlock()
		t.Fatalf("lock taken by a failed Open")
	}
}

// Open 的非法配置不建目录、不留半把锁。
func TestOpenRejectsInvalidConfig(t *testing.T) {
	cfg := kvConfig(t)
	cfg.BatchSize = 0
	dir := cfg.DataDir

	lg, records := observerLogger()
	if _, err := Open(cfg, lg); err == nil {
		t.Fatalf("Open with batch size 0 succeeded")
	} else if !errors.Is(err, ErrIO) && !strings.Contains(err.Error(), "batch size") {
		t.Errorf("Open err = %v, want the config complaint", err)
	}
	if n := records.FilterMessage("open failed").Len(); n != 1 {
		t.Errorf("open failure logged %d times, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(dir, LockFileName)); err == nil {
		t.Errorf("invalid config created the lock file in %s", dir)
	}
}
