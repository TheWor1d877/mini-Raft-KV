package kvstore

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// 建目录、拿锁、开 WAL；测试结束释放锁。
func openLockedWAL(t *testing.T, cfg Config, lg *zap.Logger) *wal {
	t.Helper()
	lock, err := lockAndPrepare(cfg, lg)
	if err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	t.Cleanup(func() { _ = lock.unlock() })

	w, err := newWAL(cfg, lg)
	if err != nil {
		lock.unlock()
		t.Fatalf("newWAL = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func newTestWALConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "debug"
	return cfg
}

// 记录级断言：测试自己按 §5 的判定顺序把文件读回来。
func readRecords(t *testing.T, path string) [][2]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out [][2]string
	for off := 0; off < len(data); {
		if len(data)-off < FrameHeaderSize {
			t.Fatalf("header at offset %d has only %d bytes", off, len(data)-off)
		}
		size, checksum, err := DecodeFrame(data[off:off+FrameHeaderSize], int64(off))
		if err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		end := off + FrameHeaderSize + int(size)
		if end > len(data) {
			t.Fatalf("entry at offset %d runs %d bytes past EOF", off, end-len(data))
		}
		entry := data[off+FrameHeaderSize : end]
		if err := CheckFrame(size, checksum, entry, int64(off)); err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		key, value, err := DecodeEntry(entry, int64(off))
		if err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		out = append(out, [2]string{key, value})
		off = end
	}
	return out
}

func appendEntry(t *testing.T, w *wal, key, value string) uint64 {
	t.Helper()
	entry, err := EncodeEntry(key, value)
	if err != nil {
		t.Fatalf("EncodeEntry(%q,%q) = %v", key, value, err)
	}
	opSeq, err := w.append(entry)
	if err != nil {
		t.Fatalf("append(%q,%q) = %v", key, value, err)
	}
	return opSeq
}

func observerLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, records := observer.New(zapcore.DebugLevel)
	return zap.New(core), records
}

func syncEvents(records *observer.ObservedLogs) []map[string]any {
	var out []map[string]any
	for _, e := range records.FilterMessage("wal synced").All() {
		out = append(out, e.ContextMap())
	}
	return out
}

func TestAppendReadsBackInOrder(t *testing.T) {
	cfg := newTestWALConfig(t)
	w := openLockedWAL(t, cfg, zap.NewNop())

	const total = 1000
	for i := 0; i < total; i++ {
		key, value := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)
		if seq := appendEntry(t, w, key, value); seq != uint64(i+1) {
			t.Fatalf("op_seq = %d, want %d", seq, i+1)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush = %v", err)
	}

	got := readRecords(t, w.activePath())
	if len(got) != total {
		t.Fatalf("recovered %d records, want %d", len(got), total)
	}
	for i, rec := range got {
		if want := [2]string{fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)}; rec != want {
			t.Fatalf("record %d = %q, want %q", i, rec, want)
		}
	}
}

// 追加序必须等于字节序：偏移单调递增，且每条记录的起点等于 w.size 的推进值。
func TestAppendOffsetMatchesFilePosition(t *testing.T) {
	cfg := newTestWALConfig(t)
	w := openLockedWAL(t, cfg, zap.NewNop())

	for i := 0; i < 50; i++ {
		before := w.size
		appendEntry(t, w, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		data, err := os.ReadFile(w.activePath())
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if int64(len(data)) != w.size {
			t.Fatalf("file is %d bytes, wal thinks %d", len(data), w.size)
		}
		if len(readRecords(t, w.activePath())) != i+1 {
			t.Fatalf("record %d is not readable at offset %d", i, before)
		}
	}
}

func TestWriteFailureLeavesNoHalfRecord(t *testing.T) {
	cfg := newTestWALConfig(t)
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)

	appendEntry(t, w, "k1", "v1")
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush = %v", err)
	}
	sizeBefore := w.size

	// 只读 fd 造写入失败：追加返回错误，批次计数与文件内容都不动。
	w.file.Close()
	f, err := os.OpenFile(w.activePath(), os.O_RDONLY, 0o644)
	if err != nil {
		t.Fatalf("reopen read-only: %v", err)
	}
	w.file = f

	entry, err := EncodeEntry("k2", "v2")
	if err != nil {
		t.Fatalf("EncodeEntry = %v", err)
	}
	seq, err := w.append(entry)
	if seq != 0 {
		t.Errorf("append returned op_seq %d on failure", seq)
	}
	if !errors.Is(err, ErrIO) {
		t.Fatalf("append err = %v, want ErrIO", err)
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) || errno != syscall.EBADF {
		t.Errorf("errno = %v, want EBADF", errno)
	}
	if w.size != sizeBefore {
		t.Errorf("size moved to %d after a failed write", w.size)
	}
	if w.pending != 0 {
		t.Errorf("batch counter = %d after a failed write, want 0", w.pending)
	}
	if got := readRecords(t, w.activePath()); len(got) != 1 {
		t.Errorf("file holds %d records after a failed append, want 1", len(got))
	}

	failed := records.FilterMessage("append failed").All()
	if len(failed) != 1 {
		t.Fatalf("append failure logged %d times", len(failed))
	}
	fields := failed[0].ContextMap()
	if fields["op_seq"] != uint64(2) || fields["errno"] != int64(syscall.EBADF) {
		t.Errorf("failure log fields = %v", fields)
	}
	if _, ok := fields["duration_ms"]; !ok {
		t.Errorf("failure log has no duration_ms: %v", fields)
	}
}

// 帧层的 4MB 上限是第二道闸：entry 超过 len 上限时不落任何字节（2.1 §5）。
func TestOversizedEntryWritesNothing(t *testing.T) {
	cfg := newTestWALConfig(t)
	w := openLockedWAL(t, cfg, zap.NewNop())

	if _, err := w.append(make([]byte, MaxEntrySize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("append err = %v, want ErrTooLarge", err)
	}
	if w.size != 0 || w.pending != 0 {
		t.Errorf("oversized write moved size=%d pending=%d", w.size, w.pending)
	}
	if _, err := os.Stat(w.activePath()); err != nil {
		t.Errorf("active segment disappeared: %v", err)
	}
}
