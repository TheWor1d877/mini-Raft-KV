package kvstore

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/zap"
)

// 第 64 条前后各发生一次 Sync（D6，批次大小取自 config 的默认值）。
func TestSyncOnlyAtBatchBoundary(t *testing.T) {
	cfg := newTestWALConfig(t)
	if cfg.BatchSize != DefaultBatchSize {
		t.Fatalf("batch size = %d, want the config default %d", cfg.BatchSize, DefaultBatchSize)
	}
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)

	for i := 1; i < DefaultBatchSize; i++ {
		appendEntry(t, w, fmt.Sprintf("k%03d", i), "v")
	}
	if got := syncEvents(records); len(got) != 0 {
		t.Fatalf("%d records produced %d syncs, want none", DefaultBatchSize-1, len(got))
	}

	appendEntry(t, w, fmt.Sprintf("k%03d", DefaultBatchSize), "v")
	got := syncEvents(records)
	if len(got) != 1 {
		t.Fatalf("64th record produced %d syncs, want 1", len(got))
	}
	if got[0]["batch_count"] != int64(DefaultBatchSize) {
		t.Errorf("batch_count = %v, want %d", got[0]["batch_count"], DefaultBatchSize)
	}
	if w.pending != 0 {
		t.Errorf("counter = %d after a full batch sync, want 0", w.pending)
	}

	for i := DefaultBatchSize + 1; i < 2*DefaultBatchSize; i++ {
		appendEntry(t, w, fmt.Sprintf("k%03d", i), "v")
	}
	if got := syncEvents(records); len(got) != 1 {
		t.Fatalf("127 records produced %d syncs, want 1", len(got))
	}
	appendEntry(t, w, fmt.Sprintf("k%03d", 2*DefaultBatchSize), "v")
	if got := syncEvents(records); len(got) != 2 || got[1]["batch_count"] != int64(DefaultBatchSize) {
		t.Fatalf("128 records produced %v, want two full batches", syncEvents(records))
	}
}

// 批次未满时 Flush 立即落盘，计数器归零，已同步偏移等于文件末尾。
func TestFlushSyncsPartialBatch(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.BatchSize = 1000
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)

	for i := 0; i < 3; i++ {
		appendEntry(t, w, fmt.Sprintf("k%d", i), "v")
	}
	if got := syncEvents(records); len(got) != 0 {
		t.Fatalf("three records synced %d times before Flush", len(got))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush = %v", err)
	}

	got := syncEvents(records)
	if len(got) != 1 || got[0]["batch_count"] != int64(3) {
		t.Fatalf("Flush events = %v, want one batch of 3", got)
	}
	if w.pending != 0 {
		t.Errorf("counter = %d after Flush, want 0", w.pending)
	}
	if w.synced != w.size {
		t.Errorf("synced offset %d lags file size %d", w.synced, w.size)
	}
	info, err := os.Stat(w.activePath())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != w.size {
		t.Errorf("file is %d bytes, wal thinks %d", info.Size(), w.size)
	}
}

// 不变量：每次 Sync 落盘的偏移，正好覆盖到已返回成功的那条记录的末尾。
func TestSyncedOffsetCoversAppendedRecords(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.BatchSize = 7
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)

	syncs := 0
	for i := 0; i < 40; i++ {
		appendEntry(t, w, fmt.Sprintf("k%02d", i), fmt.Sprintf("value-%d", i))

		if w.synced > w.size {
			t.Fatalf("synced offset %d ahead of file size %d", w.synced, w.size)
		}
		events := syncEvents(records)
		if len(events) == syncs {
			continue
		}
		syncs = len(events)
		if w.synced != w.size {
			t.Fatalf("sync %d covered %d bytes, last record ends at %d", syncs, w.synced, w.size)
		}
		info, err := os.Stat(w.activePath())
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Size() != w.synced {
			t.Fatalf("sync claims %d durable bytes, file holds %d", w.synced, info.Size())
		}
	}
	if syncs != 40/cfg.BatchSize {
		t.Errorf("syncs = %d, want one per %d records", syncs, cfg.BatchSize)
	}
}

func TestCloseFlushesRemainingBatch(t *testing.T) {
	cfg := newTestWALConfig(t)
	lg, records := observerLogger()
	w, err := newWALAfterLock(t, cfg, lg)
	if err != nil {
		t.Fatalf("newWAL = %v", err)
	}

	for i := 0; i < 5; i++ {
		appendEntry(t, w, fmt.Sprintf("k%d", i), "v")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	syncs := syncEvents(records)
	if len(syncs) != 1 || syncs[0]["batch_count"] != int64(5) {
		t.Errorf("Close produced %v, want one sync of 5", syncs)
	}
	closes := records.FilterMessage("wal closed").All()
	if len(closes) != 1 {
		t.Fatalf("close logged %d times", len(closes))
	}
	fields := closes[0].ContextMap()
	if fields["pending_batch"] != int64(5) {
		t.Errorf("pending_batch = %v, want 5", fields["pending_batch"])
	}
	if _, ok := fields["duration_ms"]; !ok {
		t.Errorf("close log has no duration_ms: %v", fields)
	}
	if got := readRecords(t, w.activePath()); len(got) != 5 {
		t.Errorf("file holds %d records after Close, want 5", len(got))
	}
}

// -fsync=false 只跳过系统调用：批次计数照归零，写路径不被失效标志关掉。
// 造错方式是把 fd 换成管道——管道上 Write 成功、Sync 必定 EINVAL，能证明真的没调 Sync。
func TestFsyncDisabledSkipsSyncCall(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.BatchSize = 2
	cfg.Fsync = false
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)
	restore := detachWALToPipe(t, w)
	defer restore()

	for i := 0; i < 6; i++ {
		appendEntry(t, w, fmt.Sprintf("k%d", i), "v")
	}
	if w.disabled() {
		t.Errorf("wal disabled with -fsync=false")
	}
	if w.pending != 0 {
		t.Errorf("counter = %d, want the batch cleared even without a sync", w.pending)
	}
	events := syncEvents(records)
	if len(events) != 3 {
		t.Fatalf("six records cleared %d batches, want one sync event per two records", len(events))
	}
	for _, e := range events {
		if e["fsync"] != false {
			t.Errorf("sync event claims fsync=%v, want false", e["fsync"])
		}
	}
	if n := records.FilterMessage("wal disabled").Len(); n != 0 {
		t.Errorf("disable log fired %d times without a sync call", n)
	}
}

func mustEntry(t *testing.T, key, value string) []byte {
	t.Helper()
	entry, err := EncodeEntry(key, value)
	if err != nil {
		t.Fatalf("EncodeEntry = %v", err)
	}
	return entry
}

// 拿锁之后开 WAL，测试自己负责关闭。
func newWALAfterLock(t *testing.T, cfg Config, lg *zap.Logger) (*wal, error) {
	t.Helper()
	lock, err := lockAndPrepare(cfg, lg)
	if err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	t.Cleanup(func() { _ = lock.unlock() })
	return newWAL(cfg, lg)
}
