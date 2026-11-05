package kvstore

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// 日志进内存 observer，级别放到 debug：Apply 的记录就在 debug 档。
func observerLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, records := observer.New(zapcore.DebugLevel)
	return zap.New(core), records
}

func kvConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "debug"
	return cfg
}

// 导出面：Snapshot() 给出全量键值且顺序稳定，Restore() 整体替换 map（5.1 §3）。
func TestSnapshotExportsEveryPairInKeyOrder(t *testing.T) {
	k, _ := openKV(t, kvConfig(t))
	k.Apply("b", "2")
	k.Apply("a", "1")
	k.Apply("c", "")

	want := []Entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: ""}}
	first, second := k.Snapshot(), k.Snapshot()
	if len(first) != len(want) {
		t.Fatalf("exported %d entries, want %d", len(first), len(want))
	}
	for i := range want {
		if first[i] != want[i] {
			t.Fatalf("export %d = %+v, want %+v: the order must be ascending by key", i, first[i], want[i])
		}
	}
	if !slices.Equal(first, second) {
		t.Fatalf("the second export differs from the first: %+v vs %+v", first, second)
	}

	k.Restore(first)
	if got := k.Snapshot(); !slices.Equal(got, want) {
		t.Fatalf("export after restoring the same content = %+v, want %+v", got, want)
	}
}

// 空状态机的导出是空切片；Restore 之后被替换掉的键读不到，留下的键读得到快照里的值。
func TestRestoreReplacesTheWholeMap(t *testing.T) {
	k, _ := openKV(t, kvConfig(t))

	if got := k.Snapshot(); len(got) != 0 {
		t.Fatalf("export of an empty state machine = %+v, want no entries", got)
	}

	k.Apply("drop", "old")
	k.Apply("keep", "old")
	k.Restore([]Entry{{Key: "keep", Value: "new"}})

	if _, err := k.Get("drop"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("the replaced key is still readable: %v", err)
	}
	value, err := k.Get("keep")
	if err != nil || value != "new" {
		t.Fatalf(`Get(keep) = %q %v, want the snapshot value "new"`, value, err)
	}
	if got := k.Snapshot(); len(got) != 1 || got[0] != (Entry{Key: "keep", Value: "new"}) {
		t.Fatalf("export after the replace = %+v, want the one entry the snapshot carried", got)
	}

	k.Restore(nil)
	if got := k.Snapshot(); len(got) != 0 {
		t.Fatalf("export after an empty restore = %+v, want no entries", got)
	}
	if _, err := k.Get("keep"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a restore of nothing left a key behind: %v", err)
	}
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
	k, _ := openKV(t, kvConfig(t))

	k.Apply("", "")
	k.Apply("empty-value", "")

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
		t.Errorf("absent key after two empty-value applies = %v", err)
	}
}

// 同 key 同 value 重放十次再改一次值，终态与「只写最后一笔」相同：
// Apply 是纯内存赋值，重复回放没有副作用（docs/8 §二，恢复与补 apply 都能从头再来）。
func TestReplayingSameApplyIsIdempotent(t *testing.T) {
	k, _ := openKV(t, kvConfig(t))

	for i := 0; i < 10; i++ {
		k.Apply("k1", "v1")
	}
	for i := 0; i < 11; i++ {
		k.Apply("k1", "v2")
	}
	if value, err := k.Get("k1"); err != nil || value != "v2" {
		t.Fatalf("k1 = %q, %v, want v2", value, err)
	}

	// 从头再来一遍同序回放，终态逐 key 相同（D4 的幂等口径）。
	fresh, _ := openKV(t, kvConfig(t))
	for i := 0; i < 10; i++ {
		fresh.Apply("k1", "v1")
	}
	for i := 0; i < 11; i++ {
		fresh.Apply("k1", "v2")
	}
	if value, err := fresh.Get("k1"); err != nil || value != "v2" {
		t.Errorf("replayed from scratch k1 = %q, %v, want v2", value, err)
	}
}

// Apply 的 debug 日志带 key（截断）、value_len 与指纹，component 是 kv（2.1 §8）。
func TestApplyLogsEntryApplied(t *testing.T) {
	k, records := openKV(t, kvConfig(t))
	k.Apply("k1", "v1")

	entries := records.FilterMessage("entry applied").All()
	if len(entries) != 1 {
		t.Fatalf("entry applied logged %d times, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["component"] != ComponentKV {
		t.Errorf("component = %v, want %q", fields["component"], ComponentKV)
	}
	if fields["key"] != "k1" || fields["value_len"] != int64(2) {
		t.Errorf("fields = %v, want key k1 and value_len 2", fields)
	}
	if fp, ok := fields["value_fp"].(string); !ok || len(fp) != 8 {
		t.Errorf("value_fp = %v, want an 8-hex-digit fingerprint", fields["value_fp"])
	}
}

// req_id 在 context 里进出：入口放进去，写路径的失败日志取出来（2.1 §8）。
func TestRequestIDRoundTripsThroughContext(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-42")
	if got := RequestID(ctx); got != "req-42" {
		t.Errorf("RequestID = %q, want req-42", got)
	}
	if got := RequestID(context.Background()); got != "" {
		t.Errorf("RequestID on a bare ctx = %q, want empty", got)
	}
	if got := RequestID(nil); got != "" {
		t.Errorf("RequestID(nil) = %q, want empty", got)
	}
}

// Close 只释放目录锁：状态机不落盘，重启后的 map 是空的，
// 数据由 raft 日志回放重建（docs/8 §二）。
func TestCloseReleasesLockAndReopensEmpty(t *testing.T) {
	cfg := kvConfig(t)
	k, _ := openKV(t, cfg)

	for i := 0; i < 5; i++ {
		k.Apply("k", "v")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	reopened, _ := openKV(t, cfg)
	if _, err := reopened.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("KV holds data after restart: %v, want an empty map for the log to replay", err)
	}
	reopened.Apply("k", "v2")
	if v, err := reopened.Get("k"); err != nil || v != "v2" {
		t.Errorf("apply after restart = %q, %v, want v2", v, err)
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
	cfg.Addr = ""

	lg, records := observerLogger()
	if _, err := Open(cfg, lg); err == nil {
		t.Fatal("Open with an empty listen addr succeeded")
	} else if !strings.Contains(err.Error(), "listen addr") {
		t.Errorf("Open err = %v, want the config complaint", err)
	}
	if n := records.FilterMessage("open failed").Len(); n != 1 {
		t.Errorf("open failure logged %d times, want 1", n)
	}
	if _, err := os.Stat(cfg.LockPath()); err == nil {
		t.Errorf("invalid config created the lock file at %s", cfg.LockPath())
	}
}
