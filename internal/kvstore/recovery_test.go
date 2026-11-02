package kvstore

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/zap"
)

// 直接按帧写一个段，返回文件长度与每条记录的起始偏移，坏尾用例要拿这些位置做断言。
func writeSegment(t *testing.T, path string, recs [][2]string) (int64, []int64) {
	t.Helper()
	var (
		buf   []byte
		start []int64
	)
	for _, rec := range recs {
		entry, err := EncodeEntry(rec[0], rec[1])
		if err != nil {
			t.Fatalf("EncodeEntry = %v", err)
		}
		frame, err := EncodeFrame(entry)
		if err != nil {
			t.Fatalf("EncodeFrame = %v", err)
		}
		start = append(start, int64(len(buf)))
		buf = append(buf, frame...)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return int64(len(buf)), start
}

func appendRaw(t *testing.T, path string, extra []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.Write(extra); err != nil {
		t.Fatalf("append to %s: %v", path, err)
	}
}

func readRaw(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func writeRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// 恢复出来的记录等于按序重放的写入终态，多个段也一样。
func TestRecoveryRebuildsWrittenState(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.MaxFileSize = 400
	lg, records := observerLogger()
	if _, err := lockAndPrepare(cfg, lg); err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	w, err := newWAL(cfg, lg)
	if err != nil {
		t.Fatalf("newWAL = %v", err)
	}

	var want [][2]string
	for i := 0; i < 300; i++ {
		rec := [2]string{fmt.Sprintf("k%02d", i%17), fmt.Sprintf("v%03d", i)}
		appendEntry(t, w, rec[0], rec[1])
		want = append(want, rec)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	logsBefore := len(records.All())
	got, err := scanWAL(cfg, lg)
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if len(got.entries) != len(want) {
		t.Fatalf("recovered %d records, want %d across %d files", len(got.entries), len(want), got.fileCount)
	}
	if got.fileCount < 2 {
		t.Fatalf("fileCount = %d, want several segments under a %d-byte limit", got.fileCount, cfg.MaxFileSize)
	}
	for i := range want {
		if got.entries[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got.entries[i], want[i])
		}
	}
	if got.truncatedBytes != 0 {
		t.Errorf("clean recovery truncated %d bytes", got.truncatedBytes)
	}

	// 干净扫描不逐条打日志，只在坏尾处打。
	if n := len(records.All()) - logsBefore; n != 0 {
		t.Errorf("clean scan logged %d entries: %v", n, records.All()[logsBefore:])
	}

	// 终态：同一批记录按序重放进 map，最后一个写入值胜出。
	state := map[string]string{}
	for _, rec := range got.entries {
		state[rec[0]] = rec[1]
	}
	if state["k00"] != "v289" {
		t.Errorf("terminal state for k00 = %q, want v289", state["k00"])
	}
}

func lockedDir(t *testing.T) Config {
	t.Helper()
	cfg := newTestWALConfig(t)
	if _, err := lockAndPrepare(cfg, zap.NewNop()); err != nil {
		t.Fatalf("lockAndPrepare = %v", err)
	}
	return cfg
}

// 三种起步输入：空目录、只有一个空段、多个段。
func TestRecoveryOnEmptyDir(t *testing.T) {
	cfg := lockedDir(t)

	got, err := scanWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if got.fileCount != 0 || len(got.entries) != 0 || got.truncatedBytes != 0 {
		t.Errorf("empty dir scan = %+v", got)
	}
}

func TestRecoveryOnEmptySegment(t *testing.T) {
	cfg := lockedDir(t)
	writeSegment(t, cfg.WALPath(1), nil)

	got, err := scanWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if got.fileCount != 1 || len(got.entries) != 0 || got.truncatedBytes != 0 {
		t.Errorf("empty segment scan = %+v", got)
	}
}

func TestRecoveryOnSeveralSegments(t *testing.T) {
	cfg := lockedDir(t)
	writeSegment(t, cfg.WALPath(1), [][2]string{{"a", "1"}})
	writeSegment(t, cfg.WALPath(2), [][2]string{{"b", "2"}})
	writeSegment(t, cfg.WALPath(10), [][2]string{{"c", "3"}})

	got, err := scanWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if got.fileCount != 3 || len(got.entries) != 3 {
		t.Fatalf("scan = %+v, want three files with three records", got)
	}
	if want := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}; !equalRecords(got.entries, want) {
		t.Errorf("replay order = %q, want %q", got.entries, want)
	}
}

func equalRecords(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
