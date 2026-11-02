package kvstore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"go.uber.org/zap"
)

func TestRolloverAtThreshold(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.MaxFileSize = 1000
	w := openLockedWAL(t, cfg, zap.NewNop())

	for i := 0; i < 1000; i++ {
		appendEntry(t, w, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush = %v", err)
	}

	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("%d segments after 1000 records, want more than one", len(segs))
	}
	for i, s := range segs {
		if s.seq != uint64(i+1) {
			t.Fatalf("segment %d has seq %d, want %d", i, s.seq, i+1)
		}
		if i < len(segs)-1 && s.size > cfg.MaxFileSize {
			t.Errorf("closed segment %d is %d bytes, above the %d limit", s.seq, s.size, cfg.MaxFileSize)
		}
	}

	var all [][2]string
	for _, s := range segs {
		all = append(all, readRecords(t, s.path)...)
	}
	if len(all) != 1000 {
		t.Fatalf("read back %d records across %d segments, want 1000", len(all), len(segs))
	}
	for i, rec := range all {
		if want := [2]string{fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i)}; rec != want {
			t.Fatalf("record %d = %q, want %q", i, rec, want)
		}
	}

	// D7：换段之后老文件一个字节都不动。
	first := segs[0].path
	before, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read first segment: %v", err)
	}
	for i := 1000; i < 1100; i++ {
		appendEntry(t, w, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
	after, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("reread first segment: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Error("the first segment changed after later writes")
	}
}

// 换段点必须先把老段落盘：每条 rollover 日志前面紧挨着一条 fsync 日志，
// 且 rollover 报出的 size_bytes 等于该文件在盘上的实际长度。
func TestRolloverSyncsBeforeSwitching(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.MaxFileSize = 200
	cfg.BatchSize = 1 << 20
	lg, records := observerLogger()
	w := openLockedWAL(t, cfg, lg)

	for i := 0; i < 100; i++ {
		appendEntry(t, w, fmt.Sprintf("k%03d", i), "v")
	}

	all := records.All()
	rollovers := 0
	for i, e := range all {
		if e.Message != "wal rolled over" {
			continue
		}
		rollovers++
		if i == 0 || all[i-1].Message != "wal synced" {
			t.Fatalf("rollover #%d is not preceded by a sync: %q then %q", rollovers, all[i-1].Message, e.Message)
		}
		fields := e.ContextMap()
		from, _ := fields["from_file"].(string)
		info, err := os.Stat(from)
		if err != nil {
			t.Fatalf("stat %s: %v", from, err)
		}
		if want, ok := fields["size_bytes"].(int64); !ok || info.Size() != want {
			t.Fatalf("rollover claims %v durable bytes for %s, file holds %d", fields["size_bytes"], from, info.Size())
		}
		if fields["to_file"] == from {
			t.Fatalf("rollover kept the same file: %v", fields)
		}
	}
	if rollovers == 0 {
		t.Fatal("no rollover happened")
	}
	if syncs := len(syncEvents(records)); syncs < rollovers {
		t.Errorf("%d syncs for %d rollovers", syncs, rollovers)
	}
	if w.size > cfg.MaxFileSize {
		t.Errorf("active segment is %d bytes above the limit", w.size)
	}
}

// 序号补零到八位：字典序与序号序必须一致。
func TestSegmentNamesSortWithBothOrders(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.MaxFileSize = 120
	w := openLockedWAL(t, cfg, zap.NewNop())

	for i := 0; i < 60; i++ {
		appendEntry(t, w, fmt.Sprintf("k%02d", i), "v")
	}
	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	if len(segs) < 12 {
		t.Fatalf("%d segments, want at least 12 to exercise ordering", len(segs))
	}

	names := make([]string, 0, len(segs))
	padded := regexp.MustCompile(`^\d{8}\.wal$`)
	for _, s := range segs {
		name := filepath.Base(s.path)
		if !padded.MatchString(name) {
			t.Fatalf("%q is not eight zero-padded digits", name)
		}
		names = append(names, name)
	}
	byName := append([]string{}, names...)
	sort.Strings(byName)
	if !equalNames(names, byName) {
		t.Errorf("numeric order %v differs from lexicographic %v", names, byName)
	}
	for i, s := range segs {
		if i > 0 && s.seq <= segs[i-1].seq {
			t.Fatalf("segment order broken at %d: %d after %d", i, s.seq, segs[i-1].seq)
		}
	}

	var replay [][2]string
	for _, s := range segs {
		replay = append(replay, readRecords(t, s.path)...)
	}
	if len(replay) != 60 {
		t.Fatalf("ordered replay gave %d records, want 60", len(replay))
	}
	if replay[0][0] != "k00" || replay[59][0] != "k59" {
		t.Errorf("replay order = %q ... %q", replay[0][0], replay[59][0])
	}
}

// 崩溃在「创建新段」与「改名」之间留下的临时文件：恢复时删掉，序号回到真实最大段。
func TestUncommittedTempFileIsIgnoredAndDeleted(t *testing.T) {
	cfg := newTestWALConfig(t)
	cfg.MaxFileSize = 200
	cfg.BatchSize = 1 << 20

	w, err := newWALAfterLock(t, cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("newWAL = %v", err)
	}
	for i := 0; i < 10; i++ {
		appendEntry(t, w, fmt.Sprintf("k%02d", i), "v")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	lastSeq := segs[len(segs)-1].seq

	stray := cfg.WALTmpPath(lastSeq + 3)
	if err := os.WriteFile(stray, bytes.Repeat([]byte{0xff}, 4096), 0o644); err != nil {
		t.Fatalf("create stray temp file: %v", err)
	}

	w2, err := newWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("newWAL with a stray file = %v", err)
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Errorf("stray %s survived recovery", stray)
	}
	if w2.seq != lastSeq {
		t.Errorf("reopened at segment %d, want the active %d", w2.seq, lastSeq)
	}
	appendEntry(t, w2, "after-recovery", "v")
	if err := w2.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	segs, err = listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	var total int
	for _, s := range segs {
		total += len(readRecords(t, s.path))
		if filepath.Ext(s.path) == ".tmp" {
			t.Errorf("a .tmp file is listed as a segment: %s", s.path)
		}
	}
	if total != 11 {
		t.Errorf("%d records after recovery, want 11", total)
	}
}

func equalNames(a, b []string) bool {
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
