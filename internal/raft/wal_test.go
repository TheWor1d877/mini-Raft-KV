package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

func openTestRaftLog(t *testing.T, cfg Config) *wal {
	t.Helper()
	l, err := openWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = l.close() })
	return l
}

func mustAppend(t *testing.T, l *wal, term, index uint64, cmd string) *walEntry {
	t.Helper()
	e, err := l.appendEntry(term, index, []byte(cmd))
	if err != nil {
		t.Fatalf("append term=%d index=%d: %v", term, index, err)
	}
	return e
}

// raftFrame 按 3.1 §5 的口径手工组一条完整记录，用来伪造坏尾。
func raftFrame(t *testing.T, term, index uint64, cmd string) []byte {
	t.Helper()
	frame, err := kvstore.EncodeFrame(encodeWALRecord(term, index, []byte(cmd)))
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return frame
}

func plantRaftSegment(t *testing.T, cfg Config, seq uint64, data []byte) {
	t.Helper()
	if err := os.WriteFile(cfg.WALPath(seq), data, 0o644); err != nil {
		t.Fatalf("plant segment %d: %v", seq, err)
	}
}

func readRaftSegment(t *testing.T, cfg Config, seq uint64) []byte {
	t.Helper()
	data, err := os.ReadFile(cfg.WALPath(seq))
	if err != nil {
		t.Fatalf("read segment %d: %v", seq, err)
	}
	return data
}

func testSegments(t *testing.T, cfg Config) []walSegment {
	t.Helper()
	segs, err := listWALSegments(cfg)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	return segs
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func countLogs(logs *observer.ObservedLogs, msg string) int {
	return logs.FilterMessage(msg).Len()
}

// entry 的字节布局逐字节核对：op 1B(=0x10)｜term 8B｜index 8B｜command，全部大端。
func TestRaftEntryByteLayout(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 3, 1, "set k=v")
	if err := l.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	data := readRaftSegment(t, cfg, 1)
	command := []byte("set k=v")
	if want := kvstore.FrameHeaderSize + walEntryHeaderSize + len(command); len(data) != want {
		t.Fatalf("segment bytes = %d, want %d", len(data), want)
	}
	head := data[:kvstore.FrameHeaderSize]
	entry := data[kvstore.FrameHeaderSize:]
	if got := binary.BigEndian.Uint32(head[:4]); int(got) != len(entry) {
		t.Errorf("frame len = %d, entry is %d bytes", got, len(entry))
	}
	if err := kvstore.CheckFrame(uint32(len(entry)), binary.BigEndian.Uint32(head[4:8]), entry, 0); err != nil {
		t.Errorf("frame crc: %v", err)
	}
	if entry[0] != OpWALRecord {
		t.Errorf("op = 0x%02x, want 0x10", entry[0])
	}
	if got := binary.BigEndian.Uint64(entry[walEntryTermOffset:]); got != 3 {
		t.Errorf("term field = %d, want 3", got)
	}
	if got := binary.BigEndian.Uint64(entry[walEntryIndexOffset:]); got != 1 {
		t.Errorf("index field = %d, want 1", got)
	}
	if got := string(entry[walEntryHeaderSize:]); got != "set k=v" {
		t.Errorf("command = %q, want %q", got, "set k=v")
	}
}

// 空 command 是 no-op 条目的形状（3.1 §7），写读都得通。
func TestRaftLogAcceptsEmptyCommand(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 1, 1, "")
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	e, ok := again.entryAt(1)
	if !ok {
		t.Fatal("no-op entry missing after reopen")
	}
	if len(e.cmd) != 0 {
		t.Errorf("command = %q, want empty", e.cmd)
	}
	if e.term != 1 {
		t.Errorf("term = %d, want 1", e.term)
	}
}

func TestRaftLogRoundTripAcrossReopen(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	want := []struct {
		term, index uint64
		cmd         string
	}{
		{1, 1, "a"}, {1, 2, "b"}, {2, 3, "c"}, {2, 4, "d"}, {3, 5, "e"},
	}
	for _, w := range want {
		mustAppend(t, l, w.term, w.index, w.cmd)
	}
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	if got := again.length(); got != len(want) {
		t.Fatalf("entries after reopen = %d, want %d", got, len(want))
	}
	for _, w := range want {
		e, ok := again.entryAt(w.index)
		if !ok {
			t.Fatalf("index %d missing after reopen", w.index)
		}
		if e.term != w.term || string(e.cmd) != w.cmd {
			t.Errorf("index %d = (term %d, %q), want (term %d, %q)",
				w.index, e.term, e.cmd, w.term, w.cmd)
		}
	}
	if got := again.lastIndex(); got != 5 {
		t.Errorf("lastIndex = %d, want 5", got)
	}
	if got := again.lastTerm(); got != 3 {
		t.Errorf("lastTerm = %d, want 3", got)
	}
	// 追加位置接在末尾之后，段号仍是 1。
	mustAppend(t, again, 3, 6, "f")
	if got := len(testSegments(t, cfg)); got != 1 {
		t.Errorf("segments after reopen-append = %d, want 1", got)
	}
}

// 不变式 6：index 必须接着最后一条，跳号在追加当场就拒。
func TestRaftLogAppendRejectsIndexHole(t *testing.T) {
	l := openTestRaftLog(t, testConfig(t))
	mustAppend(t, l, 1, 1, "a")

	if _, err := l.appendEntry(1, 3, []byte("c")); !errors.Is(err, ErrStateMismatch) {
		t.Errorf("append index 3 = %v, want ErrStateMismatch", err)
	}
	if _, err := l.appendEntry(1, 2, []byte("second")); err != nil {
		t.Errorf("append index 2 after a rejected hole: %v", err)
	}
	if got := l.lastIndex(); got != 2 {
		t.Errorf("lastIndex = %d, want 2", got)
	}
}

// 盘上已有日志中间有洞：恢复扫描报错，不当坏尾裁掉（裁剪会把复制逻辑的错藏起来）。
func TestScanRaftLogReportsHoleAsStateMismatch(t *testing.T) {
	cfg := testConfig(t)
	data := append(raftFrame(t, 1, 1, "a"), raftFrame(t, 1, 3, "c")...)
	plantRaftSegment(t, cfg, 1, data)

	_, err := scanWAL(cfg, zap.NewNop())
	if !errors.Is(err, ErrStateMismatch) {
		t.Fatalf("scan with a hole = %v, want ErrStateMismatch", err)
	}
	var mismatch *stateMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error %v does not expose the offending index", err)
	}
	if mismatch.index != 3 {
		t.Errorf("reported index = %d, want 3", mismatch.index)
	}
	if got := fileSize(t, cfg.WALPath(1)); got != int64(len(data)) {
		t.Errorf("hole log truncated to %d bytes, want it left intact at %d", got, len(data))
	}
}

// 五种坏记录各截到记录起点，第二次重启不再重复截（3.2 §3 第二行）。
func TestScanRaftLogTruncatesBadTail(t *testing.T) {
	cases := []struct {
		name   string
		tail   func(t *testing.T) []byte
		reason string
	}{
		{"incomplete header", func(*testing.T) []byte { return []byte{0x00, 0x00, 0x00, 0x11, 0xAB} },
			"incomplete header"},
		{"len out of range", func(*testing.T) []byte {
			head := make([]byte, kvstore.FrameHeaderSize)
			binary.BigEndian.PutUint32(head[:4], kvstore.MaxEntrySize+1)
			binary.BigEndian.PutUint32(head[4:], 0x11223344)
			return head
		}, "exceeds the"},
		{"entry shorter than declared", func(*testing.T) []byte {
			head := make([]byte, kvstore.FrameHeaderSize)
			binary.BigEndian.PutUint32(head[:4], 64)
			binary.BigEndian.PutUint32(head[4:], 0x11223344)
			return append(head, []byte("only ten")...)
		}, "entry shorter than declared length"},
		{"crc mismatch", func(t *testing.T) []byte {
			frame := raftFrame(t, 1, 2, "b")
			frame[len(frame)-1] ^= 0xFF
			return frame
		}, "crc mismatch"},
		{"op is not 0x10", func(t *testing.T) []byte {
			entry := make([]byte, walEntryHeaderSize+2)
			entry[0] = 0x01
			binary.BigEndian.PutUint64(entry[walEntryTermOffset:], 1)
			binary.BigEndian.PutUint64(entry[walEntryIndexOffset:], 2)
			frame, err := kvstore.EncodeFrame(entry)
			if err != nil {
				t.Fatalf("encode frame: %v", err)
			}
			return frame
		}, "unknown op 0x01"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(t)
			good := raftFrame(t, 1, 1, "a")
			tail := c.tail(t)
			plantRaftSegment(t, cfg, 1, append(append([]byte(nil), good...), tail...))

			core, logs := observer.New(zapcore.DebugLevel)
			entries, err := scanWAL(cfg, zap.New(core))
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if len(entries) != 1 || entries[0].index != 1 {
				t.Fatalf("recovered %d entries, want only the good prefix", len(entries))
			}
			if got := fileSize(t, cfg.WALPath(1)); got != int64(len(good)) {
				t.Errorf("segment length after repair = %d, want the record start at %d", got, len(good))
			}
			if n := countLogs(logs, "corrupt record tail"); n != 1 {
				t.Errorf("corrupt record tail logs = %d, want 1", n)
			}
			got := logs.FilterMessage("corrupt record tail").All()[0]
			reason := fieldOf(got, "reason")
			if reason == "" {
				t.Error("corrupt record tail log carries no reason field")
			} else if !strings.Contains(reason, c.reason) {
				t.Errorf("reason = %q, want it to contain %q", reason, c.reason)
			}
			if n := countLogs(logs, "wal truncated"); n != 1 {
				t.Errorf("wal truncated logs = %d, want 1", n)
			}

			// 第二次重启：坏尾已经在盘上被抹掉，不该再报、也不该再截。
			core2, logs2 := observer.New(zapcore.DebugLevel)
			entries2, err := scanWAL(cfg, zap.New(core2))
			if err != nil {
				t.Fatalf("second scan: %v", err)
			}
			if len(entries2) != 1 {
				t.Errorf("second scan recovered %d entries, want 1", len(entries2))
			}
			if n := countLogs(logs2, "corrupt record tail") + countLogs(logs2, "wal truncated"); n != 0 {
				t.Errorf("second scan logged %d repair events, want 0", n)
			}
			if got := fileSize(t, cfg.WALPath(1)); got != int64(len(good)) {
				t.Errorf("second scan changed the length to %d, want %d", got, len(good))
			}

			// 裁剪之后还能接着追加。
			l := openTestRaftLog(t, cfg)
			mustAppend(t, l, 1, 2, "b")
			if got := l.length(); got != 2 {
				t.Errorf("entries after repairing and re-appending = %d, want 2", got)
			}
		})
	}
}

// fieldOf 取一条日志里的字符串字段。
func fieldOf(entry observer.LoggedEntry, key string) string {
	for _, f := range entry.Context {
		if f.Key == key {
			return f.String
		}
	}
	return ""
}

// truncateTo 落在帧起点：截掉的字节正好是那条记录的帧长，截后同 index 重新追加读回新值（3.1 §5）。
func TestTruncateToLandsOnFrameStartAndAllowsReappend(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	f1 := raftFrame(t, 1, 1, "a")
	f2 := raftFrame(t, 1, 2, "b")
	mustAppend(t, l, 1, 1, "a")
	mustAppend(t, l, 1, 2, "b")
	mustAppend(t, l, 2, 3, "c")

	if err := l.truncateTo(2); err != nil {
		t.Fatalf("truncateTo(2): %v", err)
	}
	wantLen := int64(len(f1) + len(f2))
	if got := fileSize(t, cfg.WALPath(1)); got != wantLen {
		t.Errorf("segment length = %d, want %d (frame end of index 2)", got, wantLen)
	}
	if got := l.lastIndex(); got != 2 {
		t.Errorf("lastIndex = %d, want 2", got)
	}

	mustAppend(t, l, 3, 3, "rewritten")
	rewritten := raftFrame(t, 3, 3, "rewritten")
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	if got := again.length(); got != 3 {
		t.Fatalf("entries after reopen = %d, want 3", got)
	}
	e, ok := again.entryAt(3)
	if !ok {
		t.Fatal("index 3 missing after re-append")
	}
	if e.term != 3 || string(e.cmd) != "rewritten" {
		t.Errorf("index 3 = (term %d, %q), want (term 3, \"rewritten\")", e.term, e.cmd)
	}
	if got := fileSize(t, cfg.WALPath(1)); got != wantLen+int64(len(rewritten)) {
		t.Errorf("segment length = %d, want %d", got, wantLen+int64(len(rewritten)))
	}
}

// index=0 清空整棵日志：段文件保留但长度为 0，日志从 1 重新起（3.1 §5）。
func TestTruncateToZeroClearsWholeLog(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	for i := uint64(1); i <= 3; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("v%d", i))
	}
	if err := l.truncateTo(0); err != nil {
		t.Fatalf("truncateTo(0): %v", err)
	}
	for _, s := range testSegments(t, cfg) {
		if s.size != 0 {
			t.Errorf("segment %d length = %d, want 0", s.seq, s.size)
		}
	}
	if got := l.lastIndex(); got != 0 {
		t.Errorf("lastIndex = %d, want 0", got)
	}
	if got := l.lastTerm(); got != 0 {
		t.Errorf("lastTerm = %d, want 0", got)
	}
	mustAppend(t, l, 2, 1, "fresh")
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	if got := again.length(); got != 1 {
		t.Fatalf("entries after reopen = %d, want 1", got)
	}
	e, _ := again.entryAt(1)
	if e.term != 2 || string(e.cmd) != "fresh" {
		t.Errorf("index 1 = (term %d, %q), want (term 2, \"fresh\")", e.term, e.cmd)
	}
}

// 三分支截断（3.1 §5）：段内、段头连同其后的段清空、整棵日志清空。
// 段文件截到 0 长度不删文件，追加位置挪回截短的那个段。
func TestTruncateToAcrossSegments(t *testing.T) {
	cfg := testConfig(t)
	one := raftFrame(t, 1, 1, "payload-0")
	cfg.MaxFileSize = int64(2 * len(one))
	if got := len(testSegments(t, cfg)); got != 0 {
		t.Fatalf("segments before the first open = %d, want none", got)
	}

	// 段上限压到每条 60B，六个条目分落三段：1、2｜3、4｜5、6。
	l := openTestRaftLog(t, cfg)
	for i := uint64(1); i <= 6; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("payload-%d", i))
	}
	segs := testSegments(t, cfg)
	if got := len(segs); got != 3 {
		t.Fatalf("segments = %d, want 3", got)
	}
	full := int64(2 * len(one))

	// 分支一：截到 index 5——落在段 3 内部，盘上长度收到那条记录的帧终点。
	if err := l.truncateTo(5); err != nil {
		t.Fatalf("truncateTo(5): %v", err)
	}
	assertRaftLengths(t, cfg, []int64{full, full, int64(len(one))})
	if got := l.length(); got != 5 {
		t.Errorf("entries = %d, want 5", got)
	}
	if got := l.seq; got != 3 {
		t.Errorf("active segment = %d, want the segment holding index 5", got)
	}
	mustAppend(t, l, 1, 6, "payload-6")
	assertRaftLengths(t, cfg, []int64{full, full, full})

	// 分支二：截到 index 2——段 1 停在末尾，其后的段全部截到 0 长度、文件保留。
	if err := l.truncateTo(2); err != nil {
		t.Fatalf("truncateTo(2): %v", err)
	}
	assertRaftLengths(t, cfg, []int64{full, 0, 0})
	if got := len(testSegments(t, cfg)); got != 3 {
		t.Fatalf("segments after truncation = %d, want the emptied files kept, 3", got)
	}
	if got := l.lastIndex(); got != 2 {
		t.Errorf("lastIndex = %d, want 2", got)
	}
	// 同 index 重新追加：段 1 已满，滚动进段 2，段号不跳号。
	mustAppend(t, l, 2, 3, "payload-3")
	assertRaftLengths(t, cfg, []int64{full, int64(len(one)), 0})
	if got := l.seq; got != 2 {
		t.Errorf("active segment after re-append = %d, want the reused segment 2", got)
	}

	// 分支三：index=0 清空全部段。
	if err := l.truncateTo(0); err != nil {
		t.Fatalf("truncateTo(0): %v", err)
	}
	assertRaftLengths(t, cfg, []int64{0, 0, 0})
	mustAppend(t, l, 3, 1, "fresh")
	assertRaftLengths(t, cfg, []int64{int64(len(raftFrame(t, 3, 1, "fresh"))), 0, 0})
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	if got := again.length(); got != 1 {
		t.Fatalf("entries after reopen = %d, want 1", got)
	}
	e, _ := again.entryAt(1)
	if e.term != 3 || string(e.cmd) != "fresh" {
		t.Errorf("index 1 = (term %d, %q), want (term 3, \"fresh\")", e.term, e.cmd)
	}
	if _, ok := again.firstIndexOfTerm(2); ok {
		t.Error("dropped term 2 is still in the first-index map")
	}
}

func assertRaftLengths(t *testing.T, cfg Config, want []int64) {
	t.Helper()
	segs := testSegments(t, cfg)
	if len(segs) != len(want) {
		t.Fatalf("segments = %d, want %d", len(segs), len(want))
	}
	for i, w := range want {
		if segs[i].seq != uint64(i+1) {
			t.Fatalf("segment %d has seq %d, want a gapless run", i, segs[i].seq)
		}
		if got := fileSize(t, cfg.WALPath(segs[i].seq)); got != w {
			t.Errorf("segment %d length = %d, want %d", segs[i].seq, got, w)
		}
	}
}

func tmpFilesIn(t *testing.T, cfg Config) int {
	t.Helper()
	entries, err := os.ReadDir(cfg.WALDir())
	if err != nil {
		t.Fatalf("read the log dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			n++
		}
	}
	return n
}

// 截到日志末尾之后是空操作；截掉整个 term 时两个映射一起重建（D20）。
func TestTruncateToRebuildsTermIndexMaps(t *testing.T) {
	l := openTestRaftLog(t, testConfig(t))
	shapes := []struct {
		term, index uint64
	}{
		{1, 1}, {1, 2}, {2, 3}, {3, 4}, {3, 5},
	}
	for _, s := range shapes {
		mustAppend(t, l, s.term, s.index, "x")
	}
	if err := l.truncateTo(99); err != nil {
		t.Fatalf("truncateTo beyond the tail: %v", err)
	}
	if got := l.length(); got != 5 {
		t.Errorf("entries = %d after a no-op truncate, want 5", got)
	}

	// 截到 4：term 3 只剩一条，term 2 的首末都在。
	if err := l.truncateTo(4); err != nil {
		t.Fatalf("truncateTo(4): %v", err)
	}
	if first, ok := l.firstIndexOfTerm(3); !ok || first != 4 {
		t.Errorf("term 3 first index = %d/%v, want 4/true", first, ok)
	}
	if last, ok := l.lastIndexOfTerm(3); !ok || last != 4 {
		t.Errorf("term 3 last index = %d/%v, want 4/true", last, ok)
	}
	if first, ok := l.firstIndexOfTerm(2); !ok || first != 3 {
		t.Errorf("term 2 first index = %d/%v, want 3/true", first, ok)
	}
	if last, ok := l.lastIndexOfTerm(1); !ok || last != 2 {
		t.Errorf("term 1 last index = %d/%v, want 2/true", last, ok)
	}

	// 截到 2：term 2、3 整个消失，映射里不许留下指向已丢条目的 index。
	if err := l.truncateTo(2); err != nil {
		t.Fatalf("truncateTo(2): %v", err)
	}
	for _, term := range []uint64{2, 3} {
		if first, ok := l.firstIndexOfTerm(term); ok {
			t.Errorf("term %d still maps to first index %d after being dropped", term, first)
		}
		if last, ok := l.lastIndexOfTerm(term); ok {
			t.Errorf("term %d still maps to last index %d after being dropped", term, last)
		}
	}
	if first, ok := l.firstIndexOfTerm(1); !ok || first != 1 {
		t.Errorf("term 1 first index = %d/%v, want 1/true", first, ok)
	}
	if got := l.lastTerm(); got != 1 {
		t.Errorf("lastTerm = %d, want 1", got)
	}
}

// 段上限压小后滚动：段号连续不跳号，滚动前后追加连续，重启读回全部（T20.2）。
func TestRaftLogRollsOverWithContiguousSeq(t *testing.T) {
	cfg := testConfig(t)
	frame := raftFrame(t, 1, 1, "payload-0")
	cfg.MaxFileSize = int64(2*len(frame) + kvstore.FrameHeaderSize)

	l := openTestRaftLog(t, cfg)
	const n = 7
	for i := uint64(1); i <= n; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("payload-%d", i))
	}
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segs := testSegments(t, cfg)
	if len(segs) < 3 {
		t.Fatalf("segments = %d, want the squeezed cap to split the log into at least 3", len(segs))
	}
	for i, s := range segs {
		if s.seq != uint64(i+1) {
			t.Fatalf("segment %d has seq %d, want a gapless run from 1", i, s.seq)
		}
		if s.size > cfg.MaxFileSize {
			t.Errorf("segment %d length %d exceeds the cap %d", s.seq, s.size, cfg.MaxFileSize)
		}
	}
	if left := tmpFilesIn(t, cfg); left != 0 {
		t.Errorf("%d .tmp segments survived rollover, want none", left)
	}

	again := openTestRaftLog(t, cfg)
	if got := again.length(); got != n {
		t.Fatalf("entries after reopen = %d, want %d", got, n)
	}
	for i := uint64(1); i <= n; i++ {
		e, ok := again.entryAt(i)
		if !ok {
			t.Fatalf("index %d missing after reopen", i)
		}
		if want := fmt.Sprintf("payload-%d", i); string(e.cmd) != want {
			t.Errorf("index %d command = %q, want %q", i, e.cmd, want)
		}
	}
	mustAppend(t, again, 2, n+1, "next")
}

// 跨段读取时每条记录的位置记账要对得上，按 index 截断全靠它（3.1 §5）。
func TestRaftLogRecordsFramePositionsAcrossSegments(t *testing.T) {
	cfg := testConfig(t)
	frame := raftFrame(t, 1, 1, "payload-0")
	cfg.MaxFileSize = int64(2 * len(frame))

	l := openTestRaftLog(t, cfg)
	for i := uint64(1); i <= 4; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("payload-%d", i))
	}
	for i := uint64(1); i <= 4; i++ {
		e, ok := l.entryAt(i)
		if !ok {
			t.Fatalf("index %d missing", i)
		}
		if e.size != int64(len(frame)) {
			t.Errorf("index %d frame size = %d, want %d", i, e.size, len(frame))
		}
		data := readRaftSegment(t, cfg, e.seq)
		if int64(len(data)) < e.offset+e.size {
			t.Fatalf("index %d points past the end of segment %d", i, e.seq)
		}
		term, index, cmd, err := decodeWALRecord(data[e.offset+kvstore.FrameHeaderSize:e.offset+e.size], e.offset)
		if err != nil {
			t.Fatalf("decode at index %d: %v", i, err)
		}
		if term != 1 || index != i || string(cmd) != fmt.Sprintf("payload-%d", i) {
			t.Errorf("position (%d,%d) on segment %d holds (term %d, index %d, %q)",
				e.offset, e.size, e.seq, term, index, cmd)
		}
	}
}

// 崩溃留下的空临时文件在扫描前删掉，绝不当正式段读（D13、D15）。
func TestRaftLogRemovesTmpResidue(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 1, 1, "a")
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	residue := cfg.WALTmpPath(2)
	if err := os.WriteFile(residue, nil, 0o644); err != nil {
		t.Fatalf("plant tmp residue: %v", err)
	}

	again := openTestRaftLog(t, cfg)
	if _, err := os.Stat(residue); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("residue %s still there: %v", filepath.Base(residue), err)
	}
	if got := len(testSegments(t, cfg)); got != 1 {
		t.Errorf("segments = %d, want only the committed one", got)
	}
	if got := again.length(); got != 1 {
		t.Errorf("entries = %d, want 1", got)
	}
}

// 把活跃段的 fd 换成管道：Write 正常、Sync 一定 EINVAL，单独隔离出 fsync 失败。
func detachRaftLogToPipe(t *testing.T, l *wal) {
	t.Helper()
	if err := l.file.Close(); err != nil {
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
	l.file = writeEnd
	t.Cleanup(func() {
		_ = writeEnd.Close()
		_ = readEnd.Close()
	})
}

// D5、D19：raft 日志 Sync 失败只关写路径，读路径继续服务，盘上长度不变。
func TestRaftLogSyncFailureDisablesWritesOnly(t *testing.T) {
	cfg := testConfig(t)
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 1, 1, "a")
	if err := l.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	durable := fileSize(t, cfg.WALPath(1))

	detachRaftLogToPipe(t, l)
	if err := l.sync(); !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("sync on a pipe = %v, want ErrIO", err)
	}
	if !l.disabled() {
		t.Fatal("the log stayed writable after a failed Sync")
	}
	if _, err := l.appendEntry(1, 2, []byte("b")); !errors.Is(err, kvstore.ErrIO) {
		t.Errorf("append after disable = %v, want ErrIO", err)
	}
	if err := l.truncateTo(1); !errors.Is(err, kvstore.ErrIO) {
		t.Errorf("truncate after disable = %v, want ErrIO", err)
	}
	if got := l.length(); got != 1 {
		t.Errorf("read path length = %d, want 1", got)
	}
	if e, ok := l.entryAt(1); !ok || string(e.cmd) != "a" {
		t.Errorf("read path lost index 1: %v", ok)
	}
	if got := fileSize(t, cfg.WALPath(1)); got != durable {
		t.Errorf("committed file changed to %d bytes, want %d", got, durable)
	}
}

// D19 的对照档：-raft-fsync=false 时 sync 一次系统调用都不发，追加照常。
func TestRaftLogFsyncFalseSkipsTheSyscall(t *testing.T) {
	cfg := testConfig(t)
	cfg.Fsync = false
	l := openTestRaftLog(t, cfg)
	mustAppend(t, l, 1, 1, "a")

	// 管道上的 Sync 一定失败；返回 nil 就说明这次根本没发系统调用。
	detachRaftLogToPipe(t, l)
	if err := l.sync(); err != nil {
		t.Fatalf("sync with Fsync=false = %v, want no syscall", err)
	}
	if l.disabled() {
		t.Error("Fsync=false still disabled the write path")
	}
	mustAppend(t, l, 1, 2, "b")
	if got := l.length(); got != 2 {
		t.Errorf("entries = %d, want 2", got)
	}
}

// entriesFrom 按 index 取一段，Leader 组 AppendEntries 用（3.4 T23.1）。
func TestEntriesFromWindow(t *testing.T) {
	l := openTestRaftLog(t, testConfig(t))
	for i := uint64(1); i <= 5; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("v%d", i))
	}
	if got := l.entriesFrom(3, 2); len(got) != 2 || got[0].index != 3 || got[1].index != 4 {
		t.Errorf("entriesFrom(3,2) = %d entries, want index 3 and 4", len(got))
	}
	if got := l.entriesFrom(6, 0); len(got) != 0 {
		t.Errorf("entriesFrom past the tail = %d, want none", len(got))
	}
	if got := l.entriesFrom(0, 0); len(got) != 5 || got[0].index != 1 {
		t.Errorf("entriesFrom(0) = %d entries, want the whole log from 1", len(got))
	}
}

// 滚动与截断两条事件的字段按 3.1 §10 的表来；M13 的恢复数据要靠这两行读数。
func TestRaftLogRolloverAndTruncateFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := testConfig(t)
	one := raftFrame(t, 1, 1, "payload-0")
	cfg.MaxFileSize = int64(2 * len(one))

	l, err := openWAL(cfg, zap.New(core))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := uint64(1); i <= 4; i++ {
		mustAppend(t, l, 1, i, fmt.Sprintf("payload-%d", i))
	}
	rolled := logs.FilterMessage("wal rolled over").All()
	if len(rolled) != 1 {
		t.Fatalf("wal rolled over logs = %d, want 1", len(rolled))
	}
	rollFields := rolled[0].ContextMap()
	if got := rollFields["from_file"]; got != cfg.WALPath(1) {
		t.Errorf("from_file = %v, want %q", got, cfg.WALPath(1))
	}
	if got := rollFields["to_file"]; got != cfg.WALPath(2) {
		t.Errorf("to_file = %v, want %q", got, cfg.WALPath(2))
	}
	if got, ok := rollFields["size_bytes"]; !ok || got != int64(2*len(one)) {
		t.Errorf("size_bytes = %v/%v, want the full first segment", got, ok)
	}

	if err := l.truncateTo(1); err != nil {
		t.Fatalf("truncateTo(1): %v", err)
	}
	cut := logs.FilterMessage("wal suffix dropped").All()
	if len(cut) != 1 {
		t.Fatalf("wal suffix dropped logs = %d, want 1", len(cut))
	}
	for key, want := range map[string]any{
		"kept_through_index": uint64(1),
		"dropped_entries":    int64(3),
		"entries_left":       int64(1),
	} {
		if got := cut[0].ContextMap()[key]; got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Sync 失败那条事件的字段（3.1 §10 的 wal disabled 行）。
func TestRaftLogDisabledLogFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := testConfig(t)
	l, err := openWAL(cfg, zap.New(core))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	mustAppend(t, l, 1, 1, "a")
	detachRaftLogToPipe(t, l)
	if err := l.sync(); !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("sync on a pipe = %v, want ErrIO", err)
	}
	events := logs.FilterMessage("wal disabled").All()
	if len(events) != 1 {
		t.Fatalf("wal disabled logs = %d, want 1", len(events))
	}
	disabledFields := events[0].ContextMap()
	if got, ok := disabledFields["failed_at_offset"]; !ok || got != int64(26) {
		t.Errorf("failed_at_offset = %v/%v, want the length of the first frame (26)", got, ok)
	}
	if got := disabledFields["error"]; got == nil || got == "" {
		t.Error("the disabled event carries no error field")
	}
}
