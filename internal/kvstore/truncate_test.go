package kvstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

// 五种坏记录各一例：裁剪必须落在坏记录的起点，一条正常记录都不许多丢。
func TestTruncateFiveCorruptionKinds(t *testing.T) {
	cases := []struct {
		name   string
		bad    int
		reason string
		mutate func(pristine []byte, starts []int64) []byte
	}{
		{
			name: "截半（剩余不足 8 字节）", bad: 2, reason: "incomplete header",
			mutate: func(p []byte, st []int64) []byte { return p[:st[2]+3] },
		},
		{
			name: "len 越界", bad: 1, reason: "exceeds",
			mutate: func(p []byte, st []int64) []byte {
				out := append([]byte{}, p...)
				binary.BigEndian.PutUint32(out[st[1]:], MaxEntrySize+1)
				return out
			},
		},
		{
			name: "读不满 entry", bad: 3, reason: "entry shorter than declared length",
			mutate: func(p []byte, st []int64) []byte { return p[:st[3]+FrameHeaderSize+5] },
		},
		{
			name: "crc 不符", bad: 4, reason: "crc mismatch",
			mutate: func(p []byte, st []int64) []byte {
				out := append([]byte{}, p...)
				out[st[4]+FrameHeaderSize+2] ^= 0x01
				return out
			},
		},
		{
			name: "op 非法", bad: 1, reason: "unknown op",
			mutate: func(p []byte, st []int64) []byte {
				out := append([]byte{}, p...)
				size := int(binary.BigEndian.Uint32(p[st[1]:]))
				entry := out[st[1]+FrameHeaderSize : st[1]+FrameHeaderSize+int64(size)]
				entry[0] = 0x02
				binary.BigEndian.PutUint32(out[st[1]+4:], crcOf(uint32(size), entry))
				return out
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := lockedDir(t)
			path := cfg.WALPath(1)
			recs := make([][2]string, 5)
			for i := range recs {
				recs[i] = [2]string{fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i)}
			}
			_, starts := writeSegment(t, path, recs)
			pristine := readRaw(t, path)

			mutated := tc.mutate(pristine, starts)
			writeRaw(t, path, mutated)

			lg, records := observerLogger()
			got, err := scanWAL(cfg, lg)
			if err != nil {
				t.Fatalf("scanWAL = %v", err)
			}
			if len(got.entries) != tc.bad {
				t.Fatalf("recovered %d records, want the %d before the corrupt one", len(got.entries), tc.bad)
			}
			if cut := starts[tc.bad]; fileSize(t, path) != cut {
				t.Errorf("file is %d bytes, want it cut at the corrupt record start %d", fileSize(t, path), cut)
			}
			// 裁剪量等于坏记录起点之后的全部字节（对账：写入条数 = 恢复条数 + 丢弃条数）。
			if want := int64(len(mutated)) - starts[tc.bad]; got.truncatedBytes != want {
				t.Errorf("truncated %d bytes, want %d", got.truncatedBytes, want)
			}

			warns := records.FilterMessage("corrupt record tail").All()
			if len(warns) != 1 {
				t.Fatalf("bad tail logged %d times, want exactly 1", len(warns))
			}
			fields := warns[0].ContextMap()
			for _, key := range []string{"file", "offset", "expect_len", "got_len"} {
				if _, ok := fields[key]; !ok {
					t.Errorf("warn has no %q: %v", key, fields)
				}
			}
			if fields["file"] != path || fields["offset"] != starts[tc.bad] {
				t.Errorf("warn location = %v, want file %s offset %d", fields, path, starts[tc.bad])
			}
			if reason, _ := fields["reason"].(string); !strings.Contains(reason, tc.reason) {
				t.Errorf("reason = %q, want it to contain %q", reason, tc.reason)
			}

			// 第二条坏记录不许被读到：裁剪起点之后整体丢弃。
			if records.FilterMessage("corrupt record tail").Len() != 1 {
				t.Error("scan kept going past the bad tail")
			}

			// 重启第二次：不再裁剪，也不再告警。
			lg2, records2 := observerLogger()
			again, err := scanWAL(cfg, lg2)
			if err != nil {
				t.Fatalf("second scanWAL = %v", err)
			}
			if again.truncatedBytes != 0 || len(again.entries) != tc.bad {
				t.Errorf("second scan = %+v, want no truncation and %d records", again, tc.bad)
			}
			if n := len(records2.All()); n != 0 {
				t.Errorf("second scan logged %d entries: %v", n, records2.All())
			}
		})
	}
}

// 坏尾之后还有完整可用的记录：一并丢弃，裁剪位置不变。
func TestValidRecordsAfterBadTailAreDropped(t *testing.T) {
	cfg := lockedDir(t)
	path := cfg.WALPath(1)
	recs := [][2]string{{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}}
	size, starts := writeSegment(t, path, recs)

	// 把第 3 条改成 crc 对不上的坏记录，第 4、5 条保持完好。
	data := readRaw(t, path)
	data[starts[3]+FrameHeaderSize+1] ^= 0x80
	writeRaw(t, path, data)

	got, err := scanWAL(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if len(got.entries) != 3 || got.entries[2] != recs[2] {
		t.Fatalf("recovered %q, want the three records before the bad one", got.entries)
	}
	if fileSize(t, path) != starts[3] {
		t.Errorf("file is %d bytes, want cut at %d with the valid tail dropped", fileSize(t, path), starts[3])
	}
	if got.truncatedBytes != size-starts[3] {
		t.Errorf("truncated %d bytes, want %d", got.truncatedBytes, size-starts[3])
	}
}

// 坏尾落在中间段：前面的段全部恢复，本段裁到起点，之后的段清空。
func TestBadTailInMiddleSegmentClearsLaterSegments(t *testing.T) {
	cfg := lockedDir(t)
	first := cfg.WALPath(1)
	second := cfg.WALPath(2)
	third := cfg.WALPath(3)

	writeSegment(t, first, [][2]string{{"a0", "v"}, {"a1", "v"}})
	secondSize, secondStarts := writeSegment(t, second, [][2]string{{"b0", "v"}, {"b1", "v"}, {"b2", "v"}})
	thirdSize, _ := writeSegment(t, third, [][2]string{{"c0", "v"}, {"c1", "v"}})

	data := readRaw(t, second)
	data[secondStarts[1]+FrameHeaderSize+1] ^= 0x01
	writeRaw(t, second, data)

	lg, records := observerLogger()
	got, err := scanWAL(cfg, lg)
	if err != nil {
		t.Fatalf("scanWAL = %v", err)
	}
	if len(got.entries) != 3 || got.entries[2][0] != "b0" {
		t.Fatalf("recovered %q, want both of segment 1 plus b0", got.entries)
	}
	if fileSize(t, second) != secondStarts[1] {
		t.Errorf("segment 2 is %d bytes, want cut at %d", fileSize(t, second), secondStarts[1])
	}
	if fileSize(t, third) != 0 {
		t.Errorf("segment 3 is %d bytes, want it cleared", fileSize(t, third))
	}
	if want := secondSize - secondStarts[1] + thirdSize; got.truncatedBytes != want {
		t.Errorf("truncated %d bytes, want %d", got.truncatedBytes, want)
	}
	if n := records.FilterMessage("corrupt record tail").Len(); n != 1 {
		t.Errorf("%d bad-tail warnings, want 1", n)
	}

	lg2, records2 := observerLogger()
	again, err := scanWAL(cfg, lg2)
	if err != nil {
		t.Fatalf("second scanWAL = %v", err)
	}
	if again.truncatedBytes != 0 || len(again.entries) != 3 {
		t.Errorf("second scan = %+v, want no truncation", again)
	}
	if n := len(records2.All()); n != 0 {
		t.Errorf("second scan logged %d entries", n)
	}
}
