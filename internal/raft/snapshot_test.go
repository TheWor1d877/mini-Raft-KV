package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 快照测试用一组固定的投票集合与地址表：结构校验只要求 id 在地址表里，
// 具体集合大小不影响这些纯格式用例。
var (
	snapshotTestVoters = []string{"node-1"}
	snapshotTestKnown  = map[string]string{"node-1": "127.0.0.1:8801", "node-2": "127.0.0.1:8802"}
)

// newTestSnapshotter 造一份带独立数据目录的快照句柄。
func newTestSnapshotter(t *testing.T) (*snapshotter, Config) {
	t.Helper()
	cfg := testConfig(t)
	return newSnapshotter(cfg, zap.NewNop()), cfg
}

// 快照文件的布局逐字节核对：8B magic｜4B 条目数｜帧(len 4B｜crc32c 4B｜op 1B(=0x20)｜keylen｜key｜vallen｜value)。
func TestSnapshotFileByteLayout(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	entries := []kvstore.Entry{{Key: "a", Value: "1"}}

	snap, err := s.create(7, 3, entries, snapshotTestVoters)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	data, err := os.ReadFile(cfg.SnapshotPath(7))
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	if string(data[:snapshotMagicLen]) != snapshotMagic {
		t.Fatalf("magic = %q, want %q", data[:snapshotMagicLen], snapshotMagic)
	}
	if got := binary.BigEndian.Uint32(data[snapshotMagicLen : snapshotMagicLen+snapshotCountLen]); got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
	if got := binary.BigEndian.Uint16(data[snapshotMagicLen+snapshotCountLen : snapshotHeaderLen]); got != 1 {
		t.Fatalf("voter count = %d, want 1", got)
	}

	payload, err := encodeSnapshotRecord("a", "1")
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	want := make([]byte, 0, snapshotHeaderLen+kvstore.FrameHeaderSize+len(payload))
	want = append(want, snapshotMagic...)
	want = binary.BigEndian.AppendUint32(want, 1)
	want = binary.BigEndian.AppendUint16(want, 1)
	want = binary.BigEndian.AppendUint16(want, uint16(len(snapshotTestVoters[0])))
	want = append(want, snapshotTestVoters[0]...)
	frame, err := kvstore.EncodeFrame(payload)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	want = append(want, frame...)
	if len(data) != len(want) {
		t.Fatalf("snapshot file is %d bytes, want %d", len(data), len(want))
	}
	for i := range want {
		if data[i] != want[i] {
			t.Fatalf("byte %d = 0x%02x, want 0x%02x", i, data[i], want[i])
		}
	}
	if payload[0] != OpSnapshotRecord {
		t.Fatalf("record op = 0x%02x, want 0x%02x", payload[0], OpSnapshotRecord)
	}
	// op 的字面值在这里钉死：比对常量的那一句在常量被改时跟着改，
	// 两个目录互读判坏那一格才拦得住 op 撞值（5.2 §6 的第三条变异）。
	if payload[0] != 0x20 {
		t.Fatalf("record op = 0x%02x, want the literal 0x20", payload[0])
	}
	if snap.Index != 7 || snap.Term != 3 || snap.Entries != 1 {
		t.Fatalf("snapshot metadata = %+v, want index 7 term 3 entries 1", snap)
	}
	if snap.Bytes != int64(len(data)) {
		t.Fatalf("snapshot bytes = %d, want %d", snap.Bytes, len(data))
	}
}

// 重开文件能按序读回全部键值：快照文件是状态的完整拷贝。
func TestSnapshotFileRoundTripsEveryKeyInOrder(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	entries := []kvstore.Entry{
		{Key: "b", Value: "2"},
		{Key: "a", Value: "1"},
		{Key: "c", Value: "3"},
	}
	if _, err := s.create(5, 1, entries, snapshotTestVoters); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	data, err := os.ReadFile(cfg.SnapshotPath(5))
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	got, voters, err := decodeSnapshotFile(data, snapshotTestKnown)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(voters) != 1 || voters[0] != "node-1" {
		t.Fatalf("decoded voters = %v, want [node-1]", voters)
	}
	if len(got) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i] != e {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], e)
		}
	}
}

// 条目数与实际帧数对不上判坏记录：少一帧、多一帧都要判。
func TestSnapshotFileRejectsWrongEntryCount(t *testing.T) {
	s, _ := newTestSnapshotter(t)
	if _, err := s.create(1, 1, []kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	data, err := encodeSnapshotFile([]kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}

	tooMany := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(tooMany[snapshotMagicLen:snapshotMagicLen+snapshotCountLen], 2)
	if _, _, err := decodeSnapshotFile(tooMany, snapshotTestKnown); !errors.Is(err, kvstore.ErrCorruptRecord) {
		t.Fatalf("count 2 over 1 frame = %v, want ErrCorruptRecord", err)
	}

	tooFew := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(tooFew[snapshotMagicLen:snapshotMagicLen+snapshotCountLen], 0)
	if _, _, err := decodeSnapshotFile(tooFew, snapshotTestKnown); !errors.Is(err, kvstore.ErrCorruptRecord) {
		t.Fatalf("count 0 over 1 frame = %v, want ErrCorruptRecord", err)
	}
}

// payload 的 op 取 0x01（kvstore 的 WAL）或 0x10（raft 日志）都判坏记录：两个目录互读要立刻发现。
func TestSnapshotFileRejectsForeignOp(t *testing.T) {
	for _, op := range []byte{0x01, 0x10} {
		payload := make([]byte, 0, 9)
		payload = append(payload, op)
		payload = binary.BigEndian.AppendUint32(payload, 1)
		payload = append(payload, 'a')
		payload = binary.BigEndian.AppendUint32(payload, 1)
		payload = append(payload, '1')

		frame, err := kvstore.EncodeFrame(payload)
		if err != nil {
			t.Fatalf("encode frame: %v", err)
		}
		data := append([]byte(snapshotMagic), make([]byte, 0, 4)...)
		data = binary.BigEndian.AppendUint32(data, 1)
		data = binary.BigEndian.AppendUint16(data, 1)
		data = binary.BigEndian.AppendUint16(data, uint16(len(snapshotTestVoters[0])))
		data = append(data, snapshotTestVoters[0]...)
		data = append(data, frame...)

		if _, _, err := decodeSnapshotFile(data, snapshotTestKnown); !errors.Is(err, kvstore.ErrCorruptRecord) {
			t.Fatalf("op 0x%02x = %v, want ErrCorruptRecord", op, err)
		}
	}
}

// 帧的 crc 不符判坏记录。
func TestSnapshotFileRejectsBadFrameChecksum(t *testing.T) {
	payload, err := encodeSnapshotRecord("a", "1")
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	frame, err := kvstore.EncodeFrame(payload)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	frame[kvstore.FrameHeaderSize-1] ^= 0xff

	data := append([]byte(snapshotMagic), make([]byte, 0, 4)...)
	data = binary.BigEndian.AppendUint32(data, 1)
	data = binary.BigEndian.AppendUint16(data, 1)
	data = binary.BigEndian.AppendUint16(data, uint16(len(snapshotTestVoters[0])))
	data = append(data, snapshotTestVoters[0]...)
	data = append(data, frame...)

	if _, _, err := decodeSnapshotFile(data, snapshotTestKnown); !errors.Is(err, kvstore.ErrCorruptRecord) {
		t.Fatalf("bad frame crc = %v, want ErrCorruptRecord", err)
	}
}

// magic 不是 MRSNAP02 判坏记录，MRSNAP01 同样判坏（6.1 §5）。
func TestSnapshotFileRejectsBadMagic(t *testing.T) {
	for _, magic := range []string{"NOTSNAP1", "MRSNAP01"} {
		data := []byte(magic)
		data = binary.BigEndian.AppendUint32(data, 0)
		data = binary.BigEndian.AppendUint16(data, 1)
		data = binary.BigEndian.AppendUint16(data, uint16(len(snapshotTestVoters[0])))
		data = append(data, snapshotTestVoters[0]...)
		if _, _, err := decodeSnapshotFile(data, snapshotTestKnown); !errors.Is(err, kvstore.ErrCorruptRecord) {
			t.Fatalf("magic %q = %v, want ErrCorruptRecord", magic, err)
		}
	}
}

// 写文件失败时不留正式名文件：正式名一出现就是一次提交。
func TestSnapshotCreateFailureLeavesNoOfficialFile(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	// 快照目录改成一个文件的路径，OpenFile 建不出临时名。
	if err := os.RemoveAll(cfg.SnapshotDir()); err != nil {
		t.Fatalf("remove snapshot dir: %v", err)
	}
	if err := os.WriteFile(cfg.SnapshotDir(), []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("plant a file at the snapshot dir path: %v", err)
	}

	if _, err := s.create(3, 1, []kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters); err == nil {
		t.Fatal("create on a broken snapshot dir = nil error, want an error")
	}
	if _, err := os.Stat(cfg.SnapshotPath(3)); err == nil {
		t.Fatal("official snapshot file exists after a failed create")
	}
}

// 指针写读一致：写完 current.json 里的五个字段与快照元数据逐个相同。
func TestSnapshotPointerRoundTrips(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	snap, err := s.create(9, 4, []kvstore.Entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}, snapshotTestVoters)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if err := s.storePointer(snap); err != nil {
		t.Fatalf("store pointer: %v", err)
	}

	raw, err := os.ReadFile(cfg.SnapshotPointerPath())
	if err != nil {
		t.Fatalf("read pointer: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode pointer: %v", err)
	}
	for _, name := range snapshotPointerFields {
		if _, ok := fields[name]; !ok {
			t.Fatalf("pointer is missing field %q: %s", name, raw)
		}
	}
	if len(fields) != len(snapshotPointerFields) {
		t.Fatalf("pointer has %d fields, want %d: %s", len(fields), len(snapshotPointerFields), raw)
	}

	got, err := s.readPointer()
	if err != nil {
		t.Fatalf("read pointer: %v", err)
	}
	if got.SnapshotIndex != snap.Index || got.SnapshotTerm != snap.Term ||
		got.Entries != snap.Entries || got.Bytes != snap.Bytes || got.Checksum != snap.Checksum {
		t.Fatalf("pointer = %+v, want %+v", *got, *snap)
	}
	if _, err := os.Stat(cfg.SnapshotPointerTmpPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pointer tmp file is still there after the rename: %v", err)
	}
}

// 指针的三种坏输入按「没有快照」起：文件不存在、缺字段、半行 JSON。
func TestSnapshotPointerBadInputsMeanNoSnapshot(t *testing.T) {
	// 缺字段与半行 JSON 两种坏输入：指针落在这个样子时快照文件本身是好的，
	// 认得出坏的只有指针的字段校验这一层。
	s, cfg := newTestSnapshotter(t)
	pointer, err := s.create(7, 3, []kvstore.Entry{{Key: "k", Value: "v"}}, snapshotTestVoters)
	if err != nil {
		t.Fatalf("create the snapshot the pointer refers to: %v", err)
	}
	cases := []struct {
		name string
		body string
	}{
		{"missing field", fmt.Sprintf(`{"snapshotIndex":%d,"entries":%d,"bytes":%d,"checksum":%d}`,
			pointer.Index, pointer.Entries, pointer.Bytes, pointer.Checksum)},
		{"half a json line", fmt.Sprintf(`{"snapshotIndex":%d,"snapshotTerm":%d`, pointer.Index, pointer.Term)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(cfg.SnapshotPointerPath(), []byte(tc.body), 0o644); err != nil {
				t.Fatalf("write pointer: %v", err)
			}
			snap, entries, err := s.load()
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if snap != nil || entries != nil {
				t.Fatalf("load = %+v / %+v, want no snapshot", snap, entries)
			}
		})
	}

	t.Run("pointer file missing", func(t *testing.T) {
		s, _ := newTestSnapshotter(t)
		snap, entries, err := s.load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if snap != nil || entries != nil {
			t.Fatalf("load = %+v / %+v, want no snapshot", snap, entries)
		}
	})
}

// 指针指向的快照文件缺失或 checksum 不符时按「没有快照」起。
func TestSnapshotPointerWithoutUsableFileMeansNoSnapshot(t *testing.T) {
	t.Run("file missing", func(t *testing.T) {
		s, cfg := newTestSnapshotter(t)
		if err := os.WriteFile(cfg.SnapshotPointerPath(),
			[]byte(`{"snapshotIndex":4,"snapshotTerm":1,"entries":1,"bytes":10,"checksum":123}`), 0o644); err != nil {
			t.Fatalf("write pointer: %v", err)
		}
		snap, entries, err := s.load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if snap != nil || entries != nil {
			t.Fatalf("load = %+v / %+v, want no snapshot", snap, entries)
		}
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		s, cfg := newTestSnapshotter(t)
		if _, err := s.create(4, 1, []kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters); err != nil {
			t.Fatalf("create snapshot: %v", err)
		}
		if err := os.WriteFile(cfg.SnapshotPointerPath(),
			[]byte(`{"snapshotIndex":4,"snapshotTerm":1,"entries":1,"bytes":10,"checksum":1}`), 0o644); err != nil {
			t.Fatalf("write pointer: %v", err)
		}
		snap, entries, err := s.load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if snap != nil || entries != nil {
			t.Fatalf("load = %+v / %+v, want no snapshot", snap, entries)
		}
	})
}

// 启动时临时名文件与没有指针指向的孤儿快照文件被删掉：它们都不是提交点。
func TestSnapshotCleanupRemovesTmpAndOrphanFiles(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	snap, err := s.create(4, 1, []kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if err := s.storePointer(snap); err != nil {
		t.Fatalf("store pointer: %v", err)
	}

	// 孤儿快照文件：index 不在指针上；临时名文件：没改名的半成品。
	if _, err := s.create(9, 1, []kvstore.Entry{{Key: "b", Value: "2"}}, snapshotTestVoters); err != nil {
		t.Fatalf("create orphan snapshot: %v", err)
	}
	if err := os.WriteFile(cfg.SnapshotTmpPath(11), []byte("half written"), 0o644); err != nil {
		t.Fatalf("plant a tmp file: %v", err)
	}

	loaded, _, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded == nil || loaded.Index != 4 {
		t.Fatalf("load = %+v, want the snapshot at index 4", loaded)
	}
	if _, err := os.Stat(cfg.SnapshotPath(9)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan snapshot file is still there: %v", err)
	}
	if _, err := os.Stat(cfg.SnapshotTmpPath(11)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp file is still there: %v", err)
	}
	if _, err := os.Stat(cfg.SnapshotPath(4)); err != nil {
		t.Fatalf("the pointed snapshot file was removed: %v", err)
	}
}

// 没有指针时快照目录里的快照文件全是孤儿，启动后目录里不留它们。
func TestSnapshotCleanupRemovesEveryFileWithoutPointer(t *testing.T) {
	s, cfg := newTestSnapshotter(t)
	if _, err := s.create(3, 1, []kvstore.Entry{{Key: "a", Value: "1"}}, snapshotTestVoters); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if _, _, err := s.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := os.Stat(cfg.SnapshotPath(3)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot file is still there without a pointer: %v", err)
	}
}

// 快照文件名里的 index 按八位零填充解析。
func TestParseSnapshotIndex(t *testing.T) {
	if got, ok := parseSnapshotIndex("00000012.snap"); !ok || got != 12 {
		t.Fatalf("parse 00000012.snap = %d %v, want 12 true", got, ok)
	}
	for _, name := range []string{"12.snap", "00000012.snap.tmp", "00000000.snap", "current.json"} {
		if _, ok := parseSnapshotIndex(name); ok {
			t.Fatalf("parse %q = ok, want not ok", name)
		}
	}
}

// 指针文件落在快照目录里，文件名固定。
func TestSnapshotPointerPathLayout(t *testing.T) {
	cfg := testConfig(t)
	if got, want := cfg.SnapshotPointerPath(), filepath.Join(cfg.SnapshotDir(), "current.json"); got != want {
		t.Fatalf("pointer path = %q, want %q", got, want)
	}
	if got, want := cfg.SnapshotPath(12), filepath.Join(cfg.SnapshotDir(), "00000012.snap"); got != want {
		t.Fatalf("snapshot path = %q, want %q", got, want)
	}
}

// 指针先更新、日志前缀后删：指针落不了盘时删日志的那一步走不到，
// 快照文件留在盘上等下一次提交或启动清理（5.1 §7 的提交顺序）。
func TestSnapshotPointerFailureLeavesTheLogPrefixInPlace(t *testing.T) {
	c := newClusterCfg(t, 1, zap.NewNop(), nil)
	cfg := c.raftCfg["node-1"]
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	for _, key := range []string{"k1", "k2", "k3"} {
		if _, _, err := leader.Propose(context.Background(), key, "v-"+key); err != nil {
			t.Fatalf("propose %s: %v", key, err)
		}
	}
	c.tick(2)

	// 指针的正式名占成一个目录：rename 必失败，指针更新落不了盘。
	if err := os.Mkdir(cfg.SnapshotPointerPath(), 0o755); err != nil {
		t.Fatalf("block the snapshot pointer: %v", err)
	}
	if _, err := leader.TriggerSnapshot(); !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("trigger with an unwritable pointer = %v, want ErrIO", err)
	}

	leader.mu.Lock()
	baseIndex := leader.log.baseIndex
	snapshotIndex := leader.snapshotIndex
	leader.mu.Unlock()
	if baseIndex != 1 {
		t.Fatalf("baseIndex = %d, want 1: the log prefix must outlive a pointer that did not commit", baseIndex)
	}
	if snapshotIndex != 0 {
		t.Fatalf("snapshotIndex = %d, want 0", snapshotIndex)
	}

	// 快照文件本身已经落盘：它是指针之外的产物，提交点只由指针认。
	files, err := filepath.Glob(filepath.Join(cfg.SnapshotDir(), "*.snap"))
	if err != nil {
		t.Fatalf("glob the snapshot dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("snapshot files on disk = %v, want the one file that failed to get a pointer", files)
	}

	// 让开路之后同一份快照照样能提交，前缀也才删得动。
	if err := os.Remove(cfg.SnapshotPointerPath()); err != nil {
		t.Fatalf("unblock the snapshot pointer: %v", err)
	}
	index, err := leader.TriggerSnapshot()
	if err != nil {
		t.Fatalf("trigger after the pointer is writable again: %v", err)
	}
	leader.mu.Lock()
	baseIndex = leader.log.baseIndex
	leader.mu.Unlock()
	if baseIndex != index+1 {
		t.Fatalf("the retry left baseIndex %d with the snapshot point at %d, want the baseline one past it", baseIndex, index)
	}
}
