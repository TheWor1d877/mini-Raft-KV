package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 快照文件的布局（5.1 §5）：
//
//	MRSNAP01              8B magic
//	entries               4B 键值条数，大端
//	frame                 len 4B | crc32c 4B | payload
//	payload               op 1B(=0x20) | keylen 4B | key | vallen 4B | value
//
// 快照记录与 raft 日志条目同形，但语义是状态而不是操作，所以 op 值不同：
// 日志用 0x10、快照用 0x20，两个目录互读都判坏记录（5.3 D34）。
const (
	OpSnapshotRecord = byte(0x20)

	snapshotMagic     = "MRSNAP01"
	snapshotMagicLen  = 8
	snapshotCountLen  = 4
	snapshotHeaderLen = snapshotMagicLen + snapshotCountLen
)

// snapshotter 负责快照文件与元数据指针的读写（5.1 §2）。
type snapshotter struct {
	cfg Config
	lg  *zap.Logger
}

func newSnapshotter(cfg Config, lg *zap.Logger) *snapshotter {
	return &snapshotter{cfg: cfg, lg: kvstore.WithComponent(lg, ComponentRaft)}
}

// Snapshot 是一份快照的元数据：指针文件里记的就是这些字段（5.1 §4）。
type Snapshot struct {
	Index    uint64 // 快照点对应的日志 index
	Term     uint64 // 该 index 处的 term
	Entries  int    // 快照里的键值条数
	Bytes    int64  // 快照文件字节数
	Checksum uint32 // 整份文件的 crc32c
}

// snapshotPointer 是 `raft/snapshot/current.json` 的一行 JSON（D31）。
type snapshotPointer struct {
	SnapshotIndex uint64 `json:"snapshotIndex"`
	SnapshotTerm  uint64 `json:"snapshotTerm"`
	Entries       int    `json:"entries"`
	Bytes         int64  `json:"bytes"`
	Checksum      uint32 `json:"checksum"`
}

var snapshotPointerFields = []string{"snapshotIndex", "snapshotTerm", "entries", "bytes", "checksum"}

// snapshotCRCTable 是 crc32c 的表：与 kvstore 的帧校验同一套算法。
var snapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

// 崩溃注入的停点（5.2 §5 缺口表里「真掉电造不出来」的那三格）：
// MINIRAFT_SNAPSHOT_PAUSE_MS 给一个正数，快照与压缩的三处提交点各停那么久，
// scripts/crash_inject.sh 才停得进「两步之间」——那三处窗口本来只有几毫秒，轮询追不上。
// 三个停点是快照文件改名前、元数据指针改名前、日志段文件删除途中（compact.go 的两个删除循环）。
// 不设它读到 0，进程行为与没有这段代码时一致；真掉电仍然造不出来，这里拉开的只是窗口。
var crashPauseMS = func() int {
	ms, err := strconv.Atoi(os.Getenv("MINIRAFT_SNAPSHOT_PAUSE_MS"))
	if err != nil || ms < 0 {
		return 0
	}
	return ms
}()

// crashPointPause 停在一次提交之前；只有注入开关打开时才睡。
func crashPointPause() {
	if crashPauseMS > 0 {
		time.Sleep(time.Duration(crashPauseMS) * time.Millisecond)
	}
}

// encodeSnapshotRecord 编出一条快照记录：op｜keylen｜key｜vallen｜value。
func encodeSnapshotRecord(key, value string) ([]byte, error) {
	if len(key) > kvstore.MaxKeySize {
		return nil, kvstore.ErrTooLargeField("key", len(key), kvstore.MaxKeySize)
	}
	if len(value) > kvstore.MaxValueSize {
		return nil, kvstore.ErrTooLargeField("value", len(value), kvstore.MaxValueSize)
	}
	buf := make([]byte, 0, 1+4+len(key)+4+len(value))
	buf = append(buf, OpSnapshotRecord)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
	return append(buf, value...), nil
}

// decodeSnapshotRecord 解出一条快照记录；op 与内部长度对不上都算坏记录。
func decodeSnapshotRecord(payload []byte, offset int64) (string, string, error) {
	if len(payload) < 1+4+4 {
		return "", "", kvstore.ErrCorruptAt(offset,
			fmt.Sprintf("snapshot record is %d bytes, below the 9-byte minimum", len(payload)))
	}
	if op := payload[0]; op != OpSnapshotRecord {
		return "", "", kvstore.ErrCorruptAt(offset, fmt.Sprintf("unknown op 0x%02x", op))
	}
	keyLen := int(binary.BigEndian.Uint32(payload[1:5]))
	if 1+4+keyLen+4 > len(payload) {
		return "", "", kvstore.ErrCorruptAt(offset, fmt.Sprintf("keylen %d runs past the record end", keyLen))
	}
	key := string(payload[5 : 5+keyLen])
	rest := payload[5+keyLen:]
	valueLen := int(binary.BigEndian.Uint32(rest[:4]))
	if len(rest) != 4+valueLen {
		return "", "", kvstore.ErrCorruptAt(offset,
			fmt.Sprintf("record is %d bytes, vallen %d wants %d", len(payload), valueLen, 4+valueLen))
	}
	return key, string(rest[4:]), nil
}

// encodeSnapshotFile 把状态机的全量键值编成一份快照文件的字节。
func encodeSnapshotFile(entries []kvstore.Entry) ([]byte, error) {
	out := make([]byte, 0, snapshotHeaderLen+len(entries)*64)
	out = append(out, snapshotMagic...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(entries)))
	for _, e := range entries {
		payload, err := encodeSnapshotRecord(e.Key, e.Value)
		if err != nil {
			return nil, err
		}
		frame, err := kvstore.EncodeFrame(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, frame...)
	}
	return out, nil
}

// decodeSnapshotFile 解一份快照文件的字节：magic、条目数与每帧的 crc 任一不符都算坏记录。
func decodeSnapshotFile(data []byte) ([]kvstore.Entry, error) {
	if len(data) < snapshotHeaderLen {
		return nil, kvstore.ErrCorruptAt(0, "snapshot file is shorter than its header")
	}
	if string(data[:snapshotMagicLen]) != snapshotMagic {
		return nil, kvstore.ErrCorruptAt(0, "snapshot file magic mismatch")
	}
	count := int(binary.BigEndian.Uint32(data[snapshotMagicLen:snapshotHeaderLen]))

	out := make([]kvstore.Entry, 0, count)
	offset := int64(snapshotHeaderLen)
	for i := 0; i < count; i++ {
		if offset+int64(kvstore.FrameHeaderSize) > int64(len(data)) {
			return nil, kvstore.ErrCorruptAt(offset, "snapshot file ends inside a frame header")
		}
		head := data[offset : offset+kvstore.FrameHeaderSize]
		size, checksum, err := kvstore.DecodeFrame(head, offset)
		if err != nil {
			return nil, err
		}
		start := offset + kvstore.FrameHeaderSize
		if start+int64(size) > int64(len(data)) {
			return nil, kvstore.ErrCorruptAt(offset, "snapshot frame runs past the file end")
		}
		payload := data[start : start+int64(size)]
		if err := kvstore.CheckFrame(size, checksum, payload, offset); err != nil {
			return nil, err
		}
		key, value, err := decodeSnapshotRecord(payload, offset)
		if err != nil {
			return nil, err
		}
		out = append(out, kvstore.Entry{Key: key, Value: value})
		offset = start + int64(size)
	}
	if offset != int64(len(data)) {
		return nil, kvstore.ErrCorruptAt(offset, "snapshot file has trailing bytes")
	}
	return out, nil
}

// create 生成一份快照并原子提交：临时名写入 → Sync → 改名 → Sync 父目录（5.1 §7 的生成与提交第 2 步）。
// 改名之前失败不留正式名文件，正式名一出现就是一次提交。
func (s *snapshotter) create(index, term uint64, entries []kvstore.Entry) (*Snapshot, error) {
	data, err := encodeSnapshotFile(entries)
	if err != nil {
		return nil, err
	}
	// 上一次写失败留下的同名临时名先清掉：临时名用 O_EXCL 开，撞名就写不进去。
	_ = os.Remove(s.cfg.SnapshotTmpPath(index))
	if err := s.writeFile(index, data); err != nil {
		return nil, err
	}
	return &Snapshot{
		Index:    index,
		Term:     term,
		Entries:  len(entries),
		Bytes:    int64(len(data)),
		Checksum: crc32.Checksum(data, snapshotCRCTable),
	}, nil
}

// writeFile 把快照字节落到 `raft/snapshot/<index>.snap`，走临时名加改名。
// 写失败时临时名文件留在原地：它不是提交点，启动时按 *.tmp 删掉。
func (s *snapshotter) writeFile(index uint64, data []byte) error {
	tmp := s.cfg.SnapshotTmpPath(index)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return kvstore.ErrIOFrom("create snapshot tmp", 0, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("write snapshot tmp", 0, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("fsync snapshot tmp", 0, err)
	}
	if err := f.Close(); err != nil {
		return kvstore.ErrIOFrom("close snapshot tmp", 0, err)
	}
	crashPointPause()
	if err := os.Rename(tmp, s.cfg.SnapshotPath(index)); err != nil {
		return kvstore.ErrIOFrom("rename snapshot file", 0, err)
	}
	// 改名是这份快照的提交点，父目录不 Sync 的话掉电能把改名一起退掉。
	if err := syncDir(s.cfg.SnapshotDir()); err != nil {
		return err
	}
	return nil
}

// storePointer 写元数据指针：临时名 → Sync → rename → Sync 父目录（D31）。
func (s *snapshotter) storePointer(snap *Snapshot) error {
	data, err := json.Marshal(snapshotPointer{
		SnapshotIndex: snap.Index,
		SnapshotTerm:  snap.Term,
		Entries:       snap.Entries,
		Bytes:         snap.Bytes,
		Checksum:      snap.Checksum,
	})
	if err != nil {
		return kvstore.ErrIOFrom("encode snapshot pointer", 0, err)
	}
	data = append(data, '\n')

	tmp := s.cfg.SnapshotPointerTmpPath()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return kvstore.ErrIOFrom("create snapshot pointer tmp", 0, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("write snapshot pointer tmp", 0, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("fsync snapshot pointer tmp", 0, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("close snapshot pointer tmp", 0, err)
	}
	crashPointPause()
	if err := os.Rename(tmp, s.cfg.SnapshotPointerPath()); err != nil {
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("rename snapshot pointer", 0, err)
	}
	if err := syncDir(s.cfg.SnapshotDir()); err != nil {
		return err
	}
	// 指针指向新快照之后，旧的那份快照文件删掉：目录里只留指针指着的一份。
	// 删不掉的不影响恢复：启动时按指针清理孤儿文件。
	s.removeOtherSnapshots(snap.Index)
	s.lg.Info("snapshot committed",
		zap.Uint64("snapshot_index", snap.Index),
		zap.String("file", s.cfg.SnapshotPath(snap.Index)))
	return nil
}

// load 读回指针与快照内容。四种坏输入都按「没有快照」返回：指针文件不存在、
// 指针字段缺失或不是一行完整 JSON、指针指向的快照文件缺失、快照文件的校验与指针不符。
// 指针坏掉时磁盘上可能还留着一份快照文件，但那时没有索引能指出它是谁，
// 启动按「没有快照」起，缺的那一段是否还在由日志决定（5.1 §7 的启动恢复第 1 步）。
func (s *snapshotter) load() (*Snapshot, []kvstore.Entry, error) {
	pointer, err := s.readPointer()
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			s.lg.Warn("snapshot pointer unusable, starting without a snapshot", zap.Error(err))
		}
		s.cleanup(nil)
		return nil, nil, nil
	}
	// 指针与快照文件都在，才认这份快照；临时名文件与孤儿快照文件在这一步清掉。
	s.cleanup(&pointer.SnapshotIndex)

	data, err := os.ReadFile(s.cfg.SnapshotPath(pointer.SnapshotIndex))
	if err != nil {
		s.lg.Warn("snapshot file missing, starting without a snapshot",
			zap.Uint64("snapshot_index", pointer.SnapshotIndex), zap.Error(err))
		return nil, nil, nil
	}
	if got := crc32.Checksum(data, snapshotCRCTable); got != pointer.Checksum {
		s.lg.Warn("snapshot checksum mismatch, starting without a snapshot",
			zap.Uint64("snapshot_index", pointer.SnapshotIndex),
			zap.Uint32("pointer_checksum", pointer.Checksum),
			zap.Uint32("file_checksum", got))
		return nil, nil, nil
	}
	entries, err := decodeSnapshotFile(data)
	if err != nil {
		s.lg.Warn("snapshot file unusable, starting without a snapshot",
			zap.Uint64("snapshot_index", pointer.SnapshotIndex), zap.Error(err))
		return nil, nil, nil
	}
	snap := &Snapshot{
		Index:    pointer.SnapshotIndex,
		Term:     pointer.SnapshotTerm,
		Entries:  pointer.Entries,
		Bytes:    pointer.Bytes,
		Checksum: pointer.Checksum,
	}
	return snap, entries, nil
}

// removeOtherSnapshots 删掉 index 之外的那份快照文件：目录里只留指针指着的一份。
func (s *snapshotter) removeOtherSnapshots(index uint64) {
	entries, err := os.ReadDir(s.cfg.SnapshotDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		got, ok := parseSnapshotIndex(e.Name())
		if !ok || got == index {
			continue
		}
		_ = os.Remove(filepath.Join(s.cfg.SnapshotDir(), e.Name()))
	}
}

// cleanup 清掉启动时不该留下的两类文件：临时名文件与没有指针指向的孤儿快照文件。
// 指针更新之前崩溃会留下一个孤儿快照文件，它不是提交点（5.1 §7 的生成与提交第 3 步）。
// keep 是指针指向的快照 index，为 nil 表示没有指针，全部快照文件都是孤儿。
func (s *snapshotter) cleanup(keep *uint64) {
	entries, err := os.ReadDir(s.cfg.SnapshotDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(s.cfg.SnapshotDir(), e.Name())
		if filepath.Ext(e.Name()) == ".tmp" {
			_ = os.Remove(path)
			continue
		}
		index, ok := parseSnapshotIndex(e.Name())
		if !ok {
			continue
		}
		if keep != nil && index == *keep {
			continue
		}
		_ = os.Remove(path)
	}
}

// parseSnapshotIndex 从快照文件名里取 index：八位零填充加 .snap 后缀。
func parseSnapshotIndex(name string) (uint64, bool) {
	if len(name) != len("00000012.snap") || filepath.Ext(name) != ".snap" {
		return 0, false
	}
	var index uint64
	if _, err := fmt.Sscanf(name[:8], "%08d", &index); err != nil || index == 0 {
		return 0, false
	}
	return index, true
}

// readPointer 读元数据指针。文件不存在返回 fs.ErrNotExist，其余坏输入一律是错误。
func (s *snapshotter) readPointer() (*snapshotPointer, error) {
	data, err := os.ReadFile(s.cfg.SnapshotPointerPath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		return nil, kvstore.ErrIOFrom("read snapshot pointer", 0, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, kvstore.ErrCorruptAt(0, "snapshot pointer is not one JSON object: "+err.Error())
	}
	for _, want := range snapshotPointerFields {
		if _, ok := fields[want]; !ok {
			return nil, kvstore.ErrCorruptAt(0, "snapshot pointer is missing field "+want)
		}
	}
	var pointer snapshotPointer
	if err := json.Unmarshal(data, &pointer); err != nil {
		return nil, kvstore.ErrCorruptAt(0, err.Error())
	}
	return &pointer, nil
}

// readSnapshotBytes 读一份快照文件的原始字节，Leader 补发快照时用这一份（5.1 §7 接收快照的线上形状）。
func (s *snapshotter) readSnapshotBytes(index uint64) ([]byte, error) {
	data, err := os.ReadFile(s.cfg.SnapshotPath(index))
	if err != nil {
		return nil, kvstore.ErrIOFrom("read snapshot file", 0, err)
	}
	return data, nil
}
