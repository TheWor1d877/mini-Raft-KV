package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// raft 日志的 entry 布局（3.1 §5）：op 1B(=0x10) | term 8B | index 8B | command 变长，全部大端。
// kvstore 的 WAL 用 op 0x01，两套 op 值不重叠，读错目录立刻判坏记录。
const (
	OpWALRecord = byte(0x10)

	walEntryTermOffset  = 1
	walEntryIndexOffset = 9
	walEntryHeaderSize  = 1 + 8 + 8

	// 日志索引从 1 起，空日志的 lastLogIndex 是 0（doc 3 基线的日志修复一节）。
	firstRaftIndex = uint64(1)

	// 段文件序号独立于日志索引，同样从 1 起。
	firstWALSegment = uint64(1)
)

func encodeWALRecord(term, index uint64, command []byte) []byte {
	entry := make([]byte, walEntryHeaderSize+len(command))
	entry[0] = OpWALRecord
	binary.BigEndian.PutUint64(entry[walEntryTermOffset:], term)
	binary.BigEndian.PutUint64(entry[walEntryIndexOffset:], index)
	copy(entry[walEntryHeaderSize:], command)
	return entry
}

// decodeWALRecord 解一条 entry。command 允许是空字节串：no-op 条目就长这样（3.1 §7）。
// 帧头那一层的 len 与 crc 由调用方先过（3.1 §5 的判定顺序），这里只管 entry 内部。
func decodeWALRecord(entry []byte, offset int64) (term, index uint64, command []byte, err error) {
	if len(entry) < walEntryHeaderSize {
		return 0, 0, nil, kvstore.ErrCorruptAt(offset,
			fmt.Sprintf("entry is %d bytes, below the %d-byte raft header", len(entry), walEntryHeaderSize))
	}
	if op := entry[0]; op != OpWALRecord {
		return 0, 0, nil, kvstore.ErrCorruptAt(offset, fmt.Sprintf("unknown op 0x%02x", op))
	}
	return binary.BigEndian.Uint64(entry[walEntryTermOffset:]),
		binary.BigEndian.Uint64(entry[walEntryIndexOffset:]),
		entry[walEntryHeaderSize:], nil
}

// walEntry 是一条日志条目，连它在盘上的位置一起记着：按 index 截断尾部要拿这些字段定位帧起点（3.1 §5）。
type walEntry struct {
	term   uint64
	index  uint64
	cmd    []byte
	seq    uint64 // 段文件序号
	offset int64  // 段内字节偏移，指向帧头
	size   int64  // 帧总长，含 8 字节帧头
}

// wal 是 log[] 的内存视图与落盘。帧与 crc 用 kvstore 的原语，文件树独立（D15）。
type wal struct {
	cfg Config
	lg  *zap.Logger

	file *os.File
	seq  uint64
	size int64

	entries []*walEntry

	// baseIndex 是视图里第一条条目的 index：压缩之后日志不再从 1 起（5.1 §5）。
	// baseTerm 是那条位置上的 term，日志为空时 lastTerm 靠它回答投票请求。
	baseIndex uint64
	baseTerm  uint64

	// D20 的两个映射：Follower 回 conflictIndex 用首条，Leader 反查 nextIndex 用末条。
	firstIndexByTerm map[uint64]uint64
	lastIndexByTerm  map[uint64]uint64

	// failed 是 D5 的失效标志：Sync 失败之后盘上边界未知，写路径一律拒绝。
	failed bool
}

// openWAL 先扫描重建视图，再在最大序号文件末尾准备追加（3.1 §7）。
// 扫描会就地裁剪坏尾，所以列段放在扫描之后，拿到的才是裁剪后的长度。
func openWAL(cfg Config, lg *zap.Logger) (*wal, error) {
	return openWALFrom(cfg, lg, firstRaftIndex, 0)
}

// openWALFrom 按给定的基线打开日志：baseIndex 是视图第一条的 index，
// baseTerm 是那个位置上的 term。有快照时两条都来自快照指针（5.1 §7 的启动恢复第 1 步）。
func openWALFrom(cfg Config, lg *zap.Logger, baseIndex, baseTerm uint64) (*wal, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := kvstore.WithComponent(lg, kvstore.ComponentWAL)
	entries, err := scanWALFrom(cfg, log, baseIndex)
	if err != nil {
		return nil, err
	}

	l := &wal{
		cfg:              cfg,
		lg:               log,
		entries:          entries,
		baseIndex:        baseIndex,
		baseTerm:         baseTerm,
		firstIndexByTerm: map[uint64]uint64{},
		lastIndexByTerm:  map[uint64]uint64{},
	}
	if len(entries) > 0 && entries[0].index < baseIndex {
		// 快照覆盖了日志头部：这些条目不进视图，重启按快照点之后回放（5.1 §7 的启动恢复第 3 步）。
		cut := 0
		for cut < len(entries) && entries[cut].index < baseIndex {
			cut++
		}
		l.entries = entries[cut:]
		// 视图跳过的那一段字节也必须从盘上删掉：追加从视图末尾接着写，
		// 旧字节后面直接写新 index，重启扫描会报「日志中间有洞」。
		if err := l.trimStalePrefix(); err != nil {
			return nil, err
		}
	}
	l.rebuildTermIndex()

	segs, err := listWALSegments(cfg)
	if err != nil {
		return nil, err
	}
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, kvstore.ErrIOFrom("open wal file", last.size, err)
		}
		l.file, l.seq, l.size = f, last.seq, last.size
		return l, nil
	}
	f, err := createWALSegment(cfg, firstWALSegment)
	if err != nil {
		return nil, err
	}
	l.file, l.seq, l.size = f, firstWALSegment, 0
	return l, nil
}

// trimStalePrefix 把盘上早于视图基线的字节删掉，让第一条可回放的记录落在段头。
// 只由 openWALFrom 在视图已经跳过一段前缀时调用：留着这段字节，下一次追加
// 就把新 index 写在旧记录之后，重启扫描读出来的就是日志中间有洞。
func (l *wal) trimStalePrefix() error {
	segs, err := listWALSegments(l.cfg)
	if err != nil {
		return err
	}
	if len(l.entries) == 0 {
		// 盘上所有记录都在快照点之前：清空，追加从基线开始（5.1 §5 的基线口径）。
		for _, s := range segs[:max(len(segs)-1, 0)] {
			if err := os.Remove(s.path); err != nil {
				return kvstore.ErrIOFrom("remove wal segment", 0, err)
			}
		}
		if len(segs) > 0 {
			return truncateWALFile(segs[len(segs)-1].path, 0)
		}
		return nil
	}
	first := l.entries[0]
	for _, s := range segs {
		if s.seq >= first.seq {
			break
		}
		if err := os.Remove(s.path); err != nil {
			return kvstore.ErrIOFrom("remove wal segment", 0, err)
		}
	}
	if first.offset > 0 {
		if _, err := rewriteWALSegment(l.cfg, first.seq, first.offset); err != nil {
			return err
		}
		for _, e := range l.entries {
			if e.seq == first.seq {
				e.offset -= first.offset
			}
		}
	}
	return nil
}

// appendEntry 追加一条并检查 index 连续（不变式 6：日志中间不许有洞）。
// 这里不 Sync：Leader 一条一 Sync、Follower 一批一次，落盘节奏由调用方定（3.1 §7、D19）。
func (l *wal) appendEntry(term, index uint64, command []byte) (*walEntry, error) {
	if err := l.writeBlocked(); err != nil {
		return nil, err
	}
	if want := l.lastIndex() + 1; index != want {
		return nil, ErrStateMismatchAt(index, fmt.Sprintf("appended index %d but the log wants %d", index, want))
	}
	frame, err := kvstore.EncodeFrame(encodeWALRecord(term, index, command))
	if err != nil {
		return nil, err
	}
	if l.size+int64(len(frame)) > l.cfg.MaxFileSize {
		if err := l.rollover(); err != nil {
			return nil, err
		}
	}

	e := &walEntry{term: term, index: index, cmd: command, seq: l.seq, offset: l.size, size: int64(len(frame))}
	n, err := l.file.Write(frame)
	l.size += int64(n)
	if err != nil {
		return nil, kvstore.ErrIOFrom("append wal record", e.offset, err)
	}
	l.entries = append(l.entries, e)
	l.trackTerm(e)
	return e, nil
}

// sync 落一次盘。`-raft-fsync=false` 只跳过这个系统调用，追加与文件照常（D19）。
// 失败之后盘上有什么已经判断不了，写路径自此关闭（D5）。
func (l *wal) sync() error {
	if err := l.writeBlocked(); err != nil {
		return err
	}
	if l.cfg.Fsync {
		if err := l.file.Sync(); err != nil {
			l.disable(err)
			return kvstore.ErrIOFrom("fsync wal", l.size, err)
		}
	}
	return nil
}

func (l *wal) close() error {
	if l.file == nil {
		return nil
	}
	var err error
	if !l.failed {
		err = l.sync()
	}
	if cerr := l.file.Close(); err == nil {
		err = kvstore.ErrIOFrom("close wal file", l.size, cerr)
	}
	l.file = nil
	return err
}

// 写路径是否已关：读路径不受影响（D5）。
func (l *wal) disabled() bool { return l.failed }

func (l *wal) writeBlocked() error {
	if l.failed || l.file == nil {
		return errWALDisabled
	}
	return nil
}

func (l *wal) disable(cause error) {
	if l.failed {
		return
	}
	l.failed = true
	l.lg.Error("wal disabled",
		zap.Int64("failed_at_offset", l.size),
		zap.Error(cause))
}

// 换段：旧文件先落盘，新段以临时名创建再原子改名。建新段失败不置失效（3.1 §7）。
func (l *wal) rollover() error {
	if err := l.sync(); err != nil {
		return err
	}
	next := l.seq + 1
	f, err := createWALSegment(l.cfg, next)
	if err != nil {
		return err
	}
	if err := l.file.Close(); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("close wal file", l.size, err)
	}
	l.lg.Info("wal rolled over",
		zap.String("from_file", l.cfg.WALPath(l.seq)),
		zap.String("to_file", l.cfg.WALPath(next)),
		zap.Int64("size_bytes", l.size))
	l.file, l.seq, l.size = f, next, 0
	return nil
}

// truncateTo 丢掉 index 之后的全部条目，保留 index 及其之前的（3.1 §5）。
// index=0 清空整棵日志。截到的每个文件都单独 Sync：这段截断也是复制回执承诺的盘上边界（D19）。
func (l *wal) truncateTo(index uint64) error {
	if err := l.writeBlocked(); err != nil {
		return err
	}
	keep := 0
	for _, e := range l.entries {
		if e.index > index {
			break
		}
		keep++
	}
	if keep == len(l.entries) {
		return nil
	}

	segs, err := listWALSegments(l.cfg)
	if err != nil {
		return err
	}
	if keep > 0 {
		b := l.entries[keep-1]
		for _, s := range segs {
			switch {
			case s.seq < b.seq:
			case s.seq == b.seq:
				if err := truncateWALFile(s.path, b.offset+b.size); err != nil {
					return err
				}
			default:
				if err := truncateWALFile(s.path, 0); err != nil {
					return err
				}
			}
		}
		if err := l.resumeAt(b.seq, b.offset+b.size); err != nil {
			return err
		}
	} else {
		for _, s := range segs {
			if err := truncateWALFile(s.path, 0); err != nil {
				return err
			}
		}
		if err := l.resumeAt(firstRaftIndex, 0); err != nil {
			return err
		}
	}

	dropped := len(l.entries) - keep
	l.entries = l.entries[:keep]
	if keep == 0 {
		// 整棵清空之后视图从新的基线起：没有快照时基线是 1，
		// 有快照时基线不许退到快照之前（5.1 §5）。
		l.baseIndex = max(index+1, l.baseIndex)
		l.baseTerm = 0
	}
	l.rebuildTermIndex()
	l.lg.Info("wal suffix dropped",
		zap.Uint64("kept_through_index", index),
		zap.Int("dropped_entries", dropped),
		zap.Int("entries_left", keep))
	return nil
}

// resumeAt 把追加位置挪回 seq 那个段，长度按 size 记账；段文件不存在（日志清空后）就新建。
func (l *wal) resumeAt(seq uint64, size int64) error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return kvstore.ErrIOFrom("close wal file", l.size, err)
		}
		l.file = nil
	}
	path := l.cfg.WALPath(seq)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		f, err := createWALSegment(l.cfg, seq)
		if err != nil {
			return err
		}
		l.file, l.seq, l.size = f, seq, 0
		return nil
	} else if err != nil {
		return kvstore.ErrIOFrom("stat wal file", 0, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return kvstore.ErrIOFrom("open wal file", size, err)
	}
	l.file, l.seq, l.size = f, seq, size
	return nil
}

// lastIndex 是日志最后一条的 index。日志被压缩过而本地暂时为空时，
// 它是基线减一：那个位置由快照覆盖，投票请求要报的就是这个数（5.1 §5）。
func (l *wal) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.baseIndex - 1
	}
	return l.entries[len(l.entries)-1].index
}

// lastTerm 是最后一条的 term，空日志时取基线位置上的 term；投票时比较日志新旧要用（3.1 §7 第 4 步）。
func (l *wal) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.baseTerm
	}
	return l.entries[len(l.entries)-1].term
}

func (l *wal) length() int { return len(l.entries) }

// entryAt 按 index 取一条。下标是 index 减去基线：压缩之后日志不再从 1 起，
// index 小于基线表示这一条已经不在本地日志里（5.1 §5）。
func (l *wal) entryAt(index uint64) (*walEntry, bool) {
	if index < l.baseIndex {
		return nil, false
	}
	offset := index - l.baseIndex
	if offset >= uint64(len(l.entries)) {
		return nil, false
	}
	return l.entries[offset], true
}

// entriesFrom 取 index 及其之后、最多 limit 条，用于组一次 AppendEntries（3.4 T23.1）。
func (l *wal) entriesFrom(index uint64, limit int) []*walEntry {
	if index < l.baseIndex {
		index = l.baseIndex
	}
	if index > l.lastIndex() {
		return nil
	}
	out := l.entries[index-l.baseIndex:]
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// 一个 term 在本节点日志里的第一条 index，Follower 用它回 conflictIndex（D20）。
func (l *wal) firstIndexOfTerm(term uint64) (uint64, bool) {
	index, ok := l.firstIndexByTerm[term]
	return index, ok
}

// 一个 term 在本节点日志里的最后一条 index，Leader 用它落 nextIndex（D20）。
func (l *wal) lastIndexOfTerm(term uint64) (uint64, bool) {
	index, ok := l.lastIndexByTerm[term]
	return index, ok
}

func (l *wal) trackTerm(e *walEntry) {
	if _, ok := l.firstIndexByTerm[e.term]; !ok {
		l.firstIndexByTerm[e.term] = e.index
	}
	l.lastIndexByTerm[e.term] = e.index
}

// 截断之后两个映射一起重建：被丢掉的那批可能同时带走了某个 term 的首条和末条，
// 增量维护要分四种情况，不如按剩下的条目重扫一遍。
func (l *wal) rebuildTermIndex() {
	l.firstIndexByTerm = map[uint64]uint64{}
	l.lastIndexByTerm = map[uint64]uint64{}
	for _, e := range l.entries {
		l.trackTerm(e)
	}
}

// 一个已提交的 raft 日志段。
type walSegment struct {
	seq  uint64
	path string
	size int64
}

// listWALSegments 按序号排好列出正式名的段；`*.tmp` 是没来得及提交的空文件，扫描之前先删掉（2.1 §6）。
func listWALSegments(cfg Config) ([]walSegment, error) {
	entries, err := os.ReadDir(cfg.WALDir())
	if err != nil {
		return nil, kvstore.ErrIOFrom("list wal dir", 0, err)
	}
	segs := make([]walSegment, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == ".tmp" {
			if err := os.Remove(filepath.Join(cfg.WALDir(), name)); err != nil {
				return nil, kvstore.ErrIOFrom("remove uncommitted wal file", 0, err)
			}
			continue
		}
		seq, ok := parseWALSeq(name)
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, kvstore.ErrIOFrom("stat wal file", 0, err)
		}
		segs = append(segs, walSegment{seq: seq, path: cfg.WALPath(seq), size: info.Size()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
}

func parseWALSeq(name string) (uint64, bool) {
	if len(name) != len("00000001.wal") || filepath.Ext(name) != ".wal" {
		return 0, false
	}
	var seq uint64
	if _, err := fmt.Sscanf(name[:8], "%08d", &seq); err != nil || seq == 0 {
		return 0, false
	}
	return seq, true
}

// createWALSegment 用「临时名 + rename」建段：崩溃只可能落在创建与改名之间，
// 遗留的空临时文件由 listWALSegments 删掉（D13、D15）。
func createWALSegment(cfg Config, seq uint64) (*os.File, error) {
	tmp := cfg.WALTmpPath(seq)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, kvstore.ErrIOFrom("create wal file", 0, err)
	}
	if err := os.Rename(tmp, cfg.WALPath(seq)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return nil, kvstore.ErrIOFrom("rename wal file", 0, err)
	}
	return f, nil
}

// scanWAL 逐文件逐条读回，第一条坏记录起裁剪并停止；它之后的正常记录一并丢弃（2.1 §5 的口径）。
// index 不连续判 ErrStateMismatch：raft 日志中间有洞就意味着复制逻辑已经错了，裁剪只会把错藏起来。
func scanWAL(cfg Config, lg *zap.Logger) ([]*walEntry, error) {
	return scanWALFrom(cfg, lg, firstRaftIndex)
}

// scanWALFrom 扫回日志条目。expectBase 是视图基线：第一条的 index 不得大于它，
// 大于就说明日志与快照之间有洞，判 ErrStateMismatch（5.1 §5）。
func scanWALFrom(cfg Config, lg *zap.Logger, expectBase uint64) ([]*walEntry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	segs, err := listWALSegments(cfg)
	if err != nil {
		return nil, err
	}

	out := make([]*walEntry, 0, 64)
	var next uint64
	for i, seg := range segs {
		entries, cut, corrupt, err := replayWALSegment(seg, lg)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			switch {
			case next == 0:
				if e.index > expectBase {
					return nil, ErrStateMismatchAt(e.index,
						fmt.Sprintf("wal starts at index %d, past the base index %d", e.index, expectBase))
				}
				next = e.index
			case e.index != next:
				return nil, ErrStateMismatchAt(e.index,
					fmt.Sprintf("wal has a hole: index %d follows %d", e.index, next-1))
			}
			next++
		}
		out = append(out, entries...)
		if !corrupt {
			continue
		}

		// 已确认后面的段不可达：本段截到坏记录起点，其后各段清空。
		// 下一次重启不再重复裁剪，坏尾也不会永远留在盘上。
		if err := truncateWALFile(seg.path, cut); err != nil {
			return nil, err
		}
		for _, later := range segs[i+1:] {
			if err := truncateWALFile(later.path, 0); err != nil {
				return nil, err
			}
		}
		lg.Warn("wal truncated",
			zap.String("file", seg.path),
			zap.Int64("offset", cut),
			zap.Int64("truncated_bytes", seg.size-cut))
		break
	}
	return out, nil
}

// replayWALSegment 重放一个段，返回读成功的条目、裁剪起点与该段是否损坏。
// 判定顺序按 3.1 §5：剩余不足 8B、len 越界、读不满 entry、crc 不符、op 非法。
func replayWALSegment(seg walSegment, lg *zap.Logger) ([]*walEntry, int64, bool, error) {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, false, kvstore.ErrIOFrom("open wal for recovery", 0, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var (
		out    []*walEntry
		offset int64
	)
	for offset < seg.size {
		head := make([]byte, kvstore.FrameHeaderSize)
		n, err := io.ReadFull(r, head)
		if err != nil {
			// 只剩几字节的帧头，含只有 len 没有 crc：都算坏尾起点。
			lg.Warn("corrupt record tail", walCorruptFields(seg.path, offset, kvstore.FrameHeaderSize, int64(n), "incomplete header")...)
			return out, offset, true, nil
		}
		size, checksum, err := kvstore.DecodeFrame(head, offset)
		if err != nil {
			lg.Warn("corrupt record tail", walCorruptFields(seg.path, offset, int64(size), 0, err.Error())...)
			return out, offset, true, nil
		}

		entry := make([]byte, size)
		n, err = io.ReadFull(r, entry)
		if err != nil {
			lg.Warn("corrupt record tail", walCorruptFields(seg.path, offset, int64(size), int64(n), "entry shorter than declared length")...)
			return out, offset, true, nil
		}
		if err := kvstore.CheckFrame(size, checksum, entry, offset); err != nil {
			lg.Warn("corrupt record tail", walCorruptFields(seg.path, offset, int64(size), int64(size), err.Error())...)
			return out, offset, true, nil
		}

		term, index, command, err := decodeWALRecord(entry, offset)
		if err != nil {
			lg.Warn("corrupt record tail", walCorruptFields(seg.path, offset, int64(size), int64(size), err.Error())...)
			return out, offset, true, nil
		}
		out = append(out, &walEntry{
			term:   term,
			index:  index,
			cmd:    append([]byte(nil), command...),
			seq:    seg.seq,
			offset: offset,
			size:   int64(kvstore.FrameHeaderSize) + int64(size),
		})
		offset += int64(kvstore.FrameHeaderSize) + int64(size)
	}
	return out, 0, false, nil
}

// walCorruptFields 与 2.1 §8 的损坏记录字段同款（3.1 §10 末行）。
func walCorruptFields(file string, offset, expectLen, gotLen int64, reason string) []zap.Field {
	return []zap.Field{
		zap.String("file", file),
		zap.Int64("offset", offset),
		zap.Int64("expect_len", expectLen),
		zap.Int64("got_len", gotLen),
		zap.String("reason", reason),
	}
}

// truncateWALFile 把文件截到 size 并单独落一次盘：截断本身也是复制出去过的边界。
func truncateWALFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return kvstore.ErrIOFrom("open wal for truncate", size, err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("truncate wal file", size, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("fsync wal file", size, err)
	}
	if err := f.Close(); err != nil {
		return kvstore.ErrIOFrom("close wal file", size, err)
	}
	return nil
}
