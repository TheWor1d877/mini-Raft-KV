package kvstore

import (
	"os"
	"time"

	"go.uber.org/zap"
)

// 追加写与批量 fsync。批次阈值和文件大小上限只从 cfg 取（D6、D8）。
type wal struct {
	cfg     Config
	lg      *zap.Logger
	file    *os.File
	seq     uint64
	size    int64
	synced  int64
	pending int
	opSeq   uint64
	failed  bool
}

// 接在已有最大序号文件末尾；一个段都没有就从 00000001.wal 起。
func newWAL(cfg Config, lg *zap.Logger) (*wal, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	segs, err := listSegments(cfg)
	if err != nil {
		return nil, err
	}
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, ErrIOFrom("open wal file", last.size, err)
		}
		return &wal{cfg: cfg, lg: WithComponent(lg, ComponentWAL), file: f,
			seq: last.seq, size: last.size, synced: last.size}, nil
	}
	f, err := createSegment(cfg, 1)
	if err != nil {
		return nil, err
	}
	return &wal{cfg: cfg, lg: WithComponent(lg, ComponentWAL), file: f, seq: 1}, nil
}

func (w *wal) activePath() string { return w.cfg.WALPath(w.seq) }

// 越过后写路径一律返回错误，读路径继续服务（D5）。
func (w *wal) disabled() bool { return w.failed }

// fsync 失败或已关闭之后，写路径一律拒绝（D5）。
func (w *wal) writeBlocked() error {
	if w.failed || w.file == nil {
		return errWALDisabled
	}
	return nil
}

// 追加一条记录：越过上限先滚动，写成功才计批次，攒够一批落一次盘。
func (w *wal) append(entry []byte) (uint64, error) {
	if err := w.writeBlocked(); err != nil {
		return 0, err
	}
	frame, err := EncodeFrame(entry)
	if err != nil {
		return 0, err
	}
	if w.size+int64(len(frame)) > w.cfg.MaxFileSize {
		if err := w.rollover(); err != nil {
			return 0, err
		}
	}

	w.opSeq++
	opSeq := w.opSeq
	start := time.Now()
	offset := w.size
	n, err := w.file.Write(frame)
	w.size += int64(n)
	if err != nil {
		w.lg.Error("append failed",
			zap.Uint64("op_seq", opSeq),
			zap.Int("errno", int(errnoOf(err))),
			zap.Int64("duration_ms", durationMS(time.Since(start))),
			zap.Error(err))
		return 0, ErrIOFrom("append wal record", offset, err)
	}

	w.pending++
	if w.pending >= w.cfg.BatchSize {
		if err := w.sync(); err != nil {
			return 0, err
		}
	}
	return opSeq, nil
}

func (w *wal) Flush() error {
	if err := w.writeBlocked(); err != nil {
		return err
	}
	return w.sync()
}

// 一次 fsync。失败无法判断盘上有什么，直接失效（D5）。
// `-fsync=false` 只跳过系统调用，批次计数照走，给 T14 留「完全不 Sync」这一档对照。
func (w *wal) sync() error {
	if err := w.writeBlocked(); err != nil {
		return err
	}
	count := w.pending
	start := time.Now()
	if w.cfg.Fsync {
		if err := w.file.Sync(); err != nil {
			w.disable(err)
			return ErrIOFrom("fsync wal", w.size, err)
		}
	}
	w.synced = w.size
	w.pending = 0
	w.lg.Debug("wal synced",
		zap.Int("batch_count", count),
		zap.Bool("fsync", w.cfg.Fsync),
		zap.Int64("duration_ms", durationMS(time.Since(start))))
	return nil
}

func (w *wal) Close() error {
	if w.file == nil {
		return nil
	}
	pending := w.pending
	start := time.Now()

	var err error
	if !w.failed {
		err = w.sync()
	}
	if cerr := w.file.Close(); err == nil {
		err = ErrIOFrom("close wal file", w.size, cerr)
	}
	w.file = nil
	w.lg.Info("wal closed",
		zap.Int("pending_batch", pending),
		zap.Int64("duration_ms", durationMS(time.Since(start))))
	return err
}

// 换段：旧文件先落盘，新文件以临时名创建再原子改名，序号与偏移归零、批次继续。
func (w *wal) rollover() error {
	if err := w.sync(); err != nil {
		return err
	}
	from := w.activePath()
	next := w.seq + 1
	f, err := createSegment(w.cfg, next)
	if err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		_ = f.Close()
		return ErrIOFrom("close wal file", w.size, err)
	}
	w.lg.Info("wal rolled over",
		zap.String("from_file", from),
		zap.String("to_file", w.cfg.WALPath(next)),
		zap.Int64("size_bytes", w.size))
	w.file = f
	w.seq = next
	w.size = 0
	w.synced = 0
	return nil
}

func (w *wal) disable(cause error) {
	if w.failed {
		return
	}
	w.failed = true
	w.lg.Error("wal disabled",
		zap.Uint64("failed_at_seq", w.opSeq),
		zap.Int("errno", int(errnoOf(cause))),
		zap.Error(cause))
}

// 建段用「临时名 + rename」：崩溃只可能落在创建与改名之间，遗留的空临时文件由恢复删除。
// 若全程以临时名接收写入，恢复按 §6 删 tmp，丢的是整段而不是最近 63 条。
func createSegment(cfg Config, seq uint64) (*os.File, error) {
	tmp := cfg.WALTmpPath(seq)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, ErrIOFrom("create wal file", 0, err)
	}
	if err := os.Rename(tmp, cfg.WALPath(seq)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return nil, ErrIOFrom("rename wal file", 0, err)
	}
	return f, nil
}

func durationMS(d time.Duration) int64 { return d.Milliseconds() }
