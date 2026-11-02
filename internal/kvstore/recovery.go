package kvstore

import (
	"bufio"
	"io"
	"os"

	"go.uber.org/zap"
)

// 扫描结果：重放出来的记录与统计。
type recovered struct {
	entries        [][2]string
	fileCount      int
	truncatedFile  string
	truncatedBytes int64
}

// 逐文件逐条校验，第一条坏记录起裁剪并停止；它之后的正常记录一并丢弃。
func scanWAL(cfg Config, lg *zap.Logger) (recovered, error) {
	if err := cfg.Validate(); err != nil {
		return recovered{}, err
	}
	// 未提交的空临时文件先清掉，正式名才可能是完整的段。
	segs, err := listSegments(cfg)
	if err != nil {
		return recovered{}, err
	}

	log := WithComponent(lg, ComponentRecovery)
	out := recovered{fileCount: len(segs)}
	for i, seg := range segs {
		entries, cut, corrupt, err := replaySegment(seg, log)
		if err != nil {
			return recovered{}, err
		}
		out.entries = append(out.entries, entries...)
		if !corrupt {
			continue
		}

		out.truncatedFile, out.truncatedBytes = seg.path, seg.size-cut
		// 已确认后面的段不可达：本段裁到坏记录起点，其后各段清空。
		// 下一次重启不再重复裁剪，坏尾也不会永远留在盘上。
		if err := truncateAt(seg.path, cut); err != nil {
			return recovered{}, err
		}
		for _, later := range segs[i+1:] {
			if err := truncateAt(later.path, 0); err != nil {
				return recovered{}, err
			}
			out.truncatedBytes += later.size
		}
		log.Warn("wal truncated",
			zap.String("file", seg.path),
			zap.Int64("offset", cut),
			zap.Int64("truncated_bytes", seg.size-cut))
		break
	}
	return out, nil
}

// 重放一个段，返回读成功的记录、裁剪起点与该段是否有损坏。
// 判定顺序按 2.1 §5：剩余不足 8B、len 越界、读不满 entry、crc 不符、op 非法。
func replaySegment(seg segment, log *zap.Logger) ([][2]string, int64, bool, error) {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, false, ErrIOFrom("open wal for recovery", 0, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var (
		entries [][2]string
		offset  int64
	)
	for offset < seg.size {
		head := make([]byte, FrameHeaderSize)
		n, err := io.ReadFull(r, head)
		if err != nil {
			// 只剩几字节的帧头，含只有 len 没有 crc：都算坏尾起点。
			log.Warn("corrupt record tail", corruptFields(seg.path, offset, FrameHeaderSize, int64(n), "incomplete header")...)
			return entries, offset, true, nil
		}
		size, checksum, err := DecodeFrame(head, offset)
		if err != nil {
			log.Warn("corrupt record tail", corruptFields(seg.path, offset, int64(size), 0, err.Error())...)
			return entries, offset, true, nil
		}

		entry := make([]byte, size)
		n, err = io.ReadFull(r, entry)
		if err != nil {
			log.Warn("corrupt record tail", corruptFields(seg.path, offset, int64(size), int64(n), "entry shorter than declared length")...)
			return entries, offset, true, nil
		}
		if err := CheckFrame(size, checksum, entry, offset); err != nil {
			log.Warn("corrupt record tail", corruptFields(seg.path, offset, int64(size), int64(size), err.Error())...)
			return entries, offset, true, nil
		}

		key, value, err := DecodeEntry(entry, offset)
		if err != nil {
			log.Warn("corrupt record tail", corruptFields(seg.path, offset, int64(size), int64(size), err.Error())...)
			return entries, offset, true, nil
		}
		entries = append(entries, [2]string{key, value})
		offset += FrameHeaderSize + int64(size)
	}
	return entries, 0, false, nil
}

func corruptFields(file string, offset, expectLen, gotLen int64, reason string) []zap.Field {
	return []zap.Field{
		zap.String("file", file),
		zap.Int64("offset", offset),
		zap.Int64("expect_len", expectLen),
		zap.Int64("got_len", gotLen),
		zap.String("reason", reason),
	}
}

func truncateAt(path string, size int64) error {
	if err := os.Truncate(path, size); err != nil {
		return ErrIOFrom("truncate wal file", size, err)
	}
	return nil
}
