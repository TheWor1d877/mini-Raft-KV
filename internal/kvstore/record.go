package kvstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// 帧头 = len 4B + crc32c 4B；entry 对 wal 是不透明字节（D4）。
const (
	FrameHeaderSize = 8
	lenFieldSize    = 4
	minEntrySize    = 1 + lenFieldSize + lenFieldSize
)

// 记录格式见 2.1 §5；当前只有 Put，加操作码不改帧格式。
const OpPut byte = 0x01

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeEntry 编出 op|keylen|key|vallen|value。超限直接拒绝，不产生任何字节。
func EncodeEntry(key, value string) ([]byte, error) {
	if len(key) > MaxKeySize {
		return nil, ErrTooLargeField("key", len(key), MaxKeySize)
	}
	if len(value) > MaxValueSize {
		return nil, ErrTooLargeField("value", len(value), MaxValueSize)
	}
	entry := make([]byte, 0, minEntrySize+len(key)+len(value))
	entry = append(entry, OpPut)
	entry = appendUint32(entry, uint32(len(key)))
	entry = append(entry, key...)
	entry = appendUint32(entry, uint32(len(value)))
	return append(entry, value...), nil
}

// DecodeEntry 解出帧里的一条操作；op 与内部长度对不上都算坏记录。
func DecodeEntry(entry []byte, offset int64) (key, value string, err error) {
	if len(entry) < minEntrySize {
		return "", "", ErrCorruptAt(offset, fmt.Sprintf("entry is %d bytes, below the %d-byte minimum", len(entry), minEntrySize))
	}
	if op := entry[0]; op != OpPut {
		return "", "", ErrCorruptAt(offset, fmt.Sprintf("unknown op 0x%02x", op))
	}
	keyLen := int(binary.BigEndian.Uint32(entry[1:5]))
	if 1+lenFieldSize+keyLen+lenFieldSize > len(entry) {
		return "", "", ErrCorruptAt(offset, fmt.Sprintf("keylen %d runs past the entry end", keyLen))
	}
	valueLen := int(binary.BigEndian.Uint32(entry[1+lenFieldSize+keyLen:]))
	if 1+lenFieldSize+keyLen+lenFieldSize+valueLen != len(entry) {
		return "", "", ErrCorruptAt(offset, fmt.Sprintf("entry is %d bytes, keylen %d and vallen %d want %d",
			len(entry), keyLen, valueLen, 1+lenFieldSize+keyLen+lenFieldSize+valueLen))
	}
	return string(entry[5 : 5+keyLen]), string(entry[5+keyLen+lenFieldSize:]), nil
}

// EncodeFrame 给 entry 套上帧头，得到要落盘的一条记录。
func EncodeFrame(entry []byte) ([]byte, error) {
	if len(entry) > MaxEntrySize {
		return nil, ErrTooLargeField("entry", len(entry), MaxEntrySize)
	}
	size := uint32(len(entry))
	frame := make([]byte, 0, FrameHeaderSize+len(entry))
	frame = appendUint32(frame, size)
	frame = appendUint32(frame, crcOf(size, entry))
	return append(frame, entry...), nil
}

// DecodeFrame 解析 8 字节帧头，返回 entry 的长度与存储的校验值。
func DecodeFrame(head []byte, offset int64) (size, checksum uint32, err error) {
	if len(head) != FrameHeaderSize {
		return 0, 0, ErrCorruptAt(offset, fmt.Sprintf("header is %d bytes, want %d", len(head), FrameHeaderSize))
	}
	size = binary.BigEndian.Uint32(head[:4])
	if size > MaxEntrySize {
		return 0, 0, ErrCorruptAt(offset, fmt.Sprintf("declared length %d exceeds the %d-byte limit", size, MaxEntrySize))
	}
	return size, binary.BigEndian.Uint32(head[4:]), nil
}

// CheckFrame 按 crc 覆盖 4+len 的口径重算。
func CheckFrame(size, checksum uint32, entry []byte, offset int64) error {
	if got := crcOf(size, entry); got != checksum {
		return ErrCorruptAt(offset, fmt.Sprintf("crc mismatch: stored 0x%08x, recomputed 0x%08x", checksum, got))
	}
	return nil
}

// crc 覆盖帧头里的 len 字段与 entry 全部字节，即 4+len 个字节（D3）。
func crcOf(size uint32, entry []byte) uint32 {
	var head [lenFieldSize]byte
	binary.BigEndian.PutUint32(head[:], size)
	return crc32.Update(crc32.Checksum(head[:], crcTable), crcTable, entry)
}

// 日志里的 value_fp：crc32c 结果取十六进制前 8 位（2.1 §8 脱敏）。
func valueFingerprint(value string) string {
	return fmt.Sprintf("%08x", crc32.Checksum([]byte(value), crcTable))
}

func appendUint32(dst []byte, v uint32) []byte {
	var b [lenFieldSize]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}
