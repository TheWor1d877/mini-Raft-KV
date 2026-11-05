package kvstore

import (
	"errors"
	"fmt"
	"syscall"
)

// 调用方一律用 errors.Is 判类，不匹配字符串。
var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrCorruptRecord = errors.New("corrupt record")
	ErrIO            = errors.New("io error")
	ErrLocked        = errors.New("data dir locked")
	ErrTooLarge      = errors.New("entry too large")
)

// offset 必须进坏记录的错误文本：没有它，排障时无法定位到 WAL 文件的字节位置。
// 同一个 offset 还要能被 HTTP 入口取出来放进响应体，所以做成带字段的结构化错误。
type corruptError struct {
	offset int64
	reason string
}

func (e *corruptError) Error() string {
	return fmt.Sprintf("%s at offset=%d: %s", e.reason, e.offset, ErrCorruptRecord)
}

func (e *corruptError) Unwrap() error { return ErrCorruptRecord }

func ErrCorruptAt(offset int64, reason string) error {
	return &corruptError{offset: offset, reason: reason}
}

// CorruptOffset 取坏记录的字节位置；没有携带位置时第二个返回值为 false。
func CorruptOffset(err error) (int64, bool) {
	var c *corruptError
	if errors.As(err, &c) {
		return c.offset, true
	}
	return 0, false
}

// 两个 %w 分别包住 ErrIO 与原始错误：前者供 errors.Is 判类，后者让 errors.As 取得 syscall.Errno。
func ErrIOFrom(op string, offset int64, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s at offset=%d: %w: %w", op, offset, ErrIO, err)
}

func ErrLockedFrom(dir string, err error) error {
	return fmt.Errorf("lock %s: %w: %w", dir, ErrLocked, err)
}

// errnoOf 给日志的 errno 字段取值，取不到返回 0，调用方再按错误文本兜底。
func errnoOf(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return 0
}

func ErrTooLargeField(field string, size, limit int) error {
	return fmt.Errorf("%s is %d bytes, limit %d: %w", field, size, limit, ErrTooLarge)
}
