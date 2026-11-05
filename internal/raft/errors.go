package raft

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 投票回执与 raft 日志之间对不上就是这个类别：日志出现空洞，或者两个游标比日志还长。
var ErrStateMismatch = errors.New("raft state mismatch")

// ErrIO 与 ErrCorruptRecord 沿用 kvstore 的哨兵，不在 raft 里重复定义；
// 构造错误也直接用 kvstore.ErrIOFrom 与 kvstore.ErrCorruptAt（包装口径见 2.1 §4）。
// ErrNotLeader 与 ErrCanceled 随在途提案表一起建在 M10 的 T22.4：
// 转成 Follower 时在途的提案回 ErrNotLeader，Close 时回 ErrCanceled。
var (
	ErrNotLeader = errors.New("not the leader")
	ErrCanceled  = errors.New("raft proposal canceled")
)

type stateMismatchError struct {
	reason string
	index  uint64
}

func (e *stateMismatchError) Error() string {
	return fmt.Sprintf("%s at index=%d: %s", e.reason, e.index, ErrStateMismatch)
}

func (e *stateMismatchError) Unwrap() error { return ErrStateMismatch }

func ErrStateMismatchAt(index uint64, reason string) error {
	return &stateMismatchError{reason: reason, index: index}
}

// D5、D19：raft 日志的 Sync 失败之后，盘上边界未知，写路径一律拒绝，读路径继续服务。
var errWALDisabled = fmt.Errorf("wal disabled by an earlier fsync failure: %w", kvstore.ErrIO)

// handler 侧的类别映射：只按哨兵判，不看错误文本（与 2.1 §4 同口径）。
// ErrStateMismatch 有专门的 code state_mismatch（5.1 §4）：它有两类来源，
// 一类是节点状态没到（快照点选不出来），一类是复制逻辑已经错了。
func statusFor(err error) (int, string) {
	switch {
	case errors.Is(err, ErrNotLeader):
		return http.StatusServiceUnavailable, "not_leader"
	case errors.Is(err, ErrCanceled):
		return http.StatusInternalServerError, "canceled"
	case errors.Is(err, kvstore.ErrKeyNotFound):
		return http.StatusNotFound, "key_not_found"
	case errors.Is(err, kvstore.ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "too_large"
	case errors.Is(err, kvstore.ErrCorruptRecord):
		return http.StatusInternalServerError, "corrupt_record"
	case errors.Is(err, kvstore.ErrIO):
		return http.StatusInternalServerError, "io_error"
	case errors.Is(err, ErrStateMismatch):
		// 快照点选不出来这一类：请求本身没问题，是节点的状态还没到（5.1 §4）。
		return http.StatusConflict, "state_mismatch"
	default:
		return http.StatusInternalServerError, "internal"
	}
}
