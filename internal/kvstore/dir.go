package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"go.uber.org/zap"
)

// 目录独占锁。持有期间保证同一时刻只有一个进程写 WAL。
type dirLock struct {
	file *os.File
	path string
}

// 顺序不能颠倒：先把数据目录建出来才谈得上锁，WAL 子目录要拿到锁之后再建，
// 否则第二个进程会看到半初始化好的目录。
func lockAndPrepare(cfg Config, lg *zap.Logger) (*dirLock, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, ErrIOFrom("create data dir", 0, err)
	}
	lock, err := lockDir(cfg, lg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.WALDir(), 0o755); err != nil {
		_ = lock.unlock()
		return nil, ErrIOFrom("create wal dir", 0, err)
	}
	return lock, nil
}

func lockDir(cfg Config, lg *zap.Logger) (*dirLock, error) {
	path := cfg.LockPath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, ErrIOFrom("open lock file", 0, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		lg.Error("data dir is locked by another process", zap.String("lock_path", path), zap.Error(err))
		return nil, ErrLockedFrom(path, err)
	}
	lg.Info("data dir locked", zap.String("lock_path", path))
	return &dirLock{file: f, path: path}, nil
}

// 关 fd 即释放 flock；进程崩溃时内核也会释放，所以不存在残留锁挡住重启。
func (d *dirLock) unlock() error {
	if d == nil || d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return ErrIOFrom("close lock file", 0, err)
}

// 一个已提交的 WAL 段。
type segment struct {
	seq  uint64
	path string
	size int64
}

// 按序号排序列出正式名的段；临时名一律删除，不留到下一次恢复。
func listSegments(cfg Config) ([]segment, error) {
	entries, err := os.ReadDir(cfg.WALDir())
	if err != nil {
		return nil, ErrIOFrom("list wal dir", 0, err)
	}
	segs := make([]segment, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == ".tmp" {
			if err := os.Remove(filepath.Join(cfg.WALDir(), name)); err != nil {
				return nil, ErrIOFrom("remove uncommitted wal file", 0, err)
			}
			continue
		}
		seq, ok := parseSeq(name)
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, ErrIOFrom("stat wal file", 0, err)
		}
		segs = append(segs, segment{seq: seq, path: cfg.WALPath(seq), size: info.Size()})
	}
	// 序号补零到八位，字典序与序号序一致；仍按数值排，避免命名规则一改就静默错位。
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
}

func parseSeq(name string) (uint64, bool) {
	if len(name) != len("00000001.wal") || filepath.Ext(name) != ".wal" {
		return 0, false
	}
	var seq uint64
	if _, err := fmt.Sscanf(name[:8], "%08d", &seq); err != nil || seq == 0 {
		return 0, false
	}
	return seq, true
}
