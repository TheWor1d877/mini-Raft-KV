package kvstore

import (
	"os"
	"syscall"

	"go.uber.org/zap"
)

// 目录独占锁。持有期间保证同一时刻只有一个进程写这个数据目录。
type dirLock struct {
	file *os.File
	path string
}

// 顺序不能颠倒：先把数据目录建出来才谈得上锁，锁没拿到就不许再往下建任何东西。
func lockAndPrepare(cfg Config, lg *zap.Logger) (*dirLock, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, ErrIOFrom("create data dir", 0, err)
	}
	return lockDir(cfg, lg)
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
