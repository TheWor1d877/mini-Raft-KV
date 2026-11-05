package kvstore

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// 内存状态机：写路径先追加 WAL，落盘到位之后才改 map。
type KV struct {
	cfg  Config
	lg   *zap.Logger
	lock *dirLock
	wal  *wal

	mu   sync.RWMutex
	data map[string]string
}

type reqIDKey struct{}

// 入口为每个请求生成 req_id，一路带到写路径的失败日志里（2.1 §8）。
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, reqIDKey{}, id)
}

func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(reqIDKey{}).(string)
	return id
}

func Open(cfg Config, lg *zap.Logger) (*KV, error) {
	start := time.Now()

	// 锁没拿到时传 nil：unlock 对空句柄是安全的，不能去放掉别人的锁。
	fail := func(err error, lock *dirLock) (*KV, error) {
		lg.Error("open failed", zap.String("data_dir", cfg.DataDir), zap.Error(err))
		_ = lock.unlock()
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return fail(err, nil)
	}
	lock, err := lockAndPrepare(cfg, lg)
	if err != nil {
		return fail(err, nil)
	}
	var rec recovered
	if !cfg.SkipWALRecovery {
		rec, err = scanWAL(cfg, lg)
		if err != nil {
			return fail(err, lock)
		}
	}
	w, err := newWAL(cfg, lg)
	if err != nil {
		return fail(err, lock)
	}

	data := make(map[string]string, len(rec.entries))
	for _, e := range rec.entries {
		data[e[0]] = e[1]
	}
	lg.Info("store opened",
		zap.String("data_dir", cfg.DataDir),
		zap.Int("file_count", rec.fileCount),
		zap.Int("recovered_entries", len(rec.entries)),
		zap.Bool("skip_wal_recovery", cfg.SkipWALRecovery),
		zap.Int64("duration_ms", durationMS(time.Since(start))))

	return &KV{cfg: cfg, lg: WithComponent(lg, ComponentKV), lock: lock, wal: w, data: data}, nil
}

// 追加与批次落盘都成功，才让这条写入对读可见。
func (k *KV) Put(ctx context.Context, key, value string) error {
	entry, err := EncodeEntry(key, value)
	if err != nil {
		return err
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	opSeq, err := k.wal.append(entry)
	if err != nil {
		k.lg.Error("put rejected",
			zap.String("req_id", RequestID(ctx)),
			zap.Int("errno", int(errnoOf(err))),
			zap.Error(err))
		return err
	}
	k.data[key] = value
	k.lg.Debug("put applied",
		zap.String("req_id", RequestID(ctx)),
		zap.Uint64("op_seq", opSeq),
		KeyField(key),
		ValueField(value),
		zap.String("value_fp", valueFingerprint(value)))
	return nil
}

// Apply 只做状态机改动，不落任何日志：raft 日志的回放与补 apply 走这条路（docs/8 §二）。
func (k *KV) Apply(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = value
}

func (k *KV) Get(key string) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	value, ok := k.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

func (k *KV) Flush() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.wal.Flush()
}

// 落盘剩余批次，再释放文件与目录锁。
func (k *KV) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	err := k.wal.Close()
	if uerr := k.lock.unlock(); err == nil {
		err = uerr
	}
	return err
}

// 写路径是否已被 fsync 失败关掉；读路径不受影响（D5）。
func (k *KV) WriteDisabled() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.wal.disabled()
}
