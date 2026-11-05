package kvstore

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// 内存状态机：只有 map。持久性由 raft 日志的追加与 Sync 负责，这里不再有自己的操作日志（docs/8 §二）。
type KV struct {
	cfg  Config
	lg   *zap.Logger
	lock *dirLock

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

// Open 只建目录、拿锁，map 交给调用方回放日志重建（3.1 §7 的启动序列）。
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
	lg.Info("store opened",
		zap.String("data_dir", cfg.DataDir),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))

	return &KV{cfg: cfg, lg: WithComponent(lg, ComponentKV), lock: lock, data: map[string]string{}}, nil
}

// Apply 是一次状态机改动：不落日志、不 fsync，落盘边界由调用方的日志写入定（docs/8 §二）。
// 重复 apply 同一条赋值没有副作用，所以恢复与补 apply 都可以从头再来一遍（D4）。
func (k *KV) Apply(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = value
	k.lg.Debug("entry applied",
		KeyField(key),
		ValueField(value),
		zap.String("value_fp", valueFingerprint(value)))
}

// Entry 是状态机里的一条键值，导出与整体替换都用它（5.1 §3）。
type Entry struct {
	Key   string
	Value string
}

// Snapshot 导出全量键值：快照文件要的就是这份内容（5.1 §7 的生成与提交第 2 步）。
// 结果按 key 升序，顺序稳定：同一份状态机导出两次得到同样的字节。
func (k *KV) Snapshot() []Entry {
	k.mu.RLock()
	defer k.mu.RUnlock()

	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out := make([]Entry, 0, len(keys))
	for _, key := range keys {
		out = append(out, Entry{Key: key, Value: k.data[key]})
	}
	return out
}

// Restore 整体替换状态机：接收对端发来的快照时用（5.1 §7 的接收快照第 3 步）。
// 替换之后 map 里没有的键读不到；重复 Restore 同一份内容没有副作用。
func (k *KV) Restore(entries []Entry) {
	k.mu.Lock()
	defer k.mu.Unlock()

	data := make(map[string]string, len(entries))
	for _, e := range entries {
		data[e.Key] = e.Value
	}
	k.data = data
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

// 释放目录锁：map 里没有等着落盘的东西，日志与游标的收尾归 raft 侧。
func (k *KV) Close() error {
	return k.lock.unlock()
}
