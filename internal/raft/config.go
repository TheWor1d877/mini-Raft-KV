package raft

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 默认值、上限与命名规则只有这一处；state 与 WAL 都从这里取（与 2.1 §6 同口径）。
// 段上限沿用 kvstore.DefaultMaxFileSize：命令行只有一根 -max-file-size，两个模块同一个值（3.1 §6）。
const (
	DefaultLogLevel = kvstore.DefaultLogLevel

	// 三个时钟的默认值与 3.1 §6 的开关表一一对应，M12 的 flag 解析直接拿它们当默认。
	DefaultElectionTimeoutMS = int64(500)
	DefaultHeartbeatMS       = int64(100)
	DefaultTickMS            = int64(50)

	// 读路径三个开关的默认值（4.1 §6）。
	DefaultReadMode      = ReadModeLease
	DefaultClockDriftMS  = int64(50)
	DefaultReadTimeoutMS = int64(1000)

	// 快照与压缩四个开关的默认值（5.1 §6）。
	DefaultSnapshotEntries       = uint64(1000)
	DefaultSnapshotBytes         = int64(64 << 20)
	DefaultSnapshotMinIntervalMS = int64(60000)
	DefaultSnapshotExemptEntries = uint64(1000)

	RaftDirName      = "raft"
	WALDirName       = "wal"
	StateFileName    = "state"
	StateTmpFileName = "state.tmp"
	SnapshotDirName  = "snapshot"

	// 快照文件与元数据指针的命名（5.1 §6）。
	SnapshotFileFormat     = "%08d.snap"
	SnapshotTmpFileFormat  = "%08d.snap.tmp"
	SnapshotPointerName    = "current.json"
	SnapshotPointerTmpName = "current.json.tmp"

	WALFileFormat    = "%08d.wal"
	WALTmpFileFormat = "%08d.wal.tmp"

	// 一个进程一份节点日志；kvstore 的 NewLogger 固定写 2.1 的 miniraft-kv.log，导出面不动，
	// 所以 raft 侧按同款编码自己开一份（3.1 §6 末段）。
	NodeLogFileName = "miniraft-node.log"

	// component 取值在 2.1 §8 的集合上加这一个（3.1 §10）：
	// 合并之后 WAL 就是唯一的日志部件，raftlog 与 recovery 两个旧值一并收进 kvstore 的 wal。
	ComponentRaft = "raft"
)

// ReadMode 是节点级的默认线性读法：lease 走领导权租约，readindex 走逐读确认轮（4.1 §6）。
// 它与请求级的 Consistency 是两条轴：Consistency 决定这条读要不要领导权确认，
// ReadMode 决定线性读走租约还是逐读确认。
type ReadMode string

const (
	ReadModeLease     ReadMode = "lease"
	ReadModeReadIndex ReadMode = "readindex"
)

// Config 装 raft 层的参数与路径。九项字段在 M10 的 T21.1 一次建全，
// M12 的 T25.1 只做 flag 解析与 -peers 成员集合一致性检查，不再动结构体（3.1 §6）。
// M14 的 T29.2 加读路径三项（4.1 §6）。
type Config struct {
	NodeID string
	// Peers 是 id→host:port 的静态成员表，必须含 NodeID 自己；节点数 N 取它的长度。
	Peers             map[string]string
	DataDir           string
	ElectionTimeoutMS int64
	HeartbeatMS       int64
	TickMS            int64
	MaxFileSize       int64
	Fsync             bool
	LogLevel          string

	// ReadMode 是节点级默认读法；ClockDriftMS 是租约的时钟漂移余量；
	// ReadTimeoutMS 是单次线性读从登记到返回的上界（4.1 §6）。
	ReadMode      ReadMode
	ClockDriftMS  int64
	ReadTimeoutMS int64

	// 快照与压缩的四个参数（5.1 §6）：前两个是触发阈值，后两个管删除边界与豁免。
	// 两个阈值都为 0 时自动触发全关，手动入口任何时候可调。
	SnapshotEntries       uint64
	SnapshotBytes         int64
	SnapshotMinIntervalMS int64
	SnapshotExemptEntries uint64
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMS: DefaultElectionTimeoutMS,
		HeartbeatMS:       DefaultHeartbeatMS,
		TickMS:            DefaultTickMS,
		MaxFileSize:       kvstore.DefaultMaxFileSize,
		Fsync:             true,
		LogLevel:          DefaultLogLevel,
		ReadMode:          DefaultReadMode,
		ClockDriftMS:      DefaultClockDriftMS,
		ReadTimeoutMS:     DefaultReadTimeoutMS,

		SnapshotEntries:       DefaultSnapshotEntries,
		SnapshotBytes:         DefaultSnapshotBytes,
		SnapshotMinIntervalMS: DefaultSnapshotMinIntervalMS,
		SnapshotExemptEntries: DefaultSnapshotExemptEntries,
	}
}

// 成员集合的固定遍历顺序：按 id 升序。map 的迭代顺序是随机的，
// 而 D23 要「一轮选举发出的投票条数与顺序可断言」，所以发 RPC 只走这两个函数。
func (c Config) peerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for id := range c.Peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// otherPeers 是去掉自己之后的成员表，Leader 的心跳和候选人的投票都发给它。
func (c Config) otherPeers() []string {
	return slices.DeleteFunc(c.peerIDs(), func(id string) bool { return id == c.NodeID })
}

// Addr 取某个成员的监听地址，id 不在成员表里时返回空串。
func (c Config) Addr(id string) string { return c.Peers[id] }

// Majority 是 N/2+1，5 节点即 3（3.1 §7）。
func (c Config) Majority() int { return len(c.Peers)/2 + 1 }

func (c Config) RaftDir() string      { return filepath.Join(c.DataDir, RaftDirName) }
func (c Config) StatePath() string    { return filepath.Join(c.RaftDir(), StateFileName) }
func (c Config) StateTmpPath() string { return filepath.Join(c.RaftDir(), StateTmpFileName) }
func (c Config) WALDir() string       { return filepath.Join(c.RaftDir(), WALDirName) }

func (c Config) WALPath(seq uint64) string {
	return filepath.Join(c.WALDir(), fmt.Sprintf(WALFileFormat, seq))
}

func (c Config) WALTmpPath(seq uint64) string {
	return filepath.Join(c.WALDir(), fmt.Sprintf(WALTmpFileFormat, seq))
}

// 快照目录与两个路径（5.1 §6）：快照文件带 index，元数据指针固定一个名字。
func (c Config) SnapshotDir() string { return filepath.Join(c.RaftDir(), SnapshotDirName) }

func (c Config) SnapshotPath(index uint64) string {
	return filepath.Join(c.SnapshotDir(), fmt.Sprintf(SnapshotFileFormat, index))
}

func (c Config) SnapshotTmpPath(index uint64) string {
	return filepath.Join(c.SnapshotDir(), fmt.Sprintf(SnapshotTmpFileFormat, index))
}

func (c Config) SnapshotPointerPath() string {
	return filepath.Join(c.SnapshotDir(), SnapshotPointerName)
}

func (c Config) SnapshotPointerTmpPath() string {
	return filepath.Join(c.SnapshotDir(), SnapshotPointerTmpName)
}

// LogDir 与 NodeLogPath 走的是 2.1 的目录布局：日志跟 wal、raft 平级，都在数据目录里。
func (c Config) LogDir() string { return filepath.Join(c.DataDir, kvstore.LogDirName) }

func (c Config) NodeLogPath() string { return filepath.Join(c.LogDir(), NodeLogFileName) }

// rpcTimeout 是 HTTP 客户端单条 RPC 的超时：min(2×-tick-ms, -election-timeout-ms)。
// 串行 RPC 让一轮选举的最坏耗时是 4 次超时相加，取这个值才能保证一轮选举不被死节点拖过 T（3.1 §9、D23）。
func (c Config) rpcTimeout() time.Duration {
	return time.Duration(min(2*c.TickMS, c.ElectionTimeoutMS)) * time.Millisecond
}

// Validate 非法值直接报错，不回落默认（与 2.1 §6 同口径）。
// 时钟与成员的五种非法输入见 3.1 §6：心跳不短于选举超时、两个时钟与 tick 不为正、
// 成员表为空、本 id 不在成员表里。
func (c Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data dir must not be empty")
	}
	if c.MaxFileSize <= 0 {
		return fmt.Errorf("max file size %d must be positive", c.MaxFileSize)
	}
	if len(c.Peers) == 0 {
		return fmt.Errorf("peers must not be empty")
	}
	if c.NodeID == "" {
		return fmt.Errorf("node id must not be empty")
	}
	if _, ok := c.Peers[c.NodeID]; !ok {
		return fmt.Errorf("node id %q is not in the peer set", c.NodeID)
	}
	if c.TickMS <= 0 {
		return fmt.Errorf("tick interval %dms must be positive", c.TickMS)
	}
	if c.ElectionTimeoutMS <= 0 {
		return fmt.Errorf("election timeout %dms must be positive", c.ElectionTimeoutMS)
	}
	if c.HeartbeatMS >= c.ElectionTimeoutMS {
		return fmt.Errorf("heartbeat %dms must be shorter than the election timeout %dms",
			c.HeartbeatMS, c.ElectionTimeoutMS)
	}
	if _, err := parseLogLevel(c.LogLevel); err != nil {
		return err
	}
	switch c.ReadMode {
	case ReadModeLease, ReadModeReadIndex:
	default:
		return fmt.Errorf("read mode %q must be %s or %s", c.ReadMode, ReadModeLease, ReadModeReadIndex)
	}
	if c.ClockDriftMS < 0 {
		return fmt.Errorf("clock drift %dms must not be negative", c.ClockDriftMS)
	}
	if c.ReadTimeoutMS <= 0 {
		return fmt.Errorf("read timeout %dms must be positive", c.ReadTimeoutMS)
	}
	if c.SnapshotBytes < 0 {
		return fmt.Errorf("snapshot bytes %d must not be negative", c.SnapshotBytes)
	}
	if c.SnapshotMinIntervalMS < 0 {
		return fmt.Errorf("snapshot min interval %dms must not be negative", c.SnapshotMinIntervalMS)
	}
	// 一轮广播的最坏耗时是 (N-1) 次串行 RPC（4.1 §6、D23）。
	broadcastMS := int64(len(c.Peers)-1) * c.rpcTimeout().Milliseconds()
	switch c.ReadMode {
	case ReadModeLease:
		// 租约窗口要长过一轮广播，否则确认还没回来租约已经过期，Lease Read 一次也命中不了。
		if window := c.ElectionTimeoutMS - c.ClockDriftMS; window <= broadcastMS {
			return fmt.Errorf("lease window %dms is not longer than one broadcast %dms", window, broadcastMS)
		}
	case ReadModeReadIndex:
		// 读超时短过一轮广播，读会在确认回来之前就超时。
		if c.ReadTimeoutMS < broadcastMS {
			return fmt.Errorf("read timeout %dms is shorter than one broadcast %dms", c.ReadTimeoutMS, broadcastMS)
		}
	}
	return nil
}

// 建目录用 0755，与 kvstore 的 lockAndPrepare 一致（2.1 §6）。
func (c Config) ensureDirs() error {
	for _, dir := range []string{c.DataDir, c.RaftDir(), c.WALDir(), c.SnapshotDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return kvstore.ErrIOFrom("create raft dir", 0, err)
		}
	}
	return nil
}

func parseLogLevel(s string) (zapcore.Level, error) {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(s))); err != nil {
		return level, fmt.Errorf("log level %q: %w", s, err)
	}
	return level, nil
}

// Load 解析 miniraft-node 的命令行：开关表见 3.1 §6，九项进 raft 的 Config，
// `-max-file-size` 与 `-log-level` 两边同一个值（只有一根开关）。
// `-data-dir` 不给就按 `data/node-<id>` 推，`-addr` 不给就取 `-peers` 里本 id 那项（D8 的相对路径口径）。
// `-raft-fsync` 只管日志，合并之后它是写路径上唯一的数据 Sync（D19）。
func Load(args []string) (Config, kvstore.Config, error) {
	return load("miniraft-node", args, true)
}

// LoadSingleNode 解析 miniraft-kv 的命令行：成员表只有进程自己，`-peers` 与 `-node-id` 可省，
// 地址取 `-addr`，数据目录默认 `data`（docs/8 §二 的单节点集群）。
func LoadSingleNode(args []string) (Config, kvstore.Config, error) {
	return load("miniraft-kv", args, false)
}

// memberTable 为真时成员表必须由命令行给；为假时用 `-addr` 现造一张只有自己的表。
func load(name string, args []string, memberTable bool) (Config, kvstore.Config, error) {
	cfg := DefaultConfig()
	kcfg := kvstore.DefaultConfig()

	var nodeID, peersFlag, addrFlag, dataDir, readMode string
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&nodeID, "node-id", "", "member id, must appear in -peers")
	fs.StringVar(&peersFlag, "peers", "", "member table, node-1=127.0.0.1:8801,node-2=...")
	fs.StringVar(&addrFlag, "addr", "", "listen address, defaults to this id's entry in -peers")
	fs.StringVar(&dataDir, "data-dir", "", "data directory, defaults to data/node-<id>")
	fs.Int64Var(&cfg.ElectionTimeoutMS, "election-timeout-ms", cfg.ElectionTimeoutMS, "base election timeout T")
	fs.Int64Var(&cfg.HeartbeatMS, "heartbeat-ms", cfg.HeartbeatMS, "leader heartbeat period, must be under T")
	fs.Int64Var(&cfg.TickMS, "tick-ms", cfg.TickMS, "period the entry layer calls Tick() at")
	fs.BoolVar(&cfg.Fsync, "raft-fsync", cfg.Fsync, "fsync every wal append; disable only for the throughput comparison")
	fs.Int64Var(&cfg.MaxFileSize, "max-file-size", cfg.MaxFileSize, "bytes per segment file before rollover")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn, error, panic, fatal")
	fs.StringVar(&readMode, "read-mode", string(cfg.ReadMode), "linearizable read path: lease or readindex")
	fs.Int64Var(&cfg.ClockDriftMS, "clock-drift-ms", cfg.ClockDriftMS, "clock drift margin taken off the lease window")
	fs.Int64Var(&cfg.ReadTimeoutMS, "read-timeout-ms", cfg.ReadTimeoutMS, "upper bound of one linearizable read")
	fs.Uint64Var(&cfg.SnapshotEntries, "snapshot-entries", cfg.SnapshotEntries, "log entries after the last snapshot that trigger one snapshot")
	fs.Int64Var(&cfg.SnapshotBytes, "snapshot-bytes", cfg.SnapshotBytes, "log directory size that triggers one snapshot")
	fs.Int64Var(&cfg.SnapshotMinIntervalMS, "snapshot-min-interval-ms", cfg.SnapshotMinIntervalMS, "minimum interval between two snapshots")
	fs.Uint64Var(&cfg.SnapshotExemptEntries, "snapshot-exempt-entries", cfg.SnapshotExemptEntries, "lag entries behind which a member is exempt from the safe index")
	if err := fs.Parse(args); err != nil {
		return Config{}, kvstore.Config{}, err
	}
	cfg.ReadMode = ReadMode(readMode)

	if !memberTable {
		if nodeID == "" {
			nodeID = "node-1"
		}
		if addrFlag == "" {
			addrFlag = kvstore.DefaultAddr
		}
		peersFlag = nodeID + "=" + addrFlag
	}
	peers, err := parsePeers(peersFlag)
	if err != nil {
		return Config{}, kvstore.Config{}, err
	}
	cfg.NodeID, cfg.Peers = nodeID, peers
	if dataDir == "" {
		if memberTable {
			dataDir = filepath.Join(kvstore.DefaultDataDir, nodeID)
		} else {
			dataDir = kvstore.DefaultDataDir
		}
	}
	if addrFlag == "" {
		addrFlag = peers[nodeID]
	} else if own, ok := peers[nodeID]; ok && addrFlag != own {
		// 回执与 RPC 寻址只走成员表：-addr 和它不一致的话，别的节点按表发来的请求就落在没人听的地址上。
		return Config{}, kvstore.Config{}, fmt.Errorf("-addr %q disagrees with -peers entry %q for %s", addrFlag, own, nodeID)
	}
	cfg.DataDir = dataDir
	kcfg.DataDir, kcfg.Addr, kcfg.MaxFileSize, kcfg.LogLevel = dataDir, addrFlag, cfg.MaxFileSize, cfg.LogLevel

	if err := cfg.Validate(); err != nil {
		return Config{}, kvstore.Config{}, err
	}
	if err := kcfg.Validate(); err != nil {
		return Config{}, kvstore.Config{}, err
	}
	return cfg, kcfg, nil
}

// parsePeers 把成员表拆成 id→host:port。地址表只有这一处解析，两个 RPC 与客户端都从它取（3.1 §6）。
// 两项共用一个地址直接报错：回执里带的是监听地址，反查成员 id 就此歧义。
func parsePeers(s string) (map[string]string, error) {
	if s == "" {
		return nil, fmt.Errorf("peers must not be empty")
	}
	peers := map[string]string{}
	owner := map[string]string{}
	for _, item := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(item, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("peer item %q, want the form id=host:port", item)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("peer id %q appears twice", id)
		}
		if other, taken := owner[addr]; taken {
			return nil, fmt.Errorf("peers %s and %s share the address %q", other, id, addr)
		}
		peers[id] = addr
		owner[addr] = id
	}
	return peers, nil
}

// NodeLogger 装节点日志文件与它背后的 logger：kv 与 raft 两个 component 写同一个文件（3.1 §6 末段）。
type NodeLogger struct {
	*zap.Logger
	file *os.File
}

// NewNodeLogger 开 `data/node-<id>/log/miniraft-node.log`。
// JSON 编码与 2.1 §8 同款：小写 level、固定 UTC 的 ts、带 caller；kvstore 的那份构造器绑死了它自己的文件名。
func NewNodeLogger(cfg Config) (*NodeLogger, error) {
	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.LogDir(), 0o755); err != nil {
		return nil, kvstore.ErrIOFrom("create node log dir", 0, err)
	}
	f, err := os.OpenFile(cfg.NodeLogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, kvstore.ErrIOFrom("open node log file", 0, err)
	}
	enc := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		MessageKey:     "msg",
		CallerKey:      "caller",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     encodeUTC,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	return &NodeLogger{
		Logger: zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(enc), zapcore.AddSync(f), level), zap.AddCaller()),
		file:   f,
	}, nil
}

func (l *NodeLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return kvstore.ErrIOFrom("close node log file", 0, err)
}

// ts 固定 UTC：本地时间会让五个节点的日志对不上次序（2.1 §8）。
func encodeUTC(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format(time.RFC3339Nano))
}
