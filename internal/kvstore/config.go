package kvstore

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
)

// 默认值、上限、命名规则只有这一处；wal、dir、recovery 都从这里取。
const (
	DefaultDataDir     = "data"
	DefaultAddr        = "127.0.0.1:8742"
	DefaultBatchSize   = 64
	DefaultMaxFileSize = int64(100 << 20)
	DefaultLogLevel    = "info"

	LockFileName = "LOCK"
	WALDirName   = "wal"
	LogDirName   = "log"
	LogFileName  = "miniraft-kv.log"

	WALFileFormat    = "%08d.wal"
	WALTmpFileFormat = "%08d.wal.tmp"

	// 日志里打印的 key 长度上限（2.1 §8 脱敏）。
	MaxLoggedKeyBytes = 16

	MaxEntrySize = 4 << 20
	MaxKeySize   = 1 << 10
	MaxValueSize = 1 << 20
)

type Config struct {
	DataDir     string
	Addr        string
	BatchSize   int
	MaxFileSize int64
	Fsync       bool
	LogLevel    string

	// SkipWALRecovery 是单日志合并的中间态开关：置真时 Open 不回放 KV WAL，
	// map 交给 raft 日志的回放重建，KV WAL 照写不读。单机档改成单节点集群后整条取消。
	SkipWALRecovery bool
}

func DefaultConfig() Config {
	return Config{
		DataDir:     DefaultDataDir,
		Addr:        DefaultAddr,
		BatchSize:   DefaultBatchSize,
		MaxFileSize: DefaultMaxFileSize,
		Fsync:       true,
		LogLevel:    DefaultLogLevel,
	}
}

func (c Config) LockPath() string { return filepath.Join(c.DataDir, LockFileName) }
func (c Config) WALDir() string   { return filepath.Join(c.DataDir, WALDirName) }
func (c Config) LogDir() string   { return filepath.Join(c.DataDir, LogDirName) }
func (c Config) LogPath() string  { return filepath.Join(c.LogDir(), LogFileName) }

func (c Config) WALPath(seq uint64) string {
	return filepath.Join(c.WALDir(), fmt.Sprintf(WALFileFormat, seq))
}

func (c Config) WALTmpPath(seq uint64) string {
	return filepath.Join(c.WALDir(), fmt.Sprintf(WALTmpFileFormat, seq))
}

// Load 先取默认值，再用命令行覆盖；未识别的参数与非法值都返回错误，不静默回落。
func Load(args []string) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("miniraft-kv", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory")
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "host:port to serve the HTTP API on")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "records per fsync batch")
	fs.Int64Var(&cfg.MaxFileSize, "max-file-size", cfg.MaxFileSize, "bytes per WAL file before rollover")
	fs.BoolVar(&cfg.Fsync, "fsync", cfg.Fsync, "fsync each batch; disable only for local experiments")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn, error, panic, fatal")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data dir must not be empty")
	}
	if c.Addr == "" {
		return fmt.Errorf("listen addr must not be empty")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size %d must be positive", c.BatchSize)
	}
	if c.MaxFileSize <= 0 {
		return fmt.Errorf("max file size %d must be positive", c.MaxFileSize)
	}
	if _, err := parseLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}
