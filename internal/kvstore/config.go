package kvstore

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
)

// 默认值、上限、命名规则只有这一处。
const (
	DefaultDataDir     = "data"
	DefaultAddr        = "127.0.0.1:8742"
	DefaultMaxFileSize = int64(100 << 20)
	DefaultLogLevel    = "info"

	LockFileName = "LOCK"
	LogDirName   = "log"
	LogFileName  = "miniraft-kv.log"

	// 日志里打印的 key 长度上限（2.1 §8 脱敏）。
	MaxLoggedKeyBytes = 16

	MaxEntrySize = 4 << 20
	MaxKeySize   = 1 << 10
	MaxValueSize = 1 << 20
)

type Config struct {
	DataDir     string
	Addr        string
	MaxFileSize int64
	LogLevel    string
}

func DefaultConfig() Config {
	return Config{
		DataDir:     DefaultDataDir,
		Addr:        DefaultAddr,
		MaxFileSize: DefaultMaxFileSize,
		LogLevel:    DefaultLogLevel,
	}
}

func (c Config) LockPath() string { return filepath.Join(c.DataDir, LockFileName) }
func (c Config) LogDir() string   { return filepath.Join(c.DataDir, LogDirName) }
func (c Config) LogPath() string  { return filepath.Join(c.LogDir(), LogFileName) }

// Load 先取默认值，再用命令行覆盖；未识别的参数与非法值都返回错误，不静默回落。
func Load(args []string) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("miniraft-kv", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory")
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "host:port to serve the HTTP API on")
	fs.Int64Var(&cfg.MaxFileSize, "max-file-size", cfg.MaxFileSize, "bytes per log file before rollover")
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
	if c.MaxFileSize <= 0 {
		return fmt.Errorf("max file size %d must be positive", c.MaxFileSize)
	}
	if _, err := parseLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}
