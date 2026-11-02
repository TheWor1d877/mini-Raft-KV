package kvstore

import (
	"testing"
)

// 缺省值必须等于 2.1 §6 的目录布局与 §8 的批次、级别。
func TestDefaultConfigMatchesSpec(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.DataDir != "data" {
		t.Errorf("DataDir = %q, want data", cfg.DataDir)
	}
	if cfg.Addr != "127.0.0.1:8742" {
		t.Errorf("Addr = %q, want 127.0.0.1:8742", cfg.Addr)
	}
	if cfg.BatchSize != 64 {
		t.Errorf("BatchSize = %d, want 64", cfg.BatchSize)
	}
	if cfg.MaxFileSize != 100<<20 {
		t.Errorf("MaxFileSize = %d, want 100MB", cfg.MaxFileSize)
	}
	if cfg.Fsync != true {
		t.Error("Fsync must default to true")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}

	paths := map[string][2]string{
		"lock": {cfg.LockPath(), "data/LOCK"},
		"wal":  {cfg.WALDir(), "data/wal"},
		"log":  {cfg.LogDir(), "data/log"},
		"文件":   {cfg.LogPath(), "data/log/miniraft-kv.log"},
		"序号1":  {cfg.WALPath(1), "data/wal/00000001.wal"},
		"临时":   {cfg.WALTmpPath(1), "data/wal/00000001.wal.tmp"},
	}
	for name, p := range paths {
		if p[0] != p[1] {
			t.Errorf("%s path = %q, want %q", name, p[0], p[1])
		}
	}
	if MaxEntrySize != 4<<20 || MaxKeySize != 1<<10 || MaxValueSize != 1<<20 {
		t.Errorf("limits = entry %d key %d value %d, want 4MB/1KB/1MB", MaxEntrySize, MaxKeySize, MaxValueSize)
	}
}

// 默认值只有 config.go 一处来源：常量、DefaultConfig、Load 缺省三者必须相等。
func TestDefaultsComeFromOnePlace(t *testing.T) {
	fromLoad, err := Load(nil)
	if err != nil {
		t.Fatalf("Load(nil) = %v", err)
	}
	if fromLoad != DefaultConfig() {
		t.Errorf("Load(nil) = %+v, want %+v", fromLoad, DefaultConfig())
	}
	if DefaultConfig().BatchSize != DefaultBatchSize || DefaultConfig().MaxFileSize != DefaultMaxFileSize {
		t.Error("DefaultConfig must read the exported default constants")
	}
}

func TestLoadOverridesFlags(t *testing.T) {
	cfg, err := Load([]string{
		"-data-dir", "/tmp/mini-raft",
		"-addr", "127.0.0.1:19999",
		"-batch-size", "1",
		"-max-file-size", "4096",
		"-fsync=false",
		"-log-level", "debug",
	})
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if cfg.DataDir != "/tmp/mini-raft" || cfg.Addr != "127.0.0.1:19999" ||
		cfg.BatchSize != 1 || cfg.MaxFileSize != 4096 || cfg.Fsync != false || cfg.LogLevel != "debug" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.WALDir() != "/tmp/mini-raft/wal" {
		t.Errorf("WALDir = %q, want /tmp/mini-raft/wal", cfg.WALDir())
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := map[string][]string{
		"负批次":    {"-batch-size", "-1"},
		"零批次":    {"-batch-size", "0"},
		"空目录":    {"-data-dir", ""},
		"空监听地址":  {"-addr", ""},
		"非正文件大小": {"-max-file-size", "0"},
		"未知日志级别": {"-log-level", "verbose"},
		"未识别参数":  {"-nope"},
	}
	for name, args := range cases {
		cfg, err := Load(args)
		if err == nil {
			t.Errorf("%s: Load(%v) = %+v, want error", name, args, cfg)
			continue
		}
		if cfg != (Config{}) {
			t.Errorf("%s: returned a partial config %+v on error", name, cfg)
		}
	}
}

func TestValidateRejectsEmptyStruct(t *testing.T) {
	if err := (Config{}).Validate(); err == nil {
		t.Error("zero Config must not validate: DataDir is empty")
	}
}
