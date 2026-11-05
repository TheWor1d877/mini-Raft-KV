package kvstore

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var fieldKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func fileConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	return cfg
}

func TestLoggerCommonFields(t *testing.T) {
	cfg := fileConfig(t)

	lg, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger = %v", err)
	}
	WithComponent(lg.Logger, ComponentWAL).Info("wal rolled over",
		zap.String("from_file", "00000001.wal"),
		zap.String("to_file", "00000002.wal"),
		zap.Int64("size_bytes", 64))
	if err := lg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	data, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal(firstLine(t, data), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}

	for _, key := range []string{"ts", "level", "msg", "component"} {
		if _, ok := entry[key]; !ok {
			t.Errorf("missing common field %q in %v", key, entry)
		}
	}
	for key := range entry {
		if !fieldKeyPattern.MatchString(key) {
			t.Errorf("field name %q is not lowercase snake_case", key)
		}
	}
	if entry["component"] != ComponentWAL || entry["level"] != "info" || entry["msg"] != "wal rolled over" {
		t.Errorf("entry = %v", entry)
	}
	if entry["size_bytes"] != float64(64) {
		t.Errorf("size_bytes = %v", entry["size_bytes"])
	}
}

func TestLoggerMasksValue(t *testing.T) {
	cfg := fileConfig(t)
	secret := "the-whole-value-must-not-land-in-logs"

	lg, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger = %v", err)
	}
	WithComponent(lg.Logger, ComponentKV).Info("put", KeyField(secret), ValueField(secret))
	if err := lg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	data, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	line := string(firstLine(t, data))
	if len(secret) <= MaxLoggedKeyBytes {
		t.Fatal("test key must be longer than the masking limit")
	}
	if strings.Contains(line, secret) {
		t.Errorf("value leaked into log line: %s", line)
	}
	if !strings.Contains(line, "value_len") {
		t.Errorf("log line has no value_len: %s", line)
	}
	var entry map[string]any
	if err := json.Unmarshal(firstLine(t, data), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	if got, ok := entry["key"].(string); !ok || len(got) != MaxLoggedKeyBytes {
		t.Errorf("key = %v, want first %d bytes", entry["key"], MaxLoggedKeyBytes)
	}
}

func TestTimestampIsUTC(t *testing.T) {
	cfg := fileConfig(t)

	lg, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger = %v", err)
	}
	lg.Info("ts")
	if err := lg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	data, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal(firstLine(t, data), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	ts, _ := entry["ts"].(string)
	if !strings.HasSuffix(ts, "Z") {
		t.Fatalf("ts = %q, want UTC with Z suffix", ts)
	}
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("ts %q is not RFC3339: %v", ts, err)
	}
	if parsed.Location() != time.UTC {
		t.Errorf("ts %q is not UTC", ts)
	}
}

func TestLoggerRespectsLevel(t *testing.T) {
	cfg := fileConfig(t)
	cfg.LogLevel = "error"

	lg, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger = %v", err)
	}
	lg.Info("dropped")
	lg.Error("kept")
	if err := lg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	data, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if strings.Contains(string(data), "dropped") {
		t.Errorf("info line written at error level: %s", data)
	}
	if !strings.Contains(string(data), "kept") {
		t.Errorf("error line missing: %s", data)
	}
}

func TestNewLoggerRejectsBadLevel(t *testing.T) {
	cfg := fileConfig(t)
	cfg.LogLevel = "verbose"

	if _, err := NewLogger(cfg); err == nil {
		t.Fatal("NewLogger accepted an unknown level")
	}
}

func TestWithComponentViaObserver(t *testing.T) {
	core, records := observer.New(zapcore.DebugLevel)
	WithComponent(zap.New(core), ComponentWAL).Warn("corrupt record tail", zap.Int64("offset", 12345))

	all := records.All()
	if len(all) != 1 {
		t.Fatalf("records = %d, want 1", len(all))
	}
	if all[0].Level != zapcore.WarnLevel {
		t.Errorf("level = %v, want warn", all[0].Level)
	}
	fields := all[0].ContextMap()
	if fields["component"] != ComponentWAL || fields["offset"] != int64(12345) {
		t.Errorf("fields = %v", fields)
	}
}

func TestParseLevel(t *testing.T) {
	for _, s := range []string{"debug", "info", "warn", "error", "panic", "fatal", "DEBUG"} {
		if _, err := parseLevel(s); err != nil {
			t.Errorf("parseLevel(%q) = %v", s, err)
		}
	}
	if _, err := parseLevel("trace"); err == nil {
		t.Error("parseLevel accepted trace")
	}
}

func TestLoggerCloseReleasesFile(t *testing.T) {
	cfg := fileConfig(t)

	lg, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger = %v", err)
	}
	if err := lg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := lg.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if _, err := NewLogger(cfg); err != nil {
		t.Fatalf("reopen after Close = %v", err)
	}
}

func firstLine(t *testing.T, data []byte) []byte {
	t.Helper()
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			return data[:i]
		}
	}
	if len(data) == 0 {
		t.Fatal("log file is empty")
	}
	return data
}
