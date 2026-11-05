package kvstore

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// component 取值集合见 2.1 §8。合并之后 WAL 是唯一的日志部件：
// 旧集合里的 recovery 与 raft 侧的 raftlog 都收进 wal，一个部件一个值。
const (
	ComponentWAL   = "wal"
	ComponentKV    = "kv"
	ComponentEntry = "entry"
)

type Logger struct {
	*zap.Logger
	file *os.File
}

func NewLogger(cfg Config) (*Logger, error) {
	level, err := parseLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.LogDir(), 0o755); err != nil {
		return nil, ErrIOFrom("create log dir", 0, err)
	}
	f, err := os.OpenFile(cfg.LogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, ErrIOFrom("open log file", 0, err)
	}
	return &Logger{Logger: newLoggerTo(f, level), file: f}, nil
}

func (l *Logger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return ErrIOFrom("close log file", 0, err)
}

func newLoggerTo(w io.Writer, level zapcore.Level) *zap.Logger {
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig()), zapcore.AddSync(w), level)
	return zap.New(core, zap.AddCaller())
}

func encoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
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
}

// ts 固定 UTC：本地时间会让跨机器的日志对不上。
func encodeUTC(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format(time.RFC3339Nano))
}

func parseLevel(s string) (zapcore.Level, error) {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(s))); err != nil {
		return level, fmt.Errorf("log level %q: %w", s, err)
	}
	return level, nil
}

func WithComponent(l *zap.Logger, component string) *zap.Logger {
	return l.With(zap.String("component", component))
}

func KeyField(key string) zap.Field {
	if len(key) > MaxLoggedKeyBytes {
		key = key[:MaxLoggedKeyBytes]
	}
	return zap.String("key", key)
}

// value 不进日志，只记长度；指纹在编码层随 crc 一起打（2.1 §8）。
func ValueField(value string) zap.Field {
	return zap.Int("value_len", len(value))
}
