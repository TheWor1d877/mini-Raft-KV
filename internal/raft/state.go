package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// State 装四个共识标量，一个文件同批提交（D14）。空串 VotedFor 表示本任期未投票。
type State struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
}

// stateFile 是 raft/state 的读写句柄，落盘三步走临时名 + Sync + rename（3.1 §5）。
type stateFile struct {
	cfg Config
	lg  *zap.Logger

	// openTmp 造出临时名的句柄。默认按 O_CREATE|O_EXCL 开文件；
	// 测试把它换成管道写端，单独把 Sync 失败隔离出来（手法同 2.2 的 fsync 用例）。
	openTmp func(path string) (*os.File, error)
}

// defaultOpenTmp 用临时名开一个新文件；O_EXCL 保证不会续写上一个崩溃留下的半成品。
func defaultOpenTmp(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
}

var stateFields = []string{"currentTerm", "votedFor", "commitIndex", "lastApplied"}

func newStateFile(cfg Config, lg *zap.Logger) *stateFile {
	return &stateFile{cfg: cfg, lg: kvstore.WithComponent(lg, ComponentRaft), openTmp: defaultOpenTmp}
}

// load 读回四个标量。文件不存在是首次启动，按零值起；空文件、缺字段、半行 JSON 都返回 ErrIO，
// 绝不当成零值继续（3.1 §7）。
func (s *stateFile) load() (State, error) {
	// 崩溃留下的临时名先清掉：它不是提交点，留在原地还会让下一次 store 的 O_EXCL 失败。
	if err := s.removeTmp(); err != nil {
		return State{}, err
	}
	data, err := os.ReadFile(s.cfg.StatePath())
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, kvstore.ErrIOFrom("read raft state", 0, err)
	}
	return decodeState(data)
}

// store 整体重写：state.tmp 写入 → Sync → rename → Sync 父目录。
func (s *stateFile) store(st State) error {
	data, err := json.Marshal(st)
	if err != nil {
		return kvstore.ErrIOFrom("encode raft state", 0, err)
	}
	data = append(data, '\n')

	tmp := s.cfg.StateTmpPath()
	f, err := s.openTmp(tmp)
	if err != nil {
		return kvstore.ErrIOFrom("create state tmp", 0, err)
	}
	if _, err := f.Write(data); err != nil {
		s.discard(f, tmp)
		return kvstore.ErrIOFrom("write state tmp", 0, err)
	}
	start := time.Now()
	if err := f.Sync(); err != nil {
		s.discard(f, tmp)
		return kvstore.ErrIOFrom("fsync state tmp", 0, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("close state tmp", 0, err)
	}
	if err := os.Rename(tmp, s.cfg.StatePath()); err != nil {
		_ = os.Remove(tmp)
		return kvstore.ErrIOFrom("rename state file", 0, err)
	}
	// rename 就是这四个标量的提交点，父目录不 Sync 的话掉电能把改名一起退掉：
	// 节点重启后带着新 term 却留着旧投票记录，不变式 1 靠运行时判定挡不住。
	if err := syncDir(s.cfg.RaftDir()); err != nil {
		return err
	}
	s.lg.Debug("raft state persisted",
		zap.Uint64("current_term", st.CurrentTerm),
		zap.String("voted_for", st.VotedFor),
		zap.Uint64("commit_index", st.CommitIndex),
		zap.Uint64("last_applied", st.LastApplied),
		zap.Int64("sync_ms", time.Since(start).Milliseconds()))
	return nil
}

// discard 在写临时名失败时关掉句柄并删掉半成品，正式名从头到尾没被碰过。
func (s *stateFile) discard(f *os.File, tmp string) {
	_ = f.Close()
	_ = os.Remove(tmp)
}

func (s *stateFile) removeTmp() error {
	if err := os.Remove(s.cfg.StateTmpPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return kvstore.ErrIOFrom("remove uncommitted state file", 0, err)
	}
	return nil
}

// decodeState 先按 key 查四个字段齐不齐，再解进结构体。
// json.Unmarshal 遇到缺字段直接留零值，而 term 0 加空 votedFor 的意思正好是
// 「本任期没投过票」：一个被截断的 state 静默解成零值就把票收回了，缺字段必须是错误。
// 出错时返回零值 State：部分解码会把错误之前的字段填进来，
// 那正好是 term 新、票旧的中间态，调用方拿到手不能用，索性一个都别给。
func decodeState(data []byte) (State, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return State{}, badState("state file is empty")
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return State{}, badState(err.Error())
	}
	for _, want := range stateFields {
		if _, ok := fields[want]; !ok {
			return State{}, badState("missing field " + want)
		}
	}
	for name := range fields {
		if !slices.Contains(stateFields, name) {
			return State{}, badState("unknown field " + name)
		}
	}

	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, badState(err.Error())
	}
	return st, nil
}

// state 文件读坏了不能当成零值继续，所以归 ErrIO 而不是 ErrCorruptRecord：
// 这条路径上没有可裁剪的字节流，只有一个要么完整要么不可信的小文件。
func badState(reason string) error {
	return kvstore.ErrIOFrom("decode raft state", 0, errors.New(reason))
}

// syncDir 落目录项。os.File 的 Sync 对目录同样有效，Linux 上就是 fdatasync(dirfd)。
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return kvstore.ErrIOFrom("open raft dir", 0, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return kvstore.ErrIOFrom("fsync raft dir", 0, err)
	}
	if err := f.Close(); err != nil {
		return kvstore.ErrIOFrom("close raft dir", 0, err)
	}
	return nil
}
