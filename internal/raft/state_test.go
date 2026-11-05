package raft

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// validConfig 是一份能通过 Validate 的最小成员表：两个节点、时钟取默认值。
// Validate 从 M10 起要求 NodeID 与 Peers，所以拒绝类用例必须从这份合法的配置出发改一处，
// 否则报的是缺成员而不是被测的那一项，用例就废了。
func validConfig() Config {
	cfg := DefaultConfig()
	cfg.DataDir = "data/node-1"
	cfg.NodeID = "node-1"
	cfg.Peers = map[string]string{
		"node-1": "127.0.0.1:8801",
		"node-2": "127.0.0.1:8802",
	}
	return cfg
}

func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := validConfig()
	cfg.DataDir = t.TempDir()
	if err := cfg.ensureDirs(); err != nil {
		t.Fatalf("prepare dirs: %v", err)
	}
	return cfg
}

func newTestState(t *testing.T) (*stateFile, Config) {
	t.Helper()
	cfg := testConfig(t)
	return newStateFile(cfg, zap.NewNop()), cfg
}

// 四标量的字段组合写读一致，盘上文本与 3.1 §5 声明的形状逐字相同。
func TestStateRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		st   State
	}{
		{"all zero", State{}},
		{"documented shape", State{CurrentTerm: 3, VotedFor: "node-2", CommitIndex: 8, LastApplied: 8}},
		{"empty votedFor", State{CurrentTerm: 7, VotedFor: "", CommitIndex: 0, LastApplied: 0}},
		{"self vote and lagging applied", State{CurrentTerm: 1, VotedFor: "node-1", CommitIndex: 40, LastApplied: 39}},
		{"max uint64", State{CurrentTerm: 1, VotedFor: "n", CommitIndex: ^uint64(0), LastApplied: ^uint64(0) - 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, cfg := newTestState(t)
			if err := s.store(c.st); err != nil {
				t.Fatalf("store: %v", err)
			}

			// 换一个句柄读，读到的必须只是盘上的内容，跟写它的那个实例没有私道。
			got, err := newStateFile(cfg, zap.NewNop()).load()
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got != c.st {
				t.Errorf("round trip: stored %+v, got %+v", c.st, got)
			}
		})
	}
}

func TestStateFileContentMatchesDocumentedShape(t *testing.T) {
	s, cfg := newTestState(t)
	st := State{CurrentTerm: 3, VotedFor: "node-2", CommitIndex: 8, LastApplied: 8}
	if err := s.store(st); err != nil {
		t.Fatalf("store: %v", err)
	}
	data, err := os.ReadFile(cfg.StatePath())
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	want := `{"currentTerm":3,"votedFor":"node-2","commitIndex":8,"lastApplied":8}` + "\n"
	if string(data) != want {
		t.Errorf("state file = %q, want %q", string(data), want)
	}
}

// 文件不存在是首次启动：按零值起，不算错误（3.1 §7）。
func TestStateLoadWithoutFile(t *testing.T) {
	s, _ := newTestState(t)
	got, err := s.load()
	if err != nil {
		t.Fatalf("load on a fresh node: %v", err)
	}
	if got != (State{}) {
		t.Errorf("fresh state = %+v, want all zero", got)
	}
}

// 崩溃留下的 state.tmp 不是提交点：load 忽略它并清掉，正式文件的值照常读回。
// 不清的话下一次 store 的 O_EXCL 会被这个残留名挡住，节点再也写不进 state。
func TestStateTmpResidueIsIgnoredAndRemoved(t *testing.T) {
	s, cfg := newTestState(t)
	st := State{CurrentTerm: 5, VotedFor: "node-3", CommitIndex: 12, LastApplied: 12}
	if err := s.store(st); err != nil {
		t.Fatalf("store: %v", err)
	}

	tmp := []byte(`{"currentTerm":99,"votedFor":"node-9","commitIndex":99,"lastApplied":99}`)
	if err := os.WriteFile(cfg.StateTmpPath(), tmp, 0o644); err != nil {
		t.Fatalf("plant tmp: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != st {
		t.Errorf("load read the residue: got %+v, want %+v", got, st)
	}
	if _, err := os.Stat(cfg.StateTmpPath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("state.tmp survived load: stat err = %v", err)
	}
	if err := s.store(State{CurrentTerm: 6}); err != nil {
		t.Errorf("store after residue cleanup: %v", err)
	}
}

// 正式文件坏到读不出完整四个标量时一律 ErrIO，且返回零值 State 但不报 nil：
// 把半行 JSON 当成「term 0 且没投过票」就是 D14 要挡的那件事。
func TestStateRejectsPartialFile(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"empty file", ""},
		{"only a newline", "\n"},
		{"half a line", `{"currentTerm":3,"votedFor":"node-2","comm`},
		{"missing lastApplied", `{"currentTerm":3,"votedFor":"node-2","commitIndex":8}`},
		{"missing votedFor", `{"currentTerm":3,"commitIndex":8,"lastApplied":8}`},
		{"unknown field name", `{"currentTerm":3,"votedFor":"node-2","commitIndex":8,"lastApplied ":8,"spare":1}`},
		{"wrong type", `{"currentTerm":"three","votedFor":"node-2","commitIndex":8,"lastApplied":8}`},
		{"negative term", `{"currentTerm":-1,"votedFor":"node-2","commitIndex":8,"lastApplied":8}`},
		{"not an object", `[1,2,3]`},
		{"two objects", `{"currentTerm":1,"votedFor":"","commitIndex":0,"lastApplied":0} {"currentTerm":2,"votedFor":"","commitIndex":0,"lastApplied":0}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, cfg := newTestState(t)
			if err := os.WriteFile(cfg.StatePath(), []byte(c.raw), 0o644); err != nil {
				t.Fatalf("plant file: %v", err)
			}
			got, err := s.load()
			if !errors.Is(err, kvstore.ErrIO) {
				t.Fatalf("load error = %v, want ErrIO", err)
			}
			if got != (State{}) {
				t.Errorf("broken file read as %+v, want the caller to get zero values with an error", got)
			}
		})
	}
}

// Sync 失败时 store 必须失败，而且临时名绝不改成正式名。
// 这条是 3.2 §6 第一条变异「回 voteGranted=true 之前不 Sync」的杀手：
// Sync 一旦从落盘三步里去掉，store 会直接改名并把新值当成已落盘返回，两处断言同时红。
// 造错手法与 2.2 的 fsync 用例同源：真临时文件照常创建，句柄换成管道写端，
// 于是 Write 成功、Sync 一定 EINVAL、rename 也不会因为路径不存在而先失败。
func TestStateStoreFailsWhenSyncFails(t *testing.T) {
	s, cfg := newTestState(t)
	good := State{CurrentTerm: 4, VotedFor: "node-2", CommitIndex: 9, LastApplied: 9}
	if err := s.store(good); err != nil {
		t.Fatalf("store: %v", err)
	}

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() { _ = readEnd.Close() })
	go func() {
		buf := make([]byte, 4096)
		for {
			if n, err := readEnd.Read(buf); n <= 0 || err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = writeEnd.Close() })

	s.openTmp = func(path string) (*os.File, error) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
		return writeEnd, nil
	}

	if err := s.store(State{CurrentTerm: 9, VotedFor: "node-4"}); !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("store with a failing Sync = %v, want ErrIO", err)
	}
	again, err := newStateFile(cfg, zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if again != good {
		t.Errorf("state committed as %+v, want the unchanged %+v", again, good)
	}
	if _, err := os.Stat(cfg.StateTmpPath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("state.tmp survived a failed store: stat err = %v", err)
	}
}

// 写路径在改名之前就断掉时，正式文件一个字节都不动（D5 的「盘上边界已知」口径）。
func TestStateStoreFailureLeavesCommittedFileUntouched(t *testing.T) {
	s, cfg := newTestState(t)
	good := State{CurrentTerm: 4, VotedFor: "node-2", CommitIndex: 9, LastApplied: 9}
	if err := s.store(good); err != nil {
		t.Fatalf("store: %v", err)
	}

	// 临时名位置放一个同名目录：O_CREATE|O_EXCL 打不开，store 只能失败返回。
	if err := os.Mkdir(cfg.StateTmpPath(), 0o755); err != nil {
		t.Fatalf("block tmp path: %v", err)
	}
	if err := s.store(State{CurrentTerm: 5, VotedFor: "node-3"}); !errors.Is(err, kvstore.ErrIO) {
		t.Fatalf("store against a blocked tmp = %v, want ErrIO", err)
	}
	got, err := newStateFile(cfg, zap.NewNop()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != good {
		t.Errorf("committed state = %+v, want the unchanged %+v", got, good)
	}
}

// 落 state 的日志事件与字段按 3.1 §10 的表来。
func TestStatePersistedLogFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := testConfig(t)
	s := newStateFile(cfg, zap.New(core))
	st := State{CurrentTerm: 2, VotedFor: "node-1", CommitIndex: 1, LastApplied: 1}
	if err := s.store(st); err != nil {
		t.Fatalf("store: %v", err)
	}

	matches := logs.FilterMessage("raft state persisted").All()
	if len(matches) != 1 {
		t.Fatalf("logged %d entries named raft state persisted, want 1", len(matches))
	}
	fields := matches[0].ContextMap()
	for _, key := range []string{"current_term", "voted_for", "commit_index", "last_applied", "sync_ms"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("log field %s missing in %v", key, fields)
		}
	}
	if fields["voted_for"] != "node-1" {
		t.Errorf("voted_for = %v, want node-1", fields["voted_for"])
	}
	if fields["current_term"] != uint64(2) {
		t.Errorf("current_term = %v, want 2", fields["current_term"])
	}
}

// 结构体的 JSON tag 与 stateFields 必须指向同一批字段名，改一处就会让 load 判坏。
func TestStateEncoderEmitsEveryField(t *testing.T) {
	data, err := json.Marshal(State{CurrentTerm: 1, VotedFor: "node-1", CommitIndex: 2, LastApplied: 2})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range stateFields {
		if !strings.Contains(string(data), `"`+key+`"`) {
			t.Errorf("encoded state %s lacks field %q", data, key)
		}
	}
}

func TestConfigRaftPaths(t *testing.T) {
	cfg := validConfig()
	for _, c := range []struct{ got, want string }{
		{cfg.RaftDir(), filepath.Join("data/node-1", "raft")},
		{cfg.StatePath(), filepath.Join("data/node-1", "raft", "state")},
		{cfg.StateTmpPath(), filepath.Join("data/node-1", "raft", "state.tmp")},
		{cfg.RaftLogDir(), filepath.Join("data/node-1", "raft", "log")},
		{cfg.RaftLogPath(1), filepath.Join("data/node-1", "raft", "log", "00000001.rlog")},
		{cfg.RaftLogTmpPath(12), filepath.Join("data/node-1", "raft", "log", "00000012.rlog.tmp")},
	} {
		if c.got != filepath.Clean(c.want) {
			t.Errorf("path = %q, want %q", c.got, c.want)
		}
	}
}

func TestConfigValidateRejects(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"empty data dir":        func(c *Config) { c.DataDir = "" },
		"zero max file size":    func(c *Config) { c.MaxFileSize = 0 },
		"negative max size":     func(c *Config) { c.MaxFileSize = -1 },
		"bad log level":         func(c *Config) { c.LogLevel = "trace2" },
		"empty peers":           func(c *Config) { c.Peers = nil },
		"self not a peer":       func(c *Config) { c.NodeID = "node-9" },
		"empty node id":         func(c *Config) { c.NodeID = "" },
		"zero tick":             func(c *Config) { c.TickMS = 0 },
		"zero election timeout": func(c *Config) { c.ElectionTimeoutMS = 0 },
		"heartbeat not shorter": func(c *Config) { c.HeartbeatMS = 500; c.ElectionTimeoutMS = 500 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mut(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted %+v", cfg)
			}
		})
	}

	// 合法的默认组合必须过校验，否则上面十条全是靠缺成员误判。
	t.Run("documented defaults pass", func(t *testing.T) {
		if err := validConfig().Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
}

// Load 是 miniraft-node 唯一的 flag 层（3.1 §6）：两个缺省值由成员表推，
// 三处冲突一律报错，`-raft-fsync` 与 `-batch-size` 各落到自己的 Config。
func TestConfigLoadFlags(t *testing.T) {
	peers := "node-1=127.0.0.1:8801,node-2=127.0.0.1:8802"

	t.Run("both defaults come from the peer table", func(t *testing.T) {
		cfg, kcfg, err := Load([]string{"-node-id", "node-1", "-peers", peers})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.DataDir != filepath.Join(kvstore.DefaultDataDir, "node-1") {
			t.Errorf("-data-dir default = %q, want data/node-<id>", cfg.DataDir)
		}
		if kcfg.DataDir != cfg.DataDir {
			t.Errorf("kv data dir = %q, want the same directory as raft", kcfg.DataDir)
		}
		if kcfg.Addr != "127.0.0.1:8801" {
			t.Errorf("-addr default = %q, want this id's entry in -peers", kcfg.Addr)
		}
		if cfg.Peers["node-2"] != "127.0.0.1:8802" || len(cfg.Peers) != 2 {
			t.Errorf("peer table = %v, want the two entries as written", cfg.Peers)
		}
		if cfg.ElectionTimeoutMS != 500 || cfg.HeartbeatMS != 100 || cfg.TickMS != 50 {
			t.Errorf("clocks = %d/%d/%d, want 500/100/50",
				cfg.ElectionTimeoutMS, cfg.HeartbeatMS, cfg.TickMS)
		}
		if got := cfg.rpcTimeout(); got != 100*time.Millisecond {
			t.Errorf("rpc timeout = %v, want min(2×tick, T) = 100ms", got)
		}
		if !cfg.Fsync || cfg.MaxFileSize != kvstore.DefaultMaxFileSize {
			t.Errorf("Fsync/MaxFileSize = %v/%d, want true and the shared segment limit",
				cfg.Fsync, cfg.MaxFileSize)
		}
	})

	for name, args := range map[string][]string{
		"addr disagrees with the peer table": {"-node-id", "node-1", "-peers", peers, "-addr", "127.0.0.1:9999"},
		"peer id appears twice":              {"-node-id", "node-1", "-peers", "node-1=127.0.0.1:8801,node-1=127.0.0.1:8802"},
		"two peers share an address":         {"-node-id", "node-1", "-peers", "node-1=127.0.0.1:8801,node-2=127.0.0.1:8801"},
		"peer item without an equals sign":   {"-node-id", "node-1", "-peers", "node-1"},
		"peer item with an empty address":    {"-node-id", "node-1", "-peers", "node-1=,node-2=127.0.0.1:8802"},
		"heartbeat not shorter than T":       {"-node-id", "node-1", "-peers", peers, "-heartbeat-ms", "500"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Load(args); err == nil {
				t.Fatalf("Load(%q) accepted the bad command line", strings.Join(args, " "))
			}
		})
	}

	// -raft-fsync 只管 raft 日志：KV 的 WAL 仍按批次 Sync，M13 的吞吐对照隔离的就是这一层（D19）。
	t.Run("the two switches land on their own config", func(t *testing.T) {
		cfg, kcfg, err := Load([]string{"-node-id", "node-2", "-peers", peers,
			"-raft-fsync=false", "-batch-size", "7"})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Fsync {
			t.Error("raft Fsync = true, want -raft-fsync=false to reach it")
		}
		if !kcfg.Fsync {
			t.Error("kv Fsync = false, want -raft-fsync to leave the WAL batching alone")
		}
		if kcfg.BatchSize != 7 {
			t.Errorf("kv BatchSize = %d, want 7", kcfg.BatchSize)
		}
		if kcfg.Addr != "127.0.0.1:8802" {
			t.Errorf("-addr default = %q, want node-2's entry", kcfg.Addr)
		}
	})
}

// 读路径三个开关与三条新增校验（4.1 §6、4.2 §3 的 state_test.go 行）：
// 取值非法、负漂移、非正读超时各自报错；租约档的窗口要长过一轮广播，
// 确认档的读超时也要长过一轮广播。
func TestConfigLoadReadSwitches(t *testing.T) {
	five := "node-1=127.0.0.1:8801,node-2=127.0.0.1:8802,node-3=127.0.0.1:8803," +
		"node-4=127.0.0.1:8804,node-5=127.0.0.1:8805"

	t.Run("defaults land on the config", func(t *testing.T) {
		cfg, _, err := Load([]string{"-node-id", "node-1", "-peers", five})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.ReadMode != ReadModeLease || cfg.ClockDriftMS != 50 || cfg.ReadTimeoutMS != 1000 {
			t.Errorf("read switches = %s/%d/%d, want lease/50/1000",
				cfg.ReadMode, cfg.ClockDriftMS, cfg.ReadTimeoutMS)
		}
	})

	for name, args := range map[string][]string{
		"read mode outside the two values": {"-node-id", "node-1", "-peers", five, "-read-mode", "bogus"},
		"read mode empty":                  {"-node-id", "node-1", "-peers", five, "-read-mode", ""},
		"negative clock drift":             {"-node-id", "node-1", "-peers", five, "-clock-drift-ms", "-1"},
		"zero read timeout":                {"-node-id", "node-1", "-peers", five, "-read-timeout-ms", "0"},
		"negative read timeout":            {"-node-id", "node-1", "-peers", five, "-read-timeout-ms", "-5"},
		// 5 节点、RPC 超时 100ms：一轮广播最坏 400ms；T=400 时租约窗口只剩 350ms，不够。
		"lease window shorter than a broadcast": {"-node-id", "node-1", "-peers", five,
			"-election-timeout-ms", "400", "-read-mode", "lease"},
		"read timeout shorter than a broadcast": {"-node-id", "node-1", "-peers", five,
			"-read-mode", "readindex", "-read-timeout-ms", "300"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Load(args); err == nil {
				t.Fatalf("Load(%q) accepted the bad command line", strings.Join(args, " "))
			}
		})
	}

	// 同一份配置在确认档通过：租约窗口那条只约束 lease 档。
	t.Run("the same clocks pass in the readindex mode", func(t *testing.T) {
		if _, _, err := Load([]string{"-node-id", "node-1", "-peers", five,
			"-election-timeout-ms", "400", "-read-mode", "readindex"}); err != nil {
			t.Fatalf("Load: %v", err)
		}
	})
}
