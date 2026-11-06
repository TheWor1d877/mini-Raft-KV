package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

func mustEncodeConfig(t *testing.T, voters []string) string {
	t.Helper()
	cmd, err := encodeConfig(voters)
	if err != nil {
		t.Fatalf("encode config %v: %v", voters, err)
	}
	return string(cmd)
}

// 配置条目的字节布局逐字节核对：0x11｜count 2B｜idlen 2B｜id...，id 升序（6.1 §5）。
func TestEncodeConfigByteLayout(t *testing.T) {
	cmd, err := encodeConfig([]string{"node-3", "node-1"})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	want := []byte{OpConfigEntry, 0x00, 0x02}
	want = binary.BigEndian.AppendUint16(want, uint16(len("node-1")))
	want = append(want, "node-1"...)
	want = binary.BigEndian.AppendUint16(want, uint16(len("node-3")))
	want = append(want, "node-3"...)
	if !bytes.Equal(cmd, want) {
		t.Fatalf("encoded config = %x, want %x", cmd, want)
	}
	if cmd[0] != 0x11 {
		t.Fatalf("config op = 0x%02x, want the literal 0x11", cmd[0])
	}
}

func TestEncodeConfigRejectsBadInputs(t *testing.T) {
	long := string(bytes.Repeat([]byte("x"), maxMemberIDLen+1))
	cases := []struct {
		name   string
		voters []string
	}{
		{"empty", nil},
		{"duplicate", []string{"node-1", "node-1"}},
		{"empty id", []string{"node-1", ""}},
		{"id over the limit", []string{long}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodeConfig(tc.voters); err == nil {
				t.Fatalf("encodeConfig(%v) = nil error, want an error", tc.voters)
			}
		})
	}
}

func TestDecodeConfigRejectsCorruptRecords(t *testing.T) {
	known := map[string]string{"node-1": "a", "node-2": "b"}
	valid, err := encodeConfig([]string{"node-1", "node-2"})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	unknown, err := encodeConfig([]string{"node-1", "node-9"})
	if err != nil {
		t.Fatalf("encode unknown config: %v", err)
	}
	unsorted, err := encodeConfig([]string{"node-2", "node-1"})
	if err != nil {
		t.Fatalf("encode unsorted config: %v", err)
	}
	// encode 会排序；手工把两条 id 的字节调换，模拟一条不守升序约定的条目。
	unsorted = []byte{OpConfigEntry, 0, 2,
		0, 6, 'n', 'o', 'd', 'e', '-', '2',
		0, 6, 'n', 'o', 'd', 'e', '-', '1'}

	cases := []struct {
		name string
		cmd  []byte
	}{
		{"kv op", append([]byte{0x01}, valid[1:]...)},
		{"wal op", append([]byte{0x10}, valid[1:]...)},
		{"snapshot op", append([]byte{0x20}, valid[1:]...)},
		{"short", []byte{OpConfigEntry}},
		{"zero count", []byte{OpConfigEntry, 0, 0}},
		{"truncated id", valid[:len(valid)-1]},
		{"trailing bytes", append(append([]byte(nil), valid...), 0xff)},
		{"unknown id", unknown},
		{"out of order", unsorted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeConfig(tc.cmd, known); !errors.Is(err, kvstore.ErrCorruptRecord) {
				t.Fatalf("decodeConfig = %v, want ErrCorruptRecord", err)
			}
		})
	}

	got, err := decodeConfig(valid, known)
	if err != nil {
		t.Fatalf("decode valid: %v", err)
	}
	if !slices.Equal(got, []string{"node-1", "node-2"}) {
		t.Fatalf("decoded voters = %v, want [node-1 node-2]", got)
	}
}

func TestMemberSetMajorityAndLearners(t *testing.T) {
	for size := 1; size <= 5; size++ {
		voters := []string{"a", "b", "c", "d", "e"}[:size]
		if got, want := newMemberSet(voters).majority(), size/2+1; got != want {
			t.Fatalf("%d voters: majority = %d, want %d", size, got, want)
		}
	}
	set := newMemberSet([]string{"node-1", "node-2"})
	if set.isVoter("node-3") {
		t.Fatal("node-3 is a learner but isVoter returned true")
	}
	if got, want := set.others("node-1"), []string{"node-2"}; !slices.Equal(got, want) {
		t.Fatalf("others(node-1) = %v, want %v", got, want)
	}
}

// 配置条目追加到本地日志就生效，不等提交（6.1 §8 不变式 1）。
func TestAppendTakesEffectBeforeCommit(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	n := c.node("node-2")
	cmd := mustEncodeConfig(t, []string{"node-1", "node-2"})

	reply := n.handleAppendEntries(AppendRequest{
		Term: 1, LeaderID: "node-1", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte(cmd)}}, LeaderCommit: 0,
	})
	if !reply.Success {
		t.Fatalf("append config entry = %+v, want success", reply)
	}
	voters, learners, index, _ := n.Membership()
	if !slices.Equal(voters, []string{"node-1", "node-2"}) {
		t.Fatalf("voters = %v, want [node-1 node-2]", voters)
	}
	if !slices.Equal(learners, []string{"node-3"}) {
		t.Fatalf("learners = %v, want [node-3]", learners)
	}
	if index != 1 {
		t.Fatalf("configIndex = %d, want 1", index)
	}
	n.mu.Lock()
	commit := n.commitIndex
	n.mu.Unlock()
	if commit != 0 {
		t.Fatalf("commitIndex = %d, want 0: the view must change before the entry commits", commit)
	}
}

// 后缀被截断之后视图回到被截掉的那条之前（6.1 §8 不变式 7）。
func TestTruncateRollsTheViewBack(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	n := c.node("node-2")
	first := mustEncodeConfig(t, []string{"node-1", "node-2"})

	n.handleAppendEntries(AppendRequest{
		Term: 1, LeaderID: "node-1",
		Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte(first)}}, LeaderCommit: 0,
	})
	if voters, _, _, _ := n.Membership(); len(voters) != 2 {
		t.Fatalf("voters after the first config = %v, want two voters", voters)
	}

	// 同一位置换成全表：截断把前一条配置截掉，视图由剩余日志重算。
	full := mustEncodeConfig(t, []string{"node-1", "node-2", "node-3"})
	reply := n.handleAppendEntries(AppendRequest{
		Term: 1, LeaderID: "node-3", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte(full)}}, LeaderCommit: 0,
	})
	if !reply.Success {
		t.Fatalf("replacement append = %+v, want success", reply)
	}
	voters, learners, index, _ := n.Membership()
	if !slices.Equal(voters, []string{"node-1", "node-2", "node-3"}) {
		t.Fatalf("voters = %v, want the full table", voters)
	}
	if len(learners) != 0 {
		t.Fatalf("learners = %v, want none", learners)
	}
	if index != 1 {
		t.Fatalf("configIndex = %d, want 1", index)
	}
}

// 启动时视图等于「快照头里的集合 + 日志里的配置条目」，最后一条生效（6.1 §7 启动与快照）。
func TestMembershipRebuildsFromSnapshotThenLog(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	id := "node-1"
	// 先停节点再写 state 与快照：Close 会把内存里的游标写回 state，覆盖掉播种值。
	c.closeNode(id)
	if err := newStateFile(c.raftCfg[id], zap.NewNop()).store(State{CurrentTerm: 3, CommitIndex: 5, LastApplied: 5}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	snapper := newSnapshotter(c.raftCfg[id], zap.NewNop())
	snap, err := snapper.create(5, 3, []kvstore.Entry{{Key: "k", Value: "v"}}, []string{"node-1", "node-2"})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if err := snapper.storePointer(snap); err != nil {
		t.Fatalf("store pointer: %v", err)
	}
	c.start(id)
	n := c.node(id)

	voters, learners, index, _ := n.Membership()
	if !slices.Equal(voters, []string{"node-1", "node-2"}) || !slices.Equal(learners, []string{"node-3"}) {
		t.Fatalf("view after restore = %v / %v, want node-1,node-2 and learner node-3", voters, learners)
	}
	if index != 5 {
		t.Fatalf("configIndex = %d, want the snapshot point 5", index)
	}

	// 快照之后的配置条目覆盖文件头里的集合。
	next := mustEncodeConfig(t, []string{"node-1"})
	reply := n.handleAppendEntries(AppendRequest{
		Term: 4, LeaderID: "node-1", PrevLogIndex: 5, PrevLogTerm: 3,
		Entries: []LogEntry{{Term: 4, Index: 6, Command: []byte(next)}}, LeaderCommit: 0,
	})
	if !reply.Success {
		t.Fatalf("append after the snapshot = %+v, want success", reply)
	}
	voters, _, index, _ = n.Membership()
	if !slices.Equal(voters, []string{"node-1"}) || index != 6 {
		t.Fatalf("view after the log entry = %v at %d, want [node-1] at 6", voters, index)
	}
}

// 只有日志时有配置条目：从地址表全表起，未提交的条目照样生效（6.1 §8 不变式 1）。
func TestMembershipReplaysUncommittedConfigFromWAL(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	id := "node-2"
	first := mustEncodeConfig(t, []string{"node-1", "node-2"})
	second := mustEncodeConfig(t, []string{"node-1"})
	c.reseed(id, State{}, []seedEntry{
		{term: 1, index: 1, cmd: first},
		{term: 1, index: 2},
		{term: 1, index: 3, cmd: second},
	})

	voters, learners, index, _ := c.node(id).Membership()
	if !slices.Equal(voters, []string{"node-1"}) {
		t.Fatalf("voters = %v, want [node-1]: the last config entry wins", voters)
	}
	if !slices.Equal(learners, []string{"node-2", "node-3"}) {
		t.Fatalf("learners = %v, want [node-2 node-3]", learners)
	}
	if index != 3 {
		t.Fatalf("configIndex = %d, want 3", index)
	}
}

// 没有快照也没有配置条目时视图等于 -peers 全表（6.1 §7 启动第 1 步）。
func TestMembershipDefaultsToTheAddressTable(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	voters, learners, index, _ := c.node("node-1").Membership()
	if !slices.Equal(voters, []string{"node-1", "node-2", "node-3"}) {
		t.Fatalf("voters = %v, want the full address table", voters)
	}
	if len(learners) != 0 || index != 0 {
		t.Fatalf("learners = %v index = %d, want none at 0", learners, index)
	}
}

func TestChangeMembershipRejections(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)

	if _, _, err := leader.changeMembership("node-9", MemberLearner); !errors.Is(err, ErrConfigRejected) {
		t.Fatalf("unknown target = %v, want ErrConfigRejected", err)
	}
	if _, _, err := leader.changeMembership(leaderID, MemberVoter); !errors.Is(err, ErrConfigRejected) {
		t.Fatalf("unchanged role = %v, want ErrConfigRejected", err)
	}
	if _, _, err := leader.changeMembership(leaderID, "observer"); !errors.Is(err, ErrConfigRejected) {
		t.Fatalf("unknown role = %v, want ErrConfigRejected", err)
	}

	var target, other string
	for _, id := range c.ids {
		if id == leaderID {
			continue
		}
		if target == "" {
			target = id
		} else {
			other = id
		}
	}
	if _, _, err := leader.changeMembership(target, MemberLearner); err != nil {
		t.Fatalf("demote %s: %v", target, err)
	}
	if _, _, err := leader.changeMembership(other, MemberLearner); !errors.Is(err, ErrConfigRejected) {
		t.Fatalf("second change in flight = %v, want ErrConfigRejected", err)
	}

	follower := c.node(other)
	if _, _, err := follower.changeMembership(target, MemberVoter); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower change = %v, want ErrNotLeader", err)
	}

	solo := newCluster(t, 1, zap.NewNop())
	soloLeader := solo.waitForLeader(t, 60)
	if _, _, err := solo.node(soloLeader).changeMembership(soloLeader, MemberLearner); !errors.Is(err, ErrConfigRejected) {
		t.Fatalf("demoting the last voter = %v, want ErrConfigRejected", err)
	}
	// 被拒时日志一条不写。
	solo.node(soloLeader).mu.Lock()
	last := solo.node(soloLeader).log.lastIndex()
	solo.node(soloLeader).mu.Unlock()
	if last != 1 {
		t.Fatalf("last log index = %d, want the single election no-op at 1", last)
	}
}
