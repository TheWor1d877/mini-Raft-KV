package raft

import (
	"encoding/binary"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 配置条目的 command 首字节（6.1 §5）：0x01 是 KV 操作，0x11 是配置条目，
// 空字节串是选举 no-op。raft 层只在追加、回放与 apply 三处按首字节分流。
const (
	OpConfigEntry = byte(0x11)

	// 成员角色只有两个取值，由配置条目决定，命令行不给（6.1 §4、D37）。
	MemberVoter   = "voter"
	MemberLearner = "learner"

	// 一个 id 的长度上限由 2B 长度字段之外的约定给出；2B 本身能到 65535，
	// 这里按 255 卡住，防止一个手写条目把视图撑成不可读的日志。
	maxMemberIDLen  = 255
	maxMemberCount  = 0xFFFF
	configHeaderLen = 1 + 2
	configIDLenLen  = 2
)

// memberSet 是投票集合的内存视图：升序、去重、只装 voter 的 id。
// learner 由「在地址表里、不在 voters 里」推出，集合本身不记角色（6.1 §5）。
type memberSet struct {
	voters []string
}

func newMemberSet(voters []string) memberSet {
	ids := append([]string(nil), voters...)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	return memberSet{voters: ids}
}

func (m memberSet) isVoter(id string) bool {
	_, ok := slices.BinarySearch(m.voters, id)
	return ok
}

func (m memberSet) majority() int { return len(m.voters)/2 + 1 }

// others 是发给投票请求与确认轮的收件人：投票集合去掉自己。
// learner 不在这个列表里，所以它一次也收不到投票请求（6.1 §7）。
func (m memberSet) others(self string) []string {
	out := make([]string, 0, len(m.voters))
	for _, id := range m.voters {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func (m memberSet) clone() []string { return append([]string(nil), m.voters...) }

// configView 是配置视图的一次快照量：当前投票集合、最后一条配置条目的 index、
// 以及它生效之前的集合（6.1 §4）。
type configView struct {
	voters     []string
	index      uint64
	prevVoters []string
}

// pendingConfig 是一条尚未提交的配置条目：提交它要旧集合与新集合各有多数派（D38）。
// 同一时刻至多一条，提交之后清空。
type pendingConfig struct {
	old   []string
	new   []string
	index uint64
}

// encodeConfig 编出配置条目：0x11｜count 2B｜idlen 2B｜id...（6.1 §5）。
// 输入先升序排序；空集合、重复 id、超长 id 直接拒绝。
func encodeConfig(voters []string) ([]byte, error) {
	if len(voters) == 0 {
		return nil, fmt.Errorf("config entry needs at least one voter")
	}
	ids := append([]string(nil), voters...)
	slices.Sort(ids)
	if len(ids) > maxMemberCount {
		return nil, fmt.Errorf("config entry has %d voters, over the %d limit", len(ids), maxMemberCount)
	}
	buf := make([]byte, 0, configHeaderLen+len(ids)*(configIDLenLen+16))
	buf = append(buf, OpConfigEntry)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(ids)))
	for i, id := range ids {
		if id == "" {
			return nil, fmt.Errorf("config entry has an empty voter id")
		}
		if len(id) > maxMemberIDLen {
			return nil, fmt.Errorf("voter id %q is %d bytes, over the %d-byte limit", id, len(id), maxMemberIDLen)
		}
		if i > 0 && id == ids[i-1] {
			return nil, fmt.Errorf("voter id %q appears twice", id)
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(id)))
		buf = append(buf, id...)
	}
	return buf, nil
}

// decodeConfig 解一条配置条目。首字节不对、数量与长度对不上、尾部多余字节、
// 重复或乱序 id、id 不在地址表，一律按坏记录拒绝（6.1 §4 的契约表）。
func decodeConfig(cmd []byte, known map[string]string) ([]string, error) {
	if len(cmd) < configHeaderLen {
		return nil, kvstore.ErrCorruptAt(0, fmt.Sprintf("config entry is %d bytes, below the %d-byte header", len(cmd), configHeaderLen))
	}
	if cmd[0] != OpConfigEntry {
		return nil, kvstore.ErrCorruptAt(0, fmt.Sprintf("unknown config op 0x%02x", cmd[0]))
	}
	count := int(binary.BigEndian.Uint16(cmd[1:3]))
	if count == 0 {
		return nil, kvstore.ErrCorruptAt(1, "config entry has no voters")
	}
	voters := make([]string, 0, count)
	offset := configHeaderLen
	for i := 0; i < count; i++ {
		if offset+configIDLenLen > len(cmd) {
			return nil, kvstore.ErrCorruptAt(int64(offset), "config entry ends inside an id length")
		}
		size := int(binary.BigEndian.Uint16(cmd[offset : offset+configIDLenLen]))
		offset += configIDLenLen
		if size == 0 {
			return nil, kvstore.ErrCorruptAt(int64(offset), "config entry has an empty voter id")
		}
		if offset+size > len(cmd) {
			return nil, kvstore.ErrCorruptAt(int64(offset), fmt.Sprintf("voter id of %d bytes runs past the entry end", size))
		}
		id := string(cmd[offset : offset+size])
		offset += size
		if _, ok := known[id]; !ok {
			return nil, kvstore.ErrCorruptAt(int64(offset-size), fmt.Sprintf("voter id %q is not in the address table", id))
		}
		if i > 0 && id <= voters[i-1] {
			return nil, kvstore.ErrCorruptAt(int64(offset-size), fmt.Sprintf("voter id %q is duplicated or out of order", id))
		}
		voters = append(voters, id)
	}
	if offset != len(cmd) {
		return nil, kvstore.ErrCorruptAt(int64(offset), "config entry has trailing bytes")
	}
	return voters, nil
}

// isConfigEntry 判断一条 command 是不是配置条目；空字节串是 no-op，不算。
func isConfigEntry(cmd []byte) bool { return len(cmd) > 0 && cmd[0] == OpConfigEntry }

// applyConfigLocked 换掉内存视图并同步 pendingConfig，不写盘（6.1 §4）。
// 同一视图重复调用没有副作用；调用方必须持锁。
func (n *Node) applyConfigLocked(view configView) {
	n.membership = newMemberSet(view.voters)
	n.configIndex = view.index
	n.prevVoters = append([]string(nil), view.prevVoters...)
	n.syncPendingConfigLocked()
}

// syncPendingConfigLocked 让 pendingConfig 跟视图对齐：最后一条配置条目还没提交就是
// 在飞的那一条，旧集合取它生效之前的集合（6.1 §8 不变式 4、5）。
func (n *Node) syncPendingConfigLocked() {
	if n.configIndex == 0 || n.configIndex <= n.commitIndex {
		n.pendingConfig = nil
		return
	}
	n.pendingConfig = &pendingConfig{
		old:   append([]string(nil), n.prevVoters...),
		new:   n.membership.clone(),
		index: n.configIndex,
	}
}

// rebuildConfigLocked 从快照头里的集合同步重算视图：按日志顺序重放配置条目，
// 最后一条生效（6.1 §7 接收与截断、§7 启动与快照）。截断之后必须调用。
// 快照头缺失时从地址表全表起。调用方必须持锁。
func (n *Node) rebuildConfigLocked() configView {
	base := append([]string(nil), n.snapshotVoters...)
	if len(base) == 0 {
		base = n.cfg.peerIDs()
	}
	view := configView{voters: base, index: n.snapshotIndex}
	for _, e := range n.log.entries {
		if e.index <= n.snapshotIndex || !isConfigEntry(e.cmd) {
			continue
		}
		voters, err := decodeConfig(e.cmd, n.cfg.Peers)
		if err != nil {
			// 坏条目在 append 与 replay 两条路径上已经拒绝过；走到这里说明盘上数据不可信，
			// 保留上一条有效视图，不把一条坏记录当成新集合。
			continue
		}
		view.prevVoters = view.voters
		view.voters = voters
		view.index = e.index
	}
	n.applyConfigLocked(view)
	return view
}

// changeMembership 把一次角色变更写成配置条目（6.1 §7 变更七步）。
// 校验、追加、换视图、排复制都在同一段持锁内完成；被拒时日志一条不写。
func (n *Node) changeMembership(id, role string) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.changeMembershipLocked(id, role)
}

func (n *Node) changeMembershipLocked(id, role string) (uint64, uint64, error) {
	if n.role != RoleLeader {
		return 0, 0, ErrNotLeader
	}
	if role != MemberVoter && role != MemberLearner {
		return 0, 0, n.rejectConfigLocked(id, role, fmt.Sprintf("role %q must be %s or %s", role, MemberVoter, MemberLearner))
	}
	if _, ok := n.cfg.Peers[id]; !ok {
		return 0, 0, n.rejectConfigLocked(id, role, fmt.Sprintf("target id %q is not in the address table", id))
	}
	if n.membership.isVoter(id) == (role == MemberVoter) {
		return 0, 0, n.rejectConfigLocked(id, role, fmt.Sprintf("member %q is already a %s", id, role))
	}
	if n.pendingConfig != nil {
		return 0, 0, n.rejectConfigLocked(id, role, "a membership change is already in flight")
	}

	next := make([]string, 0, len(n.membership.voters)+1)
	for _, voter := range n.membership.voters {
		if role == MemberLearner && voter == id {
			continue
		}
		next = append(next, voter)
	}
	if role == MemberVoter {
		next = append(next, id)
	}
	slices.Sort(next)
	if len(next) == 0 {
		return 0, 0, n.rejectConfigLocked(id, role, "the change would leave no voters")
	}

	command, err := encodeConfig(next)
	if err != nil {
		return 0, 0, err
	}
	index, term := n.log.lastIndex()+1, n.term
	if _, err := n.log.appendEntry(term, index, command); err != nil {
		return 0, 0, err
	}
	if err := n.log.sync(); err != nil {
		// Sync 失败时盘上边界未知：视图不动、复制不排，返回 ErrIO（6.1 §4 契约表）。
		return 0, 0, err
	}

	n.applyConfigLocked(configView{
		voters:     next,
		index:      index,
		prevVoters: n.membership.clone(),
	})
	n.lg.Info("membership change proposed",
		zap.String("node_id", n.cfg.NodeID),
		zap.String("target_id", id),
		zap.String("new_role", role),
		zap.Uint64("index", index),
		zap.Uint64("term", term),
		zap.Strings("voters", n.membership.voters))

	// learner 也照常收到复制请求：按地址表而不是投票集合排（6.1 §7 第 4 步）。
	for _, peerID := range n.cfg.otherPeers() {
		n.appendOnce(peerID)
	}

	// 新视图不含自己时，排完这一轮复制再转 Follower，term 不变（D41）。
	if !n.membership.isVoter(n.cfg.NodeID) {
		n.lg.Info("leader stepped down",
			zap.String("node_id", n.cfg.NodeID),
			zap.Uint64("index", index),
			zap.Strings("voters", n.membership.voters),
			zap.String("reason", "demoted to learner"))
		_ = n.becomeFollower(n.term, "demoted to learner")
	}
	return index, term, nil
}

// rejectConfigLocked 打一条拒绝日志并返回带原因的 ErrConfigRejected；
// 四类拒绝共用一个 HTTP code，具体原因从 error 字段读（6.1 §4）。
func (n *Node) rejectConfigLocked(id, role, reason string) error {
	n.lg.Info("membership change rejected",
		zap.String("node_id", n.cfg.NodeID),
		zap.String("target_id", id),
		zap.String("new_role", role),
		zap.String("reason", reason))
	return ErrConfigRejectedFrom(reason)
}

// Membership 读当前视图：voters 与 learners 都升序，两个列表的并集是地址表。
// 返回副本，未 Start 也能读（6.1 §4）。
func (n *Node) Membership() (voters, learners []string, index, term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	voters = n.membership.clone()
	for _, id := range n.cfg.peerIDs() {
		if !n.membership.isVoter(id) {
			learners = append(learners, id)
		}
	}
	return voters, learners, n.configIndex, n.term
}

// checkAutoDemotionLocked 是自动降级的一拍（6.1 §7 自动降级五步）：
// 只看向落后超过豁免阈值的 voter，进入豁免的 tick 序号记在 exemptSince 里，
// 追平就清零；连续 demoteTicks 拍仍在豁免里就把它降成 learner。调用方必须持锁。
func (n *Node) checkAutoDemotionLocked() {
	if n.role != RoleLeader || n.demoteTicks == 0 {
		return
	}
	for _, peerID := range n.cfg.otherPeers() {
		if !n.membership.isVoter(peerID) {
			delete(n.exemptSince, peerID)
			continue
		}
		match := n.matchIndex[peerID]
		if !n.exempted(peerID, match) {
			delete(n.exemptSince, peerID)
			continue
		}
		// 已有配置条目在飞时跳过这一次，下一拍再看（6.1 §7 第 4 步）。
		if n.pendingConfig != nil {
			continue
		}
		since, ok := n.exemptSince[peerID]
		if !ok {
			n.exemptSince[peerID] = n.tickSeq
			continue
		}
		exemptTicks := n.tickSeq - since
		if exemptTicks < uint64(n.demoteTicks) {
			continue
		}
		lag := n.lagBehind(match)
		n.lg.Info("learner auto demotion triggered",
			zap.String("node_id", n.cfg.NodeID),
			zap.String("peer_id", peerID),
			zap.Uint64("lag_entries", lag),
			zap.Uint64("exempt_ticks", exemptTicks))
		delete(n.exemptSince, peerID)
		if _, _, err := n.changeMembershipLocked(peerID, MemberLearner); err != nil {
			n.lg.Warn("learner auto demotion failed",
				zap.String("node_id", n.cfg.NodeID),
				zap.String("peer_id", peerID),
				zap.Error(err))
			return
		}
		// 一拍只发一条变更，其余落后成员留到下一拍（D38）。
		return
	}
}

// clearCommittedConfigLocked 在提交点越过配置条目之后清掉 pendingConfig，
// 并打 membership change committed（6.1 §7 第 6 步）。调用方必须持锁。
func (n *Node) clearCommittedConfigLocked() {
	pending := n.pendingConfig
	if pending == nil || n.commitIndex < pending.index {
		return
	}
	n.lg.Info("membership change committed",
		zap.String("node_id", n.cfg.NodeID),
		zap.Uint64("index", pending.index),
		zap.Uint64("term", n.term),
		zap.Strings("old_voters", pending.old),
		zap.Strings("new_voters", pending.new))
	n.pendingConfig = nil
}
