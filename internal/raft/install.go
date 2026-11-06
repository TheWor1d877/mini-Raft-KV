package raft

import (
	"hash/crc32"

	"go.uber.org/zap"
)

// InstallSnapshotReply 是接收路径的回执（5.1 §4）。
type InstallSnapshotReply struct {
	OK            bool   `json:"ok"`
	SnapshotIndex uint64 `json:"snapshot_index"`
}

// handleInstallSnapshot 走 5.1 §7 的接收路径五步：
// 校验 → 判新旧 → 整体替换状态机 → 推进游标 → 写本地快照与指针、删本地日志前缀。
// 快照字节是原文件的内容：校验、解码与落盘用的都是同一份。
func (n *Node) handleInstallSnapshot(index, term uint64, body []byte) (InstallSnapshotReply, error) {
	// 1：校验 magic、条目数与 crc32c，任一不符都算坏记录（5.1 §7 接收快照第 1 步）。
	entries, voters, err := decodeSnapshotFile(body, n.cfg.Peers)
	if err != nil {
		return InstallSnapshotReply{}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 2：请求的 index 不比本地快照点新，或者比本地已应用的位置旧，直接回成功，不改状态机。
	if index <= n.snapshotIndex || index < n.lastApplied {
		return InstallSnapshotReply{OK: true, SnapshotIndex: n.snapshotIndex}, nil
	}

	// 3：整体替换状态机。快照点之前的状态只从快照来（不变式 7）。
	n.kv.Restore(entries)

	// 4：把游标推到快照 index，commitIndex 取本地值与快照 index 的较大者。
	n.lastApplied = index
	n.commitIndex = max(n.commitIndex, index)
	if err := n.state.store(n.currentState()); err != nil {
		return InstallSnapshotReply{}, err
	}

	// 5：写本地快照文件与指针，删掉本地日志里 <= 快照 index 的条目。
	snap := &Snapshot{
		Index:    index,
		Term:     term,
		Entries:  len(entries),
		Bytes:    int64(len(body)),
		Checksum: crc32.Checksum(body, snapshotCRCTable),
	}
	if err := n.snap.writeFile(index, body); err != nil {
		return InstallSnapshotReply{}, err
	}
	if err := n.snap.storePointer(snap); err != nil {
		return InstallSnapshotReply{}, err
	}
	n.snapshotIndex, n.snapshotTerm = index, term
	n.snapshotVoters = append([]string(nil), voters...)
	if err := n.log.dropPrefix(index + 1); err != nil {
		return InstallSnapshotReply{}, err
	}
	if n.log.length() == 0 {
		n.log.baseTerm = term
	}
	// 文件头里的投票集合覆盖本地视图，配置条目的压缩点随之前移（6.1 §7 接收快照）。
	n.applyConfigLocked(configView{
		voters:     voters,
		index:      index,
		prevVoters: n.membership.clone(),
	})
	n.lg.Info("snapshot installed",
		zap.String("from_node", n.leaderID),
		zap.Uint64("snapshot_index", index),
		zap.Uint64("snapshot_term", term),
		zap.Int("entries", len(entries)),
		zap.Int64("bytes", int64(len(body))))
	return InstallSnapshotReply{OK: true, SnapshotIndex: index}, nil
}

// snapshotTask 是一次补发快照的发送任务：线上带的是 index 与 term，
// 快照字节在锁外从本地文件读出来（5.1 §7 接收快照的线上形状）。
type snapshotTask struct {
	index uint64
	term  uint64
}
