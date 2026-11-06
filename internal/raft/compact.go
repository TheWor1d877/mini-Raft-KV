package raft

import (
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 压缩这一层只管两件事：算删除边界与删日志前缀（5.1 §2）。
// 快照文件本身的生成与提交在 snapshot.go，接收对端快照在 install.go。

// safeIndex 是删除边界的一半：全部成员都已经复制到的最高 index（5.1 §7 删除边界）。
// Leader 取各成员 matchIndex 的最小值，自己算一个成员；Follower 没有 matchIndex，取本地 commitIndex。
// 被豁免的成员不参与计算：它落后太多，挡住删除就等于日志永远删不掉。
func (n *Node) safeIndex() uint64 {
	if n.role != RoleLeader {
		return n.commitIndex
	}
	safe := n.log.lastIndex()
	for _, peerID := range n.cfg.otherPeers() {
		match := n.matchIndex[peerID]
		if n.exempted(peerID, match) {
			continue
		}
		safe = min(safe, match)
	}
	return safe
}

// exempted 判断一个成员是否被豁免出 safeIndex 的计算（5.1 §7 豁免规则）。
// 落后超过 -snapshot-exempt-entries 条的成员不参与计算，豁免期间它在日志里报警。
func (n *Node) exempted(peerID string, match uint64) bool {
	if n.cfg.SnapshotExemptEntries == 0 {
		return false
	}
	return n.lagBehind(match) > n.cfg.SnapshotExemptEntries
}

// lagBehind 是某个成员落后提交点的条数。
func (n *Node) lagBehind(match uint64) uint64 {
	if n.commitIndex <= match {
		return 0
	}
	return n.commitIndex - match
}

// logExempted 把豁免的成员逐个记进日志：豁免是背压信号，不静默处理（5.1 §7 豁免规则）。
func (n *Node) logExempted() {
	for _, peerID := range n.cfg.otherPeers() {
		match := n.matchIndex[peerID]
		if !n.exempted(peerID, match) {
			continue
		}
		n.lg.Warn("snapshot exemption applied",
			zap.String("peer_id", peerID),
			zap.Uint64("lag_entries", n.lagBehind(match)),
			zap.Uint64("commit_index", n.commitIndex))
	}
}

// dropLogPrefix 删掉 index 之前的日志条目，index 那条保留（5.1 §4）。
// 只在 Leader 上做：删掉的条目别的成员可能还要，Follower 的前缀由补发快照处理。
func (n *Node) dropLogPrefix(index uint64) error {
	return n.log.dropPrefix(index)
}

// snapshotPointLocked 选快照点：index 取 min(lastApplied, commitIndex)，term 取那个位置上的 term。
// lastApplied 为 0 时没有快照点（5.1 §7 的生成与提交第 1 步）。
func (n *Node) snapshotPointLocked() (uint64, uint64, bool) {
	if n.lastApplied == 0 || n.commitIndex == 0 {
		return 0, 0, false
	}
	index := min(n.lastApplied, n.commitIndex)
	term := n.snapshotTerm
	if e, ok := n.log.entryAt(index); ok {
		term = e.term
	}
	return index, term, true
}

// TriggerSnapshot 手动触发一次快照：非 Leader 回 ErrNotLeader，
// 快照点选不出来回 ErrStateMismatch（5.1 §4 的契约表）。
func (n *Node) TriggerSnapshot() (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	if n.snapshotting {
		n.mu.Unlock()
		return 0, ErrStateMismatchAt(n.snapshotIndex, "a snapshot is already in flight")
	}
	n.mu.Unlock()
	return n.runSnapshot()
}

// runSnapshot 是手动与自动触发共用的那条路径（5.1 §7 的生成与提交五步）：
// 锁内选点与导出 → 锁外写快照文件 → 锁内更新指针、写游标、算边界、删日志前缀。
func (n *Node) runSnapshot() (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	if n.snapshotting {
		n.mu.Unlock()
		return 0, ErrStateMismatchAt(n.snapshotIndex, "a snapshot is already in flight")
	}
	index, term, ok := n.snapshotPointLocked()
	if !ok {
		n.mu.Unlock()
		return 0, ErrStateMismatchAt(0, "there is no snapshot point yet")
	}
	if index < n.commitIndex {
		// apply 落后把快照点卡住，日志因此删不动：这是背压，告警并治理 apply 慢，不放宽删除边界。
		n.lg.Warn("snapshot blocked by apply lag",
			zap.Uint64("commit_index", n.commitIndex),
			zap.Uint64("last_applied", n.lastApplied),
			zap.Uint64("lag_entries", n.commitIndex-n.lastApplied))
	}
	// 导出与快照点在同一段持锁内完成，导出的内容正好是快照点那一刻的状态（不变式 8）。
	// 投票集合同样取这一刻的视图，写进快照文件头（6.1 §7 生成快照）。
	entries := n.kv.Snapshot()
	voters := n.membership.clone()
	n.snapshotting = true
	n.mu.Unlock()

	start := time.Now()
	snap, err := n.snap.create(index, term, entries, voters)
	if err != nil {
		n.finishSnapshot()
		return 0, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotting = false
	// 指针先更新再删日志：崩溃停在两步之间时，指针指向新快照，日志前缀还在，
	// 重启按 §恢复顺序跳过 <= snapshotIndex 的条目，结果一致（5.1 §7）。
	if err := n.snap.storePointer(snap); err != nil {
		return 0, err
	}
	n.snapshotIndex, n.snapshotTerm = snap.Index, snap.Term
	n.snapshotVoters = append([]string(nil), snap.Voters...)
	n.lastSnapshotMS = monotonicMS()
	n.lg.Info("snapshot created",
		zap.Uint64("snapshot_index", snap.Index),
		zap.Uint64("snapshot_term", snap.Term),
		zap.Int("entries", snap.Entries),
		zap.Int64("bytes", snap.Bytes),
		zap.Int64("cost_ms", time.Since(start).Milliseconds()))
	if err := n.state.store(n.currentState()); err != nil {
		return 0, err
	}
	if n.role != RoleLeader {
		// 期间 leadership 变了：指针已经更新，前缀留给下一任 Leader 删。
		return snap.Index, nil
	}
	n.logExempted()
	// apply 落后时快照点被 lastApplied 卡住，日志一条不删：这是背压，
	// 告警已经打出去，该治理的是 apply 慢，不是删除边界（5.1 §7 触发）。
	if snap.Index < n.commitIndex {
		return snap.Index, nil
	}
	// 删除边界取 min(snapshotIndex, safeIndex)。safeIndex 小于快照点时一条不删：
	// 删到快照点之前，删完的基线与快照点不再对齐，基线要等下一轮压缩才回得来。
	safe := n.safeIndex()
	boundary := min(snap.Index, safe)
	if boundary < snap.Index {
		n.lg.Warn("snapshot deletion boundary held back",
			zap.Uint64("snapshot_index", snap.Index),
			zap.Uint64("safe_index", safe))
		return snap.Index, nil
	}
	if boundary > n.log.baseIndex-1 {
		if err := n.dropLogPrefix(boundary + 1); err != nil {
			return 0, err
		}
		// 日志被删空时 lastTerm 靠基线上的 term 回答投票请求：那一格是快照点（5.1 §5）。
		if n.log.length() == 0 {
			n.log.baseTerm = n.snapshotTerm
		}
	}
	return snap.Index, nil
}

// finishSnapshot 清掉快照状态位：写文件失败时也要让下一次触发有机会（不变式 4）。
func (n *Node) finishSnapshot() {
	n.mu.Lock()
	n.snapshotting = false
	n.mu.Unlock()
}

// maybeTriggerSnapshot 是自动触发的那一格：三个条件都满足才动手（5.1 §7 的触发）。
// 两个阈值都为 0 时自动触发全关，手动入口不受影响。
func (n *Node) maybeTriggerSnapshot() {
	n.mu.Lock()
	if !n.shouldTriggerLocked() {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	if _, err := n.runSnapshot(); err != nil {
		n.lg.Warn("snapshot failed", zap.Error(err))
	}
}

// shouldTriggerLocked 判定自动触发的三个条件：阈值、最小间隔、当前身份。
func (n *Node) shouldTriggerLocked() bool {
	if n.role != RoleLeader || n.snapshotting {
		return false
	}
	if n.cfg.SnapshotEntries == 0 && n.cfg.SnapshotBytes == 0 {
		return false
	}
	if n.lastApplied == 0 {
		return false
	}
	if n.cfg.SnapshotMinIntervalMS > 0 && n.lastSnapshotMS > 0 {
		if monotonicMS()-n.lastSnapshotMS < n.cfg.SnapshotMinIntervalMS {
			return false
		}
	}
	if n.cfg.SnapshotEntries > 0 && n.log.lastIndex()-n.snapshotIndex >= n.cfg.SnapshotEntries {
		return true
	}
	if n.cfg.SnapshotBytes > 0 && n.walDirBytes() >= n.cfg.SnapshotBytes {
		return true
	}
	return false
}

// walDirBytes 是日志目录的总字节数：它只作触发条件，不改删除边界（5.1 §7 删除边界）。
func (n *Node) walDirBytes() int64 {
	segs, err := listWALSegments(n.cfg)
	if err != nil {
		return 0
	}
	var total int64
	for _, s := range segs {
		total += s.size
	}
	return total
}

// monotonicMS 是墙上时间的毫秒数，只用于快照的最小间隔。
// 选举与心跳不看墙上时间（D16），这一处不参与共识判定。
func monotonicMS() int64 { return time.Now().UnixMilli() }

// dropPrefix 删掉 keepFrom 之前的条目，keepFrom 那条保留（5.1 §5 的基线口径）。
// 整段文件删掉，保留的那一段去掉前缀字节，视图的基线前移到新的第一条。
func (l *wal) dropPrefix(keepFrom uint64) error {
	if err := l.writeBlocked(); err != nil {
		return err
	}
	if keepFrom <= l.baseIndex {
		return nil
	}

	idx := -1
	for i, e := range l.entries {
		if e.index >= keepFrom {
			idx = i
			break
		}
	}
	if idx < 0 {
		return l.clearPrefix(keepFrom)
	}
	first := l.entries[idx]

	segs, err := listWALSegments(l.cfg)
	if err != nil {
		return err
	}
	droppedSegments := 0
	var freedBytes int64
	for _, s := range segs {
		if s.seq >= first.seq {
			break
		}
		freedBytes += s.size
		if err := os.Remove(s.path); err != nil {
			return kvstore.ErrIOFrom("remove wal segment", 0, err)
		}
		droppedSegments++
		// 删日志途中的崩溃落点：每删掉一个段文件停一次，进程停在这里时磁盘上是「一部分段没了」。
		crashPointPause()
	}
	if first.offset > 0 {
		size, err := rewriteWALSegment(l.cfg, first.seq, first.offset)
		if err != nil {
			return err
		}
		freedBytes += first.offset
		for _, e := range l.entries[idx:] {
			if e.seq == first.seq {
				e.offset -= first.offset
			}
		}
		if l.seq == first.seq {
			// 当前段就是被重写的那一段：追加位置跟着新长度走，句柄要重开（改名换了文件）。
			l.size = size
			if err := l.resumeAt(l.seq, l.size); err != nil {
				return err
			}
		}
	}

	dropped := idx
	// 新基线前一格的 term 要留下来：那条已经从本地日志里删掉，
	// 组 AppendEntries 时它就是 prevLogTerm（5.1 §5 的基线口径）。
	l.baseTerm = l.entries[idx-1].term
	l.entries = l.entries[idx:]
	l.baseIndex = first.index
	l.rebuildTermIndex()
	l.lg.Info("wal prefix dropped",
		zap.Uint64("kept_from_index", first.index),
		zap.Int("dropped_entries", dropped),
		zap.Int("dropped_segments", droppedSegments),
		zap.Int64("freed_bytes", freedBytes))
	return nil
}

// clearPrefix 把日志清空到 keepFrom 那一格：当前段之前的段文件删掉，当前段截到 0 长度。
func (l *wal) clearPrefix(keepFrom uint64) error {
	segs, err := listWALSegments(l.cfg)
	if err != nil {
		return err
	}
	var freedBytes int64
	droppedSegments := 0
	for _, s := range segs {
		if s.seq < l.seq {
			freedBytes += s.size
			if err := os.Remove(s.path); err != nil {
				return kvstore.ErrIOFrom("remove wal segment", 0, err)
			}
			droppedSegments++
			crashPointPause()
			continue
		}
		if s.seq == l.seq {
			freedBytes += s.size
			if err := truncateWALFile(s.path, 0); err != nil {
				return err
			}
		}
	}
	dropped := len(l.entries)
	// 基线前一格的 term 只有在本地日志里正好有那一格时才认得：
	// 认不出就留 0，调用方知道自己带的快照点 term 时再补上（5.1 §5 的基线口径）。
	l.baseTerm = 0
	if dropped > 0 && l.entries[dropped-1].index == keepFrom-1 {
		l.baseTerm = l.entries[dropped-1].term
	}
	l.entries = nil
	l.baseIndex = keepFrom
	l.rebuildTermIndex()
	if err := l.resumeAt(l.seq, 0); err != nil {
		return err
	}
	l.lg.Info("wal prefix dropped",
		zap.Uint64("kept_from_index", keepFrom),
		zap.Int("dropped_entries", dropped),
		zap.Int("dropped_segments", droppedSegments),
		zap.Int64("freed_bytes", freedBytes))
	return nil
}

// rewriteWALSegment 去掉一个段文件的前缀字节：读出 offset 之后的内容写进临时名再改名。
// 返回新的文件长度。os.Truncate 只能砍尾部，砍头部只能整段重写。
func rewriteWALSegment(cfg Config, seq uint64, offset int64) (int64, error) {
	path := cfg.WALPath(seq)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, kvstore.ErrIOFrom("read wal segment", 0, err)
	}
	if offset > int64(len(data)) {
		return 0, kvstore.ErrIOFrom("read wal segment", offset, os.ErrInvalid)
	}
	suffix := data[offset:]

	tmp := cfg.WALTmpPath(seq)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, kvstore.ErrIOFrom("create wal tmp", 0, err)
	}
	if _, err := f.Write(suffix); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, kvstore.ErrIOFrom("write wal tmp", 0, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, kvstore.ErrIOFrom("fsync wal tmp", 0, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return 0, kvstore.ErrIOFrom("close wal tmp", 0, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, kvstore.ErrIOFrom("rename wal file", 0, err)
	}
	if err := syncDir(cfg.WALDir()); err != nil {
		return 0, err
	}
	return int64(len(suffix)), nil
}
