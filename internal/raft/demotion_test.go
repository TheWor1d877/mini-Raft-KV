package raft

import (
	"slices"
	"testing"

	"go.uber.org/zap"
)

// 自动降级的计时：连续 demoteTicks 拍仍在豁免里才触发，中途追平一次就清零（6.1 §7、D39）。
func TestAutoDemotionClockAndReset(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 1
		cfg.LearnerDemoteMS = 250
		cfg.TickMS = 50
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)
	if leader.demoteTicks != 5 {
		t.Fatalf("demoteTicks = %d, want 250/50 = 5", leader.demoteTicks)
	}
	var target string
	for _, id := range c.ids {
		if id != leaderID {
			target = id
			break
		}
	}

	// 进入豁免：commitIndex 远在 matchIndex 前面，落后条数超过阈值。
	leader.mu.Lock()
	leader.commitIndex = 100
	leader.matchIndex[target] = 0
	leader.tickSeq = 1
	leader.checkAutoDemotionLocked()
	since, ok := leader.exemptSince[target]
	leader.mu.Unlock()
	if !ok || since != 1 {
		t.Fatalf("exemptSince = %d %v, want tick 1 recorded", since, ok)
	}

	// 差一拍不触发。
	leader.mu.Lock()
	leader.tickSeq = 5
	leader.checkAutoDemotionLocked()
	stillVoter := leader.membership.isVoter(target)
	leader.mu.Unlock()
	if !stillVoter {
		t.Fatal("the member was demoted one tick early")
	}

	// 追平一次：计时清零。
	leader.mu.Lock()
	leader.matchIndex[target] = 100
	leader.tickSeq = 6
	leader.checkAutoDemotionLocked()
	_, still := leader.exemptSince[target]
	leader.mu.Unlock()
	if still {
		t.Fatal("the exempt clock was not cleared when the member caught up")
	}

	// 重新落后要重新数满。
	leader.mu.Lock()
	leader.matchIndex[target] = 0
	leader.tickSeq = 7
	leader.checkAutoDemotionLocked()
	leader.tickSeq = 11
	leader.checkAutoDemotionLocked()
	early := leader.membership.isVoter(target)
	leader.tickSeq = 12
	leader.checkAutoDemotionLocked()
	demoted := !leader.membership.isVoter(target)
	leader.mu.Unlock()
	if !early {
		t.Fatal("the member was demoted before the restarted clock reached the threshold")
	}
	if !demoted {
		t.Fatal("the member was not demoted after the threshold")
	}
}

// -learner-demote-ms=0 关掉自动降级：条件再满足也一条配置条目都不发（6.1 §6）。
func TestAutoDemotionDisabled(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 1
		cfg.LearnerDemoteMS = 0
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)
	var target string
	for _, id := range c.ids {
		if id != leaderID {
			target = id
			break
		}
	}

	leader.mu.Lock()
	leader.commitIndex = 100
	leader.matchIndex[target] = 0
	for tick := uint64(1); tick <= 50; tick++ {
		leader.tickSeq = tick
		leader.checkAutoDemotionLocked()
	}
	voters := leader.membership.clone()
	configIndex := leader.configIndex
	leader.mu.Unlock()
	if !slices.Contains(voters, target) {
		t.Fatalf("voters = %v, want the member kept with auto demotion disabled", voters)
	}
	if configIndex != 0 {
		t.Fatalf("configIndex = %d, want 0", configIndex)
	}
}

// 真实 Tick 驱动：停掉一个 voter，写入把提交点推前，阈值到了成员被降成 learner。
func TestAutoDemotionThroughTicks(t *testing.T) {
	c := newClusterCfg(t, 3, zap.NewNop(), func(cfg *Config) {
		cfg.SnapshotExemptEntries = 1
		cfg.LearnerDemoteMS = 100
		cfg.TickMS = 50
	})
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)
	var stopped string
	for _, id := range c.ids {
		if id != leaderID {
			stopped = id
			break
		}
	}
	c.closeNode(stopped)

	for i := 0; i < 4; i++ {
		if _, _, err := leader.Propose(t.Context(), "k"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.tick(12)

	voters, learners, index, _ := leader.Membership()
	if slices.Contains(voters, stopped) {
		t.Fatalf("voters = %v, want %s demoted after the threshold", voters, stopped)
	}
	if !slices.Contains(learners, stopped) {
		t.Fatalf("learners = %v, want %s", learners, stopped)
	}
	if index == 0 {
		t.Fatal("configIndex is still 0 after the automatic demotion")
	}
}
