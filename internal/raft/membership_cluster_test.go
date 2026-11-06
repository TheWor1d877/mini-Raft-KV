package raft

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"go.uber.org/zap"
)

// learner 照常收到日志并 apply：降级只改计票，不改复制路径（6.1 §7、§8 不变式 6）。
func TestLearnerStillReceivesTheLog(t *testing.T) {
	c := newCluster(t, 3, zap.NewNop())
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)
	var learnerID string
	for _, id := range c.ids {
		if id != leaderID {
			learnerID = id
			break
		}
	}

	if _, _, err := leader.changeMembership(learnerID, MemberLearner); err != nil {
		t.Fatalf("demote %s: %v", learnerID, err)
	}
	c.tick(4)

	if _, _, err := leader.Propose(context.Background(), "learner-key", "v"); err != nil {
		t.Fatalf("propose after the demotion: %v", err)
	}
	c.tick(4)
	value, err := c.node(learnerID).kv.Get("learner-key")
	if err != nil || value != "v" {
		t.Fatalf("learner Get = %q %v, want v", value, err)
	}
}

// 降级缩小 quorum：4 个 voter 剩 3 台能写，剩 2 台停写（6.1 §9 第三条）。
func TestClusterQuorumShrinksAfterDemotion(t *testing.T) {
	c := newCluster(t, 5, zap.NewNop())
	leaderID := c.waitForLeader(t, 60)
	leader := c.node(leaderID)
	var others []string
	for _, id := range c.ids {
		if id != leaderID {
			others = append(others, id)
		}
	}
	if len(others) != 4 {
		t.Fatalf("others = %v, want four followers", others)
	}

	if _, _, err := leader.changeMembership(others[0], MemberLearner); err != nil {
		t.Fatalf("demote %s: %v", others[0], err)
	}
	c.tick(4)
	voters, _, _, _ := leader.Membership()
	if len(voters) != 4 {
		t.Fatalf("voters = %v, want four after the demotion", voters)
	}

	// 4 个 voter 停 1 台：剩 3，仍是多数派。
	c.closeNode(others[1])
	if _, _, err := leader.Propose(context.Background(), "still-writable", "v"); err != nil {
		t.Fatalf("propose with three of four voters: %v", err)
	}
	if _, _, err := leader.Propose(context.Background(), "still-writable-2", "v"); err != nil {
		t.Fatalf("second propose with three of four voters: %v", err)
	}

	// 再停 1 台：剩 2，不到 3，写停。
	c.closeNode(others[2])
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, _, err := leader.Propose(ctx, "blocked", "v"); !errors.Is(err, ErrCanceled) {
		t.Fatalf("propose with two of four voters = %v, want ErrCanceled", err)
	}
	if !slices.Contains(voters, leaderID) {
		t.Fatalf("leader %s dropped out of voters %v", leaderID, voters)
	}
}
