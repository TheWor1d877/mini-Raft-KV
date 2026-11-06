package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func readBody(resp *http.Response) ([]byte, error) {
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

// postJSON 发一条 JSON POST，给成员变更与 /put 共用。
func (p *processCluster) postJSON(id, path, body string) (int, []byte, error) {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).
		Post(p.url(id, path), "application/json", strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := readBody(resp)
	return resp.StatusCode, raw, err
}

// membership 读一台节点的视图，顺带带回它认的 Leader。
func (p *processCluster) membership(id string) (voters, learners []string, leaderID string, status int, err error) {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(p.url(id, "/raft/membership"))
	if err != nil {
		return nil, nil, "", 0, err
	}
	defer resp.Body.Close()
	raw, err := readBody(resp)
	if err != nil {
		return nil, nil, "", resp.StatusCode, err
	}
	var body struct {
		Voters   []string `json:"voters"`
		Learners []string `json:"learners"`
		LeaderID string   `json:"leader_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, nil, "", resp.StatusCode, fmt.Errorf("%s: body %s", err, firstBytes(raw, 200))
	}
	return body.Voters, body.Learners, body.LeaderID, resp.StatusCode, nil
}

// 真实进程：停掉一个 voter，写入把提交点推前，阈值到了它被自动降成 learner；
// 之后再停一个 voter，剩下的多数派仍能提交（6.1 §7 自动降级、§9 第三条）。
func TestAutoDemotionInProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level case")
	}
	p := newProcessClusterFlags(t, 5, []string{
		"-learner-demote-ms", "200",
		"-snapshot-exempt-entries", "5",
	})
	for _, id := range p.ids {
		p.waitReady(id)
	}
	leader, _ := p.writeThrough("seed", "v")

	var target string
	for _, id := range p.ids {
		if id != leader {
			target = id
			break
		}
	}
	p.kill9(target)
	for i := 0; i < 6; i++ {
		p.writeThrough(fmt.Sprintf("k%d", i), "v")
	}

	deadline := time.Now().Add(15 * time.Second)
	var voters, learners []string
	for time.Now().Before(deadline) {
		var status int
		var err error
		voters, learners, _, status, err = p.membership(leader)
		if err == nil && status == http.StatusOK && slices.Contains(learners, target) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !slices.Contains(learners, target) {
		t.Fatalf("%s was not demoted: voters = %v learners = %v", target, voters, learners)
	}
	if slices.Contains(voters, target) {
		t.Fatalf("%s is both voter and learner: %v", target, voters)
	}

	// voter 从 5 缩到 4：再停一台，剩 3 台仍能提交。
	var next string
	for _, id := range voters {
		if id != leader {
			next = id
			break
		}
	}
	if next == "" {
		t.Fatalf("voters = %v, want one more voter to stop", voters)
	}
	p.kill9(next)
	status, _, err := p.put(leader, "after-second-stop", "v")
	if err != nil || status != http.StatusOK {
		t.Fatalf("write after stopping a second voter = %d %v, want 200", status, err)
	}
}

// 真实进程：Leader 把自己降成 learner，新 Leader 在选举窗口内产生，
// 旧 Leader 的视图里自己不再有投票权（6.1 §7 第 7 步、D41）。
func TestLeaderDemotesItselfInProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level case")
	}
	p := newProcessCluster(t, 5)
	for _, id := range p.ids {
		p.waitReady(id)
	}
	leader, _ := p.writeThrough("before", "v")

	status, raw, err := p.postJSON(leader, "/raft/membership",
		fmt.Sprintf(`{"id":%q,"role":"learner"}`, leader))
	if err != nil || status != http.StatusOK {
		t.Fatalf("demote the leader = %d %v: %s", status, err, firstBytes(raw, 200))
	}

	deadline := time.Now().Add(5 * time.Second)
	var newLeader string
	for time.Now().Before(deadline) && newLeader == "" {
		for _, id := range p.ids {
			if id == leader {
				continue
			}
			if status, _, err := p.put(id, "after-step-down", "v"); err == nil && status == http.StatusOK {
				newLeader = id
				break
			}
		}
		if newLeader == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if newLeader == "" {
		t.Fatalf("no new leader within 5s after %s stepped down", leader)
	}

	voters, learners, _, status, err := p.membership(leader)
	if err != nil || status != http.StatusOK {
		t.Fatalf("read the old leader's view = %d %v", status, err)
	}
	if slices.Contains(voters, leader) {
		t.Fatalf("the old leader is still a voter in its own view: %v", voters)
	}
	if !slices.Contains(learners, leader) {
		t.Fatalf("the old leader is not a learner in its own view: %v", learners)
	}
}
