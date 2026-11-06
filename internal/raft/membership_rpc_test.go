package raft

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func membershipValues(t *testing.T, raw []byte) (voters, learners []string, index, term uint64, leader string) {
	t.Helper()
	var body struct {
		Voters   []string `json:"voters"`
		Learners []string `json:"learners"`
		Index    uint64   `json:"index"`
		Term     uint64   `json:"term"`
		LeaderID string   `json:"leader_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode membership response: %v: %s", err, raw)
	}
	return body.Voters, body.Learners, body.Index, body.Term, body.LeaderID
}

// GET /raft/membership 的字段与排序：两个列表都升序，并集是地址表（6.1 §4 形状表）。
func TestMembershipQueryShape(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)

	status, raw := c.get(leader, "/raft/membership")
	if status != http.StatusOK {
		t.Fatalf("GET membership = %d: %s", status, firstBytes(raw, 200))
	}
	voters, learners, index, term, gotLeader := membershipValues(t, raw)
	if !slices.Equal(voters, []string{"node-1", "node-2", "node-3"}) {
		t.Fatalf("voters = %v, want the full table in order", voters)
	}
	if len(learners) != 0 {
		t.Fatalf("learners = %v, want none", learners)
	}
	if index != 0 || term == 0 {
		t.Fatalf("index = %d term = %d, want 0 and a non-zero term", index, term)
	}
	if gotLeader != leader {
		t.Fatalf("leader_id = %q, want %q", gotLeader, leader)
	}

}

// POST /raft/membership 成功之后视图立刻变化，提交之后各节点一致（6.1 §7）。
func TestMembershipChangeThroughHTTP(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)
	var target string
	for _, id := range c.ids {
		if id != leader {
			target = id
			break
		}
	}

	body := `{"id":"` + target + `","role":"learner"}`
	status, raw := c.post(leader, "/raft/membership", body)
	if status != http.StatusOK {
		t.Fatalf("POST membership = %d: %s", status, firstBytes(raw, 200))
	}
	reply := decodeMap(t, raw)
	if reply["ok"] != true {
		t.Fatalf("reply = %v, want ok", reply)
	}
	if _, ok := reply["index"]; !ok {
		t.Fatalf("reply is missing index: %v", reply)
	}

	voters, learners, index, _ := c.node[leader].Membership()
	if slices.Contains(voters, target) {
		t.Fatalf("voters = %v, want %s removed before commit", voters, target)
	}
	if !slices.Contains(learners, target) {
		t.Fatalf("learners = %v, want %s", learners, target)
	}
	if index == 0 {
		t.Fatal("configIndex is still 0 after the change")
	}

	c.tick(6)
	for _, id := range c.ids {
		if id == target {
			continue
		}
		status, raw := c.get(id, "/raft/membership")
		if status != http.StatusOK {
			t.Fatalf("GET membership on %s = %d: %s", id, status, firstBytes(raw, 200))
		}
		gotVoters, gotLearners, _, _, _ := membershipValues(t, raw)
		if !slices.Equal(gotVoters, voters) || !slices.Equal(gotLearners, learners) {
			t.Fatalf("%s view = %v / %v, want %v / %v", id, gotVoters, gotLearners, voters, learners)
		}
	}
}

func TestMembershipBadRequests(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)
	cases := []string{
		`{}`,
		`{"id":"node-2"}`,
		`{"role":"learner"}`,
		`{"id":"node-2","role":"observer"}`,
	}
	for _, body := range cases {
		status, raw := c.post(leader, "/raft/membership", body)
		if status != http.StatusBadRequest {
			t.Fatalf("POST %s = %d: %s", body, status, firstBytes(raw, 200))
		}
		if got := decodeMap(t, raw)["code"]; got != "bad_request" {
			t.Fatalf("POST %s: code = %v, want bad_request", body, got)
		}
	}
}

func TestMembershipOnFollowerIsNotLeader(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	status, raw := c.post(follower, "/raft/membership", `{"id":"`+leader+`","role":"learner"}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("POST on a follower = %d: %s", status, firstBytes(raw, 200))
	}
	if got := decodeMap(t, raw)["code"]; got != "not_leader" {
		t.Fatalf("code = %v, want not_leader", got)
	}
}

func TestMembershipRejectionsThroughHTTP(t *testing.T) {
	c := newHTTPCluster(t, 3, zap.NewNop())
	leader := c.waitForLeader(60)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown id", `{"id":"node-9","role":"learner"}`, "address table"},
		{"unchanged role", `{"id":"` + leader + `","role":"voter"}`, "already a voter"},
	}
	for _, tc := range cases {
		status, raw := c.post(leader, "/raft/membership", tc.body)
		if status != http.StatusConflict {
			t.Fatalf("%s = %d: %s", tc.name, status, firstBytes(raw, 200))
		}
		reply := decodeMap(t, raw)
		if reply["code"] != "config_rejected" || !strings.Contains(reply["error"].(string), tc.want) {
			t.Fatalf("%s reply = %v, want config_rejected with %q", tc.name, reply, tc.want)
		}
	}

	var target, other string
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if target == "" {
			target = id
		} else {
			other = id
		}
	}
	if status, raw := c.post(leader, "/raft/membership", `{"id":"`+target+`","role":"learner"}`); status != http.StatusOK {
		t.Fatalf("first change = %d: %s", status, firstBytes(raw, 200))
	}
	status, raw := c.post(leader, "/raft/membership", `{"id":"`+other+`","role":"learner"}`)
	if status != http.StatusConflict {
		t.Fatalf("second change while one is in flight = %d: %s", status, firstBytes(raw, 200))
	}
	if got := decodeMap(t, raw)["error"].(string); !strings.Contains(got, "in flight") {
		t.Fatalf("error = %q, want the in-flight reason", got)
	}
}

func TestMembershipLastVoterRefusedThroughHTTP(t *testing.T) {
	c := newHTTPCluster(t, 1, zap.NewNop())
	leader := c.waitForLeader(60)
	status, raw := c.post(leader, "/raft/membership", `{"id":"`+leader+`","role":"learner"}`)
	if status != http.StatusConflict {
		t.Fatalf("demoting the last voter = %d: %s", status, firstBytes(raw, 200))
	}
	if got := decodeMap(t, raw)["error"].(string); !strings.Contains(got, "no voters") {
		t.Fatalf("error = %q, want the no-voter reason", got)
	}
}
