package raft

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// crashEnv 给每台机器装上同一个命名停点：预热期之后才生效，
// 预热期的已确认写不受影响。
func crashEnv(point string, pauseMS, afterMS int) func(string) []string {
	return func(string) []string {
		return []string{
			"MINIRAFT_CRASH_POINT=" + point,
			fmt.Sprintf("MINIRAFT_CRASH_PAUSE_MS=%d", pauseMS),
			fmt.Sprintf("MINIRAFT_CRASH_AFTER_MS=%d", afterMS),
		}
	}
}

func (p *processCluster) clearEnv(id string) { p.env[id] = nil }

// TestFaultLeaderAndFollowerCrash 覆盖第 1 行的四个写路径窗口：
// leader 在 Append 前后、apply 落 state 前，follower 在 Append 落盘后。
// 判据是预热期的已确认写一条不丢；目标写只有拿到 200 才要求恢复后读得到。
func TestFaultLeaderAndFollowerCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	cases := []struct {
		point   string
		victim  string // leader 或 follower
		pauseMS int
	}{
		{"leader-before-sync", "leader", 4000},
		{"leader-after-sync", "leader", 4000},
		{"apply-before-state", "leader", 4000},
		// follower 在锁内停：停得比 rpcTimeout 长但短于客户端 3s，
		// 其余跟随者能在客户端超时前回完这一批，目标写才拿得到 200。
		{"follower-after-sync", "follower", 1500},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.point, func(t *testing.T) {
			p := newLoopbackProcessCluster(t, 5, nil, crashEnv(tc.point, tc.pauseMS, 5000))
			t.Cleanup(func() { healNetwork(t) })

			leader, _ := p.writeThrough("warm-"+tc.point, "v0")
			warm := map[string]string{"warm-" + tc.point: "v0"}
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("warm-%s-%d", tc.point, i)
				p.writeThrough(key, "v"+string(rune('a'+i)))
				warm[key] = "v" + string(rune('a'+i))
			}

			// 停点只在预热期之后生效：等到它一定会停的那一刻再发目标写。
			time.Sleep(5200 * time.Millisecond)

			target := "target-" + tc.point
			type outcome struct {
				status int
				body   map[string]any
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				status, body, err := p.put(leader, target, "vt")
				done <- outcome{status, body, err}
			}()

			// 目标写已经走到停点；此时杀进去，窗口就是日志行与注释指的那一处。
			time.Sleep(400 * time.Millisecond)
			victim := leader
			if tc.victim == "follower" {
				for _, id := range p.ids {
					if id != leader {
						victim = id
						break
					}
				}
			}
			p.kill9(victim)

			var out outcome
			select {
			case out = <-done:
			case <-time.After(8 * time.Second):
				t.Fatalf("the target write never returned after killing %s", victim)
			}
			t.Logf("victim=%s target status=%d err=%v", victim, out.status, out.err)

			p.clearEnv(victim)
			p.startOne(victim)

			for key, want := range warm {
				for _, id := range p.ids {
					if err := p.waitValue(id, key, want, 20*time.Second); err != nil {
						t.Fatalf("%s lost an acked warmup write: %v", id, err)
					}
				}
			}
			if out.status == http.StatusOK {
				for _, id := range p.ids {
					if err := p.waitValue(id, target, "vt", 20*time.Second); err != nil {
						t.Fatalf("%s lost the acked target write: %v", id, err)
					}
				}
			} else if out.status == http.StatusServiceUnavailable {
				for _, id := range p.ids {
					if err := p.waitValue(id, target, "vt", 300*time.Millisecond); err == nil {
						t.Fatalf("%s applied %s after a 503, want no phantom write", id, target)
					}
				}
			}
			assertSingleLeaderPerTerm(t, p)
		})
	}
}

// TestFaultCandidateCrashMidVote 覆盖 candidate 拉票中途被杀：重启后同一 term 不得出现两个 leader。
func TestFaultCandidateCrashMidVote(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, nil, crashEnv("candidate-after-self-vote", 3000, 5000))
	t.Cleanup(func() { healNetwork(t) })

	leader, _ := p.writeThrough("warm", "v")
	time.Sleep(5200 * time.Millisecond)

	p.kill9(leader)
	// 其余节点到点后各自成为 candidate，并停在自票落盘之后、发投票之前。
	time.Sleep(400 * time.Millisecond)

	var victim string
	for _, id := range p.ids {
		if id != leader {
			victim = id
			break
		}
	}
	p.kill9(victim)
	p.clearEnv(victim)
	p.startOne(victim)

	next, reply := p.writeThrough("after-candidate-crash", "v2")
	if next == leader {
		t.Fatalf("the killed leader %s came back and acked a write", leader)
	}
	if toFloat(reply["term"]) <= 0 {
		t.Fatalf("write reply has no term: %v", reply)
	}
	assertSingleLeaderPerTerm(t, p)
	if err := p.waitValue(next, "warm", "v", 15*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestFaultConfigEntryCrash 覆盖配置条目已追加并 Sync、还没提交时杀 leader：
// 新 leader 要把配置恢复成同一份视图，多数派仍成立。
func TestFaultConfigEntryCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, nil, crashEnv("config-after-sync", 4000, 5000))
	t.Cleanup(func() { healNetwork(t) })

	leader, _ := p.writeThrough("warm", "v")
	time.Sleep(5200 * time.Millisecond)

	target := ""
	for _, id := range p.ids {
		if id != leader {
			target = id
			break
		}
	}
	done := make(chan error, 1)
	go func() {
		status, raw, err := p.postJSON(leader, "/raft/membership", `{"id":"`+target+`","role":"learner"}`)
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("membership change = %d: %s", status, firstBytes(raw, 200))
		}
		done <- err
	}()
	time.Sleep(400 * time.Millisecond)
	p.kill9(leader)
	<-done

	p.clearEnv(leader)
	p.startOne(leader)

	next, _ := p.writeThrough("after-config-crash", "v2")
	deadline := time.Now().Add(20 * time.Second)
	var views []string
	for time.Now().Before(deadline) {
		views = views[:0]
		for _, id := range p.ids {
			voters, learners, _, status, err := p.membership(id)
			if err != nil || status != http.StatusOK {
				views = nil
				break
			}
			views = append(views, strings.Join(voters, ",")+"|"+strings.Join(learners, ","))
		}
		if len(views) == len(p.ids) {
			same := true
			for _, v := range views[1:] {
				if v != views[0] {
					same = false
				}
			}
			if same {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(views) != len(p.ids) {
		t.Fatalf("membership did not converge after the config crash: %v", views)
	}
	for _, v := range views[1:] {
		if v != views[0] {
			t.Fatalf("membership views diverged after the config crash: %v", views)
		}
	}
	if err := p.waitValue(next, "warm", "v", 15*time.Second); err != nil {
		t.Fatal(err)
	}
	assertSingleLeaderPerTerm(t, p)
}

// TestFaultWholeClusterCrash 覆盖整集群同时被杀：重启后能选出 leader，已确认写全部回来。
func TestFaultWholeClusterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, nil, nil)

	written := map[string]string{}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("whole-%02d", i)
		value := fmt.Sprintf("v-%02d", i)
		p.writeThrough(key, value)
		written[key] = value
	}
	for _, id := range p.ids {
		p.kill9(id)
	}
	p.start()

	leader, _ := p.writeThrough("after-whole-crash", "v")
	for key, want := range written {
		if err := p.waitValue(leader, key, want, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range p.ids {
		if err := p.waitValue(id, "after-whole-crash", "v", 20*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	assertSingleLeaderPerTerm(t, p)
}

// TestFaultFollowerCrashDuringReplay 覆盖回放日志中途被杀：连续三次起停之后仍能补齐到最新。
func TestFaultFollowerCrashDuringReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	p := newLoopbackProcessCluster(t, 5, nil, nil)

	leader, _ := p.writeThrough("replay-seed", "v")
	var victim string
	for _, id := range p.ids {
		if id != leader {
			victim = id
			break
		}
	}
	p.kill9(victim)

	written := map[string]string{}
	for i := 0; i < 400; i++ {
		key := fmt.Sprintf("replay-%03d", i)
		value := fmt.Sprintf("v-%03d", i)
		p.writeThrough(key, value)
		written[key] = value
	}

	for round := 0; round < 3; round++ {
		p.spawnOne(victim)
		time.Sleep(30 * time.Millisecond)
		p.killIfRunning(victim)
	}
	p.startOne(victim)

	for key, want := range written {
		if err := p.waitValue(victim, key, want, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

func (p *processCluster) killIfRunning(id string) {
	cmd := p.cmd[id]
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Kill()
	state, err := cmd.Process.Wait()
	if err != nil {
		p.t.Errorf("reap %s: %v", id, err)
	}
	cmd.ProcessState = state
}
