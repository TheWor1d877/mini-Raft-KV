package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFaultAutoDemotionUnderPartition covers 第 7 行：逐个隔离 voter，
// leader 把它们降成 learner；降到 2 个 voter 仍能提交；恢复后只追数据、不自动升回。
func TestFaultAutoDemotionUnderPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	requireNetAdmin(t)
	p := newLoopbackProcessCluster(t, 5, []string{
		"-learner-demote-ms", "500",
		"-snapshot-exempt-entries", "5",
	}, nil)
	t.Cleanup(func() { healNetwork(t) })

	leader, _ := p.writeThrough("demote-seed", "v")
	isolated := map[string]bool{}

	writeRound := func(step int) {
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("demote-%d-%02d", step, i)
			status, _, err := p.put(leader, key, "v")
			if err != nil || status != http.StatusOK {
				voters, learners, _, _, _ := p.membership(leader)
				t.Logf("step %d: %s refused %s (status=%d err=%v); view %v/%v",
					step, leader, key, status, err, voters, learners)
				var next string
				var reply map[string]any
				next, reply = p.writeThrough(key, "v")
				leader = next
				if toFloat(reply["term"]) == 0 {
					t.Fatalf("no leader while demoting: %v", reply)
				}
			}
		}
	}

	for step := 1; step <= 3; step++ {
		victim := ""
		for _, id := range p.ids {
			if id != leader && !isolated[id] {
				victim = id
				break
			}
		}
		if victim == "" {
			t.Fatalf("no voter left to isolate at step %d", step)
		}
		isolateNode(t, p, victim)
		isolated[victim] = true

		writeRound(step)
		voters, learners, err := p.waitMembership(leader, 5-step, step, 15*time.Second)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		// 视图在配置条目入日志时就变，提交要等旧多数派回执。先确认这次降级已经提交，
		// 再隔离下一个 voter：否则旧多数派被一起切断，这条配置永远提交不了。
		barrier := fmt.Sprintf("demote-barrier-%d", step)
		if status, _, err := p.put(leader, barrier, "v"); err != nil || status != http.StatusOK {
			var next string
			var reply map[string]any
			next, reply = p.writeThrough(barrier, "v")
			leader = next
			if toFloat(reply["term"]) == 0 {
				t.Fatalf("step %d: the demotion never committed", step)
			}
		}
		t.Logf("step %d isolated=%v voters=%v learners=%v", step, victim, voters, learners)
	}

	// 2 个 voter 时仍能提交；再把最后一个同伴切断，1/2 没有多数派，配置也降不下去。
	if status, _, _ := p.put(leader, "two-voters", "v"); status != http.StatusOK {
		t.Fatalf("2 voters could not commit: status=%d", status)
	}
	lastVoter := ""
	for _, id := range p.ids {
		if id != leader && !isolated[id] {
			lastVoter = id
			break
		}
	}
	if lastVoter == "" {
		t.Fatalf("no second voter left")
	}
	isolateNode(t, p, lastVoter)
	time.Sleep(1500 * time.Millisecond)
	if status, _, _ := p.put(leader, "one-voter", "v"); status == http.StatusOK {
		t.Fatalf("1/2 voter committed a write")
	}
	voters, learners, _, status, err := p.membership(leader)
	if err != nil || status != http.StatusOK {
		t.Fatalf("membership on %s: %d %v", leader, status, err)
	}
	if len(voters) != 2 || len(learners) != 3 {
		t.Fatalf("with one voter alive the view is voters=%v learners=%v, want 2/3", voters, learners)
	}

	healNetwork(t)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, _, err := p.waitMembership(leader, 2, 3, time.Second); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("membership did not settle at 2 voters / 3 learners after the heal")
		}
		// 隔离期间被抬高的 term 会触发一轮重选，leader 可能换人。
		if next, reply := p.writeThrough("demote-heal", "v"); toFloat(reply["term"]) > 0 {
			leader = next
		}
	}
	// 被降级的节点以 learner 身份追数据：最后一条写在恢复后必须能读到。
	for _, id := range p.ids {
		if err := p.waitValue(id, "two-voters", "v", 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

func (p *processCluster) waitMembership(id string, wantVoters, wantLearners int, timeout time.Duration) ([]string, []string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		voters, learners, _, status, err := p.membership(id)
		if err == nil && status == http.StatusOK && len(voters) == wantVoters && len(learners) == wantLearners {
			return voters, learners, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	voters, learners, _, status, err := p.membership(id)
	return voters, learners, fmt.Errorf("membership stayed %d/%d at status %d (err %v), want %d/%d",
		len(voters), len(learners), status, err, wantVoters, wantLearners)
}

// TestFaultSnapshotInstallWindows covers 第 8 行：在安装侧三处窗口杀进程。
// tmp 与 pointer 两个窗口从盘上的临时文件判定；state 窗口按 state 游标已经前移、
// 指针还没换判定。每个窗口都要求重启后能就维并补齐。
func TestFaultSnapshotInstallWindows(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level fault case")
	}
	cases := []struct {
		name   string
		env    func(string) []string
		marker string // 盘上出现的临时文件或状态
	}{
		{"snapshot-tmp", func(string) []string {
			return []string{"MINIRAFT_SNAPSHOT_PAUSE_MS=3000"}
		}, "*.snap.tmp"},
		{"pointer-tmp", func(string) []string {
			return []string{"MINIRAFT_SNAPSHOT_PAUSE_MS=3000"}
		}, "current.json.tmp"},
		{"install-before-state", func(string) []string {
			return []string{"MINIRAFT_CRASH_POINT=install-before-state",
				"MINIRAFT_CRASH_PAUSE_MS=3000"}
		}, "state-behind-snapshot"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p := newLoopbackProcessCluster(t, 5, []string{
				"-snapshot-entries", "0", "-snapshot-bytes", "0",
				"-snapshot-exempt-entries", "1",
			}, nil)
			victim := ""

			leader, _ := p.writeThrough("snap-seed", "v")
			for _, id := range p.ids {
				if id != leader {
					victim = id
					break
				}
			}
			p.kill9(victim)
			// 停点只挂在受害者身上：压在 leader 上会让它在停顿时被选掉，
			// 新 leader 日志完整，直接补日志，安装快照这条路根本走不到。
			p.env[victim] = tc.env(victim)

			written := map[string]string{"snap-seed": "v"}
			for i := 0; i < 40; i++ {
				key := fmt.Sprintf("before-snap-%02d", i)
				p.writeThrough(key, "v")
				written[key] = "v"
			}
			snapIndex := p.triggerSnapshot(t, leader)
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("after-snap-%02d", i)
				p.writeThrough(key, "v")
				written[key] = "v"
			}

			p.startOneExpectInstall(t, victim)
			p.waitInstallWindow(t, victim, snapIndex, tc.marker)
			p.kill9(victim)
			p.clearEnv(victim)

			if tc.name == "install-before-state" {
				// 快照文件与指针已经提交、state 游标还没写：正确实现靠启动时
				// 以快照为准抬起游标，节点应当能起来并补齐。
				exited, code, out := p.probeRestart(victim, 6*time.Second)
				if exited {
					t.Fatalf("restart after the install-before-state window exited %d: %s\nstderr: %s",
						code, p.logTail(victim, 20), strings.TrimSpace(out))
				}
				p.startOne(victim)
			} else {
				p.startOne(victim)
			}

			for key, want := range written {
				if err := p.waitValue(victim, key, want, 30*time.Second); err != nil {
					t.Fatal(err)
				}
			}
			if tmps := p.snapshotFiles(victim, "*.tmp"); len(tmps) != 0 {
				t.Fatalf("tmp files survived the restart: %v", tmps)
			}
		})
	}
}

func (p *processCluster) triggerSnapshot(t *testing.T, leader string) uint64 {
	t.Helper()
	// 快照停点也挂在 leader 上时，一次手动快照要穿过三处各 3s 的停顿，客户端得等得住。
	resp, err := (&http.Client{Timeout: 30 * time.Second}).
		Post(p.url(leader, "/raft/snapshot"), "application/octet-stream", strings.NewReader(""))
	if err != nil {
		t.Fatalf("trigger snapshot on %s: %v", leader, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read snapshot reply from %s: %v", leader, err)
	}
	status := resp.StatusCode
	if status != http.StatusOK {
		t.Fatalf("snapshot on %s = %d: %s", leader, status, firstBytes(raw, 200))
	}
	var reply map[string]any
	if err := json.Unmarshal(raw, &reply); err != nil || reply["ok"] != true {
		t.Fatalf("snapshot reply = %s", firstBytes(raw, 200))
	}
	index := uint64(toFloat(reply["snapshot_index"]))
	if index == 0 {
		t.Fatalf("snapshot reply has no index: %v", reply)
	}
	return index
}

// startOneExpectInstall 起被停的成员，安装请求排上之后才会走到停点。
func (p *processCluster) startOneExpectInstall(t *testing.T, id string) {
	t.Helper()
	p.startOne(id)
}

func (p *processCluster) snapshotFiles(id, pattern string) []string {
	hits, _ := filepath.Glob(filepath.Join(p.root, id, RaftDirName, SnapshotDirName, pattern))
	return hits
}

// waitInstallWindow 等盘上出现指定窗口：临时文件，或者 state 游标已经越过快照点。
func (p *processCluster) waitInstallWindow(t *testing.T, id string, snapIndex uint64, marker string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if marker == "state-behind-snapshot" {
			if p.diskSnapshotIndex(id) >= snapIndex && p.stateLastApplied(id) < snapIndex {
				return
			}
		} else if len(p.snapshotFiles(id, marker)) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never reached the %s window: %s", id, marker, p.logTail(id, 15))
}

func (p *processCluster) stateLastApplied(id string) uint64 {
	raw, err := os.ReadFile(filepath.Join(p.root, id, "raft", "state"))
	if err != nil {
		return 0
	}
	var state struct {
		LastApplied uint64 `json:"lastApplied"`
	}
	if json.Unmarshal(raw, &state) != nil {
		return 0
	}
	return state.LastApplied
}

// probeRestart 用同一份数据目录起一次进程，只关心它能不能活过启动窗口：
// 到点仍在跑就是启动成功；提前退出返回退出码与 stderr。
func (p *processCluster) probeRestart(id string, timeout time.Duration) (bool, int, string) {
	args := []string{"-node-id", id, "-peers", p.peers,
		"-data-dir", filepath.Join(p.root, id), "-log-level", "debug"}
	args = append(args, p.flags...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return false, 0, out.String()
	}
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			code = -1
		}
	}
	return true, code, out.String()
}
